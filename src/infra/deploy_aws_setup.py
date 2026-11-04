"""
===============================================================================
SCRIPT DE DEPLOY DE INFRAESTRUTURA E ARTEFATOS AWS (src/infra/deploy_aws_setup.py)
===============================================================================
Descrição: Deploy completo de infraestrutura e artefatos na AWS.
Operações:
  1. Leitura dinâmica de .env (AWS_ACCOUNT_ID, AWS_DEFAULT_REGION, S3_BUCKET_NAME, etc.).
  2. Constrói o nome completo do Bucket S3 e ARN da Role Glue via concatenação de variáveis.
  3. Valida a existência do Bucket S3 e cria um novo seguindo boas práticas AWS:
     - General purpose / Account Regional namespace
     - Object Ownership: ACLs disabled (BucketOwnerEnforced)
     - Block Public Access: Habilitado (Block all public access)
     - Criptografia padrão: SSE-S3 (AES256)
  4. Gerenciamento seguro da Secret no AWS Secrets Manager.
  5. Upload de scripts, biblioteca de qualidade e Spark BigQuery Connector JAR em /libs/.
  6. Registro ou atualização dos Glue Jobs e configuração de dependências.
===============================================================================
"""

import json
import os
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from cloudwatch_dashboard import setup_cloudwatch_dashboard

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=True)

# 1. Resolução de configurações AWS e arquivo .env
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

# Construção do nome do Bucket S3
raw_bucket_name = os.getenv("S3_BUCKET_NAME", "").strip()
if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID not in raw_bucket_name:
    S3_BUCKET = f"{raw_bucket_name}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
else:
    S3_BUCKET = raw_bucket_name

# Tempo de expiração dos eventos processados na camada bronze
try:
    LIFECYCLE_RULES_EXPIRATION_DAYS = int(
        os.getenv("LIFECYCLE_RULES_EXPIRATION_DAYS", "7")
    )
except ValueError as error:
    raise ValueError(
        "LIFECYCLE_RULES_EXPIRATION_DAYS deve ser um inteiro positivo."
    ) from error
if LIFECYCLE_RULES_EXPIRATION_DAYS < 1:
    raise ValueError("LIFECYCLE_RULES_EXPIRATION_DAYS deve ser um inteiro positivo.")

# Construção da Role ARN do AWS Glue
raw_glue_role = os.getenv("AWS_GLUE_ROLE_ARN", "LabRole")
if raw_glue_role.startswith("arn:aws:iam::"):
    GLUE_ROLE_ARN = raw_glue_role
elif AWS_ACCOUNT_ID:
    GLUE_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{raw_glue_role}"
else:
    GLUE_ROLE_ARN = raw_glue_role

# Nome do Secret Manager & Chave Local GCP
SECRET_NAME = os.getenv(
    "GCP_BIGQUERY_KEY_SECRET_NAME", "pipeline-educacao/gcp/bigquery"
)
GCP_KEY_LOCAL = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_key.json")

# Inicialização de Clientes Boto3
s3_client = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)
sm_client = boto3.client("secretsmanager", region_name=AWS_REGION)
cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)
sts_client = boto3.client("sts", region_name=AWS_REGION)

SCRIPTS_TO_DEPLOY = [
    {
        "local": "src/scripts/ingestion_bronze_layer.py",
        "s3_key": "scripts/ingestion_bronze_layer.py",
        "job_name": "01_bronze_ingestion",
    },
    {
        "local": "src/scripts/quality_silver_layer.py",
        "s3_key": "scripts/quality_silver_layer.py",
        "job_name": "02_silver_processing",
    },
    {
        "local": "src/scripts/aggregate_gold_layer.py",
        "s3_key": "scripts/aggregate_gold_layer.py",
        "job_name": "03_gold_aggregation",
    },
    {
        "local": "src/scripts/events_processor.py",
        "s3_key": "scripts/events_processor.py",
        "job_name": "04_events_processor",
    },
    {
        "local": "src/common/quality_lib.py",
        "s3_key": "scripts/common/quality_lib.py",
        "job_name": None,
    },
    {
        "local": "src/common/manifest_lib.py",
        "s3_key": "scripts/common/manifest_lib.py",
        "job_name": None,
    },
    {
        "local": "src/common/gold_aggregation.py",
        "s3_key": "scripts/common/gold_aggregation.py",
        "job_name": None,
    },
]

BIGQUERY_JAR_URL = "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
LOCAL_JAR_NAME = "spark-bigquery-with-dependencies_2.12-0.34.0.jar"
DASHBOARD_NAME = f"{raw_bucket_name}-Observability-Dashboard"
GLUE_DATABASE_NAME = f"{raw_bucket_name}_db"


def _resolve_gcp_key_file():
    configured_path = Path(GCP_KEY_LOCAL)
    if not configured_path.is_absolute():
        configured_path = PROJECT_ROOT / configured_path

    candidates = [configured_path]
    candidates.extend(PROJECT_ROOT / name for name in ("gcp-key.json", "gcp_key.json"))
    for candidate in candidates:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError(
        f"Credencial GCP não encontrada. Verifique GOOGLE_APPLICATION_CREDENTIALS "
        f"({GCP_KEY_LOCAL})."
    )


def _validate_local_configuration():
    if not raw_bucket_name:
        raise ValueError("Defina S3_BUCKET_NAME no arquivo .env.")

    missing_scripts = [
        item["local"]
        for item in SCRIPTS_TO_DEPLOY
        if not (PROJECT_ROOT / item["local"]).is_file()
    ]
    if missing_scripts:
        raise FileNotFoundError(
            "Arquivos locais necessários ao deploy não encontrados: "
            + ", ".join(missing_scripts)
        )

    key_path = _resolve_gcp_key_file()
    try:
        credentials = json.loads(key_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"A credencial GCP em '{key_path.name}' não contém JSON válido."
        ) from error

    if (
        not isinstance(credentials, dict)
        or credentials.get("type") != "service_account"
        or not credentials.get("client_email")
        or not credentials.get("private_key")
    ):
        raise ValueError(
            f"A credencial GCP em '{key_path.name}' não é uma chave de service account válida."
        )


def _resolve_aws_identity():
    global AWS_ACCOUNT_ID, S3_BUCKET, GLUE_ROLE_ARN

    identity = sts_client.get_caller_identity()
    actual_account_id = identity.get("Account")
    caller_arn = identity.get("Arn", "")
    if not actual_account_id:
        raise RuntimeError("O AWS STS não retornou o ID da conta autenticada.")

    if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID != actual_account_id:
        raise ValueError(
            "AWS_ACCOUNT_ID não corresponde à conta das credenciais AWS ativas."
        )

    configured_account_id = AWS_ACCOUNT_ID
    AWS_ACCOUNT_ID = actual_account_id
    if configured_account_id and configured_account_id not in raw_bucket_name:
        S3_BUCKET = f"{raw_bucket_name}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
    else:
        S3_BUCKET = raw_bucket_name

    if raw_glue_role.startswith("arn:"):
        role_parts = raw_glue_role.split(":")
        if len(role_parts) < 6 or role_parts[2] != "iam":
            raise ValueError("AWS_GLUE_ROLE_ARN não é um ARN de IAM válido.")
        if role_parts[4] != AWS_ACCOUNT_ID:
            raise ValueError("AWS_GLUE_ROLE_ARN pertence a outra conta AWS.")
        GLUE_ROLE_ARN = raw_glue_role
    else:
        partition = caller_arn.split(":")[1] if caller_arn.startswith("arn:") else "aws"
        GLUE_ROLE_ARN = f"arn:{partition}:iam::{AWS_ACCOUNT_ID}:role/{raw_glue_role}"


def setup_gcp_secret():
    """1. Cria ou atualiza o valor da Secret no AWS Secrets Manager."""
    print(f" 1. Gerenciando Secret no AWS Secrets Manager ('{SECRET_NAME}')...")

    secret_content = _resolve_gcp_key_file().read_text(encoding="utf-8")

    try:
        sm_client.create_secret(
            Name=SECRET_NAME,
            Description="Chave de Service Account do GCP para acesso ao BigQuery",
            SecretString=secret_content,
        )
        print(f"  Secret criada: '{SECRET_NAME}'")
    except sm_client.exceptions.ResourceExistsException:
        sm_client.put_secret_value(SecretId=SECRET_NAME, SecretString=secret_content)
        print(f"  Secret atualizada: '{SECRET_NAME}'")


def _bucket_is_available():
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"  └─  Bucket S3 existente e acessível: '{S3_BUCKET}'")
        return True
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in ["404", "NoSuchBucket", "NotFound"]:
            print(f"  └─  Bucket '{S3_BUCKET}' não encontrado. Criando novo bucket...")
            return False
        raise RuntimeError(
            f"Não foi possível verificar o bucket S3 '{S3_BUCKET}' ({error_code})."
        ) from error


def setup_s3_lifecycle_rules():
    """
    Configura a Regra de Ciclo de Vida do S3 (Lifecycle Rule) para expirar/deletar
    automaticamente os arquivos da Landing Zone (bronze/events_raw_landing/) após X dias.
    """
    print(
        f"\n Configurando S3 Lifecycle Rule para Landing Zone em 's3://{S3_BUCKET}'..."
    )
    rule_id = "ExpireRawLandingZoneEventsAfter7Days"
    lifecycle_rule = {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Prefix": "bronze/events_raw_landing/"},
        "Expiration": {"Days": LIFECYCLE_RULES_EXPIRATION_DAYS},
    }

    try:
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=S3_BUCKET)
        existing_rules = response.get("Rules", [])
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code == "NoSuchLifecycleConfiguration":
            existing_rules = []
        else:
            raise RuntimeError(
                f"Não foi possível ler as regras de ciclo de vida do bucket "
                f"'{S3_BUCKET}' ({error_code})."
            ) from error

    rules = [rule for rule in existing_rules if rule.get("ID") != rule_id]
    rules.append(lifecycle_rule)
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=S3_BUCKET,
        LifecycleConfiguration={"Rules": rules},
    )
    print(
        "  Regra de expiração configurada para "
        f"'bronze/events_raw_landing/' ({LIFECYCLE_RULES_EXPIRATION_DAYS} dias)."
    )


def _configure_bucket_security():
    s3_client.put_bucket_ownership_controls(
        Bucket=S3_BUCKET,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
    )
    print("     Object Ownership configurado como BucketOwnerEnforced.")

    s3_client.put_public_access_block(
        Bucket=S3_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print("     Bloqueio de acesso público habilitado.")

    s3_client.put_bucket_encryption(
        Bucket=S3_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                }
            ]
        },
    )
    print("     Criptografia SSE-S3 (AES256) configurada.")


def prepare_and_create_s3_bucket():
    """
    2. Checa se o Bucket S3 existe. Se não existir, cria um novo seguindo
       as configurações de segurança definidas para o pipeline:
       - General purpose
       - Account Regional namespace
       - Object Ownership: ACLs disabled (BucketOwnerEnforced)
       - Block Public Access: Habilitado para todos os acessos públicos
       - Bucket Versioning: Desabilitado por padrão
       - Criptografia padrão: SSE-S3 (AES256)
    """
    print(f"\n 2. Verificando/Criando Bucket S3 ('s3://{S3_BUCKET}')...")

    if not _bucket_is_available():
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"     Bucket criado na região {AWS_REGION}.")

    _configure_bucket_security()
    setup_s3_lifecycle_rules()


def setup_glue_catalog_database():
    """
    3. Provisiona o Database 'tc_educacao_db' no AWS Glue Data Catalog
       para permitir o registro automático de tabelas e consulta imediata via Amazon Athena.
    """
    print(f"\n 3. Provisionando Glue Data Catalog Database ('{GLUE_DATABASE_NAME}')...")
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE_NAME,
                "Description": "Database for literacy education analytics.",
                "LocationUri": f"s3://{S3_BUCKET}/",
            }
        )
        print(f"  Database '{GLUE_DATABASE_NAME}' criado no Glue Catalog.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"  └─  Database '{GLUE_DATABASE_NAME}' já existe no Glue Catalog.")
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível provisionar o database '{GLUE_DATABASE_NAME}'."
        ) from error


def _upload_connector_jar(s3_jar_key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_jar_key)
        print(f"Connector JAR já existe em 's3://{S3_BUCKET}/{s3_jar_key}'.")
        return
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code not in {"404", "NoSuchKey", "NotFound"}:
            raise RuntimeError(
                f"Não foi possível verificar o connector JAR em "
                f"'s3://{S3_BUCKET}/{s3_jar_key}' ({error_code})."
            ) from error

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_jar_path = os.path.join(temp_dir, LOCAL_JAR_NAME)
        print("Baixando o Spark BigQuery Connector.")
        with urllib.request.urlopen(BIGQUERY_JAR_URL, timeout=60) as response:
            with open(temp_jar_path, "wb") as jar_file:
                shutil.copyfileobj(response, jar_file)

        if not zipfile.is_zipfile(temp_jar_path):
            raise ValueError(
                "O arquivo baixado para o Spark BigQuery Connector é inválido."
            )

        s3_client.upload_file(temp_jar_path, S3_BUCKET, s3_jar_key)
        print(f"  └─  Upload BigQuery JAR -> s3://{S3_BUCKET}/{s3_jar_key}")


def _register_glue_job(item, extra_py_files, extra_jars, additional_modules):
    job_name = item["job_name"]
    script_s3_path = f"s3://{S3_BUCKET}/{item['s3_key']}"
    job_params = {
        "Role": GLUE_ROLE_ARN,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_path,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--extra-py-files": extra_py_files,
            "--extra-jars": extra_jars,
            "--additional-python-modules": additional_modules,
            "--datalake-formats": "delta",
            "--enable-glue-datacatalog": "true",
            "--S3_BUCKET_NAME": S3_BUCKET,
            "--GLUE_DATABASE_NAME": GLUE_DATABASE_NAME,
            "--TempDir": f"s3://{S3_BUCKET}/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-observability-metrics": "true",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        "GlueVersion": "4.0",
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X",
        "MaxRetries": 0,
    }

    try:
        glue_client.get_job(JobName=job_name)
    except glue_client.exceptions.EntityNotFoundException:
        print(f"  └─  Criando novo Glue Job: {job_name}")
        glue_client.create_job(Name=job_name, **job_params)
        return
    except ClientError as job_err:
        raise RuntimeError(
            f"Não foi possível consultar o Glue Job '{job_name}'."
        ) from job_err

    try:
        print(f"  └─  Atualizando Glue Job existente: {job_name}")
        glue_client.update_job(JobName=job_name, JobUpdate=job_params)
    except ClientError as job_err:
        raise RuntimeError(
            f"Não foi possível atualizar o Glue Job '{job_name}'."
        ) from job_err


def setup_cloudwatch_alarms():
    """
    6. Provisiona CloudWatch Alarms para monitorar falhas nos 4 Glue Jobs.
    """
    print("\n 6. Provisionando Alertas do CloudWatch para os Glue Jobs...")

    glue_jobs = [
        "01_bronze_ingestion",
        "02_silver_processing",
        "03_gold_aggregation",
        "04_events_processor",
    ]

    for job in glue_jobs:
        alarm_name = f"{raw_bucket_name}-GlueJobFailure-{job}"
        try:
            cw_client.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alerta de falha ou erro crítico no AWS Glue Job '{job}'.",
                ActionsEnabled=True,
                MetricName="glue.error.ALL",
                Namespace="Glue",
                Dimensions=[
                    {"Name": "JobName", "Value": job},
                    {"Name": "JobRunId", "Value": "ALL"},
                    {"Name": "Type", "Value": "count"},
                    {"Name": "ObservabilityGroup", "Value": "error"},
                ],
                Statistic="Sum",
                Period=300,
                EvaluationPeriods=1,
                Threshold=1.0,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
                TreatMissingData="notBreaching",
            )
            print(f"  └─  Alarme CloudWatch configurado: '{alarm_name}'")
        except ClientError as error:
            raise RuntimeError(
                f"Não foi possível configurar o alarme CloudWatch para '{job}'."
            ) from error


def deploy_artifacts_and_jobs():
    """4. Empacota artefatos no S3 e registra Glue Jobs no Catalog."""
    print(f"\n 4. Processando artefatos e enviando para s3://{S3_BUCKET}/...")

    # Upload dos scripts Python locais
    for item in SCRIPTS_TO_DEPLOY:
        local_path = item["local"]
        s3_key = item["s3_key"]
        absolute_path = PROJECT_ROOT / local_path
        s3_client.upload_file(str(absolute_path), S3_BUCKET, s3_key)
        print(f"  Upload script: {local_path} -> s3://{S3_BUCKET}/{s3_key}")

    s3_jar_key = f"libs/{LOCAL_JAR_NAME}"
    _upload_connector_jar(s3_jar_key)

    # Registro / Atualização dos Glue Jobs no Catalog
    print("\n 5. Registrando / Atualizando Glue Jobs no AWS Catalog...")

    extra_py_files = ",".join(
        f"s3://{S3_BUCKET}/{item['s3_key']}"
        for item in SCRIPTS_TO_DEPLOY
        if item["s3_key"].startswith("scripts/common/")
    )
    extra_jars = f"s3://{S3_BUCKET}/{s3_jar_key}"
    additional_modules = (
        "google-cloud-bigquery==3.11.0,google-auth==2.22.0,urllib3<2.0.0,python-dotenv"
    )

    for item in SCRIPTS_TO_DEPLOY:
        if item["job_name"]:
            _register_glue_job(item, extra_py_files, extra_jars, additional_modules)


def main():
    _validate_local_configuration()
    _resolve_aws_identity()

    print("=" * 75)
    print("Provisionamento de recursos AWS")
    print("=" * 75)
    print(f" Conta AWS ID: {AWS_ACCOUNT_ID or 'N/A'}")
    print(f" Região Padrão: {AWS_REGION}")
    print(f" Bucket S3 Alvo: s3://{S3_BUCKET}")
    print(f" Role Glue ARN: {GLUE_ROLE_ARN}")
    print(f" Database Glue Catalog: {GLUE_DATABASE_NAME}")
    print("-" * 75)

    setup_gcp_secret()
    prepare_and_create_s3_bucket()
    setup_glue_catalog_database()
    deploy_artifacts_and_jobs()
    setup_cloudwatch_dashboard(
        cw_client=cw_client,
        aws_region=AWS_REGION,
        aws_account_id=AWS_ACCOUNT_ID,
        raw_bucket_name=raw_bucket_name,
        jobs=[item["job_name"] for item in SCRIPTS_TO_DEPLOY if item["job_name"]],
        dashboard_name=DASHBOARD_NAME,
    )
    setup_cloudwatch_alarms()

    print("\n" + "=" * 75)
    print("Provisionamento concluído.")
    print(f"    Secret no Secrets Manager: '{SECRET_NAME}'")
    print(
        f"    Bucket S3 e Lifecycle Rule : 's3://{S3_BUCKET}' (Expiração: {LIFECYCLE_RULES_EXPIRATION_DAYS} dias)"
    )
    print(f"    Database no Glue Catalog: '{GLUE_DATABASE_NAME}'")
    print(f"    Dashboard de Observabilidade: '{DASHBOARD_NAME}'")
    print("    Alarmes CloudWatch ativados para os 4 Glue Jobs")
    print(
        "    Módulos Python, JARs e --enable-glue-datacatalog configurados no AWS Catalog"
    )
    print("    Suporte ao Delta Lake habilitado via --datalake-formats delta")
    print("=" * 75)


if __name__ == "__main__":
    main()
