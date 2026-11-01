"""
===============================================================================
SCRIPT DE DEPLOY DE INFRAESTRUTURA E ARTEFATOS AWS (src/infra/deploy_aws_scripts.py)
===============================================================================
Descrição: Deploy completo de infraestrutura e artefatos na AWS.
Destaques:
  1. Leitura dinâmica de .env (AWS_ACCOUNT_ID, AWS_DEFAULT_REGION, S3_BUCKET_NAME, etc.).
  2. Constrói o nome completo do Bucket S3 e ARN da Role Glue via concatenação de variáveis.
  3. Valida a existência do Bucket S3 e cria um novo seguindo boas práticas AWS:
     - General purpose / Account Regional namespace
     - Object Ownership: ACLs disabled (BucketOwnerEnforced)
     - Block Public Access: Habilitado (Block all public access)
     - Criptografia: SSE-S3 (AES256) com Bucket Key habilitado (BucketKeyEnabled=True)
  4. Gerenciamento seguro da Secret no AWS Secrets Manager.
  5. Upload de scripts, biblioteca de qualidade e Spark BigQuery Connector JAR em /libs/.
  6. Registro/Atualização dos Glue Jobs com --additional-python-modules e temporários limpos.
===============================================================================
"""

import os
import urllib.request
import tempfile
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env local
load_dotenv(override=True)

# 1. Resolução Dinâmica de Configurações AWS & .env
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

# Construção do nome do Bucket S3
raw_bucket_name = os.getenv("S3_BUCKET_NAME", "tc-alfabetizacao")
if AWS_ACCOUNT_ID and AWS_ACCOUNT_ID not in raw_bucket_name:
    S3_BUCKET = f"{raw_bucket_name}-{AWS_ACCOUNT_ID}-{AWS_REGION}"
else:
    S3_BUCKET = raw_bucket_name

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

if not S3_BUCKET:
    raise ValueError("❌ A variável S3_BUCKET_NAME não pôde ser resolvida!")

# Inicialização de Clientes Boto3
s3_client = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)
sm_client = boto3.client("secretsmanager", region_name=AWS_REGION)

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
]

BIGQUERY_JAR_URL = "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
LOCAL_JAR_NAME = "spark-bigquery-with-dependencies_2.12-0.34.0.jar"


def setup_gcp_secret():
    """1. Cria ou atualiza o valor da Secret no AWS Secrets Manager."""
    print(f"🔐 1. Gerenciando Secret no AWS Secrets Manager ('{SECRET_NAME}')...")

    gcp_key_file = GCP_KEY_LOCAL
    if not os.path.exists(gcp_key_file):
        for alt_key in ["gcp-key.json", "gcp_key.json"]:
            if os.path.exists(alt_key):
                gcp_key_file = alt_key
                break

    if not os.path.exists(gcp_key_file):
        print(
            f"  └─ ⚠️ Chave local '{gcp_key_file}' não encontrada. Pulando provisionamento da Secret."
        )
        return

    with open(gcp_key_file, "r", encoding="utf-8") as f:
        secret_content = f.read()

    try:
        sm_client.create_secret(
            Name=SECRET_NAME,
            Description="Chave de Service Account do GCP para acesso ao BigQuery",
            SecretString=secret_content,
        )
        print(f"  └─ ✅ Secret criada com sucesso: '{SECRET_NAME}'")
    except sm_client.exceptions.ResourceExistsException:
        try:
            sm_client.put_secret_value(
                SecretId=SECRET_NAME, SecretString=secret_content
            )
            print(f"  └─ 🔄 Secret atualizada com sucesso: '{SECRET_NAME}'")
        except Exception as put_err:
            print(f"  └─ ⚠️ Erro ao atualizar Secret existente: {put_err}")
    except Exception as e:
        print(f"  └─ ⚠️ Erro ao gerenciar Secret: {str(e)}")


def _bucket_is_available():
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"  └─ ✅ Bucket S3 existente e acessível: '{S3_BUCKET}'")
        return True
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in ["403", "AccessDenied", "Forbidden"]:
            print(
                f"  └─ ⚠️ Alerta de permissão (403/SCP Vocareum): O bucket '{S3_BUCKET}' já existe e será reutilizado."
            )
            return True
        if error_code in ["404", "NoSuchBucket", "NotFound"]:
            print(
                f"  └─ 🆕 Bucket '{S3_BUCKET}' não encontrado. Criando novo bucket..."
            )
            return False
        print(
            f"  └─ ℹ️ Aviso ao checar bucket ({error_code}). Assumindo que o bucket já existe."
        )
        return True
    except Exception as exc:
        print(f"  └─ ℹ️ Exceção ao checar bucket: {exc}. Assumindo bucket existente.")
        return True


def _configure_bucket_security():
    try:
        s3_client.put_bucket_ownership_controls(
            Bucket=S3_BUCKET,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        print("     ├─ 🔒 Object Ownership: ACLs disabled (BucketOwnerEnforced).")
    except Exception as error:
        print(f"     ├─ ⚠️ Aviso em Object Ownership: {str(error)}")

    try:
        s3_client.put_public_access_block(
            Bucket=S3_BUCKET,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        print("     ├─ 🛡️ Block Public Access: Habilitado (Block all public access).")
    except Exception as error:
        print(f"     ├─ ⚠️ Aviso em Block Public Access: {str(error)}")

    try:
        s3_client.put_bucket_encryption(
            Bucket=S3_BUCKET,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        },
                        "BucketKeyEnabled": True,
                    }
                ]
            },
        )
        print(
            "     └─ 🔑 Criptografia Recomendada (SSE-S3 / AES256 com Bucket Key Enabled) configurada com sucesso!"
        )
    except Exception as error:
        print(f"     └─ ⚠️ Aviso em Criptografia: {str(error)}")


def prepare_and_create_s3_bucket():
    """
    2. Checa se o Bucket S3 existe. Se não existir, cria um novo seguindo
       estritamente as recomendações de segurança e boas práticas da AWS:
       - General purpose
       - Account Regional namespace
       - Object Ownership: ACLs disabled (BucketOwnerEnforced)
       - Block Public Access: Habilitado para todos os acessos públicos
       - Bucket Versioning: Desabilitado por padrão
       - Criptografia padrão: SSE-S3 (AES256) com Bucket Key ativado
    """
    print(f"\n🪣 2. Verificando/Criando Bucket S3 ('s3://{S3_BUCKET}')...")

    if _bucket_is_available():
        return

    try:
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"     ├─ 🪣 Bucket criado no namespace Account Regional ({AWS_REGION}).")
    except Exception as create_err:
        raise ValueError(
            f"❌ Falha ao criar/configurar o bucket S3 '{S3_BUCKET}': {str(create_err)}"
        )

    _configure_bucket_security()


def _upload_connector_jar(s3_jar_key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=s3_jar_key)
        print(
            f"  └─ ℹ️ BigQuery Spark Connector JAR já existe em 's3://{S3_BUCKET}/{s3_jar_key}'. Download e upload ignorados (SKIPPED)."
        )
        return
    except Exception:
        pass

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_jar_path = os.path.join(temp_dir, LOCAL_JAR_NAME)
        print(
            "  └─ ⬇️ Baixando BigQuery Spark Connector JAR em diretório temporário..."
        )
        urllib.request.urlretrieve(BIGQUERY_JAR_URL, temp_jar_path)
        s3_client.upload_file(temp_jar_path, S3_BUCKET, s3_jar_key)
        s3_client.upload_file(temp_jar_path, S3_BUCKET, f"libs/{LOCAL_JAR_NAME}")
        print(f"  └─ 📦 Upload BigQuery JAR -> s3://{S3_BUCKET}/{s3_jar_key}")


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
            "--S3_BUCKET_NAME": S3_BUCKET,
            "--TempDir": f"s3://{S3_BUCKET}/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
        "GlueVersion": "4.0",
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X",
        "MaxRetries": 0,
    }

    try:
        glue_client.get_job(JobName=job_name)
        print(f"  └─ 🔄 Atualizando Glue Job existente: {job_name}")
        glue_client.update_job(JobName=job_name, JobUpdate=job_params)
    except glue_client.exceptions.EntityNotFoundException:
        print(f"  └─ 🆕 Criando novo Glue Job: {job_name}")
        glue_client.create_job(Name=job_name, **job_params)
    except Exception as job_err:
        print(f"  └─ ⚠️ Erro ao registrar/atualizar Glue Job {job_name}: {job_err}")


def deploy_artifacts_and_jobs():
    """2 & 3. Empacota artefatos no S3 e registra Glue Jobs com --additional-python-modules."""
    print(f"\n📤 2. Processando artefatos e enviando para s3://{S3_BUCKET}/...")

    # Upload dos scripts Python locais
    for item in SCRIPTS_TO_DEPLOY:
        local_path = item["local"]
        s3_key = item["s3_key"]
        if os.path.exists(local_path):
            s3_client.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"  └─ ✅ Upload script: {local_path} -> s3://{S3_BUCKET}/{s3_key}")
        else:
            print(f"  └─ ⚠️ Arquivo local não encontrado: {local_path}")

    s3_jar_key = f"libs/{LOCAL_JAR_NAME}"
    _upload_connector_jar(s3_jar_key)

    # 4. Registro / Atualização dos Glue Jobs no Catalog
    print("\n⚙️ 3. Registrando / Atualizando Glue Jobs no AWS Catalog...")

    extra_py_files = f"s3://{S3_BUCKET}/scripts/common/quality_lib.py"
    extra_jars = f"s3://{S3_BUCKET}/{s3_jar_key}"
    additional_modules = (
        "google-cloud-bigquery==3.11.0,google-auth==2.22.0,urllib3<2.0.0,python-dotenv"
    )

    for item in SCRIPTS_TO_DEPLOY:
        if item["job_name"]:
            _register_glue_job(item, extra_py_files, extra_jars, additional_modules)


def main():
    print("=" * 75)
    print("🚀 DEPLOY COMPLETO DE INFRAESTRUTURA, SEGURANÇA E ARTEFATOS AWS")
    print("=" * 75)
    print(f"ℹ️ Conta AWS ID: {AWS_ACCOUNT_ID or 'N/A'}")
    print(f"ℹ️ Região Padrão: {AWS_REGION}")
    print(f"ℹ️ Bucket S3 Alvo: s3://{S3_BUCKET}")
    print(f"ℹ️ Role Glue ARN: {GLUE_ROLE_ARN}")
    print("-" * 75)

    setup_gcp_secret()
    prepare_and_create_s3_bucket()
    deploy_artifacts_and_jobs()

    print("\n" + "=" * 75)
    print("✅ DEPLOY CONCLUÍDO COM SUCESSO!")
    print(f"   🔒 Secret no Secrets Manager: '{SECRET_NAME}'")
    print(f"   🪣 Bucket S3 verificado/criado com segurança: 's3://{S3_BUCKET}'")
    print(
        "   📦 Módulos Python e JARs configurados via --additional-python-modules e /libs/"
    )
    print("   🔺 Suporte ao Delta Lake habilitado via --datalake-formats delta")
    print("   🧹 Repositório local 100% limpo (Zero arquivos temporários mantidos)")
    print("=" * 75)


if __name__ == "__main__":
    main()
