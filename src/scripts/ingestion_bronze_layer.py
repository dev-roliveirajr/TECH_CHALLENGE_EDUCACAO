"""
===============================================================================
JOB AWS GLUE - CAMADA BRONZE (src/scripts/ingestion_bronze_layer.py)
===============================================================================
Descrição: Ingestão das tabelas do BigQuery para o Amazon S3 na Camada Bronze.
Operações:
  - Credenciais GCP via AWS Secrets Manager.
  - Verificação de versão por metadados do BigQuery e estado registrado no S3.
  - Particionamento de saída baseado em metadados de tamanho e configuração Spark.
===============================================================================
"""

import sys
import os
import json
import math
import base64
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Inicialização do Contexto Spark e AWS Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuração de Otimização: Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

# Obter parâmetros de execução passados pelo Glue Job
job_name = "01_bronze_ingestion"
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job_name = args["JOB_NAME"]
except Exception:
    pass

# Tenta obter o JOB_RUN_ID nativo do Glue
job_run_id = None
for i, arg in enumerate(sys.argv):
    if arg in ["--JOB_RUN_ID", "--job-run-id"] and i + 1 < len(sys.argv):
        job_run_id = sys.argv[i + 1]
        break

if not job_run_id:
    try:
        args_run = getResolvedOptions(sys.argv, ["JOB_RUN_ID"])
        job_run_id = args_run["JOB_RUN_ID"]
    except Exception:
        job_run_id = f"brz_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

# Resolução dinâmica do Bucket S3
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ["S3_BUCKET_NAME"])
        S3_BUCKET = args_s3["S3_BUCKET_NAME"]
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("A variável/argumento S3_BUCKET_NAME não foi definida.")

BRONZE_BASE_PATH = f"s3://{S3_BUCKET}/bronze"
AUDIT_BASE_PATH = "auditoria/bronze"
SOURCE_STATE_PREFIX = f"{AUDIT_BASE_PATH}/source_versions"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
SECRET_NAME = os.getenv(
    "GCP_BIGQUERY_KEY_SECRET_NAME", "pipeline-educacao/gcp/bigquery"
)
TARGET_FILE_SIZE_MB = 128.0  # Tamanho alvo ideal por bloco Parquet no Data Lake
METADATA_COLUMNS = {"_ingested_at", "_source", "ref_ano", "ref_mes", "ref_dia"}


def fetch_gcp_credentials_in_memory():
    """Obtém as credenciais do GCP do Secrets Manager ou de fallback local explícito."""
    print(f" Consultando a Secret '{SECRET_NAME}' no AWS Secrets Manager.")
    sm_client = boto3.client("secretsmanager", region_name=AWS_REGION)

    try:
        sec_resp = sm_client.get_secret_value(SecretId=SECRET_NAME)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code != "ResourceNotFoundException":
            raise RuntimeError(
                f"Não foi possível obter a Secret '{SECRET_NAME}' do "
                "AWS Secrets Manager."
            ) from error
        sec_resp = None

    if sec_resp is not None:
        sec_string = sec_resp.get("SecretString")
        if not sec_string:
            raise ValueError(f"A Secret '{SECRET_NAME}' não contém um valor textual.")
        sec_json = _parse_gcp_service_account(sec_string, f"Secret '{SECRET_NAME}'")
        b64_creds = base64.b64encode(sec_string.encode("utf-8")).decode("utf-8")
        print(f"  Credenciais obtidas do AWS Secrets Manager ('{SECRET_NAME}').")
        return sec_json, b64_creds, sec_string

    local_fallback_enabled = (
        os.getenv("GCP_CREDENTIALS_LOCAL_FALLBACK", "").lower() == "true"
        and "--JOB_NAME" not in sys.argv
    )
    if local_fallback_enabled:
        local_credentials = _load_local_gcp_credentials()
        if local_credentials:
            return local_credentials

    raise RuntimeError(
        f"A Secret '{SECRET_NAME}' não foi encontrada no AWS Secrets Manager. "
        "Para execução local, habilite GCP_CREDENTIALS_LOCAL_FALLBACK=true "
        "e forneça gcp-key.json ou gcp_key.json."
    )


def _parse_gcp_service_account(content, source_name):
    try:
        credentials = json.loads(content)
    except json.JSONDecodeError as error:
        raise ValueError(f"{source_name} não contém JSON válido.") from error
    if (
        not isinstance(credentials, dict)
        or credentials.get("type") != "service_account"
        or not credentials.get("private_key")
        or not credentials.get("client_email")
    ):
        raise ValueError(
            f"{source_name} não contém uma credencial de service account válida."
        )
    return credentials


def _load_local_gcp_credentials():
    for local_key in ["gcp-key.json", "gcp_key.json"]:
        if not os.path.exists(local_key):
            continue
        with open(local_key, "r", encoding="utf-8") as credential_file:
            content = credential_file.read()
        credentials = _parse_gcp_service_account(
            content, f"Credencial local '{local_key}'"
        )
        encoded_credentials = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        print(f"  Credenciais locais carregadas: '{local_key}'.")
        return credentials, encoded_credentials, content
    return None


# Carregamento em memória das Credenciais
gcp_secret_dict, gcp_b64_credentials, gcp_raw_string = fetch_gcp_credentials_in_memory()
gcp_project_id = None
bq_client = None

if gcp_secret_dict:
    print(" Credenciais GCP carregadas.")
    credentials = service_account.Credentials.from_service_account_info(gcp_secret_dict)
    gcp_project_id = credentials.project_id
    bq_client = bigquery.Client(credentials=credentials, project=gcp_project_id)

    # Configuração global no Spark Session para o conector BigQuery
    spark.conf.set("parentProject", gcp_project_id)
    spark.conf.set("credentials", gcp_b64_credentials)
else:
    raise RuntimeError("Credenciais GCP não foram carregadas.")

s3_client = boto3.client("s3", region_name=AWS_REGION)

DATASET_BQ = "basedosdados.br_inep_avaliacao_alfabetizacao"

ENTITIES = [
    "alunos",
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]


def load_bigquery_table(table_id):
    """Carrega a tabela do BigQuery via Spark Connector."""
    try:
        reader = spark.read.format("bigquery").option("table", table_id)
        if gcp_project_id:
            reader = reader.option("parentProject", gcp_project_id)
        if gcp_b64_credentials:
            reader = reader.option("credentials", gcp_b64_credentials)
        return reader.load()
    except Exception as e:
        print(f"    Spark BigQuery Connector falhou para {table_id}: {e}")
        raise


def _format_source_modified(modified):
    if not isinstance(modified, datetime):
        raise ValueError("O BigQuery não retornou um horário de modificação válido.")
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    return modified.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_source_modified(value):
    if not isinstance(value, str):
        raise ValueError("O marcador de ingestão não contém source_modified válido.")
    try:
        modified = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            "O marcador de ingestão contém source_modified inválido."
        ) from error
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    return modified.astimezone(timezone.utc)


def _read_entity_state(entity):
    state_key = f"{SOURCE_STATE_PREFIX}/{entity}.json"
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=state_key)
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise RuntimeError(
            f"Não foi possível consultar o estado Bronze da entidade '{entity}'."
        ) from error

    try:
        state = json.loads(response["Body"].read().decode("utf-8"))
    except (
        KeyError,
        AttributeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' é inválido."
        ) from error

    if (
        not isinstance(state, dict)
        or state.get("entity") != entity
        or state.get("status") != "SUCCEEDED"
    ):
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' não representa "
            "uma gravação concluída."
        )
    if not isinstance(state.get("bq_rows"), int) or isinstance(
        state.get("bq_rows"), bool
    ):
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' não contém "
            "uma contagem de linhas válida."
        )
    if not isinstance(state.get("source_num_bytes"), int) or isinstance(
        state.get("source_num_bytes"), bool
    ):
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' não contém "
            "um tamanho de origem válido."
        )
    state["source_modified_at"] = _parse_source_modified(state.get("source_modified"))
    expected_prefix = f"s3://{S3_BUCKET}/bronze/{entity}/"
    if not isinstance(state.get("s3_prefix"), str) or not state["s3_prefix"].startswith(
        expected_prefix
    ):
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' contém um prefixo inválido."
        )
    return state


def _entity_state_has_data(entity, state):
    s3_prefix = state.get("s3_prefix")
    expected_prefix = f"s3://{S3_BUCKET}/bronze/{entity}/"
    if not isinstance(s3_prefix, str) or not s3_prefix.startswith(expected_prefix):
        raise ValueError(
            f"O marcador de ingestão da entidade '{entity}' contém um prefixo inválido."
        )
    if state["bq_rows"] == 0:
        return True

    request = {
        "Bucket": S3_BUCKET,
        "Prefix": s3_prefix.removeprefix(f"s3://{S3_BUCKET}/"),
        "MaxKeys": 1000,
    }
    while True:
        try:
            response = s3_client.list_objects_v2(**request)
        except ClientError as error:
            raise RuntimeError(
                f"Não foi possível verificar os dados Bronze da entidade '{entity}'."
            ) from error

        if any(
            item.get("Key", "").endswith(".parquet")
            for item in response.get("Contents", [])
        ):
            return True
        if not response.get("IsTruncated"):
            return False
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            raise RuntimeError(
                f"A listagem dos dados Bronze da entidade '{entity}' "
                "foi truncada sem token de continuação."
            )
        request["ContinuationToken"] = continuation_token


def _write_entity_state(entity, state):
    state_key = f"{SOURCE_STATE_PREFIX}/{entity}.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=state_key,
            Body=json.dumps(state, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível registrar a versão ingerida da entidade '{entity}'."
        ) from error


def _source_version_matches(state, modified_at, num_rows, num_bytes):
    return (
        state["source_modified_at"] == modified_at
        and state["bq_rows"] == num_rows
        and state["source_num_bytes"] == num_bytes
    )


def process_entity(entity):
    """Processa a ingestão de uma entidade com particionamento por data."""
    table_id = f"{DATASET_BQ}.{entity}"
    print(
        f"\n [Entidade {entity.upper()}] Checando metadados no BigQuery: '{table_id}'..."
    )

    try:
        # Metadados BigQuery
        table_meta = bq_client.get_table(table_id)
        mod_time = table_meta.modified
        num_rows = getattr(table_meta, "num_rows", -1)
        if not isinstance(num_rows, int) or isinstance(num_rows, bool):
            num_rows = -1
        num_bytes = getattr(table_meta, "num_bytes", 0) or 0
        size_mb = num_bytes / (1024.0 * 1024.0)
        source_modified = _format_source_modified(mod_time)
        source_modified_at = _parse_source_modified(source_modified)

        ref_ano = f"{mod_time.year}"
        ref_mes = f"{mod_time.month:02d}"
        ref_dia = f"{mod_time.day:02d}"

        previous_state = _read_entity_state(entity)
        if (
            previous_state
            and _source_version_matches(
                previous_state, source_modified_at, num_rows, num_bytes
            )
            and _entity_state_has_data(entity, previous_state)
        ):
            print(f"    [{entity.upper()}] A versão atual já está registrada no S3.")
            return {
                "entity": entity,
                "bq_rows": previous_state["bq_rows"],
                "s3_rows": None,
                "size_mb": previous_state.get("size_mb"),
                "status": "SKIPPED (Already Up-to-Date)",
                "partitions": 0,
                "source_modified": previous_state.get("source_modified"),
                "s3_prefix": previous_state["s3_prefix"],
            }

        print(f"    [{entity.upper()}] Nova versão detectada. Lendo BigQuery...")
        df = load_bigquery_table(table_id)

        # Validação contra colisão de colunas de metadados
        collisions = METADATA_COLUMNS.intersection(name.lower() for name in df.columns)
        if collisions:
            raise ValueError(
                f"Colunas de origem conflitam com metadados de auditoria: {sorted(collisions)}"
            )

        now_utc = datetime.now(timezone.utc).isoformat()
        df_bronze = (
            df.withColumn("_ingested_at", F.lit(now_utc))
            .withColumn("_source", F.lit("basedosdados_bigquery"))
            .withColumn("ref_ano", F.lit(ref_ano))
            .withColumn("ref_mes", F.lit(ref_mes))
            .withColumn("ref_dia", F.lit(ref_dia))
        )

        output_path = f"{BRONZE_BASE_PATH}/{entity}/"
        print(f"   └─  [{entity.upper()}] Gravando no S3 em: {output_path}")

        # Otimização Dinâmica: Cálculo de partições baseado no bloco alvo (128 MB)
        target_partitions = max(1, math.ceil(size_mb / TARGET_FILE_SIZE_MB))
        current_partitions = df.rdd.getNumPartitions()

        if target_partitions < current_partitions:
            df_writer = df_bronze.coalesce(target_partitions)
            print(
                f"   └─  [{entity.upper()}] Coalesce dinâmico aplicado: {current_partitions} ➔ {target_partitions} partição(ões) ({size_mb:.2f} MB / {TARGET_FILE_SIZE_MB} MB alvo)."
            )
        else:
            df_writer = df_bronze
            print(
                f"   └─  [{entity.upper()}] Mantido particionamento Spark: {current_partitions} partição(ões) ({size_mb:.2f} MB)."
            )

        df_writer.write.mode("overwrite").partitionBy(
            "ref_ano", "ref_mes", "ref_dia"
        ).parquet(output_path)

        state = {
            "entity": entity,
            "table_id": table_id,
            "source_modified": source_modified,
            "bq_rows": num_rows,
            "source_num_bytes": num_bytes,
            "size_mb": round(size_mb, 2),
            "status": "SUCCEEDED",
            "s3_prefix": (
                f"s3://{S3_BUCKET}/bronze/{entity}/"
                f"ref_ano={ref_ano}/ref_mes={ref_mes}/ref_dia={ref_dia}/"
            ),
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        _write_entity_state(entity, state)

        rows_str = f"{num_rows:,} linhas no BigQuery" if num_rows >= 0 else "concluída"
        print(f"   [{entity.upper()}] Gravação Bronze concluída ({rows_str}).")
        return {
            "entity": entity,
            "bq_rows": num_rows,
            "s3_rows": None,
            "size_mb": round(size_mb, 2),
            "status": "SUCCEEDED",
            "partitions": df_writer.rdd.getNumPartitions(),
            "source_modified": source_modified,
            "s3_prefix": state["s3_prefix"],
        }

    except Exception as e:
        print(f"   └─  [{entity.upper()}] Falha no processamento: {str(e)}")
        raise RuntimeError(
            f"Pipeline Bronze interrompido na entidade '{entity}': {str(e)}"
        ) from e


def main():
    print("=" * 75)
    print(" Iniciando ingestão da Camada Bronze.")
    print(f"   Bucket de Destino: {BRONZE_BASE_PATH}")
    print("=" * 75)

    started_at_utc = datetime.now(timezone.utc)
    ingestion_date = started_at_utc.strftime("%Y-%m-%d")
    summary_report = []

    max_workers = min(len(ENTITIES), 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_entity = {
            executor.submit(process_entity, entity): entity for entity in ENTITIES
        }
        for future in as_completed(future_to_entity):
            entity = future_to_entity[future]
            try:
                res = future.result()
                summary_report.append(res)
            except Exception as exc:
                print(f" Erro na execução da entidade {entity}: {exc}")
                raise exc

    # Ordena o resumo de acordo com a ordem oficial das ENTITIES
    summary_report.sort(key=lambda x: ENTITIES.index(x["entity"]))
    print("\n" + "=" * 75)
    print("Resumo da ingestão Bronze:")
    for item in summary_report:
        ent = item["entity"]
        status = item["status"]
        count = item["bq_rows"]
        rows_str = (
            f"({count:,} linhas nos metadados da origem)"
            if status == "SUCCEEDED"
            else "(sem gravação nesta execução)"
        )
        print(f" - {ent.ljust(30)}: {status} {rows_str}")
    print("=" * 75)
    #  Gravação do Manifesto de Auditoria no S3
    manifest_key = f"{AUDIT_BASE_PATH}/ingestion_date={ingestion_date}/run_id={job_run_id}/manifest.json"
    manifest_payload = {
        "job_name": job_name,
        "run_id": job_run_id,
        "ingestion_date": ingestion_date,
        "started_at_utc": started_at_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "finished_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": "SUCCEEDED",
        "is_full_batch": True,
        "total_entities": len(ENTITIES),
        "manifest_s3_uri": f"s3://{S3_BUCKET}/{manifest_key}",
        "tables_metrics": summary_report,
    }

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_payload, indent=2, ensure_ascii=False).encode(
                "utf-8"
            ),
            ContentType="application/json",
        )
    except ClientError as manifest_err:
        raise RuntimeError(
            f"Não foi possível salvar o manifesto Bronze em "
            f"s3://{S3_BUCKET}/{manifest_key}."
        ) from manifest_err

    print("\n Manifesto Bronze salvo no S3:")
    print(f"   s3://{S3_BUCKET}/{manifest_key}")
    print("Camada Bronze concluída.")


if __name__ == "__main__":
    main()
