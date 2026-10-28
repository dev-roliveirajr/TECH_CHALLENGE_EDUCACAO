"""
===============================================================================
JOB AWS GLUE - CAMADA BRONZE (src/scripts/ingestion_bronze_layer.py)
===============================================================================
Descrição: Ingestão incremental, idempotente e concorrente das entidades oficiais
da educação do GCP BigQuery para o Amazon S3 na Camada Bronze.
DevSecOps & Performance:
  - Autenticação 100% IN-MEMORY via AWS Secrets Manager.
  - Resolução dinâmica do Bucket S3 e da Secret via env/argumentos.
  - Particionamento dinâmico baseado em tamanho de bloco alvo (128 MB).
  - Otimização com Adaptive Query Execution (AQE) do Spark 3.3+.
  - Extração de contagem de linhas e tamanho via metadados BQ (Zero Scan).
===============================================================================
"""

import sys
import os
import json
import math
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
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

# Obter parâmetros de execução passados pelo Glue Job
job_name = "01_bronze_ingestion"
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
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
        args_run = getResolvedOptions(sys.argv, ['JOB_RUN_ID'])
        job_run_id = args_run['JOB_RUN_ID']
    except Exception:
        job_run_id = f"brz_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

# Resolução dinâmica do Bucket S3
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ['S3_BUCKET_NAME'])
        S3_BUCKET = args_s3['S3_BUCKET_NAME']
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("❌ A variável/argumento S3_BUCKET_NAME não foi definida!")

BRONZE_BASE_PATH = f"s3://{S3_BUCKET}/bronze"
AUDIT_BASE_PATH = f"auditoria/bronze"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1"))
SECRET_NAME = os.getenv("GCP_BIGQUERY_KEY_SECRET_NAME", "pipeline-educacao/gcp/bigquery")
TARGET_FILE_SIZE_MB = 128.0  # Tamanho alvo ideal por bloco Parquet no Data Lake
METADATA_COLUMNS = {"_ingested_at", "_source", "ref_ano", "ref_mes", "ref_dia"}

def fetch_gcp_credentials_in_memory():
    """Obtém as credenciais do GCP diretamente do AWS Secrets Manager"""
    print(f"🔐 [SecOps] Buscando Secret '{SECRET_NAME}' no AWS Secrets Manager (In-Memory)...")
    sm_client = boto3.client("secretsmanager", region_name=AWS_REGION)
    
    try:
        sec_resp = sm_client.get_secret_value(SecretId=SECRET_NAME)
        sec_string = sec_resp.get("SecretString")
        if sec_string:
            sec_json = json.loads(sec_string)
            if "type" in sec_json and "private_key" in sec_json:
                b64_creds = base64.b64encode(sec_string.encode('utf-8')).decode('utf-8')
                print(f"  └─ ✅ Credenciais obtidas em memória RAM via Secrets Manager ('{SECRET_NAME}'). Zero disco utilizado.")
                return sec_json, b64_creds, sec_string
    except Exception:
        pass

    # Fallback para dev local se houver gcp-key.json no ambiente local
    for local_key in ["gcp-key.json", "gcp_key.json"]:
        if os.path.exists(local_key):
            print(f"  └─ ℹ️ [Dev Local] Carregando chave local para memória: {local_key}")
            with open(local_key, "r", encoding="utf-8") as f:
                content = f.read()
                sec_json = json.loads(content)
                b64_creds = base64.b64encode(content.encode('utf-8')).decode('utf-8')
                return sec_json, b64_creds, content

    return None, None, None

# Carregamento 100% In-Memory das Credenciais
gcp_secret_dict, gcp_b64_credentials, gcp_raw_string = fetch_gcp_credentials_in_memory()
gcp_project_id = None
bq_client = None

if gcp_secret_dict:
    print("🔑 Autenticação GCP ativada 100% In-Memory (RAM).")
    credentials = service_account.Credentials.from_service_account_info(gcp_secret_dict)
    gcp_project_id = credentials.project_id
    bq_client = bigquery.Client(credentials=credentials, project=gcp_project_id)
    
    # Configuração global no Spark Session para o conector BigQuery
    spark.conf.set("parentProject", gcp_project_id)
    spark.conf.set("credentials", gcp_b64_credentials)
else:
    print("⚠️ Nenhuma Secret encontrada. Inicializando cliente BigQuery padrão...")
    bq_client = bigquery.Client()

s3_client = boto3.client("s3", region_name=AWS_REGION)

DATASET_BQ = "basedosdados.br_inep_avaliacao_alfabetizacao"

ENTITIES = [
    "alunos",
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario"
]

def load_bigquery_table(table_id):
    """Carrega a tabela do BigQuery via Spark Connector com credenciais Base64 In-Memory (ou SDK Fallback)."""
    try:
        reader = spark.read.format("bigquery").option("table", table_id)
        if gcp_project_id:
            reader = reader.option("parentProject", gcp_project_id)
        if gcp_b64_credentials:
            reader = reader.option("credentials", gcp_b64_credentials)
        return reader.load()
    except Exception as spark_err:
        print(f"   ⚠️ Spark BigQuery Connector falhou para {table_id}: {spark_err}")
        print("   🔄 Executando leitura via Google BigQuery SDK (In-Memory)...")
        query_job = bq_client.query(f"SELECT * FROM `{table_id}`")
        results = list(query_job.result())
        if not results:
            raise ValueError(f"A tabela '{table_id}' retornou 0 registros no BigQuery.")
        rows = [dict(row.items()) for row in results]
        return spark.createDataFrame(rows)

def process_entity(entity):
    """Processa a ingestão de uma entidade individual de forma isolada, resiliente e dinamicamente particionada."""
    table_id = f"{DATASET_BQ}.{entity}"
    print(f"\n🔍 [Entidade {entity.upper()}] Checando metadados no BigQuery: '{table_id}'...")
    
    try:
        # Metadados instantâneos sem scan (Zero Cost)
        table_meta = bq_client.get_table(table_id)
        mod_time = table_meta.modified
        num_rows = getattr(table_meta, "num_rows", -1)
        num_bytes = getattr(table_meta, "num_bytes", 0) or 0
        size_mb = num_bytes / (1024.0 * 1024.0)
        
        ref_ano = f"{mod_time.year}"
        ref_mes = f"{mod_time.month:02d}"
        ref_dia = f"{mod_time.day:02d}"
        
        s3_partition_prefix = f"bronze/{entity}/ref_ano={ref_ano}/ref_mes={ref_mes}/ref_dia={ref_dia}/"
        
        s3_response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=s3_partition_prefix,
            MaxKeys=1
        )
        
        if "Contents" in s3_response and len(s3_response["Contents"]) > 0:
            print(f"   ℹ️ [{entity.upper()}] Dados já atualizados para {ref_ano}-{ref_mes}-{ref_dia}. Ingestão SKIPPED.")
            return {
                "entity": entity,
                "bq_rows": num_rows,
                "s3_rows": 0,
                "size_mb": round(size_mb, 2),
                "status": "SKIPPED (Already Up-to-Date)",
                "partitions": 0,
                "s3_prefix": f"s3://{S3_BUCKET}/{s3_partition_prefix}"
            }

        print(f"   📦 [{entity.upper()}] Nova versão detectada ({ref_ano}-{ref_mes}-{ref_dia}). Lendo BigQuery...")
        df = load_bigquery_table(table_id)

        # Validação contra colisão de colunas de metadados
        collisions = METADATA_COLUMNS.intersection(name.lower() for name in df.columns)
        if collisions:
            raise ValueError(f"Colunas de origem conflitam com metadados de auditoria: {sorted(collisions)}")
        
        now_utc = datetime.utcnow().isoformat()
        df_bronze = df \
            .withColumn("_ingested_at", F.lit(now_utc)) \
            .withColumn("_source", F.lit("basedosdados_bigquery")) \
            .withColumn("ref_ano", F.lit(ref_ano)) \
            .withColumn("ref_mes", F.lit(ref_mes)) \
            .withColumn("ref_dia", F.lit(ref_dia))
        
        output_path = f"{BRONZE_BASE_PATH}/{entity}/"
        print(f"   └─ 💾 [{entity.upper()}] Gravando no S3 em: {output_path}")
        
        # Otimização Dinâmica: Cálculo de partições baseado no bloco alvo (128 MB)
        target_partitions = max(1, math.ceil(size_mb / TARGET_FILE_SIZE_MB))
        current_partitions = df.rdd.getNumPartitions()
        
        if target_partitions < current_partitions:
            df_writer = df_bronze.coalesce(target_partitions)
            print(f"   └─ ⚡ [{entity.upper()}] Coalesce dinâmico aplicado: {current_partitions} ➔ {target_partitions} partição(ões) ({size_mb:.2f} MB / {TARGET_FILE_SIZE_MB} MB alvo).")
        else:
            df_writer = df_bronze
            print(f"   └─ ⚡ [{entity.upper()}] Mantido particionamento Spark: {current_partitions} partição(ões) ({size_mb:.2f} MB).")

        df_writer.write \
            .mode("append") \
            .partitionBy("ref_ano", "ref_mes", "ref_dia") \
            .parquet(output_path)
        
        rows_str = f"{num_rows:,} linhas" if num_rows >= 0 else "sucesso"
        print(f"   └─ ✅ [{entity.upper()}] Gravada na Camada Bronze com sucesso ({rows_str})!")
        return {
            "entity": entity,
            "bq_rows": num_rows,
            "s3_rows": num_rows,
            "size_mb": round(size_mb, 2),
            "status": "SUCCEEDED",
            "partitions": target_partitions,
            "s3_prefix": f"s3://{S3_BUCKET}/{s3_partition_prefix}"
        }
        
    except Exception as e:
        print(f"   └─ ❌ [{entity.upper()}] Falha no processamento: {str(e)}")
        raise RuntimeError(f"Pipeline Bronze interrompido na entidade '{entity}': {str(e)}") from e

def main():
    print("=" * 75)
    print(f"🚀 INICIANDO INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY & PARTICIONAMENTO DINÂMICO 128MB)")
    print(f"   Bucket de Destino: {BRONZE_BASE_PATH}")
    print("=" * 75)

    started_at_utc = datetime.utcnow()
    ingestion_date = started_at_utc.strftime("%Y-%m-%d")
    summary_report = []

    max_workers = min(len(ENTITIES), 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_entity = {executor.submit(process_entity, entity): entity for entity in ENTITIES}
        for future in as_completed(future_to_entity):
            entity = future_to_entity[future]
            try:
                res = future.result()
                summary_report.append(res)
            except Exception as exc:
                print(f"❌ Erro na execução da entidade {entity}: {exc}")
                raise exc

    # Ordena o resumo de acordo com a ordem oficial das ENTITIES
    summary_report.sort(key=lambda x: ENTITIES.index(x["entity"]))
    print("\n" + "=" * 75)
    print("RESUMO DA INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY):")
    for item in summary_report:
        ent = item["entity"]
        status = item["status"]
        count = item["bq_rows"]
        rows_str = f"({count:,} linhas ingeridas)" if status == "SUCCEEDED" else "(Already Up-to-Date)"
        print(f" - {ent.ljust(30)}: {status} {rows_str}")
    print("=" * 75)
    print("✅ Camada Bronze concluída com sucesso!")

    # 📜 Gravação do Manifesto de Auditoria no S3
    manifest_key = f"{AUDIT_BASE_PATH}/ingestion_date={ingestion_date}/run_id={job_run_id}/manifest.json"
    manifest_payload = {
        "job_name": job_name,
        "run_id": job_run_id,
        "ingestion_date": ingestion_date,
        "started_at_utc": started_at_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "finished_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": "SUCCEEDED",
        "is_full_batch": True,
        "total_entities": len(ENTITIES),
        "manifest_s3_uri": f"s3://{S3_BUCKET}/{manifest_key}",
        "tables_metrics": summary_report
    }

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_payload, indent=2, ensure_ascii=False).encode('utf-8'),
            ContentType="application/json"
        )
        print(f"\n📜 [Auditoria & Governança] Manifesto salvo no S3 com sucesso:")
        print(f"   └─ s3://{S3_BUCKET}/{manifest_key}")
    except Exception as manifest_err:
        print(f"⚠️ Alerta: Falha ao salvar manifesto no S3: {str(manifest_err)}")

if __name__ == "__main__":
    main()
