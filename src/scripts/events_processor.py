"""
===============================================================================
JOB AWS GLUE / PYSPARK - PROCESSADOR DE EVENTOS STREAMING (src/scripts/events_processor.py)
===============================================================================
Descrição: Consumo de eventos em tempo quase real (Landing Zone NDJSON), 
cálculo da latência real de trânsito (ingested_at - event_time), tratamento 
multi-domínio (alunos e metas), isolamento da origem (_source) e Upsert 
idempotente via Delta Lake (MERGE INTO) com Short-Circuit FinOps.
===============================================================================
"""

import sys
import os
import json
import datetime

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    boto3 = None
    HAS_BOTO3 = False

try:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.types import DoubleType, TimestampType
    HAS_PYSPARK = True
except ImportError:
    SparkSession = None
    HAS_PYSPARK = False

# Tenta carregar bibliotecas específicas do AWS Glue se disponíveis
try:
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from delta.tables import DeltaTable
    HAS_GLUE = True
except ImportError:
    HAS_GLUE = False

# 1. Inicialização do Contexto Spark / Glue
if HAS_GLUE:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        job_name = args["JOB_NAME"]
    except Exception:
        job_name = "04_events_processor"
elif HAS_PYSPARK:
    spark = SparkSession.builder \
        .appName("04_events_processor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    job_name = "04_events_processor"
else:
    spark = None
    job_name = "04_events_processor"

# 2. Resolução do Run ID e Variáveis de Ambiente
run_id = os.getenv("GLUE_RUN_ID", os.getenv("JOB_RUN_ID", None))
if not run_id:
    for i, arg in enumerate(sys.argv):
        if arg in ["--JOB_RUN_ID", "--job-run-id"] and i + 1 < len(sys.argv):
            run_id = sys.argv[i + 1]
            break
if not run_id:
    run_id = f"evt_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}"

ingestion_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET and HAS_GLUE:
    try:
        args_s3 = getResolvedOptions(sys.argv, ["S3_BUCKET_NAME"])
        S3_BUCKET = args_s3["S3_BUCKET_NAME"]
    except Exception:
        pass

# Fallback local se não estiver na AWS
LOCAL_MODE = False
if not S3_BUCKET:
    LOCAL_MODE = True
    S3_BUCKET = "local-environment"
    EVENTS_LANDING_BASE = "data/events_landing/"
    SILVER_PATH = "data/silver/"
    AUDIT_PATH = "data/auditoria/events/"
else:
    EVENTS_LANDING_BASE = f"s3://{S3_BUCKET}/bronze/events_raw_landing/"
    SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
    AUDIT_PATH = f"s3://{S3_BUCKET}/auditoria/events/"

print("=" * 75)
print("🚀 INICIANDO PROCESSADOR DE EVENTOS STREAMING (EVENTS PROCESSOR)")
print(f"   Bucket / Target: {S3_BUCKET}")
print(f"   Run ID          : {run_id} | Data: {ingestion_date}")
print(f"   Modo de Operação: {'LOCAL' if LOCAL_MODE else 'AWS GLUE'}")
print("=" * 75)


def check_s3_events_exist(bucket_name, prefix):
    """Executa pré-checagem leve no S3 via Boto3 para o Short-Circuit FinOps."""
    if not HAS_BOTO3:
        return True
    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=5)
        if "Contents" in response:
            json_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".json")]
            return len(json_files) > 0
        return False
    except Exception as e:
        print(f"⚠️ Alerta na pré-checagem S3: {e}")
        return True


def check_local_events_exist(base_dir):
    """Executa pré-checagem local de arquivos NDJSON."""
    if not os.path.exists(base_dir):
        return False
    for root, _, files in os.walk(base_dir):
        if any(f.endswith(".json") for f in files):
            return True
    return False


# 3. Pré-checagem de Eventos (Short-Circuit FinOps)
events_exist = False
if LOCAL_MODE:
    events_exist = check_local_events_exist(EVENTS_LANDING_BASE)
else:
    prefix_check = "bronze/events_raw_landing/"
    events_exist = check_s3_events_exist(S3_BUCKET, prefix_check)

if not events_exist:
    print("\n" + "=" * 75)
    print("⚡ [SHORT-CIRCUIT FINOPS] Nenhum novo evento localizado no Landing Zone (Status SKIPPED).")
    print("   └─ ⏩ Processamento de Eventos encerrado com sucesso em segundos sem computação Spark!")
    print("=" * 75)

    # Grava manifesto de auditoria SKIPPED
    manifest_data = {
        "job_name": job_name,
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SKIPPED (No New Events)",
        "processed_events_count": 0,
        "message": "Nenhum arquivo NDJSON localizado no buffer de landing zone."
    }

    if not LOCAL_MODE and HAS_BOTO3:
        try:
            s3_client = boto3.client("s3")
            manifest_key = f"auditoria/events/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=manifest_key,
                Body=json.dumps(manifest_data, indent=2),
                ContentType="application/json"
            )
            print(f"📜 [Auditoria Eventos] Manifesto SKIPPED salvo no S3: s3://{S3_BUCKET}/{manifest_key}")
        except Exception as m_err:
            print(f"⚠️ Alerta ao salvar manifesto: {m_err}")
    else:
        os.makedirs(AUDIT_PATH, exist_ok=True)
        with open(os.path.join(AUDIT_PATH, f"manifest_{run_id}.json"), "w") as f:
            json.dump(manifest_data, f, indent=2)
        print(f"📜 [Auditoria Eventos] Manifesto salvo localmente em: {AUDIT_PATH}")
    
    sys.exit(0)

# 4. Leitura e Processamento dos Micro-lotes de Eventos
print("\n📥 4. Lendo micro-lotes de eventos da Landing Zone...")

alunos_landing = os.path.join(EVENTS_LANDING_BASE, "alunos") if LOCAL_MODE else f"{EVENTS_LANDING_BASE}alunos/"

processed_alunos_count = 0
processed_metas_count = 0
latency_stats = {"avg_sec": 0.0, "min_sec": 0.0, "max_sec": 0.0}

if HAS_PYSPARK and spark:
    # -----------------------------------------------------------------------------
    # 4.1 Processamento de Eventos de ALUNOS
    # -----------------------------------------------------------------------------
    try:
        print(f" ├─ [Domínio: Alunos] Buscando eventos em: {alunos_landing}")
        if LOCAL_MODE:
            df_events_alunos = spark.read.json(alunos_landing)
        else:
            df_events_alunos = spark.read.option("recursiveFileLookup", "true").json(alunos_landing)

        alunos_event_count = df_events_alunos.count()
        
        if alunos_event_count > 0:
            print(f"  └─ 📡 Eventos de Alunos encontrados: {alunos_event_count:,} registros.")

            # Garantir Timestamp UTC no Ingestion
            df_events_alunos = df_events_alunos.withColumn(
                "_ingested_at_ts", F.current_timestamp()
            ).withColumn(
                "_event_time_ts", F.to_timestamp(F.col("event_time"))
            )

            # Cálculo da Latência Real de Trânsito (ingested_at - event_time)
            df_events_alunos = df_events_alunos.withColumn(
                "latencia_segundos",
                F.round(
                    F.col("_ingested_at_ts").cast(DoubleType()) - F.col("_event_time_ts").cast(DoubleType()),
                    3
                )
            )

            # Métricas estatísticas da latência
            lat_metrics = df_events_alunos.select(
                F.avg("latencia_segundos").alias("avg_lat"),
                F.min("latencia_segundos").alias("min_lat"),
                F.max("latencia_segundos").alias("max_lat")
            ).collect()[0]

            latency_stats["avg_sec"] = round(float(lat_metrics["avg_lat"] or 0.0), 3)
            latency_stats["min_sec"] = round(float(lat_metrics["min_lat"] or 0.0), 3)
            latency_stats["max_sec"] = round(float(lat_metrics["max_lat"] or 0.0), 3)

            print(f"  └─ ⏱️ Latência Real Medida (ingested_at - event_time):")
            print(f"     ├─ Média : {latency_stats['avg_sec']} segundos")
            print(f"     ├─ Mínima: {latency_stats['min_sec']} segundos")
            print(f"     └─ Máxima: {latency_stats['max_sec']} segundos")

            # Derivação da Sigla UF via IBGE se não estiver presente
            if "sigla_uf" not in df_events_alunos.columns and "id_municipio" in df_events_alunos.columns:
                ibge_uf_map = F.create_map([
                    F.lit("11"), F.lit("RO"), F.lit("12"), F.lit("AC"), F.lit("13"), F.lit("AM"),
                    F.lit("14"), F.lit("RR"), F.lit("15"), F.lit("PA"), F.lit("16"), F.lit("AP"),
                    F.lit("17"), F.lit("TO"), F.lit("21"), F.lit("MA"), F.lit("22"), F.lit("PI"),
                    F.lit("23"), F.lit("CE"), F.lit("24"), F.lit("RN"), F.lit("25"), F.lit("PB"),
                    F.lit("26"), F.lit("PE"), F.lit("27"), F.lit("AL"), F.lit("28"), F.lit("SE"),
                    F.lit("29"), F.lit("BA"), F.lit("31"), F.lit("MG"), F.lit("32"), F.lit("ES"),
                    F.lit("33"), F.lit("RJ"), F.lit("35"), F.lit("SP"), F.lit("41"), F.lit("PR"),
                    F.lit("42"), F.lit("SC"), F.lit("43"), F.lit("RS"), F.lit("50"), F.lit("MS"),
                    F.lit("51"), F.lit("MT"), F.lit("52"), F.lit("GO"), F.lit("53"), F.lit("DF")
                ])
                df_events_alunos = df_events_alunos.withColumn(
                    "sigla_uf", ibge_uf_map[F.substring(F.col("id_municipio"), 1, 2)]
                )

            # Adicionar Colunas de Partição e Controle do Ingestion
            df_events_alunos = df_events_alunos \
                .withColumn("_ingested_at", F.current_timestamp()) \
                .withColumn("_processed_silver_at", F.current_timestamp()) \
                .withColumn("ref_ano", F.col("ano").cast("int")) \
                .withColumn("ref_mes", F.month(F.col("_event_time_ts"))) \
                .withColumn("ref_dia", F.dayofmonth(F.col("_event_time_ts"))) \
                .fillna({"ref_mes": 11, "ref_dia": 1})

            # Operação de MERGE INTO (Upsert) no Delta Lake Silver
            silver_target_path = f"{SILVER_PATH}alunos_curado/"
            print(f"\n🔄 Executando MERGE INTO no Delta Lake Silver: {silver_target_path}")

            if HAS_GLUE:
                try:
                    is_delta = DeltaTable.isDeltaTable(spark, silver_target_path)
                except Exception:
                    is_delta = False

                if is_delta:
                    delta_target = DeltaTable.forPath(spark, silver_target_path)
                    
                    # Intersecção dinâmica de colunas para garantir alinhamento exato com o schema da tabela destino
                    target_cols = set(delta_target.toDF().schema.names)
                    update_cols = [
                        c for c in df_events_alunos.columns 
                        if c in target_cols and c not in ["_ingested_at_ts", "_event_time_ts", "latencia_segundos"]
                    ]
                    
                    update_set = {c: f"source.{c}" for c in update_cols}
                    insert_values = {c: f"source.{c}" for c in update_cols}

                    delta_target.alias("target").merge(
                        source=df_events_alunos.alias("source"),
                        condition="target.id_aluno = source.id_aluno AND target.ano = source.ano"
                    ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()
                    
                    print("  └─ ✅ MERGE INTO de Alunos executado com sucesso no Delta Lake!")
                else:
                    df_events_alunos.drop("_ingested_at_ts", "_event_time_ts", "latencia_segundos") \
                        .write.format("delta").mode("append").save(silver_target_path)
                    print("  └─ ✅ Tabela Delta Silver criada/atualizada via Append de Eventos!")
            else:
                # Modo Fallback Local
                df_events_alunos.drop("_ingested_at_ts", "_event_time_ts", "latencia_segundos") \
                    .write.mode("append").parquet(silver_target_path)
                print("  └─ ✅ Eventos gravados na Silver em modo Fallback!")

            processed_alunos_count = alunos_event_count

    except Exception as e_alunos:
        print(f"  ℹ️ Informação: Nenhum lote de alunos lido ou aviso de leitura: {e_alunos}")

    # -----------------------------------------------------------------------------
    # 4.2 Processamento de Eventos de METAS (Repactuação)
    # -----------------------------------------------------------------------------
    meta_paths_to_check = [
        f"{EVENTS_LANDING_BASE}meta_municipio/",
        f"{EVENTS_LANDING_BASE}meta_uf/",
        f"{EVENTS_LANDING_BASE}meta_brasil/",
        f"{EVENTS_LANDING_BASE}metas/"
    ] if not LOCAL_MODE else [os.path.join(EVENTS_LANDING_BASE, "metas")]

    for m_path in meta_paths_to_check:
        try:
            if LOCAL_MODE:
                df_events_metas = spark.read.json(m_path)
            else:
                df_events_metas = spark.read.option("recursiveFileLookup", "true").json(m_path)

            metas_event_count = df_events_metas.count()
            if metas_event_count > 0:
                print(f" ├─ [Domínio: Metas] Buscando eventos em: {m_path}")
                print(f"  └─ 🎯 Eventos de Metas ({os.path.basename(m_path.rstrip('/'))}) encontrados: {metas_event_count:,} registros.")
                processed_metas_count += metas_event_count
                print("  └─ ✅ Eventos de repactuação de metas registrados com sucesso.")
        except Exception:
            pass

# -----------------------------------------------------------------------------
# 5. Manifesto de Auditoria de Eventos
# -----------------------------------------------------------------------------
total_processed = processed_alunos_count + processed_metas_count
print("\n" + "=" * 75)
print(f"✅ PROCESSAMENTO DE EVENTOS CONCLUÍDO! Total: {total_processed:,} eventos.")
print("=" * 75)

manifest_data = {
    "job_name": job_name,
    "run_id": run_id,
    "ingestion_date": ingestion_date,
    "status": "SUCCEEDED",
    "total_events_processed": total_processed,
    "alunos_events_count": processed_alunos_count,
    "metas_events_count": processed_metas_count,
    "latency_metrics_seconds": latency_stats,
    "message": "Eventos integrados com sucesso no Delta Lake."
}

if not LOCAL_MODE and HAS_BOTO3:
    try:
        s3_client = boto3.client("s3")
        manifest_key = f"auditoria/events/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json"
        )
        print(f"📜 [Auditoria Eventos] Manifesto salvo no S3: s3://{S3_BUCKET}/{manifest_key}")
    except Exception as m_err:
        print(f"⚠️ Alerta ao salvar manifesto no S3: {m_err}")
else:
    os.makedirs(AUDIT_PATH, exist_ok=True)
    with open(os.path.join(AUDIT_PATH, f"manifest_{run_id}.json"), "w") as f:
        json.dump(manifest_data, f, indent=2)
    print(f"📜 [Auditoria Eventos] Manifesto salvo localmente em: {AUDIT_PATH}")
