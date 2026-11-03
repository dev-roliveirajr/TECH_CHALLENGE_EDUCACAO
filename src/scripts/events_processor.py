"""
===============================================================================
JOB AWS GLUE / PYSPARK - PROCESSADOR DE EVENTOS STREAMING (src/scripts/events_processor.py)
===============================================================================
Descrição: Consumo de eventos em tempo quase real (Landing Zone NDJSON) via
Spark Structured Streaming com checkpointLocation e trigger(availableNow=True).
Métricas de latência real de trânsito (ingested_at - event_time), tratamento
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
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    HAS_PYSPARK = True

    # Contrato explícito dos eventos de alunos (NDJSON).
    # Campos obrigatórios são validados no foreachBatch.
    EVENTOS_ALUNOS_SCHEMA = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("id_aluno", StringType(), True),
        StructField("id_municipio", StringType(), True),
        StructField("id_escola", StringType(), True),
        StructField("ano", IntegerType(), True),
        StructField("serie", StringType(), True),
        StructField("rede", StringType(), True),
        StructField("caderno", StringType(), True),
        StructField("presenca", StringType(), True),
        StructField("preenchimento_caderno", StringType(), True),
        StructField("alfabetizado", StringType(), True),
        StructField("proficiencia", DoubleType(), True),
        StructField("peso_aluno", DoubleType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ])
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
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    job_name = "04_events_processor"
else:
    spark = None
    job_name = "04_events_processor"

if spark:
    try:
        spark.conf.set("spark.sql.streaming.schemaInference", "true")
    except Exception:
        pass

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
    CHECKPOINT_BASE = "data/checkpoints/events/"
else:
    EVENTS_LANDING_BASE = f"s3://{S3_BUCKET}/bronze/events_raw_landing/"
    SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
    AUDIT_PATH = f"s3://{S3_BUCKET}/auditoria/events/"
    CHECKPOINT_BASE = f"s3://{S3_BUCKET}/checkpoints/events/"

print("=" * 75)
print("🚀 INICIANDO PROCESSADOR DE EVENTOS STREAMING (EVENTS PROCESSOR)")
print(f"   Bucket / Target: {S3_BUCKET}")
print(f"   Run ID          : {run_id} | Data: {ingestion_date}")
print(f"   Modo de Operação: {'LOCAL' if LOCAL_MODE else 'AWS GLUE'}")
print(f"   Engine          : Spark Structured Streaming (checkpointLocation)")
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


def check_events_path_exists(path):
    """Verifica se um caminho contém arquivos JSON antes de iniciar o streaming."""
    if LOCAL_MODE:
        return check_local_events_exist(path)

    prefix = path.replace(f"s3://{S3_BUCKET}/", "", 1)
    return check_s3_events_exist(S3_BUCKET, prefix)


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

# 4. Leitura e Processamento dos Micro-lotes via Spark Structured Streaming
print("\n📥 4. Lendo micro-lotes via Spark Structured Streaming (checkpointLocation)...")

alunos_landing = os.path.join(EVENTS_LANDING_BASE, "alunos") if LOCAL_MODE else f"{EVENTS_LANDING_BASE}alunos/"
checkpoint_alunos = os.path.join(CHECKPOINT_BASE, "alunos") if LOCAL_MODE else f"{CHECKPOINT_BASE}alunos/"
checkpoint_metas_base = os.path.join(CHECKPOINT_BASE, "metas") if LOCAL_MODE else f"{CHECKPOINT_BASE}metas/"

processed_alunos_count = 0
processed_metas_count = 0
latency_stats = {"avg_sec": 0.0, "min_sec": 0.0, "max_sec": 0.0}


def process_alunos_batch(df_batch, batch_id):
    """Callback para cada micro-lote de alunos consumido via Structured Streaming."""
    global processed_alunos_count, latency_stats

    # -------------------------------------------------------------------------
    # VALIDAÇÃO DE INTEGRIDADE DO CONTRATO
    # -------------------------------------------------------------------------

    print(f"\n  └─ ℹ️ Verificando a integridade do contrato json\n")

    try:
        invalid_condition = (
            F.col("event_id").isNull()
            | (F.trim(F.col("event_id")) == "")
            | F.col("id_aluno").isNull()
            | (F.trim(F.col("id_aluno")) == "")
            | F.col("id_municipio").isNull()
            | (F.trim(F.col("id_municipio")) == "")
            | F.col("id_escola").isNull()
            | (F.trim(F.col("id_escola")) == "")
            | F.col("ano").isNull()
        )

        invalid_records = (
            df_batch
            .filter(invalid_condition)
            .limit(1)
            .count()
        )

        if invalid_records > 0:
            raise ValueError(
                "Registro inválido: existem campos obrigatórios "
                "ausentes ou vazios no evento."
            )

        print("  └─ ✅ Estrutura contém os campos obrigatórios.\n")

    except Exception as e:
        raise ValueError(
            f"❌ Falha na validação do contrato JSON: {e}"
        ) from e

    # -------------------------------------------------------------------------

    batch_count = df_batch.count()
    if batch_count == 0:
        print(f"  └─ ℹ️ Micro-lote {batch_id} vazio; nenhum registro para processar.")
        return

    print(
        f"\n  └─ 📡 Micro-lote {batch_id} (Alunos): "
        f"{batch_count:,} registros inéditos encontrados."
    )
    
    # Garantir Timestamp UTC no Ingestion
    df_batch = df_batch.withColumn(
        "_ingested_at_ts", F.current_timestamp()
    ).withColumn(
        "_event_time_ts", F.to_timestamp(F.col("event_time"))
    )

    # Cálculo da Latência Real de Trânsito (ingested_at - event_time)
    df_batch = df_batch.withColumn(
        "latencia_segundos",
        F.round(
            F.col("_ingested_at_ts").cast(DoubleType()) - F.col("_event_time_ts").cast(DoubleType()),
            3
        )
    )

    # Métricas estatísticas da latência
    lat_metrics = df_batch.select(
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
    if "sigla_uf" not in df_batch.columns and "id_municipio" in df_batch.columns:
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
        df_batch = df_batch.withColumn(
            "sigla_uf", ibge_uf_map[F.substring(F.col("id_municipio"), 1, 2)]
        )

    # Adicionar Colunas de Partição e Controle do Ingestion
    if "_ingested_at" not in df_batch.columns:
        if "event_time" in df_batch.columns:
            df_batch = df_batch.withColumn("_ingested_at", F.col("event_time"))
        else:
            df_batch = df_batch.withColumn("_ingested_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    else:
        df_batch = df_batch.withColumn("_ingested_at", F.coalesce(F.col("_ingested_at"), F.col("event_time")))

    df_batch = df_batch \
        .withColumn("_processed_silver_at", F.current_timestamp()) \
        .withColumn("ref_ano", F.col("ano").cast("int")) \
        .withColumn("ref_mes", F.month(F.col("_event_time_ts"))) \
        .withColumn("ref_dia", F.dayofmonth(F.col("_event_time_ts"))) \
        .fillna({"ref_mes": 11, "ref_dia": 1})

    # Operação de MERGE INTO (Upsert) no Delta Lake Silver
    silver_target_path = f"{SILVER_PATH}alunos_curado/"
    print(f"🔄 Executando MERGE INTO no Delta Lake Silver: {silver_target_path}")

    if HAS_GLUE:
        is_delta = DeltaTable.isDeltaTable(spark, silver_target_path)

        if is_delta:
            delta_target = DeltaTable.forPath(spark, silver_target_path)
            target_cols = set(delta_target.toDF().schema.names)
            update_cols = [
                c for c in df_batch.columns
                if c in target_cols and c not in ["_ingested_at_ts", "_event_time_ts", "latencia_segundos"]
            ]

            update_set = {c: f"source.{c}" for c in update_cols}
            insert_values = {c: f"source.{c}" for c in update_cols}

            delta_target.alias("target").merge(
                source=df_batch.alias("source"),
                condition="target.id_aluno = source.id_aluno AND target.ano = source.ano"
            ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()

            print("  └─ ✅ MERGE INTO de Alunos executado com sucesso no Delta Lake!")
        else:
            df_batch.drop("_ingested_at_ts", "_event_time_ts", "latencia_segundos") \
                .write.format("delta").mode("append").save(silver_target_path)
            print("  └─ ✅ Tabela Delta Silver criada/atualizada via Append de Eventos!")
    else:
        # Modo Fallback Local
        df_batch.drop("_ingested_at_ts", "_event_time_ts", "latencia_segundos") \
            .write.mode("append").parquet(silver_target_path)
        print("  └─ ✅ Eventos gravados na Silver em modo Fallback!")

    processed_alunos_count += batch_count


def process_metas_batch(df_batch, batch_id):
    """Callback para cada micro-lote de repactuação de metas."""
    global processed_metas_count
    batch_count = df_batch.count()
    if batch_count == 0:
        return

    print(f"  └─ 🎯 Eventos de Metas (Micro-lote {batch_id}): {batch_count:,} registros encontrados.")

    meta_target_path = f"{SILVER_PATH}meta_alfabetizacao_municipio/"
    print(f"  └─ 🔄 Persistindo eventos de metas na Camada Silver: {meta_target_path}")

    if HAS_GLUE:
        try:
            is_delta = DeltaTable.isDeltaTable(spark, meta_target_path)
        except Exception:
            is_delta = False

        if is_delta:
            delta_target = DeltaTable.forPath(spark, meta_target_path)
            cond = "target.id_municipio = source.id_municipio AND target.ano = source.ano"
            if "rede" in df_batch.columns:
                cond += " AND target.rede = source.rede"

            delta_target.alias("target").merge(
                source=df_batch.alias("source"),
                condition=cond
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print("  └─ ✅ MERGE INTO de Metas executado com sucesso no Delta Lake Silver!")
        else:
            df_batch.write.format("delta").mode("append").save(meta_target_path)
            print("  └─ ✅ Eventos de Metas salvos via Append na Silver!")
    else:
        df_batch.write.mode("append").parquet(meta_target_path)
        print("  └─ ✅ Eventos de Metas gravados na Silver em modo Fallback!")

    processed_metas_count += batch_count


if HAS_PYSPARK and spark:
    # -----------------------------------------------------------------------------
    # 4.1 Processamento de Eventos de ALUNOS (Structured Streaming)
    # -----------------------------------------------------------------------------
    try:
        print(f" ├─ [Domínio: Alunos] Streaming com schema explícito e checkpointLocation em: {checkpoint_alunos}")
        read_stream_alunos = (
            spark.readStream
            .schema(EVENTOS_ALUNOS_SCHEMA)
            .option("mode", "FAILFAST")
        )
        if not LOCAL_MODE:
            read_stream_alunos = read_stream_alunos.option("recursiveFileLookup", "true")

        df_stream_alunos = read_stream_alunos.json(alunos_landing)

        query_alunos = df_stream_alunos.writeStream \
            .format("delta" if HAS_GLUE else "memory") \
            .option("checkpointLocation", checkpoint_alunos) \
            .trigger(availableNow=True) \
            .foreachBatch(process_alunos_batch) \
            .start()

        query_alunos.awaitTermination()
    except Exception as e_alunos:
        print(f"  ❌ Falha no processamento streaming de alunos: {e_alunos}")
        raise

    # -----------------------------------------------------------------------------
    # 4.2 Processamento de Eventos de METAS (Structured Streaming)
    # -----------------------------------------------------------------------------
    meta_paths_to_check = [
        f"{EVENTS_LANDING_BASE}meta_municipio/",
        f"{EVENTS_LANDING_BASE}meta_uf/",
        f"{EVENTS_LANDING_BASE}meta_brasil/",
        f"{EVENTS_LANDING_BASE}metas/"
    ] if not LOCAL_MODE else [os.path.join(EVENTS_LANDING_BASE, "metas")]

    for idx, m_path in enumerate(meta_paths_to_check):
        if not check_events_path_exists(m_path):
            print(
                f"  └─ ℹ️ Caminho de metas sem arquivos JSON; ignorando: {m_path}"
            )
            continue

        print(f"  └─ ℹ️ Processando eventos encontrados em: {m_path}")
        try:
            read_metas = (
                spark.readStream
                .option("spark.sql.streaming.schemaInference", "true")
                .option("mode", "FAILFAST")
            )
            if not LOCAL_MODE:
                read_metas = read_metas.option("recursiveFileLookup", "true")

            df_stream_metas = read_metas.json(m_path)
            checkpoint_meta_path = f"{checkpoint_metas_base}_{idx}"

            q_metas = df_stream_metas.writeStream \
                .option("checkpointLocation", checkpoint_meta_path) \
                .trigger(availableNow=True) \
                .foreachBatch(process_metas_batch) \
                .start()

            q_metas.awaitTermination()
        except Exception as e_metas:
            print(
                f"  ❌ Falha no processamento do caminho de metas "
                f"'{m_path}': {e_metas}"
            )
            raise

# -----------------------------------------------------------------------------
# 5. Manifesto de Auditoria de Eventos
# -----------------------------------------------------------------------------
total_processed = processed_alunos_count + processed_metas_count
print("\n" + "=" * 75)
print(f"✅ PROCESSAMENTO DE EVENTOS DE ALUNOS: {processed_alunos_count:,} eventos inéditos.")
print(f"✅ PROCESSAMENTO DE EVENTOS DE METAS: {processed_metas_count:,} eventos inéditos.")
print(f"✅ PROCESSAMENTO DE EVENTOS CONCLUÍDO! Total: {total_processed:,} eventos inéditos.")
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
    "streaming_engine": "Spark Structured Streaming (Trigger AvailableNow + checkpointLocation)",
    "message": "Eventos integrados com sucesso no Delta Lake com controle de offset por checkpoint."
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

        # Se novos eventos foram integrados à Silver, atualiza o manifesto em auditoria/silver/ para desativar o short-circuit da Gold
        if total_processed > 0:
            silver_manifest_data = {
                "job_name": job_name,
                "run_id": run_id,
                "ingestion_date": ingestion_date,
                "status": "SUCCEEDED",
                "total_events_processed": total_processed,
                "message": "Atualização incremental via Streaming Events integrada com sucesso à Camada Silver."
            }
            silver_manifest_key = f"auditoria/silver/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=silver_manifest_key,
                Body=json.dumps(silver_manifest_data, indent=2),
                ContentType="application/json"
            )
            print(f"📜 [Governança Silver] Manifesto de atualização ativado para a Camada Gold: s3://{S3_BUCKET}/{silver_manifest_key}")

    except Exception as m_err:
        print(f"⚠️ Alerta ao salvar manifesto no S3: {m_err}")
else:
    os.makedirs(AUDIT_PATH, exist_ok=True)
    with open(os.path.join(AUDIT_PATH, f"manifest_{run_id}.json"), "w") as f:
        json.dump(manifest_data, f, indent=2)
    print(f"📜 [Auditoria Eventos] Manifesto salvo localmente em: {AUDIT_PATH}")

    if total_processed > 0:
        silver_audit_dir = "data/auditoria/silver/"
        os.makedirs(silver_audit_dir, exist_ok=True)
        silver_manifest_data = {
            "job_name": job_name,
            "run_id": run_id,
            "ingestion_date": ingestion_date,
            "status": "SUCCEEDED",
            "total_events_processed": total_processed,
            "message": "Atualização incremental via Streaming Events."
        }
        with open(os.path.join(silver_audit_dir, f"manifest_{run_id}.json"), "w") as f:
            json.dump(silver_manifest_data, f, indent=2)
        print(f"📜 [Governança Silver] Manifesto de atualização salvo localmente em: {silver_audit_dir}")


