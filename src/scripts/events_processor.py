"""
===============================================================================
JOB AWS GLUE / PYSPARK - PROCESSADOR DE EVENTOS STREAMING (src/scripts/events_processor.py)
===============================================================================
Descrição: Consumo de eventos em tempo quase real (Landing Zone NDJSON) via
Spark Structured Streaming com checkpointLocation e trigger(availableNow=True).
Métricas de latência real de trânsito (ingested_at - event_time), tratamento
multi-domínio (alunos e metas), isolamento da origem (_source) e Upsert
idempotente via Delta Lake (MERGE INTO) com verificação prévia de dados.
===============================================================================
"""

import sys
import os
import json
import re
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
    from pyspark.sql.window import Window

    HAS_PYSPARK = True

    # Contrato explícito dos eventos de alunos (NDJSON).
    # Campos obrigatórios são validados no foreachBatch.
    EVENTOS_ALUNOS_SCHEMA = StructType(
        [
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
        ]
    )

    EVENTOS_METAS_SCHEMA = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("ano", IntegerType(), True),
            StructField("rede", StringType(), True),
            StructField("taxa_alfabetizacao_pactuada", DoubleType(), True),
            StructField("meta_alfabetizacao_2024", DoubleType(), True),
            StructField("meta_alfabetizacao_2025", DoubleType(), True),
            StructField("meta_alfabetizacao_2026", DoubleType(), True),
            StructField("meta_alfabetizacao_2027", DoubleType(), True),
            StructField("meta_alfabetizacao_2028", DoubleType(), True),
            StructField("meta_alfabetizacao_2029", DoubleType(), True),
            StructField("meta_alfabetizacao_2030", DoubleType(), True),
            StructField("percentual_participacao", DoubleType(), True),
            StructField("_source", StringType(), True),
            StructField("_ingested_at", StringType(), True),
            StructField("id_municipio", StringType(), True),
            StructField("sigla_uf", StringType(), True),
            StructField("pais", StringType(), True),
        ]
    )
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

# 1. Resolve o nome do job sem iniciar o contexto pesado do Spark.
if HAS_GLUE:
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        job_name = args["JOB_NAME"]
    except Exception:
        job_name = "04_events_processor"
else:
    job_name = "04_events_processor"

# 2. Resolução do Run ID e Variáveis de Ambiente
run_id = os.getenv("GLUE_RUN_ID", os.getenv("JOB_RUN_ID", None))
if not run_id:
    for i, arg in enumerate(sys.argv):
        if arg in ["--JOB_RUN_ID", "--job-run-id"] and i + 1 < len(sys.argv):
            run_id = sys.argv[i + 1]
            break
if not run_id:
    run_id = (
        f"evt_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}"
    )
safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)

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

try:
    quarantine_options = (
        getResolvedOptions(sys.argv, ["QUARANTINE_RETENTION_DAYS"])
        if HAS_GLUE
        else {}
    )
except Exception:
    quarantine_options = {}
try:
    QUARANTINE_RETENTION_DAYS = int(
        quarantine_options.get(
            "QUARANTINE_RETENTION_DAYS",
            os.getenv("QUARANTINE_RETENTION_DAYS", "10"),
        )
    )
except ValueError as error:
    raise ValueError("QUARANTINE_RETENTION_DAYS deve ser um inteiro positivo.") from error
if QUARANTINE_RETENTION_DAYS < 1:
    raise ValueError("QUARANTINE_RETENTION_DAYS deve ser um inteiro positivo.")

EVENTS_SILVER_BASE = f"{SILVER_PATH}events/runs/{safe_run_id}/"
QUARANTINE_BASE = f"{SILVER_PATH}quarantine/events/"
QUARANTINE_CONTRACT_VERSION = "events-v2"

print("=" * 75)
print(" INICIANDO PROCESSADOR DE EVENTOS STREAMING (EVENTS PROCESSOR)")
print(f"   Bucket / Target: {S3_BUCKET}")
print(f"   Run ID          : {run_id} | Data: {ingestion_date}")
print(f"   Modo de Operação: {'LOCAL' if LOCAL_MODE else 'AWS GLUE'}")
print("   Engine          : Spark Structured Streaming (checkpointLocation)")
print("=" * 75)


def check_s3_events_exist(bucket_name, prefix):
    """Retorna se há arquivos NDJSON no prefixo, propagando erros de acesso ao S3."""
    if not HAS_BOTO3:
        raise RuntimeError("boto3 é necessário para verificar eventos no S3.")

    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if any(obj["Key"].endswith(".json") for obj in page.get("Contents", [])):
            return True
    return False


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


# 3. Pré-checagem de Eventos (verificação prévia de dados)
events_exist = False
if LOCAL_MODE:
    events_exist = check_local_events_exist(EVENTS_LANDING_BASE)
else:
    prefix_check = "bronze/events_raw_landing/"
    events_exist = check_s3_events_exist(S3_BUCKET, prefix_check)

if not events_exist:
    print("\n" + "=" * 75)
    print(
        " [SEM DADOS NOVOS] Nenhum novo evento localizado no Landing Zone (Status SKIPPED)."
    )
    print("Processamento de eventos ignorado: não há arquivos novos na landing zone.")
    print("=" * 75)

    # Grava manifesto de auditoria SKIPPED
    manifest_data = {
        "job_name": job_name,
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SKIPPED (No New Events)",
        "processed_events_count": 0,
        "message": "Nenhum arquivo NDJSON localizado no buffer de landing zone.",
    }

    if not LOCAL_MODE and HAS_BOTO3:
        s3_client = boto3.client("s3")
        manifest_key = f"auditoria/events/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json",
        )
        print(
            f" [Auditoria Eventos] Manifesto SKIPPED salvo no S3: s3://{S3_BUCKET}/{manifest_key}"
        )
    else:
        os.makedirs(AUDIT_PATH, exist_ok=True)
        with open(os.path.join(AUDIT_PATH, f"manifest_{run_id}.json"), "w") as f:
            json.dump(manifest_data, f, indent=2)
        print(f" [Auditoria Eventos] Manifesto salvo localmente em: {AUDIT_PATH}")

    sys.exit(0)

# 1. Inicializa Spark somente quando há arquivos para processar.
if HAS_GLUE:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
elif HAS_PYSPARK:
    spark = (
        SparkSession.builder.appName("04_events_processor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
else:
    spark = None

if spark:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
else:
    raise RuntimeError(
        "PySpark/AWS Glue não está disponível para processar os arquivos de eventos."
    )

# 4. Leitura e Processamento dos Micro-lotes via Spark Structured Streaming
print("\n 4. Lendo micro-lotes via Spark Structured Streaming (checkpointLocation)...")

alunos_landing = (
    os.path.join(EVENTS_LANDING_BASE, "alunos")
    if LOCAL_MODE
    else f"{EVENTS_LANDING_BASE}alunos/"
)
checkpoint_alunos = (
    os.path.join(CHECKPOINT_BASE, "alunos")
    if LOCAL_MODE
    else f"{CHECKPOINT_BASE}alunos/"
)

processed_alunos_count = 0
processed_metas_count = 0
processed_metas_by_domain = {"municipio": 0, "uf": 0, "brasil": 0}
quarantined_counts = {"alunos": 0, "municipio": 0, "uf": 0, "brasil": 0}
latency_stats = {"avg_sec": 0.0, "min_sec": 0.0, "max_sec": 0.0}


def _read_events_stream(path, schema):
    reader = spark.readStream.format("text")
    if not LOCAL_MODE:
        reader = reader.option("recursiveFileLookup", "true")
    raw_events = reader.load(path)
    parsed_events = raw_events.withColumn(
        "_parsed_event",
        F.from_json(F.col("value"), schema),
    ).withColumn("_json_invalid", F.col("_parsed_event").isNull())
    return parsed_events.select(
        F.col("_parsed_event.*"),
        F.col("value").alias("_raw_payload"),
        F.col("_json_invalid"),
    )


def _quarantine_invalid_events(
    dataframe,
    invalid_condition,
    validation_rules,
    domain,
    batch_id,
):
    total_rows = dataframe.count()
    if total_rows == 0:
        return dataframe.drop("_raw_payload", "_json_invalid"), 0

    invalid_events = dataframe.filter(invalid_condition)
    invalid_count = invalid_events.count()
    if not invalid_count:
        return dataframe.drop("_raw_payload"), 0
    if "_raw_payload" not in dataframe.columns:
        raise ValueError("O fluxo de eventos não preservou o payload original.")

    reason = F.concat_ws(
        "; ",
        *[
            F.when(condition, F.lit(field_name))
            for field_name, condition in validation_rules
        ],
    )
    quarantine = invalid_events.select(
        F.col("_raw_payload").alias("raw_payload"),
        F.col("event_id")
        if "event_id" in dataframe.columns
        else F.lit(None).alias("event_id"),
        F.col("_source")
        if "_source" in dataframe.columns
        else F.lit(None).alias("_source"),
        F.lit(domain).alias("domain"),
        reason.alias("quarantine_reason"),
        F.lit(safe_run_id).alias("run_id"),
        F.lit(int(batch_id)).alias("source_batch_id"),
        F.lit(QUARANTINE_CONTRACT_VERSION).alias("contract_version"),
        F.current_timestamp().alias("quarantined_at"),
    )
    quarantine_path = (
        f"{QUARANTINE_BASE}run_id={safe_run_id}/"
        f"domain={domain}/batch_id={batch_id}/"
    )
    quarantine.write.mode("overwrite").parquet(quarantine_path)
    quarantined_counts[domain] += invalid_count
    print(
        f"  └─  Quarentena ({domain}, micro-lote {batch_id}): "
        f"{invalid_count:,} eventos em {quarantine_path}"
    )

    valid_events = dataframe.filter(~invalid_condition).drop(
        "_raw_payload",
        "_json_invalid",
    )
    return valid_events, invalid_count


def process_alunos_batch(df_batch, batch_id):
    """Callback para cada micro-lote de alunos consumido via Structured Streaming."""
    global processed_alunos_count

    # -------------------------------------------------------------------------
    # VALIDAÇÃO DE INTEGRIDADE DO CONTRATO
    # -------------------------------------------------------------------------

    print("\n  └─  Verificando a integridade do contrato json\n")

    validation_rules = [
        ("event_id", F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "")),
        (
            "event_time",
            F.col("event_time").isNull()
            | (F.trim(F.col("event_time")) == "")
            | F.to_timestamp(F.col("event_time")).isNull(),
        ),
        ("id_aluno", F.col("id_aluno").isNull() | (F.trim(F.col("id_aluno")) == "")),
        (
            "id_municipio",
            F.col("id_municipio").isNull()
            | (F.trim(F.col("id_municipio")) == ""),
        ),
        (
            "id_escola",
            F.col("id_escola").isNull() | (F.trim(F.col("id_escola")) == ""),
        ),
        ("ano", F.col("ano").isNull()),
        ("serie", F.col("serie").isNull() | (F.trim(F.col("serie")) == "")),
        ("rede", F.col("rede").isNull() | (~F.col("rede").isin("0", "1", "2", "3", "4", "5", "6"))),
        (
            "alfabetizado",
            F.col("alfabetizado").isNull()
            | (~F.col("alfabetizado").isin("0", "1")),
        ),
        ("proficiencia", F.col("proficiencia").isNull() | F.isnan("proficiencia")),
        ("peso_aluno", F.col("peso_aluno").isNull() | (F.col("peso_aluno") <= 0)),
        (
            "_source",
            F.col("_source").isNull()
            | (F.col("_source") != F.lit("streaming_live_simulation")),
        ),
        ("json", F.col("_json_invalid")),
    ]
    invalid_condition = F.lit(False)
    for _, condition in validation_rules:
        invalid_condition = invalid_condition | condition
    df_batch, quarantined = _quarantine_invalid_events(
        df_batch, invalid_condition, validation_rules, "alunos", batch_id
    )
    batch_count = df_batch.count()
    if batch_count == 0:
        print(
            f"  └─  Micro-lote {batch_id} sem eventos válidos "
            f"({quarantined:,} em quarentena)."
        )
        return

    print(
        f"\n  └─  Micro-lote {batch_id} (Alunos): "
        f"{batch_count:,} registros inéditos encontrados."
    )

    event_window = Window.partitionBy("id_aluno", "ano").orderBy(
        F.col("event_time").desc(),
        F.col("event_id").desc(),
    )
    df_batch = (
        df_batch.withColumn("_event_rank", F.row_number().over(event_window))
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
    )

    # Define o timestamp de ingestão em UTC.
    df_batch = df_batch.withColumn("_ingested_at_ts", F.current_timestamp()).withColumn(
        "_event_time_ts", F.to_timestamp(F.col("event_time"))
    )

    # Cálculo da Latência Real de Trânsito (ingested_at - event_time)
    df_batch = df_batch.withColumn(
        "latencia_segundos",
        F.round(
            F.col("_ingested_at_ts").cast(DoubleType())
            - F.col("_event_time_ts").cast(DoubleType()),
            3,
        ),
    )

    # Métricas estatísticas da latência
    lat_metrics = df_batch.select(
        F.avg("latencia_segundos").alias("avg_lat"),
        F.min("latencia_segundos").alias("min_lat"),
        F.max("latencia_segundos").alias("max_lat"),
    ).collect()[0]

    latency_stats["avg_sec"] = round(float(lat_metrics["avg_lat"] or 0.0), 3)
    latency_stats["min_sec"] = round(float(lat_metrics["min_lat"] or 0.0), 3)
    latency_stats["max_sec"] = round(float(lat_metrics["max_lat"] or 0.0), 3)

    print("  └─  Latência Real Medida (ingested_at - event_time):")
    print(f"     ├─ Média : {latency_stats['avg_sec']} segundos")
    print(f"     ├─ Mínima: {latency_stats['min_sec']} segundos")
    print(f"     └─ Máxima: {latency_stats['max_sec']} segundos")

    # Derivação da Sigla UF via IBGE se não estiver presente
    if "sigla_uf" not in df_batch.columns and "id_municipio" in df_batch.columns:
        ibge_uf_map = F.create_map(
            [
                F.lit("11"),
                F.lit("RO"),
                F.lit("12"),
                F.lit("AC"),
                F.lit("13"),
                F.lit("AM"),
                F.lit("14"),
                F.lit("RR"),
                F.lit("15"),
                F.lit("PA"),
                F.lit("16"),
                F.lit("AP"),
                F.lit("17"),
                F.lit("TO"),
                F.lit("21"),
                F.lit("MA"),
                F.lit("22"),
                F.lit("PI"),
                F.lit("23"),
                F.lit("CE"),
                F.lit("24"),
                F.lit("RN"),
                F.lit("25"),
                F.lit("PB"),
                F.lit("26"),
                F.lit("PE"),
                F.lit("27"),
                F.lit("AL"),
                F.lit("28"),
                F.lit("SE"),
                F.lit("29"),
                F.lit("BA"),
                F.lit("31"),
                F.lit("MG"),
                F.lit("32"),
                F.lit("ES"),
                F.lit("33"),
                F.lit("RJ"),
                F.lit("35"),
                F.lit("SP"),
                F.lit("41"),
                F.lit("PR"),
                F.lit("42"),
                F.lit("SC"),
                F.lit("43"),
                F.lit("RS"),
                F.lit("50"),
                F.lit("MS"),
                F.lit("51"),
                F.lit("MT"),
                F.lit("52"),
                F.lit("GO"),
                F.lit("53"),
                F.lit("DF"),
            ]
        )
        df_batch = df_batch.withColumn(
            "sigla_uf", ibge_uf_map[F.substring(F.col("id_municipio"), 1, 2)]
        )

    # Adicionar Colunas de Partição e Controle do Ingestion
    if "_ingested_at" not in df_batch.columns:
        if "event_time" in df_batch.columns:
            df_batch = df_batch.withColumn("_ingested_at", F.col("event_time"))
        else:
            df_batch = df_batch.withColumn(
                "_ingested_at",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            )
    else:
        df_batch = df_batch.withColumn(
            "_ingested_at", F.coalesce(F.col("_ingested_at"), F.col("event_time"))
        )

    df_batch = (
        df_batch.withColumn("_processed_silver_at", F.current_timestamp())
        .withColumn("ref_ano", F.col("ano").cast("int"))
        .withColumn("ref_mes", F.month(F.col("_event_time_ts")))
        .withColumn("ref_dia", F.dayofmonth(F.col("_event_time_ts")))
        .fillna({"ref_mes": 11, "ref_dia": 1})
    )

    # Operação de MERGE INTO (Upsert) no Delta Lake Silver
    event_output_path = (
        f"{EVENTS_SILVER_BASE}alunos/batch_id={batch_id}/"
    )
    df_batch.drop(
        "_ingested_at_ts", "_event_time_ts", "latencia_segundos"
    ).write.mode("overwrite").parquet(event_output_path)
    print(f" Eventos sintéticos gravados em destino isolado: {event_output_path}")

    processed_alunos_count += batch_count


def process_metas_batch(df_batch, batch_id, domain):
    """Valida e aplica o micro-lote no destino Silver do domínio correspondente."""
    global processed_metas_count

    if domain not in ("municipio", "uf", "brasil"):
        raise ValueError(f"Domínio de meta não suportado: {domain}")

    if domain == "municipio":
        territory_key = "id_municipio"
        expected_rede = "Municipal"
    elif domain == "uf":
        territory_key = "sigla_uf"
        expected_rede = "Pública"
    else:
        territory_key = "pais"
        expected_rede = "Pública"

    validation_rules = [
        ("event_id", F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "")),
        (
            "event_type",
            F.col("event_type").isNull()
            | (F.col("event_type") != F.lit("META_UPDATE")),
        ),
        ("domain", F.col("domain").isNull() | (F.col("domain") != F.lit(domain))),
        (
            "event_time",
            F.col("event_time").isNull()
            | (F.trim(F.col("event_time")) == "")
            | F.to_timestamp(F.col("event_time")).isNull(),
        ),
        ("ano", F.col("ano").isNull()),
        ("rede", F.col("rede").isNull() | (F.col("rede") != F.lit(expected_rede))),
        (
            "meta_alfabetizacao_2030",
            F.col("meta_alfabetizacao_2030").isNull()
            | (F.col("meta_alfabetizacao_2030") < 0)
            | (F.col("meta_alfabetizacao_2030") > 100),
        ),
        (
            territory_key,
            F.col(territory_key).isNull()
            | (F.trim(F.col(territory_key)) == ""),
        ),
        (
            "_source",
            F.col("_source").isNull()
            | (F.col("_source") != F.lit("streaming_live_simulation")),
        ),
        ("json", F.col("_json_invalid")),
    ]
    for year in range(2024, 2031):
        column = f"meta_alfabetizacao_{year}"
        validation_rules.append(
            (
                column,
                F.col(column).isNotNull()
                & ((F.col(column) < 0) | (F.col(column) > 100)),
            )
        )
    invalid_condition = F.lit(False)
    for _, condition in validation_rules:
        invalid_condition = invalid_condition | condition
    df_batch, quarantined = _quarantine_invalid_events(
        df_batch, invalid_condition, validation_rules, domain, batch_id
    )
    batch_count = df_batch.count()
    if batch_count == 0:
        print(
            f"  └─  Micro-lote de metas {domain}/{batch_id} sem eventos válidos "
            f"({quarantined:,} em quarentena)."
        )
        return

    print(
        f"  └─  Eventos de metas ({domain}, micro-lote {batch_id}): "
        f"{batch_count:,} registros encontrados."
    )

    business_keys = [territory_key, "ano", "rede"]
    event_window = Window.partitionBy(*business_keys).orderBy(
        F.col("event_time").desc(),
        F.col("event_id").desc(),
    )
    df_batch = (
        df_batch.withColumn("_event_rank", F.row_number().over(event_window))
        .filter(F.col("_event_rank") == 1)
        .drop("_event_rank")
        .withColumn(
            "_ingested_at", F.coalesce(F.col("_ingested_at"), F.col("event_time"))
        )
        .withColumn("_processed_silver_at", F.current_timestamp())
    )

    meta_output_path = (
        f"{EVENTS_SILVER_BASE}metas/{domain}/batch_id={batch_id}/"
    )
    df_batch.write.mode("overwrite").parquet(meta_output_path)
    print(f"  └─  Evento de meta gravado em destino isolado: {meta_output_path}")

    processed_metas_count += batch_count
    processed_metas_by_domain[domain] += batch_count


def process_student_stream():
    """Consome o lote disponível de eventos de alunos."""
    if check_events_path_exists(alunos_landing):
        try:
            print(
                f" ├─ [Domínio: Alunos] Streaming com schema explícito e checkpointLocation em: {checkpoint_alunos}"
            )
            df_stream_alunos = _read_events_stream(
                alunos_landing,
                EVENTOS_ALUNOS_SCHEMA,
            )

            query_alunos = (
                df_stream_alunos.writeStream.format("delta" if HAS_GLUE else "memory")
                .option("checkpointLocation", checkpoint_alunos)
                .trigger(availableNow=True)
                .foreachBatch(process_alunos_batch)
                .start()
            )

            query_alunos.awaitTermination()
        except Exception as e_alunos:
            print(f"   Falha no processamento streaming de alunos: {e_alunos}")
            raise
    else:
        print(" ├─ [Domínio: Alunos] Sem arquivos JSON; processamento ignorado.")


def _meta_landing_paths():
    """Retorna os diretórios de landing de eventos de metas por domínio."""
    domains = ("municipio", "uf", "brasil")
    return [
        (
            domain,
            (
                f"{EVENTS_LANDING_BASE}meta_{domain}/"
                if not LOCAL_MODE
                else os.path.join(EVENTS_LANDING_BASE, f"meta_{domain}")
            ),
        )
        for domain in domains
    ]


def process_meta_streams():
    """Consome os lotes disponíveis de eventos de metas por domínio."""
    for domain, meta_path in _meta_landing_paths():
        if not check_events_path_exists(meta_path):
            print(f"  └─  Caminho de metas sem arquivos JSON; ignorando: {meta_path}")
            continue

        print(f"  └─  Processando eventos encontrados em: {meta_path}")
        try:
            df_stream_metas = _read_events_stream(
                meta_path,
                EVENTOS_METAS_SCHEMA,
            )
            checkpoint_meta_path = (
                os.path.join(CHECKPOINT_BASE, f"metas_{domain}")
                if LOCAL_MODE
                else f"{CHECKPOINT_BASE}metas_{domain}/"
            )

            q_metas = (
                df_stream_metas.writeStream.option(
                    "checkpointLocation", checkpoint_meta_path
                )
                .trigger(availableNow=True)
                .foreachBatch(
                    lambda df_batch, batch_id: process_metas_batch(
                        df_batch, batch_id, domain
                    )
                )
                .start()
            )

            q_metas.awaitTermination()
        except Exception as e_metas:
            print(
                f"   Falha no processamento do caminho de metas "
                f"'{meta_path}': {e_metas}"
            )
            raise


if HAS_PYSPARK and spark:
    # -----------------------------------------------------------------------------
    # 4.1 Processamento de Eventos de ALUNOS e METAS (Structured Streaming)
    # -----------------------------------------------------------------------------
    process_student_stream()
    process_meta_streams()

# -----------------------------------------------------------------------------
# 5. Manifesto de Auditoria de Eventos
# -----------------------------------------------------------------------------
total_processed = processed_alunos_count + processed_metas_count
print("\n" + "=" * 75)
print(
    f" PROCESSAMENTO DE EVENTOS DE ALUNOS: "
    f"{processed_alunos_count:,} eventos inéditos."
)
print(
    f" PROCESSAMENTO DE EVENTOS DE METAS: "
    f"{processed_metas_count:,} eventos inéditos."
)
print(f"Processamento de eventos concluído. Eventos processados: {total_processed:,}.")
print("=" * 75)

manifest_data = {
    "job_name": job_name,
    "run_id": run_id,
    "ingestion_date": ingestion_date,
    "status": "SUCCEEDED",
    "total_events_processed": total_processed,
    "alunos_events_count": processed_alunos_count,
    "metas_events_count": processed_metas_count,
    "metas_events_by_domain": processed_metas_by_domain,
    "total_events_quarantined": sum(quarantined_counts.values()),
    "quarantined_events_by_domain": quarantined_counts,
    "quarantine_s3_prefix": (
        f"s3://{S3_BUCKET}/{QUARANTINE_BASE.removeprefix(f's3://{S3_BUCKET}/')}"
        if not LOCAL_MODE
        else None
    ),
    "quarantine_retention_days": QUARANTINE_RETENTION_DAYS,
    "events_silver_run_prefix": EVENTS_SILVER_BASE,
    "publication_status": "COMMITTED",
    "quarantine_status": (
        "REVIEW_REQUIRED" if sum(quarantined_counts.values()) else "EMPTY"
    ),
    "latency_metrics_seconds": latency_stats,
    "streaming_engine": "Spark Structured Streaming (Trigger AvailableNow + checkpointLocation)",
    "message": (
        "Eventos sintéticos válidos foram gravados em destino isolado; "
        "payloads inválidos foram preservados para reprocessamento."
    ),
}

if not LOCAL_MODE and HAS_BOTO3:
    s3_client = boto3.client("s3")
    manifest_key = f"auditoria/events/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest_data, indent=2),
        ContentType="application/json",
    )
    print(
        f" [Auditoria Eventos] Manifesto salvo no S3: s3://{S3_BUCKET}/{manifest_key}"
    )

else:
    os.makedirs(AUDIT_PATH, exist_ok=True)
    with open(os.path.join(AUDIT_PATH, f"manifest_{run_id}.json"), "w") as f:
        json.dump(manifest_data, f, indent=2)
    print(f" [Auditoria Eventos] Manifesto salvo localmente em: {AUDIT_PATH}")
