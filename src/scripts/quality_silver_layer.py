"""
===============================================================================
JOB AWS GLUE - CAMADA SILVER (src/scripts/quality_silver_layer.py)
===============================================================================
Descrição: Processamento, deduplicação por janela (Window Function),
limpeza, enriquecimento relacional, tratamento preservativo de NULOs (Saeb),
JOIN e verificação da taxa de correspondência.
===============================================================================
"""

import sys
import os
import json
import boto3
import datetime
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# Importando a biblioteca de qualidade enviada via --extra-py-files
from quality_lib import (
    check_join_ratio,
    deduplicate_by_window,
)
from manifest_lib import get_latest_bronze_manifest, validate_bronze_manifest

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Parâmetros passados pelo Glue Job
args = getResolvedOptions(sys.argv, ["JOB_NAME", "GLUE_DATABASE_NAME"])
job_name = args["JOB_NAME"]
GLUE_DATABASE_NAME = args["GLUE_DATABASE_NAME"]

# Obtém o JOB_RUN_ID.
run_id = os.getenv("GLUE_RUN_ID", os.getenv("JOB_RUN_ID", None))
if not run_id:
    for i, arg in enumerate(sys.argv):
        if arg in ["--JOB_RUN_ID", "--job-run-id"] and i + 1 < len(sys.argv):
            run_id = sys.argv[i + 1]
            break
if not run_id:
    run_id = (
        f"slv_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )

ingestion_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ["S3_BUCKET_NAME"])
        S3_BUCKET = args_s3["S3_BUCKET_NAME"]
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("A variável/argumento S3_BUCKET_NAME não foi definida.")

JOIN_THRESHOLD = float(os.getenv("JOIN_MATCH_THRESHOLD", "95.0"))
BLOCK_ON_FAILURE = os.getenv("BLOCK_ON_QUALITY_FAILURE", "true").lower() == "true"

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
output_path = f"{SILVER_PATH}alunos_curado/"

print("=" * 75)
print(" Iniciando processamento da Camada Silver.")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Run ID: {run_id} | Data: {ingestion_date}")
print(
    f"   Limiar do JOIN: {JOIN_THRESHOLD}% | Interrupção por falha: {BLOCK_ON_FAILURE}"
)
print("=" * 75)


# 1. Carrega e valida o manifesto Bronze concluído mais recente.
table_names = [
    "alunos",
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]

print(" 1. Carregando o manifesto Bronze mais recente.")
s3_client = boto3.client("s3")
bronze_manifest = get_latest_bronze_manifest(s3_client, S3_BUCKET)
manifest_prefixes, all_bronze_skipped = validate_bronze_manifest(
    bronze_manifest, S3_BUCKET, table_names
)
print(f" Manifesto Bronze validado: " f"{bronze_manifest['manifest_s3_uri']}")

# Ignora o reprocessamento quando a Bronze não mudou e a tabela Delta já existe.
is_delta_existing = DeltaTable.isDeltaTable(spark, output_path)


def _print_table_preview(dataframe, columns):
    print("\n Preview das primeiras linhas:")
    try:
        sample_rows = dataframe.limit(5).collect()
        print("| " + " | ".join(columns) + " |")
        print("| " + " | ".join(["---"] * len(columns)) + " |")
        for row in sample_rows:
            values = [
                str(row[column]) if row[column] is not None else "NULL"
                for column in columns
            ]
            print("| " + " | ".join(values) + " |")
    except Exception as preview_error:
        print(f"    Não foi possível gerar preview: {preview_error}")


def _profile_duplicate_keys(dataframe, valid_keys, total_rows):
    if not valid_keys:
        print("   ├─ Duplicatas por Chave Negocial : N/A (Chaves não presentes)")
        return False

    distinct_keys = dataframe.select(valid_keys).distinct().count()
    key_duplicates = total_rows - distinct_keys
    duplicate_percent = (
        (key_duplicates / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
    )
    print(
        f"   ├─ Duplicatas por Chave Negocial {valid_keys}: "
        f"{key_duplicates:,} ({duplicate_percent:.2f}%)"
    )
    return key_duplicates > 0


def _diagnose_table(name, dataframe, table_keys):
    print("\n" + "=" * 75)
    print(f" Diagnóstico da tabela: '{name.upper()}'")
    print("=" * 75)

    columns = dataframe.columns
    print("\n Schema / Estrutura de Tipos:")
    for field in dataframe.schema.fields:
        print(f"   ├─ {field.name:<40} : {field.dataType.simpleString()}")

    _print_table_preview(dataframe, columns)

    null_count_columns = [
        F.count(F.when(F.col(column).isNull(), F.lit(1))).alias(f"null_count_{index}")
        for index, column in enumerate(columns)
    ]
    profile = dataframe.agg(
        F.count(F.lit(1)).alias("total_rows"),
        *null_count_columns,
    ).first()
    total_rows = profile["total_rows"]
    print(f" Volume Total: {total_rows:,} registros | {len(columns)} colunas")
    print("\n Diagnóstico de NULOs por Coluna:")
    for index, column in enumerate(columns):
        null_count = profile[f"null_count_{index}"]
        null_percent = (
            (null_count / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
        )
        print(f"   ├─ {column:<40} : {null_count:,} nulos ({null_percent:.2f}%)")

    print("\n Diagnóstico de Duplicidades:")
    distinct_rows = dataframe.distinct().count()
    exact_duplicates = total_rows - distinct_rows
    exact_duplicate_percent = (
        (exact_duplicates / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
    )
    print(
        f"   ├─ Duplicatas Exatas (Linha Inteira) : "
        f"{exact_duplicates:,} ({exact_duplicate_percent:.2f}%)"
    )
    valid_keys = [key for key in table_keys.get(name, []) if key in columns]
    has_key_duplicates = _profile_duplicate_keys(dataframe, valid_keys, total_rows)
    print("=" * 75)
    return valid_keys if has_key_duplicates else None


def _diagnose_tables(tables, table_keys):
    print("\n" + "#" * 75)
    print(" Análise exploratória das entidades de entrada")
    print("#" * 75 + "\n")
    duplicate_keys = {}
    for name, dataframe in tables.items():
        keys = _diagnose_table(name, dataframe, table_keys)
        if keys:
            duplicate_keys[name] = keys
    return duplicate_keys


def _load_bronze_tables():
    tables = {}
    for name in table_names:
        try:
            base_path = f"{BRONZE_PATH}{name}/"
            target_path = manifest_prefixes[name]
            print(f" ├─ Lendo [{name}] de {target_path}")
            tables[name] = spark.read.option("basePath", base_path).parquet(target_path)
        except Exception as error:
            raise RuntimeError(
                f"Não foi possível carregar a entidade Bronze '{name}' "
                f"do prefixo registrado: {manifest_prefixes[name]}."
            ) from error
    return tables


def _deduplicate_tables(tables, duplicate_keys):
    print("\n" + "#" * 75)
    print(" 3. Deduplicação das tabelas com chaves repetidas.")
    print("#" * 75 + "\n")
    for name, key_columns in duplicate_keys.items():
        print(f"\nAplicando Deduplicação na Tabela {name}...")
        raw_dataframe = tables[name]
        initial_count = raw_dataframe.count()
        deduplicated = deduplicate_by_window(
            df=raw_dataframe,
            key_columns=key_columns,
            order_by_cols=["_ingested_at", "proficiencia"],
            ascending=False,
        )
        tables[name] = deduplicated
        deduplicated_count = deduplicated.count()
        removed_count = initial_count - deduplicated_count
        print(
            f"  └─  Deduplicação concluída: {initial_count:,} ➔ "
            f"{deduplicated_count:,} linhas (Removidos: {removed_count:,}).\n"
        )


def _derive_student_uf(dataframe):
    if "sigla_uf" in dataframe.columns or "id_municipio" not in dataframe.columns:
        return dataframe

    dataframe = dataframe.withColumn(
        "sigla_uf_derived", F.substring(F.col("id_municipio"), 1, 2)
    )
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
    dataframe = dataframe.withColumn(
        "sigla_uf", ibge_uf_map[F.col("sigla_uf_derived")]
    ).drop("sigla_uf_derived")
    print("  └─  [Derivação UF] Coluna 'sigla_uf' criada em 'alunos' via código IBGE.")
    return dataframe


def _drop_bronze_metadata(dataframe):
    metadata_columns = ["_ingested_at", "_source", "ref_ano", "ref_mes", "ref_dia"]
    for column in metadata_columns:
        if column in dataframe.columns:
            dataframe = dataframe.drop(column)
    return dataframe


def _rename_existing_columns(dataframe, rename_map):
    for old_column, new_column in rename_map.items():
        if old_column in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(old_column, new_column)
    return dataframe


def _prepare_dimensions(tables):
    municipio = _drop_bronze_metadata(tables["municipio"])
    uf = _drop_bronze_metadata(tables["uf"])
    meta_municipio = _drop_bronze_metadata(tables["meta_alfabetizacao_municipio"])

    municipio_rename = {
        "taxa_alfabetizacao": "taxa_alfabetizacao_municipio",
        "media_portugues": "media_portugues_municipio",
    }
    uf_rename = {
        "taxa_alfabetizacao": "taxa_alfabetizacao_uf",
        "media_portugues": "media_portugues_uf",
    }
    for level, destination in (("municipio", municipio_rename), ("uf", uf_rename)):
        for index in range(9):
            destination[f"proporcao_aluno_nivel_{index}"] = (
                f"proporcao_aluno_nivel_{index}_{level}"
            )

    meta_rename = {
        "taxa_alfabetizacao": "taxa_alfabetizacao_meta_mun",
        "percentual_participacao": "percentual_participacao_meta_mun",
        "nivel_alfabetizacao": "nivel_alfabetizacao_meta_mun",
    }
    return (
        _rename_existing_columns(municipio, municipio_rename),
        _rename_existing_columns(uf, uf_rename),
        _rename_existing_columns(meta_municipio, meta_rename),
    )


def _join_and_check_quality(tables):
    students = _derive_student_uf(tables["alunos"])
    municipality, uf, meta_municipality = _prepare_dimensions(tables)
    print("\n" + "#" * 75)
    print(" 5. Executando JOIN relacional.")
    print("#" * 75 + "\n")
    municipality = municipality.withColumn(
        "_municipio_match_key", F.col("id_municipio")
    )
    joined = (
        students.join(
            municipality,
            on=["id_municipio", "ano", "rede", "serie"],
            how="left",
        )
        .join(uf, on=["sigla_uf", "ano", "rede", "serie"], how="left")
        .join(
            meta_municipality,
            on=["id_municipio", "ano", "rede"],
            how="left",
        )
    )
    print("\n" + "#" * 75)
    print(" 6. Verificando a taxa de correspondência do JOIN...")
    print("#" * 75 + "\n")
    match_ratio, metrics = check_join_ratio(
        df_left=students,
        df_joined=joined,
        match_key_col="_municipio_match_key",
        threshold_percent=JOIN_THRESHOLD,
        raise_on_failure=BLOCK_ON_FAILURE,
    )
    return joined.drop("_municipio_match_key"), match_ratio, metrics


def _apply_target_status(dataframe):
    if (
        "meta_alfabetizacao_2024" not in dataframe.columns
        or "taxa_alfabetizacao_municipio" not in dataframe.columns
    ):
        return dataframe
    return dataframe.withColumn(
        "status_meta_2024",
        F.when(F.col("meta_alfabetizacao_2024").isNull(), F.lit("Sem Meta Cadastrada"))
        .when(
            F.col("taxa_alfabetizacao_municipio") >= F.col("meta_alfabetizacao_2024"),
            F.lit("Atingiu a Meta"),
        )
        .otherwise(F.lit("Não Atingiu a Meta")),
    )


def _persist_curated_dataset(dataframe):
    print("\n" + "#" * 75)
    print(" 7. Persistindo a Camada Silver em Delta Lake.")
    print("#" * 75 + "\n")
    print(f" Alvo do Dataset Delta Lake: {output_path}")
    if DeltaTable.isDeltaTable(spark, output_path):
        print(
            " Tabela Delta existente detectada. Executando MERGE INTO "
            "(Upsert por id_aluno + ano)..."
        )
        delta_target = DeltaTable.forPath(spark, output_path)
        delta_target.alias("target").merge(
            source=dataframe.alias("source"),
            condition="target.id_aluno = source.id_aluno AND target.ano = source.ano",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("  Operação MERGE INTO executada na tabela Delta Silver.")
    else:
        print(" Criando nova Tabela Delta Silver inicial...")
        dataframe.write.format("delta").mode("overwrite").save(output_path)
        print("  Tabela Delta Silver inicial gravada.")

    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{GLUE_DATABASE_NAME}`."
            f"silver_alunos_curado USING DELTA LOCATION '{output_path}'"
        )
    except Exception as catalog_error:
        raise RuntimeError(
            f"Não foi possível registrar a tabela Silver no Glue Data Catalog "
            f"('{GLUE_DATABASE_NAME}.silver_alunos_curado')."
        ) from catalog_error
    print(
        f" Tabela Silver registrada no Glue Data Catalog: "
        f"{GLUE_DATABASE_NAME}.silver_alunos_curado"
    )


def _save_silver_manifest(metrics, match_ratio, output_rows):
    try:
        manifest_data = {
            "job_name": job_name,
            "run_id": run_id,
            "ingestion_date": ingestion_date,
            "status": "SUCCEEDED",
            "input_rows": int(metrics.get("total_left", 0)),
            "output_rows": output_rows,
            "matched_municipality_rows": int(metrics.get("total_matched", 0)),
            "match_ratio": float(metrics.get("match_ratio", 0.0)),
            "gate_status": "APPROVED" if match_ratio >= JOIN_THRESHOLD else "FAILED",
            "quality_gate_passed": match_ratio >= JOIN_THRESHOLD,
            "join_match_ratio_percent": float(match_ratio),
            "s3_path": output_path,
            "silver_s3_prefix": output_path,
        }
        manifest_key = (
            f"auditoria/silver/ingestion_date={ingestion_date}/"
            f"run_id={run_id}/manifest.json"
        )
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json",
        )
    except ClientError as manifest_error:
        raise RuntimeError(
            f"Não foi possível salvar o manifesto Silver em "
            f"s3://{S3_BUCKET}/{manifest_key}."
        ) from manifest_error
    print(f" Manifesto Silver salvo: s3://{S3_BUCKET}/{manifest_key}")


if is_delta_existing and all_bronze_skipped:
    print("\n" + "=" * 75)
    print(" A Camada Bronze não apresenta versões novas.")
    print(" O conjunto Delta existente será mantido.")
    print("=" * 75)

    # Emite manifesto Silver com status SKIPPED
    manifest_data = {
        "job_name": job_name,
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SKIPPED (Already Up-to-Date)",
        "gate_status": "APPROVED",
        "s3_path": output_path,
    }
    manifest_key = (
        f"auditoria/silver/ingestion_date={ingestion_date}/"
        f"run_id={run_id}/manifest.json"
    )
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest_data, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível salvar o manifesto Silver em "
            f"s3://{S3_BUCKET}/{manifest_key}."
        ) from error
    print(f" Manifesto Silver salvo: s3://{S3_BUCKET}/{manifest_key}")

else:
    # Lê apenas as partições validadas no manifesto Bronze.
    tables = _load_bronze_tables()

    # Chaves de negócio para diagnóstico de duplicidades por tabela
    TABLE_KEYS = {
        "alunos": ["id_aluno", "id_municipio", "ano", "rede", "serie"],
        "municipio": ["id_municipio", "ano", "rede", "serie"],
        "uf": ["sigla_uf", "ano", "rede", "serie"],
        "meta_alfabetizacao_brasil": ["ano", "rede"],
        "meta_alfabetizacao_uf": ["sigla_uf", "ano", "rede"],
        "meta_alfabetizacao_municipio": ["id_municipio", "ano", "rede"],
        "dicionario": ["id_tabela", "nome_coluna", "chave"],
    }

    # 2. Análise exploratória das entidades de entrada
    print("\n" + "#" * 75)
    print(" Análise exploratória das entidades de entrada")
    print("#" * 75 + "\n")

    tables_key_duplicate = _diagnose_tables(tables, TABLE_KEYS)

    # 3. Deduplicação Ativa nas tabelas duplicadas
    _deduplicate_tables(tables, tables_key_duplicate)

    # 4. Limpeza, Padronização e Eliminação de Colisão de Métricas
    print("\n" + "#" * 75)
    print(" 4. Executando Limpeza, Padronização e Eliminação de Colisão de Métricas...")
    print("#" * 75 + "\n")

    # 4. Limpeza, padronização, JOIN relacional e validação de qualidade.
    df_joined, match_ratio, metrics = _join_and_check_quality(tables)

    # Tripla Categorização do Status da Meta
    df_joined = _apply_target_status(df_joined)

    # Timestamp de enriquecimento da camada Silver
    df_joined = df_joined.withColumn("_processed_silver_at", F.current_timestamp())
    output_rows = df_joined.count()

    _persist_curated_dataset(df_joined)
    _save_silver_manifest(metrics, match_ratio, output_rows)

print("\n" + "=" * 75)
print("Camada Silver concluída.")
print("=" * 75)
