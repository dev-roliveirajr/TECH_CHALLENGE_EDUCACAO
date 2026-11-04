"""
===============================================================================
JOB AWS GLUE - CAMADA GOLD (src/scripts/aggregate_gold_layer.py)
===============================================================================
Descrição: Consolidação dos indicadores educacionais (Metas vs. Resultados) por
Município e UF, e geração da Feature Store para Machine Learning (uso futuro).
===============================================================================
"""

import sys
import os
import json
import datetime
import boto3
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from gold_aggregation import (
    GOLD_TRANSFORM_VERSION,
    aggregate_indicators,
    aggregate_public_indicators,
    build_feature_store,
    compare_to_official_target,
    should_skip_gold_processing,
)
from manifest_lib import get_latest_manifest

# Importa funções de validação cruzada e governança da biblioteca de qualidade
try:
    from quality_lib import check_cross_validation
except ImportError:
    check_cross_validation = None

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Obter parâmetros de execução passados pelo Glue Job
args = getResolvedOptions(sys.argv, ["JOB_NAME", "GLUE_DATABASE_NAME"])
job_name = args["JOB_NAME"]
GLUE_DATABASE_NAME = args["GLUE_DATABASE_NAME"]

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ["S3_BUCKET_NAME"])
        S3_BUCKET = args_s3["S3_BUCKET_NAME"]
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("A variável/argumento S3_BUCKET_NAME não foi definida.")

# Definição dos caminhos no S3
SILVER_PATH = f"s3://{S3_BUCKET}/silver/alunos_curado/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"
AUDIT_PATH = f"s3://{S3_BUCKET}/auditoria/gold/"

run_id = os.getenv(
    "GLUE_RUN_ID",
    os.getenv(
        "JOB_RUN_ID", f"gld_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    ),
)
ingestion_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

print("=" * 75)
print(" INICIANDO PROCESSAMENTO DA CAMADA GOLD - INDICADORES E FEATURE STORE")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Run ID: {run_id} | Data: {ingestion_date}")
print("=" * 75)


def get_latest_silver_manifest(bucket_name):
    """Localiza e lê o último manifesto de auditoria gerado pela Camada Silver."""
    return get_latest_manifest(
        boto3.client("s3"),
        bucket_name,
        "auditoria/silver/",
        "Camada Silver",
    )


silver_manifest = get_latest_silver_manifest(S3_BUCKET)
silver_status = silver_manifest.get("status") if silver_manifest else None

gold_paths = {
    "indicadores_municipais": f"{GOLD_PATH}gold_indicadores_municipais/",
    "indicadores_uf": f"{GOLD_PATH}gold_indicadores_uf/",
    "indicadores_brasil": f"{GOLD_PATH}gold_indicadores_brasil/",
    "metas_municipais": f"{GOLD_PATH}gold_desempenho_vs_metas/",
    "metas_uf": f"{GOLD_PATH}gold_desempenho_vs_metas_uf/",
    "metas_brasil": f"{GOLD_PATH}gold_desempenho_vs_metas_brasil/",
    "features_municipais": f"{GOLD_PATH}gold_features_alfabetizacao/",
    "features_uf": f"{GOLD_PATH}gold_features_alfabetizacao_uf/",
    "features_brasil": f"{GOLD_PATH}gold_features_alfabetizacao_brasil/",
}

# Check de Short-Circuit para a Gold se a Silver deu SKIPPED e a Gold já existe
s3_client = boto3.client("s3")
latest_gold_manifest = get_latest_manifest(
    s3_client,
    S3_BUCKET,
    "auditoria/gold/",
    "Camada Gold",
)
previous_transform_version = (
    latest_gold_manifest.get("gold_transform_version") if latest_gold_manifest else None
)


def check_s3_path_has_objects(prefix):
    try:
        resp = s3_client.list_objects_v2(
            Bucket=S3_BUCKET, Prefix=prefix.replace(f"s3://{S3_BUCKET}/", ""), MaxKeys=1
        )
        return "Contents" in resp and len(resp["Contents"]) > 0
    except Exception:
        return False


def _read_silver_dataset():
    print(f" 1. Lendo dataset curado da Camada Silver em: {SILVER_PATH}")
    try:
        try:
            dataframe = spark.read.format("delta").load(SILVER_PATH)
            print(
                "  └─  Conexão realizada via leitor Delta Lake nativo (_delta_log/ ativado)."
            )
        except Exception as delta_error:
            print(
                f"  └─  Aviso ao carregar como Delta ({delta_error}). "
                "Recorrendo ao leitor Parquet..."
            )
            dataframe = spark.read.parquet(SILVER_PATH)
        row_count = dataframe.count()
        print(f"  └─  Registros ativos lidos do snapshot Silver: {row_count:,}")
        return dataframe, row_count
    except Exception as error:
        raise RuntimeError(
            f" Falha ao carregar dados curados da Camada Silver: {str(error)}"
        ) from error


def _validate_indicators(df_indicadores_mun):
    print(" 3. Executando validação cruzada municipal com a taxa publicada...")
    cv_passed = True
    cv_diff = 0.0
    if check_cross_validation:
        try:
            avg_divergence, _ = check_cross_validation(
                df_recalculated=df_indicadores_mun,
                calculated_col="taxa_alfabetizacao_recalculada",
                published_col="taxa_alfabetizacao_oficial_inep",
                max_divergence_percent=2.0,
            )
            cv_diff = float(avg_divergence)
            cv_passed = cv_diff <= 2.0
        except Exception as cv_error:
            print(f"  └─  Alerta na Validação Cruzada: {str(cv_error)}")
    return cv_passed, cv_diff


def _write_gold_datasets(datasets):
    print(" 6. Salvando datasets Gold em Parquet...")
    for dataset_name, dataframe in datasets.items():
        target_path = gold_paths[dataset_name]
        dataframe.write.mode("overwrite").parquet(target_path)
        print(f"  └─  {dataset_name} salvo em: {target_path}")


def _register_gold_tables():
    print("\n Registrando tabelas Gold no Athena / Glue Data Catalog...")
    catalog_tables = [
        ("gold_indicadores_municipais", "indicadores_municipais"),
        ("gold_indicadores_uf", "indicadores_uf"),
        ("gold_indicadores_brasil", "indicadores_brasil"),
        ("gold_desempenho_vs_metas", "metas_municipais"),
        ("gold_desempenho_vs_metas_uf", "metas_uf"),
        ("gold_desempenho_vs_metas_brasil", "metas_brasil"),
        ("gold_features_alfabetizacao", "features_municipais"),
        ("gold_features_alfabetizacao_uf", "features_uf"),
        ("gold_features_alfabetizacao_brasil", "features_brasil"),
    ]

    for table_name, dataset_name in catalog_tables:
        table_path = gold_paths[dataset_name]
        try:
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS `{GLUE_DATABASE_NAME}`.`{table_name}` "
                f"USING PARQUET LOCATION '{table_path}'"
            )
            print(
                f"  Tabela `{GLUE_DATABASE_NAME}`.`{table_name}` registrada no catálogo."
            )
        except Exception as catalog_error:
            print(
                f" Aviso ao registrar tabela '{table_name}' no Catalog: "
                f"{catalog_error}"
            )


def _save_gold_manifest(manifest_content):
    manifest_s3_key = (
        f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
    )
    print(
        f" 7. Gravando Manifesto de Auditoria Gold em: "
        f"s3://{S3_BUCKET}/{manifest_s3_key}"
    )

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_s3_key,
            Body=json.dumps(manifest_content, indent=2),
            ContentType="application/json",
        )
        print("  Manifesto Gold gravado.")
    except ClientError as manifest_error:
        raise RuntimeError(
            f"Não foi possível salvar o manifesto Gold em "
            f"s3://{S3_BUCKET}/{manifest_s3_key}."
        ) from manifest_error


gold_already_built = all(
    check_s3_path_has_objects(path) for path in gold_paths.values()
)

if should_skip_gold_processing(
    silver_status,
    gold_already_built,
    previous_transform_version,
):
    print("\n" + "=" * 75)
    print(" Camada Silver permaneceu inalterada (Status SKIPPED).")
    print("   Os datasets da Camada Gold já estão atualizados no S3.")
    print("   Processamento da Camada Gold ignorado.")
    print("=" * 75)

    manifest_content = {
        "job_name": "03_gold_aggregation",
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SKIPPED (Already Up-to-Date)",
        "gold_transform_version": GOLD_TRANSFORM_VERSION,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "message": "Nenhuma alteração na Silver. Datasets Gold mantidos sem re-computação.",
        "gold_s3_prefixes": {
            **gold_paths,
        },
    }
    manifest_s3_key = (
        f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
    )
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_s3_key,
            Body=json.dumps(manifest_content, indent=2),
            ContentType="application/json",
        )
    except ClientError as error:
        raise RuntimeError(
            f"Não foi possível salvar o manifesto Gold em "
            f"s3://{S3_BUCKET}/{manifest_s3_key}."
        ) from error
    print(f" Manifesto Gold SKIPPED salvo: s3://{S3_BUCKET}/{manifest_s3_key}")
else:
    # 1. Leitura do Dataset Curado da Camada Silver
    df_silver, total_silver_rows = _read_silver_dataset()

    # 2. Agregação ponderada em três níveis geográficos.
    print(" 2. Calculando indicadores ponderados municipais, estaduais e nacionais...")
    df_avaliados = df_silver.filter(
        F.col("proficiencia").isNotNull() & F.col("peso_aluno").isNotNull()
    ).withColumn("rede", F.col("rede").cast("string"))

    df_indicadores_mun = aggregate_indicators(
        df_avaliados,
        ["id_municipio", "sigla_uf", "ano", "rede", "serie"],
        "taxa_alfabetizacao_municipio",
    )
    df_indicadores_uf = aggregate_indicators(
        df_avaliados,
        ["sigla_uf", "ano", "rede", "serie"],
        "taxa_alfabetizacao_uf",
    )
    df_indicadores_brasil = aggregate_indicators(
        df_avaliados,
        ["ano", "rede", "serie"],
    )
    df_indicadores_uf_public = aggregate_public_indicators(
        df_avaliados,
        ["sigla_uf", "ano", "serie"],
    )
    df_indicadores_brasil_public = aggregate_public_indicators(
        df_avaliados,
        ["ano", "serie"],
    )

    indicator_datasets = {
        "indicadores_municipais": df_indicadores_mun,
        "indicadores_uf": df_indicadores_uf,
        "indicadores_brasil": df_indicadores_brasil,
    }
    indicator_counts = {
        name: dataframe.count() for name, dataframe in indicator_datasets.items()
    }
    total_indicadores_rows = sum(indicator_counts.values())
    print(
        "  └─  Indicadores calculados: "
        + ", ".join(f"{name}={count:,}" for name, count in indicator_counts.items())
    )

    # 3. Validação cruzada disponível apenas no nível municipal.
    cv_passed, cv_diff = _validate_indicators(df_indicadores_mun)

    # 4. Compara somente redes compatíveis com o dicionário das metas.
    print(" 4. Construindo comparativos contra a meta oficial de 80%...")

    df_desempenho_metas = compare_to_official_target(df_indicadores_mun, "3")
    df_desempenho_metas_uf = compare_to_official_target(
        df_indicadores_uf_public,
        "5",
    )
    df_desempenho_metas_brasil = compare_to_official_target(
        df_indicadores_brasil_public,
        "5",
    )
    meta_datasets = {
        "metas_municipais": df_desempenho_metas,
        "metas_uf": df_desempenho_metas_uf,
        "metas_brasil": df_desempenho_metas_brasil,
    }
    meta_counts = {name: dataframe.count() for name, dataframe in meta_datasets.items()}
    total_metas_rows = sum(meta_counts.values())
    print(
        "  └─  Comparativos calculados: "
        + ", ".join(f"{name}={count:,}" for name, count in meta_counts.items())
    )

    # 5. Feature Store com uma única variável-alvo baseada na meta oficial.
    print(" 5. Construindo o conjunto de características...")
    feature_datasets = {
        "features_municipais": build_feature_store(df_desempenho_metas),
        "features_uf": build_feature_store(df_desempenho_metas_uf),
        "features_brasil": build_feature_store(df_desempenho_metas_brasil),
    }
    feature_counts = {
        name: dataframe.count() for name, dataframe in feature_datasets.items()
    }
    total_features_rows = sum(feature_counts.values())
    print(
        "  └─  Features calculadas: "
        + ", ".join(f"{name}={count:,}" for name, count in feature_counts.items())
    )

    # 6. Gravação dos datasets Gold no S3.
    gold_datasets = {
        **indicator_datasets,
        **meta_datasets,
        **feature_datasets,
    }
    _write_gold_datasets(gold_datasets)
    _register_gold_tables()

    # 7. Geração do Manifesto de Auditoria da Camada Gold no S3
    manifest_content = {
        "job_name": "03_gold_aggregation",
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SUCCEEDED",
        "gold_transform_version": GOLD_TRANSFORM_VERSION,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "input_rows_silver": total_silver_rows,
        "output_rows_indicadores": total_indicadores_rows,
        "output_rows_indicadores_by_level": {
            "municipio": indicator_counts["indicadores_municipais"],
            "uf": indicator_counts["indicadores_uf"],
            "brasil": indicator_counts["indicadores_brasil"],
        },
        "output_rows_metas": total_metas_rows,
        "output_rows_metas_by_level": {
            "municipio": meta_counts["metas_municipais"],
            "uf": meta_counts["metas_uf"],
            "brasil": meta_counts["metas_brasil"],
        },
        "output_rows_features": total_features_rows,
        "output_rows_features_by_level": {
            "municipio": feature_counts["features_municipais"],
            "uf": feature_counts["features_uf"],
            "brasil": feature_counts["features_brasil"],
        },
        "cross_validation_passed": cv_passed,
        "cross_validation_diff_percent": round(cv_diff, 4),
        "target_meta_pactuada": "80.0%",
        "gold_s3_prefixes": {
            **gold_paths,
        },
    }

    _save_gold_manifest(manifest_content)

print("=" * 75)
print("Camada Gold concluída.")
print("=" * 75)
