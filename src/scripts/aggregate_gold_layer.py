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
import re
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
from manifest_lib import (
    get_latest_approved_gold_manifest,
    get_latest_approved_silver_manifest,
    get_latest_manifest,
)
from quality_lib import (
    check_cross_validation,
    persist_quality_report,
    quality_check_result,
)

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
SILVER_PATH = None
TARGET_PATHS = {}
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"
AUDIT_PATH = f"s3://{S3_BUCKET}/auditoria/gold/"
SHARED_SERIES_CONFIRMED = (
    os.getenv("OFFICIAL_TARGETS_SHARED_ACROSS_SERIES_CONFIRMED", "").lower()
    == "true"
)
try:
    gold_options = getResolvedOptions(
        sys.argv,
        ["OFFICIAL_TARGETS_SHARED_ACROSS_SERIES_CONFIRMED"],
    )
except Exception:
    gold_options = {}
SHARED_SERIES_CONFIRMED = gold_options.get(
    "OFFICIAL_TARGETS_SHARED_ACROSS_SERIES_CONFIRMED",
    str(SHARED_SERIES_CONFIRMED),
).lower() == "true"

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
latest_approved_silver_manifest = get_latest_approved_silver_manifest(
    boto3.client("s3"),
    S3_BUCKET,
)
if latest_approved_silver_manifest is None:
    raise FileNotFoundError(
        "Não há snapshot Silver oficial com publicação aprovada disponível."
    )
if (
    latest_approved_silver_manifest.get("silver_transform_version")
    != "2026-11-official-inputs-v2"
):
    raise ValueError(
        "O snapshot Silver aprovado não atende à versão do contrato oficial "
        "exigida pela Gold."
    )
SILVER_PATH = latest_approved_silver_manifest["official_student_s3_prefix"]
TARGET_PATHS = latest_approved_silver_manifest["official_target_s3_prefixes"]

gold_paths = {
    "indicadores_municipais": f"{GOLD_PATH}runs/{run_id}/gold_indicadores_municipais/",
    "indicadores_uf": f"{GOLD_PATH}runs/{run_id}/gold_indicadores_uf/",
    "indicadores_brasil": f"{GOLD_PATH}runs/{run_id}/gold_indicadores_brasil/",
    "metas_municipais": f"{GOLD_PATH}runs/{run_id}/gold_desempenho_vs_metas/",
    "metas_uf": f"{GOLD_PATH}runs/{run_id}/gold_desempenho_vs_metas_uf/",
    "metas_brasil": f"{GOLD_PATH}runs/{run_id}/gold_desempenho_vs_metas_brasil/",
    "features_municipais": f"{GOLD_PATH}runs/{run_id}/gold_features_alfabetizacao/",
    "features_uf": f"{GOLD_PATH}runs/{run_id}/gold_features_alfabetizacao_uf/",
    "features_brasil": f"{GOLD_PATH}runs/{run_id}/gold_features_alfabetizacao_brasil/",
}

# Check de Short-Circuit para a Gold se a Silver deu SKIPPED e a Gold já existe
s3_client = boto3.client("s3")
latest_gold_manifest = get_latest_manifest(
    s3_client,
    S3_BUCKET,
    "auditoria/gold/",
    "Camada Gold",
)
latest_approved_gold_manifest = get_latest_approved_gold_manifest(
    s3_client,
    S3_BUCKET,
    required_datasets=gold_paths.keys(),
)
previous_transform_version = (
    latest_approved_gold_manifest.get("gold_transform_version")
    if latest_approved_gold_manifest
    else None
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
        dataframe = spark.read.format("delta").load(SILVER_PATH)
    except Exception as error:
        raise RuntimeError(
            f"Falha ao carregar o snapshot Delta oficial da Camada Silver "
            f"em '{SILVER_PATH}'. Fallback implícito para Parquet foi removido."
        ) from error
    row_count = dataframe.count()
    print(f"  └─  Registros ativos lidos do snapshot Silver oficial: {row_count:,}")
    return dataframe, row_count


def _read_official_targets():
    return {
        level: spark.read.format("delta").load(path)
        for level, path in TARGET_PATHS.items()
    }


def _validate_indicators(df_indicadores_mun):
    print(" 3. Executando validação cruzada municipal com a taxa publicada...")
    avg_divergence, metrics = check_cross_validation(
        df_recalculated=df_indicadores_mun,
        calculated_col="taxa_alfabetizacao_recalculada",
        published_col="taxa_alfabetizacao_oficial_inep",
        max_divergence_percent=2.0,
    )
    return (
        metrics.get("total_records", 0) > 0 and avg_divergence <= 2.0,
        float(avg_divergence),
        metrics,
    )


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

    safe_run_id = re.sub(r"[^A-Za-z0-9_]", "_", run_id)[:40]
    for table_name, dataset_name in catalog_tables:
        table_path = gold_paths[dataset_name]
        try:
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS `{GLUE_DATABASE_NAME}`."
                f"`{table_name}_run_{safe_run_id}` "
                f"USING PARQUET LOCATION '{table_path}'"
            )
            print(
                f"  Tabela `{GLUE_DATABASE_NAME}`."
                f"`{table_name}_run_{safe_run_id}` registrada no catálogo."
            )
        except Exception as catalog_error:
            raise RuntimeError(
                f"Não foi possível registrar a tabela Gold versionada "
                f"'{table_name}_run_{safe_run_id}'."
            ) from catalog_error


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


gold_already_built = latest_approved_gold_manifest is not None

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
        "gate_status": "APPROVED",
        "publication_status": "COMMITTED",
        "quality_gate_passed": True,
        "gold_transform_version": GOLD_TRANSFORM_VERSION,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "message": "Nenhuma alteração na Silver. O último run Gold aprovado foi mantido.",
        "gold_s3_prefixes": {
            **latest_approved_gold_manifest["gold_s3_prefixes"],
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
    if "_source" not in df_silver.columns:
        raise ValueError("A Silver oficial não contém a coluna de proveniência '_source'.")
    non_official_rows = df_silver.filter(
        F.col("_source").isNull()
        | (F.col("_source") != F.lit("basedosdados_bigquery"))
    ).limit(1).count()
    if non_official_rows:
        raise ValueError(
            "A Gold oficial recebeu linhas sem proveniência "
            "basedosdados_bigquery; a agregação foi bloqueada."
        )

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
    cv_passed, cv_diff, cv_metrics = _validate_indicators(df_indicadores_mun)

    # 4. Compara redes compatíveis contra metas oficiais do respectivo ano.
    print(" 4. Construindo comparativos contra as metas oficiais anuais...")
    official_targets = _read_official_targets()
    try:
        df_desempenho_metas = compare_to_official_target(
            df_indicadores_mun,
            official_targets["municipio"],
            "3",
            shared_across_series_confirmed=SHARED_SERIES_CONFIRMED,
        )
        df_desempenho_metas_uf = compare_to_official_target(
            df_indicadores_uf_public,
            official_targets["uf"],
            "5",
            shared_across_series_confirmed=SHARED_SERIES_CONFIRMED,
        )
        df_desempenho_metas_brasil = compare_to_official_target(
            df_indicadores_brasil_public,
            official_targets["brasil"],
            "5",
            shared_across_series_confirmed=SHARED_SERIES_CONFIRMED,
        )
    except ValueError as error:
        failed_report = {
            "run_id": run_id,
            "layer": "gold",
            "status": "FAILED",
            "checks": [
                quality_check_result(
                    rule_id="GOLD-OFFICIAL-TARGET-CONTRACT",
                    scope="all",
                    status="FAIL",
                    blocking=True,
                    source="official_silver_target_contract",
                    detail=str(error),
                )
            ],
        }
        persist_quality_report(
            s3_client,
            S3_BUCKET,
            f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/"
            "quality_report.json",
            failed_report,
        )
        raise
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

    expected_comparison_counts = {
        "metas_municipais": df_indicadores_mun.filter(F.col("rede") == "3").count(),
        "metas_uf": df_indicadores_uf_public.count(),
        "metas_brasil": df_indicadores_brasil_public.count(),
    }
    comparison_counts = {
        name: dataframe.count() for name, dataframe in meta_datasets.items()
    }
    comparison_count_passed = all(
        comparison_counts[name] == expected
        for name, expected in expected_comparison_counts.items()
    )
    quality_checks = [
        quality_check_result(
            rule_id="GOLD-CROSS-VALIDATION",
            scope="municipio",
            status="PASS" if cv_passed else "FAIL",
            blocking=True,
            threshold=2.0,
            observed=cv_diff,
            denominator=cv_metrics.get("total_records"),
            source="taxa_alfabetizacao_recalculada_vs_publicada",
            detail=cv_metrics,
        ),
        quality_check_result(
            rule_id="GOLD-COMPARISON-GRAIN",
            scope="all",
            status="PASS" if comparison_count_passed else "FAIL",
            blocking=True,
            observed=comparison_counts,
            denominator=expected_comparison_counts,
            source="indicator_to_target_left_join",
            detail="Comparativos devem preservar o número de linhas dos indicadores.",
        ),
    ]
    quality_report = {
        "run_id": run_id,
        "layer": "gold",
        "status": "APPROVED"
        if all(
            check["status"] in {"PASS", "NOT_APPLICABLE"}
            or not check["blocking"]
            for check in quality_checks
        )
        else "FAILED",
        "checks": quality_checks,
    }
    quality_report_key = (
        f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/"
        "quality_report.json"
    )
    persist_quality_report(s3_client, S3_BUCKET, quality_report_key, quality_report)
    if quality_report["status"] != "APPROVED":
        raise ValueError("Gates de qualidade Gold reprovados; execução não promovida.")

    # 6. Gravação dos datasets Gold sob um prefixo imutável do run.
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
        "gate_status": "APPROVED",
        "publication_status": "COMMITTED",
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
        "cross_validation_metrics": cv_metrics,
        "quality_report_s3_uri": f"s3://{S3_BUCKET}/{quality_report_key}",
        "comparison_product_role": "descriptive_comparison_not_training_features",
        "target_contract": "official_annual_targets_by_geography_network_year",
        "gold_s3_prefixes": {
            **gold_paths,
        },
    }

    _save_gold_manifest(manifest_content)

print("=" * 75)
print("Camada Gold concluída.")
print("=" * 75)
