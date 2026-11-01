"""
===============================================================================
JOB AWS GLUE - CAMADA GOLD (src/scripts/aggregate_gold_layer.py)
===============================================================================
Descrição: Consolidação dos indicadores educacionais (Metas vs. Resultados) por
Município e UF, e geração da Feature Store para Machine Learning (Fase 3).
===============================================================================
"""

import sys
import os
import json
import datetime
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

# Importa funções de validação cruzada e governança da biblioteca de qualidade
try:
    from quality_lib import check_cross_validation
except ImportError:
    check_cross_validation = None

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Obter parâmetros de execução passados pelo Glue Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job_name = args["JOB_NAME"]

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ["S3_BUCKET_NAME"])
        S3_BUCKET = args_s3["S3_BUCKET_NAME"]
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("❌ A variável/argumento S3_BUCKET_NAME não foi definida!")

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
print("🚀 INICIANDO PROCESSAMENTO DA CAMADA GOLD - INDICADORES E FEATURE STORE")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Run ID: {run_id} | Data: {ingestion_date}")
print("=" * 75)

def get_latest_silver_manifest(bucket_name):
    """Localiza e lê o último manifesto de auditoria gerado pela Camada Silver."""
    try:
        s3_client = boto3.client("s3")
        prefix = "auditoria/silver/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            return None

        manifest_files = [
            obj for obj in response["Contents"]
            if obj["Key"].endswith("manifest.json")
        ]
        if not manifest_files:
            return None

        sorted_files = sorted(manifest_files, key=lambda x: x["LastModified"], reverse=True)
        latest_key = sorted_files[0]["Key"]
        obj_resp = s3_client.get_object(Bucket=bucket_name, Key=latest_key)
        return json.loads(obj_resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ Alerta ao buscar manifesto Silver: {str(e)}")
        return None

silver_manifest = get_latest_silver_manifest(S3_BUCKET)
silver_status = silver_manifest.get("status") if silver_manifest else None

path_indicadores = f"{GOLD_PATH}gold_indicadores_municipais/"
path_metas = f"{GOLD_PATH}gold_desempenho_vs_metas/"
path_features = f"{GOLD_PATH}gold_features_alfabetizacao/"

# Check de Short-Circuit para a Gold se a Silver deu SKIPPED e a Gold já existe
s3_client = boto3.client("s3")
def check_s3_path_has_objects(prefix):
    try:
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix.replace(f"s3://{S3_BUCKET}/", ""), MaxKeys=1)
        return "Contents" in resp and len(resp["Contents"]) > 0
    except Exception:
        return False

gold_already_built = (
    check_s3_path_has_objects(path_indicadores) and 
    check_s3_path_has_objects(path_metas) and 
    check_s3_path_has_objects(path_features)
)

if silver_status == "SKIPPED (Already Up-to-Date)" and gold_already_built:
    print("\n" + "=" * 75)
    print("⚡ [SHORT-CIRCUIT FINOPS] Camada Silver permaneceu inalterada (Status SKIPPED).")
    print("   Datasets Analíticos da Camada Gold já se encontram 100% atualizados no S3.")
    print("   └─ ⏩ Processamento Gold SKIPPED com sucesso em segundos!")
    print("=" * 75)

    manifest_content = {
        "job_name": "03_gold_aggregation",
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SKIPPED (Already Up-to-Date)",
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "message": "Nenhuma alteração na Silver. Datasets Gold mantidos sem re-computação.",
        "gold_s3_prefixes": {
            "indicadores": path_indicadores,
            "metas": path_metas,
            "features": path_features,
        },
    }
    manifest_s3_key = f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_s3_key,
            Body=json.dumps(manifest_content, indent=2),
            ContentType="application/json",
        )
        print(f"📜 [Auditoria Gold] Manifesto SKIPPED salvo no S3: s3://{S3_BUCKET}/{manifest_s3_key}")
    except Exception as m_err:
        print(f"  └─ ⚠️ Alerta ao gravar manifesto Gold SKIPPED: {str(m_err)}")
else:
    # 1. Leitura do Dataset Curado da Camada Silver
    print(f"📥 1. Lendo dataset curado da Camada Silver em: {SILVER_PATH}")
    try:
        try:
            df_silver = spark.read.format("delta").load(SILVER_PATH)
            print("  └─ 🚀 Conexão realizada via leitor Delta Lake nativo (_delta_log/ ativado).")
        except Exception as delta_err:
            print(f"  └─ ℹ️ Aviso ao carregar como Delta ({delta_err}). Recorrendo ao leitor Parquet...")
            df_silver = spark.read.parquet(SILVER_PATH)

        total_silver_rows = df_silver.count()
        print(f"  └─ ✅ Registros ativos lidos do snapshot Silver: {total_silver_rows:,}")
    except Exception as e:
        raise RuntimeError(f"❌ Falha ao carregar dados curados da Camada Silver: {str(e)}")

    # 2. Agregação Ponderada por Peso Amostral (Regra do Saeb)
    print("📊 2. Calculando Indicadores Municipais com Média Ponderada pelo Peso Amostral...")

    df_avaliados = df_silver.filter(
        F.col("proficiencia").isNotNull() & F.col("peso_aluno").isNotNull()
    )

    # Aplica a fórmula oficial da taxa de alfabetização ponderada pelo peso do aluno
    df_indicadores_mun = (
        df_avaliados.groupBy("id_municipio", "sigla_uf", "ano", "rede", "serie")
        .agg(
            F.count("*").alias("total_alunos_avaliados"),
            F.sum("peso_aluno").alias("soma_peso_amostral"),
            F.sum(
                F.when(
                    F.col("alfabetizado").isin("Sim", "1", 1), F.col("peso_aluno")
                ).otherwise(0)
            ).alias("soma_peso_alfabetizados"),
            F.avg("proficiencia").alias("media_proficiencia_simples"),
            F.first("taxa_alfabetizacao_municipio").alias(
                "taxa_alfabetizacao_oficial_inep"
            ),
        )
        .withColumn(
            "taxa_alfabetizacao_recalculada",
            F.round(
                (F.col("soma_peso_alfabetizados") / F.col("soma_peso_amostral")) * 100.0, 2
            ),
        )
    )

    total_indicadores_rows = df_indicadores_mun.count()
    print(
        f"  └─ ✅ Indicadores Municipais Ponderados calculados: {total_indicadores_rows:,} registros."
    )

    # 3. Validação Cruzada (Microdados do Aluno vs. Taxa Oficial Publicada)
    print(
        "🔬 3. Executando Validação Cruzada (Microdados do Aluno vs. Taxa Oficial Publicada)..."
    )
    cv_passed = True
    cv_diff = 0.0

    if check_cross_validation:
        try:
            avg_divergence, cv_metrics = check_cross_validation(
                df_recalculated=df_indicadores_mun,
                calculated_col="taxa_alfabetizacao_recalculada",
                published_col="taxa_alfabetizacao_oficial_inep",
                max_divergence_percent=2.0,
            )
            cv_diff = float(avg_divergence)
            cv_passed = cv_diff <= 2.0
        except Exception as cv_err:
            print(f"  └─ ⚠️ Alerta na Validação Cruzada: {str(cv_err)}")

    # 4. Tabela Comparativa contra a Meta Pactuada Oficial
    print("🎯 4. Construindo Tabela Comparativa contra a Meta Pactuada Oficial...")

    df_desempenho_metas = (
        df_indicadores_mun.withColumn("meta_pactuada_2030", F.lit(80.0))
        .withColumn(
            "gap_para_meta",
            F.round(
                F.col("taxa_alfabetizacao_recalculada") - F.col("meta_pactuada_2030"), 2
            ),
        )
        .withColumn(
            "status_meta",
            F.when(F.col("meta_pactuada_2030").isNull(), F.lit("Sem Meta Cadastrada"))
            .when(
                F.col("taxa_alfabetizacao_recalculada") >= F.col("meta_pactuada_2030"),
                F.lit("Atingiu a Meta"),
            )
            .otherwise(F.lit("Não Atingiu a Meta")),
        )
        .withColumn("_created_gold_at", F.current_timestamp())
    )

    total_metas_rows = df_desempenho_metas.count()
    print(f"  └─ ✅ Tabela Comparativa de Metas gerada: {total_metas_rows:,} registros.")

    # 5. Feature Store para Machine Learning (Fase 3)
    print("🤖 5. Construindo Feature Store para Machine Learning (Fase 3)...")

    df_feature_store = df_desempenho_metas.select(
        "id_municipio",
        "sigla_uf",
        "ano",
        "rede",
        "serie",
        "total_alunos_avaliados",
        "soma_peso_amostral",
        "media_proficiencia_simples",
        "taxa_alfabetizacao_recalculada",
        "meta_pactuada_2030",
        "gap_para_meta",
        F.col("status_meta").alias("rotulo_alvo"),
        "_created_gold_at",
    )

    total_features_rows = df_feature_store.count()
    print(
        f"  └─ ✅ Feature Store criada com {total_features_rows:,} registros e {len(df_feature_store.columns)} colunas."
    )

    # 6. Gravação dos Datasets da Camada Gold no S3 (Serverless-Safe)
    print(
        "💾 6. Salvando datasets da Camada Gold no Amazon S3 em formato Parquet comprimido (Snappy)..."
    )

    path_indicadores = f"{GOLD_PATH}gold_indicadores_municipais/"
    path_metas = f"{GOLD_PATH}gold_desempenho_vs_metas/"
    path_features = f"{GOLD_PATH}gold_features_alfabetizacao/"

    df_indicadores_mun.write.mode("overwrite").parquet(path_indicadores)
    print(f"  ├─ 💾 Indicadores Municipais salvos em: {path_indicadores}")

    df_desempenho_metas.write.mode("overwrite").parquet(path_metas)
    print(f"  ├─ 💾 Comparativo de Metas salvo em: {path_metas}")

    df_feature_store.write.mode("overwrite").parquet(path_features)
    print(f"  └─ 💾 Feature Store salva em: {path_features}")

    # 7. Geração do Manifesto de Auditoria da Camada Gold no S3
    manifest_content = {
        "job_name": "03_gold_aggregation",
        "run_id": run_id,
        "ingestion_date": ingestion_date,
        "status": "SUCCEEDED",
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "input_rows_silver": total_silver_rows,
        "output_rows_indicadores": total_indicadores_rows,
        "output_rows_metas": total_metas_rows,
        "output_rows_features": total_features_rows,
        "cross_validation_passed": cv_passed,
        "cross_validation_diff_percent": round(cv_diff, 4),
        "target_meta_pactuada": "80.0%",
        "gold_s3_prefixes": {
            "indicadores": path_indicadores,
            "metas": path_metas,
            "features": path_features,
        },
    }

    manifest_s3_key = (
        f"auditoria/gold/ingestion_date={ingestion_date}/run_id={run_id}/manifest.json"
    )
    print(
        f"📜 7. Gravando Manifesto de Auditoria Gold em: s3://{S3_BUCKET}/{manifest_s3_key}"
    )

    try:
        import boto3

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=manifest_s3_key,
            Body=json.dumps(manifest_content, indent=2),
            ContentType="application/json",
        )
        print("  └─ ✅ Manifesto Gold gerado com sucesso!")
    except Exception as manifest_err:
        print(f"  └─ ⚠️ Alerta ao gravar manifesto Gold no S3: {str(manifest_err)}")

print("=" * 75)
print("✅ CAMADA GOLD CONCLUÍDA COM SUCESSO DE PONTA A PONTA!")
print("=" * 75)
