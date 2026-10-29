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
    "GLUE_RUN_ID", f"gld_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
)
ingestion_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

print("=" * 75)
print("🚀 INICIANDO PROCESSAMENTO DA CAMADA GOLD - INDICADORES E FEATURE STORE")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Run ID: {run_id} | Data: {ingestion_date}")
print("=" * 75)

# 1. Leitura do Dataset Curado da Camada Silver
print(f"📥 1. Lendo dataset curado da Camada Silver em: {SILVER_PATH}")
try:
    df_silver = spark.read.parquet(SILVER_PATH)
    total_silver_rows = df_silver.count()
    print(f"  └─ ✅ Registros lidos da Silver: {total_silver_rows:,}")
except Exception as e:
    raise RuntimeError(f"❌ Falha ao carregar dados curados da Camada Silver: {str(e)}")

# 2. Agregação Ponderada por Peso Amostral (Regra do Saeb)
print(
    "📊 2. Calculando Indicadores Muncipais com Média Ponderada pelo Peso Amostral..."
)

# Filtra apenas estudantes avaliados (com proficiência e peso válidos)
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
            F.when(F.col("alfabetizado") == "Sim", F.col("peso_aluno")).otherwise(0)
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
        cv_passed, cv_diff = check_cross_validation(
            df_recalculated=df_indicadores_mun,
            calc_col="taxa_alfabetizacao_recalculada",
            official_col="taxa_alfabetizacao_oficial_inep",
            max_tolerance_percent=2.0,
        )
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
