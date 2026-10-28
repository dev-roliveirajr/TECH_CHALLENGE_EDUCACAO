"""
===============================================================================
JOB AWS GLUE - CAMADA GOLD (src/scripts/aggregate_gold_layer.py)
===============================================================================
Descrição: Consolidação dos indicadores educacionais (Metas vs. Resultados) por
Município e UF, e geração da tabela analítica / Feature Store para Machine Learning.
===============================================================================
"""

import sys
import os
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Obter parâmetros de execução passados pelo Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
if not S3_BUCKET:
    try:
        args_s3 = getResolvedOptions(sys.argv, ['S3_BUCKET_NAME'])
        S3_BUCKET = args_s3['S3_BUCKET_NAME']
    except Exception:
        pass

if not S3_BUCKET:
    raise ValueError("❌ A variável/argumento S3_BUCKET_NAME não foi definida!")

SILVER_PATH = f"s3://{S3_BUCKET}/silver/integracao_alfabetizacao/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"

print("=" * 75)
print(f"🚀 INICIANDO CONSOLIDAÇÃO DA CAMADA GOLD")
print(f"   Bucket: {S3_BUCKET}")
print("=" * 75)

# 1. Leitura da Camada Silver
print(f"📥 Lendo dados da Camada Silver em: {SILVER_PATH}")
df_silver = spark.read.parquet(SILVER_PATH)

# 2. Agregação de Indicadores por Município e UF
print("📊 Calculando agregação de indicadores por Município e UF...")

# Seleciona e calcula os agregados disponíveis
colunas_disponiveis = df_silver.columns

if "taxa_alfabetizacao" in colunas_disponiveis and "meta_alfabetizacao" in colunas_disponiveis:
    df_gold_indicadores = df_silver.groupBy("sigla_uf", "id_municipio", "nome_municipio") \
        .agg(
            F.avg("taxa_alfabetizacao").alias("media_alfabetizacao_real"),
            F.first("meta_alfabetizacao").alias("meta_estipulada")
        ) \
        .withColumn("atingiu_meta", F.when(F.col("media_alfabetizacao_real") >= F.col("meta_estipulada"), 1).otherwise(0))
else:
    # Agregação genérica por localização e agrupamento de metas
    df_gold_indicadores = df_silver.groupBy("sigla_uf", "id_municipio") \
        .agg(
            F.count("*").alias("total_registros"),
            F.max("_processed_silver_at").alias("ultima_atualizacao")
        )

# Adiciona timestamp da camada Gold
df_gold_indicadores = df_gold_indicadores.withColumn("_created_gold_at", F.current_timestamp())

# 3. Gravação dos Datasets Analíticos no S3 Gold
output_path = f"{GOLD_PATH}indicadores_municipio_uf/"
print(f"💾 Escrevendo dados analíticos da Camada Gold em: {output_path}")

df_gold_indicadores.write \
    .mode("overwrite") \
    .parquet(output_path)

print("=" * 75)
print("✅ Consolidação da Camada Gold concluída com sucesso!")
print("=" * 75)
