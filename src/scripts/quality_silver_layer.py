"""
===============================================================================
JOB AWS GLUE - CAMADA SILVER (src/scripts/quality_silver_layer.py)
===============================================================================
Descrição: Processamento, limpeza, enriquecimento e O GRANDE JOIN das 6
entidades educacionais, aplicando a trava de qualidade bloqueante (Taxa de Join).
===============================================================================
"""

import sys
import os
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F

# Importando a biblioteca de qualidade enviada via --extra-py-files
from quality_lib import check_join_ratio, check_null_threshold

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

JOIN_THRESHOLD = float(os.getenv("JOIN_MATCH_THRESHOLD", "95.0"))
BLOCK_ON_FAILURE = os.getenv("BLOCK_ON_QUALITY_FAILURE", "true").lower() == "true"

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
QUARANTINE_PATH = f"s3://{S3_BUCKET}/silver/_quarantine/"

print("=" * 75)
print(f"🚀 INICIANDO PROCESSAMENTO DA CAMADA SILVER")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Limiar de Join: {JOIN_THRESHOLD}% | Trava Bloqueante: {BLOCK_ON_FAILURE}")
print("=" * 75)

# 1. Leitura das entidades da Bronze
print("📥 Lendo dados particionados da Camada Bronze...")

try:
    df_uf = spark.read.parquet(f"{BRONZE_PATH}uf/")
    df_municipio = spark.read.parquet(f"{BRONZE_PATH}municipio/")
    df_meta_br = spark.read.parquet(f"{BRONZE_PATH}meta_alfabetizacao_brasil/")
    df_meta_uf = spark.read.parquet(f"{BRONZE_PATH}meta_alfabetizacao_uf/")
    df_meta_mun = spark.read.parquet(f"{BRONZE_PATH}meta_alfabetizacao_municipio/")
    
    # Tabela Alunos (opcional caso esteja em teste de dev)
    has_alunos = False
    try:
        df_alunos = spark.read.parquet(f"{BRONZE_PATH}alunos/")
        has_alunos = True
    except Exception:
        print("  └─ ℹ️ Tabela 'alunos' não encontrada na Bronze (Execução em modo de teste/metas).")

except Exception as e:
    raise RuntimeError(f"❌ Falha ao carregar tabelas da Camada Bronze em '{BRONZE_PATH}': {str(e)}")

# 2. Execução do Grande Join e Tratamentos
print("🔄 Executando transformações, limpezas e junções relacionais...")

if has_alunos:
    df_joined = df_alunos \
        .join(df_municipio, on="id_municipio", how="inner") \
        .join(df_uf, on="sigla_uf", how="inner") \
        .join(df_meta_mun, on="id_municipio", how="left")
    
    # Validação de Qualidade Bloqueante (Taxa de Join)
    check_join_ratio(df_alunos, df_joined, threshold_percent=JOIN_THRESHOLD, raise_on_failure=BLOCK_ON_FAILURE)
    check_null_threshold(df_joined, columns=["id_municipio", "sigla_uf"], max_null_percent=5.0)
    df_silver_final = df_joined
else:
    # Junção das tabelas de dimensão e metas
    df_silver_final = df_meta_mun \
        .join(df_municipio, on="id_municipio", how="inner") \
        .join(df_uf, on="sigla_uf", how="inner")

# Adiciona timestamp de enriquecimento da camada Silver
df_silver_final = df_silver_final.withColumn("_processed_silver_at", F.current_timestamp())

# 3. Gravação na Camada Silver em formato Parquet
output_path = f"{SILVER_PATH}integracao_alfabetizacao/"
print(f"💾 Escrevendo dataset consolidado em: {output_path}")

df_silver_final.write \
    .mode("overwrite") \
    .parquet(output_path)

print("=" * 75)
print("✅ Processamento da Camada Silver concluído com sucesso!")
print("=" * 75)
