"""
===============================================================================
JOB AWS GLUE - CAMADA SILVER (src/scripts/quality_silver_layer.py)
===============================================================================
Descrição: Processamento, limpeza, deduplicação por janela, raio-X com análise
de duplicidades em todas as 7 tabelas e O GRANDE JOIN relacional.
===============================================================================
"""

import sys
import os
import json
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
import boto3

# Importando a biblioteca de qualidade enviada via --extra-py-files
from quality_lib import (
    check_join_ratio,
    check_null_threshold,
    check_uniqueness,
    deduplicate_by_window
)

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

print("=" * 75)
print(f"🚀 INICIANDO CAMADA SILVER - EDA COMPLETA, DEDUPLICAÇÃO E JOINs")
print(f"   Bucket: {S3_BUCKET}")
print(f"   Limiar de Join: {JOIN_THRESHOLD}% | Trava Bloqueante: {BLOCK_ON_FAILURE}")
print("=" * 75)

# 1. Carregamento das Tabelas da Bronze
print("📥 1. Carregando tabelas da Camada Bronze...")

tables = {}
table_names = [
    "alunos", "uf", "municipio", 
    "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", 
    "meta_alfabetizacao_municipio", "dicionario"
]

for name in table_names:
    try:
        tables[name] = spark.read.parquet(f"{BRONZE_PATH}{name}/")
        print(f" ├─ [{name}] Carregado com sucesso de: {BRONZE_PATH}{name}/")
    except Exception as e:
        print(f" ⚠️ Erro ao carregar [{name}]: {str(e)}")

# Chaves de negócio para diagnóstico de duplicidades por tabela
TABLE_KEYS = {
    "alunos": ["id_aluno", "id_municipio", "ano", "rede", "serie"],
    "municipio": ["id_municipio", "ano", "rede", "serie"],
    "uf": ["sigla_uf", "ano", "rede", "serie"],
    "meta_alfabetizacao_brasil": ["ano", "rede"],
    "meta_alfabetizacao_uf": ["sigla_uf", "ano", "rede"],
    "meta_alfabetizacao_municipio": ["id_municipio", "ano", "rede"],
    "dicionario": ["id_tabela", "nome_coluna", "chave"]
}

# 2. Raio-X e Análise Exploratória (EDA) com Diagnóstico de Duplicidades em TODAS as Tabelas
print("\n" + "#" * 75)
print("🔬 INICIANDO ETAPA DE RAIO-X E ANÁLISE EXPLORATÓRIA (EDA INICIAL)")
print("#" * 75)

for name, df in tables.items():
    print("\n" + "=" * 75)
    print(f"🔬 [RAIO-X / EDA INICIAL] Tabela: '{name.upper()}'")
    print("=" * 75)
    
    total_rows = df.count()
    total_cols = len(df.columns)
    print(f"📊 Volume Total: {total_rows:,} registros | {total_cols} colunas")
    
    # 2.1 Schema / Tipos
    print("\n📋 Schema / Estrutura de Tipos:")
    for field in df.schema.fields:
        print(f"   ├─ {field.name:<40} : {field.dataType.simpleString()}")
    
    # 2.2 Preview das 3 primeiras linhas (Tabela Markdown)
    print("\n👀 Preview das 3 primeiras linhas (Tabela Markdown):")
    cols = df.columns
    try:
        sample_rows = df.limit(3).collect()
        header_line = "| " + " | ".join(cols) + " |"
        sep_line = "| " + " | ".join(["---"] * len(cols)) + " |"
        print(header_line)
        print(sep_line)
        for row in sample_rows:
            vals = [str(row[c]) if row[c] is not None else "NULL" for c in cols]
            print("| " + " | ".join(vals) + " |")
    except Exception as prev_err:
        print(f"   ⚠️ Não foi possível gerar preview: {prev_err}")
        
    # 2.3 Diagnóstico de NULOs por Coluna
    print("\n⚠️ Diagnóstico de NULOs por Coluna:")
    for c in cols:
        null_cnt = df.filter(F.col(c).isNull()).count()
        null_pct = (null_cnt / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
        print(f"   ├─ {c:<40} : {null_cnt:,} nulos ({null_pct:.2f}%)")
        
    # 2.4 Diagnóstico de Duplicidades (Exigência do Usuário para TODAS as 7 Tabelas)
    print("\n🔄 Diagnóstico de Duplicidades:")
    # Duplicidade exata (linha inteira idêntica)
    distinct_rows = df.distinct().count()
    exact_duplicates = total_rows - distinct_rows
    exact_dup_pct = (exact_duplicates / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
    print(f"   ├─ Duplicatas Exatas (Linha Inteira) : {exact_duplicates:,} ({exact_dup_pct:.2f}%)")
    
    # Duplicidade por Chave Negocial
    keys = TABLE_KEYS.get(name, [])
    valid_keys = [k for k in keys if k in cols]
    if valid_keys:
        distinct_keys = df.select(valid_keys).distinct().count()
        key_duplicates = total_rows - distinct_keys
        key_dup_pct = (key_duplicates / float(total_rows)) * 100.0 if total_rows > 0 else 0.0
        print(f"   ├─ Duplicatas por Chave Negocial {valid_keys}: {key_duplicates:,} ({key_dup_pct:.2f}%)")
    else:
        print(f"   ├─ Duplicatas por Chave Negocial : N/A (Chaves não presentes)")
    print("=" * 75)

# 3. Deduplicação Ativa por Janela na Tabela de Alunos
if "alunos" in tables:
    print("\n🧹 3. Aplicando Deduplicação Ativa por Janela na Tabela de Alunos...")
    df_alunos_raw = tables["alunos"]
    df_alunos_dedup = deduplicate_by_window(
        df=df_alunos_raw,
        key_columns=TABLE_KEYS["alunos"],
        order_by_cols=["_ingested_at", "proficiencia"],
        ascending=False
    )
    tables["alunos"] = df_alunos_dedup

# 4. Limpeza, Padronização e Eliminação de Colisão de Métricas
print("\n🧹 4. Executando Limpeza, Padronização e Eliminação de Colisão de Métricas...")

df_alunos = tables["alunos"]
df_municipio = tables["municipio"]
df_uf = tables["uf"]
df_meta_mun = tables["meta_alfabetizacao_municipio"]

# Derivação da coluna sigla_uf na tabela de alunos via código IBGE
if "sigla_uf" not in df_alunos.columns and "id_municipio" in df_alunos.columns:
    df_alunos = df_alunos.withColumn("sigla_uf_derived", F.substring(F.col("id_municipio"), 1, 2))
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
    df_alunos = df_alunos.withColumn("sigla_uf", ibge_uf_map[F.col("sigla_uf_derived")]).drop("sigla_uf_derived")
    print("  └─ 🗺️ [Derivação UF] Coluna 'sigla_uf' criada em 'alunos' via código IBGE.")

# Metadados da Bronze a remover das dimensões para evitar duplicação no Parquet final
meta_cols_to_drop = ["_ingested_at", "_source", "ref_ano", "ref_mes", "ref_dia"]

# Preparar Dimensão Município
for m_col in meta_cols_to_drop:
    if m_col in df_municipio.columns:
        df_municipio = df_municipio.drop(m_col)

mun_cols_rename = {
    "taxa_alfabetizacao": "taxa_alfabetizacao_municipio",
    "media_portugues": "media_portugues_municipio"
}
for i in range(9):
    mun_cols_rename[f"proporcao_aluno_nivel_{i}"] = f"proporcao_aluno_nivel_{i}_municipio"

for old_c, new_c in mun_cols_rename.items():
    if old_c in df_municipio.columns:
        df_municipio = df_municipio.withColumnRenamed(old_c, new_c)

# Preparar Dimensão UF
for m_col in meta_cols_to_drop:
    if m_col in df_uf.columns:
        df_uf = df_uf.drop(m_col)

uf_cols_rename = {
    "taxa_alfabetizacao": "taxa_alfabetizacao_uf",
    "media_portugues": "media_portugues_uf"
}
for i in range(9):
    uf_cols_rename[f"proporcao_aluno_nivel_{i}"] = f"proporcao_aluno_nivel_{i}_uf"

for old_c, new_c in uf_cols_rename.items():
    if old_c in df_uf.columns:
        df_uf = df_uf.withColumnRenamed(old_c, new_c)

# Preparar Dimensão Metas Município
for m_col in meta_cols_to_drop:
    if m_col in df_meta_mun.columns:
        df_meta_mun = df_meta_mun.drop(m_col)

meta_cols_rename = {
    "taxa_alfabetizacao": "taxa_alfabetizacao_meta_mun",
    "percentual_participacao": "percentual_participacao_meta_mun",
    "nivel_alfabetizacao": "nivel_alfabetizacao_meta_mun"
}
for old_c, new_c in meta_cols_rename.items():
    if old_c in df_meta_mun.columns:
        df_meta_mun = df_meta_mun.withColumnRenamed(old_c, new_c)

# 5. Executando O GRANDE JOIN Relacional
print("\n🔗 5. Executando O GRANDE JOIN Relacional (Sem Explosão nem Ambiguidade)...")

# JOIN 1: Alunos + Municipio
df_joined = df_alunos.join(
    df_municipio,
    on=["id_municipio", "ano", "rede", "serie"],
    how="left"
)

# JOIN 2: + UF
df_joined = df_joined.join(
    df_uf,
    on=["sigla_uf", "ano", "rede", "serie"],
    how="left"
)

# JOIN 3: + Meta Municipio
df_joined = df_joined.join(
    df_meta_mun,
    on=["id_municipio", "ano", "rede"],
    how="left"
)

# 6. Validando Gate de Qualidade
print("\n🛡️ 6. Validando Gate de Qualidade (check_join_ratio)...")
match_ratio, metrics = check_join_ratio(
    df_left=df_alunos,
    df_joined=df_joined,
    match_key_col="id_municipio",
    threshold_percent=JOIN_THRESHOLD,
    raise_on_failure=BLOCK_ON_FAILURE
)

# Tripla Categorização do Status da Meta
if "meta_alfabetizacao_2024" in df_joined.columns and "taxa_alfabetizacao_municipio" in df_joined.columns:
    df_joined = df_joined.withColumn(
        "status_meta_2024",
        F.when(F.col("meta_alfabetizacao_2024").isNull(), F.lit("Sem Meta Cadastrada"))
         .when(F.col("taxa_alfabetizacao_municipio") >= F.col("meta_alfabetizacao_2024"), F.lit("Atingiu a Meta"))
         .otherwise(F.lit("Não Atingiu a Meta"))
    )

# Timestamp de enriquecimento da camada Silver
df_joined = df_joined.withColumn("_processed_silver_at", F.current_timestamp())

# 7. Gravação no S3
output_path = f"{SILVER_PATH}alunos_curado/"
print(f"\n💾 7. Gravando dataset curado em: {output_path}")

df_joined.write \
    .mode("overwrite") \
    .parquet(output_path)

print("  └─ ✅ Dataset curado gravado na Camada Silver com sucesso!")

# 8. Salvando Manifesto da Camada Silver
try:
    s3_client = boto3.client("s3")
    manifest_data = {
        "job_name": args['JOB_NAME'],
        "status": "SUCCEEDED",
        "input_rows": int(metrics.get("total_left", 0)),
        "output_rows": int(metrics.get("total_matched", 0)),
        "match_ratio": float(metrics.get("match_ratio", 0.0)),
        "gate_status": "APPROVED" if match_ratio >= JOIN_THRESHOLD else "FAILED",
        "s3_path": output_path
    }
    
    run_id = os.getenv("GLUE_RUN_ID", "local_run")
    manifest_key = f"auditoria/silver/ingestion_date=2026-10-29/run_id={run_id}/manifest.json"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest_data, indent=2),
        ContentType="application/json"
    )
    print(f"📜 [Auditoria Silver] Manifesto salvo no S3 com sucesso: s3://{S3_BUCKET}/{manifest_key}")
except Exception as manifest_err:
    print(f"⚠️ Alerta ao salvar manifesto Silver: {manifest_err}")

print("\n" + "=" * 75)
print("✅ CAMADA SILVER CONCLUÍDA COM SUCESSO DE PONTA A PONTA!")
print("=" * 75)
