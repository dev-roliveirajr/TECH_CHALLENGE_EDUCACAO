# 📂 Datasets Analíticos e Feature Store — Camada Gold

Esta pasta armazena os conjuntos de dados consolidados da **Camada Gold**.

Os dados estão persistidos em arquivo único no formato **Apache Parquet (Snappy)**, otimizados para leitura em Pandas, PySpark, Polars ou DuckDB.

---

##  Datasets Disponíveis

### Indicadores
* `gold_indicadores_municipais.parquet`: indicadores por município, rede, ano e série.
* `gold_indicadores_uf.parquet`: indicadores por UF, rede, ano e série.
* `gold_indicadores_brasil.parquet`: indicadores nacionais por rede, ano e série.

As taxas são calculadas diretamente dos microdados com ponderação por
`peso_aluno`. Os indicadores incluem todas as redes disponíveis.

### Feature Stores por nível geográfico
* `gold_features_alfabetizacao.parquet`: features e rótulo-alvo municipais.
* `gold_features_alfabetizacao_uf.parquet`: features e rótulo-alvo estaduais.
* `gold_features_alfabetizacao_brasil.parquet`: features e rótulo-alvo nacionais.

### Comparativos com a meta oficial
* `gold_desempenho_vs_metas.parquet`: municípios, rede `"3"` (Municipal).
* `gold_desempenho_vs_metas_uf.parquet`: UFs, rede `"5"` (Pública estadual e municipal).
* `gold_desempenho_vs_metas_brasil.parquet`: Brasil, rede `"5"`.

Os comparativos contêm `gap_para_meta` e `status_meta`, ambos relativos à meta
oficial de 80%. Não há meta aspiracional.

---

##  Exemplo de Consumo no Python

### Usando Pandas (Ideal para Machine Learning)
```python
import pandas as pd

# Carregar a Feature Store de ML
df_features = pd.read_parquet("data/features/gold_features_alfabetizacao.parquet")
print(f"Volume: {df_features.shape[0]:,} linhas x {df_features.shape[1]} colunas")
df_features.head()
```

### Usando PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnaliseGold").getOrCreate()
df_features = spark.read.parquet("data/features/gold_features_alfabetizacao.parquet")
df_features.printSchema()
```
