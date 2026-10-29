# 📂 Datasets Analíticos e Feature Store — Camada Gold

Esta pasta armazena os datasets consolidados da **Camada Gold** gerados pelo pipeline de ETL do Data Lake Medalhão (**Tech Challenge - Fase 2**).

Os dados estão persistidos em arquivo único no formato **Apache Parquet (Snappy)**, otimizados para leitura em Pandas, PySpark, Polars ou DuckDB.

---

## 📊 Datasets Disponíveis

### 1. `gold_features_alfabetizacao.parquet`
* **Finalidade**: Matriz de características (*Feature Store*) para o treinamento dos modelos preditivos de Machine Learning na **Fase 3**.
* **Nível de Agregação**: Ente federativo / Município.
* **Governança**: Estrutura imune a vazamento de alvo (*target leakage*).

### 2. `gold_indicadores_municipais.parquet`
* **Finalidade**: Tabela de indicadores educacionais recalculados a partir dos microdados individuais dos estudantes do Saeb.
* **Métrica Principal**: Taxa de alfabetização ponderada pelo peso amostral (`peso_aluno`).

### 3. `gold_desempenho_vs_metas.parquet`
* **Finalidade**: Análise comparativa da taxa apurada contra a **Meta Pactuada Oficial da Fonte** (trajetória até 80,0% em 2030).
* **Categorização**: `Atingiu a Meta`, `Não Atingiu a Meta` e `Sem Meta Cadastrada`.

---

## 💻 Exemplo de Consumo no Python

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
