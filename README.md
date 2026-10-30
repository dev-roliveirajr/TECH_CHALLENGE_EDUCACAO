### Data Lake Medalhão — Compromisso Nacional Criança Alfabetizada
#### Solução de Engenharia de Dados & Pipeline Híbrido (Tech Challenge — Fase 2)

Este repositório contém a implementação completa do pipeline de dados do **Tech Challenge (Fase 2)** da Pós-Graduação em Inteligência Artificial e Ciência de Dados (FIAP / POSTECH). A solução consolida um **Data Lakehouse Medalhão Multi-cloud** (GCP ➔ AWS) focado no processamento de microdados públicos da avaliação de alfabetização do **Saeb/Inep** (corte de **743 pontos** de proficiência na Pesquisa Alfabetiza Brasil) e dos indicadores do **Compromisso Nacional Criança Alfabetizada**.

---

#### 1. Contexto e Problema de Negócio

O **Compromisso Nacional Criança Alfabetizada** estabelece a meta de garantir que 100% das crianças brasileiras estejam alfabetizadas ao final do 2º ano do Ensino Fundamental. A avaliação oficial do **Saeb/Inep** gera milhões de registros de microdados contendo informações individuais de alunos, cadernos de prova, escolas e redes de ensino.

##### Desafios de Engenharia de Dados Tratados nesta Pipeline:
1. **Escala e Heterogeneidade**: Processamento distribuído de mais de **3,86 milhões de microdados de estudantes** e cruzamento com dimensões estaduais e municipais.
2. **Eficiência de Processamento (Lakehouse / Upsert)**: Processamento incremental cirúrgico por manifesto na Camada Silver com suporte a transações ACID via **Delta Lake** e operação **MERGE INTO (Upsert)**, evitando recargas totais (*Full Scans*).
3. **Distinção de Metas (Pactuada 80% vs. Aspiracional 100%)**: Aplicação rigorosa da **Meta Pactuada Oficial da Fonte (80,0% até 2030 para a Rede Pública)** para o cálculo do GAP de desempenho, diferenciando-a do **Cenário Aspiracional de Universalização (100,0%)**, conforme orientação do edital.
4. **Ponderação Estatística do Saeb**: Impedir a armadilha da *média simples*, aplicando rigorosamente o **fator de expansão amostral (`peso_aluno`)** nas agregadas oficiais da Camada Gold.
5. **Preservação Estatística de NULOs**: Garantir que estudantes ausentes ou com prova não preenchida mantenham o estado NULL, evitando a redução artificial das médias de proficiência das escolas.
6. **Governança Multi-cloud e FinOps**: Ingestão segura sem escrita em disco local na origem e monitoramento em tempo real dos custos de computação no AWS Glue.

---

#### 2. Arquitetura do Data Lake Medalhão

A solução adota o padrão **Medallion Architecture**, dividindo o ciclo de vida dos dados em três camadas progressivas de qualidade e valor analítico, com **governança 100% orientada a manifestos JSON de auditoria**.

```text
+-----------------------------------------------------------------------------------+
|                        CAMADA BRONZE (RAW / INGESTÃO)                             |
|  * Origem: GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)            |
|  * Processamento: AWS Glue (pyspark-bigquery-connector em memória)               |
|  * Destino: s3://tc-alfabetizacao-.../bronze/ (Parquet / Snappy)                  |
|  * Manifesto: auditoria/bronze/ingestion_date=.../manifest.json                   |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura cirúrgica via Manifesto JSON
                                          v
+-----------------------------------------------------------------------------------+
|                  CAMADA SILVER (CURATED / DELTA LAKE LAKEHOUSE)                   |
|  * Formato: Delta Lake com Transações ACID                                        |
|  * Processamento: PySpark / AWS Glue 4.0 (--datalake-formats delta)               |
|  * Ingestão Incremental: Leitura restrita aos caminhos gravados no manifesto      |
|  * Operação: MERGE INTO (Upsert por chave id_aluno + ano)                         |
|  * Deduplicação por Janela: Window.partitionBy("id_aluno", "ano")                 |
|  * O Grande JOIN: Junção por chaves compostas (id_municipio, ano, rede, serie)    |
|  * Gate de Qualidade: check_join_ratio >= 95.0% (Bloqueante)                      |
|  * Destino: s3://tc-alfabetizacao-.../silver/alunos_curado/                        |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura guiada por Manifesto JSON
                                          v
+-----------------------------------------------------------------------------------+
|                        CAMADA GOLD (ANALYTICS & FEATURE STORE)                    |
|  * Tabela 1: gold_indicadores_municipais (Média ponderada pelo peso_aluno)        |
|  * Tabela 2: gold_desempenho_vs_metas (GAP contra Meta Pactuada de 80% em 2030)    |
|  * Tabela 3: gold_features_alfabetizacao (Matriz para ML sem Target Leakage)      |
|  * Validação Cruzada: Batimento entre microdados e taxa publicada Inep (0,036%)   |
|  * Exportação Local: Sincronização automática para data/features/*.parquet        |
+-----------------------------------------------------------------------------------+
```

---

#### 3. Matriz FinOps e Custos Medidos em Nuvem

Cada execução do pipeline aciona o gerador automático de observabilidade (`src/monitor/report_generator.py`), registrando as métricas reais de tempo, DPUs consumidas e custo estimado da infraestrutura serverless do **AWS Glue** (baseado em US$ 0,44 por DPU-hora).

| Camada / Job Glue | Registros Processados | Duração Medida | DPUs (Type G.1X) | Consumo (DPU-h) | Custo Estimado (USD) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **01_bronze_ingestion** | 3.867.999 alunos + 6 dimensões | ~185s (~3,08 min) | 2,0 DPUs | 0,1028 DPU-h | US$ 0,0452 |
| **02_silver_processing** | 3.867.999 alunos (Delta Lake MERGE) | ~220s (~3,67 min) | 2,0 DPUs | 0,1222 DPU-h | US$ 0,0538 |
| **03_gold_aggregation** | 12.417 agregações analíticas | ~160s (~2,67 min) | 2,0 DPUs | 0,0889 DPU-h | US$ 0,0391 |
| **TOTAL DO PIPELINE** | **3,86 Milhões de Linhas** | **~9,42 Minutos** | **2,0 DPUs** | **0,3139 DPU-h** | **US$ 0,1381** |

---

#### 4. Decisões Arquiteturais e Trade-offs de Engenharia

| Decisão de Engenharia | Estratégia Adotada | Justificativa e Impacto |
| :--- | :--- | :--- |
| **Acesso à Origem** | GCP BigQuery ➔ AWS S3 via Connector | Elimina a necessidade de download de arquivos pesados em disco local. O conector PySpark carrega as tabelas em memória RAM, garantindo alta velocidade de transferência. |
| **Governança por Manifesto** | `manifest.json` em cada camada | Impede varreduras cegas por *wildcards* no S3. O Job da camada seguinte lê o arquivo de manifesto para identificar cirurgicamente quais partições foram validadas. |
| **Tabelas ACID na Silver** | Delta Lake + `MERGE INTO` (Upsert) | Elimina a necessidade de reprocessar o histórico antigo da Bronze. O Spark faz a correspondência por chave primária (`id_aluno` + `ano`), inserindo novos registros e atualizando alterações com suporte a transações ACID. |
| **Deduplicação Determinística** | `Window.partitionBy` e `row_number()` | Garante a remoção de duplicidades operacionais no micro-lote antes da mesclagem, mantendo o registro mais completo ou mais recente sem perda de cardinalidade. |
| **Ponderação do Saeb** | $\\frac{\\sum (\\text{peso\\_aluno} \\times \\text{alfabetizado})}{\\sum \\text{peso\\_aluno}}$ | Atende estritamente à metodologia do Inep/Saeb. Evita a distorção da média simples, onde municípios pequenos teriam o mesmo peso que metrópoles. |
| **Meta Pactuada Oficial** | Trajetória oficial (80,0% até 2030) | Permite avaliar o desempenho da gestão pública contra o compromisso formal firmado com o MEC, aplicando a tripla categorização. |

---

#### 5. Estrutura do Repositório e Guia de Execução

##### Estrutura de Diretórios
```text
TECH_CHALLENGE_EDUCACAO/
├── data/
│   └── features/                      <-- Arquivos Parquet consolidados para ML
├── docs/                              <-- Especificações técnicas completas
│   ├── ingestion_bronze_layer.md
│   ├── quality_silver_layer.md
│   └── aggregate_gold_layer.md
├── notebooks/                         <-- Jupyter Notebooks de Análise Exploratória (EDA Plus)
│   ├── README.md                      <-- Guia de execução dos notebooks
│   └── eda_analytics_fase2.ipynb      <-- Análise visual e diagnóstico das metas Gold
├── reports/                           <-- Relatórios Markdown de FinOps e Observabilidade
├── src/                               <-- Código-fonte
│   ├── common/                        <-- Biblioteca de Qualidade (quality_lib.py)
│   ├── infra/                         <-- Script de Deploy do Glue (deploy_aws_scripts.py)
│   ├── monitor/                       <-- Gerador de Relatórios (report_generator.py)
│   ├── scripts/                       <-- Jobs AWS Glue (Bronze, Silver, Gold)
│   └── pipeline.py                    <-- Orquestrador CLI Principal
├── tests/                             <-- Testes unitários com pytest
├── .env_example                       <-- Modelo de variáveis de ambiente
├── Makefile                           <-- Comandos automatizados do projeto
└── requirements.txt                   <-- Dependências Python
```

##### Instruções de Execução (Passo a Passo)

1. **Configurar o Ambiente Virtual e Variáveis**:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   cp .env_example .env
   ```

2. **Fazer o Deploy dos Scripts para o AWS Glue**:
   ```bash
   python src/infra/deploy_aws_scripts.py
   ```

3. **Executar a Pipeline Completa (Bronze ➔ Silver ➔ Gold ➔ Sincronização Local)**:
   ```bash
   python src/pipeline.py
   ```

4. **Explorar o Jupyter Notebook Analítico (Plus / Visualizações)**:
   ```bash
   jupyter notebook notebooks/eda_analytics_fase2.ipynb
   ```

---

#### 6. Preparação para Inteligência Artificial (Fase 3) & Notebook EDA

A Camada Gold gera a **Feature Store** no formato Parquet (`data/features/gold_features_alfabetizacao.parquet`) contendo as variáveis preditivas para os modelos da próxima fase.

##### Mitigação de Target Leakage (Vazamento de Dados):
* **Variáveis Preditivas ($X$)**: Indicadores demográficos, porte do município, rede de ensino, série, média de proficiência histórica e peso amostral.
* **Variável Alvo ($y$)**: `alvo_atingiu_meta` (Binária: $1$ se atingiu a meta pactuada e $0$ caso contrário).
* **Isolamento**: Nenhuma coluna calculada a partir da meta do próprio ano letivo é incluída nas variáveis explicativas $X$.

##### Notebook Analítico Interativo (`notebooks/eda_analytics_fase2.ipynb`):
Como **entregável adicional (Plus)**, o projeto acompanha um notebook contendo gráficos de alta qualidade para apresentação executiva:
* **Gráfico de Rosca**: Distribuição dos municípios em relação à meta de 80%.
* **Ranking por UF**: Desempenho dos 27 estados confrontado com as linhas de 80% (Pactuada) e 100% (Aspiracional).
* **Heatmap de Correlação**: Matriz de associação das variáveis da Feature Store.

---

#### 7. Limitações Conhecidas e Melhorias Futuras (Roadmap)

1. **Rotina de Limpeza Delta (VACUUM)**: Como o Delta Lake mantém o histórico de transações ACID para *Time Travel*, o agendamento de uma rotina periódica de `VACUUM` (retenção de 7 dias) está mapeado no roadmap FinOps para apagar arquivos legados do S3.
2. **Latência Batch**: A esteira atual opera em processamento em lote (Batch). Uma simulação de ingestão em micro-lotes / streaming será adicionada para cenários de eventos em tempo quase real.
3. **Dependência de Credenciais do GCP**: A leitura da origem requer credenciais válidas do BigQuery / Base dos Dados salvas no AWS Secrets Manager.
