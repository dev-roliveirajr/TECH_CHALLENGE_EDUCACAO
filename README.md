# Data Lake Medalhão Híbrido (Batch & Streaming) — Compromisso Nacional Criança Alfabetizada
## Solução de Engenharia de Dados & Pipeline Serverless (Tech Challenge — Fase 2)

Este repositório contém a implementação completa da arquitetura de dados do **Tech Challenge (Fase 2)** da Pós-Graduação em Inteligência Artificial e Ciência de Dados (FIAP / POSTECH). A solução consolida um **Data Lake Medalhão Multi-cloud e Híbrido** (GCP ➔ AWS), integrando o processamento em lote (*Batch*) de mais de 3,86 milhões de microdados públicos da avaliação de alfabetização do **Saeb/Inep** com a ingestão contínua em tempo quase real (*Streaming/Event-driven*) do **Compromisso Nacional Criança Alfabetizada**.

---

## 1. Contexto e Problema de Negócio

O **Compromisso Nacional Criança Alfabetizada** estabelece a meta de garantir que 100% das crianças brasileiras estejam alfabetizadas ao final do 2º ano do Ensino Fundamental. A avaliação oficial do **Saeb/Inep** gera milhões de registros de microdados contendo informações individuais de alunos, cadernos de prova, escolas e redes de ensino.

### Desafios de Engenharia de Dados Tratados nesta Pipeline:
1. **Escala e Heterogeneidade Híbrida**: Processamento distribuído de mais de **3,86 milhões de microdados de estudantes** (Batch) e suporte a ingestão incremental de micro-lotes em tempo quase real (Streaming).
2. **Upsert Idempotente no Delta Lake**: Ingestão contínua de eventos com *MERGE INTO* tolerante a falhas na camada Silver, preservando colunas calculadas e partições de data (`ref_ano`, `ref_mes`, `ref_dia`).
3. **Ponderação Estatística do Saeb**: Impedir a armadilha da *média simples*, aplicando rigorosamente o **fator de expansão amostral (`peso_aluno`)** nas agregações oficiais.
4. **Preservação Estatística de NULOs**: Garantir que estudantes ausentes ou com prova não preenchida mantenham o estado `NULL`, evitando a redução artificial das médias de proficiência das escolas.
5. **Governança Multi-cloud e FinOps**: Ingestão segura sem escrita em disco local na origem e monitoramento em tempo real dos custos de computação serverless no AWS Glue.

---

## 2. Arquitetura do Data Lake Medalhão Híbrido

A solução adota o padrão **Medallion Architecture Híbrida**, dividindo o ciclo de vida dos dados em três camadas progressivas de qualidade e valor analítico, apoiada por uma **Landing Zone de Eventos** e **governança 100% orientada a manifestos JSON de auditoria**.

```text
+-----------------------------------------------------------------------------------+
|                        CAMADA BRONZE (RAW / INGESTÃO BATCH)                       |
|  * Origem: GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)            |
|  * Processamento: AWS Glue (pyspark-bigquery-connector em memória)               |
|  * Destino: s3://tc-alfabetizacao-.../bronze/ (Parquet / Snappy)                  |
|  * Manifesto: auditoria/bronze/ingestion_date=.../manifest.json                   |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura guiada por Manifesto JSON
                                          v
+-----------------------------------------------------------------------------------+
|                        CAMADA SILVER (CURATED / CLEARED)                          |
|  * Processamento: PySpark / AWS Glue 4.0                                          |
|  * Regras: Snake_case, trimming, tradução de dicionários, derivação de UF (IBGE)  |
|  * Deduplicação por Janela: Window.partitionBy("id_aluno", "ano")                 |
|  * O Grande JOIN: Junção por chaves compostas (id_municipio, ano, rede, serie)    |
|  * Gate de Qualidade: check_join_ratio >= 95.0% (Bloqueante)                      |
|  * Destino: s3://tc-alfabetizacao-.../silver/alunos_curado/ (Delta Lake)          |
+-----------------------------------------------------------------------------------+
                                          ^
                                          |  MERGE INTO (Upsert Idempotente)
+-----------------------------------------+-----------------------------------------+
|                    BUFFER DE EVENTOS STREAMING (LANDING ZONE)                     |
|  * Origem: Geradores de Eventos NDJSON (Alunos + Repactuação de Metas)            |
|  * Landing: s3://tc-alfabetizacao-.../bronze/events_raw_landing/{alunos|meta_*}/    |
|  * Processador: AWS Glue Job 04_events_processor (recursiveFileLookup)            |
|  * Latência Real: Medição automatizada (ingested_at - event_time em ~48.7s min)   |
|  * Short-Circuit FinOps: Encerramento imediato em segundos caso não haja eventos  |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura do Snapshot Delta Lake
                                          v
+-----------------------------------------------------------------------------------+
|                        CAMADA GOLD (ANALYTICS & FEATURE STORE)                    |
|  * Tabela 1: gold_indicadores_municipais (Média ponderada pelo peso_aluno)        |
|  * Tabela 2: gold_desempenho_vs_metas (GAP contra Meta Pactuada de 80% em 2030)    |
|  * Tabela 3: gold_features_alfabetizacao (Matriz para ML sem Target Leakage)      |
|  * Validação Cruzada: Batimento entre microdados e taxa publicada Inep (0,037%)   |
|  * Exportação Local: Sincronização automática para data/features/*.parquet        |
+-----------------------------------------------------------------------------------+
```

---

## 3. Matriz FinOps e Custos Medidos em Nuvem

Cada execução do pipeline aciona o gerador automático de observabilidade (`src/monitor/report_generator.py`), registrando as métricas reais de tempo, DPUs consumidas e custo estimado da infraestrutura serverless do **AWS Glue** (baseado em US\$ 0,44 por DPU-hora).

### A. Pipeline Batch Principal (Carga Inicial)
| Camada / Job Glue | Registros Processados | Duração Medida | DPUs (Type G.1X) | Consumo (DPU-h) | Custo Estimado (USD) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **01_bronze_ingestion** | 3.867.999 alunos + 6 dimensões | ~185s (~3,08 min) | 2,0 DPUs | 0,1028 DPU-h | US\$ 0,0452 |
| **02_silver_processing** | 3.867.999 alunos curados | ~220s (~3,67 min) | 2,0 DPUs | 0,1222 DPU-h | US\$ 0,0538 |
| **03_gold_aggregation** | 12.417 agregações analíticas | ~160s (~2,67 min) | 2,0 DPUs | 0,0889 DPU-h | US\$ 0,0391 |
| **TOTAL BATCH** | **3,86 Milhões de Linhas** | **~9,42 Minutos** | **2,0 DPUs** | **0,3139 DPU-h** | **US\$ 0,1381** |

### B. Pipeline Streaming & Incremental (Eventos em Tempo Quase Real)
| Job Glue / Etapa | Eventos / Registros | Duração Medida | DPUs (Type G.1X) | Consumo (DPU-h) | Custo Estimado (USD) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **04_events_processor** | 240 eventos (200 alunos + 40 metas) | 191s (~3,18 min) | 2,0 DPUs | 0,1061 DPU-h | US\$ 0,0467 |
| **03_gold_aggregation** | 3.868.196 registros (+97 novos) | 107s (~1,78 min) | 2,0 DPUs | 0,0594 DPU-h | US\$ 0,0262 |
| **TOTAL STREAMING RUN** | **240 Eventos + Re-agregação** | **~4,96 Minutos** | **2,0 DPUs** | **0,1655 DPU-h** | **US\$ 0,0729** |

---

## 4. Decisões Arquiteturais e Trade-offs de Engenharia

| Decisão de Engenharia | Estratégia Adotada | Justificativa e Impacto |
| :--- | :--- | :--- |
| **Acesso à Origem** | GCP BigQuery ➔ AWS S3 via Connector | Elimina a necessidade de download de arquivos pesados em disco local. O conector PySpark carrega as tabelas em memória RAM, garantindo alta velocidade de transferência. |
| **Governança por Manifesto** | `manifest.json` em cada camada | Impede varreduras cegas por *wildcards* no S3. O Job da camada seguinte lê o arquivo de manifesto da camada anterior para identificar exatamente quais partições foram validadas. |
| **Ingestão Streaming & Upsert** | Delta Lake `MERGE INTO` com intersecção dinâmica | Garante que novos eventos de estudantes e repactuações de metas atualizem o snapshot Silver sem duplicar registros ou sobreescrever colunas calculadas. |
| **Varredura Recursiva S3** | `recursiveFileLookup = true` | Permite ao Spark ler arquivos NDJSON salvos em partições multinível (`ref_ano=YYYY/ref_mes=MM/ref_dia=DD/`) sem erros de máscara de caminho. |
| **Medição de Latência Real** | `ingested_at - event_time` | Registra no manifesto de auditoria a latência real de trânsito dos micro-lotes (medida em média de 2.734s / ~45 min nos testes). |
| **Short-Circuit FinOps** | Pré-checagem via Boto3/S3 | Caso o buffer de Landing Zone esteja vazio, o job de eventos encerra em milissegundos com status `SKIPPED`, evitando custos desnecessários de computação Spark. |
| **Ponderação do Saeb** | $\frac{\sum (\text{peso\_aluno} \times \text{alfabetizado})}{\sum \text{peso\_aluno}}$ | Atende estritamente à metodologia do Inep/Saeb. Evita a distorção da média simples, onde municípios pequenos teriam o mesmo peso que metrópoles. |
| **Validação Cruzada Automática** | Tolerância $\le 2,0\%$ (Medido: $0,037\%$) | Realiza o batimento em tempo real entre o microdado recalculado e a taxa oficial publicada pelo Inep, garantindo auditabilidade analítica. |

---

## 5. Estrutura do Repositório e Guia de Execução

### Estrutura de Diretórios
```text
TECH_CHALLENGE_EDUCACAO/
├── data/
│   ├── events_landing/                <-- Buffer local de testes para eventos NDJSON
│   └── features/                      <-- Arquivos Parquet consolidados para ML
├── docs/                              <-- Especificações técnicas detalhadas
│   ├── ingestion_bronze_layer.md
│   ├── quality_silver_layer.md
│   └── aggregate_gold_layer.md
├── reports/                           <-- Relatórios Markdown de FinOps e Observabilidade
├── src/                               <-- Código-fonte do Projeto
│   ├── common/                        <-- Biblioteca de Qualidade (quality_lib.py)
│   ├── generators/                    <-- Geradores de Eventos (Alunos e Metas)
│   │   ├── generate_alunos_events.py
│   │   └── generate_metas_events.py
│   ├── infra/                         <-- Script de Deploy Serverless (deploy_aws_scripts.py)
│   ├── monitor/                       <-- Gerador de Relatórios (report_generator.py)
│   ├── scripts/                       <-- AWS Glue Jobs (01_bronze, 02_silver, 03_gold, 04_events)
│   ├── pipeline.py                    <-- Orquestrador Batch Principal
│   └── pipeline_events.py             <-- Orquestrador Streaming / Event-Driven
├── tests/                             <-- Testes unitários com pytest
├── .env_example                       <-- Modelo de variáveis de ambiente
├── Makefile                           <-- Comandos de automação (make batch, make events)
└── requirements.txt                   <-- Dependências Python
```

### Instruções de Execução (Passo a Passo)

1. **Configurar o Ambiente Virtual e Variáveis**:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   cp .env_example .env
   ```

2. **Fazer o Deploy de Todos os Scripts para o AWS Glue**:
   ```bash
   python src/infra/deploy_aws_scripts.py
   ```

3. **Executar a Pipeline Batch Completa (Carga Inicial)**:
   ```bash
   python src/pipeline.py
   # ou via Makefile:
   make batch
   ```

4. **Gerar e Processar Eventos em Tempo Quase Real (Streaming)**:
   ```bash
   # 1. Emitir eventos sintéticos de alunos e repactuação de metas
   python src/generators/generate_alunos_events.py -n 100
   python src/generators/generate_metas_events.py -d municipio -n 20

   # 2. Executar o pipeline consumidor de eventos + re-agregação Gold
   python src/pipeline_events.py
   # ou via Makefile:
   make events
   ```

---

## 6. Preparação para Inteligência Artificial (Fase 3)

A Camada Gold gera a **Feature Store** no formato Parquet (`data/features/gold_features_alfabetizacao.parquet`) contendo as variáveis preditivas para os modelos da próxima fase.

### Mitigação de Target Leakage (Vazamento de Dados):
* **Variáveis Preditivas ($X$)**: Indicadores demográficos, porte do município, rede de ensino, série, média de proficiência histórica e peso amostral.
* **Variável Alvo ($y$)**: `alvo_atingiu_meta` (Binária: $1$ se atingiu a meta pactuada e $0$ caso contrário).
* **Isolamento**: Nenhuma coluna calculada a partir da meta do próprio ano letivo é incluída nas variáveis explicativas $X$.

---

## 7. Estado Final do Projeto e Próximos Passos

1. **Idempotência e Tolerância a Falhas**: Validada com sucesso no Delta Lake com a integração de 240 eventos de teste e re-agregação contínua da Gold.
2. **Validação Cruzada Mantida**: A divergência entre o microdado recalculado e a publicação oficial do Inep permaneceu em **0,037%**, comprovando a integridade estatística da esteira híbrida.
3. **Evolução Futura (Delta CDF)**: Para cenários de volumetria extrema (dezenas de milhões de eventos por hora), a arquitetura está pronta para receber o **Delta Change Data Feed (CDF)**, permitindo re-agregação Gold incremental restrita apenas aos municípios alterados no lote.
