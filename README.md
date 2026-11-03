# Data Lake Medalhão Híbrido (Batch & Streaming) — Compromisso Nacional Criança Alfabetizada
### Solução de Engenharia de Dados, FinOps & Observabilidade Serverless (Tech Challenge — Fase 2)

Este repositório contém a implementação completa da arquitetura de dados do **Tech Challenge (Fase 2)** da Pós-Graduação em Inteligência Artificial e Ciência de Dados (FIAP / POSTECH). A solução consolida um **Data Lake Medalhão Multi-cloud e Híbrido** (GCP ➔ AWS), integrando o processamento em lote (*Batch*) de mais de 3,86 milhões de microdados públicos da avaliação de alfabetização do **Saeb/Inep** com a ingestão contínua em tempo quase real (*Streaming/Event-driven*) do **Compromisso Nacional Criança Alfabetizada**.

---

### 1. Contexto e Problema de Negócio

O **Compromisso Nacional Criança Alfabetizada** estabelece a meta de garantir que 100% das crianças brasileiras estejam alfabetizadas ao final do 2º ano do Ensino Fundamental. A avaliação oficial do **Saeb/Inep** gera milhões de registros de microdados contendo informações individuais de alunos, cadernos de prova, escolas e redes de ensino.

#### Desafios de Engenharia de Dados Tratados nesta Pipeline:
1. **Escala e Heterogeneidade Híbrida**: Processamento distribuído de mais de **3,86 milhões de microdados de estudantes** (Batch) e suporte a ingestão incremental de micro-lotes em tempo quase real (Streaming).
2. **Upsert Idempotente no Delta Lake**: Ingestão contínua de eventos com *MERGE INTO* tolerante a falhas na camada Silver, preservando colunas calculadas e partições de data (`ref_ano`, `ref_mes`, `ref_dia`).
3. **Ponderação Estatística do Saeb**: Impedir a armadilha da *média simples*, aplicando rigorosamente o **fator de expansão amostral (`peso_aluno`)** nas agregações oficiais.
4. **Preservação Estatística de NULOs**: Garantir que estudantes ausentes ou com prova não preenchida mantenham o estado NULL, evitando a redução artificial das médias de proficiência das escolas.
5. **Governança Multi-cloud, Observabilidade & FinOps**: Ingestão segura sem escrita em disco local na origem, monitoramento em tempo real dos custos de computação serverless no AWS Glue e alertas automatizados por e-mail via `smtplib` e CloudWatch.

---

### 2. Arquitetura do Data Lake Medalhão Híbrido

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
|  * Processador: AWS Glue Job 04_events_processor (Structured Streaming)           |
|  * Offsets: Controle incremental via checkpointLocation no S3                     |
|  * Limpeza S3: Lifecycle Rule expirando arquivos brutos em 7 dias                 |
|  * Short-Circuit FinOps: Encerramento em segundos quando a landing estiver vazia  |
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

### 3. Comparativo de Workloads: Batch vs. Streaming

| Dimensão de Engenharia | Carga Batch Principal (Carga Inicial) | Ingestão Streaming / Incremental |
| :--- | :--- | :--- |
| **Frequência de Execução** | Agendada / Sob demanda (Carga Histórica) | Contínua / Micro-lotes em Tempo Quase Real |
| **Engine de Processamento** | PySpark estático (`spark.read`) | Spark Structured Streaming (`spark.readStream`) |
| **Controle de Offsets** | Orientado a Manifestos JSON por camada | Automático via `checkpointLocation` no S3 |
| **Estratégia de Disparo** | Processamento completo do dataset | Trigger sob demanda (`trigger(availableNow=True)`) |
| **Latência Típica** | ~9,42 minutos (Carga de 3,86M registros) | ~2,5 a 4,3 minutos por micro-lote de eventos |
| **Tolerância a Falhas** | Reprocessamento idempotente de partições | *Exactly-Once semantics* via Delta Lake `MERGE INTO` |
| **Gestão de Buffer S3** | Persistência definitiva na Bronze | S3 Lifecycle Rule expirando landing brutos em 7 dias |

---

### 4. Observabilidade, Alarmes & Notificações de Falha

#### ❓ Pergunta do Edital: *"Em produção, quem descobre que o pipeline quebrou, e em quanto tempo?"*

> **Resposta Oficial de Arquitetura**:
> Em produção, a equipe de DataOps / SRE descobre que o pipeline quebrou em **menos de 10 segundos** de duas formas automatizadas e integradas:
> 1. **Notificação Ativa por E-mail (`smtplib`)**: Ao ocorrer uma falha não tratada ou disparo do portão bloqueante de qualidade (*Join Match Ratio < 95%*), o orquestrador (`pipeline_batch.py` / `pipeline_events.py`) aciona o módulo de e-mail nativo enviando uma notificação contendo o identificador do job, Run ID, timestamp UTC, pilha de erros e o relatório completo em Markdown.
> 2. **Painel & Alarmes CloudWatch**: Os **CloudWatch MetricAlarms** (`TC-Educacao-GlueJobFailure-*`) mudam imediatamente de estado para `ALARM`, ativando o indicador em vermelho no **CloudWatch Dashboard** (`TC-Educacao-Observability-Dashboard`).

```text
+-----------------------------------------------------------------------------------+
|                           FLUXO DE ALERTA E OBSERVABILIDADE                       |
+-----------------------------------------------------------------------------------+
   AWS Glue Job / Quality Gate -> FALHA DETECTADA (Status FAILED / Match Ratio < 95%)
                                     |
         +---------------------------+---------------------------+
         |                                                       |
         v                                                       v
[1. CloudWatch MetricAlarm]                             [2. Orquestrador Local/CI-CD]
  - Alarme muda para ALARM                                - Interrompe a esteira
  - Widget vermelho no CloudWatch Dashboard               - Gera relatório Markdown
  - Monitoramento visual centralizado                     - Dispara E-mail via smtplib
                                                          - Notificação enviada em <10s
```

---

### 5. Matriz FinOps e Custos Estimados em Nuvem

#### A. Tabela de Custos Projetados (Formato Oficial do Edital)

| Serviço AWS | Unidade de Medida | Quantidade Medida | Preço Unitário | Custo Mensal Projetado |
| :--- | :--- | :--- | :--- | :--- |
| **AWS Glue 4.0 (Batch)** | DPU-Horas (Type G.1X) | 0,3139 DPU-h / execução | US$ 0,44 / DPU-h | US$ 4,14 / mês (30 exec.) |
| **AWS Glue 4.0 (Streaming)** | DPU-Horas (Type G.1X) | 0,1655 DPU-h / execução | US$ 0,44 / DPU-h | US$ 2,18 / mês (30 exec.) |
| **Amazon S3 (Storage)** | Gigabytes / Mês | ~2,8 GB (Snappy Parquet / Delta) | US$ 0,023 / GB | US$ 0,06 / mês |
| **Amazon S3 (PUT/POST)** | Milhares de Requisições | ~15,0 mil requisições | US$ 0,005 / 1k req | US$ 0,08 / mês |
| **Amazon CloudWatch** | Dashboards & Metrics | 1 Dashboard + 4 Alarms | US$ 0,00 (Free Tier) | US$ 0,00 / mês |
| **AWS Secrets Manager** | Segredos Armazenados | 1 Secret (`pipeline-educacao/gcp`) | US$ 0,40 / secret | US$ 0,40 / mês |
| **TOTAL GERAL PROJETADO** | — | — | — | **US$ 6,86 / mês** |

#### B. Custos Medidos em Execuções Reais (AWS Glue)

| Camada / Job Glue | Registros Processados | Duração Medida | DPUs (Type G.1X) | Consumo (DPU-h) | Custo Medido (USD) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **01_bronze_ingestion** | 3.867.999 alunos + 6 dim. | ~185s (~3,08 min) | 2,0 DPUs | 0,1028 DPU-h | US$ 0,0452 |
| **02_silver_processing** | 3.867.999 alunos curados | ~220s (~3,67 min) | 2,0 DPUs | 0,1222 DPU-h | US$ 0,0538 |
| **03_gold_aggregation** | 12.511 agregações analíticas | ~107s (~1,78 min) | 2,0 DPUs | 0,0594 DPU-h | US$ 0,0262 |
| **04_events_processor** | 1.057 eventos (micro-lote) | ~172s (~2,87 min) | 2,0 DPUs | 0,0956 DPU-h | US$ 0,0420 |

---

### 6. Decisões Arquiteturais e Trade-offs de Engenharia

| Decisão de Engenharia | Estratégia Adotada | Justificativa e Impacto |
| :--- | :--- | :--- |
| **Acesso à Origem** | GCP BigQuery ➔ AWS S3 via Connector | Elimina download de arquivos pesados em disco local. O conector PySpark carrega as tabelas em RAM, garantindo alta velocidade. |
| **Governança por Manifesto** | `manifest.json` em cada camada | Impede varreduras cegas por wildcards no S3. Cada job lê exatamente as partições validadas pelo manifesto da camada anterior. |
| **Ingestão Streaming & Upsert** | Delta Lake `MERGE INTO` com schema dinâmico | Atualiza eventos de estudantes e repactuações de metas sem duplicar linhas ou sobrescrever colunas calculadas. |
| **Controle de Offsets Streaming** | Spark Structured Streaming com `checkpointLocation` | O Spark gerencia automaticamente os offsets em `s3://.../checkpoints/`. Consome estritamente os arquivos inéditos da Landing Zone. |
| **Limpeza Automática S3 (FinOps)** | S3 Lifecycle Expiration Rule (7 Dias) | Regra nativa da AWS S3 que expira e remove arquivos JSON de `bronze/events_raw_landing/` após 7 dias. |
| **Notificação de Falhas (`smtplib`)** | Módulo de e-mail nativo em Python | Envia alertas em <10s contendo stack trace e relatório Markdown sem dependências de pacotes pesados. |
| **Ponderação estatística Saeb** | $\frac{\sum (\text{peso_aluno} \times \text{alfabetizado})}{\sum \text{peso_aluno}}$ | Atende estritamente à metodologia do Inep/Saeb. Evita a distorção da média simples, onde municípios pequenos teriam o mesmo peso que metrópoles. |
| **Validação Cruzada Automática** | Tolerância $\le 2,0\%$ (Medido: $0,037\%$) | Realiza o batimento em tempo real entre o microdado recalculado e a taxa oficial publicada pelo Inep, garantindo auditabilidade analítica. |

---

### 7. Estrutura do Repositório e Guia de Execução

#### Estrutura de Diretórios
```text
TECH_CHALLENGE_EDUCACAO/
├── data/
│   ├── events_landing/                <-- Buffer local de testes para eventos NDJSON
│   └── features/                      <-- Arquivos Parquet consolidados para ML
├── docs/                              <-- Especificações técnicas detalhadas
│   ├── ingestion_bronze_layer.md
│   ├── quality_silver_layer.md
│   ├── aggregate_gold_layer.md
│   ├── events_processor.md            <-- Especificação técnica do consumidor de eventos
│   └── deploy_aws_setup.md            <-- Documentação da automação de infraestrutura
├── reports/                           <-- Relatórios Markdown de FinOps e Observabilidade
├── src/                               <-- Código-fonte do Projeto
│   ├── common/                        <-- Biblioteca de Qualidade (quality_lib.py)
│   ├── generators/                    <-- Geradores de Eventos (Alunos e Metas)
│   │   ├── generate_alunos_events.py
│   │   └── generate_metas_events.py
│   ├── infra/                         <-- Script de Deploy Serverless (deploy_aws_setup.py)
│   ├── monitor/                       <-- Gerador de Relatórios (report_generator.py)
│   ├── scripts/                       <-- AWS Glue Jobs (01_bronze, 02_silver, 03_gold, 04_events)
│   ├── pipeline_batch.py              <-- Orquestrador Batch Principal
│   └── pipeline_events.py             <-- Orquestrador Streaming / Event-Driven
├── tests/                             <-- Testes unitários com pytest
├── .env_example                       <-- Modelo de variáveis de ambiente
├── Makefile                           <-- Comandos de automação (make batch, make events)
└── requirements.txt                   <-- Dependências Python
```

#### Instruções de Execução (Passo a Passo)

1. **Configurar o Ambiente Virtual e Variáveis**:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env_example .env
```

2. **Fazer o Deploy Completo de Infraestrutura, Observabilidade e Glue Jobs**:
```bash
python src/infra/deploy_aws_setup.py
```

3. **Executar a Pipeline Batch Completa (Carga Inicial)**:
```bash
python src/pipeline_batch.py
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

### 8. Preparação para Inteligência Artificial (Fase 3)

A Camada Gold gera a **Feature Store** no formato Parquet (`data/features/gold_features_alfabetizacao.parquet`) contendo as variáveis preditivas para os modelos da próxima fase.

#### Mitigação de Target Leakage (Vazamento de Dados):
* **Variáveis Preditivas ($X$)**: Indicadores demográficos, porte do município, rede de ensino, série, média de proficiência histórica e peso amostral.
* **Variável Alvo ($y$)**: `alvo_atingiu_meta` (Binária: $1$ se atingiu a meta pactuada e $0$ caso contrário).
* **Isolamento**: Nenhuma coluna calculada a partir da meta do próprio ano letivo é incluída nas variáveis explicativas $X$.

---

### 9. Estado Final do Projeto e Entregáveis

1. **Idempotência e Tolerância a Falhas**: Validada no Delta Lake Silver com `MERGE INTO` incremental.
2. **Validação Cruzada Mantida**: A divergência estatística em relação ao Inep oficial permaneceu em **0,037%** ($\le 2,0\%$).
3. **Observabilidade Serverless**: CloudWatch Dashboard (`TC-Educacao-Observability-Dashboard`), 4 Alarmes de tarefas Glue e Alerta por e-mail via `smtplib` ativos.
