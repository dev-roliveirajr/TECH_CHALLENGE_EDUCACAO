# Especificação Técnica de Ingestão — Camada Bronze (Raw Data)

Este documento apresenta a arquitetura, as decisões de engenharia, os padrões de segurança e a observabilidade implementados no pipeline de ingestão da **Camada Bronze** da solução do **Tech Challenge (Fase 2)**. A pipeline é responsável por extrair os dados brutos oficiais sobre alfabetização no Brasil a partir da plataforma **Base dos Dados (GCP BigQuery)** e persisti-los de forma segura, incremental, auditável e idempotente no **Amazon S3 Data Lake** via **AWS Glue ETL**.

---

## 1. Visão Geral e Propósito da Camada Bronze

No modelo de **Arquitetura Medalhão**, a **Camada Bronze (Raw Data)** atua como a zona de atracação primária (*landing zone*) do Data Lake. O seu propósito fundamental é garantir a fidelidade absoluta dos dados de origem, preservando o histórico completo das bases sem aplicar transformações destrutivas ou limpezas de negócio que possam adulterar a informação original.

No contexto do **Compromisso Nacional Criança Alfabetizada** e da **Pesquisa Alfabetiza Brasil (Saeb/Inep)**, o pipeline consome o dataset oficial `basedosdados.br_inep_avaliacao_alfabetizacao`, composto por **sete entidades estratégicas**:

*   **Microdados Individuais de Alunos (`alunos`)**: Registros no nível de estudante contendo proficiência, presença, preenchimento de caderno, indicador de alfabetizado e peso amostral.
*   **Indicadores Territoriais e de Rede (`municipio` e `uf`)**: Desempenho e taxa de alfabetização agregados por município, estado, ano, série e rede de ensino.
*   **Trajetórias e Metas de Alfabetização (`meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf` e `meta_alfabetizacao_municipio`)**: Séries históricas e pactuações anuais de metas (2024 a 2030) nos âmbitos federal, estadual e municipal.
*   **Dicionário de Dados e Categóricos (`dicionario`)**: Mapeamento oficial e de-para de códigos, redes de ensino, condições de presença e variáveis categóricas das avaliações do Inep.

A Camada Bronze provê a base necessária para que as camadas subsequentes (Silver e Gold) realizem limpezas, padronizações, JOINs com travas de qualidade e modelagens analíticas com total auditabilidade, linhagem e rastreabilidade.

---

## 2. Arquitetura de Ingestão e Fluxo Multi-Cloud (GCP ➔ AWS)

A solução adota uma arquitetura **Multi-Cloud consciente**, integrando os serviços gerenciados do **Google Cloud Platform (GCP)** e do **Amazon Web Services (AWS)**. A escolha de consultar a origem via BigQuery e persistir o Data Lake no Amazon S3 fundamenta-se na combinação entre a disponibilidade nativa das bases públicas no GCP e a capacidade de processamento serverless distribuído do AWS Glue.

```text
+-----------------------------------------------------------------------------------+
|                            ORIGEM: GCP BIGQUERY                                   |
|               Dataset: basedosdados.br_inep_avaliacao_alfabetizacao               |
| [alunos] [municipio] [uf] [meta_brasil] [meta_uf] [meta_municipio] [dicionario]   |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Spark BigQuery Connector (RAM / Base64)
                                          v
+-----------------------------------------------------------------------------------+
|                            PROCESSAMENTO: AWS GLUE 4.0                            |
|  * AWS Secrets Manager: Recuperação de credenciais JSON In-Memory (RAM)           |
|  * PySpark + AQE: Leitura paralela em memória e cálculo dinâmico de partições     |
|  * Checagem de Idempotência: Comparação table_meta.modified vs. S3 Prefix         |
|  * Trava Anti-Colisão: Verificação preventiva de atributos de auditoria           |
+-----------------------------------------------------------------------------------+
                                          |
                                          +-----------------------------------------+
                                          |                                         |
                                          v Gravação Parquet (Append)               v Publicação JSON
+--------------------------------------------------+ +------------------------------+
|             DESTINO: AMAZON S3 BRONZE            | |     AUDITORIA & GOVERNANÇA   |
| s3://tc-alfabetizacao.../bronze/<entidade>/...   | | s3://.../auditoria/bronze/  |
|                                                  | | manifest.json                |
+--------------------------------------------------+ +------------------------------+
                                          |
                                          | Telemetria CloudWatch Logs & CLI Streaming
                                          v
+-----------------------------------------------------------------------------------+
|                        OBSERVABILIDADE: REPORTS & FINOPS                          |
|  Relatórios Markdown salvos em reports/job_executions/report_01_bronze_...md       |
|  Seção 5: Tabela Comparativa Metadados BigQuery vs. Arquivos Ingeridos S3         |
+-----------------------------------------------------------------------------------+
```

### Justificativa de Engenharia e Análise de Trade-Offs

A decisão de arquitetura para a ingestão da fonte primária envolveu trade-offs estratégicos de custo, latência e governança:

| Dimensão de Análise | Estratégia Adotada | Justificativa e Mitigação |
| :--- | :--- | :--- |
| **Origem dos Dados** | GCP BigQuery (Base dos Dados) | Permite acesso direto aos dados atualizados via SDK/Connector On-Demand, dispensando o gerenciamento de *downloads* manuais de dumps compactados. |
| **Transferência de Dados (Egress)** | Conexão Direta Spark Connector | O volume de leitura fica coberto pela cota gratuita (*Free Tier*) de 1 TB/mês de processamento de consultas On-Demand do BigQuery. |
| **Custo Computacional** | AWS Glue ETL 4.0 Serverless | Execução sob demanda utilizando instâncias Worker Type G.1X (2.0 DPUs) alocadas estritamente pelo tempo necessário de execução. |
| **Formato de Armazenamento** | Apache Parquet (Snappy) | Formato colunar altamente comprimido que reduz em até 80% o custo de armazenamento no S3 e acelera as consultas no Amazon Athena. |
| **Governança de Ingestão** | Manifesto de Auditoria (`manifest.json`) | Publicação de contrato de lote validado para isolar execuções incompletas e servir de porta de entrada para a Camada Silver. |

---

## 3. Segurança e DevSecOps: Autenticação In-Memory & Resiliência

A segurança e o cumprimento das diretrizes de **DevSecOps** constituem um pilar central da ingestão. O pipeline elimina o risco de vazamento de credenciais ao não armazenar arquivos de chaves privadas em disco e ao garantir que nenhuma credencial seja versionada no repositório Git.

### Princípio de Zero-Disk Persistence

A chave de conta de serviço (*Service Account*) do GCP fica armazenada de forma criptografada no **AWS Secrets Manager** sob o identificador `pipeline-educacao/gcp/bigquery`. Durante a execução do Job no Glue, as credenciais são lidas diretamente para a memória RAM do container Python, convertidas em Base64 e injetadas na sessão Spark sem gravar qualquer arquivo temporário em `/tmp`.

```text
[AWS Secrets Manager] --(encrypted)--> [boto3 client] --(RAM)--> [Base64 String] --(SparkConf)--> [BigQuery Connector]
```

### Garantias de Segurança e Resiliência em Ambientes de Laboratório

*   **Renovação Automática de Credenciais em Labs (`load_dotenv(override=True)`)**:
    Para evitar falhas de autenticação quando sessões de ambientes de laboratório (AWS Academy / Vocareum) expiram e são renovadas, todas as rotinas utilizam `load_dotenv(override=True)`, forçando a atualização imediata das variáveis de ambiente com os tokens do arquivo `.env` local.
*   **Tratamento de Políticas de Serviço (SCPs)**:
    O script de deploy foi projetado com resiliência contra restrições de Service Control Policies (SCPs) do ambiente de estudo, tratando exceções `403 AccessDenied` em chamadas de inspeção de *Object Lock* (`s3:GetBucketObjectLockConfiguration`) para manter a execução contínua sem interrupções.
*   **Criptografia em Trânsito e em Repouso**:
    O bucket S3 `s3://tc-alfabetizacao-869769158828-us-east-1` utiliza criptografia padrão de servidor **SSE-S3 (AES256)** com **Bucket Key habilitado**, reduzindo chamadas KMS e custos operacionais.
*   **Bloqueio de Acesso Público**:
    O bucket possui a configuração **Block All Public Access** ativada (`BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`).
*   **Object Ownership**:
    Configurado como **BucketOwnerEnforced**, desabilitando ACLs legadas e centralizando a governança via políticas de IAM.

---

## 4. Idempotência, Incrementalidade e Auditoria com Manifestos (`manifest.json`)

Para atender às exigências do desafio e evitar custos desnecessários de reprocessamento ou duplicação de dados no Data Lake, o pipeline implementa uma trava estrita de **idempotência baseada em metadados de modificação da fonte** e um **sistema de auditoria baseado em manifestos**.

```text
                       +-----------------------------------+
                       | Checar data da última modificação |
                       |    na tabela do GCP BigQuery      |
                       |       (table_meta.modified)       |
                       +-----------------------------------+
                                         |
                                         v
                       +-----------------------------------+
                       |  Montar prefixo de partição S3:   |
                       | bronze/<entidade>/ref_ano=/ref_mes/|
                       +-----------------------------------+
                                         |
                                         v
                       +-----------------------------------+
                       | Prefix existe no S3 com arquivos? |
                       +-----------------------------------+
                                  /         \
                             Sim /           \ Não
                                /             \
                               v               v
                +--------------------+   +---------------------+
                |  SKIPPED           |   | Executar Ingestão   |
                |  Data atualizada   |   | Ler BigQuery        |
                |  Zero reprocessam. |   | Append em Parquet   |
                +--------------------+   +---------------------+
                                                   |
                                                   v
                                         +---------------------+
                                         | Gerar e publicar    |
                                         | auditoria/bronze/   |
                                         | manifest.json       |
                                         +---------------------+
```

### Regras de Engenharia para Preservação e Linhagem de Dados

1.  **Checagem Leve de Metadados**:
    Antes de ler qualquer registro do BigQuery, o script consulta o método `bq_client.get_table(table_id)` e recupera a data/hora de modificação da tabela (`modified`), além da contagem oficial de linhas (`num_rows`) e do tamanho em bytes (`num_bytes`).
2.  **Particionamento Hive Sem Sobrescrita Destrutiva**:
    Os dados são gravados no S3 seguindo a convenção Hive Partitioning:
    `s3://<bucket>/bronze/<entidade>/ref_ano=YYYY/ref_mes=MM/ref_dia=DD/`
3.  **Modo de Escrita Append (`.mode("append")`)**:
    A escrita é realizada estritamente em modo de anexação. Nenhuma execução utiliza `overwrite` ou limpeza geral de caminhos, garantindo a preservação do histórico temporal.
4.  **Trava Anti-Colisão de Colunas**:
    Antes do enriquecimento, o script valida proativamente se o esquema de origem contém atributos com nomes idênticos às colunas de auditoria (`_ingested_at`, `_source`, `ref_ano`, `ref_mes`, `ref_dia`). Havendo sobreposição, a execução é abortada preventivamente para impedir corrupção de dados.
5.  **Contrato de Lote via Manifesto de Auditoria (`manifest.json`)**:
    Ao término do processamento com sucesso, o Glue salva um arquivo JSON em:
    `s3://<bucket>/auditoria/bronze/ingestion_date=YYYY-MM-DD/run_id=<JOB_RUN_ID>/manifest.json`
    Este manifesto lista os prefixos S3 validados, contagens de linhas na origem e no destino, e a marcação `"is_full_batch": true`. A Camada Silver consome exclusivamente manifestos válidos com status `SUCCEEDED`, evitando a leitura acidental de snapshots parciais ou falhos.

---

## 5. Otimização de Performance: Particionamento Dinâmico e AQE

A ingestão de bases de grande escala, como a tabela de `alunos` (~3,9 milhões de registros), apresenta o risco do *Small Files Problem* (geração de milhares de arquivos Parquet de poucos kilobytes) ou de *Out Of Memory (OOM)* quando se força uma única partição com `coalesce(1)`.

Para resolver esse dilema, o pipeline combina o **Adaptive Query Execution (AQE)** do Spark 3.3+ com um algoritmo de **Cálculo Dinâmico de Partições baseado no Tamanho Alvo do Bloco de Data Lake (128 MB)**.

```python
# 1. Ativação do AQE na Spark Session (AWS Glue 4.0 / Spark 3.3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Definição do Tamanho Alvo de Bloco Parquet no S3
TARGET_FILE_SIZE_MB = 128.0

# 3. Cálculo Dinâmico do Número de Partições Ideais por Metadados
num_bytes = getattr(table_meta, "num_bytes", 0) or 0
size_mb = num_bytes / (1024.0 * 1024.0)
target_partitions = max(1, math.ceil(size_mb / TARGET_FILE_SIZE_MB))

# 4. Aplicação do Coalesce Inteligente
if target_partitions < df.rdd.getNumPartitions():
    df_writer = df_bronze.coalesce(target_partitions)
```

### Vantagens do Particionamento Dinâmico de 128 MB

*   **Eliminação do Gargalo de Thread Única**: Tabelas volumosas como `alunos` são divididas dinamicamente em partições equilibradas (ex: 3 partições de ~85 MB), garantindo o uso paralelo de múltiplos *cores* do cluster Glue.
*   **Proteção de Memória RAM**: Evita a expansão de dados descompactados em memória que ocorre ao forçar `coalesce(1)` em datasets grandes.
*   **Eficiência para Leitura no Athena**: Gera arquivos Parquet no tamanho ideal recomendado para escaneamento colunar de alta performance.

---

## 6. Observabilidade, Streaming em Tempo Real e FinOps

Alinhado ao requisito de que *"logging não é observabilidade"*, a solução implementa a telemetria ao vivo no terminal CLI e a persistência acumulativa de relatórios de auditoria.

### Streaming de Logs em Tempo Real (`pipeline.py` V3)

O orquestrador local monitora o Job no AWS Glue via consulta contínua à API do CloudWatch Logs no Log Group `/aws-glue/jobs/output`. Utilizando ponteiros de paginação (`nextToken`), o terminal exibe instantaneamente as mensagens impressas pela aplicação PySpark na nuvem, aplicando filtros inteligentes para ocultar alertas ruidosos da JVM e destacar apenas as etapas de negócio.

### Módulo de Relatórios FinOps (`src/monitor/report_generator.py` V6)

Ao término do Job, o gerador de relatórios é invocado para consolidar a execução em um arquivo Markdown em `reports/job_executions/report_01_bronze_ingestion_YYYYMMDD_HHMMSS.md`. O relatório inclui:

1.  **Cálculo de Custo FinOps em Tempo Real**:
    $$\text{Custo USD} = \left(\frac{\text{DPUSeconds}}{3600}\right) \times \text{Preço por DPU-Hora (\$0.44)}$$
2.  **Seção 5: Comparativo de Contagem Origem (BigQuery) vs. Destino (S3 Bronze)**:
    Uma tabela que relaciona cada entidade com as linhas de metadados do BigQuery, o volume gravado no S3, o status da ingestão e o tamanho gerado em MB.
3.  **Fallback Resiliente de Manifesto**:
    O gerador busca a chave exata no S3 e, caso necessário, localiza o manifesto mais recente gravado no diretório `auditoria/bronze/ingestion_date=YYYY-MM-DD/`.

---

## 7. Matriz de Entidades Ingeridas e Evidências de Produção

A tabela abaixo sumariza as **sete entidades oficiais** ingeridas do dataset `basedosdados.br_inep_avaliacao_alfabetizacao` para o Amazon S3 na Camada Bronze, incluindo a contagem exata de linhas validadas nas execuções de produção:

| Entidade / Tabela Oficial | Escopo e Conteúdo do Dataset | Registros no BigQuery | Registros Ingeridos no S3 Bronze | Estratégia de Particionamento S3 |
| :--- | :--- | :---: | :---: | :--- |
| **`alunos`** | Microdados individuais de proficiência, presença e peso amostral. | **3.867.999** | **3.867.999 linhas** | 3 partições Parquet (~85 MB cada) via `ref_ano/ref_mes/ref_dia` |
| **`municipio`** | Indicadores agregados por município, ano, série e rede. | **23.995** | **23.995 linhas** | 1 partição Parquet (1.75 MB) via `ref_ano/ref_mes/ref_dia` |
| **`meta_alfabetizacao_municipio`** | Trajetória e metas de alfabetização pactuadas municipais (2024-2030). | **10.704** | **10.704 linhas** | 1 partição Parquet (1.10 MB) via `ref_ano/ref_mes/ref_dia` |
| **`uf`** | Indicadores de alfabetização agregados por Estado/UF. | **145** | **145 linhas** | 1 partição Parquet (0.01 MB) via `ref_ano/ref_mes/ref_dia` |
| **`meta_alfabetizacao_uf`** | Trajetória e metas de alfabetização estaduais (2024-2030). | **81** | **81 linhas** | 1 partição Parquet (0.01 MB) via `ref_ano/ref_mes/ref_dia` |
| **`dicionario`** | De-para oficial e códigos de variáveis categóricas das avaliações do Inep. | **27** | **27 linhas** | 1 partição Parquet (0.00 MB) via `ref_ano/ref_mes/ref_dia` |
| **`meta_alfabetizacao_brasil`** | Trajetória e metas de alfabetização nacionais (2024-2030). | **3** | **3 linhas** | 1 partição Parquet (0.00 MB) via `ref_ano/ref_mes/ref_dia` |

---

## 8. Conformidade com os Requisitos do Tech Challenge (Fase 2)

O pipeline da Camada Bronze atende integralmente às exigências do edital do projeto integrador:

*   **Integração Completa das 7 Entidades**: Todas as tabelas oficiais foram ingeridas, validadas e persistidas na Camada Bronze.
*   **Preservação de Histórico Sem Overwrite**: Utilização de modo `append` com particionamento Hive por data de modificação da fonte.
*   **Idempotência**: Mecanismo de trava que detecta dados já atualizados (`SKIPPED`) e evita reprocessamento, reduzindo custos de DPU e tráfego de rede.
*   **Governança & Contratos de Dados**: Publicação automática do manifesto `manifest.json` para desacoplamento e consumo seguro pela Camada Silver.
*   **DevSecOps & Resiliência em Laboratório**: Autenticação GCP 100% In-Memory via Secrets Manager e tratamento seguro para `load_dotenv(override=True)` e SCPs da AWS Academy.
*   **Observabilidade & FinOps Medido**: Logs em tempo real no terminal CLI e relatórios automáticos gerados em `reports/job_executions/`, registrando o custo real de **US$ 0.0342 por execução completa** com logs extraídos do CloudWatch.
