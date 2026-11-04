# Especificação Técnica de Ingestão — Camada Bronze (Raw Data)

Este documento apresenta a arquitetura e as decisões de engenharia do pipeline de ingestão da **Camada Bronze**. O pipeline extrai dados sobre alfabetização da **Base dos Dados (GCP BigQuery)** e os persiste no **Amazon S3** por meio do **AWS Glue**.

---

## 1. Visão Geral e Propósito da Camada Bronze

No modelo de **Arquitetura Medalhão**, a **Camada Bronze (Raw Data)** recebe os dados de origem no Data Lake sem aplicar transformações de negócio. As gravações são particionadas pela data de modificação da tabela; uma nova gravação na mesma data substitui essa partição, enquanto partições de outras datas são mantidas.

O pipeline consome o dataset `basedosdados.br_inep_avaliacao_alfabetizacao`, composto por sete entidades:

*   **Microdados Individuais de Alunos (`alunos`)**: Registros no nível de estudante contendo proficiência, presença, preenchimento de caderno, indicador de alfabetizado e peso amostral.
*   **Indicadores Territoriais e de Rede (`municipio` e `uf`)**: Desempenho e taxa de alfabetização agregados por município, estado, ano, série e rede de ensino.
*   **Trajetórias e Metas de Alfabetização (`meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf` e `meta_alfabetizacao_municipio`)**: Séries históricas e pactuações anuais de metas (2024 a 2030) nos âmbitos federal, estadual e municipal.
*   **Dicionário de Dados e Categóricos (`dicionario`)**: Mapeamento oficial e de-para de códigos, redes de ensino, condições de presença e variáveis categóricas das avaliações do Inep.

As camadas Silver e Gold usam os dados Bronze como entrada para limpeza, padronização, cruzamentos e agregações.

---

## 2. Arquitetura de Ingestão e Fluxo Multi-Cloud (GCP ➔ AWS)

A solução integra serviços do **Google Cloud Platform (GCP)** e da **Amazon Web Services (AWS)**. O BigQuery fornece a origem dos dados e o AWS Glue executa o processamento e grava os resultados no Amazon S3.

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
                                  v Gravação Parquet                           v Publicação JSON
+--------------------------------------------------+ +------------------------------+
|             DESTINO: AMAZON S3 BRONZE            | |     AUDITORIA & GOVERNANÇA   |
| s3://tc-alfabetizacao.../bronze/<entidade>/...   | | s3://.../auditoria/bronze/  |
|                                                  | | manifest.json                |
+--------------------------------------------------+ +------------------------------+
                                          |
                                          | Telemetria CloudWatch Logs & CLI Streaming
                                          v
+-----------------------------------------------------------------------------------+
|                    RELATÓRIOS DE EXECUÇÃO E CUSTOS                                |
|  Relatórios Markdown salvos em reports/job_executions/report_01_bronze_...md       |
|  Seção 5: Tabela Comparativa Metadados BigQuery vs. Arquivos Ingeridos S3         |
+-----------------------------------------------------------------------------------+
```

### Justificativa de Engenharia e Análise de Trade-Offs

As opções de ingestão envolvem custos de consulta, transferência e processamento:

| Dimensão de Análise | Estratégia Adotada | Justificativa e Mitigação |
| :--- | :--- | :--- |
| **Origem dos Dados** | GCP BigQuery (Base dos Dados) | Permite acesso direto aos dados atualizados via SDK/Connector On-Demand, dispensando o gerenciamento de *downloads* manuais de dumps compactados. |
| **Transferência de Dados** | Conexão direta Spark Connector | Consultas e transferências podem gerar custos conforme a configuração e o uso da conta GCP. |
| **Processamento** | AWS Glue ETL 4.0 | Execução sob demanda com Worker Type G.1X e dois workers, conforme configuração do job. |
| **Formato de armazenamento** | Apache Parquet (Snappy) | Formato colunar usado para os dados no S3 e as consultas no Amazon Athena. |
| **Manifesto de ingestão** | `manifest.json` | Registra os dados da execução consumidos pela Camada Silver. |

---

## 3. Segurança e autenticação

As credenciais do GCP são obtidas do AWS Secrets Manager e usadas durante a execução sem gravar arquivos de chave em disco. Para execução local, o fallback para `gcp-key.json` ou `gcp_key.json` só é usado quando `GCP_CREDENTIALS_LOCAL_FALLBACK=true`; esse fallback não é aceito quando o script é iniciado com `--JOB_NAME`.

### Carregamento de credenciais

A chave de conta de serviço (*Service Account*) do GCP fica armazenada de forma criptografada no **AWS Secrets Manager** sob o identificador `pipeline-educacao/gcp/bigquery`. Durante a execução do Job no Glue, as credenciais são lidas diretamente para a memória RAM do container Python, convertidas em Base64 e injetadas na sessão Spark sem gravar qualquer arquivo temporário em `/tmp`.

```text
[AWS Secrets Manager] --(encrypted)--> [boto3 client] --(RAM)--> [Base64 String] --(SparkConf)--> [BigQuery Connector]
```

### Segurança e tratamento de falhas

* **Carregamento de credenciais**:
    A execução depende da Secret configurada. Erros de acesso ou conteúdo inválido interrompem o job. Não há fallback automático para credenciais padrão do BigQuery.
* **Permissões S3 do Glue Role**:
    A role precisa ler e gravar marcadores em `auditoria/bronze/source_versions/`, gravar manifestos e dados Bronze e listar os prefixos Bronze para verificar os arquivos da versão registrada.
* **Criptografia em trânsito e em repouso**:
O bucket S3 `s3://tc-alfabetizacao-869769158828-us-east-1` utiliza criptografia de servidor **SSE-S3 (AES256)**.
*   **Bloqueio de Acesso Público**:
    O bucket possui a configuração **Block All Public Access** ativada (`BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`).
*   **Object Ownership**:
    Configurado como **BucketOwnerEnforced**, desabilitando ACLs legadas e centralizando a governança via políticas de IAM.

---

## 4. Idempotência, Incrementalidade e Auditoria com Manifestos (`manifest.json`)

Para evitar leituras completas desnecessárias, o pipeline consulta os metadados da tabela no BigQuery e compara `modified` com um marcador de conclusão salvo no S3 para cada entidade. A tabela só é lida pelo conector quando não há marcador nem evidência válida de uma ingestão anterior, a versão do BigQuery é mais recente ou os arquivos esperados não estão disponíveis no prefixo registrado.

O marcador fica em `auditoria/bronze/source_versions/<entidade>.json` e só é atualizado depois que a gravação Parquet termina. Se o job falhar antes dessa atualização, a próxima execução tenta novamente. Manifestos antigos registram apenas a data da partição, não a versão exata da tabela; por isso, a primeira execução após essa mudança lê a tabela uma vez para estabelecer o marcador com o `modified` exato. Nas execuções seguintes, sem versão mais recente no BigQuery, a tabela não é lida pelo conector.

```text
                       +-----------------------------------+
                       | Checar data da última modificação |
                       |    na tabela do GCP BigQuery      |
                       |       (table_meta.modified)       |
                       +-----------------------------------+
                                         |
                                         v
                       +-----------------------------------+
                       | Comparar com a versão confirmada |
                       | no marcador de estado S3         |
                       +-----------------------------------+
                                         |
                                         v
                       +-----------------------------------+
                       | A versão atual já está no S3?    |
                       +-----------------------------------+
                                  /         \
                             Sim /           \ Não
                                /             \
                               v               v
                +--------------------+   +---------------------+
                |  Manter estado     |   | Executar ingestão   |
                |  Sem leitura       |   | Ler BigQuery        |
                |  completo recente  |   | Gravar partição     |
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
2.  **Particionamento Hive**:
    Os dados são gravados no S3 seguindo a convenção Hive Partitioning:
    `s3://<bucket>/bronze/<entidade>/ref_ano=YYYY/ref_mes=MM/ref_dia=DD/`
3.  **Substituição da partição alterada**:
    A escrita usa overwrite dinâmico do Spark. Somente a partição da data de modificação processada é substituída; as demais partições da entidade são preservadas. Isso permite repetir uma gravação no mesmo dia sem acumular cópias daquela partição.
4.  **Trava Anti-Colisão de Colunas**:
    Antes do enriquecimento, o script valida proativamente se o esquema de origem contém atributos com nomes idênticos às colunas de auditoria (`_ingested_at`, `_source`, `ref_ano`, `ref_mes`, `ref_dia`). Havendo sobreposição, a execução é abortada preventivamente para impedir corrupção de dados.
5.  **Contrato de Lote via Manifesto de Auditoria (`manifest.json`)**:
    Ao término do processamento , o Glue salva um arquivo JSON em:
    `s3://<bucket>/auditoria/bronze/ingestion_date=YYYY-MM-DD/run_id=<JOB_RUN_ID>/manifest.json`
    O manifesto lista os prefixos S3, metadados da origem e resultados por entidade. A contagem de linhas do BigQuery vem dos metadados; a contagem efetivamente gravada no S3 não é medida por uma segunda leitura. Falha ao salvar o manifesto interrompe a execução.

---

## 5. Otimização de Performance: Particionamento Dinâmico e AQE

O job habilita o Adaptive Query Execution (AQE) do Spark e calcula um limite de partições com base no tamanho informado pelos metadados do BigQuery. Esse tamanho é uma referência da origem e não garante o tamanho dos arquivos Parquet gerados no S3.

```python
# 1. Ativação do AQE na Spark Session (AWS Glue 4.0 / Spark 3.3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Definição do Tamanho Alvo de Bloco Parquet no S3
TARGET_FILE_SIZE_MB = 128.0

# 3. Estimativa do número de partições a partir dos metadados
num_bytes = getattr(table_meta, "num_bytes", 0) or 0
size_mb = num_bytes / (1024.0 * 1024.0)
target_partitions = max(1, math.ceil(size_mb / TARGET_FILE_SIZE_MB))

# 4. Aplicação do Coalesce Inteligente
if target_partitions < df.rdd.getNumPartitions():
    df_writer = df_bronze.coalesce(target_partitions)
```

O cálculo é uma heurística para reduzir partições Spark quando o valor estimado é menor que a quantidade atual. O tamanho final e a quantidade dos arquivos Parquet dependem dos dados, do plano de execução e dos recursos disponíveis.

---

## 6. Logs, streaming e custos

O orquestrador apresenta logs do AWS Glue no terminal e mantém relatórios de execução.

### Streaming de logs (`pipeline.py`)

O orquestrador consulta o grupo `/aws-glue/jobs/output` pela API do CloudWatch Logs. Utiliza o token de paginação (`nextToken`) para exibir as mensagens do job no terminal.

### Relatórios de execução (`src/monitor/report_generator.py`)

Ao término do Job, o gerador de relatórios é invocado para consolidar a execução em um arquivo Markdown em `reports/job_executions/report_01_bronze_ingestion_YYYYMMDD_HHMMSS.md`. O relatório inclui:

1.  **Cálculo do custo de execução**:
    $$\text{Custo USD} = \left(\frac{\text{DPUSeconds}}{3600}\right) \times \text{Preço por DPU-Hora (\$0.44)}$$
2.  **Seção 5: Metadados da origem e resultado da ingestão Bronze**:
    Uma tabela com contagens disponíveis nos metadados do BigQuery, o status da ingestão e o tamanho informado pela origem. A contagem gravada no S3 não é medida por uma leitura adicional.
3.  **Fallback de manifesto**:
    O gerador busca a chave exata no S3 e, caso necessário, localiza o manifesto mais recente gravado no diretório `auditoria/bronze/ingestion_date=YYYY-MM-DD/`.

---

## 7. Entidades processadas

O job processa as seguintes tabelas do dataset `basedosdados.br_inep_avaliacao_alfabetizacao`:

| Entidade | Conteúdo |
|---|---|
| `alunos` | Registros de estudantes, proficiência, presença e peso amostral. |
| `municipio` | Indicadores agregados por município, ano, série e rede. |
| `uf` | Indicadores agregados por estado. |
| `meta_alfabetizacao_brasil` | Metas nacionais de alfabetização. |
| `meta_alfabetizacao_uf` | Metas estaduais de alfabetização. |
| `meta_alfabetizacao_municipio` | Metas municipais de alfabetização. |
| `dicionario` | Descrições de variáveis e códigos categóricos. |

---

## 8. Resumo do processamento

* As sete entidades são particionadas pela data de modificação da fonte.
* Metadados do BigQuery são comparados com marcadores de ingestão no S3 antes da leitura dos dados.
* Um manifesto JSON registra o resultado da ingestão para consumo pelas camadas seguintes.
* As credenciais do GCP são obtidas do AWS Secrets Manager durante a execução.
* Relatórios de execução são salvos em `reports/job_executions/`.
