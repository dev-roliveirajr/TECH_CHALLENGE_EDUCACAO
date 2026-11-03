# Script de Automação de Infraestrutura, Observabilidade e Segurança AWS (`deploy_aws_setup.py`)

#### Solução Serverless & DevSecOps — Tech Challenge (Fase 2)
Este documento descreve detalhadamente o funcionamento, a arquitetura e as responsabilidades do script de automação de infraestrutura `src/infra/deploy_aws_setup.py`. O script atua como o motor de provisionamento *Infrastructure as Code* (IaC) serverless da pipeline de dados do **Compromisso Nacional Criança Alfabetizada**, automatizando a criação de recursos de armazenamento, segurança, governança, observabilidade em nuvem (CloudWatch Dashboards com FinOps e Alertas) e catálogo de jobs no **AWS Glue 4.0**.

---

### 1. Visão Geral e Arquitetura do Deploy

O script `deploy_aws_setup.py` é executado na máquina local ou via esteira de CI/CD para provisionar e sincronizar os componentes da nuvem AWS sem necessidade de intervenção manual no console.

```text
+-----------------------------------------------------------------------------------+
|                         LOCAL ENVIRONMENT / CI/CD RUNNER                          |
|  * Leitura dinâmica de variáveis do .env (AWS Account, Region, Bucket Name)       |
|  * Leitura da chave GCP (gcp_key.json) para acesso ao BigQuery                    |
+-----------------------------------------------------------------------------------+
                                          |
                                          v  (boto3 / AWS SDK)
+-----------------------------------------------------------------------------------+
|                            AWS CLOUD PROVISIONING ENGINE                          |
|                                                                                   |
|  1. AWS Secrets Manager : Criação/Atualização da Secret 'pipeline-educacao/gcp'  |
|  2. Amazon S3           : Bucket Regional + Criptografia SSE-S3 + BucketKey     |
|                         : Block Public Access (100% Privado) + ACLs Disabled       |
|                         : S3 Lifecycle Rule (Expiração de 7 dias na Landing Zone) |
|  3. Artefatos & JARs    : Upload de Scripts PySpark e Spark BigQuery Connector    |
|  4. AWS Glue Catalog    : Registro/Atualização dos 4 Glue Jobs (01, 02, 03, 04)   |
|  5. CloudWatch Dashboard: Painel 'TC-Educacao-Observability-Dashboard' (FinOps)   |
|  6. CloudWatch Alarms   : 4 Alarmes de Falha para monitoramento de Glue Jobs      |
+-----------------------------------------------------------------------------------+
```

---

### 2. Principais Funcionalidades e Etapas de Execução

#### A. Resolução Dinâmica de Configurações (`.env`)
O script lê as variáveis de ambiente utilizando `python-dotenv`, garantindo zero credenciais *hardcoded* no código-fonte:
* `AWS_ACCOUNT_ID`: Identificador da conta AWS.
* `AWS_DEFAULT_REGION` / `AWS_REGION`: Região AWS de destino (padrão: `us-east-1`).
* `S3_BUCKET_NAME`: Nome base do bucket S3. O script constrói o nome regionalizado e exclusivo: `tc-alfabetizacao-{ACCOUNT_ID}-{REGION}`.
* `AWS_GLUE_ROLE_ARN`: IAM Role utilizada pelo AWS Glue (`arn:aws:iam::{ACCOUNT_ID}:role/LabRole`).

#### B. Gestão de Segredos no AWS Secrets Manager (`setup_gcp_secret`)
* Localiza a chave de Service Account do GCP (`gcp_key.json` ou `gcp-key.json`).
* Armazena o conteúdo JSON de forma segura no AWS Secrets Manager sob a Secret `pipeline-educacao/gcp/bigquery`.
* Garante idempotência: cria uma nova Secret se não existir ou atualiza o valor existente (`put_secret_value`).

#### C. Provisionamento e Criptografia do Bucket S3 (`prepare_and_create_s3_bucket`)
Valida a existência do bucket e aplica as melhores práticas de segurança DevSecOps da AWS:
* **Account Regional Namespace**: Bucket criado na região configurada.
* **Object Ownership**: `BucketOwnerEnforced` (ACLs desabilitadas por padrão).
* **Block Public Access**: Habilitado em nível total (`BlockAllPublicAccess = True`).
* **Criptografia Padrão**: SSE-S3 (`AES256`) com `BucketKeyEnabled=True` para redução de custos com chamadas KMS.

#### D. Política de Expiração do S3 (`setup_s3_lifecycle_rules`)
* Configura a **S3 Lifecycle Rule** nomeada `ExpireRawLandingZoneEventsAfter7Days`.
* Define que todos os arquivos brutos NDJSON gravados no prefixo `bronze/events_raw_landing/` expiram e são removidos automaticamente do S3 após **7 dias**.
* **Impacto FinOps**: Elimina custos desnecessários de armazenamento prolongado de arquivos de staging/buffer sem requerer rotinas de limpeza via código Spark.

#### E. Empacotamento de Artefatos e Registro de Glue Jobs (`deploy_artifacts_and_jobs`)
* Faz upload dos scripts PySpark locais (`01_bronze_ingestion.py`, `02_silver_processing.py`, `03_gold_aggregation.py`, `04_events_processor.py`) e da biblioteca comum (`quality_lib.py`) para `s3://.../scripts/`.
* Realiza o download temporário e upload do **BigQuery Spark Connector JAR** (`spark-bigquery-with-dependencies_2.12-0.34.0.jar`) para `s3://.../libs/`.
* Registra ou atualiza os 4 AWS Glue Jobs no Glue Data Catalog utilizando a versão **AWS Glue 4.0** (Python 3, Spark 3.3):
  * Módulos adicionais configurados via `--additional-python-modules`: `google-cloud-bigquery==3.11.0,google-auth==2.22.0,urllib3<2.0.0,python-dotenv`.
  * Suporte nativo ao Delta Lake habilitado via `--datalake-formats delta`.
  * Parâmetros Spark configurados para catálogo e extensões Delta: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`.

#### F. Provisionamento do CloudWatch Dashboard (`setup_cloudwatch_dashboard`)
Cria automaticamente o painel visual centralizado **`TC-Educacao-Observability-Dashboard`** no Amazon CloudWatch:
* **Widget de Cabeçalho**: Título e contexto da pipeline de observabilidade em Markdown.
* **Widget FinOps e Gestão Financeira**: Tabela comparativa em Markdown com a fórmula oficial de cálculo de custos serverless das DPUs (`Custo = DPUs * (segundos / 3600) * $0.44`), detalhando os custos medidos por camada e o total geral.
* **Widget de Duração dos Jobs**: Gráfico de séries temporais acompanhando a duração em segundos (`elapsedTime`) de cada um dos 4 Glue Jobs.
* **Widget de Tarefas Concluídas**: Métricas de tarefas Spark processadas por job (`numCompletedTasks`).
* **Widget de Status dos Alarmes**: Exibição em tempo real do estado de saúde dos 4 alarmes de falha.

#### G. Provisionamento de Alarmes CloudWatch (`setup_cloudwatch_alarms`)
Configura 4 **CloudWatch MetricAlarms** (`TC-Educacao-GlueJobFailure-{job_name}`):
* Monitora a métrica `AWS/Glue -> numFailedTasks`.
* Dispara estado de alerta (**`ALARM`**) quando o número de tarefas com falha for maior ou igual a 1 em um intervalo de 5 minutos.
* Integra-se diretamente ao Dashboard visual e à esteira de notificação por e-mail via `smtplib`.

---

### 3. Matriz de Componentes Provisionados

| Componente AWS | Recurso Provisionado | Finalidade Técnica |
| :--- | :--- | :--- |
| **AWS Secrets Manager** | `pipeline-educacao/gcp/bigquery` | Guarda a credencial de acesso GCP para consulta ao BigQuery sem expor arquivos locais. |
| **Amazon S3** | `s3://tc-alfabetizacao-{ACCOUNT_ID}-{REGION}` | Armazenamento de dados do Data Lake Medalhão (Bronze, Silver Delta Lake, Gold) e scripts. |
| **S3 Lifecycle Rule** | `ExpireRawLandingZoneEventsAfter7Days` | Expiração e deleção automática de arquivos NDJSON da Landing Zone após 7 dias. |
| **AWS Glue Job 01** | `01_bronze_ingestion` | Ingestão em lote dos microdados do Saeb do GCP BigQuery para a camada Bronze no S3. |
| **AWS Glue Job 02** | `02_silver_processing` | Tratamento, deduplicação, cruzamento de entidades e gravações Delta Lake Silver (`alunos_curado`). |
| **AWS Glue Job 03** | `03_gold_aggregation` | Agregação ponderada, comparativo contra a meta de 80% (2030) e geração da Feature Store para ML. |
| **AWS Glue Job 04** | `04_events_processor` | Consumo em tempo quase real via Spark Structured Streaming com controle por `checkpointLocation`. |
| **CloudWatch Dashboard** | `TC-Educacao-Observability-Dashboard` | Painel gráfico unificado para monitorar tempos de execução, DPUs, custos FinOps e alarmes. |
| **CloudWatch Alarms** | 4 x `TC-Educacao-GlueJobFailure-*` | Monitoramento proativo de erros e falhas nas tarefas dos Glue Jobs. |

---

### 4. Guia de Execução

Para executar o deploy completo da infraestrutura e observabilidade, certifique-se de que o ambiente virtual Python esteja ativo e com as credenciais AWS configuradas no `.env`:

```bash
# 1. Ativar o ambiente virtual
source venv/bin/activate

# 2. Executar o script de deploy
python src/infra/deploy_aws_setup.py
```

#### Saída Esperada no Terminal:
```text
===========================================================================
🚀 DEPLOY COMPLETO DE INFRAESTRUTURA, OBSERVABILIDADE E SEGURANÇA AWS
===========================================================================
ℹ️ Conta AWS ID : 869769158828
ℹ️ Região Padrão: us-east-1
ℹ️ Bucket S3    : s3://tc-alfabetizacao-869769158828-us-east-1
ℹ️ Role Glue ARN: arn:aws:iam::869769158828:role/LabRole
---------------------------------------------------------------------------
🔐 1. Gerenciando Secret no AWS Secrets Manager ('pipeline-educacao/gcp/bigquery')...
  └─ ✅ Secret gerenciada com sucesso!
🪣 2. Verificando/Criando Bucket S3 ('s3://tc-alfabetizacao-...')...
  └─ ✅ Bucket S3 existente e acessível.
♻️ Configurando S3 Lifecycle Rule para Landing Zone em 's3://tc-alfabetizacao-...'...
  └─ ✅ Regra de Expiração S3 criada/atualizada: 'bronze/events_raw_landing/' (Expiração: 7 Dias).
📤 3. Processando artefatos e enviando para s3://...
⚙️ Registrando / Atualizando Glue Jobs no AWS Catalog...
  └─ 🔄 Atualizando Glue Job existente: 01_bronze_ingestion
  └─ 🔄 Atualizando Glue Job existente: 02_silver_processing
  └─ 🔄 Atualizando Glue Job existente: 03_gold_aggregation
  └─ 🔄 Atualizando Glue Job existente: 04_events_processor
📊 4. Provisionando CloudWatch Dashboard ('TC-Educacao-Observability-Dashboard')...
  └─ ✅ CloudWatch Dashboard criado/atualizado com sucesso!
🚨 5. Provisionando Alertas do CloudWatch para os Glue Jobs...
  └─ 🔔 Alarme CloudWatch configurado: 'TC-Educacao-GlueJobFailure-01_bronze_ingestion'
  └─ 🔔 Alarme CloudWatch configurado: 'TC-Educacao-GlueJobFailure-02_silver_processing'
  └─ 🔔 Alarme CloudWatch configurado: 'TC-Educacao-GlueJobFailure-03_gold_aggregation'
  └─ 🔔 Alarme CloudWatch configurado: 'TC-Educacao-GlueJobFailure-04_events_processor'
===========================================================================
✅ DEPLOY CONCLUÍDO COM SUCESSO!
===========================================================================
```
