#  Relatório de Execução do Job: `01_bronze_ingestion`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-03 23:43:35 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `01_bronze_ingestion`
* **JobRunId:** `jr_93d03c617a1d58f7f495ad4db9032ece3cf7b362cbe0093f041e67557a797d44`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_93d03c617a1d58f7f495ad4db9032ece3cf7b362cbe0093f041e67557a797d44/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **140 segundos** (~2.33 min) |
| **DPU-Horas Medidas** | **0.0778 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0342** |
| **Início / Término** | 2026-11-03 23:41:07 UTC ➔ 2026-11-03 23:43:35 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-03 23:42:10 UTC] Consultando a Secret 'pipeline-educacao/gcp/bigquery' no AWS Secrets Manager.
[2026-11-03 23:42:10 UTC] Credenciais obtidas do AWS Secrets Manager ('pipeline-educacao/gcp/bigquery').
[2026-11-03 23:42:10 UTC] Credenciais GCP carregadas.
[2026-11-03 23:42:11 UTC] ===========================================================================
 Iniciando ingestão da Camada Bronze.
   Bucket de Destino: s3://tc-alfabetizacao-869769158828-us-east-1/bronze
===========================================================================
[2026-11-03 23:42:11 UTC] [Entidade ALUNOS] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.alunos'...
[2026-11-03 23:42:11 UTC] [Entidade UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.uf'...
[2026-11-03 23:42:11 UTC] [Entidade MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.municipio'...
[2026-11-03 23:42:11 UTC] [Entidade META_ALFABETIZACAO_BRASIL] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil'...
[2026-11-03 23:42:11 UTC]
[2026-11-03 23:42:11 UTC] [Entidade META_ALFABETIZACAO_UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf'...
[2026-11-03 23:42:11 UTC]
[2026-11-03 23:42:11 UTC] [MUNICIPIO] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:42:11 UTC] [META_ALFABETIZACAO_BRASIL] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:42:11 UTC] [ALUNOS] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:42:11 UTC] [META_ALFABETIZACAO_UF] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:42:12 UTC] [UF] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:42:18 UTC] └─  [META_ALFABETIZACAO_UF] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_uf/
[2026-11-03 23:42:18 UTC] └─  [META_ALFABETIZACAO_BRASIL] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_brasil/
[2026-11-03 23:42:18 UTC] └─  [UF] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/uf/
[2026-11-03 23:42:18 UTC] └─  [ALUNOS] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/alunos/
[2026-11-03 23:42:18 UTC] └─  [MUNICIPIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/municipio/
[2026-11-03 23:42:24 UTC] └─  [UF] Mantido particionamento Spark: 1 partição(ões) (0.01 MB).
[2026-11-03 23:42:24 UTC] └─  [MUNICIPIO] Mantido particionamento Spark: 1 partição(ões) (1.75 MB).
   └─  [META_ALFABETIZACAO_UF] Mantido particionamento Spark: 1 partição(ões) (0.01 MB).
[2026-11-03 23:42:24 UTC] └─  [META_ALFABETIZACAO_BRASIL] Mantido particionamento Spark: 1 partição(ões) (0.00 MB).
[2026-11-03 23:42:24 UTC] └─  [ALUNOS] Coalesce dinâmico aplicado: 9 ➔ 3 partição(ões) (256.10 MB / 128.0 MB alvo).
[2026-11-03 23:43:04 UTC] [META_ALFABETIZACAO_UF] Gravação Bronze concluída (81 linhas no BigQuery).
[2026-11-03 23:43:04 UTC] [MUNICIPIO] Gravação Bronze concluída (23,995 linhas no BigQuery).
[2026-11-03 23:43:04 UTC] [UF] Gravação Bronze concluída (145 linhas no BigQuery).
[2026-11-03 23:43:04 UTC] [META_ALFABETIZACAO_BRASIL] Gravação Bronze concluída (3 linhas no BigQuery).
[2026-11-03 23:43:05 UTC] [Entidade META_ALFABETIZACAO_MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio'...
[2026-11-03 23:43:05 UTC]
[2026-11-03 23:43:05 UTC] [Entidade DICIONARIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.dicionario'...
[2026-11-03 23:43:05 UTC] [META_ALFABETIZACAO_MUNICIPIO] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:43:05 UTC] [DICIONARIO] Nova versão detectada. Lendo BigQuery...
[2026-11-03 23:43:05 UTC]
[2026-11-03 23:43:05 UTC]
[2026-11-03 23:43:05 UTC] └─  [DICIONARIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/dicionario/
[2026-11-03 23:43:05 UTC] └─  [META_ALFABETIZACAO_MUNICIPIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_municipio/
[2026-11-03 23:43:06 UTC] └─  [META_ALFABETIZACAO_MUNICIPIO] Mantido particionamento Spark: 1 partição(ões) (1.10 MB).
[2026-11-03 23:43:07 UTC] └─  [DICIONARIO] Mantido particionamento Spark: 1 partição(ões) (0.00 MB).
[2026-11-03 23:43:09 UTC] [META_ALFABETIZACAO_MUNICIPIO] Gravação Bronze concluída (10,704 linhas no BigQuery).
[2026-11-03 23:43:10 UTC] [DICIONARIO] Gravação Bronze concluída (27 linhas no BigQuery).
[2026-11-03 23:43:24 UTC] [ALUNOS] Gravação Bronze concluída (3,867,999 linhas no BigQuery).
[2026-11-03 23:43:24 UTC] ===========================================================================
Resumo da ingestão Bronze:
[2026-11-03 23:43:24 UTC] - alunos                        : SUCCEEDED (3,867,999 linhas nos metadados da origem)
 - uf                            : SUCCEEDED (145 linhas nos metadados da origem)
 - municipio                     : SUCCEEDED (23,995 linhas nos metadados da origem)
 - meta_alfabetizacao_brasil     : SUCCEEDED (3 linhas nos metadados da origem)
 - meta_alfabetizacao_uf         : SUCCEEDED (81 linhas nos metadados da origem)
 - meta_alfabetizacao_municipio  : SUCCEEDED (10,704 linhas nos metadados da origem)
 - dicionario                    : SUCCEEDED (27 linhas nos metadados da origem)
===========================================================================
[2026-11-03 23:43:24 UTC] Manifesto Bronze salvo no S3:
   s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_93d03c617a1d58f7f495ad4db9032ece3cf7b362cbe0093f041e67557a797d44/manifest.json
Camada Bronze concluída.
```

---

## 5. Metadados da origem e resultado da ingestão Bronze
| Entidade | Linhas no BigQuery (Metadados) | Linhas gravadas no S3 (quando medidas) | Status | Particionamento e tamanho |
| :--- | :---: | :---: | :---: | :---: |
| `alunos` | 3,867,999 | N/A | `SUCCEEDED` | 3 part. (256.1 MB) |
| `uf` | 145 | N/A | `SUCCEEDED` | 1 part. (0.01 MB) |
| `municipio` | 23,995 | N/A | `SUCCEEDED` | 1 part. (1.75 MB) |
| `meta_alfabetizacao_brasil` | 3 | N/A | `SUCCEEDED` | 1 part. (0.0 MB) |
| `meta_alfabetizacao_uf` | 81 | N/A | `SUCCEEDED` | 1 part. (0.01 MB) |
| `meta_alfabetizacao_municipio` | 10,704 | N/A | `SUCCEEDED` | 1 part. (1.1 MB) |
| `dicionario` | 27 | N/A | `SUCCEEDED` | 1 part. (0.0 MB) |

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
