#  Relatório de Execução do Job: `01_bronze_ingestion`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-03 23:53:46 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `01_bronze_ingestion`
* **JobRunId:** `jr_3ade157efe27d8d85284d5006d36c001b3385c784704850ce16dde1d3fb40b22`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_3ade157efe27d8d85284d5006d36c001b3385c784704850ce16dde1d3fb40b22/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **81 segundos** (~1.35 min) |
| **DPU-Horas Medidas** | **0.0450 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0198** |
| **Início / Término** | 2026-11-03 23:52:17 UTC ➔ 2026-11-03 23:53:46 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `GCP BigQuery (basedosdados.br_inep_avaliacao_alfabetizacao)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-03 23:53:33 UTC] Consultando a Secret 'pipeline-educacao/gcp/bigquery' no AWS Secrets Manager.
[2026-11-03 23:53:34 UTC] Credenciais obtidas do AWS Secrets Manager ('pipeline-educacao/gcp/bigquery').
[2026-11-03 23:53:34 UTC] Credenciais GCP carregadas.
[2026-11-03 23:53:34 UTC] ===========================================================================
 Iniciando ingestão da Camada Bronze.
   Bucket de Destino: s3://tc-alfabetizacao-869769158828-us-east-1/bronze
===========================================================================
[2026-11-03 23:53:34 UTC] [Entidade ALUNOS] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.alunos'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [Entidade UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.uf'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [Entidade MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.municipio'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [Entidade META_ALFABETIZACAO_BRASIL] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [Entidade META_ALFABETIZACAO_UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [META_ALFABETIZACAO_BRASIL] A versão atual já está registrada no S3.
[2026-11-03 23:53:34 UTC] [Entidade META_ALFABETIZACAO_MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio'...
[2026-11-03 23:53:34 UTC] [META_ALFABETIZACAO_UF] A versão atual já está registrada no S3.
[2026-11-03 23:53:34 UTC] [UF] A versão atual já está registrada no S3.
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [Entidade DICIONARIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.dicionario'...
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [MUNICIPIO] A versão atual já está registrada no S3.
[2026-11-03 23:53:34 UTC]
[2026-11-03 23:53:34 UTC] [ALUNOS] A versão atual já está registrada no S3.
[2026-11-03 23:53:35 UTC] [META_ALFABETIZACAO_MUNICIPIO] A versão atual já está registrada no S3.
[2026-11-03 23:53:35 UTC] [DICIONARIO] A versão atual já está registrada no S3.
[2026-11-03 23:53:35 UTC] ===========================================================================
Resumo da ingestão Bronze:
 - alunos                        : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - uf                            : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - municipio                     : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - meta_alfabetizacao_brasil     : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - meta_alfabetizacao_uf         : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - meta_alfabetizacao_municipio  : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
 - dicionario                    : SKIPPED (Already Up-to-Date) (sem gravação nesta execução)
===========================================================================
[2026-11-03 23:53:35 UTC] Manifesto Bronze salvo no S3:
   s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_3ade157efe27d8d85284d5006d36c001b3385c784704850ce16dde1d3fb40b22/manifest.json
Camada Bronze concluída.
```

---

## 5. Metadados da origem e resultado da ingestão Bronze
| Entidade | Linhas no BigQuery (Metadados) | Linhas gravadas no S3 (quando medidas) | Status | Particionamento e tamanho |
| :--- | :---: | :---: | :---: | :---: |
| `alunos` | 3,867,999 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (256.1 MB) |
| `uf` | 145 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `municipio` | 23,995 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (1.75 MB) |
| `meta_alfabetizacao_brasil` | 3 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (0.0 MB) |
| `meta_alfabetizacao_uf` | 81 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `meta_alfabetizacao_municipio` | 10,704 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (1.1 MB) |
| `dicionario` | 27 | N/A | `SKIPPED (Already Up-to-Date)` | 0 part. (0.0 MB) |

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
