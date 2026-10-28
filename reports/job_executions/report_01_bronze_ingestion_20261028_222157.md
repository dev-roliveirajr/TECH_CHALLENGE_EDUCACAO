# 📊 Relatório de Execução do Job: `01_bronze_ingestion`

## 1. ℹ️ Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-10-28 19:21:53 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `01_bronze_ingestion`
* **JobRunId:** `jr_656f9cbc6655bf832a5c1cb7b9b84213cb7c8099ce631ce44ad05714ba560fde`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_656f9cbc6655bf832a5c1cb7b9b84213cb7c8099ce631ce44ad05714ba560fde/manifest.json`

---

## 2. 💰 Métricas de Desempenho e FinOps (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **124 segundos** (~2.07 min) |
| **DPU-Horas Medidas** | **0.0689 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0303** |
| **Início / Término** | 2026-10-28 19:19:42 UTC ➔ 2026-10-28 19:21:53 UTC |

---

## 3. 🌐 Consumo e Origem GCP BigQuery
* **Dataset Origem:** `basedosdados.br_inep_avaliacao_alfabetizacao` (7 Entidades Oficiais)
* **Custo de Leitura BigQuery (On-Demand):** **US$ 0,00** *(Até 1 TB/mês no Free Tier)*
* **Estratégia de Segurança:** Credenciais recuperadas 100% em memória RAM via AWS Secrets Manager (`pipeline-educacao/gcp/bigquery`). Zero arquivos em disco (`/tmp`).

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
```text
[2026-10-28 19:20:40] 🔐 [SecOps] Buscando Secret 'pipeline-educacao/gcp/bigquery' no AWS Secrets Manager (In-Memory)...
[2026-10-28 19:20:40] └─ ✅ Credenciais obtidas em memória RAM via Secrets Manager ('pipeline-educacao/gcp/bigquery'). Zero disco utilizado.
[2026-10-28 19:20:40] 🔑 Autenticação GCP ativada 100% In-Memory (RAM).
[2026-10-28 19:20:40] ===========================================================================
🚀 INICIANDO INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY & PARTICIONAMENTO DINÂMICO 128MB)
   Bucket de Destino: s3://tc-alfabetizacao-869769158828-us-east-1/bronze
===========================================================================
[2026-10-28 19:20:40] 🔍 [Entidade ALUNOS] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.alunos'...
[2026-10-28 19:20:40] 
[2026-10-28 19:20:40] 🔍 [Entidade UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.uf'...
[2026-10-28 19:20:40] 
[2026-10-28 19:20:40] 🔍 [Entidade MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.municipio'...
[2026-10-28 19:20:40] 
[2026-10-28 19:20:40] 🔍 [Entidade META_ALFABETIZACAO_BRASIL] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil'...
[2026-10-28 19:20:40] 
[2026-10-28 19:20:40] 🔍 [Entidade META_ALFABETIZACAO_UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf'...
[2026-10-28 19:20:40] 
[2026-10-28 19:20:41] ℹ️ [UF] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:20:41] 🔍 [Entidade META_ALFABETIZACAO_MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio'...
[2026-10-28 19:20:41] 
[2026-10-28 19:20:41] 📦 [META_ALFABETIZACAO_BRASIL] Nova versão detectada (2026-03-24). Lendo BigQuery...
[2026-10-28 19:20:41] ℹ️ [ALUNOS] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:20:41] 🔍 [Entidade DICIONARIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.dicionario'...
[2026-10-28 19:20:41] ℹ️ [META_ALFABETIZACAO_UF] Dados já atualizados para 2026-03-24. Ingestão SKIPPED.
[2026-10-28 19:20:41] ℹ️ [MUNICIPIO] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:20:41] ℹ️ [META_ALFABETIZACAO_MUNICIPIO] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:20:41] ℹ️ [DICIONARIO] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:20:46] └─ 💾 [META_ALFABETIZACAO_BRASIL] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_brasil/
[2026-10-28 19:20:52] └─ ⚡ [META_ALFABETIZACAO_BRASIL] Mantido particionamento Spark: 1 partição(ões) (0.00 MB).
[2026-10-28 19:21:41] └─ ✅ [META_ALFABETIZACAO_BRASIL] Gravada na Camada Bronze com sucesso (3 linhas)!
[2026-10-28 19:21:41] ===========================================================================
RESUMO DA INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY):
 - alunos                        : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - uf                            : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - municipio                     : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - meta_alfabetizacao_brasil     : SUCCEEDED (3 linhas ingeridas)
 - meta_alfabetizacao_uf         : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - meta_alfabetizacao_municipio  : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - dicionario                    : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
===========================================================================
✅ Camada Bronze concluída com sucesso!
[2026-10-28 19:21:41] 📜 [Auditoria & Governança] Manifesto salvo no S3 com sucesso:
   └─ s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_656f9cbc6655bf832a5c1cb7b9b84213cb7c8099ce631ce44ad05714ba560fde/manifest.json
```

---

## 5. ⚖️ Comparativo de Contagem: Origem (BigQuery) vs. Destino (S3 Bronze)
| Entidade | Linhas no BigQuery (Metadados) | Linhas Gravadas no S3 | Status Ingestão | Particionamento & Tamanho |
| :--- | :---: | :---: | :---: | :---: |
| `alunos` | 3,867,999 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (256.1 MB) |
| `uf` | 145 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `municipio` | 23,995 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (1.75 MB) |
| `meta_alfabetizacao_brasil` | 3 | 3 | `SUCCEEDED` | 1 part. (0.0 MB) |
| `meta_alfabetizacao_uf` | 81 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `meta_alfabetizacao_municipio` | 10,704 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (1.1 MB) |
| `dicionario` | 27 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.0 MB) |

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
