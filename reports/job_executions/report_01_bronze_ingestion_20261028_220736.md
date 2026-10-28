# 📊 Relatório de Execução do Job: `01_bronze_ingestion`

## 1. ℹ️ Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-10-28 19:07:33 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `01_bronze_ingestion`
* **JobRunId:** `jr_e34a02d8d802a31e27d8be30563e6f51f7a480f85a272ef4641dbb04e18c53d9`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_e34a02d8d802a31e27d8be30563e6f51f7a480f85a272ef4641dbb04e18c53d9/manifest.json`

---

## 2. 💰 Métricas de Desempenho e FinOps (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **181 segundos** (~3.02 min) |
| **DPU-Horas Medidas** | **0.1006 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0442** |
| **Início / Término** | 2026-10-28 19:03:48 UTC ➔ 2026-10-28 19:07:33 UTC |

---

## 3. 🌐 Consumo e Origem GCP BigQuery
* **Dataset Origem:** `basedosdados.br_inep_avaliacao_alfabetizacao` (7 Entidades Oficiais)
* **Custo de Leitura BigQuery (On-Demand):** **US$ 0,00** *(Até 1 TB/mês no Free Tier)*
* **Estratégia de Segurança:** Credenciais recuperadas 100% em memória RAM via AWS Secrets Manager (`pipeline-educacao/gcp/bigquery`). Zero arquivos em disco (`/tmp`).

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
```text
[2026-10-28 19:05:32] 🔐 [SecOps] Buscando Secret 'pipeline-educacao/gcp/bigquery' no AWS Secrets Manager (In-Memory)...
[2026-10-28 19:05:32] └─ ✅ Credenciais obtidas em memória RAM via Secrets Manager ('pipeline-educacao/gcp/bigquery'). Zero disco utilizado.
[2026-10-28 19:05:32] 🔑 Autenticação GCP ativada 100% In-Memory (RAM).
[2026-10-28 19:05:32] ===========================================================================
🚀 INICIANDO INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY & PARTICIONAMENTO DINÂMICO 128MB)
   Bucket de Destino: s3://tc-alfabetizacao-869769158828-us-east-1/bronze
===========================================================================
[2026-10-28 19:05:32] 🔍 [Entidade ALUNOS] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.alunos'...
[2026-10-28 19:05:32] 
[2026-10-28 19:05:32] 🔍 [Entidade UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.uf'...
[2026-10-28 19:05:32] 
[2026-10-28 19:05:32] 🔍 [Entidade MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.municipio'...
[2026-10-28 19:05:32] 🔍 [Entidade META_ALFABETIZACAO_BRASIL] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil'...
[2026-10-28 19:05:32] 🔍 [Entidade META_ALFABETIZACAO_UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf'...
[2026-10-28 19:05:32] 
[2026-10-28 19:05:33] ℹ️ [META_ALFABETIZACAO_UF] Dados já atualizados para 2026-03-24. Ingestão SKIPPED.
[2026-10-28 19:05:33] ℹ️ [META_ALFABETIZACAO_BRASIL] Dados já atualizados para 2026-03-24. Ingestão SKIPPED.
[2026-10-28 19:05:33] 🔍 [Entidade META_ALFABETIZACAO_MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio'...
[2026-10-28 19:05:33] 
[2026-10-28 19:05:33] 
[2026-10-28 19:05:33] 🔍 [Entidade DICIONARIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.dicionario'...
[2026-10-28 19:05:33] ℹ️ [UF] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:05:33] 
[2026-10-28 19:05:33] 📦 [ALUNOS] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 19:05:33] ℹ️ [MUNICIPIO] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:05:33] 📦 [META_ALFABETIZACAO_MUNICIPIO] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 19:05:33] ℹ️ [DICIONARIO] Dados já atualizados para 2026-01-14. Ingestão SKIPPED.
[2026-10-28 19:05:39] └─ 💾 [META_ALFABETIZACAO_MUNICIPIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_municipio/
[2026-10-28 19:05:39] └─ 💾 [ALUNOS] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/alunos/
[2026-10-28 19:05:44] └─ ⚡ [META_ALFABETIZACAO_MUNICIPIO] Mantido particionamento Spark: 1 partição(ões) (1.10 MB).
[2026-10-28 19:05:44] └─ ⚡ [ALUNOS] Coalesce dinâmico aplicado: 6 ➔ 3 partição(ões) (256.10 MB / 128.0 MB alvo).
[2026-10-28 19:07:18] └─ ✅ [META_ALFABETIZACAO_MUNICIPIO] Gravada na Camada Bronze com sucesso (10,704 linhas)!
[2026-10-28 19:07:22] └─ ✅ [ALUNOS] Gravada na Camada Bronze com sucesso (3,867,999 linhas)!
[2026-10-28 19:07:22] ===========================================================================
RESUMO DA INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY):
 - alunos                        : SUCCEEDED (3,867,999 linhas ingeridas)
 - uf                            : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - municipio                     : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - meta_alfabetizacao_brasil     : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - meta_alfabetizacao_uf         : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
 - meta_alfabetizacao_municipio  : SUCCEEDED (10,704 linhas ingeridas)
 - dicionario                    : SKIPPED (Already Up-to-Date) (Already Up-to-Date)
===========================================================================
✅ Camada Bronze concluída com sucesso!
[2026-10-28 19:07:22] 📜 [Auditoria & Governança] Manifesto salvo no S3 com sucesso:
   └─ s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_e34a02d8d802a31e27d8be30563e6f51f7a480f85a272ef4641dbb04e18c53d9/manifest.json
```

---

## 5. ⚖️ Comparativo de Contagem: Origem (BigQuery) vs. Destino (S3 Bronze)
| Entidade | Linhas no BigQuery (Metadados) | Linhas Gravadas no S3 | Status Ingestão | Particionamento & Tamanho |
| :--- | :---: | :---: | :---: | :---: |
| `alunos` | 3,867,999 | 3,867,999 | `SUCCEEDED` | 3 part. (256.1 MB) |
| `uf` | 145 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `municipio` | 23,995 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (1.75 MB) |
| `meta_alfabetizacao_brasil` | 3 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.0 MB) |
| `meta_alfabetizacao_uf` | 81 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.01 MB) |
| `meta_alfabetizacao_municipio` | 10,704 | 10,704 | `SUCCEEDED` | 1 part. (1.1 MB) |
| `dicionario` | 27 | 0 (Already Up-to-Date) | `SKIPPED (Already Up-to-Date)` | 0 part. (0.0 MB) |

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
