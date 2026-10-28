# 📊 Relatório de Execução do Job: `01_bronze_ingestion`

## 1. ℹ️ Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-10-28 18:47:10 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `01_bronze_ingestion`
* **JobRunId:** `jr_bae3d0df40d1a3ad3b2a6be041307fe7ad5bbbb88d9b7b59921ff925e3066a9d`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_bae3d0df40d1a3ad3b2a6be041307fe7ad5bbbb88d9b7b59921ff925e3066a9d/manifest.json`

---

## 2. 💰 Métricas de Desempenho e FinOps (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **114 segundos** (~1.90 min) |
| **DPU-Horas Medidas** | **0.0633 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0279** |
| **Início / Término** | 2026-10-28 18:45:10 UTC ➔ 2026-10-28 18:47:10 UTC |

---

## 3. 🌐 Consumo e Origem GCP BigQuery
* **Dataset Origem:** `basedosdados.br_inep_avaliacao_alfabetizacao` (7 Entidades Oficiais)
* **Custo de Leitura BigQuery (On-Demand):** **US$ 0,00** *(Até 1 TB/mês no Free Tier)*
* **Estratégia de Segurança:** Credenciais recuperadas 100% em memória RAM via AWS Secrets Manager (`pipeline-educacao/gcp/bigquery`). Zero arquivos em disco (`/tmp`).

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
```text
[2026-10-28 18:46:08] 🔐 [SecOps] Buscando Secret 'pipeline-educacao/gcp/bigquery' no AWS Secrets Manager (In-Memory)...
[2026-10-28 18:46:08] └─ ✅ Credenciais obtidas em memória RAM via Secrets Manager ('pipeline-educacao/gcp/bigquery'). Zero disco utilizado.
[2026-10-28 18:46:08] 🔑 Autenticação GCP ativada 100% In-Memory (RAM).
[2026-10-28 18:46:08] ===========================================================================
🚀 INICIANDO INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY & PARTICIONAMENTO DINÂMICO 128MB)
   Bucket de Destino: s3://tc-alfabetizacao-869769158828-us-east-1/bronze
===========================================================================
[2026-10-28 18:46:08] 🔍 [Entidade UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.uf'...
[2026-10-28 18:46:08] 
[2026-10-28 18:46:08] 🔍 [Entidade MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.municipio'...
[2026-10-28 18:46:08] 
[2026-10-28 18:46:08] 🔍 [Entidade META_ALFABETIZACAO_BRASIL] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil'...
[2026-10-28 18:46:08] 
[2026-10-28 18:46:08] 🔍 [Entidade META_ALFABETIZACAO_UF] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf'...
[2026-10-28 18:46:08] 
[2026-10-28 18:46:08] 🔍 [Entidade META_ALFABETIZACAO_MUNICIPIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio'...
[2026-10-28 18:46:08] 
[2026-10-28 18:46:09] 📦 [META_ALFABETIZACAO_UF] Nova versão detectada (2026-03-24). Lendo BigQuery...
[2026-10-28 18:46:09] 📦 [MUNICIPIO] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 18:46:09] 📦 [UF] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 18:46:09] 
[2026-10-28 18:46:09] 📦 [META_ALFABETIZACAO_BRASIL] Nova versão detectada (2026-03-24). Lendo BigQuery...
[2026-10-28 18:46:09] 📦 [META_ALFABETIZACAO_MUNICIPIO] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 18:46:15] └─ 💾 [META_ALFABETIZACAO_BRASIL] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_brasil/
[2026-10-28 18:46:15] └─ 💾 [META_ALFABETIZACAO_MUNICIPIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_municipio/
[2026-10-28 18:46:15] └─ 💾 [MUNICIPIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/municipio/
[2026-10-28 18:46:15] └─ 💾 [UF] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/uf/
[2026-10-28 18:46:15] └─ 💾 [META_ALFABETIZACAO_UF] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_uf/
[2026-10-28 18:46:21] └─ ⚡ [META_ALFABETIZACAO_UF] Mantido particionamento Spark: 1 partição(ões) (0.01 MB).
[2026-10-28 18:46:21] └─ ⚡ [MUNICIPIO] Mantido particionamento Spark: 1 partição(ões) (1.75 MB).
[2026-10-28 18:46:21] └─ ⚡ [META_ALFABETIZACAO_BRASIL] Mantido particionamento Spark: 1 partição(ões) (0.00 MB).
   └─ ⚡ [META_ALFABETIZACAO_MUNICIPIO] Mantido particionamento Spark: 1 partição(ões) (1.10 MB).
[2026-10-28 18:46:21] └─ ⚡ [UF] Mantido particionamento Spark: 1 partição(ões) (0.01 MB).
[2026-10-28 18:46:55] └─ ✅ [MUNICIPIO] Gravada na Camada Bronze com sucesso (23,995 linhas)!
[2026-10-28 18:46:55] 🔍 [Entidade DICIONARIO] Checando metadados no BigQuery: 'basedosdados.br_inep_avaliacao_alfabetizacao.dicionario'...
[2026-10-28 18:46:55] └─ ✅ [META_ALFABETIZACAO_MUNICIPIO] Gravada na Camada Bronze com sucesso (10,704 linhas)!
[2026-10-28 18:46:55] └─ ✅ [META_ALFABETIZACAO_BRASIL] Gravada na Camada Bronze com sucesso (3 linhas)!
[2026-10-28 18:46:55] └─ ✅ [META_ALFABETIZACAO_UF] Gravada na Camada Bronze com sucesso (81 linhas)!
[2026-10-28 18:46:55] └─ ✅ [UF] Gravada na Camada Bronze com sucesso (145 linhas)!
[2026-10-28 18:46:56] 📦 [DICIONARIO] Nova versão detectada (2026-01-14). Lendo BigQuery...
[2026-10-28 18:46:56] └─ 💾 [DICIONARIO] Gravando no S3 em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/dicionario/
[2026-10-28 18:46:57] └─ ⚡ [DICIONARIO] Mantido particionamento Spark: 1 partição(ões) (0.00 MB).
[2026-10-28 18:46:58] └─ ✅ [DICIONARIO] Gravada na Camada Bronze com sucesso (27 linhas)!
[2026-10-28 18:46:58] ===========================================================================
RESUMO DA INGESTÃO BRONZE (AUTENTICAÇÃO IN-MEMORY):
 - uf                            : SUCCEEDED (145 linhas ingeridas)
 - municipio                     : SUCCEEDED (23,995 linhas ingeridas)
 - meta_alfabetizacao_brasil     : SUCCEEDED (3 linhas ingeridas)
 - meta_alfabetizacao_uf         : SUCCEEDED (81 linhas ingeridas)
 - meta_alfabetizacao_municipio  : SUCCEEDED (10,704 linhas ingeridas)
 - dicionario                    : SUCCEEDED (27 linhas ingeridas)
===========================================================================
✅ Camada Bronze concluída com sucesso!
[2026-10-28 18:46:58] 📜 [Auditoria & Governança] Manifesto salvo no S3 com sucesso:
   └─ s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-10-28/run_id=jr_bae3d0df40d1a3ad3b2a6be041307fe7ad5bbbb88d9b7b59921ff925e3066a9d/manifest.json
```

---

## 5. ⚖️ Comparativo de Contagem: Origem (BigQuery) vs. Destino (S3 Bronze)
| Entidade | Linhas no BigQuery (Metadados) | Linhas Gravadas no S3 | Status Ingestão | Particionamento & Tamanho |
| :--- | :---: | :---: | :---: | :---: |
| `uf` | 145 | 145 | `SUCCEEDED` | 1 part. (0.01 MB) |
| `municipio` | 23,995 | 23,995 | `SUCCEEDED` | 1 part. (1.75 MB) |
| `meta_alfabetizacao_brasil` | 3 | 3 | `SUCCEEDED` | 1 part. (0.0 MB) |
| `meta_alfabetizacao_uf` | 81 | 81 | `SUCCEEDED` | 1 part. (0.01 MB) |
| `meta_alfabetizacao_municipio` | 10,704 | 10,704 | `SUCCEEDED` | 1 part. (1.1 MB) |
| `dicionario` | 27 | 27 | `SUCCEEDED` | 1 part. (0.0 MB) |

---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
