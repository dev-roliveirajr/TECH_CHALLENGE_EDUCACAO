# 📊 Relatório de Execução do Job: `02_silver_processing`

## 1. ℹ️ Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-10-29 15:04:48 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `02_silver_processing`
* **JobRunId:** `jr_c9d95a429b151d819676be710e7f8de7583bea774617782657f7a6d64ff92e32`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-10-29/run_id=jr_c9d95a429b151d819676be710e7f8de7583bea774617782657f7a6d64ff92e32/manifest.json`

---

## 2. 💰 Métricas de Desempenho e FinOps (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **244 segundos** (~4.07 min) |
| **DPU-Horas Medidas** | **0.1356 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0596** |
| **Início / Término** | 2026-10-29 15:00:32 UTC ➔ 2026-10-29 15:04:48 UTC |

---

## 3. 🌐 Consumo e Origem dos Dados
* **Camada de Origem:** `Camada Bronze (Parquet / S3 via Manifesto)`
* **Custo de Leitura / Processamento:** **US$ 0,00 Adicional** *(Incluso no pipeline serverless)*
* **Estratégia de Governança:** Conexão 100% orientada por manifestos de auditoria JSON.

---

## 4. 📜 Saída da Aplicação (CloudWatch Stdout)
```text
[2026-10-29 15:01:28] ===========================================================================
🚀 INICIANDO CAMADA SILVER - DEDUPLICAÇÃO POR JANELA, EDA E JOINs
   Bucket: tc-alfabetizacao-869769158828-us-east-1
   Limiar de Join: 95.0% | Trava Bloqueante: True
===========================================================================
📥 1. Carregando tabelas da Camada Bronze...
[2026-10-29 15:02:33] ├─ [alunos] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/alunos/
[2026-10-29 15:02:34] ├─ [uf] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/uf/
[2026-10-29 15:02:34] ├─ [municipio] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/municipio/
[2026-10-29 15:02:35] ├─ [meta_alfabetizacao_brasil] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_brasil/
[2026-10-29 15:02:36] ├─ [meta_alfabetizacao_uf] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_uf/
[2026-10-29 15:02:36] ├─ [meta_alfabetizacao_municipio] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_municipio/
[2026-10-29 15:02:37] ├─ [dicionario] Carregado com sucesso de: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/dicionario/
[2026-10-29 15:02:37] ###########################################################################
🔬 INICIANDO ETAPA DE RAIO-X E ANÁLISE EXPLORATÓRIA (EDA INICIAL)
###########################################################################


===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'ALUNOS'
===========================================================================
[2026-10-29 15:02:42] 📊 Volume Total: 3,868,099 registros | 17 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ id_municipio                             : string
   ├─ id_escola                                : string
   ├─ id_aluno                                 : string
   ├─ caderno                                  : string
   ├─ serie                                    : string
   ├─ rede                                     : string
   ├─ presenca                                 : string
   ├─ preenchimento_caderno                    : string
   ├─ alfabetizado                             : string
   ├─ proficiencia                             : double
   ├─ peso_aluno                               : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:02:43] | ano | id_municipio | id_escola | id_aluno | caderno | serie | rede | presenca | preenchimento_caderno | alfabetizado | proficiencia | peso_aluno | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
[2026-10-29 15:02:43] | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2023 | 1302504 | 60000951 | 13015851 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-10-28T22:05:39.196644 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1302603 | 60000963 | 13030738 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-10-28T22:05:39.196644 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1300631 | 60001351 | 13003982 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-10-28T22:05:39.196644 | basedosdados_bigquery | 2026 | 1 | 14 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:02:44] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:02:46] ├─ id_municipio                             : 0 nulos (0.00%)
[2026-10-29 15:02:47] ├─ id_escola                                : 0 nulos (0.00%)
[2026-10-29 15:02:47] ├─ id_aluno                                 : 0 nulos (0.00%)
[2026-10-29 15:02:48] ├─ caderno                                  : 0 nulos (0.00%)
[2026-10-29 15:02:49] ├─ serie                                    : 0 nulos (0.00%)
[2026-10-29 15:02:50] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:02:50] ├─ presenca                                 : 0 nulos (0.00%)
[2026-10-29 15:02:51] ├─ preenchimento_caderno                    : 0 nulos (0.00%)
[2026-10-29 15:02:52] ├─ alfabetizado                             : 0 nulos (0.00%)
[2026-10-29 15:02:53] ├─ proficiencia                             : 513,438 nulos (13.27%)
[2026-10-29 15:02:53] ├─ peso_aluno                               : 513,438 nulos (13.27%)
[2026-10-29 15:02:54] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:02:55] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:02:55] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:02:56] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:02:56] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:07] ├─ Duplicatas Exatas (Linha Inteira) : 100 (0.00%)
[2026-10-29 15:03:12] ├─ Duplicatas por Chave Negocial ['id_aluno', 'id_municipio', 'ano', 'rede', 'serie']: 100 (0.00%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'UF'
===========================================================================
[2026-10-29 15:03:13] 📊 Volume Total: 145 registros | 20 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ sigla_uf                                 : string
   ├─ serie                                    : string
   ├─ rede                                     : string
   ├─ taxa_alfabetizacao                       : double
   ├─ media_portugues                          : double
   ├─ proporcao_aluno_nivel_0                  : double
   ├─ proporcao_aluno_nivel_1                  : double
   ├─ proporcao_aluno_nivel_2                  : double
   ├─ proporcao_aluno_nivel_3                  : double
   ├─ proporcao_aluno_nivel_4                  : double
   ├─ proporcao_aluno_nivel_5                  : double
   ├─ proporcao_aluno_nivel_6                  : double
   ├─ proporcao_aluno_nivel_7                  : double
   ├─ proporcao_aluno_nivel_8                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:13] | ano | sigla_uf | serie | rede | taxa_alfabetizacao | media_portugues | proporcao_aluno_nivel_0 | proporcao_aluno_nivel_1 | proporcao_aluno_nivel_2 | proporcao_aluno_nivel_3 | proporcao_aluno_nivel_4 | proporcao_aluno_nivel_5 | proporcao_aluno_nivel_6 | proporcao_aluno_nivel_7 | proporcao_aluno_nivel_8 | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2023 | AM | 2 | 3 | 49.2 | 733.6637 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.895189 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | PB | 2 | 2 | 55.23 | 744.8152 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.895189 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | PR | 2 | 5 | 73.12 | 757.2146 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.895189 | basedosdados_bigquery | 2026 | 1 | 14 |

⚠️ Diagnóstico d
[2026-10-29 15:03:13] e NULOs por Coluna:
[2026-10-29 15:03:13] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:03:13] ├─ sigla_uf                                 : 0 nulos (0.00%)
[2026-10-29 15:03:14] ├─ serie                                    : 0 nulos (0.00%)
[2026-10-29 15:03:14] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:03:14] ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
[2026-10-29 15:03:14] ├─ media_portugues                          : 0 nulos (0.00%)
[2026-10-29 15:03:14] ├─ proporcao_aluno_nivel_0                  : 70 nulos (48.28%)
[2026-10-29 15:03:15] ├─ proporcao_aluno_nivel_1                  : 70 nulos (48.28%)
[2026-10-29 15:03:15] ├─ proporcao_aluno_nivel_2                  : 70 nulos (48.28%)
[2026-10-29 15:03:15] ├─ proporcao_aluno_nivel_3                  : 70 nulos (48.28%)
[2026-10-29 15:03:15] ├─ proporcao_aluno_nivel_4                  : 70 nulos (48.28%)
[2026-10-29 15:03:16] ├─ proporcao_aluno_nivel_5                  : 70 nulos (48.28%)
[2026-10-29 15:03:16] ├─ proporcao_aluno_nivel_6                  : 70 nulos (48.28%)
[2026-10-29 15:03:16] ├─ proporcao_aluno_nivel_7                  : 70 nulos (48.28%)
[2026-10-29 15:03:16] ├─ proporcao_aluno_nivel_8                  : 70 nulos (48.28%)
[2026-10-29 15:03:17] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:17] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:17] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:17] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:17] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:18] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-10-29 15:03:18] ├─ Duplicatas por Chave Negocial ['sigla_uf', 'ano', 'rede', 'serie']: 0 (0.00%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'MUNICIPIO'
===========================================================================
[2026-10-29 15:03:19] 📊 Volume Total: 24,045 registros | 20 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ id_municipio                             : string
   ├─ serie                                    : string
   ├─ rede                                     : string
   ├─ taxa_alfabetizacao                       : double
   ├─ media_portugues                          : double
   ├─ proporcao_aluno_nivel_0                  : double
   ├─ proporcao_aluno_nivel_1                  : double
   ├─ proporcao_aluno_nivel_2                  : double
   ├─ proporcao_aluno_nivel_3                  : double
   ├─ proporcao_aluno_nivel_4                  : double
   ├─ proporcao_aluno_nivel_5                  : double
   ├─ proporcao_aluno_nivel_6                  : double
   ├─ proporcao_aluno_nivel_7                  : double
   ├─ proporcao_aluno_nivel_8                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:19] | ano | id_municipio | serie | rede | taxa_alfabetizacao | media_portugues | proporcao_aluno_nivel_0 | proporcao_aluno_nivel_1 | proporcao_aluno_nivel_2 | proporcao_aluno_nivel_3 | proporcao_aluno_nivel_4 | proporcao_aluno_nivel_5 | proporcao_aluno_nivel_6 | proporcao_aluno_nivel_7 | proporcao_aluno_nivel_8 | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2023 | 1100031 | 2 | 3 | 69.1 | 767.8763 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.893854 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-10-29 15:03:19] | 2023 | 1100072 | 2 | 3 | 58.2 | 747.8918 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.893854 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1100189 | 2 | 5 | 69.73 | 762.4062 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.893854 | basedosdados_bigquery | 2026 | 1 | 14 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:03:19] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:03:20] ├─ id_municipio                             : 0 nulos (0.00%)
[2026-10-29 15:03:20] ├─ serie                                    : 0 nulos (0.00%)
[2026-10-29 15:03:21] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:03:21] ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
[2026-10-29 15:03:21] ├─ media_portugues                          : 0 nulos (0.00%)
[2026-10-29 15:03:22] ├─ proporcao_aluno_nivel_0                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:22] ├─ proporcao_aluno_nivel_1                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:23] ├─ proporcao_aluno_nivel_2                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:23] ├─ proporcao_aluno_nivel_3                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:24] ├─ proporcao_aluno_nivel_4                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:24] ├─ proporcao_aluno_nivel_5                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:24] ├─ proporcao_aluno_nivel_6                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:25] ├─ proporcao_aluno_nivel_7                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:25] ├─ proporcao_aluno_nivel_8                  : 11,597 nulos (48.23%)
[2026-10-29 15:03:26] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:26] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:26] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:27] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:27] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:28] ├─ Duplicatas Exatas (Linha Inteira) : 50 (0.21%)
[2026-10-29 15:03:29] ├─ Duplicatas por Chave Negocial ['id_municipio', 'ano', 'rede', 'serie']: 50 (0.21%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'META_ALFABETIZACAO_BRASIL'
===========================================================================
[2026-10-29 15:03:29] 📊 Volume Total: 3 registros | 16 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ rede                                     : string
[2026-10-29 15:03:29] ├─ taxa_alfabetizacao                       : double
   ├─ meta_alfabetizacao_2024                  : double
   ├─ meta_alfabetizacao_2025                  : double
   ├─ meta_alfabetizacao_2026                  : double
   ├─ meta_alfabetizacao_2027                  : double
   ├─ meta_alfabetizacao_2028                  : double
   ├─ meta_alfabetizacao_2029                  : double
   ├─ meta_alfabetizacao_2030                  : double
   ├─ percentual_participacao                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int
[2026-10-29 15:03:29] 👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:29] | ano | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2025 | Pública | 66.0 | 60.0 | 64.0 | 67.0 | 71.0 | 74.0 | 77.0 | 80.0 | 88.0 | 2026-10-28T22:20:46.548512 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2024 | Pública | 59.2 | 59.9 | 63.77 | 67.47 | 70.97 | 74.23 | 77.24 | 80.0 | 87.37 | 2026-10-28T22:20:46.548512 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2023 | Pública | 55.9 | 59.9 | 63.77 | 67.47 | 70.97 | 74.23 | 77.24 | 80.0 | 86.0 | 2026-10-28T22:20:46.548512 | basedosdados_bigquery | 2026 | 3 | 24 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:03:29] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:03:29] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:03:30] ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
[2026-10-29 15:03:30] ├─ meta_alfabetizacao_2024                  : 0 nulos (0.00%)
[2026-10-29 15:03:30] ├─ meta_alfabetizacao_2025                  : 0 nulos (0.00%)
[2026-10-29 15:03:30] ├─ meta_alfabetizacao_2026                  : 0 nulos (0.00%)
[2026-10-29 15:03:30] ├─ meta_alfabetizacao_2027                  : 0 nulos (0.00%)
[2026-10-29 15:03:31] ├─ meta_alfabetizacao_2028                  : 0 nulos (0.00%)
[2026-10-29 15:03:31] ├─ meta_alfabetizacao_2029                  : 0 nulos (0.00%)
[2026-10-29 15:03:31] ├─ meta_alfabetizacao_2030                  : 0 nulos (0.00%)
[2026-10-29 15:03:31] ├─ percentual_participacao                  : 0 nulos (0.00%)
[2026-10-29 15:03:31] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:32] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:32] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:32] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:32] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:32] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-10-29 15:03:33] ├─ Duplicatas por Chave Negocial ['ano', 'rede']: 0 (0.00%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'META_ALFABETIZACAO_UF'
===========================================================================
[2026-10-29 15:03:33] 📊 Volume Total: 81 registros | 17 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ sigla_uf                                 : string
   ├─ rede                                     : string
   ├─ taxa_alfabetizacao                       : double
   ├─ meta_alfabetizacao_2024                  : double
   ├─ meta_alfabetizacao_2025                  : double
   ├─ meta_alfabetizacao_2026                  : double
   ├─ meta_alfabetizacao_2027                  : double
   ├─ meta_alfabetizacao_2028                  : double
   ├─ meta_alfabetizacao_2029                  : double
   ├─ meta_alfabetizacao_2030                  : double
   ├─ percentual_participacao                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes
[2026-10-29 15:03:33] : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:33] | ano | sigla_uf | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
[2026-10-29 15:03:33] | 2024 | RR | Pública | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.899207 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2023 | RR | Pública | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-10-28T21:46:14.899207 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2024 | SE | Pública | 38.39 | 38.3 | 45.9 | 53.6 | 61.2 | 68.3 | 74.6 | 80.0 | 92.84 | 2026-10-28T21:46:14.899207 | basedosdados_bigquery | 2026 | 3 | 24 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:03:33] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:03:33] ├─ sigla_uf                                 : 0 nulos (0.00%)
[2026-10-29 15:03:34] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:03:34] ├─ taxa_alfabetizacao                       : 4 nulos (4.94%)
[2026-10-29 15:03:34] ├─ meta_alfabetizacao_2024                  : 9 nulos (11.11%)
[2026-10-29 15:03:34] ├─ meta_alfabetizacao_2025                  : 3 nulos (3.70%)
[2026-10-29 15:03:34] ├─ meta_alfabetizacao_2026                  : 2 nulos (2.47%)
[2026-10-29 15:03:35] ├─ meta_alfabetizacao_2027                  : 2 nulos (2.47%)
[2026-10-29 15:03:35] ├─ meta_alfabetizacao_2028                  : 2 nulos (2.47%)
[2026-10-29 15:03:35] ├─ meta_alfabetizacao_2029                  : 2 nulos (2.47%)
[2026-10-29 15:03:35] ├─ meta_alfabetizacao_2030                  : 2 nulos (2.47%)
[2026-10-29 15:03:35] ├─ percentual_participacao                  : 4 nulos (4.94%)
[2026-10-29 15:03:36] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:36] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:36] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:36] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:36] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:37] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-10-29 15:03:37] ├─ Duplicatas por Chave Negocial ['sigla_uf', 'ano', 'rede']: 0 (0.00%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'META_ALFABETIZACAO_MUNICIPIO'
===========================================================================
[2026-10-29 15:03:37] 📊 Volume Total: 21,408 registros | 18 colunas

📋 Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
   ├─ id_municipio                             : string
   ├─ rede                                     : string
   ├─ taxa_alfabetizacao                       : double
   ├─ meta_alfabetizacao_2024                  : double
   ├─ meta_alfabetizacao_2025                  : double
   ├─ meta_alfabetizacao_2026                  : double
   ├─ meta_alfabetizacao_2027                  : double
   ├─ meta_alfabetizacao_2028                  : double
   ├─ meta_alfabetizacao_2029                  : double
   ├─ meta_alfabetizacao_2030                  : double
   ├─ nivel_alfabetizacao                      : bigint
   ├─ percentual_participacao                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano
[2026-10-29 15:03:37] : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:37] | ano | id_municipio | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | nivel_alfabetizacao | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
[2026-10-29 15:03:37] | 2023 | 4301750 | Municipal | NULL | NULL | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | NULL | NULL | 2026-10-28T21:46:14.893546 | basedosdados_bigquery | 2026 | 1 | 3 |
[2026-10-29 15:03:37] | 2024 | 4301750 | Municipal | 4.4 | NULL | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | 0 | 92.59 | 2026-10-28T21:46:14.893546 | basedosdados_bigquery | 2026 | 1 | 3 |
[2026-10-29 15:03:37] | 2024 | 2406908 | Municipal | 42.86 | 7.94 | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | 1 | 84.0 | 2026-10-28T21:46:14.893546 | basedosdados_bigquery | 2026 | 1 | 3 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:03:38] ├─ ano                                      : 0 nulos (0.00%)
[2026-10-29 15:03:38] ├─ id_municipio                             : 0 nulos (0.00%)
[2026-10-29 15:03:38] ├─ rede                                     : 0 nulos (0.00%)
[2026-10-29 15:03:38] ├─ taxa_alfabetizacao                       : 240 nulos (1.12%)
[2026-10-29 15:03:38] ├─ meta_alfabetizacao_2024                  : 480 nulos (2.24%)
[2026-10-29 15:03:39] ├─ meta_alfabetizacao_2025                  : 0 nulos (0.00%)
[2026-10-29 15:03:39] ├─ meta_alfabetizacao_2026                  : 0 nulos (0.00%)
[2026-10-29 15:03:39] ├─ meta_alfabetizacao_2027                  : 0 nulos (0.00%)
[2026-10-29 15:03:39] ├─ meta_alfabetizacao_2028                  : 0 nulos (0.00%)
[2026-10-29 15:03:39] ├─ meta_alfabetizacao_2029                  : 0 nulos (0.00%)
[2026-10-29 15:03:40] ├─ meta_alfabetizacao_2030                  : 0 nulos (0.00%)
[2026-10-29 15:03:40] ├─ nivel_alfabetizacao                      : 240 nulos (1.12%)
[2026-10-29 15:03:40] ├─ percentual_participacao                  : 240 nulos (1.12%)
[2026-10-29 15:03:40] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:41] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:41] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:41] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:41] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:41] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-10-29 15:03:42] ├─ Duplicatas por Chave Negocial ['id_municipio', 'ano', 'rede']: 10,704 (50.00%)
===========================================================================

===========================================================================
🔬 [RAIO-X / EDA INICIAL] Tabela: 'DICIONARIO'
===========================================================================
[2026-10-29 15:03:42] 📊 Volume Total: 27 registros | 10 colunas

📋 Schema / Estrutura de Tipos:
   ├─ id_tabela                                : string
[2026-10-29 15:03:42] ├─ nome_coluna                              : string
   ├─ chave                                    : string
   ├─ cobertura_temporal                       : string
   ├─ valor                                    : string
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

👀 Preview das 3 primeiras linhas (Tabela Markdown):
[2026-10-29 15:03:42] | id_tabela | nome_coluna | chave | cobertura_temporal | valor | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| alunos | alfabetizado | 0 | (1) | Não | 2026-10-28T21:46:56.408304 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-10-29 15:03:42] | alunos | alfabetizado | 1 | (1) | Sim | 2026-10-28T21:46:56.408304 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-10-29 15:03:42] | alunos | preenchimento_caderno | 0 | (1) | Prova não preenchida | 2026-10-28T21:46:56.408304 | basedosdados_bigquery | 2026 | 1 | 14 |

⚠️ Diagnóstico de NULOs por Coluna:
[2026-10-29 15:03:42] ├─ id_tabela                                : 0 nulos (0.00%)
[2026-10-29 15:03:42] ├─ nome_coluna                              : 0 nulos (0.00%)
[2026-10-29 15:03:43] ├─ chave                                    : 0 nulos (0.00%)
[2026-10-29 15:03:43] ├─ cobertura_temporal                       : 0 nulos (0.00%)
[2026-10-29 15:03:43] ├─ valor                                    : 0 nulos (0.00%)
[2026-10-29 15:03:43] ├─ _ingested_at                             : 0 nulos (0.00%)
[2026-10-29 15:03:43] ├─ _source                                  : 0 nulos (0.00%)
[2026-10-29 15:03:44] ├─ ref_ano                                  : 0 nulos (0.00%)
[2026-10-29 15:03:44] ├─ ref_mes                                  : 0 nulos (0.00%)
[2026-10-29 15:03:44] ├─ ref_dia                                  : 0 nulos (0.00%)

🔄 Diagnóstico de Duplicidades:
[2026-10-29 15:03:44] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-10-29 15:03:44] ├─ Duplicatas por Chave Negocial ['id_tabela', 'nome_coluna', 'chave']: 0 (0.00%)
===========================================================================

###########################################################################
🧹 3. TRATAMENTO PARA TABELAS COM CHAVES DE NEGOCIO DUPLICADAS.
###########################################################################


Aplicando Deduplicação na Tabela alunos...
[2026-10-29 15:03:52] 🧹 [DEDUPLICAÇÃO DETERMINÍSTICA POR JANELA]
   ├─ Registros Iniciais : 3,868,099
   ├─ Registros Finais   : 3,867,999
   └─ Registros Removidos: 100

Aplicando Deduplicação na Tabela municipio...
[2026-10-29 15:03:53] 🧹 [DEDUPLICAÇÃO DETERMINÍSTICA POR JANELA]
   ├─ Registros Iniciais : 24,045
   ├─ Registros Finais   : 23,995
   └─ Registros Removidos: 50

Aplicando Deduplicação na Tabela meta_alfabetizacao_municipio...
[2026-10-29 15:03:54] 🧹 [DEDUPLICAÇÃO DETERMINÍSTICA POR JANELA]
   ├─ Registros Iniciais : 21,408
   ├─ Registros Finais   : 10,704
   └─ Registros Removidos: 10,704
[2026-10-29 15:03:54] 🧹 4. Executando Limpeza, Padronização e Eliminação de Colisão de Métricas...
[2026-10-29 15:03:54] └─ 🗺️ [Derivação UF] Coluna 'sigla_uf' criada em 'alunos' via código IBGE.
[2026-10-29 15:03:54] 🔗 5. Executando O GRANDE JOIN Relacional (Sem Explosão nem Ambiguidade)...
[2026-10-29 15:03:54] 🛡️ 6. Validando Gate de Qualidade (check_join_ratio)...
[2026-10-29 15:04:07] ======================================================================
🔍 [GATE DE QUALIDADE - TAXA DE MATCH DO JOIN]
   ├─ Registros na Origem (Fato/Esquerda): 3,867,999
   ├─ Registros Combinados com Sucesso   : 3,867,999
   ├─ Taxa de Correspondência Medida     : 100.00%
   └─ Limiar Mínimo Requerido (Gate)     : 95.00%
======================================================================
[2026-10-29 15:04:07] ✅ [GATE APROVADO] Taxa de JOIN dentro do padrão estipulado (100.00% >= 95.00%).
[2026-10-29 15:04:07] 💾 7. Gravando dataset curado em: s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/
[2026-10-29 15:04:36] └─ ✅ Dataset curado gravado na Camada Silver com sucesso!
[2026-10-29 15:04:37] 📜 [Auditoria Silver] Manifesto salvo no S3 com sucesso: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-10-29/run_id=local_run/manifest.json

===========================================================================
✅ CAMADA SILVER CONCLUÍDA COM SUCESSO DE PONTA A PONTA!
===========================================================================
```

---

## 5. 🛡️ Auditoria Qualitativa e Estado Final da Camada Silver
| Métrica de Curadoria / Qualidade | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas de Alunos (Entrada)** | **3,867,999** | Registros brutos lidos do manifesto Bronze |
| **Linhas Curadas (Saída)** | **3,867,999** | Registros finais enriquecidos e validados |
| **Taxa de Match do JOIN** | **100.00%** | Percentual de alinhamento com dimensões territoriais |
| **Status do Gate de Qualidade** | **✅ APROVADO (>= 95.0%)** | Trava bloqueante da pipeline (check_join_ratio) |
| **Tratamento de NULOs (Saeb)** | **Preservado** | Nenhuma atribuição de zero em proficiências/notas |
| **Status das Metas Educacionais** | **Tripla Categorização** | 'Atingiu', 'Não Atingiu' ou 'Sem Meta Cadastrada' |
| **S3 Prefix Final (Catálogo)** | `s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/` | Formato Parquet (Snappy) particionado por data |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
