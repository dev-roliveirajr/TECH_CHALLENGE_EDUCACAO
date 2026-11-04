#  Relatório de Execução do Job: `02_silver_processing`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-03 23:46:58 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `02_silver_processing`
* **JobRunId:** `jr_2fdf42280493ad2b8a5b6d690010b93c0606c604894b4a3f5ec6c7d872c4ee17`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-11-03/run_id=jr_2fdf42280493ad2b8a5b6d690010b93c0606c604894b4a3f5ec6c7d872c4ee17/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **191 segundos** (~3.18 min) |
| **DPU-Horas Medidas** | **0.1061 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0467** |
| **Início / Término** | 2026-11-03 23:43:44 UTC ➔ 2026-11-03 23:46:58 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `Camada Bronze (Parquet / S3 via Manifesto)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-03 23:44:18 UTC] ===========================================================================
 Iniciando processamento da Camada Silver.
   Bucket: tc-alfabetizacao-869769158828-us-east-1
   Run ID: jr_2fdf42280493ad2b8a5b6d690010b93c0606c604894b4a3f5ec6c7d872c4ee17 | Data: 2026-11-03
   Limiar do JOIN: 95.0% | Interrupção por falha: True
===========================================================================
 1. Carregando o manifesto Bronze mais recente.
[2026-11-03 23:44:19 UTC] Manifesto Bronze validado: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_93d03c617a1d58f7f495ad4db9032ece3cf7b362cbe0093f041e67557a797d44/manifest.json
[2026-11-03 23:44:23 UTC] ├─ Lendo [alunos] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/alunos/ref_ano=2026/ref_mes=01/ref_dia=14/
[2026-11-03 23:45:10 UTC] ├─ Lendo [uf] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/uf/ref_ano=2026/ref_mes=01/ref_dia=14/
[2026-11-03 23:45:11 UTC] ├─ Lendo [municipio] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/municipio/ref_ano=2026/ref_mes=01/ref_dia=14/
[2026-11-03 23:45:12 UTC] ├─ Lendo [meta_alfabetizacao_brasil] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_brasil/ref_ano=2026/ref_mes=03/ref_dia=24/
[2026-11-03 23:45:12 UTC] ├─ Lendo [meta_alfabetizacao_uf] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_uf/ref_ano=2026/ref_mes=03/ref_dia=24/
[2026-11-03 23:45:12 UTC] ├─ Lendo [meta_alfabetizacao_municipio] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/meta_alfabetizacao_municipio/ref_ano=2026/ref_mes=01/ref_dia=14/
[2026-11-03 23:45:13 UTC] ├─ Lendo [dicionario] de s3://tc-alfabetizacao-869769158828-us-east-1/bronze/dicionario/ref_ano=2026/ref_mes=01/ref_dia=14/
[2026-11-03 23:45:14 UTC] ###########################################################################
 Análise exploratória das entidades de entrada
###########################################################################


===========================================================================
 Diagnóstico da tabela: 'ALUNOS'
===========================================================================
[2026-11-03 23:45:14 UTC] Schema / Estrutura de Tipos:
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

 Preview das primeiras linhas:
[2026-11-03 23:45:18 UTC] | ano | id_municipio | id_escola | id_aluno | caderno | serie | rede | presenca | preenchimento_caderno | alfabetizado | proficiencia | peso_aluno | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2023 | 1304062 | 60000459 | 13059733 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-11-03T23:42:17.810543+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1302603 | 60000712 | 13047236 | 1 | 2 | 2 | 0 | 0 | 0 | NULL | NULL | 2026-11-03T23:42:17.810543+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1303403 | 60001028 | 13053625 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-11-03T23:42:17.810543+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1502806 | 60003295 | 15041371 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-11-03T23:42:17.810543+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1500602 | 60003373 | 15006332 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL | 2026-11-03T23:42:17.8
[2026-11-03 23:45:18 UTC] 10543+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:23 UTC] Volume Total: 3,867,999 registros | 17 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ ano                                      : 0 nulos (0.00%)
   ├─ id_municipio                             : 0 nulos (0.00%)
   ├─ id_escola                                : 0 nulos (0.00%)
   ├─ id_aluno                                 : 0 nulos (0.00%)
   ├─ caderno                                  : 0 nulos (0.00%)
[2026-11-03 23:45:23 UTC] ├─ serie                                    : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ presenca                                 : 0 nulos (0.00%)
   ├─ preenchimento_caderno                    : 0 nulos (0.00%)
   ├─ alfabetizado                             : 0 nulos (0.00%)
   ├─ proficiencia                             : 513,338 nulos (13.27%)
   ├─ peso_aluno                               : 513,338 nulos (13.27%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:32 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:36 UTC] ├─ Duplicatas por Chave Negocial ['id_aluno', 'id_municipio', 'ano', 'rede', 'serie']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'UF'
===========================================================================
[2026-11-03 23:45:36 UTC] Schema / Estrutura de Tipos:
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
[2026-11-03 23:45:36 UTC] ├─ proporcao_aluno_nivel_6                  : double
   ├─ proporcao_aluno_nivel_7                  : double
   ├─ proporcao_aluno_nivel_8                  : double
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das primeiras linhas:
[2026-11-03 23:45:37 UTC] | ano | sigla_uf | serie | rede | taxa_alfabetizacao | media_portugues | proporcao_aluno_nivel_0 | proporcao_aluno_nivel_1 | proporcao_aluno_nivel_2 | proporcao_aluno_nivel_3 | proporcao_aluno_nivel_4 | proporcao_aluno_nivel_5 | proporcao_aluno_nivel_6 | proporcao_aluno_nivel_7 | proporcao_aluno_nivel_8 | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
[2026-11-03 23:45:37 UTC] | 2023 | AM | 2 | 3 | 49.2 | 733.6637 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810218+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:37 UTC] | 2023 | PB | 2 | 2 | 55.23 | 744.8152 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810218+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | PR | 2 | 5 | 73.12 | 757.2146 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810218+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | AP | 2 | 3 | 41.87 | 732.7858 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810218+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | PE | 2 | 5 | 58.95 | 747.4522 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810218+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:38 UTC] Volume Total: 145 registros | 20 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ ano                                      : 0 nulos (0.00%)
   ├─ sigla_uf                                 : 0 nulos (0.00%)
   ├─ serie                                    : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
   ├─ media_portugues                          : 0 nulos (0.00%)
   ├─ proporcao_aluno_nivel_0                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_1                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_2                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_3                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_4                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_5                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_6                  : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_7
[2026-11-03 23:45:38 UTC] : 70 nulos (48.28%)
   ├─ proporcao_aluno_nivel_8                  : 70 nulos (48.28%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:39 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:39 UTC] ├─ Duplicatas por Chave Negocial ['sigla_uf', 'ano', 'rede', 'serie']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'MUNICIPIO'
===========================================================================
[2026-11-03 23:45:39 UTC] Schema / Estrutura de Tipos:
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
   ├─ _source
[2026-11-03 23:45:39 UTC] : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das primeiras linhas:
[2026-11-03 23:45:39 UTC] | ano | id_municipio | serie | rede | taxa_alfabetizacao | media_portugues | proporcao_aluno_nivel_0 | proporcao_aluno_nivel_1 | proporcao_aluno_nivel_2 | proporcao_aluno_nivel_3 | proporcao_aluno_nivel_4 | proporcao_aluno_nivel_5 | proporcao_aluno_nivel_6 | proporcao_aluno_nivel_7 | proporcao_aluno_nivel_8 | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2023 | 1100031 | 2 | 3 | 69.1 | 767.8763 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810895+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1100072 | 2 | 3 | 58.2 | 747.8918 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810895+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1100189 | 2 | 5 | 69.73 | 762.4062 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810895+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1101609 | 2 | 3 | 50.7 | 745.6802 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810895+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| 2023 | 1101807 | 2 | 3 | 55.69 | 752.3724 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.810895+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:40 UTC] Volume Total: 23,995 registros | 20 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ ano                                      : 0 nulos (0.00%)
   ├─ id_municipio                             : 0 nulos (0.00%)
   ├─ serie                                    : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
   ├─ media_portugues                          : 0 nulos (0.00%)
   ├─ proporcao_aluno_nivel_0                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_1                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_2                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_3                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_4                  : 11,547 nulos (48.12%)
[2026-11-03 23:45:40 UTC] ├─ proporcao_aluno_nivel_5                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_6                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_7                  : 11,547 nulos (48.12%)
   ├─ proporcao_aluno_nivel_8                  : 11,547 nulos (48.12%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:41 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:42 UTC] ├─ Duplicatas por Chave Negocial ['id_municipio', 'ano', 'rede', 'serie']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'META_ALFABETIZACAO_BRASIL'
===========================================================================
[2026-11-03 23:45:42 UTC] Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
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
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das primeiras linhas:
[2026-11-03 23:45:42 UTC] | ano | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2025 | Pública | 66.0 | 60.0 | 64.0 | 67.0 | 71.0 | 74.0 | 77.0 | 80.0 | 88.0 | 2026-11-03T23:42:17.804291+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2024 | Pública | 59.2 | 59.9 | 63.77 | 67.47 | 70.97 | 74.23 | 77.24 | 80.0 | 87.37 | 2026-11-03T23:42:17.804291+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2023 | Pública | 55.9 | 59.9 | 63.77 | 67.47 | 70.97 | 74.23 | 77.24 | 80.0 | 86.0 | 2026-11-03T23:42:17.804291+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
[2026-11-03 23:45:43 UTC] Volume Total: 3 registros | 16 colunas

 Diagnóstico de NULOs por Coluna:
[2026-11-03 23:45:43 UTC] ├─ ano                                      : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ taxa_alfabetizacao                       : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2024                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2025                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2026                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2027                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2028                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2029                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2030                  : 0 nulos (0.00%)
   ├─ percentual_participacao                  : 0 nulos (0.00%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nul
[2026-11-03 23:45:43 UTC] os (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:43 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:44 UTC] ├─ Duplicatas por Chave Negocial ['ano', 'rede']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'META_ALFABETIZACAO_UF'
===========================================================================
[2026-11-03 23:45:44 UTC] Schema / Estrutura de Tipos:
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
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das primeiras linhas:
[2026-11-03 23:45:44 UTC] | ano | sigla_uf | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024 | RR | Pública | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.811908+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
[2026-11-03 23:45:44 UTC] | 2023 | RR | Pública | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | 2026-11-03T23:42:17.811908+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2024 | SE | Pública | 38.39 | 38.3 | 45.9 | 53.6 | 61.2 | 68.3 | 74.6 | 80.0 | 92.84 | 2026-11-03T23:42:17.811908+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2023 | SE | Pública | 31.3 | 38.3 | 45.9 | 53.6 | 61.2 | 68.3 | 74.6 | 80.0 | 88.34 | 2026-11-03T23:42:17.811908+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
| 2025 | SE | Pública | 50.0 | 38.0 | 46.0 | 54.0 | 61.0 | 68.0 | 75.0 | 80.0 | 87.0 | 2026-11-03T23:42:17.811908+00:00 | basedosdados_bigquery | 2026 | 3 | 24 |
[2026-11-03 23:45:45 UTC] Volume Total: 81 registros | 17 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ ano                                      : 0 nulos (0.00%)
   ├─ sigla_uf                                 : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ taxa_alfabetizacao                       : 4 nulos (4.94%)
   ├─ meta_alfabetizacao_2024                  : 9 nulos (11.11%)
   ├─ meta_alfabetizacao_2025                  : 3 nulos (3.70%)
   ├─ meta_alfabetizacao_2026                  : 2 nulos (2.47%)
   ├─ meta_alfabetizacao_2027                  : 2 nulos (2.47%)
   ├─ meta_alfabetizacao_2028                  : 2 nulos (2.47%)
   ├─ meta_alfabetizacao_2029                  : 2 nulos (2.47%)
   ├─ meta_alfabetizacao_2030                  : 2 nulos (2.47%)
   ├─ percentual_participacao                  : 4 nulos (4.94%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:46 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:46 UTC] ├─ Duplicatas por Chave Negocial ['sigla_uf', 'ano', 'rede']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'META_ALFABETIZACAO_MUNICIPIO'
===========================================================================
[2026-11-03 23:45:46 UTC] Schema / Estrutura de Tipos:
   ├─ ano                                      : bigint
[2026-11-03 23:45:46 UTC] ├─ id_municipio                             : string
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
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das
[2026-11-03 23:45:46 UTC] primeiras linhas:
[2026-11-03 23:45:46 UTC] | ano | id_municipio | rede | taxa_alfabetizacao | meta_alfabetizacao_2024 | meta_alfabetizacao_2025 | meta_alfabetizacao_2026 | meta_alfabetizacao_2027 | meta_alfabetizacao_2028 | meta_alfabetizacao_2029 | meta_alfabetizacao_2030 | nivel_alfabetizacao | percentual_participacao | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
[2026-11-03 23:45:46 UTC] | 2023 | 4301750 | Municipal | NULL | NULL | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | NULL | NULL | 2026-11-03T23:43:05.751343+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:46 UTC] | 2024 | 4301750 | Municipal | 4.4 | NULL | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | 0 | 92.59 | 2026-11-03T23:43:05.751343+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:46 UTC] | 2024 | 2406908 | Municipal | 42.86 | 7.94 | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | 1 | 84.0 | 2026-11-03T23:43:05.751343+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:46 UTC] | 2023 | 2406908 | Municipal | 4.4 | 7.94 | 14.05 | 23.65 | 37.0 | 52.68 | 67.85 | 80.0 | 0 | 82.14 | 2026-11-03T23:43:05.751343+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:46 UTC] | 2023 | 1718501 | Municipal | 4.6 | 8.25 | 14.48 | 24.16 | 37.49 | 53.03 | 68.0 | 80.0 | 0 | 95.65 | 2026-11-03T23:43:05.751343+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:47 UTC] Volume Total: 10,704 registros | 18 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ ano                                      : 0 nulos (0.00%)
   ├─ id_municipio                             : 0 nulos (0.00%)
   ├─ rede                                     : 0 nulos (0.00%)
   ├─ taxa_alfabetizacao                       : 120 nulos (1.12%)
   ├─ meta_alfabetizacao_2024                  : 240 nulos (2.24%)
   ├─ meta_alfabetizacao_2025                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2026                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2027                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2028                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2029                  : 0 nulos (0.00%)
   ├─ meta_alfabetizacao_2030                  : 0 nulos (0.00%)
   ├─ nivel_alfabetizacao                      : 120 nulos (1.12%)
   ├─ percentual_participacao                  : 120 nulos (1.12%)
   ├─ _ingested_at
[2026-11-03 23:45:47 UTC] : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:48 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:48 UTC] ├─ Duplicatas por Chave Negocial ['id_municipio', 'ano', 'rede']: 0 (0.00%)
===========================================================================

===========================================================================
 Diagnóstico da tabela: 'DICIONARIO'
===========================================================================
[2026-11-03 23:45:48 UTC] Schema / Estrutura de Tipos:
   ├─ id_tabela                                : string
   ├─ nome_coluna                              : string
   ├─ chave                                    : string
   ├─ cobertura_temporal                       : string
   ├─ valor                                    : string
   ├─ _ingested_at                             : string
   ├─ _source                                  : string
   ├─ ref_ano                                  : int
   ├─ ref_mes                                  : int
   ├─ ref_dia                                  : int

 Preview das primeiras linhas:
[2026-11-03 23:45:48 UTC] | id_tabela | nome_coluna | chave | cobertura_temporal | valor | _ingested_at | _source | ref_ano | ref_mes | ref_dia |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
[2026-11-03 23:45:48 UTC] | alunos | alfabetizado | 0 | (1) | Não | 2026-11-03T23:43:05.773489+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| alunos | alfabetizado | 1 | (1) | Sim | 2026-11-03T23:43:05.773489+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| alunos | preenchimento_caderno | 0 | (1) | Prova não preenchida | 2026-11-03T23:43:05.773489+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| alunos | preenchimento_caderno | 1 | (1) | Prova preenchida | 2026-11-03T23:43:05.773489+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
| alunos | presenca | 0 | (1) | Ausente | 2026-11-03T23:43:05.773489+00:00 | basedosdados_bigquery | 2026 | 1 | 14 |
[2026-11-03 23:45:49 UTC] Volume Total: 27 registros | 10 colunas

 Diagnóstico de NULOs por Coluna:
   ├─ id_tabela                                : 0 nulos (0.00%)
[2026-11-03 23:45:49 UTC] ├─ nome_coluna                              : 0 nulos (0.00%)
   ├─ chave                                    : 0 nulos (0.00%)
   ├─ cobertura_temporal                       : 0 nulos (0.00%)
   ├─ valor                                    : 0 nulos (0.00%)
   ├─ _ingested_at                             : 0 nulos (0.00%)
   ├─ _source                                  : 0 nulos (0.00%)
   ├─ ref_ano                                  : 0 nulos (0.00%)
   ├─ ref_mes                                  : 0 nulos (0.00%)
   ├─ ref_dia                                  : 0 nulos (0.00%)

 Diagnóstico de Duplicidades:
[2026-11-03 23:45:49 UTC] ├─ Duplicatas Exatas (Linha Inteira) : 0 (0.00%)
[2026-11-03 23:45:50 UTC] ├─ Duplicatas por Chave Negocial ['id_tabela', 'nome_coluna', 'chave']: 0 (0.00%)
===========================================================================

###########################################################################
 3. Deduplicação das tabelas com chaves repetidas.
###########################################################################


###########################################################################
 4. Executando Limpeza, Padronização e Eliminação de Colisão de Métricas...
###########################################################################
[2026-11-03 23:45:50 UTC] └─  [Derivação UF] Coluna 'sigla_uf' criada em 'alunos' via código IBGE.
[2026-11-03 23:45:50 UTC] ###########################################################################
 5. Executando JOIN relacional.
###########################################################################
[2026-11-03 23:45:51 UTC] ###########################################################################
 6. Verificando a taxa de correspondência do JOIN...
###########################################################################
[2026-11-03 23:45:54 UTC] ======================================================================
 [VERIFICAÇÃO DA TAXA DE CORRESPONDÊNCIA DO JOIN]
   ├─ Registros na Origem (Fato/Esquerda): 3,867,999
   ├─ Registros correspondentes         : 3,863,800
   ├─ Taxa de Correspondência Medida     : 99.89%
   └─ Limiar configurado                 : 95.00%
======================================================================
Taxa de correspondência do JOIN: 99.89% (limiar: 95.00%).
[2026-11-03 23:45:58 UTC] ###########################################################################
 7. Persistindo a Camada Silver em Delta Lake.
###########################################################################

 Alvo do Dataset Delta Lake: s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/
[2026-11-03 23:45:58 UTC] Criando nova Tabela Delta Silver inicial...
[2026-11-03 23:46:39 UTC] Tabela Delta Silver inicial gravada.
[2026-11-03 23:46:47 UTC] Tabela Silver registrada no Glue Data Catalog: tc-alfabetizacao_db.silver_alunos_curado
[2026-11-03 23:46:47 UTC] Manifesto Silver salvo: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-11-03/run_id=jr_2fdf42280493ad2b8a5b6d690010b93c0606c604894b4a3f5ec6c7d872c4ee17/manifest.json

===========================================================================
Camada Silver concluída.
===========================================================================
```

---

## 5. Resumo da Camada Silver
| Métrica de Curadoria / Qualidade | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas de Alunos (Entrada)** | **3,867,999** | Registros brutos lidos do manifesto Bronze |
| **Linhas Curadas (Saída)** | **3,867,999** | Registros finais enriquecidos e validados |
| **Taxa de correspondência do JOIN** | **99.89%** | Percentual de registros correspondentes |
| **Resultado da verificação do JOIN** | **Dentro do limite** | Limite configurado em `check_join_ratio`; não reexecutada quando a camada foi ignorada |
| **Tratamento de NULOs** | **Preservado** | Valores existentes são mantidos quando o job é ignorado |
| **Status das Metas** | **Tripla categorização** | 'Atingiu', 'Não Atingiu' ou 'Sem Meta Cadastrada' |
| **Destino S3** | `s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/` | Formato Delta Lake, sem partição por data configurada pelo job |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
