#  Relatório de Execução do Job: `04_events_processor`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-04 01:42:19 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `04_events_processor`
* **JobRunId:** `jr_c99828e2dd0be7264ec562420f758a2414ef770e7bc11e6ee5e367512c0a7b60`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/events/ingestion_date=2026-11-04/run_id=jr_c99828e2dd0be7264ec562420f758a2414ef770e7bc11e6ee5e367512c0a7b60/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **195 segundos** (~3.25 min) |
| **DPU-Horas Medidas** | **0.1083 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0477** |
| **Início / Término** | 2026-11-04 01:38:58 UTC ➔ 2026-11-04 01:42:19 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `S3 Landing Zone (arquivos NDJSON de eventos)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-04 01:39:48 UTC] ===========================================================================
 INICIANDO PROCESSADOR DE EVENTOS STREAMING (EVENTS PROCESSOR)
   Bucket / Target: tc-alfabetizacao-869769158828-us-east-1
   Run ID          : jr_c99828e2dd0be7264ec562420f758a2414ef770e7bc11e6ee5e367512c0a7b60 | Data: 2026-11-04
   Modo de Operação: AWS GLUE
   Engine          : Spark Structured Streaming (checkpointLocation)
===========================================================================
[2026-11-04 01:39:57 UTC] 4. Lendo micro-lotes via Spark Structured Streaming (checkpointLocation)...
[2026-11-04 01:39:57 UTC] ├─ [Domínio: Alunos] Streaming com schema explícito e checkpointLocation em: s3://tc-alfabetizacao-869769158828-us-east-1/checkpoints/events/alunos/
[2026-11-04 01:40:10 UTC] └─  Verificando a integridade do contrato json
[2026-11-04 01:40:33 UTC] └─  Estrutura contém os campos obrigatórios.
[2026-11-04 01:40:33 UTC] └─  Micro-lote 2 (Alunos): 31 registros inéditos encontrados.
[2026-11-04 01:40:36 UTC] └─  Latência Real Medida (ingested_at - event_time):
     ├─ Média : 161.013 segundos
     ├─ Mínima: 161.012 segundos
     └─ Máxima: 161.013 segundos
[2026-11-04 01:40:36 UTC] Executando MERGE INTO no Delta Lake Silver: s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/
[2026-11-04 01:41:33 UTC] MERGE INTO de alunos executado no Delta Lake.
[2026-11-04 01:41:33 UTC] └─  Processando eventos encontrados em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/events_raw_landing/meta_municipio/
[2026-11-04 01:41:36 UTC] └─  Eventos de metas (municipio, micro-lote 2): 7 registros encontrados.
[2026-11-04 01:41:36 UTC] └─  Persistindo eventos de metas na Camada Silver: s3://tc-alfabetizacao-869769158828-us-east-1/silver/meta_alfabetizacao_municipio/
[2026-11-04 01:41:46 UTC] MERGE INTO de metas executado no Delta Lake Silver.
[2026-11-04 01:41:46 UTC] └─  Processando eventos encontrados em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/events_raw_landing/meta_uf/
[2026-11-04 01:41:50 UTC] └─  Eventos de metas (uf, micro-lote 2): 2 registros encontrados.
[2026-11-04 01:41:50 UTC] └─  Persistindo eventos de metas na Camada Silver: s3://tc-alfabetizacao-869769158828-us-east-1/silver/meta_alfabetizacao_uf/
[2026-11-04 01:41:57 UTC] MERGE INTO de metas executado no Delta Lake Silver.
[2026-11-04 01:41:57 UTC] └─  Processando eventos encontrados em: s3://tc-alfabetizacao-869769158828-us-east-1/bronze/events_raw_landing/meta_brasil/
[2026-11-04 01:42:00 UTC] └─  Eventos de metas (brasil, micro-lote 2): 1 registros encontrados.
[2026-11-04 01:42:00 UTC] └─  Persistindo eventos de metas na Camada Silver: s3://tc-alfabetizacao-869769158828-us-east-1/silver/meta_alfabetizacao_brasil/
[2026-11-04 01:42:07 UTC] MERGE INTO de metas executado no Delta Lake Silver.
[2026-11-04 01:42:08 UTC] ===========================================================================
 PROCESSAMENTO DE EVENTOS DE ALUNOS: 31 eventos inéditos.
 PROCESSAMENTO DE EVENTOS DE METAS: 10 eventos inéditos.
Processamento de eventos concluído. Eventos processados: 41.
===========================================================================
[2026-11-04 01:42:08 UTC] [Auditoria Eventos] Manifesto salvo no S3: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/events/ingestion_date=2026-11-04/run_id=jr_c99828e2dd0be7264ec562420f758a2414ef770e7bc11e6ee5e367512c0a7b60/manifest.json
[2026-11-04 01:42:08 UTC] [Governança Silver] Manifesto de atualização ativado para a Camada Gold: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-11-04/run_id=jr_c99828e2dd0be7264ec562420f758a2414ef770e7bc11e6ee5e367512c0a7b60/manifest.json
```

---

## 5. Resumo do Processamento de Eventos
| Métrica de Eventos | Valor |
| :--- | :---: |
| **Status** | `SUCCEEDED` |
| **Total processado** | **41** |
| **Eventos de alunos** | **31** |
| **Eventos de metas** | **10** |
| **Metas por município / UF / Brasil** | **7 / 2 / 1** |
| **Latência média / mínima / máxima** | **161.013 / 161.012 / 161.013 s** |
| **Engine** | Spark Structured Streaming (Trigger AvailableNow + checkpointLocation) |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
