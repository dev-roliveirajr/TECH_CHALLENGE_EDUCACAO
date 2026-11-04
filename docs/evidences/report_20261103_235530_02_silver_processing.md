#  Relatório de Execução do Job: `02_silver_processing`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-03 23:55:18 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `02_silver_processing`
* **JobRunId:** `jr_130456e4a532cb29f9cc3b64f16e72f6f0bcf76ea2c180f696673227bfb8dc00`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-11-03/run_id=jr_130456e4a532cb29f9cc3b64f16e72f6f0bcf76ea2c180f696673227bfb8dc00/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **75 segundos** (~1.25 min) |
| **DPU-Horas Medidas** | **0.0417 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0183** |
| **Início / Término** | 2026-11-03 23:53:59 UTC ➔ 2026-11-03 23:55:18 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `Camada Bronze (Parquet / S3 via Manifesto)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-03 23:54:32 UTC] ===========================================================================
 Iniciando processamento da Camada Silver.
   Bucket: tc-alfabetizacao-869769158828-us-east-1
   Run ID: jr_130456e4a532cb29f9cc3b64f16e72f6f0bcf76ea2c180f696673227bfb8dc00 | Data: 2026-11-03
   Limiar do JOIN: 95.0% | Interrupção por falha: True
===========================================================================
 1. Carregando o manifesto Bronze mais recente.
[2026-11-03 23:54:32 UTC] Manifesto Bronze validado: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/ingestion_date=2026-11-03/run_id=jr_3ade157efe27d8d85284d5006d36c001b3385c784704850ce16dde1d3fb40b22/manifest.json
[2026-11-03 23:55:07 UTC] ===========================================================================
 A Camada Bronze não apresenta versões novas.
 O conjunto Delta existente será mantido.
===========================================================================
[2026-11-03 23:55:07 UTC] Manifesto Silver salvo: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/silver/ingestion_date=2026-11-03/run_id=jr_130456e4a532cb29f9cc3b64f16e72f6f0bcf76ea2c180f696673227bfb8dc00/manifest.json

===========================================================================
Camada Silver concluída.
===========================================================================
```

---

## 5. Resumo da Camada Silver
| Métrica de Curadoria / Qualidade | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas de Alunos (Entrada)** | **N/A (sem processamento nesta execução)** | Registros brutos lidos do manifesto Bronze |
| **Linhas Curadas (Saída)** | **N/A (sem processamento nesta execução)** | Registros finais enriquecidos e validados |
| **Taxa de correspondência do JOIN** | **N/A (sem processamento nesta execução)** | Percentual de registros correspondentes |
| **Resultado da verificação do JOIN** | **Não reexecutado (Silver existente preservada)** | Limite configurado em `check_join_ratio`; não reexecutada quando a camada foi ignorada |
| **Tratamento de NULOs** | **Não reexecutado (conjunto existente mantido)** | Valores existentes são mantidos quando o job é ignorado |
| **Status das Metas** | **Não reexecutado (conjunto existente mantido)** | 'Atingiu', 'Não Atingiu' ou 'Sem Meta Cadastrada' |
| **Destino S3** | `s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/` | Formato Delta Lake, sem partição por data configurada pelo job |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
