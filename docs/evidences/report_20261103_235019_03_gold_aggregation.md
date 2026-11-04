#  Relatório de Execução do Job: `03_gold_aggregation`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-03 23:50:05 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `03_gold_aggregation`
* **JobRunId:** `jr_febfed15963505db0f48296c9abca2711bd4babf84b57708c3951fd365164104`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/gold/ingestion_date=2026-11-03/run_id=jr_febfed15963505db0f48296c9abca2711bd4babf84b57708c3951fd365164104/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **174 segundos** (~2.90 min) |
| **DPU-Horas Medidas** | **0.0967 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0425** |
| **Início / Término** | 2026-11-03 23:47:07 UTC ➔ 2026-11-03 23:50:05 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `Camada Silver (Parquet / S3 em alunos_curado/)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-03 23:47:40 UTC] ===========================================================================
 INICIANDO PROCESSAMENTO DA CAMADA GOLD - INDICADORES E FEATURE STORE
   Bucket: tc-alfabetizacao-869769158828-us-east-1
   Run ID: gld_20261103_234740 | Data: 2026-11-03
===========================================================================
[2026-11-03 23:47:40 UTC] 1. Lendo dataset curado da Camada Silver em: s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/
[2026-11-03 23:48:41 UTC] └─  Conexão realizada via leitor Delta Lake nativo (_delta_log/ ativado).
[2026-11-03 23:48:45 UTC] └─  Registros ativos lidos do snapshot Silver: 3,867,999
 2. Calculando indicadores ponderados municipais, estaduais e nacionais...
[2026-11-03 23:48:58 UTC] └─  Indicadores calculados: indicadores_municipais=12,417, indicadores_uf=95, indicadores_brasil=5
 3. Executando validação cruzada municipal com a taxa publicada...
[2026-11-03 23:49:06 UTC] ======================================================================
 [VALIDAÇÃO CRUZADA - MICRODADOS vs. TAXA PUBLICADA]
   ├─ Registros Comparados           : 12,408
   ├─ Divergência Média Absoluta     : 0.036%
   └─ Tolerância Máxima Permitida    : 2.00%
[2026-11-03 23:49:06 UTC] ======================================================================
[2026-11-03 23:49:06 UTC] Validação cruzada concluída. Divergência média: 0.036%.
 4. Construindo comparativos contra a meta oficial de 80%...
[2026-11-03 23:49:11 UTC] └─  Comparativos calculados: metas_municipais=10,272, metas_uf=0, metas_brasil=0
 5. Construindo o conjunto de características...
[2026-11-03 23:49:16 UTC] └─  Features calculadas: features_municipais=10,272, features_uf=0, features_brasil=0
 6. Salvando datasets Gold em Parquet...
[2026-11-03 23:49:20 UTC] └─  indicadores_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_municipais/
[2026-11-03 23:49:23 UTC] └─  indicadores_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_uf/
[2026-11-03 23:49:26 UTC] └─  indicadores_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_brasil/
[2026-11-03 23:49:30 UTC] └─  metas_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas/
[2026-11-03 23:49:31 UTC] └─  metas_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_uf/
[2026-11-03 23:49:32 UTC] └─  metas_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_brasil/
[2026-11-03 23:49:36 UTC] └─  features_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao/
[2026-11-03 23:49:37 UTC] └─  features_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_uf/
[2026-11-03 23:49:39 UTC] └─  features_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_brasil/

 Registrando tabelas Gold no Athena / Glue Data Catalog...
[2026-11-03 23:49:44 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_municipais` registrada no catálogo.
[2026-11-03 23:49:45 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_uf` registrada no catálogo.
[2026-11-03 23:49:47 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_brasil` registrada no catálogo.
[2026-11-03 23:49:48 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas` registrada no catálogo.
[2026-11-03 23:49:49 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas_uf` registrada no catálogo.
[2026-11-03 23:49:50 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas_brasil` registrada no catálogo.
[2026-11-03 23:49:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao` registrada no catálogo.
[2026-11-03 23:49:53 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao_uf` registrada no catálogo.
[2026-11-03 23:49:54 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao_brasil` registrada no catálogo.
[2026-11-03 23:49:54 UTC] 7. Gravando Manifesto de Auditoria Gold em: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/gold/ingestion_date=2026-11-03/run_id=gld_20261103_234740/manifest.json
[2026-11-03 23:49:54 UTC] Manifesto Gold gravado.
===========================================================================
Camada Gold concluída.
===========================================================================
```

---

## 5. Resumo da Camada Gold
| Métrica Analítica / Governança | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas Lidas da Silver** | **3,867,999** | Registros curados da tabela `alunos_curado` |
| **Indicadores Ponderados (Município/UF/Brasil)** | **12,517** | municipio: 12,417<br>uf: 95<br>brasil: 5 |
| **Comparativos de Metas (Município/UF/Brasil)** | **10,272** | municipio: 10,272<br>uf: 0<br>brasil: 0 |
| **Feature Stores (Município/UF/Brasil)** | **10,272** | municipio: 10,272<br>uf: 0<br>brasil: 0 |
| **Validação Cruzada (Microdado vs. Inep)** | **Dentro da tolerância (0.036%)** | Batimento da taxa recalculada vs. oficial |
| **Meta Oficial de Referência** | **80.0%** | Meta de 2030; redes compatíveis conforme dicionário oficial |
| **Destinos S3 Gold** | indicadores_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_municipais/`<br>indicadores_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_uf/`<br>indicadores_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_brasil/`<br>metas_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas/`<br>metas_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_uf/`<br>metas_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_brasil/`<br>features_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao/`<br>features_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_uf/`<br>features_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_brasil/` | Localização dos conjuntos de saída |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
