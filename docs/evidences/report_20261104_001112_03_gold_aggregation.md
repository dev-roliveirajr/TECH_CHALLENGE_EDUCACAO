#  Relatório de Execução do Job: `03_gold_aggregation`

## 1.  Informações Gerais e Contexto
* **Data/Hora UTC:** 2026-11-04 00:11:04 UTC
* **Ambiente AWS:** Account ID `869769158828` | Região `us-east-1`
* **AWS Glue Job:** `03_gold_aggregation`
* **JobRunId:** `jr_96dd1e4f5495701bae48088c0843ff70e4e45a35c43f91e2957cd905abd4152d`
* **Status Final:** `SUCCEEDED`
* **Bucket S3 Alvo:** `s3://tc-alfabetizacao-869769158828-us-east-1`
* **Manifesto de Auditoria S3:** `s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/gold/ingestion_date=2026-11-03/run_id=jr_96dd1e4f5495701bae48088c0843ff70e4e45a35c43f91e2957cd905abd4152d/manifest.json`

---

## 2. Métricas de execução e custo (AWS Glue)
| Métrica | Valor Medido |
| :--- | :--- |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **249 segundos** (~4.15 min) |
| **DPU-Horas Medidas** | **0.1383 DPU-h** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0.0609** |
| **Início / Término** | 2026-11-04 00:05:50 UTC ➔ 2026-11-04 00:11:04 UTC |

---

## 3.  Consumo e Origem dos Dados
* **Camada de Origem:** `Camada Silver (Parquet / S3 em alunos_curado/)`
* **Custos de serviços de origem:** Não medidos por este relatório.
* **Rastreabilidade:** Manifestos JSON registram os dados de execução.

---

## 4.  Saída da Aplicação (CloudWatch Stdout)
```text
[2026-11-04 00:07:51 UTC] ===========================================================================
 INICIANDO PROCESSAMENTO DA CAMADA GOLD - INDICADORES E FEATURE STORE
   Bucket: tc-alfabetizacao-869769158828-us-east-1
   Run ID: gld_20261104_000751 | Data: 2026-11-04
===========================================================================
[2026-11-04 00:07:52 UTC] 1. Lendo dataset curado da Camada Silver em: s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/
[2026-11-04 00:09:37 UTC] └─  Conexão realizada via leitor Delta Lake nativo (_delta_log/ ativado).
[2026-11-04 00:09:41 UTC] └─  Registros ativos lidos do snapshot Silver: 3,867,999
 2. Calculando indicadores ponderados municipais, estaduais e nacionais...
[2026-11-04 00:09:54 UTC] └─  Indicadores calculados: indicadores_municipais=12,417, indicadores_uf=95, indicadores_brasil=5
 3. Executando validação cruzada municipal com a taxa publicada...
[2026-11-04 00:10:02 UTC] ======================================================================
 [VALIDAÇÃO CRUZADA - MICRODADOS vs. TAXA PUBLICADA]
   ├─ Registros Comparados           : 12,408
   ├─ Divergência Média Absoluta     : 0.036%
   └─ Tolerância Máxima Permitida    : 2.00%
======================================================================
Validação cruzada concluída. Divergência média: 0.036%.
 4. Construindo comparativos contra a meta oficial de 80%...
[2026-11-04 00:10:09 UTC] └─  Comparativos calculados: metas_municipais=10,272, metas_uf=49, metas_brasil=2
 5. Construindo o conjunto de características...
[2026-11-04 00:10:16 UTC] └─  Features calculadas: features_municipais=10,272, features_uf=49, features_brasil=2
 6. Salvando datasets Gold em Parquet...
[2026-11-04 00:10:21 UTC] └─  indicadores_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_municipais/
[2026-11-04 00:10:24 UTC] └─  indicadores_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_uf/
[2026-11-04 00:10:27 UTC] └─  indicadores_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_brasil/
[2026-11-04 00:10:30 UTC] └─  metas_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas/
[2026-11-04 00:10:33 UTC] └─  metas_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_uf/
[2026-11-04 00:10:36 UTC] └─  metas_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_brasil/
[2026-11-04 00:10:39 UTC] └─  features_municipais salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao/
[2026-11-04 00:10:42 UTC] └─  features_uf salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_uf/
[2026-11-04 00:10:44 UTC] └─  features_brasil salvo em: s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_brasil/

 Registrando tabelas Gold no Athena / Glue Data Catalog...
[2026-11-04 00:10:51 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_municipais` registrada no catálogo.
[2026-11-04 00:10:51 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_uf` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_indicadores_brasil` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas_uf` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_desempenho_vs_metas_brasil` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao_uf` registrada no catálogo.
[2026-11-04 00:10:52 UTC] Tabela `tc-alfabetizacao_db`.`gold_features_alfabetizacao_brasil` registrada no catálogo.
 7. Gravando Manifesto de Auditoria Gold em: s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/gold/ingestion_date=2026-11-04/run_id=gld_20261104_000751/manifest.json
[2026-11-04 00:10:53 UTC] Manifesto Gold gravado.
===========================================================================
Camada Gold concluída.
===========================================================================
```

---

## 5. Resumo da Camada Gold
| Métrica Analítica / Governança | Valor Registrado | Observação de Engenharia |
| :--- | :---: | :--- |
| **Linhas Lidas da Silver** | **N/A (sem processamento nesta execução)** | Registros curados da tabela `alunos_curado` |
| **Indicadores Ponderados (Município/UF/Brasil)** | **N/A (sem processamento nesta execução)** | Agregação ponderada pelo peso do aluno |
| **Comparativos de Metas (Município/UF/Brasil)** | **N/A (sem processamento nesta execução)** | Comparação com a meta oficial de 80% |
| **Feature Stores (Município/UF/Brasil)** | **N/A (sem processamento nesta execução)** | Registros gravados nos conjuntos de características |
| **Validação Cruzada (Microdado vs. Inep)** | **Não reexecutada (conjuntos existentes mantidos)** | Batimento da taxa recalculada vs. oficial |
| **Meta Oficial de Referência** | **80.0%** | Meta de 2030; redes compatíveis conforme dicionário oficial |
| **Destinos S3 Gold** | indicadores_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_municipais/`<br>indicadores_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_uf/`<br>indicadores_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_indicadores_brasil/`<br>metas_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas/`<br>metas_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_uf/`<br>metas_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_desempenho_vs_metas_brasil/`<br>features_municipais: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao/`<br>features_uf: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_uf/`<br>features_brasil: `s3://tc-alfabetizacao-869769158828-us-east-1/gold/gold_features_alfabetizacao_brasil/` | Localização dos conjuntos de saída |


---
*Relatório gerado automaticamente pelo orquestrador local (`src/monitor/report_generator.py`).*
