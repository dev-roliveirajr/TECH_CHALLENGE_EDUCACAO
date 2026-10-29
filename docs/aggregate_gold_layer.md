# Especificação Técnica e Curadoria de Dados — Camada Gold (Analytics & Feature Store Layer)

Este documento detalha a arquitetura, as regras de negócio de estatística educacional, os algoritmos de ponderação por amostragem do Saeb, a modelagem da Feature Store para Machine Learning e os mecanismos de validação cruzada e governança implementados na **Camada Gold** da solução do **Tech Challenge (Fase 2)**. A pipeline é responsável por consumir o dataset curado da Camada Silver armazenado no **Amazon S3**, construir tabelas analíticas agregadas de alto desempenho, medir a evolução frente às metas pactuadas e exportar os dados prontos para consumo na Fase 3.

---

## 1. Visão Geral e Propósito da Camada Gold

Na **Arquitetura Medalhão**, a **Camada Gold (Analytics Zone / Business Layer)** representa o nível máximo de refinamento, onde os dados curados da Camada Silver são transformados em agregados de negócios, métricas oficiais e estruturas otimizadas para consumo de BI, dashboards no Amazon Athena e inteligência preditiva.

No contexto do **Compromisso Nacional Criança Alfabetizada** e das avaliações do **Saeb/Inep**, a Camada Gold entrega três produtos analíticos fundamentais:
* **Tabela de Indicadores Municipais (`gold_indicadores_municipais`)**: Taxas reais de alfabetização e proficiência recalculadas via microdados de alunos através de **média ponderada pelo peso amostral**.
* **Tabela de Comparativo de Metas (`gold_desempenho_vs_metas`)**: Avaliação do cumprimento do indicador frente à **Meta Pactuada Oficial da Fonte** (trajetória de 2024 a 2030), aplicando a tripla categorização de status.
* **Feature Store para Machine Learning (`gold_features_alfabetizacao`)**: Matriz de características estruturada em nível de ente federativo para o treinamento de modelos preditivos na Fase 3, imune a vazamento de dados (*target leakage*) e com **persistência consolidada no repositório local (`data/features/`)**.

---

## 2. Arquitetura de Processamento e Linhagem de Dados

A arquitetura da Camada Gold foi projetada sob o princípio de **leitura indireta governada por manifesto**, **cálculo estatístico distribuído em PySpark** e **automação de auditoria e sincronização local**.

```
+-----------------------------------------------------------------------------------+
|                            ORIGEM: AMAZON S3 (SILVER)                             |
|       s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/          |
|                          Manifesto: manifest.json                                 |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura orientada por Manifesto JSON
                                          v
+-----------------------------------------------------------------------------------+
|                        PROCESSAMENTO: AWS GLUE 4.0 (PYSPARK)                      |
|  * Leitura e Validação do Dataset Curado da Camada Silver                         |
|  * Média Ponderada Saeb: SUM(peso_aluno * alfabetizado) / SUM(peso_aluno)         |
|  * Comparativo de Metas: GAP contra a Meta Pactuada Oficial (80% em 2030)         |
|  * Feature Store ML: Seleção de atributos e criação da flag alvo_atingiu_meta     |
|  * Validação Cruzada: Batimento entre microdados recalculados e taxa Inep         |
+-----------------------------------------------------------------------------------+
                                          |
                                          +-----------------------------------------+
                                          |                                         |
                                          | Gravação Parquet (Snappy)               | Sincronização Local
                                          v                                         v
+---------------------------------------------------+     +-----------------------------------------------+
|             DESTINO 1: AMAZON S3 (GOLD)           |     |             DESTINO 2: PROJETO LOCAL          |
|  s3://.../gold/gold_indicadores_municipais/       |     |  data/features/                               |
|  s3://.../gold/gold_desempenho_vs_metas/          |     |  ├── gold_features_alfabetizacao.parquet      |
|  s3://.../gold/gold_features_alfabetizacao/       |     |  ├── gold_indicadores_municipais.parquet      |
|  Manifesto: auditoria/gold/.../manifest.json      |     |  └── gold_desempenho_vs_metas.parquet         |
+---------------------------------------------------+     +-----------------------------------------------+
```

### Decisões de Arquitetura e Estratégia Tecnológica

| Dimensão de Engenharia | Estratégia Adotada | Justificativa Técnica |
| :--- | :--- | :--- |
| **Consumo de Origem** | Leitura via `manifest.json` da Silver | Garante rastreabilidade total e evita reprocessamento de partições temporárias. |
| **Ponderação Estatística** | Média Amostral pelo `peso_aluno` | Atende estritamente à metodologia do Saeb, evitando a distorção de médias simples. |
| **Alinhamento de Metas** | Meta Pactuada Oficial da Fonte | Adota a trajetória oficial de pactuação (80% para 2030) para avaliação justa de gestão. |
| **Exportação para ML** | Sincronização Local Consolidada (`data/features/`) | Salva arquivos `.parquet` únicos e limpos para execução imediata dos modelos da Fase 3 em Pandas. |
| **Auditoria e Integridade** | Validação Cruzada Automatizada | Compara a taxa recalculada via alunos contra o valor publicado pelo Inep na dimensão. |

---

## 3. Regra de Negócio 1: Média Ponderada Amostral do Saeb

Uma das principais armadilhas em análises de microdados educacionais públicos é a utilização de média simples para calcular a taxa de alfabetização de um município ou estado.

### 1. A Armadilha da Média Simples
A amostragem do Saeb atribui um **fator de expansão (`peso_aluno`)** a cada estudante avaliado para representar proporcionalmente a população de sua escola, rede de ensino e município. Se calcularmos a média simples de acertos, um aluno de uma turma amostral reduzida teria o mesmo peso relativo que um aluno de uma rede municipal densa, distorcendo o indicador oficial.

### 2. A Fórmula Oficial de Ponderação Implementada
A Camada Gold aplica a fórmula de ponderação sobre os estudantes presentes e efetivamente avaliados:

$$\text{Taxa de Alfabetização Ponderada (\%)} = \left( \frac{\sum_{i=1}^{N} (\text{peso\_aluno}_i \times \text{alfabetizado}_i)}{\sum_{i=1}^{N} \text{peso\_aluno}_i} \right) \times 100$$

Em código PySpark, a agregação é executada via:

```python
df_indicadores_mun = df_avaliados.groupBy("id_municipio", "sigla_uf", "ano", "rede", "serie") \
    .agg(
        F.count("id_aluno").alias("total_alunos_avaliados"),
        F.sum("peso_aluno").alias("soma_peso_amostral"),
        F.sum(F.col("peso_aluno") * F.col("_alfabetizado_num")).alias("soma_peso_alfabetizados"),
        F.avg("proficiencia").alias("media_proficiencia_recalculada")
    ) \
    .withColumn(
        "taxa_alfabetizacao_calculada",
        F.round((F.col("soma_peso_alfabetizados") / F.col("soma_peso_amostral")) * 100.0, 2)
    )
```

---

## 4. Regra de Negócio 2: Meta Pactuada Oficial e Tripla Categorização

Para avaliar o cumprimento das metas do **Compromisso Nacional Criança Alfabetizada**, o pipeline adota a **Meta Pactuada Oficial da Fonte** registrada nas tabelas de metas do Inep/Base dos Dados.

### 1. Justificativa da Escolha da Meta Pactuada (80,0% até 2030)
Enquanto a *Meta Aspiracional (Universalização)* representa o conceito idealista de alcançar 100% de crianças alfabetizadas, a **Meta Pactuada Oficial** reflete os compromissos reais e progressivos firmados com o Ministério da Educação (MEC) para cada ano do ciclo:
* **Meta 2024**: 59,9%
* **Meta 2025**: 63,77%
* **Meta 2030 (Rede Pública)**: **80,0%**

A utilização da meta pactuada permite uma auditoria realista e defensável sobre a evolução da gestão pública.

### 2. Lógica da Tripla Categorização de Status
Para evitar classificar entes federativos sem cadastro de metas como inadimplentes, o pipeline atribui três estados:
* **`Atingiu a Meta`**: Quando a taxa de alfabetização calculada é maior ou igual à meta pactuada ($\text{Taxa} \ge \text{Meta}$).
* **`Não Atingiu a Meta`**: Quando a taxa calculada fica abaixo da meta ($\text{Taxa} < \text{Meta}$).
* **`Sem Meta Cadastrada`**: Quando o valor da meta na fonte original for `NULL`.

---

## 5. Regra de Negócio 3: Feature Store para Machine Learning (Fase 3)

A Camada Gold prepara e disponibiliza a matriz de entrada para os modelos de inteligência artificial e aprendizado de máquina da próxima etapa do curso.

### 1. Prevenção Rígida contra Vazamento de Alvo (*Target Leakage*)
Para evitar que o modelo de Machine Learning "decore" a resposta no treinamento, a Feature Store isola estritamente as variáveis preditivas da variável alvo:
* **Variáveis Explicativas ($X$)**: `total_alunos_avaliados`, `soma_peso_amostral`, `media_proficiencia_recalculada`, `rede`, `serie`, `sigla_uf`.
* **Variável Alvo Binária ($y$)**: `alvo_atingiu_meta` ($1$ para 'Atingiu a Meta' e $0$ para os demais casos).

### 2. Estratégia de Sincronização Local
O orquestrador `pipeline.py` realiza o download das partições do S3 e utiliza o Pandas para unificar e salvar arquivos `.parquet` consolidados únicos na pasta local `data/features/`:
1. `gold_features_alfabetizacao.parquet` (12.417 linhas x 13 colunas)
2. `gold_indicadores_municipais.parquet` (12.417 linhas)
3. `gold_desempenho_vs_metas.parquet` (12.417 linhas)

---

## 6. Governança e Validação Cruzada (Microdados vs. Taxa Publicada)

Como diferencial de qualidade e garantia de integridade, o pipeline executa a função **`check_cross_validation()`** da biblioteca `quality_lib.py`.

### O Batimento de Integridade
A rotina compara a taxa média ponderada recalculada a partir dos 3,86 milhões de microdados de estudantes contra a taxa sintética oficial publicada pelo Inep na dimensão municipal.

```text
🔬 [VALIDAÇÃO CRUZADA - MICRODADOS vs. TAXA PUBLICADA]
   ├─ Registros Comparados           : 12,408
   ├─ Divergência Média Absoluta     : 0.036%
   └─ Tolerância Máxima Permitida    : 2.00%
✅ [VALIDAÇÃO CRUZADA APROVADA] Alinhamento dentro da margem de tolerância (0.036% <= 2.00%).
```

Essa convergência de **99,96%** prova matematicamente que o tratamento de NULOs, a deduplicação por janela e as junções relacionais executadas nas Camadas Bronze e Silver preservaram a exatidão estatística da fonte oficial.

---

## 7. Catálogo de Dados S3, Manifesto Gold e FinOps

### 1. Datasets Finais Gravados no S3 (Camada Gold)

| Dataset Analítico / Tabela | S3 Prefix Final | Formato & Compressão | Finalidade de Negócio |
| :--- | :--- | :--- | :--- |
| **`gold_indicadores_municipais`** | `s3://.../gold/gold_indicadores_municipais/` | Parquet (Snappy) | Consultas BI / Athena de taxas ponderadas por município. |
| **`gold_desempenho_vs_metas`** | `s3://.../gold/gold_desempenho_vs_metas/` | Parquet (Snappy) | Painel executivo do GAP de metas do Compromisso Alfabetização. |
| **`gold_features_alfabetizacao`** | `s3://.../gold/gold_features_alfabetizacao/` | Parquet (Snappy) | Matriz de treino para os modelos preditivos da Fase 3 (ML). |

### 2. Manifesto de Auditoria da Camada Gold
O término do Job grava o manifesto JSON em `s3://.../auditoria/gold/ingestion_date=YYYY-MM-DD/run_id=.../manifest.json`, registrando os volumes consumidos, as linhas geradas e o indicador de divergência da validação cruzada.

### 3. Métricas de Desempenho e FinOps (AWS Glue)

| Métrica FinOps | Valor Medido na Gold |
| :--- | :---: |
| **Status da Execução** | `SUCCEEDED` |
| **Duração Total** | **160 segundos** (~2,67 min) |
| **Consumo de Computação** | **0,0889 DPU-horas** (2.0 DPUs Worker Type G.1X) |
| **Custo Estimado AWS Glue** | **US$ 0,0391** |
| **Volume de Saída Gerado** | **12.417 registros** por dataset |

---

## 8. Resumo da Conformidade com os Requisitos do Tech Challenge

* **Média Ponderada pelo Peso Amostral**: Aplicação rigorosa da expansão amostral do Saeb.
* **Meta Pactuada Oficial**: Alinhamento com a trajetória pactuada (80% em 2030) e tripla categorização.
* **Feature Store para ML**: Preparação do dataset para a Fase 3 com consolidação de arquivo único Parquet local (`data/features/`).
* **Validação Cruzada**: Demonstração de convergência matemática de 99,96% entre microdados e agregados oficiais.
* **Governança por Manifesto e FinOps**: Rastreabilidade completa no Amazon S3 e relatórios de custo.
