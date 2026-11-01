### Especificação Técnica e Curadoria de Dados — Camada Silver (Curated Layer)
Este documento detalha a arquitetura, as regras de engenharia de dados, a governança por manifestos, o protocolo de Análise Exploratória (EDA / Raio-X), as políticas de qualidade do Saeb, o tratamento determinístico de duplicidades por chaves de negócio, a persistência ACID via **Delta Lake (MERGE INTO)**, o mecanismo de **Short-Circuit FinOps** e os portões de validação bloqueante implementados no pipeline da **Camada Silver** da solução do **Tech Challenge (Fase 2)**. A pipeline é responsável por consumir os dados brutos da Camada Bronze armazenados no **Amazon S3**, realizar limpezas, deduplicação por janelas, desambiguação, enriquecimento relacional e persistir o dataset curado no formato **Delta Lake / Apache Parquet**.

--------------------------------------------------------------------------------

#### 1. Visão Geral e Propósito da Camada Silver
No modelo de **Arquitetura Medalhão**, a **Camada Silver (Curated Data / Cleansed Zone)** é a zona de dados limpos, padronizados, tipados e integrados. Enquanto a Camada Bronze preserva os dados brutos em sua forma original com todas as imperfeições da fonte, a Camada Silver atua como a única fonte da verdade (*Single Source of Truth*) para análises avançadas e para o consumo da Camada Gold.
No contexto do **Compromisso Nacional Criança Alfabetizada** e dos microdados do **Saeb/Inep**, a Camada Silver executa a convergência entre:
* **Microdados de Alunos**: Registro individual de proficiência, presença e preenchimento de caderno.
* **Indicadores Territoriais**: Taxas de alfabetização e médias de português em nível municipal e estadual.
* **Metas de Alfabetização**: Trajetórias pactuadas anualmente (2024 a 2030) nos âmbitos federal, estadual e municipal.
* **Dicionários de Variáveis**: Mapeamento e tradução de códigos categóricos para rótulos legíveis.

--------------------------------------------------------------------------------

#### 2. Arquitetura de Processamento e Linhagem de Dados
A arquitetura da Camada Silver foi desenhada sob os princípios de **governança orientada a manifestos**, **idempotência**, **mecanismo de Short-Circuit FinOps**, **deduplicação por janelas** e **processamento transacional em Delta Lake**.

```text
+-----------------------------------------------------------------------------------+
|                            ORIGEM: AMAZON S3 (BRONZE)                             |
|       s3://tc-alfabetizacao-869769158828-us-east-1/auditoria/bronze/...           |
|                          Manifesto: manifest.json                                 |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Leitura orientada por Manifesto JSON
                                          v
+-----------------------------------------------------------------------------------+
|                        PROCESSAMENTO: AWS GLUE 4.0 (PYSPARK)                      |
|  * Otimização Short-Circuit FinOps: Se Bronze = SKIPPED & Delta existe -> SKIP    |
|  * Raio-X & EDA Inicial: Contagem, Schemas, Diagnóstico NULOs, Duplicatas e Preview|
|  * Deduplicação Determinística: Window Function (partitionBy + row_number)        |
|  * Curadoria & Limpeza: Padronização snake_case, trimming e mapas do Dicionário   |
|  * Regra Saeb: Preservação estrita de NULOs em notas e proficiências              |
|  * Derivação Territorial: Criação de 'sigla_uf' a partir do id_municipio (IBGE)   |
|  * O Grande JOIN: Junção por chaves compostas (id_municipio, ano, rede, serie)    |
|  * Gate de Qualidade: Validação bloqueante via check_join_ratio (>= 95.0%)        |
|  * Operação ACID: Delta Lake MERGE INTO (Upsert por id_aluno + ano)               |
+-----------------------------------------------------------------------------------+
                                          |
                                          | Escrita Delta Lake (ACID / Snappy)
                                          v
+-----------------------------------------------------------------------------------+
|                            DESTINO: AMAZON S3 (SILVER)                            |
|       s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/          |
|       Manifesto Silver: auditoria/silver/ingestion_date=.../manifest.json         |
+-----------------------------------------------------------------------------------+
```

##### Análise de Decisões de Arquitetura e Trade-Offs
| Dimensão de Engenharia | Estratégia Adotada | Justificativa Técnica |
| ------ | ------ | ------ |
| **Descoberta de Dados** | Leitura orientada a Manifesto (`manifest.json`) | Elimina varreduras cegas com wildcards (`s3://.../bronze/*`), garantindo que apenas partições validadas da última ingestão bem-sucedida sejam processadas. |
| **Short-Circuit FinOps** | Checagem preventiva de `all_bronze_skipped` & `DeltaTable.isDeltaTable()` | Quando a Bronze não possui novos dados e a Silver já foi construída, o Job pula o reprocessamento em segundos com `status: SKIPPED`, reduzindo o custo computacional no AWS Glue a zero. |
| **Engine de Processamento** | PySpark no AWS Glue 4.0 (Spark 3.3+) | Permite processar mais de 3,8 milhões de registros com alta eficiência de memória utilizando o *Adaptive Query Execution* (AQE). |
| **Deduplicação de Negócio** | Janela PySpark (`Window.partitionBy`) | Garante a escolha determinística da versão mais recente e completa da entidade com base em chaves negociais primárias. |
| **Tabelas ACID & Upsert** | Delta Lake (`MERGE INTO`) | Permite atualizar o histórico de estudantes (*Upsert* por `id_aluno` + `ano`) sem duplicidades de cardinalidade nem necessidade de recargas totais (*Full Overwrite*). |
| **Formato de Persistência** | Apache Parquet com Compressão Snappy em Delta | Garante alta taxa de compressão colunar, log de transações em `_delta_log/` e leitura acelerada para consultas analíticas no Amazon Athena. |
| **Qualidade Bloqueante** | Biblioteca de Qualidade (`quality_lib.py`) | Interrompe o Job e dispara exceção se a taxa de alinhamento do JOIN ficar abaixo de 95,0%. |

--------------------------------------------------------------------------------

#### 3. Protocolo de Raio-X e Análise Exploratória (EDA Inicial)
Antes de executar qualquer modificação, limpeza ou junção nos dados, o Job executa automaticamente uma rotina de **Raio-X (Análise Exploratória Inicial)** em cada uma das 7 entidades carregadas da Camada Bronze.

##### 1. Verificação Diagnóstica de Volume, NULOs e Duplicidades
Para todas as 7 tabelas do dataset, o protocolo avalia a volumetria, os schemas, o percentual de NULOs por coluna e realiza o diagnóstico de duplicidades (tanto por linhas totalmente exatas quanto por chaves negociais primárias):
| Tabela / Entidade | Volume Total de Linhas | Total de Colunas | Diagnóstico de Duplicidades Negociais | Diagnóstico de NULOs |
| ------ | ------ | ------ | ------ | ------ |
| **alunos** | 3.867.999 | 17 | Verificação via `['id_aluno', 'id_municipio', 'ano', 'rede', 'serie']` | proficiencia e peso_aluno: 13,27% NULOs (513.338 linhas) |
| **municipio** | 23.995 | 20 | Verificação via `['id_municipio', 'ano', 'rede', 'serie']` | proporcao_aluno_nivel_0 a 8: 48,12% NULOs (11.547 linhas) |
| **uf** | 145 | 20 | Verificação via `['sigla_uf', 'ano', 'rede', 'serie']` | proporcao_aluno_nivel_0 a 8: 48,28% NULOs (70 linhas) |
| **meta_alfabetizacao_brasil** | 3 | 16 | Verificação via `['ano', 'rede']` | Sem NULOs (0,00%) |
| **meta_alfabetizacao_uf** | 81 | 17 | Verificação via `['sigla_uf', 'ano', 'rede']` | meta_alfabetizacao_2024: 11,11% NULOs (9 linhas) |
| **meta_alfabetizacao_municipio** | 21.408 | 18 | Verificação via `['id_municipio', 'ano', 'rede']` | meta_alfabetizacao_2024: 2,24% NULOs (480 linhas) |
| **dicionario** | 27 | 10 | Verificação via `['id_tabela', 'nome_coluna', 'chave']` | Sem NULOs (0,00%) |

##### 2. Formatação de Previews em Tabelas Markdown
Para facilitar a auditoria via CloudWatch e relatórios de observabilidade, a rotina de Raio-X formata as 3 primeiras linhas de cada tabela em formato Markdown nativo, como exemplificado na tabela de alunos:
| ano | id_municipio | id_escola | id_aluno | caderno | serie | rede | presenca | preenchimento_caderno | alfabetizado | proficiencia | peso_aluno |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 2023 | 1302504 | 60000951 | 13015851 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL |
| 2023 | 1302603 | 60000963 | 13030738 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL |
| 2023 | 1300631 | 60001351 | 13003982 | 1 | 2 | 3 | 0 | 0 | 0 | NULL | NULL |

--------------------------------------------------------------------------------

#### 4. Regras de Negócio, Curadoria e Governança
##### 1. Deduplicação Ativa por Janela em Chaves Negociais Primárias
Em pipelines de Big Data, aplicar um simples `.dropDuplicates()` pode gerar descartes não-determinísticos caso existam atualizações parciais ou cópias enviadas pela fonte. Para garantir um tratamento determinístico e auditável, a Camada Silver implementa a função `deduplicate_by_window()` na biblioteca `quality_lib.py`.

###### Lógica e Critérios de Priorização Negocial
Para a tabela de microdados (alunos), a chave negocial primária é composta por `["id_aluno", "id_municipio", "ano", "rede", "serie"]`. Havendo mais de um registro para a mesma chave, a janela de partição ordena os candidatos e escolhe exatamente a linha #1 conforme a hierarquia:
1. **Recência da Ingestão (`_ingested_at.desc()`)**: Prioriza o registro ingerido mais recentemente.
2. **Completude do Dado (`proficiencia.desc_nulls_last()`)**: Prioriza o registro que possui a nota de proficiência preenchida sobre um registro com valor NULL.

```python
def deduplicate_by_window(
    df: DataFrame,
    key_columns: List[str],
    order_by_cols: Optional[List[str]] = None,
    ascending: bool = False
) -> DataFrame:
    """
    Deduplica um DataFrame com base em uma chave primária composta utilizando Window Function.
    Garante a escolha determinística do melhor registro conforme os critérios de prioridade.
    """
    window_spec = Window.partitionBy(key_columns)

    if order_by_cols:
        order_exprs = [
            F.col(c).asc_nulls_last() if ascending else F.col(c).desc_nulls_last()
            for c in order_by_cols if c in df.columns
        ]
        if order_exprs:
            window_spec = window_spec.orderBy(*order_exprs)

    return df.withColumn("_row_num", F.row_number().over(window_spec)) \
             .filter(F.col("_row_num") == 1) \
             .drop("_row_num")
```

##### 2. Transações ACID e Operação MERGE INTO (Upsert)
A Silver utiliza a engine **Delta Lake** para persistência no S3. Quando novos lotes ou modificações são recebidos da Bronze:
```python
if is_delta:
    delta_target = DeltaTable.forPath(spark, output_path)
    delta_target.alias("target").merge(
        source=df_joined.alias("source"),
        condition="target.id_aluno = source.id_aluno AND target.ano = source.ano",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_joined.write.format("delta").mode("overwrite").save(output_path)
```
* **Preservação de Cardinalidade**: Registros existentes são atualizados em-local (*in-place*), garantindo que atualizações incrementais não dupliquem o volume total de registros.

##### 3. Padronização de Nomes e Trimming de Strings
* Todas as colunas têm seus nomes convertidos para o padrão **snake_case**.
* Campos de texto (como `sigla_uf` e `rede`) passam por remoção automática de espaços em branco nas extremidades (*trimming*).

##### 4. Mapeamento Categórico via Tabela `dicionario`
Utilizando os metadados da tabela `dicionario`, os códigos numéricos são convertidos em rótulos descritivos de negócio:
* **alfabetizado**: O código 0 é mapeado para 'Não' e 1 para 'Sim'.
* **preenchimento_caderno**: Códigos numéricos são traduzidos para descrições como 'Prova não preenchida' ou 'Prova preenchida'.

##### 5. Política Estrita de Tratamento de NULOs (Regra do Saeb)
Conforme exigência do edital do Tech Challenge, **valores NULOS nas colunas proficiencia e peso_aluno NÃO são preenchidos com zero**.
* **Justificativa de Estatística Educacional**: A atribuição de zero para um estudante ausente ou que não realizou o exame reduz artificialmente a média de proficiência da escola e do município, gerando uma distorção grave nas análises de desempenho do Saeb.
* Os 513.338 registros NULOs (13,27%) correspondem a estudantes ausentes e são estritamente preservados como NULL.

##### 6. Tripla Categorização de Metas Educacionais
Para evitar classificar erroneamente entes federativos que não possuem pactuação cadastrada como inadimplentes, o pipeline adota três estados distintos:
* **Atingiu a Meta**: Quando a taxa de alfabetização é maior ou igual à meta do ano.
* **Não Atingiu a Meta**: Quando a taxa fica abaixo da meta.
* **Sem Meta Cadastrada**: Quando o valor da meta na fonte é NULL.

##### 7. Desambiguação de Métricas e Prevenção de Colisão de Schemas
Para evitar o erro de colunas ambíguas no PySpark quando múltiplas dimensões possuem métricas com o mesmo nome, o script renomeia as colunas adicionando sufixos específicos de contexto:
* Na dimensão municipal (`municipio`): `media_portugues_municipio`, `taxa_alfabetizacao_municipio`, `proporcao_aluno_nivel_X_municipio`.
* Na dimensão estadual (`uf`): `media_portugues_uf`, `taxa_alfabetizacao_uf`, `proporcao_aluno_nivel_X_uf`.
* Na dimensão de metas (`meta_alfabetizacao_municipio`): sufixos `_meta_mun`.
* As colunas de auditoria da Camada Bronze (`_ingested_at`, `_source`, `ref_ano`, `ref_mes`, `ref_dia`) são removidas das dimensões antes do JOIN para evitar duplicação no Parquet final.

--------------------------------------------------------------------------------

#### 5. O Grande JOIN Relacional e Derivação Territorial
O enriquecimento da tabela principal de microdados de estudantes (`alunos`) com as dimensões territoriais e de metas ocorre em uma estrutura relacional otimizada.

##### 1. Derivação da Sigla de UF em Memória ($O(1)$)
A tabela de alunos na Camada Bronze não contém a coluna `sigla_uf` diretamente, mas possui o código do município do IBGE (`id_municipio`). O script extrai os dois primeiros dígitos do código municipal e faz o mapeamento em memória via `create_map` com complexidade $O(1)$ (ex.: 13 ➔ 'AM'). Essa abordagem evita JOINs desnecessários de rede no Spark e aproveita a imutabilidade do padrão oficial de códigos estaduais do IBGE.

##### 2. Junção por Chaves Compostas
Para evitar a explosão de registros decorrente de combinações multivaloradas por ano, rede e série, o JOIN relacional é executado via **chaves compostas**:
```python
df_joined = (
    df_alunos.join(
        df_municipio, on=["id_municipio", "ano", "rede", "serie"], how="left"
    )
    .join(df_uf, on=["sigla_uf", "ano", "rede", "serie"], how="left")
    .join(df_meta_mun, on=["id_municipio", "ano", "rede"], how="left")
)
```
Essa modelagem garante que cada estudante seja associado exatamente aos indicadores da sua rede de ensino, série e ano letivo correspondentes.

--------------------------------------------------------------------------------

#### 6. Portão de Qualidade Bloqueante (Gate de Qualidade - Join Match Ratio)
Conforme estabelecido nos requisitos da Fase 2, o pipeline incorpora um mecanismo de **interrupção automática de emergência** caso a integridade referencial entre a tabela fato e as dimensões seja comprometida.

##### A Função check_join_ratio() (`src/common/quality_lib.py`)
A função mede o percentual de registros da tabela principal de alunos que foram combinados com sucesso na dimensão municipal e compara o resultado contra o limiar configurado (`JOIN_MATCH_THRESHOLD = 95.0%`).
```python
match_ratio, metrics = check_join_ratio(
    df_left=df_alunos,
    df_joined=df_joined,
    match_key_col="id_municipio",
    threshold_percent=95.0,
    raise_on_failure=True
)
```

##### Evidência de Execução do Gate de Qualidade
Na execução oficial de produção, o gate de qualidade registrou os seguintes indicadores:
* **Registros na Origem (`alunos`)**: 3.867.999 linhas
* **Registros Combinados no JOIN**: 3.867.999 linhas
* **Taxa de Correspondência Medida**: **100,00%**
* **Limiar Mínimo do Gate**: **95,00%**
* **Resultado**: **✅ GATE APROVADO**

--------------------------------------------------------------------------------

#### 7. Catálogo de Dados S3, Otimização FinOps e Auditoria
##### 1. Mecanismo de Short-Circuit FinOps
Quando a Camada Bronze executa e constata que os dados da fonte não sofreram alterações (`status == "SKIPPED"` em todas as tabelas) e a tabela Delta `alunos_curado/` já existe no S3:
1. O Job Silver detecta essa condição e ativa o **Short-Circuit FinOps**.
2. Ele grava o manifesto de auditoria no S3 com `status: "SKIPPED (Already Up-to-Date)"` e `gate_status: "APPROVED"`.
3. O script salta a leitura dos 3,86M de linhas, os JOINs e o `MERGE INTO`, concluindo a etapa em menos de **77 segundos no Glue** (consumindo apenas ~10 segundos de computação ativa).

##### 2. Estado Final do Catálogo no S3
Após o processamento e aprovação no gate de qualidade:
* **Caminho do Catálogo**: `s3://tc-alfabetizacao-869769158828-us-east-1/silver/alunos_curado/`
* **Formato**: Delta Lake (Apache Parquet com compressão Snappy e log de transações em `_delta_log/`).
* **Volume Final Gravado**: **3.867.999 registros ativos**.

##### 3. Manifesto de Auditoria da Camada Silver
Ao término do Job, o script grava o manifesto em formato JSON na pasta de auditoria do S3 (`s3://.../auditoria/silver/ingestion_date=YYYY-MM-DD/run_id=slv_.../manifest.json`), contendo o status da execução, o volume de entrada/saída, a taxa de match do JOIN e o resultado do gate de qualidade.

##### 4. Métricas FinOps da Execução
| Cenário de Execução | Duração Total | DPUs Consumidas | Custo Estimado (USD) | Status Final |
| ------ | ------ | ------ | ------ | ------ |
| **Carga Inicial / Atualização Delta (`MERGE INTO`)** | ~220s (~3,67 min) | 2.0 DPUs (G.1X) | **US$ 0,0538** | SUCCEEDED |
| **Re-execução sem novos dados (Short-Circuit FinOps)** | ~77s (~1,28 min) | 2.0 DPUs (G.1X) | **US$ 0,0188** | SUCCEEDED (SKIPPED) |

--------------------------------------------------------------------------------

#### 8. Resumo da Conformidade com os Requisitos do Tech Challenge
* **Consumo via Manifesto**: Leitura da Bronze orientada por `manifest.json`.
* **Short-Circuiting FinOps**: Encerramento limpo em re-execuções sem dados novos.
* **Transações ACID no Lakehouse**: Persistência via Delta Lake com `MERGE INTO` (Upsert).
* **Análise Exploratória (EDA / Raio-X)**: Rotina de diagnóstico de NULOs, schemas, duplicidades e preview Markdown das 7 tabelas.
* **Deduplicação Determinística**: Aplicação de Janela PySpark (`deduplicate_by_window`) em chaves primárias de negócio.
* **Tratamento de NULOs do Saeb**: Preservação estrita dos 513.338 NULOs em proficiências.
* **O Grande JOIN**: Integração relacional por chaves compostas sem explosão de linhas.
* **Gate de Qualidade Bloqueante**: Validação da taxa de JOIN em **100,00%** (limiar de 95,0%).
* **Observabilidade e FinOps**: Emissão de relatórios Markdown automáticos e salvamento do manifesto da Silver no S3.
