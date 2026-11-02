# Processador de Eventos Streaming — Arquitetura e Engenharia de Ingestão (events_processor.py)

O componente **`events_processor.py`** é o motor de processamento em tempo quase real do Data Lake Medalhão Híbrido, responsável por consumir, validar, enriquecer e integrar micro-lotes de eventos oriundos da Landing Zone no Amazon S3 à Camada Silver em Delta Lake. Este documento detalha as especificações técnicas, os mecanismos de resiliência e as decisões de engenharia adotadas na implementação do Job AWS Glue.

---

## 1. Visão Geral e Arquitetura do Componente

A arquitetura do fluxo de eventos foi desenhada para conectar a emissão descentralizada de dados operacionais (como atualizações de cadastros de estudantes e repactuações de metas municipais) ao repositório analítico curado. O processador atua como a ponte entre o buffer não estruturado de landing e o snapshot transacional Delta Lake na Camada Silver.

A esteira de execução do processador opera sob quatro pilares fundamentais de engenharia:
1. **Curadoria Reativa e Idempotente**: Atualização dos dados do estudante via operabilidade *Upsert* sem duplicação de registros.
2. **Short-Circuit FinOps**: Cancelamento antecipado de tarefas Spark caso não existam arquivos no buffer, reduzindo drasticamente os custos computacionais.
3. **Controle Estrito de Offsets**: Utilização de mecanismo de streaming sob demanda com *checkpoints* para garantir o consumo exclusivo de arquivos inéditos.
4. **Auditabilidade de Trânsito**: Medição precisa da latência entre o momento do evento na origem e sua efetiva gravação no Data Lake.

```text
+-----------------------------------------------------------------------------------+
|                        1. S3 LANDING ZONE (NDJSON)                                |
|  s3://tc-alfabetizacao-.../bronze/events_raw_landing/{alunos|meta_*}/             |
+-----------------------------------------------------------------------------------+
                                          |
                                          | (1) Pre-Check Boto3 (Short-Circuit)
                                          v
+-----------------------------------------------------------------------------------+
|                   2. AWS GLUE JOB (events_processor.py)                           |
|  * Spark Structured Streaming (trigger AvailableNow)                              |
|  * Control Offsets via Checkpoint no S3                                           |
|  * Enriquecimento: Mapeamento IBGE UF + Timestamps UTC                           |
|  * Cálculo de Latência Real: ingested_at - event_time                             |
+-----------------------------------------------------------------------------------+
                                          |
                                          | (2) MERGE INTO (Intersecção Dinâmica)
                                          v
+-----------------------------------------------------------------------------------+
|                   3. DELTA LAKE SILVER (alunos_curado/)                           |
|  s3://tc-alfabetizacao-.../silver/alunos_curado/                                  |
+-----------------------------------------------------------------------------------+
                                          |
                                          | (3) Registro de Auditoria
                                          v
+-----------------------------------------------------------------------------------+
|                   4. MANIFESTO DE AUDITORIA JSON                                  |
|  s3://tc-alfabetizacao-.../auditoria/events/ingestion_date=.../manifest.json       |
+-----------------------------------------------------------------------------------+
```

---

## 2. Mecanismo FinOps de Pré-Checagem (Short-Circuit)

Em arquiteturas serverless distribuídas como o AWS Glue, a inicialização da JVM e a alocação de Workers PySpark (Worker Type G.1X) geram custos mínimos de billing mesmo para execuções sem dados a processar. Para evitar desperdício de DPUs (*Data Processing Units*), o script executa uma etapa de **Short-Circuit FinOps** utilizando a biblioteca leve `boto3` antes de carregar o contexto pesado do Spark.

A função de pré-checagem consulta o prefixo do S3 de forma rápida e encerra o script com código de saída limpo caso a Landing Zone esteja vazia.

| Parâmetro de Checagem | Valor / Implementação |
| :--- | :--- |
| **Cliente de Ingestão** | `boto3.client('s3')` |
| **Prefixo Monitorado** | `bronze/events_raw_landing/` |
| **Estratégia de Busca** | `list_objects_v2` com `MaxKeys=5` filtrando sufixos `.json` |
| **Ação Vazia** | Gravação de manifesto com status `SKIPPED (No New Events)` e `sys.exit(0)` |
| **Economia Estimada** | Redução do tempo de execução de ~120s para < 3s quando não há eventos |

```python
def check_s3_events_exist(bucket_name, prefix):
    """Executa pré-checagem leve no S3 via Boto3 para o Short-Circuit FinOps."""
    if not HAS_BOTO3:
        return True
    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=5)
        if "Contents" in response:
            json_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".json")]
            return len(json_files) > 0
        return False
    except Exception as e:
        print(f"⚠️ Alerta na pré-checagem S3: {e}")
        return True
```

---

## 3. Estratégia de Ingestão e Leitura Incremental

Para lidar com estruturas de diretórios organizadas em partições de data (ex: `ref_ano=YYYY/ref_mes=MM/ref_dia=DD/`), o leitor do PySpark utiliza a propriedade `recursiveFileLookup = true`. Isso permite que o mecanismo encontre recursivamente todos os arquivos NDJSON espalhados por subpastas dentro da Landing Zone.

A leitura streaming combina a flexibilidade do Spark Structured Streaming com o padrão de execução sob demanda (*Trigger AvailableNow*), garantindo controle de *offsets* sem a necessidade de mover arquivos no S3.

### Funcionalidades do Leitor Incremental:
* **Inexistência de Duplo Processamento**: O Spark armazena o estado dos arquivos já processados no diretório de *checkpoint* (`s3://.../checkpoints/events_alunos/`).
* **Varredura Recursiva**: A opção `recursiveFileLookup` supera limitações de máscaras de caminho (*glob patterns*), varrendo partições dinâmicas de landing.
* **Processamento Batch sob Demanda**: O gatilho `trigger(availableNow=True)` consome todos os micro-lotes pendentes na fila, consolida as alterações e encerra o job com segurança.

---

## 4. Transformação, Enriquecimento e Medição de Latência

Assim que os registros em formato NDJSON são lidos, o processador aplica um conjunto de transformações de esquema, enriquecimentos de dados geográficos e cálculos de observabilidade.

O fluxo de transformação executa as seguintes operações nos dados de estudantes:
1. **Timestamp UTC de Ingestão**: Criação das colunas de controle `_ingested_at` e `_processed_silver_at`.
2. **Cálculo da Latência Real de Trânsito**: A latência em segundos é calculada pela diferença entre o horário de ingestão e o momento da ocorrência do evento (`event_time`):
   $$\text{latencia\_segundos} = \text{cast}(\_ingested\_at, \text{double}) - \text{cast}(\_event\_time, \text{double})$$
3. **Métricas Estatísticas de Trânsito**: Coleta automatizada da latência média, mínima e máxima do lote para inclusão no manifesto de auditoria.
4. **Derivação de Sigla UF via IBGE**: Mapeamento do código de 2 dígitos do município (`id_municipio`) para a sigla da Unidade Federativa correspondente, utilizando tabela Hash nativa em Spark SQL.
5. **Derivação de Partições do Delta Lake**: Atribuição explícita de `ref_ano`, `ref_mes` e `ref_dia` a partir da data do evento para compatibilidade total com o esquema de partição da Silver.

---

## 5. Integração Idempotente na Camada Silver (Delta MERGE INTO)

A gravação na Camada Silver (`s3://.../silver/alunos_curado/`) exige rigor no tratamento de atualizações. Para evitar duplicidades e garantir a integridade dos dados, o script utiliza a instrução `MERGE INTO` do Delta Lake através da biblioteca `delta.tables.DeltaTable`.

A instrução verifica a correspondência pela chave composta formada pelo identificador único do estudante e o ano letivo (`target.id_aluno = source.id_aluno AND target.ano = source.ano`).

### Desafio de Esquema e Intersecção Dinâmica de Colunas

A tabela destino na Camada Silver contém colunas agregadas previamente calculadas durante a carga em lote (como `taxa_alfabetizacao_municipio`). Caso o comando de *merge* utilize a cláusula simplificada `whenMatchedUpdateAll()`, o Delta Lake tenta obrigatoriamente encontrar essa coluna no evento de origem, gerando erros de resolução de esquema.

Para solucionar este desafio, o `events_processor.py` realiza a **intersecção dinâmica de colunas**:

```python
# Descobre dinamicamente apenas as colunas presentes tanto na origem quanto no destino Delta
target_cols = set(delta_target.toDF().schema.names)
update_cols = [
    c for c in df_events_alunos.columns
    if c in target_cols and c not in ["_ingested_at_ts", "_event_time_ts", "latencia_segundos"]
]

update_set = {c: f"source.{c}" for c in update_cols}
insert_values = {c: f"source.{c}" for c in update_cols}

delta_target.alias("target").merge(
    source=df_events_alunos.alias("source"),
    condition="target.id_aluno = source.id_aluno AND target.ano = source.ano"
).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_values).execute()
```

Esta técnica garante três benefícios críticos de engenharia:
- **Preservação de Colunas Agregadas**: Mantém os valores de colunas da tabela destino que não existem no evento.
- **Isolamento de Colunas Temporárias**: Descarta colunas utilitárias de processamento (ex: `latencia_segundos`) antes de gravar no Delta Lake.
- **Segurança de Tipagem**: Atualiza estritamente os atributos operacionais modificados (ex: proficiência, presença, preenchimento do caderno).

---

## 6. Governança, Ciclo de Vida do S3 e Manifesto de Auditoria

A governança do fluxo de eventos combina rastreabilidade transacional com políticas automáticas de gerenciamento de custos de armazenamento no Amazon S3.

### A. Política de Expiração do S3 (S3 Lifecycle Rule)
Durante o deploy da infraestrutura (`deploy_aws_scripts.py`), o ambiente provisiona uma regra nativa de ciclo de vida no bucket S3 chamada `ExpireRawLandingZoneEventsAfter7Days`.

| Propriedade S3 Lifecycle | Configuração Aplicada |
| :--- | :--- |
| **Nome da Regra** | `ExpireRawLandingZoneEventsAfter7Days` |
| **Filtro de Prefixo** | `bronze/events_raw_landing/` |
| **Ação Automática** | Exclusão permanente de objetos após 7 dias da criação |
| **Benefício** | Eliminação de custos recorrentes de armazenamento de JSONs brutos já consumidos |

### B. Manifesto de Auditoria JSON
Ao final de cada execução (seja processada ou omitida por falta de dados), o job salva um manifesto de auditoria no formato JSON em `s3://.../auditoria/events/ingestion_date=YYYY-MM-DD/run_id=.../manifest.json`.

```json
{
  "job_name": "04_events_processor",
  "run_id": "jr_7e2f2a9d72100d776a0a816ac50b12c2f48d135694ccd218c42b6118de278397",
  "ingestion_date": "2026-11-01",
  "status": "SUCCEEDED",
  "total_events_processed": 120,
  "alunos_events_count": 100,
  "metas_events_count": 20,
  "latency_metrics_seconds": {
    "avg_sec": 4899.403,
    "min_sec": 4899.402,
    "max_sec": 4899.404
  },
  "message": "Eventos integrados com sucesso no Delta Lake."
}
```

O manifesto garante rastreabilidade completa para equipes de observabilidade, contendo a contagem exata de registros afetados por domínio, o identificador da execução no AWS Glue e os parâmetros estatísticos de tempo de trânsito dos micro-lotes.
