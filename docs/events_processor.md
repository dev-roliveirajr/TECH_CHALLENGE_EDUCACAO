# Processador de eventos

O `events_processor.py` consome arquivos NDJSON enviados diretamente à landing
de eventos no S3. Essa entrada não passa pelo job de ingestão batch da Bronze.
O job usa Spark Structured Streaming com `trigger(availableNow=True)` e
checkpoints independentes para alunos e cada domínio de metas.

## Contrato de entrada

| Prefixo na landing | Domínio | Chave territorial | `rede` aceita |
|---|---|---|---|
| `alunos/` | Alunos | `id_municipio` | Código oficial textual de `"0"` a `"6"`; o gerador usa `"2"` e `"3"` |
| `meta_municipio/` | Município | `id_municipio` | `"Municipal"` |
| `meta_uf/` | UF | `sigla_uf` | `"Pública"` |
| `meta_brasil/` | Brasil | `pais="Brasil"` | `"Pública"` |

Eventos de meta devem usar `event_type="META_UPDATE"` e
`meta_alfabetizacao_2030=80.0`. Os rótulos das metas seguem os valores
observados nos Parquets Bronze; nos resultados de alunos, `rede` é o código
textual definido pelo dicionário oficial.

Os geradores gravam JSON Lines com extensão `.json` sob
`bronze/events_raw_landing/{alunos|meta_municipio|meta_uf|meta_brasil}/`,
particionados por `ref_ano`, `ref_mes` e `ref_dia`.

## Pré-checagem e leitura

Antes de criar o contexto Spark, o processador pagina a listagem do prefixo
`bronze/events_raw_landing/` e procura arquivos `.json`. Erros de acesso ao S3
interrompem o job, em vez de serem tratados como ausência de eventos. Quando
não há arquivos, grava um manifesto `SKIPPED (No New Events)` e encerra.

Com dados presentes, o Spark usa `recursiveFileLookup` para encontrar arquivos
nas partições. Os checkpoints ficam em:

- `checkpoints/events/alunos/`
- `checkpoints/events/metas_municipio/`
- `checkpoints/events/metas_uf/`
- `checkpoints/events/metas_brasil/`

O checkpoint registra os arquivos lidos por cada consulta. Os arquivos da
landing não são removidos pelo processador; a retenção é controlada pela regra
de lifecycle configurada no bucket.

## Persistência na Silver

Eventos de alunos são validados, enriquecidos com `sigla_uf` a partir do
prefixo do código IBGE municipal e gravados em `silver/alunos_curado/`. O
processador mede a latência entre `event_time` e o horário de ingestão. Se um
lote tiver mais de um evento para o mesmo aluno e ano, mantém o evento mais
recente antes do upsert.

Eventos de metas são validados contra o domínio do prefixo, a rede esperada, a
chave territorial e a meta oficial de 80%. Cada domínio é gravado em seu
próprio destino:

- `silver/meta_alfabetizacao_municipio/`
- `silver/meta_alfabetizacao_uf/`
- `silver/meta_alfabetizacao_brasil/`

As metas são atualizadas por chave territorial, ano e rede. Duplicatas da
mesma chave em um micro-lote são reduzidas ao evento mais recente antes do
upsert. No AWS Glue, a persistência usa Delta Lake; o modo local usa Parquet.

## Auditoria

Cada execução grava um manifesto em
`auditoria/events/ingestion_date=YYYY-MM-DD/run_id=.../manifest.json`.
O manifesto inclui o total de eventos processados, contagens de alunos e
metas por domínio e estatísticas de latência dos eventos de alunos. Quando há
alterações na Silver, também é gravado um manifesto de atualização em
`auditoria/silver/` para que a execução Gold subsequente não seja ignorada.
