# Camada Silver

Este documento descreve o processamento das entidades Bronze, as transformações da
Camada Silver e os contratos de entrada e saída do job AWS Glue
`02_silver_processing`.

## Entradas

O job lê o manifesto Bronze mais recente em
`auditoria/bronze/ingestion_date=*/run_id=*/manifest.json`. O manifesto precisa
ter status `SUCCEEDED`, registrar as sete entidades esperadas e apontar para
prefixos no bucket configurado:

- `alunos`
- `uf`
- `municipio`
- `meta_alfabetizacao_brasil`
- `meta_alfabetizacao_uf`
- `meta_alfabetizacao_municipio`
- `dicionario`

Entidades com status `SUCCEEDED` ou `SKIPPED (Already Up-to-Date)` são aceitas.
O job lê somente os prefixos informados no manifesto. Manifesto ausente, inválido
ou incompleto, e falhas de acesso ou leitura interrompem a execução.

## Processamento

Para cada entidade, o job registra schema, uma amostra limitada a cinco linhas,
volume, nulos por coluna e duplicidades exatas e por chave de negócio. O cálculo
dos nulos é feito em uma agregação por tabela. Quando existem duplicidades por
chave, aplica `deduplicate_by_window` de `src/common/quality_lib.py`, ordenando
por `_ingested_at` e `proficiencia` quando essas colunas estão disponíveis.

As chaves usadas para identificar duplicidades são:

| Entidade | Chaves |
|---|---|
| `alunos` | `id_aluno`, `id_municipio`, `ano`, `rede`, `serie` |
| `municipio` | `id_municipio`, `ano`, `rede`, `serie` |
| `uf` | `sigla_uf`, `ano`, `rede`, `serie` |
| `meta_alfabetizacao_brasil` | `ano`, `rede` |
| `meta_alfabetizacao_uf` | `sigla_uf`, `ano`, `rede` |
| `meta_alfabetizacao_municipio` | `id_municipio`, `ano`, `rede` |
| `dicionario` | `id_tabela`, `nome_coluna`, `chave` |

O job deriva `sigla_uf` dos dois primeiros dígitos de `id_municipio`, quando a
coluna ainda não existe. Remove metadados de ingestão das dimensões, renomeia
métricas territoriais para evitar colisões e realiza junções à esquerda entre
alunos e as dimensões municipal, estadual e de metas. Valores nulos de
proficiência e peso do aluno são preservados.

A taxa de correspondência municipal é calculada a partir de uma coluna auxiliar
proveniente da dimensão municipal, para distinguir correspondências reais das
chaves mantidas pela junção à esquerda. O limiar é configurado por
`JOIN_MATCH_THRESHOLD` (padrão `95.0`). `BLOCK_ON_QUALITY_FAILURE` controla se
uma taxa abaixo do limiar interrompe o job (padrão `true`).

## Saídas e execução sem alterações

O conjunto curado é gravado em
`s3://<bucket>/silver/alunos_curado/`, no formato Delta Lake. Se o destino Delta
já existir, o job executa `MERGE` usando `id_aluno` e `ano`; caso contrário,
cria o conjunto. A tabela é registrada como `silver_alunos_curado` no Glue Data
Catalog, no banco configurado por `GLUE_DATABASE_NAME`.

Quando todas as entidades Bronze estão como `SKIPPED (Already Up-to-Date)` e o
destino Delta existe, o job mantém a Silver sem ler as tabelas e grava um
manifesto com status `SKIPPED (Already Up-to-Date)`. Nos demais casos, grava um
manifesto de sucesso em
`auditoria/silver/ingestion_date=<data>/run_id=<execução>/manifest.json`, com
contagens de entrada e saída, quantidade de correspondências municipais, taxa
de correspondência, status do gate de qualidade e caminho de saída.

Falhas ao registrar a tabela no catálogo ou gravar o manifesto são propagadas
como falhas do job. O módulo `src/common/manifest_lib.py` é distribuído ao Glue
junto aos demais arquivos Python usados pelo job.
