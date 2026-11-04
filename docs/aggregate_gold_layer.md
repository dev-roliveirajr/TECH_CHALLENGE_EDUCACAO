# Camada Gold

O job `aggregate_gold_layer.py` lê o snapshot curado de alunos em
`silver/alunos_curado/` e calcula taxas ponderadas usando `peso_aluno`.
Os indicadores são produzidos por município, UF e Brasil, agrupados por ano,
rede e série. Cada nível agrega diretamente os microdados avaliados; as taxas
de UF e Brasil não são médias simples das taxas dos municípios ou estados.

## Regra de cálculo

São considerados alunos com proficiência e peso amostral informados. Para cada
grupo, a taxa recalculada é:

`100 * soma(peso_aluno dos alfabetizados) / soma(peso_aluno)`

Os valores de `rede` permanecem conforme o dicionário oficial:

- `"2"`: Estadual
- `"3"`: Municipal
- `"5"`: Pública (Estadual e Municipal)

Os indicadores descritivos mantêm todas as redes que existirem nos dados. Nos
comparativos municipal, estadual e nacional, a rede `"3"` representa
`Municipal`; para comparar com a meta `Pública` de UF e Brasil, o job combina
os microdados das redes `"2"` (Estadual) e `"3"` (Municipal), soma os pesos e
os pesos dos alfabetizados e identifica o resultado derivado como rede `"5"`.
Assim, a taxa combinada é recalculada sobre os microdados e não é uma média
simples das taxas das redes.

## Meta e variável-alvo

A meta usada em todas as comparações é a meta oficial de 2030, fixada em
`80.0%`. O job calcula `gap_para_meta` e `status_meta` em relação a esse único
valor. As Feature Stores são publicadas separadamente por nível geográfico e
expõem `rotulo_alvo` com o status de alcance da meta; não há meta aspiracional.

A validação cruzada entre microdados e a taxa publicada é executada no nível
municipal, quando a taxa oficial está disponível na Silver.

## Saídas Gold

Cada dataset é gravado em Parquet no S3 e registrado no Glue Data Catalog:

| Tabela | Prefixo S3 | Conteúdo |
|---|---|---|
| `gold_indicadores_municipais` | `gold/gold_indicadores_municipais/` | Indicadores por município, ano, rede e série |
| `gold_indicadores_uf` | `gold/gold_indicadores_uf/` | Indicadores por UF, ano, rede e série |
| `gold_indicadores_brasil` | `gold/gold_indicadores_brasil/` | Indicadores nacionais por ano, rede e série |
| `gold_desempenho_vs_metas` | `gold/gold_desempenho_vs_metas/` | Comparativo municipal para a rede `"3"` |
| `gold_desempenho_vs_metas_uf` | `gold/gold_desempenho_vs_metas_uf/` | Comparativo estadual para a rede `"5"` |
| `gold_desempenho_vs_metas_brasil` | `gold/gold_desempenho_vs_metas_brasil/` | Comparativo nacional para a rede `"5"` |
| `gold_features_alfabetizacao` | `gold/gold_features_alfabetizacao/` | Feature Store municipal |
| `gold_features_alfabetizacao_uf` | `gold/gold_features_alfabetizacao_uf/` | Feature Store estadual |
| `gold_features_alfabetizacao_brasil` | `gold/gold_features_alfabetizacao_brasil/` | Feature Store nacional |

O manifesto em `auditoria/gold/` registra contagens totais e por nível, a
meta aplicada, a versão da transformação e os prefixos gerados. Os datasets
municipais mantêm seus nomes e esquemas existentes; as saídas de UF e Brasil
são tabelas adicionais. O job só registra `SKIPPED` quando a Silver não mudou,
todos os nove datasets Gold esperados existem e foram produzidos pela versão
atual da transformação.

Após a execução da Gold, as pipelines batch e de eventos sincronizam os nove
datasets para `data/features/` como arquivos Parquet consolidados.
