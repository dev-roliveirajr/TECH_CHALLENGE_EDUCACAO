# 📓 Notebooks de Análise Exploratória & Visualizações (EDA)

Esta pasta contém um notebook Jupyter para análise dos dados consolidados na **Camada Gold**.

---

##  Como Executar

### Pré-requisito
Antes de abrir o notebook, execute o pipeline para gerar os arquivos em `data/features/`:

```bash
# Executa a esteira end-to-end (Bronze -> Silver -> Gold -> Sync Local)
python src/pipeline.py
```

### 2. Abrindo o Notebook
Após a conclusão da pipeline, inicie a interface do Jupyter em seu ambiente virtual:

```bash
jupyter notebook notebooks/eda_analytics.ipynb
```

---

## Estrutura e conteúdo do notebook (`eda_analytics.ipynb`)

1. **Carregamento das Feature Stores**: Leitura dos Parquets de município, UF e Brasil.
2. **Visão Executiva Municipal**: Gráfico de rosca do status frente à meta oficial de **80,0%**, na rede Municipal (`"3"`), no último ano oficial disponível.
3. **Ranking das UFs**: Gráfico de barras da taxa ponderada da rede Pública (`"5"`) no último ano oficial disponível, comparada à meta oficial.
4. **Evolução Nacional**: Série dos anos oficiais frente à meta de 80%; registros sintéticos de teste aparecem separados, sem conexão à série oficial.
5. **Distribuição Municipal**: Histograma das taxas municipais no último ano oficial disponível.
6. **Matriz de Correlação**: Heatmap das relações entre volume de alunos, proficiência e taxas recalculadas.

---

## Governança e regras de dados

* A análise utiliza os dados disponíveis em `data/features/`.
* A taxa de alfabetização é calculada como média ponderada pelo peso amostral (`peso_aluno`).
* A variável alvo (`rotulo_alvo`) deve ser separada das variáveis explicativas durante o treinamento dos modelos.
* As features e os indicadores são publicados em arquivos separados por nível geográfico.
* As comparações usam exclusivamente a meta oficial de 80% em 2030; não há meta aspiracional.
* As visões públicas de UF e Brasil usam a Feature Store gerada pela soma ponderada das redes Estadual (`"2"`) e Municipal (`"3"`), identificada como rede `"5"`.
* Eventos emitidos pelos geradores são sintéticos; os registros desses testes não representam resultados oficiais do Inep.
* No snapshot atual, os anos oficiais são 2023 e 2024. Os registros sintéticos de 2025 e 2026 são excluídos dos rankings, da distribuição municipal e da correlação; a evolução nacional os exibe apenas como pontos de teste separados.
