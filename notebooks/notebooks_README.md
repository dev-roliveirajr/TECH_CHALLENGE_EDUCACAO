# 📓 Notebooks de Análise Exploratória & Visualizações (EDA)

Esta pasta contém o Jupyter Notebook interativo para exploração visual e diagnóstico analítico dos dados consolidados na **Camada Gold** do Data Lakehouse do projeto **Tech Challenge (Fase 2)**.

---

## 🚀 Como Executar

### 1. Pré-requisito Obrigatório
Este notebook foi projetado para consumir **exclusivamente os dados reais processados e sincronizados pelo pipeline**. Antes de abri-lo, execute a esteira completa para gerar e popular a pasta local `data/features/`:

```bash
# Executa a esteira end-to-end (Bronze -> Silver -> Gold -> Sync Local)
python src/pipeline.py
```

### 2. Abrindo o Notebook
Após a conclusão da pipeline, inicie a interface do Jupyter em seu ambiente virtual:

```bash
jupyter notebook notebooks/eda_analytics_fase2.ipynb
```

---

## 📊 Estrutura e Conteúdo do Notebook (`eda_analytics_fase2.ipynb`)

1. **Carregamento e Diagnóstico de Dados**: Ingestão dos arquivos Parquet locais (`data/features/gold_features_alfabetizacao.parquet`).
2. **Visão Executiva de Metas (2030)**: Gráfico de Rosca (*Donut Chart*) exibindo o percentual de municípios que atingiram a meta oficial pactuada de **80,0% de alfabetização na rede pública**.
3. **Ranking por Unidade da Federação (UF)**: Gráfico de barras horizontal comparando o desempenho médio dos 27 estados com as duas linhas de referência exigidas no edital:
   * 🔴 **Meta Pactuada Oficial (80,0%)**: Trajetória formal mensurável de 2024 a 2030.
   * 🟢 **Cenário Aspiracional (100,0%)**: Leitura de universalização do Compromisso Nacional.
4. **Distribuição de Frequência Municipal**: Histograma com curva KDE de densidade e mediana nacional.
5. **Matriz de Correlação de Pearson**: Heatmap avaliando a relação entre volume de alunos, proficiência simples e taxas recalculadas, orientando a preparação de variáveis para **Machine Learning na Fase 3**.

---

## 💡 Destaques de Governança e Regras do Tech Challenge

* **Sem Simulações Sintéticas**: Consome unicamente o resultado curado das 3,86 milhões de linhas de microdados do Saeb.
* **Metos e Corte do Saeb**: Respeita o corte oficial de **743 pontos** de proficiência da Pesquisa Alfabetiza Brasil.
* **Metodologia Saeb Preservada**: Utiliza a taxa de alfabetização recalculada via **média ponderada com o peso amostral (`peso_aluno`)**.
* **Prevenção contra Target Leakage**: Identifica e orienta o isolamento da variável alvo binária (`rotulo_alvo`) para o futuro treinamento de modelos preditivos na Fase 3.
