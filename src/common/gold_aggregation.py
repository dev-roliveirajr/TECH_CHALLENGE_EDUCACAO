"""Reusable Spark transformations for Gold-level student indicators."""

from pyspark.sql import functions as F

FEATURE_COLUMNS = [
    "id_municipio",
    "sigla_uf",
    "ano",
    "rede",
    "serie",
    "total_alunos_avaliados",
    "soma_peso_amostral",
    "media_proficiencia_simples",
    "taxa_alfabetizacao_recalculada",
    "meta_pactuada_2030",
    "gap_para_meta",
    "status_meta",
    "_created_gold_at",
]

GOLD_TRANSFORM_VERSION = "2026-11-public-network-weighted-v1"


def aggregate_indicators(
    students,
    group_columns,
    official_rate_column=None,
):
    """Calcula a taxa ponderada e indicadores para uma granularidade geográfica."""
    aggregations = [
        F.count("*").alias("total_alunos_avaliados"),
        F.sum("peso_aluno").alias("soma_peso_amostral"),
        F.sum(
            F.when(
                F.col("alfabetizado").isin("Sim", "1", 1),
                F.col("peso_aluno"),
            ).otherwise(0)
        ).alias("soma_peso_alfabetizados"),
        F.avg("proficiencia").alias("media_proficiencia_simples"),
    ]
    if official_rate_column and official_rate_column in students.columns:
        aggregations.append(
            F.first(
                F.col(official_rate_column),
                ignorenulls=True,
            ).alias("taxa_alfabetizacao_oficial_inep")
        )

    indicators = students.groupBy(*group_columns).agg(*aggregations)
    if "taxa_alfabetizacao_oficial_inep" not in indicators.columns:
        indicators = indicators.withColumn(
            "taxa_alfabetizacao_oficial_inep",
            F.lit(None).cast("double"),
        )

    indicators = indicators.withColumn(
        "taxa_alfabetizacao_recalculada",
        F.when(
            F.col("soma_peso_amostral") > 0,
            F.round(
                F.col("soma_peso_alfabetizados") / F.col("soma_peso_amostral") * 100.0,
                2,
            ),
        ),
    )
    for column in ("id_municipio", "sigla_uf"):
        if column not in indicators.columns:
            indicators = indicators.withColumn(column, F.lit(None).cast("string"))
    return indicators


def aggregate_public_indicators(students, group_columns):
    """Agrega as redes Estadual e Municipal como a rede pública oficial."""
    public_students = students.filter(F.col("rede").isin("2", "3")).withColumn(
        "rede", F.lit("5")
    )
    return aggregate_indicators(
        public_students,
        [*group_columns, "rede"],
    )


def compare_to_official_target(indicators, target_network):
    """Compara somente a rede compatível com a meta oficial do respectivo nível."""
    return (
        indicators.filter(F.col("rede") == target_network)
        .withColumn("meta_pactuada_2030", F.lit(80.0))
        .withColumn(
            "gap_para_meta",
            F.round(
                F.col("taxa_alfabetizacao_recalculada") - F.col("meta_pactuada_2030"),
                2,
            ),
        )
        .withColumn(
            "status_meta",
            F.when(
                F.col("taxa_alfabetizacao_recalculada").isNull(),
                F.lit("Sem Resultado Calculado"),
            )
            .when(
                F.col("taxa_alfabetizacao_recalculada") >= F.col("meta_pactuada_2030"),
                F.lit("Atingiu a Meta"),
            )
            .otherwise(F.lit("Não Atingiu a Meta")),
        )
        .withColumn("_created_gold_at", F.current_timestamp())
    )


def should_skip_gold_processing(
    silver_status,
    gold_outputs_exist,
    previous_transform_version,
):
    """Só reutiliza Gold quando a versão da transformação também é atual."""
    return (
        silver_status == "SKIPPED (Already Up-to-Date)"
        and gold_outputs_exist
        and previous_transform_version == GOLD_TRANSFORM_VERSION
    )


def build_feature_store(meta_dataset):
    """Seleciona as características e o rótulo-alvo para um nível geográfico."""
    feature_columns = [
        F.col(column).alias("rotulo_alvo") if column == "status_meta" else F.col(column)
        for column in FEATURE_COLUMNS
    ]
    return meta_dataset.select(*feature_columns)
