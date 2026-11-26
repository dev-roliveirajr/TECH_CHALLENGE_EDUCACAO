"""Reusable Spark transformations for Gold-level student indicators."""

from functools import reduce

from pyspark.sql import functions as F

COMPARISON_COLUMNS = [
    "id_municipio",
    "sigla_uf",
    "ano",
    "rede",
    "serie",
    "total_alunos_avaliados",
    "soma_peso_amostral",
    "media_proficiencia_simples",
    "taxa_alfabetizacao_recalculada",
    "meta_pactuada_anual",
    "gap_para_meta_anual",
    "status_meta_anual",
    "_created_gold_at",
]

ANNUAL_TARGET_COLUMNS = tuple(range(2024, 2031))
COMPARISON_PRODUCT_ROLE = "descriptive_comparison_not_training_features"
GOLD_TRANSFORM_VERSION = "2026-11-annual-official-targets-v2"


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


def normalize_official_targets(
    source,
    geo_level,
    source_entity,
    source_manifest,
    shared_across_series_confirmed=False,
):
    """Converte metas oficiais wide em um contrato anual long e validado."""
    territory_column = {
        "municipio": "id_municipio",
        "uf": "sigla_uf",
        "brasil": None,
    }.get(geo_level)
    if geo_level not in {"municipio", "uf", "brasil"}:
        raise ValueError(f"Nível geográfico de meta inválido: {geo_level!r}.")

    annual_columns = {
        f"meta_alfabetizacao_{year}" for year in ANNUAL_TARGET_COLUMNS
    }
    missing_columns = annual_columns.difference(source.columns)
    if missing_columns:
        raise ValueError(
            f"A fonte de metas {source_entity!r} não contém a trajetória anual "
            f"esperada: {sorted(missing_columns)}."
        )
    if "rede" not in source.columns:
        raise ValueError(f"A fonte de metas {source_entity!r} não contém 'rede'.")
    if territory_column and territory_column not in source.columns:
        raise ValueError(
            f"A fonte de metas {source_entity!r} não contém a chave "
            f"territorial {territory_column!r}."
        )
    if "_source" not in source.columns:
        raise ValueError(
            f"A fonte de metas {source_entity!r} não contém a proveniência '_source'."
        )
    if source.limit(1).count() == 0:
        raise ValueError(f"A fonte de metas {source_entity!r} está vazia.")

    invalid_source_rows = source.filter(
        F.col("_source").isNull()
        | (F.col("_source") != F.lit("basedosdados_bigquery"))
    ).limit(1).count()
    if invalid_source_rows:
        raise ValueError(
            f"A fonte {source_entity!r} contém linhas sem proveniência oficial "
            "basedosdados_bigquery."
        )
    invalid_target_values = source.filter(
        reduce(
            lambda left, right: left | right,
            [
                F.col(column).isNotNull() & F.col(column).cast("double").isNull()
                for column in sorted(annual_columns)
            ],
            F.lit(False),
        )
    ).limit(1).count()
    if invalid_target_values:
        raise ValueError(
            f"A fonte de metas {source_entity!r} contém valores anuais "
            "não numéricos."
        )

    key_columns = [territory_column] if territory_column else []
    has_series = "serie" in source.columns
    if has_series:
        key_columns.append("serie")
    elif not shared_across_series_confirmed:
        raise ValueError(
            f"A fonte {source_entity!r} não informa 'serie'. Confirme no "
            "dicionário oficial que a meta é compartilhada entre séries antes "
            "de permitir essa comparação."
        )

    key_expressions = [
        F.col(column).alias(column) for column in key_columns
    ]
    source_year = (
        F.col("ano").alias("source_ano")
        if "ano" in source.columns
        else F.lit(None).cast("int").alias("source_ano")
    )
    network = F.trim(F.col("rede").cast("string"))
    normalized_network = (
        F.when(F.lower(network).isin("3", "municipal"), F.lit("3"))
        .when(
            F.lower(network).isin("5", "publica", "pública"),
            F.lit("5"),
        )
        .otherwise(network)
        .alias("target_rede")
    )
    stack_values = ", ".join(
        f"{year}, cast(`meta_alfabetizacao_{year}` as double)"
        for year in ANNUAL_TARGET_COLUMNS
    )
    normalized = source.select(
        *key_expressions,
        normalized_network,
        source_year,
        F.lit(geo_level).alias("geo_level"),
        F.lit(source_entity).alias("source_entity"),
        F.lit(source_manifest).alias("source_manifest"),
        F.col("_source"),
        F.col("_ingested_at") if "_ingested_at" in source.columns else F.lit(None).alias("_ingested_at"),
        F.expr(
            f"stack({len(ANNUAL_TARGET_COLUMNS)}, {stack_values}) "
            "as (target_year, target_percent)"
        ),
    )

    invalid_targets = normalized.filter(
        F.col("target_percent").isNotNull()
        & ((F.col("target_percent") < 0) | (F.col("target_percent") > 100))
    ).limit(1).count()
    if invalid_targets:
        raise ValueError(
            f"A fonte de metas {source_entity!r} contém percentuais fora "
            "do intervalo [0, 100]."
        )

    identity_columns = [
        *key_columns,
        "target_rede",
        "target_year",
    ]
    grouped = normalized.groupBy(*identity_columns).agg(
        F.count("*").alias("_source_rows"),
        F.count("target_percent").alias("_non_null_target_rows"),
        F.countDistinct("target_percent").alias("_distinct_target_values"),
        F.min("target_percent").alias("target_percent"),
        F.min("source_ano").alias("source_ano_min"),
        F.max("source_ano").alias("source_ano_max"),
        F.first("geo_level").alias("geo_level"),
        F.first("source_entity").alias("source_entity"),
        F.first("source_manifest").alias("source_manifest"),
        F.first("_source").alias("_source"),
        F.first("_ingested_at", ignorenulls=True).alias("_ingested_at"),
    )
    conflicts = grouped.filter(
        (F.col("_distinct_target_values") > 1)
        | (
            (F.col("_non_null_target_rows") > 0)
            & (F.col("_non_null_target_rows") != F.col("_source_rows"))
        )
    )
    if conflicts.limit(1).count():
        sample = conflicts.select(*identity_columns).limit(5).collect()
        raise ValueError(
            f"A fonte de metas {source_entity!r} contém valores conflitantes "
            f"para a chave anual: {[row.asDict() for row in sample]}."
        )

    return grouped.drop(
        "_source_rows",
        "_non_null_target_rows",
        "_distinct_target_values",
    ).withColumnRenamed("source_ano_min", "source_ano").drop("source_ano_max")


def compare_to_official_target(
    indicators,
    targets,
    target_network,
    shared_across_series_confirmed=False,
):
    """Compara indicadores com meta oficial do mesmo nível, rede e ano."""
    required_target_columns = {
        "target_year",
        "target_rede",
        "target_percent",
        "geo_level",
    }
    missing_target_columns = required_target_columns.difference(targets.columns)
    if missing_target_columns:
        raise ValueError(
            "O conjunto Silver de metas não atende ao contrato Gold: "
            f"colunas ausentes {sorted(missing_target_columns)}."
        )
    if "ano" not in indicators.columns or "rede" not in indicators.columns:
        raise ValueError("Indicadores Gold precisam conter 'ano' e 'rede'.")

    join_keys = []
    for column in ("id_municipio", "sigla_uf"):
        if column in indicators.columns and column in targets.columns:
            join_keys.append(column)
    if "serie" in indicators.columns:
        if "serie" in targets.columns:
            join_keys.append("serie")
        elif not shared_across_series_confirmed:
            raise ValueError(
                "A meta não contém 'serie'. Confirme no dicionário oficial que "
                "ela é compartilhada entre séries antes da comparação."
            )
    if "geo_level" not in targets.columns:
        raise ValueError("O contrato da meta não identifica o nível geográfico.")

    target_rows = targets.filter(F.col("target_rede") == F.lit(target_network))
    target_key_columns = [
        *join_keys,
        "target_year",
        "target_rede",
        "geo_level",
    ]
    duplicate_targets = (
        target_rows.groupBy(*target_key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_targets:
        raise ValueError(
            f"Metas Silver não são únicas para a rede {target_network!r}."
        )

    left = indicators.filter(F.col("rede").cast("string") == target_network).alias(
        "indicator"
    )
    right = target_rows.alias("target")
    conditions = [
        F.col("indicator.ano").cast("int") == F.col("target.target_year").cast("int"),
        F.col("indicator.rede").cast("string") == F.col("target.target_rede"),
    ]
    conditions.extend(
        F.col(f"indicator.{column}") == F.col(f"target.{column}")
        for column in join_keys
    )
    joined = left.join(right, on=conditions, how="left")
    output = joined.select(
        "indicator.*",
        F.col("target.target_percent").alias("meta_pactuada_anual"),
        F.col("target.geo_level").alias("meta_geo_level"),
        F.col("target.source_entity").alias("meta_source_entity"),
        F.col("target.source_manifest").alias("meta_source_manifest"),
        F.col("target.source_ano").alias("meta_source_ano"),
    )
    return (
        output.withColumn(
            "gap_para_meta_anual",
            F.when(
                F.col("taxa_alfabetizacao_recalculada").isNotNull()
                & F.col("meta_pactuada_anual").isNotNull(),
                F.round(
                    F.col("taxa_alfabetizacao_recalculada")
                    - F.col("meta_pactuada_anual"),
                    2,
                ),
            ),
        )
        .withColumn(
            "status_meta_anual",
            F.when(
                F.col("taxa_alfabetizacao_recalculada").isNull()
                & F.col("meta_pactuada_anual").isNull(),
                F.lit("Sem Resultado e Sem Meta"),
            )
            .when(
                F.col("taxa_alfabetizacao_recalculada").isNull(),
                F.lit("Sem Resultado Calculado"),
            )
            .when(
                F.col("meta_pactuada_anual").isNull(),
                F.lit("Sem Meta Cadastrada"),
            )
            .when(
                F.col("taxa_alfabetizacao_recalculada")
                >= F.col("meta_pactuada_anual"),
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
    """Mantém o nome legado sem classificar comparativos como features de ML."""
    return meta_dataset.withColumn(
        "product_role",
        F.lit(COMPARISON_PRODUCT_ROLE),
    )
