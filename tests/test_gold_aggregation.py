import os
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from common.gold_aggregation import (
    GOLD_TRANSFORM_VERSION,
    aggregate_indicators,
    aggregate_public_indicators,
    build_feature_store,
    compare_to_official_target,
    normalize_official_targets,
    should_skip_gold_processing,
)


@pytest.fixture(scope="module")
def spark():
    java_home = os.environ.get("JAVA_HOME")
    java_from_home = Path(java_home, "bin", "java") if java_home else None
    if not shutil.which("java") and not (
        java_from_home
        and java_from_home.is_file()
        and os.access(java_from_home, os.X_OK)
    ):
        pytest.skip("Java is required for local Spark aggregation tests.")

    session = (
        SparkSession.builder.appName("gold-aggregation-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def student_rows():
    return [
        {
            "id_municipio": "1",
            "sigla_uf": "AA",
            "ano": 2024,
            "rede": "3",
            "serie": "2",
            "proficiencia": 700.0,
            "peso_aluno": 1.0,
            "alfabetizado": "1",
            "taxa_alfabetizacao_municipio": 70.0,
            "taxa_alfabetizacao_uf": 75.0,
        },
        {
            "id_municipio": "1",
            "sigla_uf": "AA",
            "ano": 2024,
            "rede": "3",
            "serie": "2",
            "proficiencia": 800.0,
            "peso_aluno": 3.0,
            "alfabetizado": "0",
            "taxa_alfabetizacao_municipio": 70.0,
            "taxa_alfabetizacao_uf": 75.0,
        },
        {
            "id_municipio": "2",
            "sigla_uf": "AA",
            "ano": 2024,
            "rede": "3",
            "serie": "2",
            "proficiencia": 800.0,
            "peso_aluno": 1.0,
            "alfabetizado": "1",
            "taxa_alfabetizacao_municipio": 90.0,
            "taxa_alfabetizacao_uf": 75.0,
        },
        {
            "id_municipio": "1",
            "sigla_uf": "AA",
            "ano": 2024,
            "rede": "2",
            "serie": "2",
            "proficiencia": 800.0,
            "peso_aluno": 2.0,
            "alfabetizado": "1",
            "taxa_alfabetizacao_municipio": 70.0,
            "taxa_alfabetizacao_uf": 75.0,
        },
        {
            "id_municipio": "3",
            "sigla_uf": "BB",
            "ano": 2024,
            "rede": "2",
            "serie": "2",
            "proficiencia": 700.0,
            "peso_aluno": 1.0,
            "alfabetizado": "0",
            "taxa_alfabetizacao_municipio": 50.0,
            "taxa_alfabetizacao_uf": 50.0,
        },
        {
            "id_municipio": "3",
            "sigla_uf": "BB",
            "ano": 2024,
            "rede": "3",
            "serie": "2",
            "proficiencia": 700.0,
            "peso_aluno": 1.0,
            "alfabetizado": "1",
            "taxa_alfabetizacao_municipio": 50.0,
            "taxa_alfabetizacao_uf": 50.0,
        },
        {
            "id_municipio": "1",
            "sigla_uf": "AA",
            "ano": 2024,
            "rede": "4",
            "serie": "2",
            "proficiencia": 900.0,
            "peso_aluno": 50.0,
            "alfabetizado": "1",
            "taxa_alfabetizacao_municipio": 100.0,
            "taxa_alfabetizacao_uf": 100.0,
        },
    ]


def _indicators_by_level(spark, student_rows):
    students = spark.createDataFrame(student_rows)
    return {
        "municipio": aggregate_indicators(
            students,
            ["id_municipio", "sigla_uf", "ano", "rede", "serie"],
            "taxa_alfabetizacao_municipio",
        ),
        "uf": aggregate_indicators(
            students,
            ["sigla_uf", "ano", "rede", "serie"],
            "taxa_alfabetizacao_uf",
        ),
        "brasil": aggregate_indicators(
            students,
            ["ano", "rede", "serie"],
        ),
    }


def test_indicators_are_weighted_at_each_geographic_level(spark, student_rows):
    indicators = _indicators_by_level(spark, student_rows)

    municipal = indicators["municipio"].filter("id_municipio = '1' AND rede = '3'")
    assert municipal.select("taxa_alfabetizacao_recalculada").first()[0] == 25.0
    assert municipal.select("taxa_alfabetizacao_oficial_inep").first()[0] == 70.0

    uf = indicators["uf"].filter("rede = '3' AND sigla_uf = 'AA'")
    assert uf.select("taxa_alfabetizacao_recalculada").first()[0] == 40.0
    assert uf.select("id_municipio").first()[0] is None

    brasil = indicators["brasil"].filter("rede = '2'")
    assert brasil.select("taxa_alfabetizacao_recalculada").first()[0] == 66.67
    assert brasil.select("sigla_uf").first()[0] is None


def test_public_indicators_combine_state_and_municipal_networks_by_weight(
    spark, student_rows
):
    students = spark.createDataFrame(student_rows)
    uf = aggregate_public_indicators(
        students,
        ["sigla_uf", "ano", "serie"],
    )
    brasil = aggregate_public_indicators(
        students,
        ["ano", "serie"],
    )

    uf_rows = {
        row["sigla_uf"]: row
        for row in uf.select(
            "sigla_uf",
            "rede",
            "soma_peso_amostral",
            "soma_peso_alfabetizados",
            "taxa_alfabetizacao_recalculada",
            "taxa_alfabetizacao_oficial_inep",
        ).collect()
    }
    assert set(uf_rows) == {"AA", "BB"}
    assert uf_rows["AA"]["rede"] == "5"
    assert uf_rows["AA"]["soma_peso_amostral"] == 7.0
    assert uf_rows["AA"]["soma_peso_alfabetizados"] == 4.0
    assert uf_rows["AA"]["taxa_alfabetizacao_recalculada"] == 57.14
    assert uf_rows["AA"]["taxa_alfabetizacao_oficial_inep"] is None
    assert uf_rows["BB"]["taxa_alfabetizacao_recalculada"] == 50.0

    national = brasil.first()
    assert national["rede"] == "5"
    assert national["soma_peso_amostral"] == 9.0
    assert national["soma_peso_alfabetizados"] == 5.0
    assert national["taxa_alfabetizacao_recalculada"] == 55.56


def _official_targets(spark, geo_level, records):
    return spark.createDataFrame(
        [
            {
                **record,
                "target_year": 2024,
                "target_rede": "3" if geo_level == "municipio" else "5",
                "target_percent": 80.0,
                "geo_level": geo_level,
                "source_entity": f"meta_alfabetizacao_{geo_level}",
                "source_manifest": "s3://data-bucket/manifest.json",
                "_source": "basedosdados_bigquery",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "source_ano": 2024,
            }
            for record in records
        ]
    )


def test_comparisons_match_official_network_and_yearly_target(spark, student_rows):
    indicators = _indicators_by_level(spark, student_rows)
    students = spark.createDataFrame(student_rows)

    municipal_targets = _official_targets(
        spark,
        "municipio",
        [
            {"id_municipio": municipality, "serie": "2"}
            for municipality in ("1", "2", "3")
        ],
    )
    uf_targets = _official_targets(
        spark,
        "uf",
        [{"sigla_uf": state, "serie": "2"} for state in ("AA", "BB")],
    )
    brasil_targets = _official_targets(spark, "brasil", [{"serie": "2"}])

    municipal = compare_to_official_target(
        indicators["municipio"], municipal_targets, "3"
    )
    uf = compare_to_official_target(
        aggregate_public_indicators(students, ["sigla_uf", "ano", "serie"]),
        uf_targets,
        "5",
    )
    brasil = compare_to_official_target(
        aggregate_public_indicators(students, ["ano", "serie"]),
        brasil_targets,
        "5",
    )

    assert {row["rede"] for row in municipal.select("rede").distinct().collect()} == {
        "3"
    }
    assert {row["rede"] for row in uf.select("rede").distinct().collect()} == {"5"}
    assert {row["rede"] for row in brasil.select("rede").distinct().collect()} == {"5"}

    municipal_below_target = municipal.filter("id_municipio = '1'").first()
    assert municipal_below_target["meta_pactuada_anual"] == 80.0
    assert municipal_below_target["gap_para_meta_anual"] == -55.0
    assert municipal_below_target["status_meta_anual"] == "Não Atingiu a Meta"
    assert (
        uf.filter("sigla_uf = 'AA'").first()["taxa_alfabetizacao_recalculada"] == 57.14
    )
    assert brasil.first()["taxa_alfabetizacao_recalculada"] == 55.56


def test_legacy_feature_exports_are_descriptive_not_model_features(spark, student_rows):
    indicators = _indicators_by_level(spark, student_rows)
    students = spark.createDataFrame(student_rows)
    municipal_targets = _official_targets(
        spark,
        "municipio",
        [
            {"id_municipio": municipality, "serie": "2"}
            for municipality in ("1", "2", "3")
        ],
    )
    uf_targets = _official_targets(
        spark,
        "uf",
        [{"sigla_uf": state, "serie": "2"} for state in ("AA", "BB")],
    )
    brasil_targets = _official_targets(spark, "brasil", [{"serie": "2"}])
    comparisons = [
        compare_to_official_target(indicators["municipio"], municipal_targets, "3"),
        compare_to_official_target(
            aggregate_public_indicators(students, ["sigla_uf", "ano", "serie"]),
            uf_targets,
            "5",
        ),
        compare_to_official_target(
            aggregate_public_indicators(students, ["ano", "serie"]),
            brasil_targets,
            "5",
        ),
    ]

    feature_stores = [build_feature_store(comparison) for comparison in comparisons]
    assert [feature_store.count() for feature_store in feature_stores] == [3, 2, 1]
    for feature_store in feature_stores:
        assert "rotulo_alvo" not in feature_store.columns
        assert "product_role" in feature_store.columns
        assert (
            feature_store.filter(
                "product_role != 'descriptive_comparison_not_training_features'"
            ).count()
            == 0
        )


def test_annual_target_normalization_validates_source_and_conflicts(spark):
    target_columns = {
        f"meta_alfabetizacao_{year}": 70.0 + year - 2024
        for year in range(2024, 2031)
    }
    source = spark.createDataFrame(
        [
            {
                "id_municipio": "1",
                "serie": "2",
                "ano": 2024,
                "rede": "Municipal",
                "_source": "basedosdados_bigquery",
                "_ingested_at": "2026-01-01T00:00:00Z",
                **target_columns,
            }
        ]
    )

    normalized = normalize_official_targets(
        source,
        geo_level="municipio",
        source_entity="meta_alfabetizacao_municipio",
        source_manifest="s3://data-bucket/manifest.json",
    )
    assert normalized.count() == 7
    assert (
        normalized.filter("target_year = 2030").first()["target_percent"] == 76.0
    )

    without_series = source.drop("serie")
    with pytest.raises(ValueError, match="não informa 'serie'"):
        normalize_official_targets(
            without_series,
            geo_level="municipio",
            source_entity="meta_alfabetizacao_municipio",
            source_manifest="s3://data-bucket/manifest.json",
        )

    conflicting_source = source.unionByName(
        source.withColumn("meta_alfabetizacao_2024", source["meta_alfabetizacao_2024"] + 1)
    )
    with pytest.raises(ValueError, match="valores conflitantes"):
        normalize_official_targets(
            conflicting_source,
            geo_level="municipio",
            source_entity="meta_alfabetizacao_municipio",
            source_manifest="s3://data-bucket/manifest.json",
        )


@pytest.mark.parametrize(
    ("rate", "target", "expected_status"),
    [
        (None, None, "Sem Resultado e Sem Meta"),
        (None, 80.0, "Sem Resultado Calculado"),
        (70.0, None, "Sem Meta Cadastrada"),
        (75.0, 80.0, "Não Atingiu a Meta"),
    ],
)
def test_comparison_status_distinguishes_missing_result_and_target(
    spark, rate, target, expected_status
):
    indicators = spark.createDataFrame(
        [("1", 2024, "3", "2", rate)],
        schema=(
            "id_municipio string, ano int, rede string, serie string, "
            "taxa_alfabetizacao_recalculada double"
        ),
    )
    targets = spark.createDataFrame(
        [
            (
                "1",
                "2",
                2024,
                "3",
                target,
                "municipio",
                "meta_alfabetizacao_municipio",
                "s3://data-bucket/manifest.json",
                2024,
            )
        ],
        schema=(
            "id_municipio string, serie string, target_year int, "
            "target_rede string, target_percent double, geo_level string, "
            "source_entity string, source_manifest string, source_ano int"
        ),
    )

    comparison = compare_to_official_target(indicators, targets, "3")

    assert comparison.first()["status_meta_anual"] == expected_status


def test_gold_skip_requires_the_current_transformation_version():
    assert should_skip_gold_processing(
        "SKIPPED (Already Up-to-Date)",
        True,
        GOLD_TRANSFORM_VERSION,
    )
    assert not should_skip_gold_processing(
        "SKIPPED (Already Up-to-Date)",
        True,
        "previous-version",
    )
    assert not should_skip_gold_processing(
        "SUCCEEDED",
        True,
        GOLD_TRANSFORM_VERSION,
    )
    assert not should_skip_gold_processing(
        "SKIPPED (Already Up-to-Date)",
        False,
        GOLD_TRANSFORM_VERSION,
    )
