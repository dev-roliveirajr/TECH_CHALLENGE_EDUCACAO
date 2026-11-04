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


def test_comparisons_match_official_network_and_80_percent_target(spark, student_rows):
    indicators = _indicators_by_level(spark, student_rows)
    students = spark.createDataFrame(student_rows)

    municipal = compare_to_official_target(indicators["municipio"], "3")
    uf = compare_to_official_target(
        aggregate_public_indicators(students, ["sigla_uf", "ano", "serie"]),
        "5",
    )
    brasil = compare_to_official_target(
        aggregate_public_indicators(students, ["ano", "serie"]),
        "5",
    )

    assert {row["rede"] for row in municipal.select("rede").distinct().collect()} == {
        "3"
    }
    assert {row["rede"] for row in uf.select("rede").distinct().collect()} == {"5"}
    assert {row["rede"] for row in brasil.select("rede").distinct().collect()} == {"5"}

    municipal_below_target = municipal.filter("id_municipio = '1'").first()
    assert municipal_below_target["meta_pactuada_2030"] == 80.0
    assert municipal_below_target["gap_para_meta"] == -55.0
    assert municipal_below_target["status_meta"] == "Não Atingiu a Meta"
    assert (
        uf.filter("sigla_uf = 'AA'").first()["taxa_alfabetizacao_recalculada"] == 57.14
    )
    assert brasil.first()["taxa_alfabetizacao_recalculada"] == 55.56


def test_feature_stores_keep_each_level_and_the_official_target(spark, student_rows):
    indicators = _indicators_by_level(spark, student_rows)
    students = spark.createDataFrame(student_rows)
    comparisons = [
        compare_to_official_target(indicators["municipio"], "3"),
        compare_to_official_target(
            aggregate_public_indicators(students, ["sigla_uf", "ano", "serie"]),
            "5",
        ),
        compare_to_official_target(
            aggregate_public_indicators(students, ["ano", "serie"]),
            "5",
        ),
    ]

    feature_stores = [build_feature_store(comparison) for comparison in comparisons]
    assert [feature_store.count() for feature_store in feature_stores] == [3, 2, 1]
    for feature_store in feature_stores:
        assert "rotulo_alvo" in feature_store.columns
        assert "status_meta" not in feature_store.columns
        assert feature_store.filter("meta_pactuada_2030 != 80.0").count() == 0


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
