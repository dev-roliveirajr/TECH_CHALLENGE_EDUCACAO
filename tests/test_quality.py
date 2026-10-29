"""
===============================================================================
SUÍTE DE TESTES AUTOMATIZADOS - QUALIDADE DE DADOS (tests/test_quality.py)
===============================================================================
Descrição: Testes unitários com pytest para validar o comportamento do módulo
quality_lib.py (check_join_ratio, check_null_threshold, check_uniqueness
e deduplicate_by_window).
===============================================================================
"""

import os
import shutil
from pathlib import Path

import pytest
from unittest.mock import MagicMock

# Tentativa de importação de módulos PySpark (com fallback gracioso para mocks)
try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


# =============================================================================
# FIXTURE DO SPARK (Execução Local quando PySpark está instalado)
# =============================================================================
@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão local leve do Spark para testes unitários em memória."""
    if not PYSPARK_AVAILABLE:
        pytest.skip(
            "PySpark não está instalado no ambiente local. Ignorando testes com Spark local."
        )

    java_home = os.environ.get("JAVA_HOME")
    java_from_home = Path(java_home, "bin", "java") if java_home else None
    if not shutil.which("java") and not (
        java_from_home
        and java_from_home.is_file()
        and os.access(java_from_home, os.X_OK)
    ):
        pytest.skip(
            "Java não está instalado ou configurado. Ignorando testes com Spark local."
        )

    spark = (
        SparkSession.builder.appName("TestesUnitarios-QualityLib")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# =============================================================================
# 1. TESTES PARA check_join_ratio
# =============================================================================
def test_check_join_ratio_success():
    """Valida se o gate de JOIN aprova quando a taxa de correspondência é 100%."""
    from common.quality_lib import check_join_ratio

    df_left = MagicMock()
    df_joined = MagicMock()
    df_left.count.return_value = 1000
    df_joined.count.return_value = 1000

    ratio, metrics = check_join_ratio(
        df_left=df_left,
        df_joined=df_joined,
        threshold_percent=95.0,
        raise_on_failure=False,
    )

    assert ratio == 100.0
    assert metrics["total_left"] == 1000
    assert metrics["total_matched"] == 1000
    assert metrics["match_ratio"] == 100.0


def test_check_join_ratio_failure_raises_exception():
    """Valida se o gate de JOIN lança exceção ValueError quando fica abaixo do limiar."""
    from common.quality_lib import check_join_ratio

    df_left = MagicMock()
    df_joined = MagicMock()
    df_left.count.return_value = 1000
    df_joined.count.return_value = 800  # 80% (abaixo dos 95% exigidos)

    with pytest.raises(ValueError) as exc_info:
        check_join_ratio(
            df_left=df_left,
            df_joined=df_joined,
            threshold_percent=95.0,
            raise_on_failure=True,
        )

    assert "FALHA BLOQUEANTE DE QUALIDADE" in str(exc_info.value)
    assert "80.00%" in str(exc_info.value)


def test_check_join_ratio_zero_left():
    """Tratamento defensivo quando a tabela de origem está vazia (0 registros)."""
    from common.quality_lib import check_join_ratio

    df_left = MagicMock()
    df_joined = MagicMock()
    df_left.count.return_value = 0

    ratio, metrics = check_join_ratio(
        df_left=df_left,
        df_joined=df_joined,
        threshold_percent=95.0,
        raise_on_failure=False,
    )

    assert ratio == 0.0
    assert metrics["total_left"] == 0


# =============================================================================
# 2. TESTES PARA check_null_threshold
# =============================================================================
def test_check_null_threshold_success(spark_session):
    """Verifica se colunas com nulos dentro do limite tolerado são aprovadas."""
    from common.quality_lib import check_null_threshold

    data = [("1302504", "AM"), ("1302603", "AM"), ("1300631", "AM")]
    df = spark_session.createDataFrame(data, ["id_municipio", "sigla_uf"])

    metrics = check_null_threshold(
        df, columns=["id_municipio", "sigla_uf"], max_null_percent=5.0
    )

    assert metrics["id_municipio"] == 0.0
    assert metrics["sigla_uf"] == 0.0


def test_check_null_threshold_exceeded_raises_exception(spark_session):
    """Verifica se lança exceção quando uma coluna excede o limite tolerado de NULOs."""
    from common.quality_lib import check_null_threshold

    # 2 de 4 registros com NULO em id_municipio (50% nulos > 5% limite)
    data = [(None, "AM"), (None, "AM"), ("1302504", "AM"), ("1302603", "AM")]
    df = spark_session.createDataFrame(data, ["id_municipio", "sigla_uf"])

    with pytest.raises(ValueError) as exc_info:
        check_null_threshold(
            df, columns=["id_municipio"], max_null_percent=5.0, raise_on_failure=True
        )

    assert "FALHA BLOQUEANTE" in str(exc_info.value)
    assert "50.00%" in str(exc_info.value)


# =============================================================================
# 3. TESTES PARA check_uniqueness
# =============================================================================
def test_check_uniqueness_success(spark_session):
    """Valida aprovação quando não há linhas duplicadas nas chaves primárias."""
    from common.quality_lib import check_uniqueness

    data = [(2023, "1302504", "ALUNO_1"), (2023, "1302504", "ALUNO_2")]
    df = spark_session.createDataFrame(data, ["ano", "id_municipio", "id_aluno"])

    dup_count, dup_pct = check_uniqueness(
        df, key_columns=["ano", "id_municipio", "id_aluno"]
    )

    assert dup_count == 0
    assert dup_pct == 0.0


def test_check_uniqueness_failure_raises_exception(spark_session):
    """Valida se lança exceção quando detecta duplicidade de chaves primárias."""
    from common.quality_lib import check_uniqueness

    # Registro duplicado para ALUNO_1
    data = [
        (2023, "1302504", "ALUNO_1"),
        (2023, "1302504", "ALUNO_1"),  # DUPLICADO
        (2023, "1302504", "ALUNO_2"),
    ]
    df = spark_session.createDataFrame(data, ["ano", "id_municipio", "id_aluno"])

    with pytest.raises(ValueError) as exc_info:
        check_uniqueness(
            df, key_columns=["ano", "id_municipio", "id_aluno"], raise_on_failure=True
        )

    assert "Encontradas 1 linhas duplicadas" in str(exc_info.value)


# =============================================================================
# 4. TESTES PARA deduplicate_by_window (DEDUPLICAÇÃO ATIVA)
# =============================================================================
def test_deduplicate_by_window_active_deduplication(spark_session):
    """
    Testa se a deduplicação ativa por Janela (Window Function) escolhe o registro correto:
    - Mesma chave composta: (ano=2023, id_municipio="1302504", id_aluno="13015851", rede="3", serie="2")
    - Registro 1: _ingested_at antigo + proficiência NULL
    - Registro 2: _ingested_at recente + proficiência = 750.5 (DEVE SER MANTIDO)
    """
    from common.quality_lib import deduplicate_by_window

    data = [
        # Aluno 1 (Registro Antigo com NOTA NULL)
        (2023, "1302504", "13015851", "3", "2", None, "2026-10-28T10:00:00"),
        # Aluno 1 (Registro Recente com NOTA Preenchida - ESCOLHIDO)
        (2023, "1302504", "13015851", "3", "2", 750.5, "2026-10-28T12:00:00"),
        # Aluno 2 (Único)
        (2023, "1302603", "13030738", "3", "2", 680.0, "2026-10-28T10:00:00"),
    ]

    schema = [
        "ano",
        "id_municipio",
        "id_aluno",
        "rede",
        "serie",
        "proficiencia",
        "_ingested_at",
    ]
    df_mock = spark_session.createDataFrame(data, schema)

    # Act
    df_dedup = deduplicate_by_window(
        df=df_mock,
        key_columns=["id_aluno", "id_municipio", "ano", "rede", "serie"],
        order_by_cols=["_ingested_at", "proficiencia"],
        ascending=False,
    )

    # Assert
    assert df_dedup.count() == 2  # De 3 linhas originais, sobram 2 únicas

    # Valida se o registro mantido do Aluno 1 possui a nota 750.5 e o timestamp mais recente
    aluno_1_row = df_dedup.filter("id_aluno == '13015851'").collect()[0]
    assert aluno_1_row["proficiencia"] == 750.5
    assert aluno_1_row["_ingested_at"] == "2026-10-28T12:00:00"
