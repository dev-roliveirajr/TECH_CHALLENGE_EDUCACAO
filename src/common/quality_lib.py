"""
===============================================================================
BIBLIOTECA DE REGRAS DE QUALIDADE, CURADORIA E GOVERNANÇA (src/common/quality_lib.py)
===============================================================================
Descrição: Módulo reutilizável para PySpark contendo travas bloqueantes de
qualidade de dados (Taxa de Join, verificação de nulos, unicidade de chaves,
deduplicação determinística por janela e validação cruzada para Gold).
===============================================================================
"""

from typing import List, Dict, Optional, Tuple
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


def check_join_ratio(
    df_left: DataFrame,
    df_joined: DataFrame,
    match_key_col: Optional[str] = None,
    threshold_percent: float = 95.0,
    raise_on_failure: bool = True,
) -> Tuple[float, Dict[str, float]]:
    """Calcula e valida a Taxa de Correspondência (Match Ratio) de JOIN."""
    total_left = df_left.count()
    if total_left == 0:
        print("⚠️ [QUALIDADE DE JOIN] Alerta: DataFrame de origem possui 0 registros.")
        return 0.0, {"total_left": 0, "total_matched": 0, "match_ratio": 0.0}

    if match_key_col and match_key_col in df_joined.columns:
        total_matched = df_joined.filter(F.col(match_key_col).isNotNull()).count()
    else:
        total_matched = df_joined.count()

    match_ratio = (total_matched / float(total_left)) * 100.0

    print("=" * 70)
    print("🔍 [GATE DE QUALIDADE - TAXA DE MATCH DO JOIN]")
    print(f"   ├─ Registros na Origem (Fato/Esquerda): {total_left:,}")
    print(f"   ├─ Registros Combinados com Sucesso   : {total_matched:,}")
    print(f"   ├─ Taxa de Correspondência Medida     : {match_ratio:.2f}%")
    print(f"   └─ Limiar Mínimo Requerido (Gate)     : {threshold_percent:.2f}%")
    print("=" * 70)

    metrics = {
        "total_left": total_left,
        "total_matched": total_matched,
        "match_ratio": round(match_ratio, 2),
        "threshold_percent": threshold_percent,
    }

    if match_ratio < threshold_percent:
        msg = (
            f"❌ [FALHA BLOQUEANTE DE QUALIDADE] Taxa de correspondência de JOIN ({match_ratio:.2f}%) "
            f"ficou abaixo do limiar aceitável ({threshold_percent:.2f}%)."
        )
        print(msg)
        if raise_on_failure:
            raise ValueError(msg)
    else:
        print(
            f"✅ [GATE APROVADO] Taxa de JOIN dentro do padrão estipulado ({match_ratio:.2f}% >= {threshold_percent:.2f}%)."
        )

    return match_ratio, metrics


def check_null_threshold(
    df: DataFrame,
    columns: List[str],
    max_null_percent: float = 5.0,
    raise_on_failure: bool = True,
) -> Dict[str, float]:
    """Valida se o percentual de NULOs está dentro do limite tolerado."""
    total_rows = df.count()
    null_metrics = {}
    if total_rows == 0:
        return null_metrics

    print("🔍 [QUALIDADE - NULOS EM COLUNAS CHAVE]")
    failed_cols = []

    for col in columns:
        if col not in df.columns:
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        null_percent = (null_count / float(total_rows)) * 100.0
        null_metrics[col] = round(null_percent, 2)

        status = "✅ OK" if null_percent <= max_null_percent else "❌ EXCEDIDO"
        print(
            f"   ├─ Coluna '{col}': {null_percent:.2f}% nulos ({null_count:,} linhas) -> {status}"
        )

        if null_percent > max_null_percent:
            failed_cols.append((col, null_percent))

    if failed_cols and raise_on_failure:
        msg = f"❌ [FALHA BLOQUEANTE] Colunas excederam o limite máximo de NULOs ({max_null_percent}%): {failed_cols}."
        raise ValueError(msg)

    return null_metrics


def check_uniqueness(
    df: DataFrame, key_columns: List[str], raise_on_failure: bool = True
) -> Tuple[int, float]:
    """Verifica a unicidade das chaves do dataset."""
    total_rows = df.count()
    if total_rows == 0:
        return 0, 0.0

    distinct_keys = df.select(key_columns).distinct().count()
    duplicate_rows = total_rows - distinct_keys
    duplicate_percent = (duplicate_rows / float(total_rows)) * 100.0

    print("🔍 [QUALIDADE - UNICIDADE DE REGISTROS]")
    print(f"   ├─ Total de Linhas: {total_rows:,} | Chaves Únicas: {distinct_keys:,}")
    print(f"   └─ Linhas Duplicadas: {duplicate_rows:,} ({duplicate_percent:.2f}%)")

    if duplicate_rows > 0 and raise_on_failure:
        msg = f"❌ [FALHA BLOQUEANTE] Encontradas {duplicate_rows:,} linhas duplicadas nas chaves: {key_columns}."
        raise ValueError(msg)

    return duplicate_rows, round(duplicate_percent, 2)


def deduplicate_by_window(
    df: DataFrame,
    key_columns: List[str],
    order_by_cols: Optional[List[str]] = None,
    ascending: bool = False,
) -> DataFrame:
    """
    Deduplica um DataFrame com base em chave primária composta utilizando Window Function (row_number).
    Garante escolha determinística por prioridade (ex: recência de ingestão, notas não-nulas).
    """
    window_spec = Window.partitionBy(key_columns)

    if order_by_cols:
        order_exprs = []
        for c in order_by_cols:
            if c in df.columns:
                col_expr = (
                    F.col(c).asc_nulls_last()
                    if ascending
                    else F.col(c).desc_nulls_last()
                )
                order_exprs.append(col_expr)
        if order_exprs:
            window_spec = window_spec.orderBy(*order_exprs)

    df_dedup = (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return df_dedup


def check_cross_validation(
    df_recalculated: DataFrame,
    calculated_col: str,
    published_col: str,
    max_divergence_percent: float = 2.0,
    raise_on_failure: bool = False,
) -> Tuple[float, Dict[str, float]]:
    """
    Validação Cruzada: Compara a taxa agregada recalculada via microdados de alunos com peso amostral
    contra a taxa oficial publicada pela dimensão territorial (Inep/Base dos Dados).
    """
    df_valid = df_recalculated.filter(
        F.col(calculated_col).isNotNull() & F.col(published_col).isNotNull()
    )
    total_records = df_valid.count()

    if total_records == 0:
        print("⚠️ [VALIDAÇÃO CRUZADA] Sem registros pareados para comparação.")
        return 0.0, {"total_records": 0, "avg_divergence": 0.0}

    # Diferença absoluta entre taxa calculada pelo aluno e taxa publicada pelo município
    df_diff = df_valid.withColumn(
        "_diff_abs", F.abs(F.col(calculated_col) - F.col(published_col))
    )

    rows = df_diff.select(F.avg("_diff_abs")).collect()
    avg_divergence = float(rows[0][0]) if rows and rows[0][0] is not None else 0.0

    print("=" * 70)
    print("🔬 [VALIDAÇÃO CRUZADA - MICRODADOS vs. TAXA PUBLICADA]")
    print(f"   ├─ Registros Comparados           : {total_records:,}")
    print(f"   ├─ Divergência Média Absoluta     : {avg_divergence:.3f}%")
    print(f"   └─ Tolerância Máxima Permitida    : {max_divergence_percent:.2f}%")
    print("=" * 70)

    metrics = {
        "total_records": total_records,
        "avg_divergence": round(avg_divergence, 3),
        "tolerance": max_divergence_percent,
    }

    if avg_divergence > max_divergence_percent:
        msg = f"⚠️ [ALERTA DE DIVERGÊNCIA] Divergência média ({avg_divergence:.3f}%) excedeu a tolerância ({max_divergence_percent}%)."
        print(msg)
        if raise_on_failure:
            raise ValueError(msg)
    else:
        print(
            f"✅ [VALIDAÇÃO CRUZADA APROVADA] Alinhamento dentro da margem de tolerância ({avg_divergence:.3f}% <= {max_divergence_percent}%)."
        )

    return avg_divergence, metrics
