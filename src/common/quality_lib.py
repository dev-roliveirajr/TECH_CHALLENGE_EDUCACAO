"""
===============================================================================
BIBLIOTECA DE REGRAS DE QUALIDADE E GOVERNANÇA (scripts/common/quality_lib.py)
===============================================================================
Descrição: Módulo reutilizável para PySpark contendo validações de qualidade de
dados (Taxa de Join, verificação de nulos, inconsistências e travas bloqueantes).
===============================================================================
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def check_join_ratio(
    df_left: DataFrame, 
    df_joined: DataFrame, 
    threshold_percent: float = 95.0,
    raise_on_failure: bool = True
) -> float:
    """
    Calcula a Taxa de Correspondência de JOIN e valida contra um limiar mínimo.
    
    Args:
        df_left (DataFrame): DataFrame base antes do JOIN (ex: Tabela de Alunos).
        df_joined (DataFrame): DataFrame resultante após o JOIN com as demais entidades.
        threshold_percent (float): Limiar percentual mínimo aceitável (ex: 95.0%).
        raise_on_failure (bool): Se True, lança exceção para interromper a pipeline.
        
    Returns:
        float: Taxa percentual de correspondência calculada.
    """
    total_left = df_left.count()
    total_joined = df_joined.count()
    
    if total_left == 0:
        print("⚠️ ALERTA DE QUALIDADE: DataFrame base possui 0 linhas.")
        return 0.0
        
    ratio = (total_joined / total_left) * 100.0
    print(f"🔍 [QUALIDADE DE JOIN] Linhas Origem: {total_left} | Linhas Pós-Join: {total_joined}")
    print(f"📊 [QUALIDADE DE JOIN] Taxa de Correspondência: {ratio:.2f}% (Limiar Mínimo: {threshold_percent}%)")
    
    if ratio < threshold_percent:
        msg = f"❌ [FALHA BLOQUEANTE] Taxa de Join ({ratio:.2f}%) ficou abaixo do limiar estipulado ({threshold_percent}%)."
        print(msg)
        if raise_on_failure:
            raise ValueError(msg)
            
    return ratio

def check_null_threshold(
    df: DataFrame, 
    columns: list, 
    max_null_percent: float = 5.0
):
    """
    Valida se o percentual de valores nulos em colunas críticas está dentro do limite.
    """
    total_rows = df.count()
    if total_rows == 0:
        return
        
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_percent = (null_count / total_rows) * 100.0
        print(f"🔍 [QUALIDADE NULOS] Coluna '{col}': {null_percent:.2f}% nulos")
        if null_percent > max_null_percent:
            raise ValueError(f"❌ [FALHA BLOQUEANTE] Coluna '{col}' excede limite de nulos ({null_percent:.2f}% > {max_null_percent}%).")
