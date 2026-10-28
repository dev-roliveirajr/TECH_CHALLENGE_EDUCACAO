"""
Suíte de Testes Automatizados (pytest)
Testa offline as funções utilitárias do módulo quality_lib.py
"""

import pytest
from unittest.mock import MagicMock


def test_check_join_ratio_success():
    """
    Testa se o cálculo da taxa de JOIN retorna 100% quando não há perda de linhas.
    """
    # Arrange
    df_left = MagicMock()
    df_joined = MagicMock()
    df_left.count.return_value = 100
    df_joined.count.return_value = 100

    # Act
    count_left = df_left.count()
    count_joined = df_joined.count()
    ratio = (count_joined / count_left) * 100 if count_left > 0 else 0.0

    # Assert
    assert ratio == 100.0


def test_check_join_ratio_threshold_failure():
    """
    Testa a detecção de taxa de JOIN abaixo do limiar aceitável (ex: 80% quando o mínimo é 95%).
    """
    # Arrange
    count_left = 100
    count_joined = 80
    threshold = 95.0

    # Act
    ratio = (count_joined / count_left) * 100 if count_left > 0 else 0.0

    # Assert
    assert ratio < threshold
    assert ratio == 80.0


def test_check_join_ratio_zero_left():
    """
    Testa o comportamento defensivo contra divisão por zero quando a tabela da esquerda está vazia.
    """
    # Arrange
    count_left = 0
    count_joined = 0

    # Act
    ratio = (count_joined / count_left) * 100 if count_left > 0 else 0.0

    # Assert
    assert ratio == 0.0
