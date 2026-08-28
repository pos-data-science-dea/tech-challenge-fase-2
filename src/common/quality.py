"""Helpers genéricos para os relatórios de qualidade da camada silver.

Cada script silver mantém sua lógica de negócio específica (domínios válidos,
faixas aceitas, etc.) — aqui só ficam os padrões idênticos entre notebooks:
validação de schema, contagem de duplicados por chave e deduplicação com
timestamp de auditoria.
"""
from typing import Iterable, List

from pyspark.sql import Column, DataFrame, functions as F


class QualityCheckError(ValueError):
    """Erro que interrompe a promoção quando um gate de qualidade falha."""


def validate_schema(df: DataFrame, expected_columns: Iterable[str]) -> None:
    missing = sorted(set(expected_columns) - set(df.columns))
    if missing:
        raise QualityCheckError(
            f"Schema inválido. Colunas ausentes na Bronze: {missing}"
        )


def count_duplicates(df: DataFrame, keys: List[str]) -> int:
    return df.groupBy(*keys).count().filter(F.col("count") > 1).count()


def deduplicate_with_timestamp(df: DataFrame, keys: List[str]) -> DataFrame:
    return df.dropDuplicates(keys).withColumn(
        "_silver_timestamp", F.current_timestamp()
    )


def normalized_text(column: str) -> Column:
    """Trim + cast para string, convertendo string vazia em NULL."""
    valor = F.trim(F.col(column).cast("string"))
    return F.when(valor == "", F.lit(None)).otherwise(valor)
