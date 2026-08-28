"""Mapeamento compartilhado de código de rede -> rótulo descritivo.

Usado por silver_alunos, silver_uf e silver_municipio (verbatim, hoje
duplicado idêntico nos três notebooks).
"""
from pyspark.sql import Column, functions as F

REDE_MAP = {
    "0": "Total (Federal, Estadual, Municipal e Privada)",
    "1": "Federal",
    "2": "Estadual",
    "3": "Municipal",
    "4": "Privada",
    "5": "Pública (Estadual e Municipal)",
    "6": "Pública (Federal, Estadual e Municipal)",
}


def build_rede_map_column() -> Column:
    """Retorna a expressão `map(...)` do Spark para decodificar `rede_id`."""
    return F.create_map([F.lit(x) for item in REDE_MAP.items() for x in item])


# Variantes (incluindo mojibake) do rótulo textual "Pública" observadas nas
# tabelas bronze de meta de alfabetização (brasil/uf/municipio) — idêntico
# nos três notebooks correspondentes.
VARIANTES_REDE_PUBLICA = ["pública", "publica", "p�blica", "pãºblica"]


def normalize_rede_publica_label(column: Column) -> Column:
    """Normaliza o rótulo textual de rede, unificando variantes de "Pública"."""
    rede_limpa = F.trim(column.cast("string"))
    rede_minuscula = F.lower(rede_limpa)
    return (
        F.when(rede_minuscula.isin(*VARIANTES_REDE_PUBLICA), F.lit("Pública"))
        .when(rede_limpa == "", F.lit(None))
        .otherwise(F.initcap(rede_minuscula))
    )
