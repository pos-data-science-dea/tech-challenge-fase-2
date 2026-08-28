"""Constrói gold.fato_meta.

Conversão de `gold_fato_meta.ipynb`. Despivota (`stack`) as colunas anuais
`meta_alfabetizacao_20XX` das três tabelas silver de meta (brasil/uf/
município) para um formato longo, e junta com gold.dim_rede pelo rótulo
`rede`.
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, spark_session

ANOS_META = [str(ano) for ano in range(2024, 2031)]
_STACK_EXPR = ", ".join(f"'{ano}', meta_alfabetizacao_{ano}" for ano in ANOS_META)


def _unpivot(df: DataFrame, nivel: str, local_col: Optional[str]) -> DataFrame:
    local_expr = f"CAST({local_col} AS STRING)" if local_col else "CAST(NULL AS STRING)"
    return (
        df.selectExpr(
            f"'{nivel}' AS nivel_geografico",
            f"{local_expr} AS local_id",
            "rede",
            "ano AS ano_referencia",
            f"stack({len(ANOS_META)}, {_STACK_EXPR}) AS (ano_meta, valor_meta)",
        )
        .withColumn("ano_meta", F.col("ano_meta").cast("int"))
        .filter(F.col("valor_meta").isNotNull())
    )


def run(spark: SparkSession) -> DataFrame:
    fq_silver_meta_municipio = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_municipio")
    fq_silver_meta_uf = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_uf")
    fq_silver_meta_brasil = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_brasil")
    fq_gold_dim_rede = config.fq(config.GOLD_DATASET, "dim_rede")
    fq_gold_fato_meta = config.fq(config.GOLD_DATASET, "fato_meta")

    df_scr_alfabetizacao_municipio = bigquery_io.read_table(spark, fq_silver_meta_municipio)
    df_scr_alfabetizacao_uf = bigquery_io.read_table(spark, fq_silver_meta_uf)
    df_scr_alfabetizacao_brasil = bigquery_io.read_table(spark, fq_silver_meta_brasil)
    df_scr_rede = bigquery_io.read_table(spark, fq_gold_dim_rede)

    fato_meta = (
        _unpivot(df_scr_alfabetizacao_brasil, "brasil", None)
        .unionByName(_unpivot(df_scr_alfabetizacao_uf, "uf", "sigla_uf"))
        .unionByName(_unpivot(df_scr_alfabetizacao_municipio, "municipio", "id_municipio"))
        .join(df_scr_rede, on="rede", how="left")
    )

    bigquery_io.write_table(fato_meta, fq_gold_fato_meta)
    return fato_meta


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_fato_meta")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
