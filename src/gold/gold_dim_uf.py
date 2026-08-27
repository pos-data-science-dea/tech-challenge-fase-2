"""Constrói gold.dim_uf.

Conversão de `gold_dim_uf.ipynb`. UFs distintas de `silver.uf`, enriquecidas
com nome da UF e região a partir do diretório público do IBGE.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_uf = config.fq(config.SILVER_DATASET, "uf")
    fq_gold_uf = config.fq(config.GOLD_DATASET, "dim_uf")

    df_scr_uf = bigquery_io.read_table(spark, fq_silver_uf)
    dir_mun = bigquery_io.read_table(spark, config.EXTERNAL_MUNICIPIO_TABLE)

    universo_uf = df_scr_uf.select("sigla_uf").distinct()

    dim_uf = universo_uf.join(
        dir_mun.select("sigla_uf", "nome_uf", "nome_regiao").distinct(),
        on="sigla_uf",
        how="left",
    )

    bigquery_io.write_table(dim_uf, fq_gold_uf)
    return dim_uf


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_dim_uf")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
