"""Constrói gold.dim_rede.

Conversão de `gold_dim_rede.ipynb`. União dos pares (`rede_id`, `rede`)
observados em silver.municipio, silver.aluno e silver.uf.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_municipio = config.fq(config.SILVER_DATASET, "municipio")
    fq_silver_uf = config.fq(config.SILVER_DATASET, "uf")
    fq_silver_aluno = config.fq(config.SILVER_DATASET, "aluno")
    fq_gold_dim_rede = config.fq(config.GOLD_DATASET, "dim_rede")

    df_scr_municipio = bigquery_io.read_table(spark, fq_silver_municipio)
    df_scr_uf = bigquery_io.read_table(spark, fq_silver_uf)
    df_scr_aluno = bigquery_io.read_table(spark, fq_silver_aluno)

    dim_rede = (
        df_scr_municipio.select("rede_id", "rede")
        .union(df_scr_aluno.select("rede_id", "rede"))
        .union(df_scr_uf.select("rede_id", "rede"))
        .distinct()
        .filter(F.col("rede_id").isNotNull())
    )

    bigquery_io.write_table(dim_rede, fq_gold_dim_rede)
    return dim_rede


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_dim_rede")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
