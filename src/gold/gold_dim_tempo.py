"""Constrói gold.dim_tempo.

Conversão de `gold_dim_tempo.ipynb`. Dimensão sintética (sem leitura do
BigQuery) com o horizonte de anos das metas de alfabetização.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_gold_tempo = config.fq(config.GOLD_DATASET, "dim_tempo")

    dim_tempo = (
        spark.range(2023, 2031).withColumnRenamed("id", "ano")
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn(
            "tipo_ano",
            F.when(F.col("ano") <= 2024, F.lit("realizado")).otherwise(F.lit("projetado")),
        )
    )

    bigquery_io.write_table(dim_tempo, fq_gold_tempo)
    return dim_tempo


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_dim_tempo")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
