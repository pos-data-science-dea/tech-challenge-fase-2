"""Helpers de leitura/escrita no BigQuery via conector Spark-BigQuery."""
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession


def read_table(spark: SparkSession, fq_table: str) -> DataFrame:
    return spark.read.format("bigquery").option("table", fq_table).load()


def write_table(
    df: DataFrame,
    fq_table: str,
    mode: str = "overwrite",
    clustered_fields: Optional[Iterable[str]] = None,
) -> None:
    writer = (
        df.write.format("bigquery")
        .option("table", fq_table)
        .option("writeMethod", "direct")
    )
    if clustered_fields:
        writer = writer.option("clusteredFields", ",".join(clustered_fields))
    writer.mode(mode).save()
