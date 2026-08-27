"""Constrói gold.dim_municipio.

Conversão de `gold_dim_municipio.ipynb`. União dos `id_municipio` observados
em silver.municipio, silver.aluno e silver.meta_alfabetizacao_municipio,
enriquecidos com nome/UF/região a partir do diretório público do IBGE.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_municipio = config.fq(config.SILVER_DATASET, "municipio")
    fq_silver_aluno = config.fq(config.SILVER_DATASET, "aluno")
    fq_silver_meta_municipio = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_municipio")
    fq_gold_dim_municipio = config.fq(config.GOLD_DATASET, "dim_municipio")

    df_scr_municipio = bigquery_io.read_table(spark, fq_silver_municipio)
    df_scr_alfabetizacao_municipio = bigquery_io.read_table(spark, fq_silver_meta_municipio)
    df_scr_aluno = bigquery_io.read_table(spark, fq_silver_aluno)
    dir_mun = bigquery_io.read_table(spark, config.EXTERNAL_MUNICIPIO_TABLE)

    universo_mun = (
        df_scr_municipio.select("id_municipio")
        .union(df_scr_aluno.select("id_municipio"))
        .union(df_scr_alfabetizacao_municipio.select("id_municipio"))
        .distinct()
    )

    dim_municipio = (
        universo_mun
        .join(
            dir_mun.select("id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao"),
            on="id_municipio",
            how="left",
        )
        .withColumnRenamed("nome", "nome_municipio")
    )

    bigquery_io.write_table(dim_municipio, fq_gold_dim_municipio)
    return dim_municipio


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_dim_municipio")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
