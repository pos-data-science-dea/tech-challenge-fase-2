"""Constrói gold.fato_indicador_municipio.

Conversão de `gold_fato_indicador_municipio.ipynb`. Projeção direta de
silver.municipio.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_municipio = config.fq(config.SILVER_DATASET, "municipio")
    fq_gold_fato_indicador_municipio = config.fq(config.GOLD_DATASET, "fato_indicador_municipio")

    df_scr_municipio = bigquery_io.read_table(spark, fq_silver_municipio)

    fato_ind_mun = df_scr_municipio.select(
        "ano", "id_municipio", "rede_id", "serie",
        "taxa_alfabetizacao", "media_portugues",
        *[f"proporcao_aluno_nivel_{i}" for i in range(9)],
    )

    bigquery_io.write_table(fato_ind_mun, fq_gold_fato_indicador_municipio)
    return fato_ind_mun


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_fato_indicador_municipio")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
