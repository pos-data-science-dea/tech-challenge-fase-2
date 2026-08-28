"""Constrói gold.fato_indicador_uf.

Conversão de `gold_fato_indicador_uf.ipynb`. Projeção direta de silver.uf.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_uf = config.fq(config.SILVER_DATASET, "uf")
    fq_gold_fato_indicador_uf = config.fq(config.GOLD_DATASET, "fato_indicador_uf")

    df_scr_uf = bigquery_io.read_table(spark, fq_silver_uf)

    fato_ind_uf = df_scr_uf.select(
        "ano", "sigla_uf", "rede_id", "serie",
        "taxa_alfabetizacao", "media_portugues",
        *[f"proporcao_aluno_nivel_{i}" for i in range(9)],
    )

    bigquery_io.write_table(fato_ind_uf, fq_gold_fato_indicador_uf)
    return fato_ind_uf


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_fato_indicador_uf")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
