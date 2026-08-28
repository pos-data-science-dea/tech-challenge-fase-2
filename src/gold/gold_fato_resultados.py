"""Constrói gold.fato_resultados.

Conversão de `gold_fato_resultados.ipynb`. Cruza gold.fato_indicador_municipio
com gold.dim_municipio, gold.dim_rede e a meta mais recente de cada chave em
gold.fato_meta (via Window), calculando o gap para a meta do ano corrente e
para a meta de 2030.
"""
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_gold_fato_meta = config.fq(config.GOLD_DATASET, "fato_meta")
    fq_gold_fato_indicador_municipio = config.fq(config.GOLD_DATASET, "fato_indicador_municipio")
    fq_gold_dim_municipio = config.fq(config.GOLD_DATASET, "dim_municipio")
    fq_gold_dim_rede = config.fq(config.GOLD_DATASET, "dim_rede")
    fq_gold_fato_resultados = config.fq(config.GOLD_DATASET, "fato_resultados")

    df_scr_meta = bigquery_io.read_table(spark, fq_gold_fato_meta)
    df_scr_indicador_municipio = bigquery_io.read_table(spark, fq_gold_fato_indicador_municipio)
    df_scr_municipio = bigquery_io.read_table(spark, fq_gold_dim_municipio)
    df_scr_rede = bigquery_io.read_table(spark, fq_gold_dim_rede)

    w = (
        Window.partitionBy("nivel_geografico", "local_id", "rede_id", "ano_meta")
        .orderBy(F.col("ano_referencia").desc())
    )

    meta_muni = (
        df_scr_meta
        .filter(F.col("nivel_geografico") == "municipio")
        .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    )

    meta_ano = meta_muni.select(
        F.col("local_id").alias("id_municipio"), "rede_id",
        F.col("ano_meta").alias("ano"),
        F.col("valor_meta").alias("meta_ano"),
    )

    meta_2030 = (
        meta_muni
        .filter(F.col("ano_meta") == 2030)
        .select(
            F.col("local_id").alias("id_municipio"),
            "rede_id",
            F.col("valor_meta").alias("meta_2030"),
        )
    )

    fato_resultados = (
        df_scr_indicador_municipio
        .join(df_scr_municipio, "id_municipio", "left")
        .join(df_scr_rede, "rede_id", "left")
        .join(meta_ano, ["id_municipio", "rede_id", "ano"], "left")
        .join(meta_2030, ["id_municipio", "rede_id"], "left")
        .withColumn("gap_meta_ano", F.round(F.col("taxa_alfabetizacao") - F.col("meta_ano"), 2))
        .withColumn("atingiu_meta_ano", F.col("taxa_alfabetizacao") >= F.col("meta_ano"))
        .withColumn("gap_ate_2030", F.round(F.col("meta_2030") - F.col("taxa_alfabetizacao"), 2))
        .select(
            "ano", "id_municipio", "nome_municipio", "sigla_uf", "nome_uf", "nome_regiao",
            "rede_id", "rede",
            "taxa_alfabetizacao", "media_portugues",
            "meta_ano", "gap_meta_ano", "atingiu_meta_ano",
            "meta_2030", "gap_ate_2030",
        )
    )

    bigquery_io.write_table(fato_resultados, fq_gold_fato_resultados)
    return fato_resultados


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_fato_resultados")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
