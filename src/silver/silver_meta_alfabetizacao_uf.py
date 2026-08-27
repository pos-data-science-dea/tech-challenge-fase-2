"""Promove bronze.meta_alfabetizacao_uf para silver.meta_alfabetizacao_uf.

Conversão de `silver_meta_alfabetizacao_uf.ipynb`. Uma linha por `ano`,
`sigla_uf` e `rede`. As validações de qualidade são informativas — apenas a
ausência de colunas obrigatórias interrompe a execução.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, quality, spark_session
from src.common.rede import normalize_rede_publica_label

ANOS_META = range(2024, 2031)
COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in ANOS_META]
COLUNAS_PERCENTUAIS = ["taxa_alfabetizacao", *COLUNAS_META, "percentual_participacao"]
COLUNAS_ESPERADAS = [
    "ano", "sigla_uf", "rede", *COLUNAS_PERCENTUAIS, "_ingestao_timestamp", "_fonte",
]

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]
REDES_CONHECIDAS = ["Federal", "Estadual", "Municipal", "Privada", "Pública"]
CHAVE = ["ano", "sigla_uf", "rede"]


def run(spark: SparkSession) -> DataFrame:
    fq_bronze_meta = config.fq(config.BRONZE_DATASET, "meta_alfabetizacao_uf")
    fq_silver_meta = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_uf")

    df_src_meta = bigquery_io.read_table(spark, fq_bronze_meta)

    quality.validate_schema(df_src_meta, COLUNAS_ESPERADAS)
    df_meta = df_src_meta.select(*COLUNAS_ESPERADAS)

    df_silver_meta = (
        df_meta
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("rede", normalize_rede_publica_label(F.col("rede")))
        .withColumn("_ingestao_timestamp", F.col("_ingestao_timestamp").cast("timestamp"))
        .withColumn("_fonte", F.trim(F.col("_fonte")))
    )
    for coluna in COLUNAS_PERCENTUAIS:
        df_silver_meta = df_silver_meta.withColumn(coluna, F.col(coluna).cast("double"))

    df_silver_meta_antes_dedup = df_silver_meta
    df_silver_meta = quality.deduplicate_with_timestamp(df_silver_meta, CHAVE)

    _relatorio_qualidade(df_src_meta, df_silver_meta, df_silver_meta_antes_dedup)

    bigquery_io.write_table(
        df_silver_meta, fq_silver_meta, clustered_fields=["ano", "sigla_uf", "rede"]
    )
    return df_silver_meta


def _relatorio_qualidade(
    df_src_meta: DataFrame, df_silver_meta: DataFrame, df_silver_meta_antes_dedup: DataFrame
) -> None:
    print("=== Relatório de Qualidade — silver.meta_alfabetizacao_uf ===")

    qtd_bronze = df_src_meta.count()
    qtd_silver = df_silver_meta.count()

    dups_antes = quality.count_duplicates(df_silver_meta_antes_dedup, CHAVE)
    dups_depois = quality.count_duplicates(df_silver_meta, CHAVE)
    print(f"Chaves duplicadas antes da deduplicação: {dups_antes}")
    print(f"Chaves duplicadas após deduplicação: {dups_depois}")

    uf_invalida = df_silver_meta.filter(
        F.col("sigla_uf").isNotNull() & ~F.col("sigla_uf").isin(*UFS_VALIDAS)
    ).count()
    print(f"Siglas de UF inválidas: {uf_invalida}")

    print("Redes não reconhecidas:")
    (
        df_silver_meta.filter(F.col("rede").isNotNull() & ~F.col("rede").isin(*REDES_CONHECIDAS))
        .groupBy("rede").count().orderBy(F.desc("count"))
        .show(100, truncate=False)
    )

    for coluna in COLUNAS_ESPERADAS:
        quantidade = df_silver_meta.filter(F.col(coluna).isNull()).count()
        print(f"Nulos em '{coluna}': {quantidade}")

    for coluna in COLUNAS_PERCENTUAIS:
        quantidade = df_silver_meta.filter(
            F.col(coluna).isNotNull()
            & ((F.col(coluna) < 0) | (F.col(coluna) > 100) | F.isnan(coluna))
        ).count()
        print(f"Valores preenchidos fora de [0, 100] ou NaN em '{coluna}': {quantidade}")

    meta_nao_monotona = df_silver_meta.filter(
        (F.col("meta_alfabetizacao_2025") < F.col("meta_alfabetizacao_2024"))
        | (F.col("meta_alfabetizacao_2026") < F.col("meta_alfabetizacao_2025"))
        | (F.col("meta_alfabetizacao_2027") < F.col("meta_alfabetizacao_2026"))
        | (F.col("meta_alfabetizacao_2028") < F.col("meta_alfabetizacao_2027"))
        | (F.col("meta_alfabetizacao_2029") < F.col("meta_alfabetizacao_2028"))
        | (F.col("meta_alfabetizacao_2030") < F.col("meta_alfabetizacao_2029"))
    ).count()
    print(f"Linhas com metas decrescentes: {meta_nao_monotona}")

    nulidade_resultado_inconsistente = df_silver_meta.filter(
        F.col("taxa_alfabetizacao").isNull() != F.col("percentual_participacao").isNull()
    ).count()
    print(
        "Linhas com nulidade inconsistente entre taxa e participação: "
        f"{nulidade_resultado_inconsistente}"
    )

    qtd_metas_preenchidas = sum(
        F.when(F.col(coluna).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for coluna in COLUNAS_META
    )
    print("Quantidade de metas preenchidas por ano:")
    (
        df_silver_meta.withColumn("qtd_metas_preenchidas", qtd_metas_preenchidas)
        .groupBy("ano", "qtd_metas_preenchidas").count()
        .orderBy("ano", "qtd_metas_preenchidas")
        .show(100, truncate=False)
    )

    print("Quantidade de UFs distintas por ano:")
    (
        df_silver_meta.groupBy("ano")
        .agg(F.countDistinct("sigla_uf").alias("qtd_ufs"))
        .orderBy("ano").show()
    )

    print(f"Linhas Bronze: {qtd_bronze} -> Linhas Silver: {qtd_silver}")


def main() -> None:
    spark = spark_session.bootstrap_spark_session(
        app_name="bronze_to_silver_meta_alfabetizacao_uf"
    )
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
