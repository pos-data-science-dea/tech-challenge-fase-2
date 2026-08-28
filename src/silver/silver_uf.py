"""Promove bronze.uf para silver.uf.

Conversão de `silver_uf.ipynb`. Uma linha por `ano`, `sigla_uf`, `serie` e
`rede`. As validações de qualidade abaixo são informativas — apenas a
ausência de colunas obrigatórias (validate_schema) interrompe a execução.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, quality, spark_session
from src.common.rede import build_rede_map_column

COLUNAS_NIVEIS = [f"proporcao_aluno_nivel_{nivel}" for nivel in range(9)]
COLUNAS_MEDIDAS = ["taxa_alfabetizacao", "media_portugues", *COLUNAS_NIVEIS]
COLUNAS_ESPERADAS = [
    "ano", "sigla_uf", "serie", "rede", *COLUNAS_MEDIDAS,
    "_ingestao_timestamp", "_fonte",
]

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

CHAVE = ["ano", "sigla_uf", "serie", "rede_id"]


def run(spark: SparkSession) -> DataFrame:
    fq_bronze_uf = config.fq(config.BRONZE_DATASET, "uf")
    fq_silver_uf = config.fq(config.SILVER_DATASET, "uf")

    df_src_uf = bigquery_io.read_table(spark, fq_bronze_uf)

    quality.validate_schema(df_src_uf, COLUNAS_ESPERADAS)
    df_uf = df_src_uf.select(*COLUNAS_ESPERADAS)

    map_rede = build_rede_map_column()
    df_silver_uf = (
        df_uf
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("serie", F.trim(F.col("serie")).cast("int"))
        .withColumnRenamed("rede", "rede_id")
        .withColumn("rede_id", F.trim(F.col("rede_id").cast("string")))
        .withColumn("rede", map_rede[F.col("rede_id")])
        .withColumn("_ingestao_timestamp", F.col("_ingestao_timestamp").cast("timestamp"))
        .withColumn("_fonte", F.trim(F.col("_fonte")))
    )
    for coluna in COLUNAS_MEDIDAS:
        df_silver_uf = df_silver_uf.withColumn(coluna, F.col(coluna).cast("double"))

    df_silver_uf_antes_dedup = df_silver_uf
    df_silver_uf = quality.deduplicate_with_timestamp(df_silver_uf, CHAVE)

    _relatorio_qualidade(df_src_uf, df_silver_uf, df_silver_uf_antes_dedup)

    bigquery_io.write_table(
        df_silver_uf, fq_silver_uf, clustered_fields=["ano", "sigla_uf", "rede_id"]
    )
    return df_silver_uf


def _relatorio_qualidade(
    df_src_uf: DataFrame, df_silver_uf: DataFrame, df_silver_uf_antes_dedup: DataFrame
) -> None:
    print("=== Relatório de Qualidade — silver.uf ===")

    qtd_bronze = df_src_uf.count()
    qtd_silver = df_silver_uf.count()

    dups_antes = quality.count_duplicates(df_silver_uf_antes_dedup, CHAVE)
    dups_depois = quality.count_duplicates(df_silver_uf, CHAVE)
    print(f"Chaves duplicadas antes da deduplicação: {dups_antes}")
    print(f"Chaves duplicadas após deduplicação: {dups_depois}")

    uf_invalida = df_silver_uf.filter(
        F.col("sigla_uf").isNotNull() & ~F.col("sigla_uf").isin(*UFS_VALIDAS)
    ).count()
    serie_inesperada = df_silver_uf.filter(
        F.col("serie").isNotNull() & (F.col("serie") != 2)
    ).count()
    print(f"Siglas de UF inválidas: {uf_invalida}")
    print(f"Séries diferentes de 2: {serie_inesperada}")

    print("Códigos de rede não mapeados:")
    (
        df_silver_uf.filter(F.col("rede_id").isNotNull() & F.col("rede").isNull())
        .groupBy("rede_id").count().orderBy(F.desc("count"))
        .show(100, truncate=False)
    )

    for coluna in [
        "ano", "sigla_uf", "serie", "rede_id", "taxa_alfabetizacao",
        "media_portugues", "_ingestao_timestamp", "_fonte",
    ]:
        quantidade = df_silver_uf.filter(F.col(coluna).isNull()).count()
        print(f"Nulos em '{coluna}': {quantidade}")

    taxa_fora_faixa = df_silver_uf.filter(
        (F.col("taxa_alfabetizacao") < 0)
        | (F.col("taxa_alfabetizacao") > 100)
        | F.isnan("taxa_alfabetizacao")
    ).count()
    media_invalida = df_silver_uf.filter(
        (F.col("media_portugues") < 0) | F.isnan("media_portugues")
    ).count()
    print(f"taxa_alfabetizacao fora de [0, 100] ou NaN: {taxa_fora_faixa}")
    print(f"media_portugues negativa ou NaN: {media_invalida}")

    for coluna in COLUNAS_NIVEIS:
        quantidade = df_silver_uf.filter(
            F.col(coluna).isNotNull()
            & ((F.col(coluna) < 0) | (F.col(coluna) > 100) | F.isnan(coluna))
        ).count()
        print(f"{coluna} preenchida e fora de [0, 100] ou NaN: {quantidade}")

    qtd_niveis_preenchidos = sum(
        F.when(F.col(coluna).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for coluna in COLUNAS_NIVEIS
    )
    soma_niveis = sum(F.coalesce(F.col(coluna), F.lit(0.0)) for coluna in COLUNAS_NIVEIS)

    niveis_parcialmente_preenchidos = df_silver_uf.filter(
        (qtd_niveis_preenchidos > 0) & (qtd_niveis_preenchidos < 9)
    ).count()
    soma_niveis_invalida = df_silver_uf.filter(
        (qtd_niveis_preenchidos == 9)
        & ((soma_niveis < 99.9) | (soma_niveis > 100.1))
    ).count()
    print(f"Linhas com apenas parte dos nove níveis preenchida: {niveis_parcialmente_preenchidos}")
    print(f"Linhas completas cuja soma dos níveis está fora de [99.9, 100.1]: {soma_niveis_invalida}")

    print("Preenchimento dos níveis por ano:")
    (
        df_silver_uf.withColumn("qtd_niveis_preenchidos", qtd_niveis_preenchidos)
        .groupBy("ano", "qtd_niveis_preenchidos").count()
        .orderBy("ano", "qtd_niveis_preenchidos")
        .show(100, truncate=False)
    )

    print("Quantidade de UFs distintas por ano:")
    (
        df_silver_uf.groupBy("ano")
        .agg(F.countDistinct("sigla_uf").alias("qtd_ufs"))
        .orderBy("ano").show()
    )

    print(f"Linhas Bronze: {qtd_bronze} -> Linhas Silver: {qtd_silver}")


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="bronze_to_silver_uf")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
