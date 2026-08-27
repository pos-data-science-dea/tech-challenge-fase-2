"""Promove bronze.meta_alfabetizacao_municipio para silver.meta_alfabetizacao_municipio.

Conversão de `silver_meta_alfabetizacao_municipio.ipynb`. Uma linha por `ano`,
`id_municipio` e `rede`. As validações de qualidade são informativas — apenas
a ausência de colunas obrigatórias interrompe a execução.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, quality, spark_session
from src.common.rede import normalize_rede_publica_label

ANOS_META = range(2024, 2031)
COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in ANOS_META]
COLUNAS_PERCENTUAIS = ["taxa_alfabetizacao", *COLUNAS_META, "percentual_participacao"]
COLUNAS_ESPERADAS = [
    "ano", "id_municipio", "rede", "taxa_alfabetizacao",
    *COLUNAS_META, "nivel_alfabetizacao", "percentual_participacao",
    "_ingestao_timestamp", "_fonte",
]

REDES_CONHECIDAS = ["Federal", "Estadual", "Municipal", "Privada", "Pública"]
CHAVE = ["ano", "id_municipio", "rede"]


def run(spark: SparkSession) -> DataFrame:
    fq_bronze_meta = config.fq(config.BRONZE_DATASET, "meta_alfabetizacao_municipio")
    fq_silver_meta = config.fq(config.SILVER_DATASET, "meta_alfabetizacao_municipio")

    df_src_meta = bigquery_io.read_table(spark, fq_bronze_meta)

    quality.validate_schema(df_src_meta, COLUNAS_ESPERADAS)
    df_meta = df_src_meta.select(*COLUNAS_ESPERADAS)

    id_municipio_limpo = F.trim(F.col("id_municipio").cast("string"))

    df_silver_meta = (
        df_meta
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn(
            "id_municipio",
            F.when(
                id_municipio_limpo.rlike("^[0-9]{1,7}$"),
                F.lpad(id_municipio_limpo, 7, "0"),
            ).otherwise(id_municipio_limpo),
        )
        .withColumn("rede", normalize_rede_publica_label(F.col("rede")))
        .withColumn("nivel_alfabetizacao", F.col("nivel_alfabetizacao").cast("int"))
        .withColumn("_ingestao_timestamp", F.col("_ingestao_timestamp").cast("timestamp"))
        .withColumn("_fonte", F.trim(F.col("_fonte")))
    )
    for coluna in COLUNAS_PERCENTUAIS:
        df_silver_meta = df_silver_meta.withColumn(coluna, F.col(coluna).cast("double"))

    df_silver_meta_antes_dedup = df_silver_meta
    df_silver_meta = quality.deduplicate_with_timestamp(df_silver_meta, CHAVE)

    _relatorio_qualidade(df_src_meta, df_silver_meta, df_silver_meta_antes_dedup)

    bigquery_io.write_table(
        df_silver_meta, fq_silver_meta, clustered_fields=["ano", "id_municipio", "rede"]
    )
    return df_silver_meta


def _relatorio_qualidade(
    df_src_meta: DataFrame, df_silver_meta: DataFrame, df_silver_meta_antes_dedup: DataFrame
) -> None:
    print("=== Relatório de Qualidade — silver.meta_alfabetizacao_municipio ===")

    qtd_bronze = df_src_meta.count()
    qtd_silver = df_silver_meta.count()

    dups_antes = quality.count_duplicates(df_silver_meta_antes_dedup, CHAVE)
    dups_depois = quality.count_duplicates(df_silver_meta, CHAVE)
    print(f"Chaves duplicadas antes da deduplicação: {dups_antes}")
    print(f"Chaves duplicadas após deduplicação: {dups_depois}")

    id_municipio_invalido = df_silver_meta.filter(
        F.col("id_municipio").isNotNull()
        & ~F.col("id_municipio").rlike("^[0-9]{7}$")
    ).count()
    nivel_invalido = df_silver_meta.filter(
        F.col("nivel_alfabetizacao").isNotNull()
        & ~F.col("nivel_alfabetizacao").isin(0, 1, 2, 3, 4, 5)
    ).count()
    print(f"id_municipio fora do padrão de 7 dígitos: {id_municipio_invalido}")
    print(f"nivel_alfabetizacao fora de [0, 5]: {nivel_invalido}")

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

    # Regra de negócio especificada pelo grupo,
    # pode ser flexibilizado a depender de definições de negócio diferentes
    meta_nao_monotona = df_silver_meta.filter(
        (F.col("meta_alfabetizacao_2025") < F.col("meta_alfabetizacao_2024"))
        | (F.col("meta_alfabetizacao_2026") < F.col("meta_alfabetizacao_2025"))
        | (F.col("meta_alfabetizacao_2027") < F.col("meta_alfabetizacao_2026"))
        | (F.col("meta_alfabetizacao_2028") < F.col("meta_alfabetizacao_2027"))
        | (F.col("meta_alfabetizacao_2029") < F.col("meta_alfabetizacao_2028"))
        | (F.col("meta_alfabetizacao_2030") < F.col("meta_alfabetizacao_2029"))
    ).count()
    print(f"Linhas com metas decrescentes: {meta_nao_monotona}")

    qtd_campos_resultado_nulos = (
        F.when(F.col("taxa_alfabetizacao").isNull(), 1).otherwise(0)
        + F.when(F.col("nivel_alfabetizacao").isNull(), 1).otherwise(0)
        + F.when(F.col("percentual_participacao").isNull(), 1).otherwise(0)
    )
    resultados_parcialmente_nulos = df_silver_meta.filter(
        (qtd_campos_resultado_nulos > 0) & (qtd_campos_resultado_nulos < 3)
    ).count()
    print(
        "Linhas com nulidade inconsistente entre taxa, nível e participação: "
        f"{resultados_parcialmente_nulos}"
    )

    print("Nulos das metas por ano de referência:")
    (
        df_silver_meta.groupBy("ano").agg(
            *[
                F.sum(F.when(F.col(coluna).isNull(), 1).otherwise(0)).alias(f"nulos_{coluna}")
                for coluna in COLUNAS_META
            ]
        ).orderBy("ano").show(truncate=False)
    )

    print(f"Linhas Bronze: {qtd_bronze} -> Linhas Silver: {qtd_silver}")


def main() -> None:
    spark = spark_session.bootstrap_spark_session(
        app_name="bronze_to_silver_meta_alfabetizacao_municipio"
    )
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
