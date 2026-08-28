"""Promove bronze.municipio para silver.municipio.

Conversão de `silver_municipio.ipynb`. Cria uma tabela completa da
alfabetização por municípios. As validações de qualidade abaixo são
informativas — não há gate que interrompa a promoção neste notebook.
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, quality, spark_session
from src.common.rede import build_rede_map_column

COLUNAS_SELECIONADAS = [
    "ano", "id_municipio", "serie", "rede", "taxa_alfabetizacao", "media_portugues",
    "proporcao_aluno_nivel_0", "proporcao_aluno_nivel_1", "proporcao_aluno_nivel_2",
    "proporcao_aluno_nivel_3", "proporcao_aluno_nivel_4", "proporcao_aluno_nivel_5",
    "proporcao_aluno_nivel_6", "proporcao_aluno_nivel_7", "proporcao_aluno_nivel_8",
]

CHAVE = ["ano", "id_municipio", "serie", "rede_id"]


def run(spark: SparkSession) -> DataFrame:
    fq_bronze_municipio = config.fq(config.BRONZE_DATASET, "municipio")
    fq_silver_municipio = config.fq(config.SILVER_DATASET, "municipio")

    df_scr_municipios = bigquery_io.read_table(spark, fq_bronze_municipio)

    df_municipios = df_scr_municipios.select(*COLUNAS_SELECIONADAS)

    map_rede = build_rede_map_column()

    df_silver_municipios = (
        df_municipios
        # chave: código IBGE como string de 7 dígitos com zero à esquerda
        .withColumn("id_municipio", F.lpad(F.trim(F.col("id_municipio")), 7, "0"))
        # tipos e padronização
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("serie", F.trim(F.col("serie")).cast("int"))
        # rede: preserva o código original e adiciona o rótulo decodificado
        .withColumnRenamed("rede", "rede_id")
        .withColumn("rede_id", F.trim(F.col("rede_id")))
        .withColumn("rede", map_rede[F.col("rede_id")])
        # auditoria da camada
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

    # deduplicação pela chave natural
    df_silver_municipios = df_silver_municipios.dropDuplicates(CHAVE)

    _relatorio_qualidade(df_scr_municipios, df_silver_municipios)

    bigquery_io.write_table(
        df_silver_municipios, fq_silver_municipio, clustered_fields=["ano", "rede_id"]
    )
    return df_silver_municipios


def _relatorio_qualidade(df_scr_municipios: DataFrame, df_silver_municipios: DataFrame) -> None:
    print("=== Relatório de Qualidade — df_silver_municipios.municipio ===")

    qtd_bronze = df_scr_municipios.count()
    chave_bronze = ["ano", "id_municipio", "serie", "rede"]
    dups = (
        df_scr_municipios.withColumn("id_municipio", F.lpad(F.trim("id_municipio"), 7, "0"))
        .groupBy(chave_bronze).count().filter("count > 1").count()
    )
    print(f"Duplicatas na chave (antes do dedup): {dups}")

    inval_id = df_silver_municipios.filter(~F.col("id_municipio").rlike("^[0-9]{7}$")).count()
    print(f"id_municipio fora do padrão de 7 dígitos: {inval_id}")

    rede_nao_map = df_silver_municipios.filter(F.col("rede").isNull()).select("rede_id").distinct()
    print("Códigos de rede NÃO mapeados (corrigir REDE_MAP):")
    rede_nao_map.show()

    fora_faixa = df_silver_municipios.filter(
        (F.col("taxa_alfabetizacao") < 0) | (F.col("taxa_alfabetizacao") > 100)
    ).count()
    print(f"taxa_alfabetizacao fora de [0,100]: {fora_faixa}")

    for coluna in ["ano", "id_municipio", "rede", "taxa_alfabetizacao", "media_portugues"]:
        quantidade = df_silver_municipios.filter(F.col(coluna).isNull()).count()
        print(f"Nulos em coluna crítica '{coluna}': {quantidade}")

    print(f"Linhas Bronze: {qtd_bronze}  ->  Linhas Silver (pós-dedup): {df_silver_municipios.count()}")


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="bronze_to_silver_municipio")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
