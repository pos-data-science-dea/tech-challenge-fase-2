"""Constrói gold.fato_aluno.

Conversão de `gold_fato_aluno.ipynb`. Projeção direta de silver.aluno.

Nota: o notebook original escrevia em `gold.fato_indicador_aluno` (nome
inconsistente com o arquivo/tabela esperada). Esta conversão corrige o
destino para `gold.fato_aluno` — dashboards/consultas existentes que
referenciam `gold.fato_indicador_aluno` precisam ser atualizados.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common import bigquery_io, config, spark_session


def run(spark: SparkSession) -> DataFrame:
    fq_silver_aluno = config.fq(config.SILVER_DATASET, "aluno")
    fq_gold_fato_aluno = config.fq(config.GOLD_DATASET, "fato_aluno")

    df_scr_aluno = bigquery_io.read_table(spark, fq_silver_aluno)

    fato_aluno = df_scr_aluno.select(
        "ano", "id_municipio", "id_escola", "id_aluno", "caderno", "serie", "rede_id",
        "proficiencia", "peso_aluno",
        "presenca", "preenchimento_caderno", "alfabetizado",
        "presenca_id", "preenchimento_caderno_id", "alfabetizado_id",
    )

    bigquery_io.write_table(fato_aluno, fq_gold_fato_aluno)
    return fato_aluno


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="gold_fato_aluno")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
