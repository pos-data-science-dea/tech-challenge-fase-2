"""Bootstrap de SparkSession compartilhado por todos os scripts silver/gold.

Em Dataproc Serverless (Batches) o conector Spark-BigQuery é injetado na
submissão do job (--properties spark.jars.packages=... ou --jars), não dentro
do código — por isso o builder aqui NÃO fixa `spark.jars.packages` por padrão.
Para rodar um script localmente/fora de um Batch (ex.: `spark-submit` manual
ou um teste na própria instância Workbench), defina a variável de ambiente
`SPARK_INCLUDE_BQ_CONNECTOR_JAR=true` para reproduzir o comportamento dos
notebooks originais, que sempre declaravam o jar no próprio builder.
"""
import os

from pyspark.sql import SparkSession

from . import config


def bootstrap_spark_session(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if os.environ.get("SPARK_INCLUDE_BQ_CONNECTOR_JAR", "false").lower() == "true":
        builder = builder.config("spark.jars.packages", config.BQ_CONNECTOR_PACKAGE)

    spark = builder.getOrCreate()

    # Projeto usado para faturamento das consultas ao BigQuery.
    spark.conf.set("parentProject", config.PROJECT_ID)

    return spark
