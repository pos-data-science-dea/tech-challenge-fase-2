"""Configuração de projeto/datasets, parametrizável via variáveis de ambiente.

Os defaults reproduzem exatamente os valores hoje hardcoded em cada notebook
(`par_source_project = "tech-challenge-fase-2-505123"` etc.), então nada muda
de comportamento se as variáveis de ambiente não forem definidas.
"""
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tech-challenge-fase-2-505123")

BRONZE_DATASET = os.environ.get("BRONZE_DATASET", "bronze")
SILVER_DATASET = os.environ.get("SILVER_DATASET", "silver")
GOLD_DATASET = os.environ.get("GOLD_DATASET", "gold")

# Coordenada Maven do conector Spark-BigQuery. Em Dataproc Serverless isso é
# passado na submissão do batch (--properties spark.jars.packages=...), não
# no código; mantido aqui como fonte única de verdade para documentação/scripts
# de submissão e para uso local/interativo (ver spark_session.py).
BQ_CONNECTOR_PACKAGE = os.environ.get(
    "BQ_CONNECTOR_PACKAGE",
    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2",
)

# Dataset público externo (Base dos Dados) usado pelas dimensões de município/UF.
EXTERNAL_MUNICIPIO_TABLE = os.environ.get(
    "EXTERNAL_MUNICIPIO_TABLE",
    "basedosdados.br_bd_diretorios_brasil.municipio",
)


def fq(dataset: str, table: str) -> str:
    """Monta o nome totalmente qualificado `projeto.dataset.tabela`."""
    return f"{PROJECT_ID}.{dataset}.{table}"
