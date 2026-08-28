"""Orquestrador do pipeline silver -> gold.

Cria uma única SparkSession compartilhada e chama `run(spark)` de cada
módulo silver/gold, respeitando a ordem de dependência entre as tabelas gold.
Pode rodar o pipeline inteiro como um único Dataproc Serverless Batch, ou um
subconjunto via `--only`.

Uso:
    python -m src.run_pipeline                # pipeline completo
    python -m src.run_pipeline --only silver   # só a camada silver
    python -m src.run_pipeline --only gold     # só a camada gold
    python -m src.run_pipeline --only silver_uf gold_dim_uf   # módulos específicos
"""
import argparse

from src.common import spark_session
from src.gold import (
    gold_dim_municipio,
    gold_dim_rede,
    gold_dim_tempo,
    gold_dim_uf,
    gold_fato_aluno,
    gold_fato_indicador_municipio,
    gold_fato_indicador_uf,
    gold_fato_meta,
    gold_fato_resultados,
)
from src.silver import (
    silver_alunos,
    silver_meta_alfabetizacao_brasil,
    silver_meta_alfabetizacao_municipio,
    silver_meta_alfabetizacao_uf,
    silver_municipio,
    silver_uf,
)

# Silver: sem interdependência entre si, qualquer ordem serve.
SILVER_STEPS = {
    "silver_alunos": silver_alunos,
    "silver_municipio": silver_municipio,
    "silver_uf": silver_uf,
    "silver_meta_alfabetizacao_brasil": silver_meta_alfabetizacao_brasil,
    "silver_meta_alfabetizacao_municipio": silver_meta_alfabetizacao_municipio,
    "silver_meta_alfabetizacao_uf": silver_meta_alfabetizacao_uf,
}

# Gold: ordem importa — dimensões antes dos fatos independentes, fato_meta
# depende de dim_rede, fato_resultados depende de fato_meta + fato_indicador_municipio
# + dim_municipio + dim_rede (é sempre o último passo).
GOLD_STEPS = {
    "gold_dim_tempo": gold_dim_tempo,
    "gold_dim_uf": gold_dim_uf,
    "gold_dim_municipio": gold_dim_municipio,
    "gold_dim_rede": gold_dim_rede,
    "gold_fato_aluno": gold_fato_aluno,
    "gold_fato_indicador_municipio": gold_fato_indicador_municipio,
    "gold_fato_indicador_uf": gold_fato_indicador_uf,
    "gold_fato_meta": gold_fato_meta,
    "gold_fato_resultados": gold_fato_resultados,
}

ALL_STEPS = {**SILVER_STEPS, **GOLD_STEPS}


def _resolve_steps(only: list) -> dict:
    if not only:
        return ALL_STEPS
    resolved = {}
    for nome in only:
        if nome == "silver":
            resolved.update(SILVER_STEPS)
        elif nome == "gold":
            resolved.update(GOLD_STEPS)
        elif nome in ALL_STEPS:
            resolved[nome] = ALL_STEPS[nome]
        else:
            raise SystemExit(f"Módulo desconhecido em --only: {nome!r}")
    # preserva a ordem de dependência definida em ALL_STEPS, não a ordem do --only
    return {nome: modulo for nome, modulo in ALL_STEPS.items() if nome in resolved}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        nargs="+",
        default=None,
        help="Executa só 'silver', 'gold', e/ou nomes de módulos específicos "
        "(ex.: silver_uf gold_dim_uf). Sem essa flag, roda o pipeline inteiro.",
    )
    args = parser.parse_args()

    steps = _resolve_steps(args.only)

    spark = spark_session.bootstrap_spark_session(app_name="pipeline_bronze_to_gold")
    try:
        for nome, modulo in steps.items():
            print(f"=== Executando {nome} ===")
            modulo.run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
