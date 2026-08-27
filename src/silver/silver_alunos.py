"""Promove bronze.aluno para silver.aluno.

Conversão de `silver_alunos.ipynb`. Padroniza chaves e categorias, decodifica
indicadores booleanos e preserva rastreabilidade da ingestão. As checagens de
domínio categórico e de indicadores booleanos interrompem a promoção
(`raise` no notebook original) quando encontram valores inesperados.
"""
from pyspark.sql import Column, DataFrame, SparkSession, functions as F

from src.common import bigquery_io, config, quality, spark_session
from src.common.quality import QualityCheckError
from src.common.rede import build_rede_map_column

COLUNAS_ESPERADAS = [
    "ano", "id_municipio", "id_escola", "id_aluno", "caderno",
    "serie", "rede", "presenca", "preenchimento_caderno",
    "alfabetizado", "proficiencia", "peso_aluno",
    "_ingestao_timestamp", "_fonte",
]
COLUNAS_CATEGORICAS = [
    "caderno", "serie", "rede", "presenca",
    "preenchimento_caderno", "alfabetizado",
]
COLUNAS_INDICADORAS = ["presenca", "preenchimento_caderno", "alfabetizado"]

CADERNOS_VALIDOS = [str(valor) for valor in range(1, 22)] + ["43"]
SERIES_VALIDAS = [2]
REDES_VALIDAS = ["2", "3", "4"]

CHAVE = ["ano", "id_aluno"]


def _inteiro_para_booleano(coluna: str) -> Column:
    return (
        F.when(F.col(coluna) == 1, F.lit(True))
        .when(F.col(coluna) == 0, F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def run(spark: SparkSession) -> DataFrame:
    fq_bronze_aluno = config.fq(config.BRONZE_DATASET, "aluno")
    fq_silver_aluno = config.fq(config.SILVER_DATASET, "aluno")

    df_src_alunos = bigquery_io.read_table(spark, fq_bronze_aluno)

    quality.validate_schema(df_src_alunos, COLUNAS_ESPERADAS)
    df_alunos = df_src_alunos.select(*COLUNAS_ESPERADAS)

    # A Bronze pode representar os indicadores como STRING ou INTEGER. O
    # contrato valida o domínio, não o tipo físico recebido do conector.
    expressoes_invalidas = []
    for coluna in COLUNAS_INDICADORAS:
        valor = F.trim(F.col(coluna).cast("string"))
        expressoes_invalidas.append(
            F.sum(
                F.when(F.col(coluna).isNotNull() & ~valor.isin("0", "1"), 1).otherwise(0)
            ).alias(coluna)
        )
    invalidos_por_indicador = df_alunos.agg(*expressoes_invalidas).first().asDict()
    invalidos_por_indicador = {
        coluna: quantidade
        for coluna, quantidade in invalidos_por_indicador.items()
        if quantidade > 0
    }
    if invalidos_por_indicador:
        raise QualityCheckError(
            f"Indicadores fora do domínio booleano {{0, 1}}: {invalidos_por_indicador}"
        )

    id_municipio_limpo = quality.normalized_text("id_municipio")
    map_rede = build_rede_map_column()

    df_silver_alunos = (
        df_alunos
        .withColumn("ano", F.col("ano").cast("int"))
        # Código IBGE: completa com zero apenas quando o conteúdo já é numérico.
        .withColumn(
            "id_municipio",
            F.when(
                id_municipio_limpo.rlike("^[0-9]{1,7}$"),
                F.lpad(id_municipio_limpo, 7, "0"),
            ).otherwise(id_municipio_limpo),
        )
        # Escola e aluno são identificadores opacos: não converter para
        # número nem preencher zeros.
        .withColumn("id_escola", quality.normalized_text("id_escola"))
        .withColumn("id_aluno", quality.normalized_text("id_aluno"))
        .withColumn("caderno", F.upper(quality.normalized_text("caderno")))
        .withColumn("serie", quality.normalized_text("serie").cast("int"))
        .withColumnRenamed("rede", "rede_id")
        .withColumn("rede_id", F.upper(quality.normalized_text("rede_id")))
        .withColumn("rede", map_rede[F.col("rede_id")])
        .withColumnRenamed("presenca", "presenca_id")
        .withColumn("presenca_id", F.col("presenca_id").cast("int"))
        .withColumn("presenca", _inteiro_para_booleano("presenca_id"))
        .withColumnRenamed("preenchimento_caderno", "preenchimento_caderno_id")
        .withColumn("preenchimento_caderno_id", F.col("preenchimento_caderno_id").cast("int"))
        .withColumn("preenchimento_caderno", _inteiro_para_booleano("preenchimento_caderno_id"))
        .withColumnRenamed("alfabetizado", "alfabetizado_id")
        .withColumn("alfabetizado_id", F.col("alfabetizado_id").cast("int"))
        .withColumn("alfabetizado", _inteiro_para_booleano("alfabetizado_id"))
        .withColumn("proficiencia", F.col("proficiencia").cast("double"))
        .withColumn("peso_aluno", F.col("peso_aluno").cast("double"))
        .withColumn("_ingestao_timestamp", F.col("_ingestao_timestamp").cast("timestamp"))
        .withColumn("_fonte", quality.normalized_text("_fonte"))
    )

    # Chave natural observada: um aluno por ano de aplicação da avaliação.
    # Não há regra de versionamento por _ingestao_timestamp nesta promoção.
    df_silver_alunos_antes_dedup = df_silver_alunos
    df_silver_alunos = quality.deduplicate_with_timestamp(df_silver_alunos, CHAVE)

    _relatorio_qualidade(df_src_alunos, df_silver_alunos, df_silver_alunos_antes_dedup)

    bigquery_io.write_table(
        df_silver_alunos,
        fq_silver_aluno,
        clustered_fields=["ano", "id_municipio", "id_escola", "rede_id"],
    )
    return df_silver_alunos


def _relatorio_qualidade(
    df_src_alunos: DataFrame,
    df_silver_alunos: DataFrame,
    df_silver_alunos_antes_dedup: DataFrame,
) -> None:
    print("=== Relatório de Qualidade — silver.aluno ===")

    qtd_bronze = df_src_alunos.count()
    qtd_silver = df_silver_alunos.count()

    dups_antes = quality.count_duplicates(df_silver_alunos_antes_dedup, CHAVE)
    dups_silver = quality.count_duplicates(df_silver_alunos, CHAVE)
    print(f"Chaves duplicadas antes da deduplicação: {dups_antes}")
    print(f"Chaves duplicadas após deduplicação: {dups_silver}")

    id_municipio_invalido = df_silver_alunos.filter(
        F.col("id_municipio").isNotNull()
        & ~F.col("id_municipio").rlike("^[0-9]{7}$")
    ).count()
    print(f"id_municipio fora do padrão de 7 dígitos: {id_municipio_invalido}")

    id_escola_invalido = df_silver_alunos.filter(
        F.col("id_escola").isNotNull()
        & ~F.col("id_escola").rlike("^[0-9]{8}$")
    ).count()
    print(f"id_escola mascarado fora do formato observado de 8 dígitos: {id_escola_invalido}")

    colunas_criticas = list(dict.fromkeys(
        CHAVE + [
            "serie", "rede_id", "presenca_id", "preenchimento_caderno_id",
            "alfabetizado_id", "_ingestao_timestamp", "_fonte",
        ]
    ))
    for coluna in colunas_criticas:
        quantidade = df_silver_alunos.filter(F.col(coluna).isNull()).count()
        print(f"Nulos em '{coluna}': {quantidade}")

    dominios_invalidos = {
        "caderno": df_silver_alunos_antes_dedup.filter(
            F.col("caderno").isNotNull() & ~F.col("caderno").isin(*CADERNOS_VALIDOS)
        ).count(),
        "serie": df_silver_alunos_antes_dedup.filter(
            F.col("serie").isNotNull() & ~F.col("serie").isin(*SERIES_VALIDAS)
        ).count(),
        "rede_id": df_silver_alunos_antes_dedup.filter(
            F.col("rede_id").isNotNull() & ~F.col("rede_id").isin(*REDES_VALIDAS)
        ).count(),
    }
    for coluna, quantidade in dominios_invalidos.items():
        print(f"Valores fora do domínio esperado em {coluna}: {quantidade}")

    if any(quantidade > 0 for quantidade in dominios_invalidos.values()):
        raise QualityCheckError(
            f"Promoção interrompida: domínios categóricos inesperados: {dominios_invalidos}"
        )

    qtd_caderno_43 = df_silver_alunos_antes_dedup.filter(F.col("caderno") == "43").count()
    print(f"Registros com o código raro caderno=43: {qtd_caderno_43}")

    print("Códigos de rede não mapeados:")
    (
        df_silver_alunos
        .filter(F.col("rede_id").isNotNull() & F.col("rede").isNull())
        .groupBy("rede_id").count().orderBy(F.desc("count"))
        .show(100, truncate=False)
    )

    indicadores_fora_dominio_total = 0
    for coluna_id, coluna_descricao in [
        ("presenca_id", "presenca"),
        ("preenchimento_caderno_id", "preenchimento_caderno"),
        ("alfabetizado_id", "alfabetizado"),
    ]:
        nao_mapeados = (
            df_silver_alunos
            .filter(F.col(coluna_id).isNotNull() & F.col(coluna_descricao).isNull())
            .count()
        )
        indicadores_fora_dominio_total += nao_mapeados
        print(f"Valores fora de {{0, 1}} em {coluna_id}: {nao_mapeados}")

    if indicadores_fora_dominio_total > 0:
        raise QualityCheckError(
            "Promoção interrompida: indicadores booleanos contêm valores fora de {0, 1}."
        )

    proficiencia_invalida = df_silver_alunos.filter(
        F.isnan("proficiencia") | (F.col("proficiencia") < 0)
    ).count()
    peso_invalido = df_silver_alunos.filter(
        F.isnan("peso_aluno") | (F.col("peso_aluno") <= 0)
    ).count()
    print(f"proficiencia negativa ou NaN: {proficiencia_invalida}")
    print(f"peso_aluno não positivo ou NaN: {peso_invalido}")

    metricas_ausentes_com_caderno_preenchido = df_silver_alunos.filter(
        (F.col("preenchimento_caderno_id") == 1)
        & (F.col("proficiencia").isNull() | F.col("peso_aluno").isNull())
    ).count()
    print(
        "Linhas com caderno preenchido e proficiencia/peso ausente: "
        f"{metricas_ausentes_com_caderno_preenchido}"
    )

    inconsistencia_presenca = df_silver_alunos.filter(
        (F.col("presenca_id") == 0)
        & (F.col("preenchimento_caderno_id") == 1)
    ).count()
    inconsistencia_alfabetizado = df_silver_alunos.filter(
        (F.col("alfabetizado_id") == 1)
        & (F.col("preenchimento_caderno_id") != 1)
    ).count()
    print(f"Ausente com caderno preenchido: {inconsistencia_presenca}")
    print(f"Alfabetizado sem caderno preenchido: {inconsistencia_alfabetizado}")

    escolas_multiplos_municipios = (
        df_silver_alunos
        .filter(F.col("id_escola").isNotNull())
        .groupBy("ano", "id_escola")
        .agg(F.countDistinct("id_municipio").alias("qtd_municipios"))
        .filter(F.col("qtd_municipios") > 1)
        .count()
    )
    print(f"Escolas associadas a mais de um município no mesmo ano: {escolas_multiplos_municipios}")
    print(f"Linhas Bronze: {qtd_bronze} -> Linhas Silver: {qtd_silver}")


def main() -> None:
    spark = spark_session.bootstrap_spark_session(app_name="bronze_to_silver_aluno")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
