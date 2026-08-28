# Rodando os scripts silver/gold como Dataproc Serverless Batches

Os notebooks em `notebooks/silver/` e `notebooks/gold/` foram convertidos
para scripts Python equivalentes (mesmo nome, extensão `.py`), organizados
no pacote `src/`. Os `.ipynb` originais continuam no repositório (pasta
`notebooks/`) como referência e para execução interativa no Workbench — os
`.py` em `src/` são o que roda em produção.

## Estrutura

```
src/
  common/            # config, bootstrap de Spark, leitura/escrita BigQuery, utilitários
  silver/            # um script por tabela silver
  gold/              # um script por tabela gold
  run_pipeline.py    # orquestrador: roda todos os passos em uma única SparkSession
notebooks/
  silver/            # notebooks .ipynb originais (referência/execução interativa)
  gold/
orchestration/       # reservado para configuração futura de agendamento/orquestração
```

Cada script silver/gold expõe:
- `run(spark)`: lógica de negócio, recebe uma SparkSession já criada.
- `main()`: cria a SparkSession, chama `run(spark)`, encerra a sessão — usado
  quando o script roda como seu próprio Batch.

## Diferença importante em relação aos notebooks

Os notebooks fixam o conector Spark-BigQuery dentro do próprio
`SparkSession.builder`:

```python
.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2")
```

Em Dataproc Serverless isso é responsabilidade da submissão do Batch, não do
código. `src/common/spark_session.py` cria a sessão **sem** esse config por
padrão. Para rodar um script fora de um Batch (ex.: teste local com
`spark-submit`), defina `SPARK_INCLUDE_BQ_CONNECTOR_JAR=true` para reproduzir
o comportamento do notebook.

## Configuração via variáveis de ambiente

Todas com default idêntico ao valor hoje hardcoded nos notebooks — nada muda
se você não configurar nada:

| Variável | Default |
|---|---|
| `GCP_PROJECT_ID` | `tech-challenge-fase-2-505123` |
| `BRONZE_DATASET` | `bronze` |
| `SILVER_DATASET` | `silver` |
| `GOLD_DATASET` | `gold` |
| `BQ_CONNECTOR_PACKAGE` | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2` |
| `EXTERNAL_MUNICIPIO_TABLE` | `basedosdados.br_bd_diretorios_brasil.municipio` |

Use `SILVER_DATASET=silver_test` / `GOLD_DATASET=gold_test` para validar um
script contra um dataset de teste, sem sobrescrever as tabelas reais.

## Empacotando `src/` para `--py-files`

Os scripts fazem `from src.common import ...`, então o pacote `src/` inteiro
(com todos os `__init__.py`) precisa estar disponível no `sys.path` do Batch:

```bash
# a partir da raiz do repositório
zip -r src.zip src -x "*__pycache__*"
```

No Windows (PowerShell), a partir da raiz do repositório:

```powershell
Compress-Archive -Path src -DestinationPath src.zip -Force
```

Depois, faça upload do `src.zip` (e do script principal) para um bucket do
GCS acessível pelo Dataproc, ou aponte `--py-files`/o próprio comando
`gcloud dataproc batches submit pyspark` para o caminho local — o `gcloud`
cuida do upload automaticamente.

## Submetendo um Batch

**Uma tabela isolada** (granularidade de Job por tabela, útil para agendar
cada atualização separadamente):

```bash
gcloud dataproc batches submit pyspark src/silver/silver_alunos.py \
  --project=tech-challenge-fase-2-505123 \
  --region=<region> \
  --batch=silver-alunos-$(date +%Y%m%d-%H%M%S) \
  --py-files=src.zip \
  --properties=spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2
```

**Pipeline completo** (um único Batch, respeitando a ordem de dependência
silver -> gold):

```bash
gcloud dataproc batches submit pyspark src/run_pipeline.py \
  --project=tech-challenge-fase-2-505123 \
  --region=<region> \
  --batch=pipeline-full-$(date +%Y%m%d-%H%M%S) \
  --py-files=src.zip \
  --properties=spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2
```

**Subconjunto do pipeline** (ex.: só a camada gold, numa execução separada da
silver):

```bash
gcloud dataproc batches submit pyspark src/run_pipeline.py \
  --project=tech-challenge-fase-2-505123 \
  --region=<region> \
  --batch=pipeline-gold-$(date +%Y%m%d-%H%M%S) \
  --py-files=src.zip \
  --properties=spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.2 \
  -- --only gold
```

Substitua `<region>` pela região do projeto (ex.: `us-central1`) e confirme
a versão de runtime do Dataproc Serverless (`--version`) compatível com a
versão de PySpark usada — ver `requirements.txt`.

## Verificação antes de apontar para produção

Não há ambiente Spark/BigQuery disponível para testes automatizados fora do
GCP. Para cada script convertido, valide na instância Workbench (ou num
Batch de teste) contra um dataset de teste (`SILVER_DATASET=silver_test`,
`GOLD_DATASET=gold_test`):

1. Rode o notebook original em `notebooks/` (produz a tabela de produção hoje).
2. Rode o script novo em `src/` apontando para o dataset de teste.
3. Compare contagem de linhas, schema (`bq show --schema`) e um diff de
   dados (`EXCEPT DISTINCT` entre as duas tabelas no BigQuery).
4. Para os scripts gold com joins (`gold_dim_uf`, `gold_fato_meta`,
   `gold_fato_resultados`), confira também uma amostra de linhas manualmente.
5. Só depois de todos os scripts baterem com seus notebooks, teste
   `run_pipeline.py` de ponta a ponta contra o dataset de teste, e só então
   aponte para os datasets reais `silver`/`gold`.

## Mudança de nome de tabela nesta conversão

`gold_fato_aluno.py` passa a escrever em `gold.fato_aluno` (antes:
`gold.fato_indicador_aluno`, no notebook original). Atualize dashboards ou
consultas existentes que referenciem o nome antigo.
