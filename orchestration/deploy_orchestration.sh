#!/usr/bin/env bash
#
# Publica a orquestração do pipeline batch:
#   1) sobe o código (run_pipeline.py + src.zip) para o GCS
#   2) publica/atualiza o Cloud Workflow
#   3) cria o Cloud Scheduler (execução diária)
#
# Rodar da RAIZ do repo, no terminal do Workbench.
#
set -euo pipefail

PROJECT="${GCP_PROJECT_ID:-tech-challenge-fase-2-505123}"
REGION="${REGION:-us-central1}"
BUCKET="${DEPS_BUCKET:-gs://tech-challenge-fase-2-spark}"
SA="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')-compute@developer.gserviceaccount.com"

echo ">> 1/3 empacotando e subindo o código para o GCS"
zip -qr src.zip src -x "*__pycache__*" "*.ipynb_checkpoints*"
gcloud storage cp src.zip                "$BUCKET/code/src.zip"
gcloud storage cp src/run_pipeline.py    "$BUCKET/code/run_pipeline.py"

echo ">> 2/3 publicando o Cloud Workflow"
gcloud workflows deploy pipeline-medalhao \
  --project="$PROJECT" --location="$REGION" \
  --source=orchestration/pipeline_workflow.yaml

echo ">> 3/3 criando o agendamento diário (06:00 BRT)"
gcloud scheduler jobs create http pipeline-diario \
  --project="$PROJECT" --location="$REGION" \
  --schedule="0 6 * * *" --time-zone="America/Sao_Paulo" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT/locations/$REGION/workflows/pipeline-medalhao/executions" \
  --http-method=POST \
  --oauth-service-account-email="$SA" \
  || echo "   (scheduler ja existe ou exigiu Cloud Shell — ver nota no chat)"

echo ">> pronto. Teste manual:  gcloud workflows run pipeline-medalhao --location=$REGION"
