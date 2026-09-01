set -euo pipefail
 
PROJECT="${GCP_PROJECT_ID:-tech-challenge-fase-2-505123}"
REGION="${REGION:-us-central1}"
RUNTIME_VERSION="${RUNTIME_VERSION:-2.2}"      # NUNCA usar o default do serverless
DEPS_BUCKET="${DEPS_BUCKET:-gs://tech-challenge-fase-2-spark}"
SUBNET="${SUBNET:-default}"
 
MODE="${1:-pipeline}"
 
case "$MODE" in
  pipeline)
    MAIN="src/run_pipeline.py"
    NAME="pipeline-full"
    ;;
  script)
    MAIN="${2:?informe o caminho do script, ex: src/silver/silver_uf.py}"
    NAME="$(basename "${MAIN%.py}" | tr '_' '-')"
    ;;
  *)
    echo "uso: $0 [pipeline | script <caminho.py>]" >&2
    exit 1
    ;;
esac
 
# rezipa o estado atual do código (exclui checkpoints e cache dos ipynb)
zip -qr src.zip src -x "*__pycache__*" "*.ipynb_checkpoints*"
 
gcloud dataproc batches submit pyspark "$MAIN" \
  --project="$PROJECT" \
  --region="$REGION" \
  --version="$RUNTIME_VERSION" \
  --batch="${NAME}-$(date +%Y%m%d-%H%M%S)" \
  --deps-bucket="$DEPS_BUCKET" \
  --py-files=src.zip \
  --subnet="$SUBNET"