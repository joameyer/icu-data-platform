#!/usr/bin/env bash
#SBATCH --job-name=asic-blocking
#SBATCH --output=logs/asic_blocking_%j.out
#SBATCH --error=logs/asic_blocking_%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-${SLURM_SUBMIT_DIR:-$PWD}}"
INPUT_DIR="${INPUT_DIR:-${PROJECT_DIR}/artifacts/asic_harmonized_full}"
OUTPUT_DIR="${OUTPUT_DIR:-${INPUT_DIR}}"
INPUT_FORMAT="${INPUT_FORMAT:-csv}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-csv}"
DYNAMIC_CHUNKSIZE="${DYNAMIC_CHUNKSIZE:-250000}"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "PROJECT_DIR does not exist: $PROJECT_DIR" >&2
  exit 1
fi

if [ ! -d "$INPUT_DIR" ]; then
  echo "INPUT_DIR does not exist: $INPUT_DIR" >&2
  exit 1
fi

if [ ! -f "$PROJECT_DIR/scripts/run_asic_standardized_from_harmonized.sh" ]; then
  echo "Missing launcher script: $PROJECT_DIR/scripts/run_asic_standardized_from_harmonized.sh" >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$OUTPUT_DIR"

cd "$PROJECT_DIR"

# Optional environment setup for your cluster:
# module load Python/3.11.5
# source .venv/bin/activate

echo "[$(date)] Starting ASIC per-hospital blocking job"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_DIR: $PROJECT_DIR"
echo "INPUT_DIR: $INPUT_DIR"
echo "OUTPUT_DIR: $OUTPUT_DIR"
echo "INPUT_FORMAT: $INPUT_FORMAT"
echo "OUTPUT_FORMAT: $OUTPUT_FORMAT"
echo "DYNAMIC_CHUNKSIZE: $DYNAMIC_CHUNKSIZE"

"$PROJECT_DIR/scripts/run_asic_standardized_from_harmonized.sh" \
  --input-dir "$INPUT_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --input-format "$INPUT_FORMAT" \
  --output-format "$OUTPUT_FORMAT" \
  --dynamic-chunksize "$DYNAMIC_CHUNKSIZE"

echo "[$(date)] ASIC per-hospital blocking job finished"
