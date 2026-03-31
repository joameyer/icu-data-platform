#!/usr/bin/env bash
#SBATCH --job-name=asic-harmonize
#SBATCH --output=logs/asic_harmonize_%j.out
#SBATCH --error=logs/asic_harmonize_%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-${SLURM_SUBMIT_DIR:-$PWD}}"
RAW_DIR="${RAW_DIR:?Set RAW_DIR to the ASIC raw-data root on the cluster.}"
OUTPUT_DIR="${OUTPUT_DIR:?Set OUTPUT_DIR for the harmonized outputs.}"
FORMAT="${FORMAT:-csv}"
MIN_NON_NULL="${MIN_NON_NULL:-20}"
MIN_HOSPITALS="${MIN_HOSPITALS:-4}"
FENCE_FACTOR="${FENCE_FACTOR:-1.5}"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "PROJECT_DIR does not exist: $PROJECT_DIR" >&2
  exit 1
fi

if [ ! -d "$RAW_DIR" ]; then
  echo "RAW_DIR does not exist: $RAW_DIR" >&2
  exit 1
fi

if [ ! -f "$PROJECT_DIR/scripts/run_asic_harmonization.sh" ]; then
  echo "Missing launcher script: $PROJECT_DIR/scripts/run_asic_harmonization.sh" >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$OUTPUT_DIR"

cd "$PROJECT_DIR"

# Optional environment setup for your cluster:
# module load Python/3.11.5
# source .venv/bin/activate

echo "[$(date)] Starting ASIC harmonization job"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_DIR: $PROJECT_DIR"
echo "RAW_DIR: $RAW_DIR"
echo "OUTPUT_DIR: $OUTPUT_DIR"
echo "FORMAT: $FORMAT"
echo "MIN_NON_NULL: $MIN_NON_NULL"
echo "MIN_HOSPITALS: $MIN_HOSPITALS"
echo "FENCE_FACTOR: $FENCE_FACTOR"

"$PROJECT_DIR/scripts/run_asic_harmonization.sh" \
  --raw-dir "$RAW_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --format "$FORMAT" \
  --min-non-null "$MIN_NON_NULL" \
  --min-hospitals "$MIN_HOSPITALS" \
  --fence-factor "$FENCE_FACTOR"

echo "[$(date)] ASIC harmonization job finished"
