#!/bin/bash

# SLURM directives
# Submit this script from inside hpc-icu-data-platform/ with: sbatch run_blocking.sh
#SBATCH --job-name=asic_blocking
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --output=logs/%x_%j.log
#SBATCH --error=logs/%x_%j.err
# #SBATCH --account=rwth1641

set -euo pipefail

project_root="${PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}}"

INPUT_DIR="${INPUT_DIR:-${project_root}/artifacts/asic_harmonized_full}"
OUTPUT_DIR="${OUTPUT_DIR:-${INPUT_DIR}}"
INPUT_FORMAT="${INPUT_FORMAT:-csv}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-csv}"
DYNAMIC_CHUNKSIZE="${DYNAMIC_CHUNKSIZE:-250000}"

VENV_PATH="/home/am861154/mypyenv"

mkdir -p "${project_root}/logs"
mkdir -p "${OUTPUT_DIR}"

if [ ! -d "${INPUT_DIR}" ]; then
    echo "INPUT_DIR does not exist: ${INPUT_DIR}" >&2
    exit 1
fi

module purge
# module load Python/3.9.6

if [ -f "${VENV_PATH}/bin/activate" ]; then
    source "${VENV_PATH}/bin/activate"
fi

cd "${project_root}"

echo "[$(date)] Starting ASIC per-hospital blocking job"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_ROOT: ${project_root}"
echo "INPUT_DIR: ${INPUT_DIR}"
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "INPUT_FORMAT: ${INPUT_FORMAT}"
echo "OUTPUT_FORMAT: ${OUTPUT_FORMAT}"
echo "DYNAMIC_CHUNKSIZE: ${DYNAMIC_CHUNKSIZE}"

python run_asic_standardized_from_harmonized.py \
    --input-dir "${INPUT_DIR}" \
    --output-dir "${OUTPUT_DIR}" \
    --input-format "${INPUT_FORMAT}" \
    --output-format "${OUTPUT_FORMAT}" \
    --dynamic-chunksize "${DYNAMIC_CHUNKSIZE}"

echo "[$(date)] ASIC per-hospital blocking job finished"
