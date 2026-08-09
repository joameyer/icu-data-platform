#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-raw-inventory
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/raw_inventory-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/raw_inventory-%j.err
#SBATCH --time=02:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RAW_ROOT=/hpcwork/jrc_combine/richard/asic_2026

if [[ ! -d "${PROJECT_ROOT}" ]]; then
    echo "error: dedicated v3 deployment root is unavailable" >&2
    exit 1
fi
if [[ ! -d "${RAW_ROOT}" ]]; then
    echo "error: authoritative raw root is unavailable" >&2
    exit 1
fi
if [[ ! -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    echo "error: dedicated v3 Python environment is unavailable" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${PROJECT_ROOT}/src"

"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline inventory-raw \
    --config "${PROJECT_ROOT}/asic/config/datasets/production.yaml"
