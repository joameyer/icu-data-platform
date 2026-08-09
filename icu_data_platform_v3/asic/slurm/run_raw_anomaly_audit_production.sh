#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-raw-anomalies
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/raw_anomalies-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/raw_anomalies-%j.err
#SBATCH --time=06:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RAW_ROOT=/hpcwork/jrc_combine/richard/asic_2026

if [[ -z "${INVENTORY_RUN_ID:-}" ]]; then
    echo "error: submit with INVENTORY_RUN_ID set to a reviewed inventory run" >&2
    exit 1
fi
if [[ ! "${INVENTORY_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid INVENTORY_RUN_ID" >&2
    exit 1
fi
if [[ ! -d "${PROJECT_ROOT}" || ! -d "${RAW_ROOT}" ]]; then
    echo "error: v3 project or authoritative raw root is unavailable" >&2
    exit 1
fi
if [[ ! -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    echo "error: dedicated v3 Python environment is unavailable" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${PROJECT_ROOT}/src"

"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline audit-raw-anomalies \
    --config "${PROJECT_ROOT}/asic/config/datasets/production.yaml" \
    --inventory-run-id "${INVENTORY_RUN_ID}"
