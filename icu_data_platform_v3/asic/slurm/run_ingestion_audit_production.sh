#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-ingest-audit
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/ingestion-audit-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/ingestion-audit-%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RAW_ROOT=/hpcwork/jrc_combine/richard/asic_2026
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_ingestion_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${INVENTORY_RUN_ID:-}" ]]; then
    echo "error: set INVENTORY_RUN_ID to the inventory used for ingestion" >&2
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
if [[ ! -f "${CONFIG}" || ! -f "${JOB_SCRIPT}" ]]; then
    echo "error: production config or deployed audit script is unavailable" >&2
    exit 1
fi
if [[ ! -f "${PROJECT_ROOT}/asic/reports/production/private/raw_inventory/${INVENTORY_RUN_ID}/inventory_manifest.json" ]]; then
    echo "error: immutable private inventory evidence is unavailable" >&2
    exit 1
fi
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_INGESTION_AUDIT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,INVENTORY_RUN_ID="${INVENTORY_RUN_ID}",ASIC_INGESTION_AUDIT_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: ingestion audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    audit-ingested \
    --config "${CONFIG}" \
    --inventory-run-id "${INVENTORY_RUN_ID}"
audit_exit=$?
set -e

if [[ ${audit_exit} -eq 2 ]]; then
    echo "ingestion_audit_completed_with_human_review_blocker=true"
    exit 0
fi
exit "${audit_exit}"
