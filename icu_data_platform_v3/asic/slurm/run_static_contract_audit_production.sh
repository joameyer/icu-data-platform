#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-static-contract
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/static-contract-audit-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/static-contract-audit-%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_static_contract_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${REGISTRY_REVIEW_RUN_ID:-}" ]]; then
    echo "error: set REGISTRY_REVIEW_RUN_ID to the approved immutable review run" >&2
    exit 1
fi
if [[ -z "${COMPOSITE_AUDIT_RUN_ID:-}" ]]; then
    echo "error: set COMPOSITE_AUDIT_RUN_ID to the passing immutable composite audit" >&2
    exit 1
fi
for run_id in "${REGISTRY_REVIEW_RUN_ID}" "${COMPOSITE_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable input run ID" >&2
        exit 1
    fi
done
if [[ ! -d "${PROJECT_ROOT}" ]]; then
    echo "error: dedicated v3 project root is unavailable" >&2
    exit 1
fi
if [[ ! -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    echo "error: dedicated v3 Python environment is unavailable" >&2
    exit 1
fi
if [[ ! -f "${CONFIG}" || ! -f "${JOB_SCRIPT}" ]]; then
    echo "error: production config or deployed job script is unavailable" >&2
    exit 1
fi
REGISTRY_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}
REGISTRY_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}.json
COMPOSITE_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/composite_source_audit/${COMPOSITE_AUDIT_RUN_ID}.json
for required_path in \
    "${REGISTRY_PRIVATE}/harmonization_registry_review_manifest.json" \
    "${REGISTRY_PRIVATE}/occurrences.parquet" \
    "${REGISTRY_PRIVATE}/variables.parquet" \
    "${REGISTRY_REVIEW}" \
    "${COMPOSITE_REVIEW}"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: immutable static-contract input evidence is incomplete" >&2
        exit 1
    fi
done
for hospital in asic_UK00 asic_UK01 asic_UK02 asic_UK03 asic_UK04 asic_UK06 asic_UK07 asic_UK08; do
    for required_path in \
        "${PROJECT_ROOT}/asic/data/production/ingested/${hospital}/static.parquet" \
        "${PROJECT_ROOT}/asic/data/production/ingested/${hospital}/ingestion_manifest.json"; do
        if [[ ! -f "${required_path}" ]]; then
            echo "error: ingested static evidence is incomplete for ${hospital}" >&2
            exit 1
        fi
    done
done
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_STATIC_CONTRACT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,REGISTRY_REVIEW_RUN_ID="${REGISTRY_REVIEW_RUN_ID}",COMPOSITE_AUDIT_RUN_ID="${COMPOSITE_AUDIT_RUN_ID}",ASIC_STATIC_CONTRACT_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: static-contract audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    audit-static-contract \
    --config "${CONFIG}" \
    --registry-review-run-id "${REGISTRY_REVIEW_RUN_ID}" \
    --composite-audit-run-id "${COMPOSITE_AUDIT_RUN_ID}"
audit_exit=$?
set -e

if [[ ${audit_exit} -eq 2 ]]; then
    echo "static_contract_audit_completed_with_human_review_findings=true"
    exit 0
fi
exit "${audit_exit}"
