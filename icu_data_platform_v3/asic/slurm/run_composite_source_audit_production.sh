#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-composite-audit
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/composite-source-audit-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/composite-source-audit-%j.err
#SBATCH --time=04:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_composite_source_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${REGISTRY_REVIEW_RUN_ID:-}" ]]; then
    echo "error: set REGISTRY_REVIEW_RUN_ID to the approved immutable review run" >&2
    exit 1
fi
if [[ ! "${REGISTRY_REVIEW_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid REGISTRY_REVIEW_RUN_ID" >&2
    exit 1
fi
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
REVIEW_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}
REVIEW_JSON=${PROJECT_ROOT}/asic/reports/production/review/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}.json
for required_path in \
    "${REVIEW_PRIVATE}/harmonization_registry_review_manifest.json" \
    "${REVIEW_PRIVATE}/occurrences.parquet" \
    "${REVIEW_JSON}" \
    "${PROJECT_ROOT}/asic/data/production/ingested/asic_UK00/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/ingested/asic_UK00/ingestion_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: immutable composite-audit input evidence is incomplete" >&2
        exit 1
    fi
done
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_COMPOSITE_AUDIT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,REGISTRY_REVIEW_RUN_ID="${REGISTRY_REVIEW_RUN_ID}",ASIC_COMPOSITE_AUDIT_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: composite-source audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    audit-reviewed-composite-source \
    --config "${CONFIG}" \
    --registry-review-run-id "${REGISTRY_REVIEW_RUN_ID}"
