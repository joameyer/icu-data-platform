#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-schema-tokens
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/schema-tokens-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/schema-tokens-%j.err
#SBATCH --time=48:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_schema_token_inventory_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

for required_name in INVENTORY_RUN_ID INGESTION_AUDIT_RUN_ID; do
    if [[ -z "${!required_name:-}" ]]; then
        echo "error: set ${required_name} to the reviewed immutable input run" >&2
        exit 1
    fi
    if [[ ! "${!required_name}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid ${required_name}" >&2
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
if [[ ! -f "${PROJECT_ROOT}/asic/reports/production/private/raw_inventory/${INVENTORY_RUN_ID}/inventory_manifest.json" ]]; then
    echo "error: immutable raw inventory evidence is unavailable" >&2
    exit 1
fi
if [[ ! -f "${PROJECT_ROOT}/asic/reports/production/private/ingestion_audit/${INGESTION_AUDIT_RUN_ID}/ingestion_audit_manifest.json" ]]; then
    echo "error: immutable ingestion-audit evidence is unavailable" >&2
    exit 1
fi
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_SCHEMA_TOKEN_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,INVENTORY_RUN_ID="${INVENTORY_RUN_ID}",INGESTION_AUDIT_RUN_ID="${INGESTION_AUDIT_RUN_ID}",ASIC_SCHEMA_TOKEN_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: schema-token worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    inventory-schema-tokens \
    --config "${CONFIG}" \
    --inventory-run-id "${INVENTORY_RUN_ID}" \
    --ingestion-audit-run-id "${INGESTION_AUDIT_RUN_ID}"
inventory_exit=$?
set -e

if [[ ${inventory_exit} -eq 2 ]]; then
    echo "schema_token_inventory_completed_with_human_review_findings=true"
    exit 0
fi
exit "${inventory_exit}"
