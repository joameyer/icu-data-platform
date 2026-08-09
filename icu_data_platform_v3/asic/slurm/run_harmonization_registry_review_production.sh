#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-harm-reg-review
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonization-registry-review-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonization-registry-review-%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_harmonization_registry_review_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${SCHEMA_TOKEN_RUN_ID:-}" ]]; then
    echo "error: set SCHEMA_TOKEN_RUN_ID to the reviewed immutable policy-0.3 run" >&2
    exit 1
fi
if [[ ! "${SCHEMA_TOKEN_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid SCHEMA_TOKEN_RUN_ID" >&2
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
SCHEMA_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/schema_token_inventory/${SCHEMA_TOKEN_RUN_ID}
SCHEMA_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/schema_token_inventory/${SCHEMA_TOKEN_RUN_ID}.json
for required_path in \
    "${SCHEMA_PRIVATE}/schema_token_inventory_manifest.json" \
    "${SCHEMA_PRIVATE}/columns.parquet" \
    "${SCHEMA_PRIVATE}/tokens.parquet" \
    "${SCHEMA_REVIEW}"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: immutable schema-token evidence is incomplete" >&2
        exit 1
    fi
done
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_HARMONIZATION_REVIEW_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,SCHEMA_TOKEN_RUN_ID="${SCHEMA_TOKEN_RUN_ID}",ASIC_HARMONIZATION_REVIEW_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: harmonization registry-review worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    review-harmonization-registry \
    --config "${CONFIG}" \
    --schema-token-run-id "${SCHEMA_TOKEN_RUN_ID}"
review_exit=$?
set -e

if [[ ${review_exit} -eq 2 ]]; then
    echo "harmonization_registry_review_completed_with_human_review_findings=true"
    exit 0
fi
exit "${review_exit}"
