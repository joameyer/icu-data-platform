#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-derived-review
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-review-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-review-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_derivation_contract_review_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANED_RELEASE_ID=20260806T114234Z

if [[ -z "${DERIVATION_REVIEW_RUN_ID:-}" ]]; then
    DERIVATION_REVIEW_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${DERIVATION_REVIEW_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable derivation-review run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/derivation/contract_review.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/dynamic.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required released cleaned input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_DERIVATION_REVIEW_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "derivation_review_run_id=${DERIVATION_REVIEW_RUN_ID}"
    exec sbatch \
        --export=ALL,DERIVATION_REVIEW_RUN_ID="${DERIVATION_REVIEW_RUN_ID}",ASIC_DERIVATION_REVIEW_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: derivation review worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline review-derived-contract \
    --config "${CONFIG}" \
    --run-id "${DERIVATION_REVIEW_RUN_ID}"
review_status=$?
set -e
if [[ "${review_status}" -ne 2 ]]; then
    echo "error: expected technically passing derivation review pending human approval (exit 2); observed ${review_status}" >&2
    exit 1
fi

echo "derivation_contract_review_completed=true"
echo "cleaned_release_modified=false"
echo "clinical_data_written=false"
echo "cohort_filtering_applied=false"
echo "time_blocking_applied=false"
echo "publication_ready=false"
