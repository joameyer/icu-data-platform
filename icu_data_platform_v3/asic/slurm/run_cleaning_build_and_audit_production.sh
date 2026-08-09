#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-cleaning
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaning-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaning-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_cleaning_build_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
HARMONIZED_RELEASE_ID=20260806T111156Z

if [[ -z "${CLEANING_RUN_ID:-}" ]]; then
    CLEANING_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ -z "${CLEANING_AUDIT_RUN_ID:-}" ]]; then
    CLEANING_AUDIT_RUN_ID=${CLEANING_RUN_ID}
fi
for run_id in "${CLEANING_RUN_ID}" "${CLEANING_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable cleaning run ID" >&2
        exit 1
    fi
done
for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/dynamic.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required released harmonized input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CLEANING_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "cleaning_run_id=${CLEANING_RUN_ID}"
    echo "cleaning_audit_run_id=${CLEANING_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,CLEANING_RUN_ID="${CLEANING_RUN_ID}",CLEANING_AUDIT_RUN_ID="${CLEANING_AUDIT_RUN_ID}",ASIC_CLEANING_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: cleaning worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-cleaned-candidate \
    --config "${CONFIG}" \
    --run-id "${CLEANING_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-cleaned-candidate \
    --config "${CONFIG}" \
    --cleaning-run-id "${CLEANING_RUN_ID}" \
    --run-id "${CLEANING_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing cleaning audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "cleaning_build_and_audit_completed=true"
echo "harmonized_data_modified=false"
echo "rows_or_stays_filtered=false"
echo "columns_dropped=false"
echo "derivation_applied=false"
echo "publication_ready=false"
