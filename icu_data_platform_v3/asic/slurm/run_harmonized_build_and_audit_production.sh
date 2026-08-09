#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-harmonized-build
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-build-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-build-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_harmonized_build_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${HARMONIZED_BUILD_RUN_ID:-}" ]]; then
    HARMONIZED_BUILD_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ -z "${HARMONIZED_BUILD_AUDIT_RUN_ID:-}" ]]; then
    HARMONIZED_BUILD_AUDIT_RUN_ID=${HARMONIZED_BUILD_RUN_ID}
fi
for run_id in "${HARMONIZED_BUILD_RUN_ID}" "${HARMONIZED_BUILD_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable run ID" >&2
        exit 1
    fi
done
for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonization_candidates/20260806T074852Z/candidate_run_manifest.json"; do
    if [[ ! -e "${required_path}" ]]; then
        echo "error: required frozen build input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_HARMONIZED_BUILD_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "harmonized_build_run_id=${HARMONIZED_BUILD_RUN_ID}"
    echo "harmonized_build_audit_run_id=${HARMONIZED_BUILD_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,HARMONIZED_BUILD_RUN_ID="${HARMONIZED_BUILD_RUN_ID}",HARMONIZED_BUILD_AUDIT_RUN_ID="${HARMONIZED_BUILD_AUDIT_RUN_ID}",ASIC_HARMONIZED_BUILD_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: harmonized build worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-harmonized-candidate \
    --config "${CONFIG}" \
    --run-id "${HARMONIZED_BUILD_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-harmonized-build \
    --config "${CONFIG}" \
    --harmonized-build-run-id "${HARMONIZED_BUILD_RUN_ID}" \
    --run-id "${HARMONIZED_BUILD_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "harmonized_build_and_audit_completed=true"
echo "publication_ready=false"
