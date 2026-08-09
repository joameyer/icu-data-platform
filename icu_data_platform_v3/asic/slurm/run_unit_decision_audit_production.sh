#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-unit-decisions
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-decisions-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-decisions-%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_unit_decision_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANED_RELEASE_ID=20260806T114234Z

if [[ -z "${UNIT_DECISION_AUDIT_RUN_ID:-}" ]]; then
    UNIT_DECISION_AUDIT_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${UNIT_DECISION_AUDIT_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable unit-decision audit run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/decision_audit.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/derivation/contract_review.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/dynamic.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable unit-decision input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_UNIT_DECISION_AUDIT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "unit_decision_audit_run_id=${UNIT_DECISION_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,UNIT_DECISION_AUDIT_RUN_ID="${UNIT_DECISION_AUDIT_RUN_ID}",ASIC_UNIT_DECISION_AUDIT_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: unit-decision audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline audit-candidate-unit-decisions \
    --config "${CONFIG}" \
    --run-id "${UNIT_DECISION_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing unit-decision audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "unit_decision_audit_completed_with_human_review_findings=true"
echo "clinical_data_written=false"
echo "harmonized_release_modified=false"
echo "cleaned_release_modified=false"
echo "derived_release_modified=false"
echo "values_converted=false"
echo "values_masked=false"
echo "contract_0_2_activated=false"
echo "publication_ready=false"
