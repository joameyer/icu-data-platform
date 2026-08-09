#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-clean-range
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/clean-range-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/clean-range-%j.err
#SBATCH --time=00:15:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_cleaning_range_evidence_0_2_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
RELEASE_ID=20260807T112402Z

if [[ -z "${CLEANING_RANGE_EVIDENCE_RUN_ID:-}" ]]; then
    CLEANING_RANGE_EVIDENCE_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${CLEANING_RANGE_EVIDENCE_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable cleaning range-evidence run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/cleaning/range_evidence_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_policy_0_2_review.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/freeze_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable cleaning range-evidence input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CLEANING_RANGE_EVIDENCE_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" || -n "${SLURM_STEP_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "cleaning_range_evidence_run_id=${CLEANING_RANGE_EVIDENCE_RUN_ID}"
    exec sbatch \
        --export=ALL,CLEANING_RANGE_EVIDENCE_RUN_ID="${CLEANING_RANGE_EVIDENCE_RUN_ID}",ASIC_CLEANING_RANGE_EVIDENCE_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: cleaning range-evidence worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline audit-cleaning-0-2-ranges \
    --config "${CONFIG}" \
    --run-id "${CLEANING_RANGE_EVIDENCE_RUN_ID}"
audit_status=$?
set -e

if [[ ${audit_status} -ne 2 ]]; then
    echo "error: expected a technical pass pending one human range decision; observed exit ${audit_status}" >&2
    exit 1
fi

echo "cleaning_range_evidence_completed=true"
echo "clinical_data_written=false"
echo "existing_releases_modified=false"
echo "cleaning_rules_activated=false"
echo "publication_ready=false"
