#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-clean02
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaning-0-2-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaning-0-2-%j.err
#SBATCH --time=00:40:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_cleaning_0_2_build_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
HARMONIZED_RELEASE_ID=20260807T112402Z
RANGE_EVIDENCE_RUN_ID=20260807T133609Z

if [[ -z "${CLEANING_0_2_RUN_ID:-}" ]]; then
    CLEANING_0_2_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ -z "${CLEANING_0_2_AUDIT_RUN_ID:-}" ]]; then
    CLEANING_0_2_AUDIT_RUN_ID=${CLEANING_0_2_RUN_ID}
fi
for run_id in "${CLEANING_0_2_RUN_ID}" "${CLEANING_0_2_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable cleaning-0.2 run ID" >&2
        exit 1
    fi
done

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/cleaning/reviewed_cleaning_policy_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_0_2_audit.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_policy.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/range_evidence_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${HARMONIZED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/review/cleaning_range_evidence_0_2/${RANGE_EVIDENCE_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/cleaning_range_evidence_0_2/${RANGE_EVIDENCE_RUN_ID}/audit_manifest.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/cleaning_range_evidence_0_2/${RANGE_EVIDENCE_RUN_ID}/hospital_variable_profiles.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/cleaning_range_evidence_0_2/${RANGE_EVIDENCE_RUN_ID}/decision_relevant_value_frequencies.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable cleaning-0.2 input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

CANDIDATE_DIR=${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${CLEANING_0_2_RUN_ID}
INCOMPLETE_DIR=${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/.${CLEANING_0_2_RUN_ID}.incomplete
if [[ -e "${CANDIDATE_DIR}" || -e "${INCOMPLETE_DIR}" ]]; then
    echo "error: cleaning-0.2 run output already exists" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CLEANING_0_2_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "cleaning_0_2_run_id=${CLEANING_0_2_RUN_ID}"
    echo "cleaning_0_2_audit_run_id=${CLEANING_0_2_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,CLEANING_0_2_RUN_ID="${CLEANING_0_2_RUN_ID}",CLEANING_0_2_AUDIT_RUN_ID="${CLEANING_0_2_AUDIT_RUN_ID}",ASIC_CLEANING_0_2_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: cleaning-0.2 worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-cleaned-0-2-candidate \
    --config "${CONFIG}" \
    --run-id "${CLEANING_0_2_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-cleaned-0-2-candidate \
    --config "${CONFIG}" \
    --build-run-id "${CLEANING_0_2_RUN_ID}" \
    --run-id "${CLEANING_0_2_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technical pass pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "cleaning_0_2_build_and_audit_completed=true"
echo "harmonized_release_modified=false"
echo "existing_cleaned_releases_modified=false"
echo "existing_derived_releases_modified=false"
echo "cleaned_dictionary_generated=true"
echo "derivation_applied=false"
echo "publication_ready=false"
