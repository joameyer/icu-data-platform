#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-med-zero
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/med-zero-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/med-zero-%j.err
#SBATCH --time=00:15:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_medication_semantics_and_schema_review_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANED_RELEASE_ID=20260806T114234Z
UNIT_DECISION_AUDIT_RUN_ID=20260807T083117Z

if [[ -z "${MEDICATION_SEMANTICS_RUN_ID:-}" ]]; then
    MEDICATION_SEMANTICS_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${MEDICATION_SEMANTICS_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable medication-semantics run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/medication_value_semantics_audit.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/schema_dictionary_amendment.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/derivation/contract_review.yaml" \
    "${PROJECT_ROOT}/asic/config/harmonization/reviewed_schema_dictionary_freeze_0_1.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/static_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/dynamic_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/unit_decision_audit/${UNIT_DECISION_AUDIT_RUN_ID}/unit_decision_audit_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable medication-semantics input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_MEDICATION_SEMANTICS_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "medication_semantics_run_id=${MEDICATION_SEMANTICS_RUN_ID}"
    exec sbatch \
        --export=ALL,MEDICATION_SEMANTICS_RUN_ID="${MEDICATION_SEMANTICS_RUN_ID}",ASIC_MEDICATION_SEMANTICS_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: medication-semantics worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline audit-medication-value-semantics \
    --config "${CONFIG}" \
    --run-id "${MEDICATION_SEMANTICS_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing medication audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

set +e
.venv/bin/asic-pipeline review-unit-schema-amendment \
    --config "${CONFIG}" \
    --run-id "${MEDICATION_SEMANTICS_RUN_ID}"
schema_status=$?
set -e
if [[ "${schema_status}" -ne 2 ]]; then
    echo "error: expected technically passing revised schema review pending human approval (exit 2); observed ${schema_status}" >&2
    exit 1
fi

echo "medication_semantics_and_schema_review_completed=true"
echo "clinical_data_written=false"
echo "existing_releases_modified=false"
echo "null_to_zero_imputation_applied=false"
echo "zero_to_null_conversion_applied=false"
echo "carry_forward_applied=false"
echo "schema_frozen=false"
echo "dictionary_frozen=false"
echo "publication_ready=false"
