#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-unit-audit
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-audit-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-audit-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_unit_resolution_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANED_RELEASE_ID=20260806T114234Z
HARMONIZATION_DRY_RUN_ID=20260806T074852Z
CONSOLIDATED_AUDIT_RUN_ID=20260806T092818Z

if [[ -z "${UNIT_RESOLUTION_AUDIT_RUN_ID:-}" ]]; then
    UNIT_RESOLUTION_AUDIT_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${UNIT_RESOLUTION_AUDIT_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable unit-resolution audit run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/audit.yaml" \
    "${PROJECT_ROOT}/asic/config/harmonization/reviewed_schema_dictionary_freeze_0_1.yaml" \
    "${PROJECT_ROOT}/asic/config/harmonization/cross_hospital_quality_audit.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/harmonization_dry_run/${HARMONIZATION_DRY_RUN_ID}/harmonization_dry_run_manifest.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/harmonization_dry_run/${HARMONIZATION_DRY_RUN_ID}/occurrence_plan.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}/consolidated_audit_manifest.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}/unit_review.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}/numeric_profiles.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable unit-review input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_UNIT_RESOLUTION_AUDIT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "unit_resolution_audit_run_id=${UNIT_RESOLUTION_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,UNIT_RESOLUTION_AUDIT_RUN_ID="${UNIT_RESOLUTION_AUDIT_RUN_ID}",ASIC_UNIT_RESOLUTION_AUDIT_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: unit-resolution audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline audit-unresolved-units \
    --config "${CONFIG}" \
    --run-id "${UNIT_RESOLUTION_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing unit-resolution audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "unit_resolution_audit_completed_with_human_review_findings=true"
echo "clinical_data_written=false"
echo "harmonized_release_modified=false"
echo "cleaned_release_modified=false"
echo "derived_release_modified=false"
echo "values_converted=false"
echo "values_masked=false"
echo "publication_ready=false"
