#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-unit-schema
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-schema-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/unit-schema-%j.err
#SBATCH --time=00:05:00
#SBATCH --mem=1G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_unit_schema_amendment_review_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
AUDIT_RUN_ID=20260807T083117Z

if [[ -z "${UNIT_SCHEMA_AMENDMENT_RUN_ID:-}" ]]; then
    UNIT_SCHEMA_AMENDMENT_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${UNIT_SCHEMA_AMENDMENT_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable unit-schema amendment run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/schema_dictionary_amendment.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/harmonization/reviewed_schema_dictionary_freeze_0_1.yaml" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/static_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/dynamic_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/private/unit_decision_audit/${AUDIT_RUN_ID}/unit_decision_audit_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable schema-amendment input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_UNIT_SCHEMA_AMENDMENT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "unit_schema_amendment_run_id=${UNIT_SCHEMA_AMENDMENT_RUN_ID}"
    exec sbatch \
        --export=ALL,UNIT_SCHEMA_AMENDMENT_RUN_ID="${UNIT_SCHEMA_AMENDMENT_RUN_ID}",ASIC_UNIT_SCHEMA_AMENDMENT_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: unit-schema amendment worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline review-unit-schema-amendment \
    --config "${CONFIG}" \
    --run-id "${UNIT_SCHEMA_AMENDMENT_RUN_ID}"
review_status=$?
set -e
if [[ "${review_status}" -ne 2 ]]; then
    echo "error: expected technically passing schema amendment pending human approval (exit 2); observed ${review_status}" >&2
    exit 1
fi

echo "unit_schema_amendment_review_completed_with_human_findings=true"
echo "clinical_rows_read=0"
echo "clinical_data_written=false"
echo "schema_frozen=false"
echo "dictionary_frozen=false"
echo "existing_releases_modified=false"
echo "cleaning_ranges_activated=false"
echo "publication_ready=false"
