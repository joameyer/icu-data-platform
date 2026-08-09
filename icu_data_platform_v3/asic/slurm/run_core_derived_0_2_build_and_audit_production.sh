#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-derived02
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-0-2-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-0-2-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_core_derived_0_2_build_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANED_RELEASE_ID=20260807T154100Z
DRIVING_PRESSURE_REVIEW_RUN_ID=20260806T161232Z

if [[ -z "${CORE_DERIVED_0_2_RUN_ID:-}" ]]; then
    CORE_DERIVED_0_2_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ -z "${CORE_DERIVED_0_2_AUDIT_RUN_ID:-}" ]]; then
    CORE_DERIVED_0_2_AUDIT_RUN_ID=${CORE_DERIVED_0_2_RUN_ID}
fi
for run_id in "${CORE_DERIVED_0_2_RUN_ID}" "${CORE_DERIVED_0_2_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable core-derived-0.2 run ID" >&2
        exit 1
    fi
done

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/derivation/reviewed_core_derived_contract_0_1.yaml" \
    "${PROJECT_ROOT}/asic/config/derivation/reviewed_core_derived_contract_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/reviewed_cleaned_0_2_promotion_20260807T154100Z.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${CLEANED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/review/driving_pressure_semantics/${DRIVING_PRESSURE_REVIEW_RUN_ID}.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required core-derived-0.2 evidence is unavailable: ${required_path}" >&2
        exit 1
    fi
done
if [[ ! -d "${PROJECT_ROOT}/asic/reports/production/review/derivation_contract_review" ]]; then
    echo "error: approved derivation-review evidence is unavailable" >&2
    exit 1
fi

for prohibited_path in \
    "${PROJECT_ROOT}/asic/data/production/derived_0_2_candidates/${CORE_DERIVED_0_2_RUN_ID}" \
    "${PROJECT_ROOT}/asic/data/production/derived_0_2_candidates/.${CORE_DERIVED_0_2_RUN_ID}.incomplete" \
    "${PROJECT_ROOT}/asic/reports/production/private/derived_0_2_audit/${CORE_DERIVED_0_2_AUDIT_RUN_ID}" \
    "${PROJECT_ROOT}/asic/reports/production/review/derived_0_2_build/${CORE_DERIVED_0_2_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/review/derived_0_2_audit/${CORE_DERIVED_0_2_AUDIT_RUN_ID}.json"; do
    if [[ -e "${prohibited_path}" ]]; then
        echo "error: core-derived-0.2 output already exists; nothing will be overwritten: ${prohibited_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CORE_DERIVED_0_2_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "core_derived_0_2_run_id=${CORE_DERIVED_0_2_RUN_ID}"
    echo "core_derived_0_2_audit_run_id=${CORE_DERIVED_0_2_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,CORE_DERIVED_0_2_RUN_ID="${CORE_DERIVED_0_2_RUN_ID}",CORE_DERIVED_0_2_AUDIT_RUN_ID="${CORE_DERIVED_0_2_AUDIT_RUN_ID}",ASIC_CORE_DERIVED_0_2_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: core-derived-0.2 worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-core-derived-0-2-candidate \
    --config "${CONFIG}" \
    --run-id "${CORE_DERIVED_0_2_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-core-derived-0-2-candidate \
    --config "${CONFIG}" \
    --derived-run-id "${CORE_DERIVED_0_2_RUN_ID}" \
    --run-id "${CORE_DERIVED_0_2_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing core-derived-0.2 audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "core_derived_0_2_build_and_audit_completed=true"
echo "input_cleaned_release=${CLEANED_RELEASE_ID}"
echo "clinical_formula_changes_from_0_1=0"
echo "every_cleaned_0_2_input_column_preserved_exactly=true"
echo "every_derived_value_recomputed_exactly=true"
echo "cleaned_release_modified=false"
echo "rows_or_stays_filtered=false"
echo "cohort_generated=false"
echo "time_blocking_applied=false"
echo "publication_ready=false"
