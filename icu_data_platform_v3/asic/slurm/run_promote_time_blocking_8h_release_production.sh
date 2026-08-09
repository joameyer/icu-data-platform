#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-tb8h-promote
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-promote-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-promote-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_promote_time_blocking_8h_release_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CANDIDATE_RUN_ID=20260808T130534Z
AUDIT_RUN_ID=20260808T130534Z
RELEASE_ID=20260808T130534Z
CORE_DERIVED_RELEASE_ID=20260808T074305Z
CANDIDATE_DIR=${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/candidates/${CANDIDATE_RUN_ID}
RELEASE_DIR=${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/releases/${RELEASE_ID}
STAGING_DIR=${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/releases/.${RELEASE_ID}.incomplete
BLOCKED_CURRENT_POINTER=${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/current_release.json
CORE_CURRENT_POINTER=${PROJECT_ROOT}/asic/data/production/derived/current_release.json
CORE_RELEASE_DIR=${PROJECT_ROOT}/asic/data/production/derived/releases/${CORE_DERIVED_RELEASE_ID}

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/time_blocking/reviewed_8h_contract_0_1.yaml" \
    "${PROJECT_ROOT}/asic/config/time_blocking/production_candidate_audit_8h.yaml" \
    "${PROJECT_ROOT}/asic/config/time_blocking/reviewed_8h_production_execution.yaml" \
    "${PROJECT_ROOT}/asic/config/time_blocking/reviewed_8h_promotion_20260808T130534Z.yaml" \
    "${CANDIDATE_DIR}/candidate_manifest.json" \
    "${CANDIDATE_DIR}/blocks.parquet" \
    "${CANDIDATE_DIR}/stay_block_summary.parquet" \
    "${CANDIDATE_DIR}/blocked_variable_dictionary.parquet" \
    "${CANDIDATE_DIR}/blocked_variable_dictionary.md" \
    "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/build/${CANDIDATE_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/time_blocking/8h/build/${CANDIDATE_RUN_ID}/build_manifest.json" \
    "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/audit/${AUDIT_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/time_blocking/8h/audit/${AUDIT_RUN_ID}/audit_manifest.json" \
    "${CORE_CURRENT_POINTER}" \
    "${CORE_RELEASE_DIR}/release_manifest.json" \
    "${CORE_RELEASE_DIR}/static.parquet" \
    "${CORE_RELEASE_DIR}/dynamic.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required approved time-blocking promotion input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

if [[ -e "${RELEASE_DIR}" ]] \
    || [[ -e "${STAGING_DIR}" ]] \
    || [[ -e "${BLOCKED_CURRENT_POINTER}" ]] \
    || [[ -e "${BLOCKED_CURRENT_POINTER}.tmp" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/promotion/${RELEASE_ID}.json" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/promotion/${RELEASE_ID}.md" ]]; then
    echo "error: time-blocking promotion target or initial pointer already exists; nothing will be overwritten" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_TIME_BLOCKING_8H_PROMOTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "time_blocking_8h_release_id=${RELEASE_ID}"
    exec sbatch \
        --export=ALL,ASIC_TIME_BLOCKING_8H_PROMOTION_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: time-blocking promotion worker must run inside a Slurm allocation" >&2
    exit 1
fi

candidate_manifest_before=$(sha256sum "${CANDIDATE_DIR}/candidate_manifest.json" | awk '{print $1}')
blocks_before=$(sha256sum "${CANDIDATE_DIR}/blocks.parquet" | awk '{print $1}')
summary_before=$(sha256sum "${CANDIDATE_DIR}/stay_block_summary.parquet" | awk '{print $1}')
dictionary_before=$(sha256sum "${CANDIDATE_DIR}/blocked_variable_dictionary.parquet" | awk '{print $1}')
dictionary_md_before=$(sha256sum "${CANDIDATE_DIR}/blocked_variable_dictionary.md" | awk '{print $1}')
core_pointer_before=$(sha256sum "${CORE_CURRENT_POINTER}" | awk '{print $1}')
core_manifest_before=$(sha256sum "${CORE_RELEASE_DIR}/release_manifest.json" | awk '{print $1}')
core_static_before=$(sha256sum "${CORE_RELEASE_DIR}/static.parquet" | awk '{print $1}')
core_dynamic_before=$(sha256sum "${CORE_RELEASE_DIR}/dynamic.parquet" | awk '{print $1}')

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline promote-time-blocking-release \
    --config "${CONFIG}" \
    --resolution 8h

check_unchanged() {
    local path=$1
    local expected=$2
    local label=$3
    local observed
    observed=$(sha256sum "${path}" | awk '{print $1}')
    if [[ "${observed}" != "${expected}" ]]; then
        echo "error: ${label} changed during promotion" >&2
        exit 1
    fi
}

check_unchanged "${CANDIDATE_DIR}/candidate_manifest.json" "${candidate_manifest_before}" "candidate manifest"
check_unchanged "${CANDIDATE_DIR}/blocks.parquet" "${blocks_before}" "candidate blocks"
check_unchanged "${CANDIDATE_DIR}/stay_block_summary.parquet" "${summary_before}" "candidate stay summary"
check_unchanged "${CANDIDATE_DIR}/blocked_variable_dictionary.parquet" "${dictionary_before}" "candidate dictionary"
check_unchanged "${CANDIDATE_DIR}/blocked_variable_dictionary.md" "${dictionary_md_before}" "candidate dictionary Markdown"
check_unchanged "${CORE_CURRENT_POINTER}" "${core_pointer_before}" "core-derived current pointer"
check_unchanged "${CORE_RELEASE_DIR}/release_manifest.json" "${core_manifest_before}" "core-derived release manifest"
check_unchanged "${CORE_RELEASE_DIR}/static.parquet" "${core_static_before}" "core-derived static"
check_unchanged "${CORE_RELEASE_DIR}/dynamic.parquet" "${core_dynamic_before}" "core-derived dynamic"

check_unchanged "${RELEASE_DIR}/candidate_manifest.json" "${candidate_manifest_before}" "released candidate manifest"
check_unchanged "${RELEASE_DIR}/blocks.parquet" "${blocks_before}" "released blocks"
check_unchanged "${RELEASE_DIR}/stay_block_summary.parquet" "${summary_before}" "released stay summary"
check_unchanged "${RELEASE_DIR}/blocked_variable_dictionary.parquet" "${dictionary_before}" "released dictionary"
check_unchanged "${RELEASE_DIR}/blocked_variable_dictionary.md" "${dictionary_md_before}" "released dictionary Markdown"

if [[ ! -f "${BLOCKED_CURRENT_POINTER}" ]]; then
    echo "error: approved 8-hour current-release pointer was not created" >&2
    exit 1
fi
if [[ -e "${RELEASE_DIR}/static.parquet" ]]; then
    echo "error: static data was unexpectedly copied into the blocked release" >&2
    exit 1
fi

echo "time_blocking_8h_promotion_completed=true"
echo "time_blocking_8h_release_id=${RELEASE_ID}"
echo "audited_candidate_bytes_preserved=true"
echo "candidate_preserved=true"
echo "core_derived_release_modified=false"
echo "core_derived_current_pointer_modified=false"
echo "only_time_blocking_8h_current_pointer_created=true"
echo "static_table_copied=false"
echo "static_table_repeated_on_blocks=false"
echo "time_blocking_rerun_during_promotion=false"
echo "rows_or_stays_filtered=false"
echo "analysis_cohort_created=false"
echo "carry_forward_or_imputation_applied=false"
echo "medication_dose_totals_emitted=false"
echo "time_blocking_layer_ready=true"
echo "analysis_input_approved=true"
echo "publication_ready=true"
echo "external_data_export_authorized=false"
