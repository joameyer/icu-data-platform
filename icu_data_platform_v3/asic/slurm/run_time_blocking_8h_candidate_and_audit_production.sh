#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-tb8h
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_time_blocking_8h_candidate_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CONTRACT=${PROJECT_ROOT}/asic/config/time_blocking/reviewed_8h_contract_0_1.yaml
WORKFLOW_POLICY=${PROJECT_ROOT}/asic/config/time_blocking/production_candidate_audit_8h.yaml
EXECUTION_AUTHORIZATION=${PROJECT_ROOT}/asic/config/time_blocking/reviewed_8h_production_execution.yaml
CORE_DERIVED_RELEASE_ID=20260808T074305Z
CURRENT_DERIVED_POINTER=${PROJECT_ROOT}/asic/data/production/derived/current_release.json
BLOCKED_CURRENT_POINTER=${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/current_release.json

if [[ -z "${TIME_BLOCKING_8H_CANDIDATE_RUN_ID:-}" ]]; then
    echo "error: set the exact human-approved TIME_BLOCKING_8H_CANDIDATE_RUN_ID" >&2
    exit 1
fi
if [[ -z "${TIME_BLOCKING_8H_AUDIT_RUN_ID:-}" ]]; then
    echo "error: set the exact human-approved TIME_BLOCKING_8H_AUDIT_RUN_ID" >&2
    exit 1
fi
for run_id in \
    "${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}" \
    "${TIME_BLOCKING_8H_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable time-blocking run ID" >&2
        exit 1
    fi
done

for required_path in \
    "${CONFIG}" \
    "${CONTRACT}" \
    "${WORKFLOW_POLICY}" \
    "${EXECUTION_AUTHORIZATION}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${CURRENT_DERIVED_POINTER}" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${CORE_DERIVED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${CORE_DERIVED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${CORE_DERIVED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/review/time_blocking_8h_contract_evidence/20260808T114902Z.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required reviewed time-blocking input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

for prohibited_path in \
    "${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/candidates/${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}" \
    "${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/candidates/.${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}.incomplete" \
    "${PROJECT_ROOT}/asic/reports/production/private/time_blocking/8h/build/${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}" \
    "${PROJECT_ROOT}/asic/reports/production/private/time_blocking/8h/audit/${TIME_BLOCKING_8H_AUDIT_RUN_ID}" \
    "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/build/${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/review/time_blocking/8h/audit/${TIME_BLOCKING_8H_AUDIT_RUN_ID}.json"; do
    if [[ -e "${prohibited_path}" ]]; then
        echo "error: immutable time-blocking output already exists; nothing will be overwritten: ${prohibited_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_TIME_BLOCKING_8H_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "time_blocking_8h_candidate_run_id=${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}"
    echo "time_blocking_8h_audit_run_id=${TIME_BLOCKING_8H_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,TIME_BLOCKING_8H_CANDIDATE_RUN_ID="${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}",TIME_BLOCKING_8H_AUDIT_RUN_ID="${TIME_BLOCKING_8H_AUDIT_RUN_ID}",ASIC_TIME_BLOCKING_8H_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: time-blocking worker must run inside a Slurm allocation" >&2
    exit 1
fi

core_pointer_sha256_before=$(sha256sum "${CURRENT_DERIVED_POINTER}" | awk '{print $1}')
if [[ -f "${BLOCKED_CURRENT_POINTER}" ]]; then
    blocked_pointer_state_before=$(sha256sum "${BLOCKED_CURRENT_POINTER}" | awk '{print $1}')
else
    blocked_pointer_state_before=absent
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-time-blocking-candidate \
    --config "${CONFIG}" \
    --resolution 8h \
    --run-id "${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-time-blocking-candidate \
    --config "${CONFIG}" \
    --resolution 8h \
    --build-run-id "${TIME_BLOCKING_8H_CANDIDATE_RUN_ID}" \
    --run-id "${TIME_BLOCKING_8H_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing independent audit pending human promotion approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

core_pointer_sha256_after=$(sha256sum "${CURRENT_DERIVED_POINTER}" | awk '{print $1}')
if [[ "${core_pointer_sha256_after}" != "${core_pointer_sha256_before}" ]]; then
    echo "error: core-derived current pointer changed during time blocking" >&2
    exit 1
fi
if [[ -f "${BLOCKED_CURRENT_POINTER}" ]]; then
    blocked_pointer_state_after=$(sha256sum "${BLOCKED_CURRENT_POINTER}" | awk '{print $1}')
else
    blocked_pointer_state_after=absent
fi
if [[ "${blocked_pointer_state_after}" != "${blocked_pointer_state_before}" ]]; then
    echo "error: blocked current-release pointer changed during candidate workflow" >&2
    exit 1
fi

echo "time_blocking_8h_candidate_and_audit_completed=true"
echo "core_derived_release=${CORE_DERIVED_RELEASE_ID}"
echo "independent_every_cell_audit_completed=true"
echo "core_derived_release_modified=false"
echo "static_table_copied=false"
echo "static_values_repeated_on_blocks=false"
echo "rows_or_stays_filtered=false"
echo "analysis_cohort_created=false"
echo "carry_forward_or_imputation_applied=false"
echo "medication_dose_totals_emitted=false"
echo "release_created=false"
echo "current_release_pointer_modified=false"
echo "publication_ready=false"
echo "external_data_export_authorized=false"
