#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-tb8h-evidence
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-evidence-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/time-blocking-8h-evidence-%j.err
#SBATCH --time=04:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_time_blocking_8h_contract_evidence_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
POLICY=${PROJECT_ROOT}/asic/config/time_blocking/contract_evidence_8h.yaml
DERIVED_RELEASE_ID=20260808T074305Z

if [[ -z "${TIME_BLOCKING_EVIDENCE_RUN_ID:-}" ]]; then
    TIME_BLOCKING_EVIDENCE_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ ! "${TIME_BLOCKING_EVIDENCE_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable time-blocking evidence run ID" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${POLICY}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/data/production/derived/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${DERIVED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${DERIVED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/derived/releases/${DERIVED_RELEASE_ID}/dynamic.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required released core-derived input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

for prohibited_path in \
    "${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/candidates/${TIME_BLOCKING_EVIDENCE_RUN_ID}" \
    "${PROJECT_ROOT}/asic/data/production/derived/time_blocking/8h/releases/${TIME_BLOCKING_EVIDENCE_RUN_ID}"; do
    if [[ -e "${prohibited_path}" ]]; then
        echo "error: prohibited time-blocking data target exists: ${prohibited_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_TIME_BLOCKING_EVIDENCE_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "time_blocking_evidence_run_id=${TIME_BLOCKING_EVIDENCE_RUN_ID}"
    exec sbatch \
        --export=ALL,TIME_BLOCKING_EVIDENCE_RUN_ID="${TIME_BLOCKING_EVIDENCE_RUN_ID}",ASIC_TIME_BLOCKING_EVIDENCE_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: time-blocking evidence worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline audit-time-blocking-8h-contract-evidence \
    --config "${CONFIG}" \
    --run-id "${TIME_BLOCKING_EVIDENCE_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing evidence pending human review (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "time_blocking_contract_evidence_completed=true"
echo "core_derived_release_modified=false"
echo "clinical_data_written=false"
echo "blocked_rows_written=false"
echo "time_blocking_candidate_created=false"
echo "time_blocking_release_created=false"
echo "current_release_pointer_modified=false"
echo "rows_or_stays_filtered=false"
echo "carry_forward_or_imputation_applied=false"
echo "analysis_cohort_created=false"
echo "publication_ready=false"
echo "external_data_export_authorized=false"
