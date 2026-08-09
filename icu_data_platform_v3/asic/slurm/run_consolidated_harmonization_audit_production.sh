#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-consolidated-audit
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/consolidated-harmonization-audit-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/consolidated-harmonization-audit-%j.err
#SBATCH --partition=c23ms
#SBATCH --time=00:30:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_consolidated_harmonization_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${HARMONIZATION_RUN_ID:-}" ]]; then
    echo "error: set HARMONIZATION_RUN_ID to the completed immutable candidate run" >&2
    exit 1
fi
if [[ -z "${CONSOLIDATED_AUDIT_RUN_ID:-}" ]]; then
    CONSOLIDATED_AUDIT_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
for run_id in "${HARMONIZATION_RUN_ID}" "${CONSOLIDATED_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable run ID" >&2
        exit 1
    fi
done
if [[ ! -d "${PROJECT_ROOT}" ]]; then
    echo "error: dedicated v3 project root is unavailable" >&2
    exit 1
fi
if [[ ! -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
    echo "error: dedicated v3 Python environment is unavailable" >&2
    exit 1
fi
if [[ ! -f "${CONFIG}" || ! -f "${JOB_SCRIPT}" ]]; then
    echo "error: production config or deployed job script is unavailable" >&2
    exit 1
fi

CANDIDATE_ROOT=${PROJECT_ROOT}/asic/data/production/harmonization_candidates/${HARMONIZATION_RUN_ID}
CANDIDATE_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/harmonization_dry_run/${HARMONIZATION_RUN_ID}
CANDIDATE_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/harmonization_dry_run/${HARMONIZATION_RUN_ID}.json
for required_path in \
    "${CANDIDATE_ROOT}/candidate_run_manifest.json" \
    "${CANDIDATE_PRIVATE}/harmonization_dry_run_manifest.json" \
    "${CANDIDATE_PRIVATE}/column_metrics.parquet" \
    "${CANDIDATE_PRIVATE}/unresolved_tokens.parquet" \
    "${CANDIDATE_PRIVATE}/categorical_values.parquet" \
    "${CANDIDATE_PRIVATE}/candidate_variable_dictionary.parquet" \
    "${CANDIDATE_PRIVATE}/occurrence_plan.parquet" \
    "${CANDIDATE_REVIEW}"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: consolidated audit evidence is incomplete" >&2
        exit 1
    fi
done
for hospital in asic_UK00 asic_UK01 asic_UK02 asic_UK03 asic_UK04 asic_UK06 asic_UK07 asic_UK08; do
    for table in static dynamic; do
        if [[ ! -f "${CANDIDATE_ROOT}/${hospital}/${table}.parquet" ]]; then
            echo "error: candidate output is incomplete for ${hospital} ${table}" >&2
            exit 1
        fi
        if [[ ! -f "${PROJECT_ROOT}/asic/data/production/ingested/${hospital}/${table}.parquet" ]]; then
            echo "error: ingested provenance source is incomplete for ${hospital} ${table}" >&2
            exit 1
        fi
    done
done

PRIVATE_OUTPUT=${PROJECT_ROOT}/asic/reports/production/private/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}
REVIEW_JSON=${PROJECT_ROOT}/asic/reports/production/review/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}.json
REVIEW_MARKDOWN=${PROJECT_ROOT}/asic/reports/production/review/consolidated_harmonization_audit/${CONSOLIDATED_AUDIT_RUN_ID}.md
for output_path in "${PRIVATE_OUTPUT}" "${REVIEW_JSON}" "${REVIEW_MARKDOWN}"; do
    if [[ -e "${output_path}" ]]; then
        echo "error: consolidated audit run ID already exists and will not be overwritten" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CONSOLIDATED_AUDIT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "consolidated_audit_run_id=${CONSOLIDATED_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,HARMONIZATION_RUN_ID="${HARMONIZATION_RUN_ID}",CONSOLIDATED_AUDIT_RUN_ID="${CONSOLIDATED_AUDIT_RUN_ID}",ASIC_CONSOLIDATED_AUDIT_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: consolidated audit worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-2}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-2}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK:-2}
export NUMEXPR_NUM_THREADS=${SLURM_CPUS_PER_TASK:-2}

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    audit-harmonization-candidate \
    --config "${CONFIG}" \
    --harmonization-run-id "${HARMONIZATION_RUN_ID}" \
    --run-id "${CONSOLIDATED_AUDIT_RUN_ID}"
audit_exit=$?
set -e

if [[ ${audit_exit} -eq 2 ]]; then
    echo "consolidated_harmonization_audit_completed_with_human_review_findings=true"
    exit 0
fi
exit "${audit_exit}"
