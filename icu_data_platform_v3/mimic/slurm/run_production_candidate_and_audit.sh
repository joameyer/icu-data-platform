#!/usr/bin/env bash
#SBATCH --job-name=mimic-v3-layer
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/mimic/runs/production/mimic-layer-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/mimic/runs/production/mimic-layer-%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=2

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
SOURCE_ROOT=/hpcwork/jrc_combine/joana/mimic/data
CONFIG=${PROJECT_ROOT}/mimic/config/production_candidate_0_1.yaml
SCRIPT=${PROJECT_ROOT}/mimic/slurm/run_production_candidate_and_audit.sh
RUNS_ROOT=${PROJECT_ROOT}/mimic/runs/production
CANDIDATE_ROOT=${PROJECT_ROOT}/mimic/data/production_candidates
AUDIT_ROOT=${PROJECT_ROOT}/mimic/data/production_audits

if [[ -z "${MIMIC_CANDIDATE_RUN_ID:-}" ]]; then
    echo "error: set MIMIC_CANDIDATE_RUN_ID to YYYYMMDDTHHMMSSZ" >&2
    exit 1
fi
if [[ ! "${MIMIC_CANDIDATE_RUN_ID}" =~ ^[0-9]{8}T[0-9]{6}Z(-[a-z0-9][a-z0-9_-]*)?$ ]]; then
    echo "error: invalid MIMIC_CANDIDATE_RUN_ID" >&2
    exit 1
fi

for required_file in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/src/mimic_iv_pipeline/production.py" \
    "${PROJECT_ROOT}/.venv/bin/python" \
    "${SOURCE_ROOT}/hosp/patients.csv.gz" \
    "${SOURCE_ROOT}/hosp/admissions.csv.gz" \
    "${SOURCE_ROOT}/hosp/services.csv.gz" \
    "${SOURCE_ROOT}/hosp/diagnoses_icd.csv.gz" \
    "${SOURCE_ROOT}/hosp/d_labitems.csv.gz" \
    "${SOURCE_ROOT}/hosp/labevents.csv.gz" \
    "${SOURCE_ROOT}/icu/icustays.csv.gz" \
    "${SOURCE_ROOT}/icu/chartevents.csv.gz"; do
    if [[ ! -f "${required_file}" ]]; then
        echo "error: required input is missing: ${required_file}" >&2
        exit 1
    fi
done

for prohibited_path in \
    "${CANDIDATE_ROOT}/${MIMIC_CANDIDATE_RUN_ID}" \
    "${CANDIDATE_ROOT}/.${MIMIC_CANDIDATE_RUN_ID}.staging" \
    "${AUDIT_ROOT}/${MIMIC_CANDIDATE_RUN_ID}"; do
    if [[ -e "${prohibited_path}" ]]; then
        echo "error: immutable run-scoped path already exists: ${prohibited_path}" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}" "${CANDIDATE_ROOT}" "${AUDIT_ROOT}"
chmod 700 "${RUNS_ROOT}" "${CANDIDATE_ROOT}" "${AUDIT_ROOT}"

if [[ "${MIMIC_LAYER_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: launch this wrapper with bash from a login shell" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "mimic_candidate_run_id=${MIMIC_CANDIDATE_RUN_ID}"
    exec sbatch \
        --export=ALL,MIMIC_CANDIDATE_RUN_ID="${MIMIC_CANDIDATE_RUN_ID}",MIMIC_LAYER_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: worker phase requires a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${PROJECT_ROOT}/src"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
"${PROJECT_ROOT}/.venv/bin/python" -m mimic_iv_pipeline.cli \
    build-production-candidate \
    --config "${CONFIG}" \
    --run-id "${MIMIC_CANDIDATE_RUN_ID}"

"${PROJECT_ROOT}/.venv/bin/python" -m mimic_iv_pipeline.cli \
    audit-production-candidate \
    --config "${CONFIG}" \
    --run-id "${MIMIC_CANDIDATE_RUN_ID}"

echo "mimic_candidate_and_audit_completed=true"
echo "run_id=${MIMIC_CANDIDATE_RUN_ID}"
echo "production_source_modified=false"
echo "release_created=false"
echo "pointer_modified=false"
echo "row_level_data_exported=false"
echo "model_trained=false"
