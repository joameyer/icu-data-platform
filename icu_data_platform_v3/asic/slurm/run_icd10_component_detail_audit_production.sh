#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-icd10-details
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/icd10-component-detail-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/icd10-component-detail-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_icd10_component_detail_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${ICD10_COMPONENT_AUDIT_RUN_ID:-}" ]]; then
    echo "error: set ICD10_COMPONENT_AUDIT_RUN_ID to the reviewed immutable component-audit run" >&2
    exit 1
fi
if [[ ! "${ICD10_COMPONENT_AUDIT_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid immutable ICD-10 component-audit run ID" >&2
    exit 1
fi
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
COMPONENT_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/icd10_component_audit/${ICD10_COMPONENT_AUDIT_RUN_ID}
COMPONENT_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/icd10_component_audit/${ICD10_COMPONENT_AUDIT_RUN_ID}.json
STATIC_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/static_contract_audit/20260805T112746Z
for required_path in \
    "${COMPONENT_PRIVATE}/icd10_component_audit_manifest.json" \
    "${COMPONENT_REVIEW}" \
    "${STATIC_PRIVATE}/static_contract_audit_manifest.json" \
    "${STATIC_PRIVATE}/occurrences.parquet" \
    "${PROJECT_ROOT}/asic/config/harmonization/reviewed_icd10_contract_0_1.yaml"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: immutable ICD-10 component-detail evidence is incomplete" >&2
        exit 1
    fi
done
for hospital in asic_UK00 asic_UK01 asic_UK02 asic_UK03 asic_UK04 asic_UK06 asic_UK07 asic_UK08; do
    for required_path in \
        "${PROJECT_ROOT}/asic/data/production/ingested/${hospital}/static.parquet" \
        "${PROJECT_ROOT}/asic/data/production/ingested/${hospital}/ingestion_manifest.json"; do
        if [[ ! -f "${required_path}" ]]; then
            echo "error: ingested static ICD-10 evidence is incomplete for ${hospital}" >&2
            exit 1
        fi
    done
done
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_ICD10_COMPONENT_DETAIL_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,ICD10_COMPONENT_AUDIT_RUN_ID="${ICD10_COMPONENT_AUDIT_RUN_ID}",ASIC_ICD10_COMPONENT_DETAIL_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: ICD-10 component-detail worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    audit-icd10-component-details \
    --config "${CONFIG}" \
    --icd10-component-audit-run-id "${ICD10_COMPONENT_AUDIT_RUN_ID}"
audit_exit=$?
set -e

if [[ ${audit_exit} -eq 2 ]]; then
    echo "icd10_component_detail_audit_completed_with_human_review_findings=true"
    exit 0
fi
exit "${audit_exit}"
