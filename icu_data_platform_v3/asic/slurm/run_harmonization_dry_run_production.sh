#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-harmonize-candidate
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonize-candidate-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonize-candidate-%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=4

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_harmonization_dry_run_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml

if [[ -z "${REGISTRY_REVIEW_RUN_ID:-}" ]]; then
    echo "error: set REGISTRY_REVIEW_RUN_ID to the reviewed immutable registry-review run" >&2
    exit 1
fi
if [[ -z "${INGESTION_AUDIT_RUN_ID:-}" ]]; then
    echo "error: set INGESTION_AUDIT_RUN_ID to the immutable post-ingestion audit run" >&2
    exit 1
fi
if [[ -z "${HARMONIZATION_RUN_ID:-}" ]]; then
    HARMONIZATION_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
for run_id in \
    "${REGISTRY_REVIEW_RUN_ID}" \
    "${INGESTION_AUDIT_RUN_ID}" \
    "${HARMONIZATION_RUN_ID}"; do
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

REGISTRY_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}
REGISTRY_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/harmonization_registry_review/${REGISTRY_REVIEW_RUN_ID}.json
INGESTION_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/ingestion_audit/${INGESTION_AUDIT_RUN_ID}/ingestion_audit_manifest.json
INGESTION_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/ingestion_audit/${INGESTION_AUDIT_RUN_ID}.json
COMPOSITE_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/composite_source_audit/20260805T111026Z.json
ICD10_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/icd10_component_detail_audit/20260805T135859Z/examples.parquet
for required_path in \
    "${REGISTRY_PRIVATE}/harmonization_registry_review_manifest.json" \
    "${REGISTRY_PRIVATE}/occurrences.parquet" \
    "${REGISTRY_PRIVATE}/variables.parquet" \
    "${REGISTRY_REVIEW}" \
    "${INGESTION_PRIVATE}" \
    "${INGESTION_REVIEW}" \
    "${COMPOSITE_REVIEW}" \
    "${ICD10_PRIVATE}"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: immutable harmonization input evidence is incomplete" >&2
        exit 1
    fi
done
for hospital in asic_UK00 asic_UK01 asic_UK02 asic_UK03 asic_UK04 asic_UK06 asic_UK07 asic_UK08; do
    hospital_root=${PROJECT_ROOT}/asic/data/production/ingested/${hospital}
    for required_path in \
        "${hospital_root}/ingestion_manifest.json" \
        "${hospital_root}/static.parquet" \
        "${hospital_root}/dynamic.parquet"; do
        if [[ ! -f "${required_path}" ]]; then
            echo "error: ingested hospital input is incomplete for ${hospital}" >&2
            exit 1
        fi
    done
done

CANDIDATE_ROOT=${PROJECT_ROOT}/asic/data/production/harmonization_candidates/${HARMONIZATION_RUN_ID}
CANDIDATE_STAGING=${PROJECT_ROOT}/asic/data/production/harmonization_candidates/.${HARMONIZATION_RUN_ID}.incomplete
PRIVATE_OUTPUT=${PROJECT_ROOT}/asic/reports/production/private/harmonization_dry_run/${HARMONIZATION_RUN_ID}
REVIEW_JSON=${PROJECT_ROOT}/asic/reports/production/review/harmonization_dry_run/${HARMONIZATION_RUN_ID}.json
REVIEW_MARKDOWN=${PROJECT_ROOT}/asic/reports/production/review/harmonization_dry_run/${HARMONIZATION_RUN_ID}.md
for output_path in \
    "${CANDIDATE_ROOT}" \
    "${CANDIDATE_STAGING}" \
    "${PRIVATE_OUTPUT}" \
    "${REVIEW_JSON}" \
    "${REVIEW_MARKDOWN}"; do
    if [[ -e "${output_path}" ]]; then
        echo "error: harmonization run ID already exists and will not be overwritten" >&2
        exit 1
    fi
done

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_HARMONIZATION_DRY_RUN_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "harmonization_run_id=${HARMONIZATION_RUN_ID}"
    exec sbatch \
        --export=ALL,REGISTRY_REVIEW_RUN_ID="${REGISTRY_REVIEW_RUN_ID}",INGESTION_AUDIT_RUN_ID="${INGESTION_AUDIT_RUN_ID}",HARMONIZATION_RUN_ID="${HARMONIZATION_RUN_ID}",ASIC_HARMONIZATION_DRY_RUN_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: harmonization worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

set +e
"${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
    harmonize-candidate-dry-run \
    --config "${CONFIG}" \
    --registry-review-run-id "${REGISTRY_REVIEW_RUN_ID}" \
    --ingestion-audit-run-id "${INGESTION_AUDIT_RUN_ID}" \
    --run-id "${HARMONIZATION_RUN_ID}"
harmonization_exit=$?
set -e

if [[ ${harmonization_exit} -eq 2 ]]; then
    echo "harmonization_dry_run_completed_with_human_review_findings=true"
    exit 0
fi
exit "${harmonization_exit}"
