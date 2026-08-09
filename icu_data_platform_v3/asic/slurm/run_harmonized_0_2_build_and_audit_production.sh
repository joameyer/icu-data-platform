#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-harm02
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-0-2-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-0-2-%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_harmonized_0_2_build_and_audit_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
SOURCE_HARMONIZED_RELEASE_ID=20260806T111156Z
STATIC_WEIGHT_CLEANED_RELEASE_ID=20260806T114234Z

if [[ -z "${HARMONIZED_0_2_RUN_ID:-}" ]]; then
    HARMONIZED_0_2_RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
fi
if [[ -z "${HARMONIZED_0_2_AUDIT_RUN_ID:-}" ]]; then
    HARMONIZED_0_2_AUDIT_RUN_ID=${HARMONIZED_0_2_RUN_ID}
fi
for run_id in "${HARMONIZED_0_2_RUN_ID}" "${HARMONIZED_0_2_AUDIT_RUN_ID}"; do
    if [[ ! "${run_id}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
        echo "error: invalid immutable harmonized-0.2 run ID" >&2
        exit 1
    fi
done

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/harmonized_0_2_build.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/harmonized_0_2_audit.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/freeze_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/static_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/dynamic_schema.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${SOURCE_HARMONIZED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${SOURCE_HARMONIZED_RELEASE_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${SOURCE_HARMONIZED_RELEASE_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${STATIC_WEIGHT_CLEANED_RELEASE_ID}/release_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${STATIC_WEIGHT_CLEANED_RELEASE_ID}/static.parquet"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable harmonized-0.2 input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

CANDIDATE_DIR=${PROJECT_ROOT}/asic/data/production/harmonized_0_2_candidates/${HARMONIZED_0_2_RUN_ID}
INCOMPLETE_DIR=${PROJECT_ROOT}/asic/data/production/harmonized_0_2_candidates/.${HARMONIZED_0_2_RUN_ID}.incomplete
if [[ -e "${CANDIDATE_DIR}" || -e "${INCOMPLETE_DIR}" ]]; then
    echo "error: harmonized-0.2 run output already exists" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_HARMONIZED_0_2_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "harmonized_0_2_run_id=${HARMONIZED_0_2_RUN_ID}"
    echo "harmonized_0_2_audit_run_id=${HARMONIZED_0_2_AUDIT_RUN_ID}"
    exec sbatch \
        --export=ALL,HARMONIZED_0_2_RUN_ID="${HARMONIZED_0_2_RUN_ID}",HARMONIZED_0_2_AUDIT_RUN_ID="${HARMONIZED_0_2_AUDIT_RUN_ID}",ASIC_HARMONIZED_0_2_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: harmonized-0.2 worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline build-harmonized-0-2-candidate \
    --config "${CONFIG}" \
    --run-id "${HARMONIZED_0_2_RUN_ID}"

set +e
.venv/bin/asic-pipeline audit-harmonized-0-2-candidate \
    --config "${CONFIG}" \
    --build-run-id "${HARMONIZED_0_2_RUN_ID}" \
    --run-id "${HARMONIZED_0_2_AUDIT_RUN_ID}"
audit_status=$?
set -e
if [[ "${audit_status}" -ne 2 ]]; then
    echo "error: expected technically passing audit pending human approval (exit 2); observed ${audit_status}" >&2
    exit 1
fi

echo "harmonized_0_2_build_and_audit_completed=true"
echo "source_harmonized_release_modified=false"
echo "cleaned_release_modified=false"
echo "cleaning_applied=false"
echo "derivation_applied=false"
echo "publication_ready=false"
