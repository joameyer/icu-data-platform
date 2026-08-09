#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-harmonized-promote
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-promote-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/harmonized-promote-%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_promote_harmonized_release_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
BUILD_RUN_ID=20260806T111156Z
AUDIT_RUN_ID=20260806T111156Z
RELEASE_ID=20260806T111156Z

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/data/production/harmonized_candidates/${BUILD_RUN_ID}/harmonized_build_manifest.json" \
    "${PROJECT_ROOT}/asic/reports/production/review/harmonized_build_audit/${AUDIT_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/harmonized_build_audit/${AUDIT_RUN_ID}/harmonized_build_audit_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required approved promotion evidence is unavailable: ${required_path}" >&2
        exit 1
    fi
done

if [[ -e "${PROJECT_ROOT}/asic/data/production/harmonized/releases/${RELEASE_ID}" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/data/production/harmonized/current_release.json" ]]; then
    echo "error: harmonized release target already exists; promotion will not overwrite it" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_HARMONIZED_PROMOTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "harmonized_release_id=${RELEASE_ID}"
    exec sbatch --export=ALL,ASIC_HARMONIZED_PROMOTION_WORKER=1 "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: harmonized promotion worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline promote-harmonized-release --config "${CONFIG}"

echo "harmonized_promotion_completed=true"
echo "harmonized_layer_ready=true"
echo "cleaning_input_approved=true"
echo "publication_ready=true"
echo "external_data_export_authorized=false"
