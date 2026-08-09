#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-cleaned-promote
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaned-promote-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaned-promote-%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_promote_cleaned_release_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CLEANING_RUN_ID=20260806T114234Z
AUDIT_RUN_ID=20260806T114234Z
RELEASE_ID=20260806T114234Z

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_policy.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/reviewed_cleaned_promotion_20260806T114234Z.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_candidates/${CLEANING_RUN_ID}/cleaning_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_candidates/${CLEANING_RUN_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_candidates/${CLEANING_RUN_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/review/cleaning_audit/${AUDIT_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/cleaning_audit/${AUDIT_RUN_ID}/cleaning_audit_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/harmonized/current_release.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required approved cleaned-promotion evidence is unavailable: ${required_path}" >&2
        exit 1
    fi
done

if [[ -e "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${RELEASE_ID}" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" ]]; then
    echo "error: cleaned release target already exists; promotion will not overwrite it" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CLEANED_PROMOTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "cleaned_release_id=${RELEASE_ID}"
    exec sbatch --export=ALL,ASIC_CLEANED_PROMOTION_WORKER=1 "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: cleaned promotion worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline promote-cleaned-release --config "${CONFIG}"

echo "cleaned_promotion_completed=true"
echo "cleaned_layer_ready=true"
echo "derived_input_approved=true"
echo "cleaning_rerun_during_promotion=false"
echo "derivation_applied=false"
echo "publication_ready=true"
echo "external_data_export_authorized=false"
