#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-clean02-promote
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaned-0-2-promote-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/cleaned-0-2-promote-%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_promote_cleaned_0_2_release_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
BUILD_RUN_ID=20260807T154100Z
AUDIT_RUN_ID=20260807T154100Z
RELEASE_ID=20260807T154100Z
PREVIOUS_RELEASE_ID=20260806T114234Z

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/cleaning/reviewed_cleaned_0_2_promotion_20260807T154100Z.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/reviewed_cleaning_policy_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_0_2_audit.yaml" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${BUILD_RUN_ID}/cleaning_0_2_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${BUILD_RUN_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${BUILD_RUN_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${BUILD_RUN_ID}/cleaned_variable_dictionary.parquet" \
    "${PROJECT_ROOT}/asic/data/production/cleaned_0_2_candidates/${BUILD_RUN_ID}/cleaned_variable_dictionary.md" \
    "${PROJECT_ROOT}/asic/reports/production/review/cleaning_0_2_audit/${AUDIT_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/cleaning_0_2_audit/${AUDIT_RUN_ID}/cleaning_0_2_audit_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${PREVIOUS_RELEASE_ID}/release_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required approved cleaned-0.2 promotion evidence is unavailable: ${required_path}" >&2
        exit 1
    fi
done

if [[ -e "${PROJECT_ROOT}/asic/data/production/cleaned/releases/${RELEASE_ID}" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/data/production/cleaned/releases/.${RELEASE_ID}.incomplete" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/reports/production/review/cleaned_0_2_promotion/${RELEASE_ID}.json" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/reports/production/review/cleaned_0_2_promotion/${RELEASE_ID}.md" ]]; then
    echo "error: cleaned-0.2 promotion output already exists; nothing will be overwritten" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CLEANED_0_2_PROMOTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "cleaned_0_2_release_id=${RELEASE_ID}"
    exec sbatch \
        --export=ALL,ASIC_CLEANED_0_2_PROMOTION_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: cleaned-0.2 promotion worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline promote-cleaned-0-2-release \
    --config "${CONFIG}"

echo "cleaned_0_2_promotion_completed=true"
echo "previous_cleaned_release_preserved=true"
echo "current_cleaned_release=${RELEASE_ID}"
echo "audited_parquet_and_dictionary_bytes_preserved=true"
echo "cleaned_layer_ready=true"
echo "cleaned_dictionary_ready=true"
echo "derived_input_approved=true"
echo "cleaning_rerun_during_promotion=false"
echo "derivation_applied=false"
echo "publication_ready=true"
echo "external_data_export_authorized=false"
