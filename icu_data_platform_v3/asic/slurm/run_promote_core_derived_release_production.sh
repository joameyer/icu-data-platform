#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-derived-promote
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-promote-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/derived-promote-%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
SCRIPT=${PROJECT_ROOT}/asic/slurm/run_promote_core_derived_release_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
DERIVED_RUN_ID=20260806T170134Z
AUDIT_RUN_ID=20260806T170134Z
RELEASE_ID=20260806T170134Z

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/derivation/reviewed_core_derived_contract_0_1.yaml" \
    "${PROJECT_ROOT}/asic/config/derivation/reviewed_core_derived_promotion_20260806T170134Z.yaml" \
    "${PROJECT_ROOT}/asic/data/production/derived_candidates/${DERIVED_RUN_ID}/derived_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/derived_candidates/${DERIVED_RUN_ID}/static.parquet" \
    "${PROJECT_ROOT}/asic/data/production/derived_candidates/${DERIVED_RUN_ID}/dynamic.parquet" \
    "${PROJECT_ROOT}/asic/reports/production/review/derived_audit/${AUDIT_RUN_ID}.json" \
    "${PROJECT_ROOT}/asic/reports/production/private/derived_audit/${AUDIT_RUN_ID}/derived_audit_manifest.json" \
    "${PROJECT_ROOT}/asic/data/production/cleaned/current_release.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required approved core-derived promotion evidence is unavailable: ${required_path}" >&2
        exit 1
    fi
done

if [[ -e "${PROJECT_ROOT}/asic/data/production/derived/releases/${RELEASE_ID}" ]] \
    || [[ -e "${PROJECT_ROOT}/asic/data/production/derived/current_release.json" ]]; then
    echo "error: core-derived release target already exists; promotion will not overwrite it" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_CORE_DERIVED_PROMOTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    echo "core_derived_release_id=${RELEASE_ID}"
    exec sbatch --export=ALL,ASIC_CORE_DERIVED_PROMOTION_WORKER=1 "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: core-derived promotion worker must run inside a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
.venv/bin/asic-pipeline promote-core-derived-release --config "${CONFIG}"

echo "core_derived_promotion_completed=true"
echo "core_derived_layer_ready=true"
echo "analysis_input_approved=true"
echo "derivation_rerun_during_promotion=false"
echo "cleaning_rerun_during_promotion=false"
echo "cohort_generated=false"
echo "time_blocking_applied=false"
echo "external_data_export_authorized=false"
