#!/usr/bin/env bash
#SBATCH --job-name=asic-v3-ingest-all
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/ingest-all-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/asic/runs/production/ingest-all-%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
RAW_ROOT=/hpcwork/jrc_combine/richard/asic_2026
OUTPUT_ROOT=${PROJECT_ROOT}/asic/data/production/ingested
RUNS_ROOT=${PROJECT_ROOT}/asic/runs/production
JOB_SCRIPT=${PROJECT_ROOT}/asic/slurm/run_lossless_ingestion_all_production.sh
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
HOSPITALS=(
    asic_UK00
    asic_UK01
    asic_UK02
    asic_UK03
    asic_UK04
    asic_UK06
    asic_UK07
    asic_UK08
)

preflight_output_tree() {
    local hospital
    local hospital_output
    local -a staging_paths
    for hospital in "${HOSPITALS[@]}"; do
        hospital_output=${OUTPUT_ROOT}/${hospital}
        if [[ -e "${hospital_output}" || -L "${hospital_output}" ]]; then
            echo "error: existing hospital output blocks all-hospital ingestion: ${hospital}" >&2
            return 1
        fi
    done
    shopt -s nullglob
    staging_paths=("${OUTPUT_ROOT}"/.asic_UK*-ingestion-*)
    if (( ${#staging_paths[@]} > 0 )); then
        echo "error: incomplete ingestion staging directories require review" >&2
        return 1
    fi
}

if [[ -z "${INVENTORY_RUN_ID:-}" ]]; then
    echo "error: set INVENTORY_RUN_ID to the reviewed inventory 0.4 run" >&2
    exit 1
fi
if [[ ! "${INVENTORY_RUN_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$ ]]; then
    echo "error: invalid INVENTORY_RUN_ID" >&2
    exit 1
fi
if [[ ! -d "${PROJECT_ROOT}" || ! -d "${RAW_ROOT}" ]]; then
    echo "error: v3 project or authoritative raw root is unavailable" >&2
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
INVENTORY_DIRECTORY=${PROJECT_ROOT}/asic/reports/production/private/raw_inventory/${INVENTORY_RUN_ID}
INVENTORY_MANIFEST=${INVENTORY_DIRECTORY}/inventory_manifest.json
if [[ ! -f "${INVENTORY_MANIFEST}" ]]; then
    echo "error: reviewed private inventory manifest is unavailable" >&2
    exit 1
fi

preflight_output_tree
mkdir -p "${RUNS_ROOT}"
chmod 700 "${RUNS_ROOT}"

if [[ "${ASIC_INGESTION_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: run this script with bash from a login shell, not with sbatch" >&2
        exit 1
    fi
    if ! command -v sbatch >/dev/null 2>&1; then
        echo "error: sbatch is unavailable" >&2
        exit 1
    fi
    exec sbatch \
        --export=ALL,INVENTORY_RUN_ID="${INVENTORY_RUN_ID}",ASIC_INGESTION_WORKER=1 \
        "${JOB_SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: ingestion worker must run inside a Slurm allocation" >&2
    exit 1
fi

preflight_output_tree

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH=${PROJECT_ROOT}/src

for hospital in "${HOSPITALS[@]}"; do
    echo "ingestion_start hospital=${hospital}"
    "${PROJECT_ROOT}/.venv/bin/python" -m asic_pipeline \
        ingest-raw-hospital \
        --config "${CONFIG}" \
        --inventory-run-id "${INVENTORY_RUN_ID}" \
        --hospital "${hospital}"
    echo "ingestion_complete hospital=${hospital}"
done

echo "batch_ingestion_status=pass"
echo "hospital_count=${#HOSPITALS[@]}"
echo "publication_ready=false"
