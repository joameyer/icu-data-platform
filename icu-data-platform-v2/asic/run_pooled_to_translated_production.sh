#!/usr/bin/env bash

# Submit from the repository root with:
#   mkdir -p asic/runs/production
#   sbatch asic/run_pooled_to_translated_production.sh
#
# The log directory must exist before submission because Slurm opens the log
# files before this script starts.
#SBATCH --job-name=asic_pooled_to_translated
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=asic/runs/production/%x_%j.log
#SBATCH --error=asic/runs/production/%x_%j.err
##SBATCH --account=rwth1641

set -euo pipefail

# Slurm may execute a spool copy of this file. Prefer an explicit PROJECT_ROOT,
# then the directory from which sbatch was called, and use the script location
# only as a non-Slurm fallback.
project_root="${PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}"
python_bin="${project_root}/.venv/bin/python"
config_path="${project_root}/asic/config/datasets/production.yaml"
pooled_dir="${project_root}/asic/data/production/pooled"
translated_dir="${project_root}/asic/data/production/translated"

if [[ ! -x "${python_bin}" ]]; then
    echo "Project virtual-environment Python is missing or not executable: ${python_bin}" >&2
    exit 1
fi

if [[ ! -f "${config_path}" ]]; then
    echo "Production dataset configuration is missing: ${config_path}" >&2
    exit 1
fi

for pooled_file in static.parquet dynamic.parquet; do
    if [[ ! -f "${pooled_dir}/${pooled_file}" ]]; then
        echo "Required pooled production input is missing: ${pooled_dir}/${pooled_file}" >&2
        exit 1
    fi
done

# Production replacement is intentionally not supported by this job. Existing
# outputs require a separate human-reviewed archive/replacement decision.
for output_file in static.parquet dynamic.parquet translation_manifest.json; do
    if [[ -e "${translated_dir}/${output_file}" ]]; then
        echo "Production translated output already exists: ${translated_dir}/${output_file}" >&2
        echo "The job will not overwrite it. Follow the reviewed rerun procedure in asic/README.md." >&2
        exit 1
    fi
done

cd "${project_root}"

echo "[$(date --iso-8601=seconds)] Starting ASIC pooled-to-translated production job"
echo "SLURM_JOB_ID: ${SLURM_JOB_ID:-not_running_under_slurm}"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_ROOT: ${project_root}"
echo "PYTHON: ${python_bin}"
echo "CONFIG: ${config_path}"
echo "POOLED_INPUT: ${pooled_dir}"
echo "TRANSLATED_OUTPUT: ${translated_dir}"
"${python_bin}" --version
df -h "${project_root}/asic/data/production"
du -sh "${pooled_dir}"

echo "[$(date --iso-8601=seconds)] Validating translation policy"
"${python_bin}" -m asic_pipeline validate-translation-policy \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] Auditing pooled production input"
"${python_bin}" -m asic_pipeline audit-pooled \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] Inventorying pooled production categories"
"${python_bin}" -m asic_pipeline inventory-pooled-categories \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] Building translated production artifacts"
"${python_bin}" -m asic_pipeline pooled-to-translated \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] ASIC pooled-to-translated production job finished"
echo "Manifest: ${translated_dir}/translation_manifest.json"
