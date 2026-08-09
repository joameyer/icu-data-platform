#!/usr/bin/env bash

# Submit from the repository root with:
#   mkdir -p asic/runs/production
#   sbatch asic/run_cross_hospital_audit_production.sh
#
# This job is read-only with respect to all data artifacts. It writes only
# aggregate JSON audit reports and Slurm logs.
#SBATCH --job-name=asic_cross_hospital_qc
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --output=asic/runs/production/%x_%j.log
#SBATCH --error=asic/runs/production/%x_%j.err
##SBATCH --account=rwth1641

set -euo pipefail

project_root="${PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}"
python_bin="${project_root}/.venv/bin/python"
config_path="${project_root}/asic/config/datasets/production.yaml"
translated_dir="${project_root}/asic/data/production/translated"

if [[ ! -x "${python_bin}" ]]; then
    echo "Project virtual-environment Python is missing or not executable: ${python_bin}" >&2
    exit 1
fi

if [[ ! -f "${config_path}" ]]; then
    echo "Production dataset configuration is missing: ${config_path}" >&2
    exit 1
fi

for input_file in static.parquet dynamic.parquet translation_manifest.json; do
    if [[ ! -f "${translated_dir}/${input_file}" ]]; then
        echo "Required translated production input is missing: ${translated_dir}/${input_file}" >&2
        exit 1
    fi
done

cd "${project_root}"

echo "[$(date --iso-8601=seconds)] Starting ASIC cross-hospital data-quality audit"
echo "SLURM_JOB_ID: ${SLURM_JOB_ID:-not_running_under_slurm}"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_ROOT: ${project_root}"
echo "PYTHON: ${python_bin}"
echo "CONFIG: ${config_path}"
echo "TRANSLATED_INPUT: ${translated_dir}"
"${python_bin}" --version
df -h "${project_root}/asic/data/production"
du -sh "${translated_dir}"

echo "[$(date --iso-8601=seconds)] Validating translated structure and provenance"
"${python_bin}" -m asic_pipeline audit-translated \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] Profiling cross-hospital data quality"
"${python_bin}" -m asic_pipeline audit-cross-hospital \
    --config "${config_path}"

echo "[$(date --iso-8601=seconds)] ASIC cross-hospital data-quality audit finished"
echo "Review the newest cross_hospital_data_quality_audit report under ${project_root}/asic/reports/production/"

