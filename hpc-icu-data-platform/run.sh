#!/bin/bash

# SLURM directives
# Submit this script from inside hpc-icu-data-platform/ with: sbatch run.sh
#SBATCH --job-name=asic_harmonization
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --output=logs/%x_%j.log
#SBATCH --error=logs/%x_%j.err
# #SBATCH --account=rwth1641

set -euo pipefail

# Define project root.
# In Slurm, the script may execute from a spool copy under /var/spool/slurm/,
# so prefer the submission directory or an explicit PROJECT_ROOT override.
project_root="home/am861154/projects/hpc-icu-data-platform"

# Save the full ASIC raw-data path here.
RAW_DIR="/hpcwork/jrc_combine/richard/asic_sftp/Kontrolldaten"

# Save the output location here.
OUTPUT_DIR="${project_root}/artifacts/asic_harmonized_full"

# Optional runtime settings.
FORMAT="csv"
MIN_NON_NULL=20
MIN_HOSPITALS=4
FENCE_FACTOR=1.5

# Optional: point this to your existing virtual environment.
VENV_PATH="/home/am861154/mypyenv"

# Create necessary directories.
mkdir -p "${project_root}/logs"
mkdir -p "${OUTPUT_DIR}"

if [ ! -d "${RAW_DIR}" ]; then
    echo "RAW_DIR does not exist: ${RAW_DIR}" >&2
    exit 1
fi

# Load modules needed for this CPU-only pipeline.
module purge
module load Python/3.9.6

# Activate Python environment if present.
if [ -f "${VENV_PATH}/bin/activate" ]; then
    source "${VENV_PATH}/bin/activate"
fi

# Navigate to the project root.
cd "${project_root}"

echo "[$(date)] Starting ASIC harmonization job"
echo "HOSTNAME: $(hostname)"
echo "PROJECT_ROOT: ${project_root}"
echo "RAW_DIR: ${RAW_DIR}"
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "FORMAT: ${FORMAT}"
echo "MIN_NON_NULL: ${MIN_NON_NULL}"
echo "MIN_HOSPITALS: ${MIN_HOSPITALS}"
echo "FENCE_FACTOR: ${FENCE_FACTOR}"

# Run the ASIC harmonization + generic 8h blocking pipeline.
python run_asic_harmonization.py \
    --raw-dir "${RAW_DIR}" \
    --output-dir "${OUTPUT_DIR}" \
    --format "${FORMAT}" \
    --min-non-null "${MIN_NON_NULL}" \
    --min-hospitals "${MIN_HOSPITALS}" \
    --fence-factor "${FENCE_FACTOR}"

echo "[$(date)] ASIC harmonization job finished"
