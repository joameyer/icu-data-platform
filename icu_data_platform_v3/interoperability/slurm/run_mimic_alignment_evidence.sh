#!/usr/bin/env bash
#SBATCH --job-name=mimic-align-audit
#SBATCH --partition=c23ms
#SBATCH --output=/hpcwork/jrc_combine/joana/icu_data_platform_v3/interoperability/runs/mimic-align-%j.out
#SBATCH --error=/hpcwork/jrc_combine/joana/icu_data_platform_v3/interoperability/runs/mimic-align-%j.err
#SBATCH --time=02:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
SOURCE_ROOT=/hpcwork/jrc_combine/joana/mimic/data
SCRIPT=${PROJECT_ROOT}/interoperability/slurm/run_mimic_alignment_evidence.sh
RUNS_ROOT=${PROJECT_ROOT}/interoperability/runs
REPORT_ROOT=${PROJECT_ROOT}/interoperability/reports

if [[ -z "${MIMIC_ALIGNMENT_RUN_ID:-}" ]]; then
    echo "error: set MIMIC_ALIGNMENT_RUN_ID to YYYYMMDDTHHMMSSZ" >&2
    exit 1
fi
if [[ ! "${MIMIC_ALIGNMENT_RUN_ID}" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
    echo "error: invalid MIMIC_ALIGNMENT_RUN_ID" >&2
    exit 1
fi

for required_file in \
    "${PROJECT_ROOT}/.venv/bin/python" \
    "${PROJECT_ROOT}/interoperability/config/mimic_source_mappings_0_1.csv" \
    "${SOURCE_ROOT}/icu/d_items.csv.gz" \
    "${SOURCE_ROOT}/hosp/d_labitems.csv.gz" \
    "${SOURCE_ROOT}/icu/chartevents.csv.gz" \
    "${SOURCE_ROOT}/hosp/labevents.csv.gz"; do
    if [[ ! -f "${required_file}" ]]; then
        echo "error: required input is missing: ${required_file}" >&2
        exit 1
    fi
done

PRIVATE_OUTPUT=${REPORT_ROOT}/private/${MIMIC_ALIGNMENT_RUN_ID}/aggregate_evidence.json
PUBLIC_OUTPUT=${REPORT_ROOT}/review/${MIMIC_ALIGNMENT_RUN_ID}/aggregate_summary.json
if [[ -e "${PRIVATE_OUTPUT}" || -e "${PUBLIC_OUTPUT}" ]]; then
    echo "error: immutable run-scoped report already exists" >&2
    exit 1
fi

mkdir -p "${RUNS_ROOT}" "$(dirname "${PRIVATE_OUTPUT}")" "$(dirname "${PUBLIC_OUTPUT}")"
chmod 700 "${RUNS_ROOT}" "${REPORT_ROOT}" "${REPORT_ROOT}/private" "${REPORT_ROOT}/review" \
    "$(dirname "${PRIVATE_OUTPUT}")" "$(dirname "${PUBLIC_OUTPUT}")"

if [[ "${MIMIC_ALIGNMENT_WORKER:-0}" != "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        echo "error: launch this wrapper with bash from a login shell" >&2
        exit 1
    fi
    echo "mimic_alignment_run_id=${MIMIC_ALIGNMENT_RUN_ID}"
    exec sbatch \
        --export=ALL,MIMIC_ALIGNMENT_RUN_ID="${MIMIC_ALIGNMENT_RUN_ID}",MIMIC_ALIGNMENT_WORKER=1 \
        "${SCRIPT}"
fi

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "error: worker phase requires a Slurm allocation" >&2
    exit 1
fi

cd "${PROJECT_ROOT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${PROJECT_ROOT}/src"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
"${PROJECT_ROOT}/.venv/bin/python" -m interoperability_catalog.cli audit-mimic-evidence \
    --project-root "${PROJECT_ROOT}" \
    --mimic-root "${SOURCE_ROOT}" \
    --private-output "${PRIVATE_OUTPUT}" \
    --public-output "${PUBLIC_OUTPUT}"

echo "production_source_modified=false"
echo "profile_expanded=false"
echo "release_created=false"
echo "pointer_modified=false"
echo "row_level_data_exported=false"
