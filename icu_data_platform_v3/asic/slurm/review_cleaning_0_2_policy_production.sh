#!/usr/bin/env bash

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
CURRENT_RELEASE=${PROJECT_ROOT}/asic/data/production/harmonized/current_release.json
RELEASE_MANIFEST=${PROJECT_ROOT}/asic/data/production/harmonized/releases/20260807T112402Z/release_manifest.json
CONTRACT_MANIFEST=${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2/freeze_manifest.json

if [[ -n "${SLURM_JOB_ID:-}" || -n "${SLURM_STEP_ID:-}" ]]; then
    echo "error: run this metadata-only review with bash from a login frontend" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_policy_0_2_review.yaml" \
    "${PROJECT_ROOT}/asic/config/cleaning/cleaning_policy.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${CURRENT_RELEASE}" \
    "${RELEASE_MANIFEST}" \
    "${CONTRACT_MANIFEST}"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable cleaning-policy review input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

cd "${PROJECT_ROOT}"
set +e
.venv/bin/asic-pipeline review-cleaning-0-2-policy --config "${CONFIG}"
status=$?
set -e

if [[ ${status} -ne 2 ]]; then
    echo "error: expected a technically passing review with one human approval gate; observed exit ${status}" >&2
    exit 1
fi

echo "cleaning_0_2_policy_review_completed=true"
echo "clinical_rows_read=0"
echo "clinical_data_written=false"
echo "cleaned_candidate_generated=false"
echo "publication_ready=false"
