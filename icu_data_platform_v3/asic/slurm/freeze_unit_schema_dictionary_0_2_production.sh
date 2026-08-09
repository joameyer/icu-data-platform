#!/usr/bin/env bash

set -euo pipefail
umask 077

PROJECT_ROOT=/hpcwork/jrc_combine/joana/icu_data_platform_v3
CONFIG=${PROJECT_ROOT}/asic/config/datasets/production.yaml
SOURCE_REVIEW_RUN_ID=20260807T101321Z
SOURCE_PRIVATE=${PROJECT_ROOT}/asic/reports/production/private/unit_schema_dictionary_amendment/${SOURCE_REVIEW_RUN_ID}
SOURCE_REVIEW=${PROJECT_ROOT}/asic/reports/production/review/unit_schema_dictionary_amendment/${SOURCE_REVIEW_RUN_ID}.json
CONTRACT_0_1=${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.1
CONTRACT_0_2=${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/0.2
INCOMPLETE_0_2=${PROJECT_ROOT}/asic/data/production/contracts/harmonized_schema_dictionary/.0.2.incomplete
FREEZE_REVIEW_JSON=${PROJECT_ROOT}/asic/reports/production/review/schema_dictionary_freeze/0.2.json
FREEZE_REVIEW_MD=${PROJECT_ROOT}/asic/reports/production/review/schema_dictionary_freeze/0.2.md

if [[ -n "${SLURM_JOB_ID:-}" || -n "${SLURM_STEP_ID:-}" ]]; then
    echo "error: run this metadata-only freeze with bash from a login frontend" >&2
    exit 1
fi

for required_path in \
    "${CONFIG}" \
    "${PROJECT_ROOT}/.venv/bin/asic-pipeline" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_schema_dictionary_freeze_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/schema_dictionary_amendment.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml" \
    "${PROJECT_ROOT}/asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml" \
    "${SOURCE_REVIEW}" \
    "${SOURCE_PRIVATE}/schema_amendment_manifest.json" \
    "${SOURCE_PRIVATE}/proposed_static_schema.json" \
    "${SOURCE_PRIVATE}/proposed_dynamic_schema.json" \
    "${SOURCE_PRIVATE}/proposed_variable_dictionary.parquet" \
    "${CONTRACT_0_1}/freeze_manifest.json"; do
    if [[ ! -f "${required_path}" ]]; then
        echo "error: required immutable contract-0.2 freeze input is unavailable: ${required_path}" >&2
        exit 1
    fi
done

for output_path in \
    "${CONTRACT_0_2}" \
    "${INCOMPLETE_0_2}" \
    "${FREEZE_REVIEW_JSON}" \
    "${FREEZE_REVIEW_MD}"; do
    if [[ -e "${output_path}" ]]; then
        echo "error: contract-0.2 freeze output already exists and will not be overwritten: ${output_path}" >&2
        exit 1
    fi
done

cd "${PROJECT_ROOT}"
exec .venv/bin/asic-pipeline freeze-unit-schema-dictionary --config "${CONFIG}"
