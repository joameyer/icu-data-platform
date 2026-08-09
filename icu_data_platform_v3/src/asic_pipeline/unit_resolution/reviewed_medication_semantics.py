from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError


EXPECTED_EVIDENCE = {
    "audit_artifact": "asic_v3_medication_value_semantics_review",
    "audit_artifact_version": "0.1",
    "audit_generated_at_utc": "2026-08-07T09:35:48+00:00",
    "input_cleaned_release_id": "20260806T114234Z",
    "dynamic_rows_scanned": 24_069_379,
    "source_medication_variable_count": 25,
    "hospital_variable_profile_count": 200,
    "explicit_zero_count": 5_013_108,
    "positive_value_count": 19_034_792,
    "negative_value_count": 201,
    "nonfinite_value_count": 0,
    "null_count": 577_686_374,
    "variables_with_explicit_zero_count": 24,
    "all_missing_hospital_variable_profile_count": 105,
    "zero_and_positive_profile_count": 71,
    "medication_conversion_scope_count": 4,
    "semantic_split_scope_count": 9,
    "projected_zero_to_null_count": 0,
    "projected_null_to_zero_count": 0,
}

EXPECTED_NEGATIVE_EVIDENCE = {
    "affected_variable_count": 1,
    "affected_hospital_count": 1,
    "affected_variable": "inhaled_no",
    "affected_hospital": "asic_UK08",
    "negative_value_count": 201,
    "distinct_negative_values": [
        {"value": -1.8, "count": 11},
        {"value": -1.72, "count": 1},
        {"value": -1.7, "count": 96},
        {"value": -0.5, "count": 50},
        {"value": -0.4, "count": 32},
        {"value": -0.15, "count": 1},
        {"value": -0.1, "count": 10},
    ],
    "classified_as_single_missing_sentinel": False,
    "deterministic_recovery_available": False,
}

EXPECTED_VALUE_SEMANTICS = {
    "explicit_numeric_zero": "preserve_as_observed_zero",
    "null": "preserve_as_unavailable_or_unobserved",
    "null_implies_inactive_medication": False,
    "null_to_zero_imputation": "prohibited",
    "zero_to_null_conversion": "prohibited",
    "positive_exposure_candidate": "value_greater_than_zero",
    "carry_forward_or_interval_imputation": (
        "requires_separate_reviewed_contract"
    ),
}

EXPECTED_NEGATIVE_RULE = {
    "rule_id": "mask_negative_medication_or_therapy_value",
    "selection": "dictionary_medication_or_therapy_variable_true",
    "supported_physical_type": "float64",
    "finite_negative_condition": "value_less_than_zero",
    "action": "set_null_and_count_by_hospital_and_variable",
    "layer": "cleaned",
    "apply_after_hospital_harmonization_and_semantic_splitting": True,
    "preserve_negative_in_ingested_layer": True,
    "preserve_negative_in_harmonized_layer": True,
    "preserve_zero": True,
    "preserve_positive": True,
    "preserve_null": True,
    "take_absolute_value": False,
    "clip_negative_to_zero": False,
    "future_signed_variable_requires_explicit_exemption": True,
    "expected_current_mask_count": 201,
}

EXPECTED_BOUNDARY = {
    "authorize_schema_dictionary_0_2_planning": True,
    "authorize_schema_dictionary_0_2_freeze": False,
    "authorize_cleaning_policy_0_2_implementation": True,
    "activate_cleaning_policy_0_2": False,
    "modify_existing_harmonized_release": False,
    "modify_existing_cleaned_release": False,
    "modify_existing_derived_release": False,
    "write_clinical_data": False,
    "filter_rows_or_stays": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class ReviewedMedicationSemantics:
    version: str
    evidence: dict[str, Any]
    negative_evidence: dict[str, Any]
    value_semantics: dict[str, Any]
    negative_cleaning_rule: dict[str, Any]
    source_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def load_reviewed_medication_semantics(
    path: str | Path,
) -> ReviewedMedicationSemantics:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed medication value semantics")
    if (
        raw.get("reviewed_medication_value_semantics_version") != "0.2"
        or raw.get("status")
        != "approved_pending_schema_dictionary_0_2_freeze"
    ):
        raise ConfigurationError("Reviewed medication semantics status is invalid")
    evidence = _mapping(raw.get("evidence"), "evidence")
    negative = _mapping(raw.get("negative_value_evidence"), "negative_value_evidence")
    value_semantics = _mapping(raw.get("value_semantics"), "value_semantics")
    negative_rule = _mapping(
        raw.get("general_negative_cleaning_rule"),
        "general_negative_cleaning_rule",
    )
    if evidence != EXPECTED_EVIDENCE:
        raise ConfigurationError("Medication audit evidence changed")
    if negative != EXPECTED_NEGATIVE_EVIDENCE:
        raise ConfigurationError("Negative medication evidence changed")
    if value_semantics != EXPECTED_VALUE_SEMANTICS:
        raise ConfigurationError("Medication zero/null semantics changed")
    if negative_rule != EXPECTED_NEGATIVE_RULE:
        raise ConfigurationError("General negative medication rule changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Reviewed medication semantics boundary changed")
    human = _mapping(raw.get("human_decision"), "human_decision")
    if human.get("decision_date") != "2026-08-07":
        raise ConfigurationError("Medication rule approval date changed")
    required_string(human, "decision", "human_decision")
    required_string(human, "approval_record", "human_decision")
    if (
        sum(row["count"] for row in negative["distinct_negative_values"])
        != negative["negative_value_count"]
        or evidence["negative_value_count"] != negative["negative_value_count"]
        or (
            evidence["explicit_zero_count"]
            + evidence["positive_value_count"]
            + evidence["negative_value_count"]
            + evidence["nonfinite_value_count"]
            + evidence["null_count"]
        )
        != evidence["dynamic_rows_scanned"]
        * evidence["source_medication_variable_count"]
    ):
        raise ConfigurationError("Medication evidence accounting is invalid")
    return ReviewedMedicationSemantics(
        version="0.2",
        evidence=evidence,
        negative_evidence=negative,
        value_semantics=value_semantics,
        negative_cleaning_rule=negative_rule,
        source_path=source,
    )
