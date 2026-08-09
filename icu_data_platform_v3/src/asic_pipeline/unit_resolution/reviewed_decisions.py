from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN
from asic_pipeline.unit_resolution.decisions import (
    CandidateUnitDecisions,
    load_candidate_unit_decisions,
)


EXPECTED_ACTION_IDS = frozenset(
    {
        "UK08-ALBUMIN-MG-L-TO-DG-L",
        "UK03-D-DIMER-UG-ML-TO-NG-ML",
        "UK08-D-DIMER-UG-ML-TO-NG-ML",
        "UK03-TROPONIN-NG-L-TO-NG-ML",
        "UK08-UREA-BUN-MG-DL-TO-MMOL-L",
        "UK08-ISOFLURANE-HUNDREDTHS-TO-VOL-PERCENT",
        "UK03-FUROSEMIDE-MG-DAY-TO-MG-H",
        "UK08-VASOPRESSIN-IU-H-TO-IU-MIN",
        "UK00-VASOPRESSIN-IU-KG-MIN-TO-IU-MIN",
    }
)

EXPECTED_SPLITS = {
    ("asic_UK08", "clonidine_iv_cont"): (
        "clonidine_iv_cont_weight_normalized",
        "ug_per_kg_per_h",
        "eligible",
        "moderate",
    ),
    ("asic_UK03", "epinephrine_iv_cont"): (
        "epinephrine_iv_cont_absolute",
        "ug_per_min",
        "eligible",
        "moderate",
    ),
    ("asic_UK08", "hydrocortisone_iv_bolus"): (
        "hydrocortisone_iv_bolus_source_uk08",
        "unresolved_source_scale",
        "ineligible_pending_definition",
        "unresolved",
    ),
    ("asic_UK08", "ketanest_iv_cont"): (
        "ketanest_iv_cont_weight_normalized",
        "mg_per_kg_per_h",
        "eligible",
        "moderate",
    ),
    ("asic_UK08", "morphine_iv_cont"): (
        "morphine_iv_cont_weight_normalized",
        "ug_per_kg_per_h",
        "eligible",
        "moderate",
    ),
    ("asic_UK03", "norepinephrine_iv_cont"): (
        "norepinephrine_iv_cont_absolute",
        "ug_per_min",
        "eligible",
        "moderate",
    ),
    ("asic_UK02", "prednisolone_iv_bolus"): (
        "prednisolone_iv_bolus_source_uk02",
        "unresolved_source_scale",
        "ineligible_pending_definition",
        "unresolved",
    ),
    ("asic_UK08", "propofol_iv_cont"): (
        "propofol_iv_cont_weight_normalized",
        "mg_per_kg_per_h",
        "eligible",
        "moderate",
    ),
    ("asic_UK00", "sufentanil_iv_cont"): (
        "sufentanil_iv_cont_source_uk00",
        "unresolved_source_scale",
        "ineligible_pending_definition",
        "unresolved",
    ),
}

EXPECTED_EXECUTION_CONTRACT = {
    "route_source_value_unchanged_to_parallel_target": True,
    "canonical_variable_missing_for_split_hospital": True,
    "preserve_zero_values": True,
    "preserve_null_values_as_null": True,
    "cleaning_mask_split_source_values": False,
    "retain_parallel_variables_in_cleaned_layer": True,
    "allow_automatic_coalescence_between_representations": False,
    "allow_automatic_weight_conversion_without_observed_weight": False,
    "require_source_to_target_provenance": True,
}

EXPECTED_MEDICATION_VARIABLES = (
    "clonidine_iv_cont",
    "dexamethasone_iv_bolus",
    "dexmedetomidine_iv_cont",
    "dobutamine_iv_cont",
    "epinephrine_iv_cont",
    "fentanyl_iv_cont",
    "fludrocortisone_po_bolus",
    "furosemide_iv_cont",
    "hydrocortisone_iv_bolus",
    "inhaled_iloprost",
    "inhaled_no",
    "isoflurane_inh",
    "ketanest_iv_cont",
    "levosimendan_iv_cont",
    "midazolam_iv_cont",
    "milrinone_iv_cont",
    "morphine_iv_cont",
    "norepinephrine_iv_cont",
    "prednisolone_iv_bolus",
    "propofol_iv_cont",
    "rocuronium_iv_bolus",
    "sevoflurane_inh",
    "sufentanil_iv_cont",
    "terlipressin_iv_bolus",
    "vasopressin_iv_cont",
)

EXPECTED_MEDICATION_VALUE_SEMANTICS = {
    "explicit_numeric_zero": "preserve_as_observed_zero",
    "empty_or_approved_missing_token": "preserve_as_null",
    "hospital_variable_without_observed_values": "preserve_as_null_not_zero",
    "missing_cell_in_partially_observed_hospital_variable": (
        "preserve_as_null_not_zero"
    ),
    "null_to_zero_imputation": "prohibited",
    "zero_to_null_conversion": "prohibited",
    "null_implies_medication_inactive": False,
    "harmonization_conversions_preserve_zero_and_null_independently": True,
    "semantic_splits_preserve_zero_and_null_independently": True,
    "continuous_infusion_active_candidate": "value_greater_than_zero",
    "bolus_administration_candidate": "value_greater_than_zero",
    "carry_forward_or_interval_imputation": (
        "requires_separate_reviewed_contract"
    ),
}

EXPECTED_BOUNDARY = {
    "authorize_unit_and_semantic_split_schema_planning": True,
    "authorize_harmonized_schema_dictionary_freeze": False,
    "authorize_clinical_data_build": False,
    "authorize_cleaning_policy_0_2": False,
    "modify_existing_harmonized_release": False,
    "modify_existing_cleaned_release": False,
    "modify_existing_derived_release": False,
    "write_clinical_data": False,
    "filter_rows_or_stays": False,
    "drop_source_values": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class ReviewedUnitDecisions:
    version: str
    base_candidate: CandidateUnitDecisions
    base_candidate_sha256: str
    audit_run_id: str
    approved_harmonization_action_ids: tuple[str, ...]
    semantic_splits: tuple[dict[str, Any], ...]
    execution_contract: dict[str, Any]
    medication_variables: tuple[str, ...]
    medication_value_semantics: dict[str, Any]
    source_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _sequence_of_mappings(value: Any, location: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise ConfigurationError(f"{location} must be a YAML list")
    return tuple(_mapping(item, f"{location}[{index}]") for index, item in enumerate(value))


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ConfigurationError(f"{location} must be a list of non-empty strings")
    result = tuple(item.strip() for item in value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def _validate_splits(
    raw_splits: tuple[dict[str, Any], ...],
    candidate: CandidateUnitDecisions,
) -> tuple[dict[str, Any], ...]:
    old_masks = {
        (str(row["hospital"]), str(row["variable"])): str(row["decision_id"])
        for row in candidate.hospital_cleaning_masks
    }
    if set(old_masks) != set(EXPECTED_SPLITS):
        raise ConfigurationError("Audited mask scopes differ from reviewed split scopes")
    observed: dict[tuple[str, str], dict[str, Any]] = {}
    decision_ids: set[str] = set()
    target_names: set[str] = set()
    for index, split in enumerate(raw_splits):
        location = f"semantic_splits[{index}]"
        decision_id = required_string(split, "decision_id", location)
        if decision_id in decision_ids:
            raise ConfigurationError(f"Duplicate semantic-split decision {decision_id}")
        decision_ids.add(decision_id)
        hospital = required_string(split, "hospital", location)
        source = required_string(split, "source_variable", location)
        scope = (hospital, source)
        if scope in observed or scope not in EXPECTED_SPLITS:
            raise ConfigurationError(f"Unexpected or duplicate semantic-split scope {scope}")
        expected_target, expected_unit, expected_eligibility, expected_confidence = (
            EXPECTED_SPLITS[scope]
        )
        if (
            split.get("evidence_decision_id") != old_masks[scope]
            or split.get("canonical_variable") != source
            or split.get("target_variable") != expected_target
            or split.get("target_value_type") != "float64"
            or split.get("target_unit") != expected_unit
            or split.get("analysis_eligibility") != expected_eligibility
            or split.get("confidence") != expected_confidence
        ):
            raise ConfigurationError(f"Reviewed semantic-split contract changed for {scope}")
        required_string(split, "description", location)
        required_string(split, "target_display_unit", location)
        required_string(split, "evidence_basis", location)
        if expected_target in candidate.variables or expected_target in target_names:
            raise ConfigurationError(f"Semantic-split target is not new and unique: {expected_target}")
        target_names.add(expected_target)
        observed[scope] = split
    if set(observed) != set(EXPECTED_SPLITS):
        raise ConfigurationError("Reviewed semantic splits do not cover all nine scopes")
    return tuple(observed[scope] for scope in EXPECTED_SPLITS)


def load_reviewed_unit_decisions(path: str | Path) -> ReviewedUnitDecisions:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed unit decisions")
    if (
        raw.get("reviewed_unit_decisions_version") != "0.2"
        or raw.get("status")
        != "approved_unit_and_semantic_split_decisions_pending_schema_freeze"
    ):
        raise ConfigurationError("Reviewed unit-decision status is invalid")

    lineage = _mapping(raw.get("lineage"), "lineage")
    candidate_path = resolve_path(
        required_string(lineage, "base_candidate", "lineage"), source
    )
    expected_sha = required_string(lineage, "base_candidate_sha256", "lineage")
    if sha256_file(candidate_path) != expected_sha:
        raise ConfigurationError("Reviewed base unit candidate changed")
    candidate = load_candidate_unit_decisions(candidate_path)
    audit_run_id = required_string(lineage, "unit_decision_audit_run_id", "lineage")
    if (
        audit_run_id != "20260807T083117Z"
        or not RUN_ID_PATTERN.fullmatch(audit_run_id)
        or lineage.get("unit_decision_audit_artifact_version") != "0.2"
        or lineage.get("input_cleaned_release_id") != "20260806T114234Z"
    ):
        raise ConfigurationError("Reviewed unit-decision audit lineage changed")

    human = _mapping(raw.get("human_decision"), "human_decision")
    if human.get("decision_date") != "2026-08-07":
        raise ConfigurationError("Reviewed unit-decision approval date changed")
    required_string(human, "decision", "human_decision")
    required_string(human, "approval_record", "human_decision")

    action_ids = _string_list(
        raw.get("approved_harmonization_action_ids"),
        "approved_harmonization_action_ids",
    )
    candidate_action_ids = {
        str(row["decision_id"]) for row in candidate.hospital_harmonization_actions
    }
    if set(action_ids) != EXPECTED_ACTION_IDS or set(action_ids) != candidate_action_ids:
        raise ConfigurationError("Approved harmonization-action coverage changed")

    splits = _validate_splits(
        _sequence_of_mappings(raw.get("semantic_splits"), "semantic_splits"),
        candidate,
    )
    execution = _mapping(
        raw.get("semantic_split_execution_contract"),
        "semantic_split_execution_contract",
    )
    if execution != EXPECTED_EXECUTION_CONTRACT:
        raise ConfigurationError("Semantic-split execution contract changed")

    medication = _mapping(
        raw.get("medication_value_semantics"),
        "medication_value_semantics",
    )
    if medication.pop("decision_date", None) != "2026-08-07":
        raise ConfigurationError("Medication value-semantics decision date changed")
    required_string(
        medication,
        "approval_record",
        "medication_value_semantics",
    )
    medication.pop("approval_record")
    medication_variables = _string_list(
        medication.pop("medication_or_therapy_variables", None),
        "medication_value_semantics.medication_or_therapy_variables",
    )
    if medication_variables != EXPECTED_MEDICATION_VARIABLES:
        raise ConfigurationError("Medication/therapy variable scope changed")
    if medication != EXPECTED_MEDICATION_VALUE_SEMANTICS:
        raise ConfigurationError("Medication zero/missing semantics changed")

    superseded = set(
        _string_list(
            raw.get("superseded_cleaning_mask_decision_ids"),
            "superseded_cleaning_mask_decision_ids",
        )
    )
    if superseded != set(old["decision_id"] for old in candidate.hospital_cleaning_masks):
        raise ConfigurationError("Superseded cleaning-mask coverage changed")

    ranges = _mapping(
        raw.get("priority_cleaning_range_decisions"),
        "priority_cleaning_range_decisions",
    )
    if ranges != {
        "status": "pending_separate_cleaning_policy_0_2_review",
        "activated_by_this_contract": False,
        "reviewed_legacy_finding_count": 10_021,
    }:
        raise ConfigurationError("Cleaning-range decision boundary changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Reviewed unit-decision boundary changed")

    return ReviewedUnitDecisions(
        version="0.2",
        base_candidate=candidate,
        base_candidate_sha256=expected_sha,
        audit_run_id=audit_run_id,
        approved_harmonization_action_ids=action_ids,
        semantic_splits=splits,
        execution_contract=execution,
        medication_variables=medication_variables,
        medication_value_semantics=dict(EXPECTED_MEDICATION_VALUE_SEMANTICS),
        source_path=source,
    )
