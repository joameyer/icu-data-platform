from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping
from asic_pipeline.errors import ConfigurationError


EXPECTED_VARIABLES = frozenset(
    {
        "albumin",
        "alt",
        "amylase",
        "ast",
        "base_excess_art",
        "bicarbonate_art",
        "bilirubin_total",
        "bnp",
        "ck",
        "ck_mb",
        "clonidine_iv_cont",
        "creatinine",
        "crp",
        "d_dimer",
        "dexamethasone_iv_bolus",
        "dexmedetomidine_iv_cont",
        "dobutamine_iv_cont",
        "epinephrine_iv_cont",
        "evlwi",
        "fentanyl_iv_cont",
        "fludrocortisone_po_bolus",
        "furosemide_iv_cont",
        "gedvi",
        "hemoglobin",
        "hydrocortisone_iv_bolus",
        "il6",
        "inhaled_iloprost",
        "inhaled_no",
        "inr",
        "isofa_cardiovascular",
        "isofa_cns",
        "isofa_liver",
        "isofa_renal",
        "isofa_respiratory",
        "isofa_thrombocyte",
        "isofa_total_score",
        "isoflurane_inh",
        "ketanest_iv_cont",
        "lactate_art",
        "ldh",
        "levosimendan_iv_cont",
        "lipase",
        "lymph_abs",
        "midazolam_iv_cont",
        "milrinone_iv_cont",
        "morphine_iv_cont",
        "norepinephrine_iv_cont",
        "ntprobnp",
        "pct",
        "platelets",
        "prednisolone_iv_bolus",
        "propofol_iv_cont",
        "ptt",
        "pvri",
        "rocuronium_iv_bolus",
        "sevoflurane_inh",
        "sofa_blood",
        "sofa_cns",
        "sofa_liver",
        "sofa_renal",
        "sofa_respiratory",
        "sofa_respiratory_calculated",
        "sofa_score_unspecified",
        "sofa_score_without_gcs",
        "sofa_total_score",
        "sufentanil_iv_cont",
        "svri",
        "terlipressin_iv_bolus",
        "troponin",
        "urea",
        "vasopressin_iv_cont",
        "wbc",
    }
)

SCORE_VARIABLES = frozenset(
    variable
    for variable in EXPECTED_VARIABLES
    if variable.startswith("sofa_") or variable.startswith("isofa_")
)

EXPECTED_PRIORITY_RANGE_VARIABLES = frozenset(
    {
        "albumin",
        "creatinine",
        "evlwi",
        "gedvi",
        "hemoglobin",
        "inr",
        "lactate_art",
        "platelets",
        "ptt",
        "sofa_score_unspecified",
        "sofa_score_without_gcs",
        "svri",
    }
)

ALLOWED_CONFIDENCE = frozenset({"high", "moderate"})
ALLOWED_HOSPITALS = frozenset(
    {
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    }
)


@dataclass(frozen=True)
class CandidateUnitDecisions:
    version: str
    variables: dict[str, dict[str, Any]]
    hospital_harmonization_actions: tuple[dict[str, Any], ...]
    hospital_cleaning_masks: tuple[dict[str, Any], ...]
    priority_cleaning_range_decisions: dict[str, dict[str, Any]]
    provenance_statement: dict[str, Any]
    source_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _sequence_of_mappings(value: Any, location: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise ConfigurationError(f"{location} must be a YAML list")
    result: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        result.append(_mapping(item, f"{location}[{index}]"))
    return tuple(result)


def _required_nonempty_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _validate_variables(variables: dict[str, Any]) -> dict[str, dict[str, Any]]:
    observed = set(variables)
    if observed != EXPECTED_VARIABLES:
        missing = sorted(EXPECTED_VARIABLES - observed)
        extra = sorted(observed - EXPECTED_VARIABLES)
        raise ConfigurationError(
            f"Candidate unit decisions do not cover the exact 72-variable scope; "
            f"missing={missing}, extra={extra}"
        )
    result: dict[str, dict[str, Any]] = {}
    for variable in sorted(EXPECTED_VARIABLES):
        row = _mapping(variables[variable], f"variables.{variable}")
        _required_nonempty_string(row, "description", f"variables.{variable}")
        unit = _required_nonempty_string(row, "unit", f"variables.{variable}")
        if unit == "unresolved":
            raise ConfigurationError(f"variables.{variable}.unit remains unresolved")
        _required_nonempty_string(row, "display_unit", f"variables.{variable}")
        _required_nonempty_string(row, "evidence_basis", f"variables.{variable}")
        if row.get("eligibility") != "eligible":
            raise ConfigurationError(
                f"variables.{variable}.eligibility must be eligible; availability is separate"
            )
        if row.get("confidence") not in ALLOWED_CONFIDENCE:
            raise ConfigurationError(
                f"variables.{variable}.confidence must be one of {sorted(ALLOWED_CONFIDENCE)}"
            )
        result[variable] = row
    if any(result[variable]["unit"] != "score_point" for variable in SCORE_VARIABLES):
        raise ConfigurationError("Every SOFA and iSOFA field must use score_point")
    return result


def _validate_actions(
    actions: tuple[dict[str, Any], ...],
    variables: dict[str, dict[str, Any]],
    location: str,
) -> None:
    identifiers: set[str] = set()
    for index, action in enumerate(actions):
        item_location = f"{location}[{index}]"
        identifier = _required_nonempty_string(action, "decision_id", item_location)
        if identifier in identifiers:
            raise ConfigurationError(f"Duplicate decision_id {identifier}")
        identifiers.add(identifier)
        hospital = _required_nonempty_string(action, "hospital", item_location)
        if hospital not in ALLOWED_HOSPITALS:
            raise ConfigurationError(f"{item_location}.hospital is not approved")
        variable = _required_nonempty_string(action, "variable", item_location)
        if variable not in variables:
            raise ConfigurationError(f"{item_location}.variable is outside the 72-variable scope")
        if location == "hospital_harmonization_actions":
            target = _required_nonempty_string(action, "target_unit", item_location)
            if target != variables[variable]["unit"]:
                raise ConfigurationError(
                    f"{item_location}.target_unit does not match the variable dictionary"
                )
            if action.get("confidence") not in ALLOWED_CONFIDENCE:
                raise ConfigurationError(f"{item_location}.confidence is invalid")
            _required_nonempty_string(action, "evidence", item_location)
        else:
            _required_nonempty_string(action, "reason", item_location)


def load_candidate_unit_decisions(path: str | Path) -> CandidateUnitDecisions:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Candidate unit decisions")
    version = raw.get("unit_resolution_decisions_version")
    if version != "0.2-candidate.1":
        raise ConfigurationError("Candidate unit decision version is invalid")
    if raw.get("status") != "complete_candidate_pending_data_owner_approval":
        raise ConfigurationError("Candidate unit decision status is invalid")

    provenance = _mapping(raw.get("provenance_statement"), "provenance_statement")
    if provenance.get("hospital_unit_dictionary_was_provided") is not False:
        raise ConfigurationError("Hospital unit-dictionary provenance changed")
    if provenance.get("units_are_team_derived") is not True:
        raise ConfigurationError("Team-derived unit provenance must be explicit")
    _required_nonempty_string(
        provenance, "mandatory_dictionary_note", "provenance_statement"
    )

    boundary = _mapping(raw.get("activation_boundary"), "activation_boundary")
    required_boundary = {
        "candidate_only": True,
        "human_approval_required": True,
        "modify_existing_harmonized_release": False,
        "modify_existing_cleaned_release": False,
        "modify_existing_derived_release": False,
        "requires_new_harmonized_contract_version": "0.2",
        "requires_new_cleaning_policy_version": "0.2",
        "requires_complete_rebuild_and_audit": True,
    }
    if boundary != required_boundary:
        raise ConfigurationError("Candidate activation boundary changed")

    variables = _validate_variables(_mapping(raw.get("variables"), "variables"))
    harmonization = _sequence_of_mappings(
        raw.get("hospital_harmonization_actions"),
        "hospital_harmonization_actions",
    )
    masks = _sequence_of_mappings(
        raw.get("hospital_cleaning_masks"), "hospital_cleaning_masks"
    )
    _validate_actions(harmonization, variables, "hospital_harmonization_actions")
    _validate_actions(masks, variables, "hospital_cleaning_masks")

    ranges_raw = _mapping(
        raw.get("priority_cleaning_range_decisions"),
        "priority_cleaning_range_decisions",
    )
    if set(ranges_raw) != EXPECTED_PRIORITY_RANGE_VARIABLES:
        raise ConfigurationError("Priority range decisions do not cover the exact scope")
    ranges: dict[str, dict[str, Any]] = {}
    for variable in sorted(EXPECTED_PRIORITY_RANGE_VARIABLES):
        row = _mapping(
            ranges_raw[variable], f"priority_cleaning_range_decisions.{variable}"
        )
        if row.get("unit") != variables[variable]["unit"]:
            raise ConfigurationError(
                f"Priority range unit for {variable} differs from the dictionary"
            )
        _required_nonempty_string(
            row, "rule", f"priority_cleaning_range_decisions.{variable}"
        )
        count = row.get("legacy_outside_count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ConfigurationError(
                f"priority_cleaning_range_decisions.{variable}.legacy_outside_count is invalid"
            )
        ranges[variable] = row
    if sum(row["legacy_outside_count"] for row in ranges.values()) != 10_021:
        raise ConfigurationError("Priority legacy outside-range accounting changed")

    return CandidateUnitDecisions(
        version=version,
        variables=variables,
        hospital_harmonization_actions=harmonization,
        hospital_cleaning_masks=masks,
        priority_cleaning_range_decisions=ranges,
        provenance_statement=provenance,
        source_path=source,
    )
