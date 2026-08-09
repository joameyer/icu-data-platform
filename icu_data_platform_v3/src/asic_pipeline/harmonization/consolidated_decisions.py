from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError


EXPECTED_CONVERSIONS = frozenset(
    {
        ("asic_UK04", "dynamic", "etco2", "divide", 7.50062, "mmHg"),
        ("asic_UK03", "dynamic", "fio2", "multiply", 100.0, "percent"),
        ("asic_UK03", "dynamic", "hematocrit", "multiply", 100.0, "percent"),
        ("asic_UK00", "dynamic", "lymph_pct", "multiply", 100.0, "percent"),
    }
)
EXPECTED_ALL_MISSING = frozenset(
    {
        "feo2",
        "severity_read_confirmation",
        "sofa_score_without_gcs",
        "stroke_volume_bolus",
    }
)
PERCENTAGE_POINT_VARIABLES = frozenset(
    {"ecmo_o2", "feo2", "fio2", "fio2_set", "sao2", "scvo2", "spo2"}
)


@dataclass(frozen=True)
class ApprovedHospitalConversion:
    decision_id: str
    hospital: str
    table: str
    variable: str
    operation: str
    factor: float
    output_unit: str


@dataclass(frozen=True)
class ReviewedConsolidatedDecisions:
    version: str
    status: str
    candidate_run_id: str
    consolidated_audit_run_id: str
    approved_units: tuple[tuple[str, str, str], ...]
    conversions: tuple[ApprovedHospitalConversion, ...]
    all_missing_variables: tuple[str, ...]
    source_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _required_bool(mapping: dict[str, Any], key: str, location: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location}.{key} must be boolean")
    return value


def load_reviewed_consolidated_decisions(
    path: str | Path,
) -> ReviewedConsolidatedDecisions:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed consolidated harmonization decisions")
    version = required_string(
        raw, "reviewed_consolidated_decisions_version", "decisions"
    )
    status = required_string(raw, "status", "decisions")
    if version != "0.1" or status != (
        "approved_partial_pending_categorical_and_schema_freeze"
    ):
        raise ConfigurationError("Reviewed consolidated decision status is invalid")

    evidence = _mapping(raw.get("evidence"), "decisions.evidence")
    if evidence.get("consolidated_audit_technical_blocking_finding_count") != 0:
        raise ConfigurationError("Reviewed decisions require a technically passing audit")
    candidate_run_id = required_string(
        evidence, "candidate_run_id", "decisions.evidence"
    )
    audit_run_id = required_string(
        evidence, "consolidated_audit_run_id", "decisions.evidence"
    )

    activation = _mapping(raw.get("activation"), "decisions.activation")
    if not _required_bool(
        activation, "existing_candidate_is_immutable", "decisions.activation"
    ) or not _required_bool(
        activation,
        "apply_only_when_building_a_new_harmonized_candidate",
        "decisions.activation",
    ):
        raise ConfigurationError("Approved decisions may not mutate existing candidates")
    for key in (
        "categorical_contract_approved",
        "ordered_union_schema_approved",
        "variable_dictionary_approved",
        "publication_approved",
    ):
        if _required_bool(activation, key, "decisions.activation"):
            raise ConfigurationError(f"The unreviewed gate {key} must remain closed")

    unit_root = _mapping(raw.get("approved_units"), "decisions.approved_units")
    units: list[tuple[str, str, str]] = []
    for table in ("static", "dynamic"):
        table_units = _mapping(unit_root.get(table), f"approved_units.{table}")
        for variable, unit in table_units.items():
            if not isinstance(variable, str) or not variable or not isinstance(unit, str) or not unit:
                raise ConfigurationError("Approved unit entries must be non-empty strings")
            units.append((table, variable, unit))
    if len(units) != len({(table, variable) for table, variable, _ in units}):
        raise ConfigurationError("Approved units contain a duplicate table-variable")
    percentage_variables = {
        variable
        for table, variable, unit in units
        if table == "dynamic" and unit == "percent"
    }
    if percentage_variables != PERCENTAGE_POINT_VARIABLES:
        raise ConfigurationError("Percentage-point unit decisions changed")

    conversion_values = raw.get("approved_hospital_conversions")
    if not isinstance(conversion_values, list):
        raise ConfigurationError("approved_hospital_conversions must be a list")
    conversions: list[ApprovedHospitalConversion] = []
    for index, item in enumerate(conversion_values):
        location = f"approved_hospital_conversions[{index}]"
        value = _mapping(item, location)
        factor = value.get("factor")
        if not isinstance(factor, (int, float)) or isinstance(factor, bool) or factor <= 0:
            raise ConfigurationError(f"{location}.factor must be positive")
        conversion = ApprovedHospitalConversion(
            decision_id=required_string(value, "decision_id", location),
            hospital=required_string(value, "hospital", location),
            table=required_string(value, "table", location),
            variable=required_string(value, "variable", location),
            operation=required_string(value, "operation", location),
            factor=float(factor),
            output_unit=required_string(value, "output_unit", location),
        )
        if conversion.operation not in {"multiply", "divide"}:
            raise ConfigurationError(f"{location}.operation is not approved")
        conversions.append(conversion)
    observed_conversions = frozenset(
        (
            item.hospital,
            item.table,
            item.variable,
            item.operation,
            item.factor,
            item.output_unit,
        )
        for item in conversions
    )
    if observed_conversions != EXPECTED_CONVERSIONS or len(conversions) != 4:
        raise ConfigurationError("Approved hospital conversion decisions changed")

    unresolved = _mapping(
        raw.get("unresolved_unit_policy"), "decisions.unresolved_unit_policy"
    )
    if unresolved != {
        "action": "preserve_value_without_conversion",
        "unit": "unresolved",
        "analysis_eligible": False,
        "distribution_alignment_is_not_unit_evidence": True,
        "applies_to_all_numeric_variables_not_listed_under_approved_units": True,
    }:
        raise ConfigurationError("Unresolved-unit fail-closed policy changed")

    restrictions = raw.get("analysis_restrictions")
    if not isinstance(restrictions, list) or len(restrictions) != 2:
        raise ConfigurationError("Analysis restrictions changed")
    restriction_map = {
        tuple(item.get("variables", [])): item
        for item in restrictions
        if isinstance(item, dict)
    }
    if (
        restriction_map.get(("ie_ratio", "ie_ratio_set"), {}).get("scope")
        != "all_hospitals"
        or restriction_map.get(("ie_ratio", "ie_ratio_set"), {}).get(
            "analysis_eligible"
        )
        is not False
        or restriction_map.get(
            ("vt_per_kg", "vt_per_kg_ideal_body_weight"), {}
        ).get("scope")
        != ["asic_UK06"]
        or restriction_map.get(
            ("vt_per_kg", "vt_per_kg_ideal_body_weight"), {}
        ).get("analysis_eligible")
        is not False
    ):
        raise ConfigurationError("Approved analysis restrictions changed")

    semantics = _mapping(raw.get("semantic_decisions"), "semantic_decisions")
    driving = _mapping(semantics.get("driving_pressure"), "semantic.driving_pressure")
    oxygen = _mapping(
        semantics.get("oxygen_saturations"), "semantic.oxygen_saturations"
    )
    free_days = _mapping(
        semantics.get("free_day_variables"), "semantic.free_day_variables"
    )
    split = _mapping(
        semantics.get("uk00_tidal_volume_semantic_split"),
        "semantic.uk00_tidal_volume_semantic_split",
    )
    sofa = _mapping(semantics.get("sofa_and_isofa"), "semantic.sofa_and_isofa")
    if (
        driving
        != {
            "decision": "keep_reported_and_future_computed_variables_separate",
            "source_backed_harmonized_variable": "delta_p_reported",
            "future_derived_variable": "delta_p_computed",
        }
        or oxygen.get("decision") != "keep_scvo2_sao2_and_spo2_separate"
        or free_days.get("decision")
        != "keep_dialysis_free_days_and_vent_free_days_separate"
        or sofa.get("decision")
        != "preserve_reported_calculated_and_isofa_variables_separately"
        or sofa.get("floating_point_component_sum_tolerance") != 1.0e-12
        or split
        != {
            "decision": "keep_separate",
            "variable": "vt_per_ideal_bw_total",
            "unit": "mL",
            "must_not_merge_with": ["vt", "vt_per_kg_ideal_body_weight"],
        }
    ):
        raise ConfigurationError("Approved semantic decisions changed")

    cleaning = _mapping(raw.get("cleaning_boundary"), "decisions.cleaning_boundary")
    if cleaning != {
        "invalid_range_variable_decisions": 42,
        "row_level_scale_rule_decisions": 1,
        "action": "defer_all_to_cleaned_layer",
        "apply_physiologic_masking_during_harmonization": False,
        "apply_row_level_power_of_ten_correction_during_harmonization": False,
    }:
        raise ConfigurationError("Approved harmonized/cleaned boundary changed")

    all_missing = _mapping(
        raw.get("globally_all_missing_columns"),
        "decisions.globally_all_missing_columns",
    )
    values = all_missing.get("variables")
    if (
        all_missing.get("action")
        != "preserve_with_explicit_all_missing_dictionary_flag"
        or not isinstance(values, list)
        or frozenset(values) != EXPECTED_ALL_MISSING
        or len(values) != len(EXPECTED_ALL_MISSING)
    ):
        raise ConfigurationError("Globally all-missing preservation decision changed")

    remaining = _mapping(raw.get("remaining_human_gates"), "remaining_human_gates")
    if set(remaining.values()) != {"pending"} or set(remaining) != {
        "categorical_domain_contract",
        "ordered_union_schema_freeze",
        "variable_dictionary_freeze",
        "release_publication",
    }:
        raise ConfigurationError("Required remaining human gates changed")

    return ReviewedConsolidatedDecisions(
        version=version,
        status=status,
        candidate_run_id=candidate_run_id,
        consolidated_audit_run_id=audit_run_id,
        approved_units=tuple(sorted(units)),
        conversions=tuple(conversions),
        all_missing_variables=tuple(sorted(str(item) for item in values)),
        source_path=source,
    )
