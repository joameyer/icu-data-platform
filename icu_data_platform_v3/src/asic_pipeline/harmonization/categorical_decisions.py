from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError


APPROVED_HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)
EXPECTED_RULE_KEYS = frozenset(
    {
        ("dynamic", "ards_diagnosis_app"),
        ("dynamic", "ecmo"),
        ("dynamic", "position_therapy"),
        ("dynamic", "severity_read_confirmation"),
        ("dynamic", "therapy_read_confirmation_utc"),
        ("static", "age_group"),
        ("static", "bmi_group"),
        ("static", "cluster_id"),
        ("static", "death_status"),
        ("static", "discharge_status"),
        ("static", "height_group"),
        ("static", "hospital_mortality_reported"),
        ("static", "icu_readmit"),
        ("static", "sex"),
        ("static", "study_implementation_phase"),
        ("static", "weight_group"),
    }
)


@dataclass(frozen=True)
class ReviewedCategoricalRule:
    table: str
    variable: str
    physical_type: str
    unit: str
    parser: str
    analysis_eligible: bool
    allowed_values: tuple[Any, ...]
    allowed_values_by_hospital: tuple[tuple[str, tuple[Any, ...]], ...]
    missing_values_by_hospital: tuple[tuple[str, tuple[str, ...]], ...]
    expected_non_null_count: int | None
    expected_null_count: int | None
    caveat: str | None
    all_missing: bool


@dataclass(frozen=True)
class ReviewedCategoricalDecisions:
    version: str
    candidate_run_id: str
    consolidated_audit_run_id: str
    categorical_review_run_id: str
    rules: tuple[ReviewedCategoricalRule, ...]
    source_path: Path

    def rule_for(self, table: str, variable: str) -> ReviewedCategoricalRule | None:
        return next(
            (
                rule
                for rule in self.rules
                if rule.table == table and rule.variable == variable
            ),
            None,
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _optional_count(value: Any, location: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a non-negative integer")
    return value


def _hospital_values(value: Any, location: str) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    if value is None:
        return ()
    mapping = _mapping(value, location)
    if set(mapping) - set(APPROVED_HOSPITALS):
        raise ConfigurationError(f"{location} contains an unknown hospital")
    result: list[tuple[str, tuple[Any, ...]]] = []
    for hospital, values in mapping.items():
        if not isinstance(values, list) or len(values) != len(set(values)):
            raise ConfigurationError(f"{location}.{hospital} must be a unique list")
        result.append((hospital, tuple(values)))
    return tuple(sorted(result))


def load_reviewed_categorical_decisions(
    path: str | Path,
) -> ReviewedCategoricalDecisions:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed categorical decisions")
    if raw.get("reviewed_categorical_decisions_version") != "0.1" or raw.get(
        "status"
    ) != "approved_complete_categorical_contract_pending_schema_dictionary_freeze":
        raise ConfigurationError("Reviewed categorical contract is invalid")
    evidence = _mapping(raw.get("evidence"), "categorical.evidence")
    if (
        evidence.get("domain_truncated_variable_count") != 0
        or evidence.get("technical_blocking_finding_count") != 0
    ):
        raise ConfigurationError("Categorical decisions require complete evidence")
    rules_root = _mapping(raw.get("rules"), "categorical.rules")
    rules: list[ReviewedCategoricalRule] = []
    for table in ("static", "dynamic"):
        table_rules = _mapping(rules_root.get(table), f"categorical.rules.{table}")
        for variable, raw_rule in table_rules.items():
            location = f"categorical.rules.{table}.{variable}"
            rule = _mapping(raw_rule, location)
            eligible = rule.get("analysis_eligible")
            if not isinstance(eligible, bool):
                raise ConfigurationError(f"{location}.analysis_eligible must be boolean")
            allowed_values = rule.get("allowed_values", [])
            if not isinstance(allowed_values, list) or len(allowed_values) != len(
                set(allowed_values)
            ):
                raise ConfigurationError(f"{location}.allowed_values must be unique")
            rules.append(
                ReviewedCategoricalRule(
                    table=table,
                    variable=str(variable),
                    physical_type=required_string(rule, "physical_type", location),
                    unit=required_string(rule, "unit", location),
                    parser=required_string(rule, "parser", location),
                    analysis_eligible=eligible,
                    allowed_values=tuple(allowed_values),
                    allowed_values_by_hospital=_hospital_values(
                        rule.get(
                            "allowed_values_by_hospital",
                            rule.get("allowed_codes_by_hospital"),
                        ),
                        f"{location}.allowed_values_by_hospital",
                    ),
                    missing_values_by_hospital=tuple(
                        (hospital, tuple(str(item) for item in values))
                        for hospital, values in _hospital_values(
                            rule.get("approved_textual_missing_by_hospital"),
                            f"{location}.approved_textual_missing_by_hospital",
                        )
                    ),
                    expected_non_null_count=_optional_count(
                        rule.get("expected_non_null_count"),
                        f"{location}.expected_non_null_count",
                    ),
                    expected_null_count=_optional_count(
                        rule.get("expected_null_count"),
                        f"{location}.expected_null_count",
                    ),
                    caveat=(
                        str(rule["caveat"])
                        if isinstance(rule.get("caveat"), str)
                        else None
                    ),
                    all_missing=rule.get("all_missing") is True,
                )
            )
    keys = {(rule.table, rule.variable) for rule in rules}
    if keys != EXPECTED_RULE_KEYS or len(rules) != len(EXPECTED_RULE_KEYS):
        raise ConfigurationError("Complete categorical variable coverage changed")

    by_key = {(rule.table, rule.variable): rule for rule in rules}
    ards = by_key[("dynamic", "ards_diagnosis_app")]
    if (
        ards.physical_type != "int32"
        or ards.analysis_eligible
        or dict(ards.allowed_values_by_hospital).get("asic_UK00")
        != (0, 10, 100, 200, 1600, 3000, 60000)
    ):
        raise ConfigurationError("Reviewed ARDS preservation decision changed")
    ecmo = by_key[("dynamic", "ecmo")]
    if ecmo.physical_type != "bool" or not ecmo.analysis_eligible:
        raise ConfigurationError("Reviewed ECMO Boolean decision changed")
    position = by_key[("dynamic", "position_therapy")]
    if (
        position.physical_type != "float64"
        or position.unit != "dimensionless_proportion"
        or not position.analysis_eligible
    ):
        raise ConfigurationError("Reviewed position-therapy decision changed")
    height = by_key[("static", "height_group")]
    if dict(height.missing_values_by_hospital) != {"asic_UK01": ("-1",)}:
        raise ConfigurationError("Reviewed height-group sentinel decision changed")
    severity = by_key[("dynamic", "severity_read_confirmation")]
    if not severity.all_missing or severity.analysis_eligible:
        raise ConfigurationError("Reviewed all-missing categorical decision changed")

    activation = _mapping(raw.get("activation"), "categorical.activation")
    if activation != {
        "existing_candidate_is_immutable": True,
        "apply_only_when_building_a_new_harmonized_candidate": True,
        "ordered_union_schema_approved": False,
        "variable_dictionary_approved": False,
        "publication_approved": False,
    }:
        raise ConfigurationError("Categorical activation boundary changed")
    return ReviewedCategoricalDecisions(
        version="0.1",
        candidate_run_id=required_string(
            evidence, "candidate_run_id", "categorical.evidence"
        ),
        consolidated_audit_run_id=required_string(
            evidence, "consolidated_audit_run_id", "categorical.evidence"
        ),
        categorical_review_run_id=required_string(
            evidence, "categorical_review_run_id", "categorical.evidence"
        ),
        rules=tuple(sorted(rules, key=lambda item: (item.table, item.variable))),
        source_path=source,
    )
