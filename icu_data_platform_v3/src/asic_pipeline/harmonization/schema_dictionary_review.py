from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
import math
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.categorical_decisions import (
    ReviewedCategoricalDecisions,
    ReviewedCategoricalRule,
    load_reviewed_categorical_decisions,
)
from asic_pipeline.harmonization.consolidated_audit import (
    ConsolidatedAuditConfig,
    load_consolidated_audit_config,
)
from asic_pipeline.harmonization.consolidated_decisions import (
    ReviewedConsolidatedDecisions,
    load_reviewed_consolidated_decisions,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


PHYSICAL_TYPE_OVERRIDES = {
    "large_string": "large_string",
    "bool": "bool",
    "int32": "int32",
    "float64": "double",
    "fixed_size_list_bool_2": "fixed_size_list<item: bool>[2]",
}
CATEGORICAL_DEFINITIONS = {
    "ards_diagnosis_app": "Source application ARDS-diagnosis code; code meanings and UK00 nonstandard codes remain unresolved.",
    "ecmo": "Whether extracorporeal membrane oxygenation support was recorded.",
    "position_therapy": "Recorded position-therapy value represented as a dimensionless proportion; UK00 includes fractional values while other reporting sites are mostly binary.",
    "severity_read_confirmation": "Severity read-confirmation source field; globally all missing in the reviewed export.",
    "therapy_read_confirmation_utc": "Fixed-position pair preserving both UK00 therapy read-confirmation sources without OR, consensus, or preference.",
    "age_group": "Patient age group in years using the reviewed source range labels.",
    "bmi_group": "Body-mass-index category translated to the reviewed English labels.",
    "cluster_id": "ASIC study cluster assignment; the permitted cluster domain is hospital scoped.",
    "death_status": "Reported death-status category; kept separate from discharge status and hospital mortality.",
    "discharge_status": "Reported discharge disposition; kept separate from death status and hospital mortality.",
    "height_group": "Patient height group in centimetres using the reviewed source range labels.",
    "hospital_mortality_reported": "Source-reported hospital mortality indicator.",
    "icu_readmit": "Whether the ICU stay was recorded as a readmission.",
    "sex": "Recorded patient sex translated to the reviewed female/male vocabulary.",
    "study_implementation_phase": "Study phase: calibration, roll-in, or application implementation.",
    "weight_group": "Patient weight group in kilograms using the reviewed source range labels.",
}


@dataclass(frozen=True)
class SchemaDictionaryReviewPolicy:
    version: str
    consolidated_decisions_path: Path
    consolidated_decisions_sha256: str
    categorical_decisions_path: Path
    categorical_decisions_sha256: str
    candidate_run_id: str
    consolidated_audit_run_id: str
    categorical_review_run_id: str
    consolidated_private_version: str
    categorical_private_version: str
    consolidated_private_directory_name: str
    categorical_private_directory_name: str
    provenance_fields: tuple[tuple[str, str], ...]
    representation_overrides: tuple[tuple[str, str, str, str, str], ...]
    generated_outputs: tuple[
        tuple[str, str, str, str, str, str, str, str, str, str], ...
    ]
    private_artifact_version: str
    review_artifact_version: str
    private_directory_name: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class SchemaDictionaryReviewConfig:
    consolidated: ConsolidatedAuditConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.consolidated.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.consolidated.reports_root


@dataclass(frozen=True)
class SchemaDictionaryReviewResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def load_schema_dictionary_review_config(
    path: str | Path,
) -> SchemaDictionaryReviewConfig:
    source = Path(path).expanduser().resolve()
    consolidated = load_consolidated_audit_config(source)
    raw = load_yaml_mapping(source, "Schema/dictionary review configuration")
    policy_path = resolve_path(
        required_string(raw, "schema_dictionary_review_policy", "config"), source
    )
    return SchemaDictionaryReviewConfig(consolidated, policy_path)


def load_schema_dictionary_review_policy(
    path: str | Path,
) -> SchemaDictionaryReviewPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Schema/dictionary review policy")
    if raw.get("schema_dictionary_review_policy_version") != "0.2" or raw.get(
        "status"
    ) != "approved_for_report_only_freeze_review_from_immutable_evidence":
        raise ConfigurationError("Schema/dictionary review policy is invalid")
    sources = _mapping(raw.get("decision_sources"), "decision_sources")
    evidence = _mapping(raw.get("required_evidence"), "required_evidence")
    boundary = _mapping(raw.get("review_boundary"), "review_boundary")
    if boundary != {
        "read_candidate_data": False,
        "rescan_clinical_rows": False,
        "modify_candidate_data": False,
        "freeze_schema_automatically": False,
        "freeze_dictionary_automatically": False,
        "write_harmonized_data": False,
        "publish_data": False,
    }:
        raise ConfigurationError("Schema/dictionary review boundary changed")
    provenance = raw.get("operational_provenance_fields")
    if not isinstance(provenance, list) or len(provenance) != 5:
        raise ConfigurationError("Operational provenance contract changed")
    provenance_fields: list[tuple[str, str]] = []
    for index, item in enumerate(provenance):
        value = _mapping(item, f"operational_provenance_fields[{index}]")
        provenance_fields.append(
            (
                required_string(value, "name", "operational_provenance_fields"),
                required_string(
                    value, "physical_type", "operational_provenance_fields"
                ),
            )
        )
    override_root = _mapping(
        raw.get("reviewed_representation_overrides"),
        "reviewed_representation_overrides",
    )
    representation_overrides: list[tuple[str, str, str, str, str]] = []
    for table in ("static", "dynamic"):
        table_overrides = _mapping(
            override_root.get(table), f"reviewed_representation_overrides.{table}"
        )
        for variable, raw_override in table_overrides.items():
            location = f"reviewed_representation_overrides.{table}.{variable}"
            override = _mapping(raw_override, location)
            eligibility = required_string(
                override, "analysis_eligibility", location
            )
            if eligibility not in {"eligible", "ineligible", "conditionally_ineligible"}:
                raise ConfigurationError(f"{location}.analysis_eligibility is invalid")
            representation_overrides.append(
                (
                    table,
                    str(variable),
                    required_string(override, "unit", location),
                    eligibility,
                    required_string(override, "caveat", location),
                )
            )
    generated_root = _mapping(
        raw.get("reviewed_generated_outputs"), "reviewed_generated_outputs"
    )
    generated_outputs: list[
        tuple[str, str, str, str, str, str, str, str, str, str]
    ] = []
    for table in ("static", "dynamic"):
        table_outputs = _mapping(
            generated_root.get(table, {}), f"reviewed_generated_outputs.{table}"
        )
        for variable, raw_output in table_outputs.items():
            location = f"reviewed_generated_outputs.{table}.{variable}"
            output = _mapping(raw_output, location)
            eligibility = required_string(
                output, "analysis_eligibility", location
            )
            if eligibility not in {
                "eligible",
                "ineligible",
                "conditionally_ineligible",
            }:
                raise ConfigurationError(
                    f"{location}.analysis_eligibility is invalid"
                )
            generated_outputs.append(
                (
                    table,
                    str(variable),
                    required_string(output, "insert_after", location),
                    required_string(output, "physical_type", location),
                    required_string(output, "unit", location),
                    eligibility,
                    required_string(output, "definition", location),
                    required_string(output, "caveat", location),
                    required_string(output, "source_variable", location),
                    required_string(output, "anchor", location),
                )
            )
            if output.get("operation") != "anchor_plus_minutes":
                raise ConfigurationError(f"{location}.operation is invalid")
    reporting = _mapping(raw.get("reporting"), "reporting")
    return SchemaDictionaryReviewPolicy(
        version="0.2",
        consolidated_decisions_path=resolve_path(
            required_string(sources, "consolidated_decisions", "decision_sources"),
            source,
        ),
        consolidated_decisions_sha256=required_string(
            sources, "consolidated_decisions_sha256", "decision_sources"
        ),
        categorical_decisions_path=resolve_path(
            required_string(sources, "categorical_decisions", "decision_sources"),
            source,
        ),
        categorical_decisions_sha256=required_string(
            sources, "categorical_decisions_sha256", "decision_sources"
        ),
        candidate_run_id=required_string(
            evidence, "candidate_run_id", "required_evidence"
        ),
        consolidated_audit_run_id=required_string(
            evidence, "consolidated_audit_run_id", "required_evidence"
        ),
        categorical_review_run_id=required_string(
            evidence, "categorical_review_run_id", "required_evidence"
        ),
        consolidated_private_version=required_string(
            evidence, "consolidated_private_artifact_version", "required_evidence"
        ),
        categorical_private_version=required_string(
            evidence, "categorical_private_artifact_version", "required_evidence"
        ),
        consolidated_private_directory_name=required_string(
            evidence, "consolidated_private_directory_name", "required_evidence"
        ),
        categorical_private_directory_name=required_string(
            evidence, "categorical_private_directory_name", "required_evidence"
        ),
        provenance_fields=tuple(provenance_fields),
        representation_overrides=tuple(sorted(representation_overrides)),
        generated_outputs=tuple(sorted(generated_outputs)),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
        ),
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
        ),
        source_path=source,
    )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _write_json(path: Path, value: Any, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _scoped_values(
    values: tuple[tuple[str, tuple[Any, ...]], ...], hospital: str
) -> tuple[Any, ...]:
    return dict(values).get(hospital, ())


def _decimal_integer(token: str) -> int | None:
    try:
        value = Decimal(token.strip())
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite() or value != value.to_integral_value():
        return None
    return int(value)


def resolve_reviewed_categorical_token(
    rule: ReviewedCategoricalRule,
    hospital: str,
    token: str | None,
) -> tuple[bool, Any | None]:
    if token is None:
        return True, None
    if token in _scoped_values(rule.missing_values_by_hospital, hospital):
        return True, None
    if rule.parser == "integral_numeric_then_hospital_code_allowlist":
        value = _decimal_integer(token)
        allowed = _scoped_values(rule.allowed_values_by_hospital, hospital)
        return value is not None and value in allowed, value
    if rule.parser == "integral_binary_numeric":
        value = _decimal_integer(token)
        return value in {0, 1}, None if value is None else bool(value)
    if rule.parser == "direct_numeric_exact_reviewed_domain":
        try:
            value = float(token.strip())
        except ValueError:
            return False, None
        resolved = math.isfinite(value) and any(
            abs(value - float(candidate)) <= 1.0e-12
            for candidate in rule.allowed_values
        )
        return resolved, value if resolved else None
    if rule.parser == "all_missing_only":
        return False, None
    if rule.parser == "exact_hospital_scoped_domain":
        return token in _scoped_values(rule.allowed_values_by_hospital, hospital), token
    if rule.parser == "exact_domain_with_scoped_missing_sentinel":
        return token in rule.allowed_values, token
    if rule.parser in {
        "exact_reviewed_domain",
        "previously_reviewed_study_phase",
    }:
        return token in rule.allowed_values, token
    if rule.parser == "previously_reviewed_nullable_boolean":
        normalized = token.strip().casefold()
        if normalized in {"false", "0", "0.0"}:
            return True, False
        if normalized in {"true", "1", "1.0"}:
            return True, True
        return False, None
    return False, None


def validate_categorical_domain_evidence(
    decisions: ReviewedCategoricalDecisions,
    domain_rows: list[dict[str, Any]],
    list_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    scalar_rules = {
        (rule.table, rule.variable): rule
        for rule in decisions.rules
        if rule.parser != "previously_reviewed_fixed_position_composite"
    }
    observed_keys = {
        (str(row.get("table")), str(row.get("variable"))) for row in domain_rows
    }
    if observed_keys != set(scalar_rules):
        raise HarmonizationError("Categorical domain coverage changed")
    summaries: list[dict[str, Any]] = []
    for key, rule in sorted(scalar_rules.items()):
        rows = [
            row
            for row in domain_rows
            if (str(row.get("table")), str(row.get("variable"))) == key
        ]
        unresolved = 0
        output_null = 0
        output_non_null = 0
        for row in rows:
            count = int(row.get("count", 0))
            token = None if bool(row.get("value_is_null")) else str(row.get("value_repr"))
            resolved, output = resolve_reviewed_categorical_token(
                rule, str(row.get("hospital")), token
            )
            if not resolved:
                unresolved += count
            elif output is None:
                output_null += count
            else:
                output_non_null += count
        expected_non_null = rule.expected_non_null_count
        expected_null = rule.expected_null_count
        passes = (
            unresolved == 0
            and (expected_non_null is None or output_non_null == expected_non_null)
            and (expected_null is None or output_null == expected_null)
            and not any(bool(row.get("domain_truncated")) for row in rows)
        )
        summaries.append(
            {
                "table": rule.table,
                "variable": rule.variable,
                "physical_type": rule.physical_type,
                "unit": rule.unit,
                "analysis_eligible": rule.analysis_eligible,
                "input_domain_row_count": len(rows),
                "output_non_null_count": output_non_null,
                "output_null_count": output_null,
                "unresolved_count": unresolved,
                "passes": passes,
            }
        )
    composite_rules = [
        rule
        for rule in decisions.rules
        if rule.parser == "previously_reviewed_fixed_position_composite"
    ]
    for rule in composite_rules:
        matching = [
            row
            for row in list_rows
            if row.get("table") == rule.table and row.get("variable") == rule.variable
        ]
        passes = bool(matching) and all(
            row.get("minimum_list_length") in {None, 2}
            and row.get("maximum_list_length") in {None, 2}
            and "[2]" in str(row.get("arrow_type"))
            for row in matching
        )
        summaries.append(
            {
                "table": rule.table,
                "variable": rule.variable,
                "physical_type": rule.physical_type,
                "unit": rule.unit,
                "analysis_eligible": rule.analysis_eligible,
                "input_domain_row_count": len(matching),
                "output_non_null_count": None,
                "output_null_count": None,
                "unresolved_count": 0 if passes else 1,
                "passes": passes,
            }
        )
    return sorted(summaries, key=lambda row: (row["table"], row["variable"]))


def _dictionary_overlay(
    rows: list[dict[str, Any]],
    consolidated: ReviewedConsolidatedDecisions,
    categorical: ReviewedCategoricalDecisions,
    representation_overrides: tuple[tuple[str, str, str, str, str], ...] = (),
) -> list[dict[str, Any]]:
    approved_units = {
        (table, variable): unit
        for table, variable, unit in consolidated.approved_units
    }
    conversion_units: dict[tuple[str, str], str] = {}
    for conversion in consolidated.conversions:
        key = (conversion.table, conversion.variable)
        existing = conversion_units.get(key)
        if existing is not None and existing != conversion.output_unit:
            raise HarmonizationError(
                "Approved hospital conversions disagree on their output unit"
            )
        conversion_units[key] = conversion.output_unit
    for key, unit in conversion_units.items():
        if key in approved_units and approved_units[key] != unit:
            raise HarmonizationError(
                "Approved unit and hospital-conversion output unit disagree"
            )
    overrides = {
        (table, variable): (unit, eligibility, caveat)
        for table, variable, unit, eligibility, caveat in representation_overrides
    }
    result: list[dict[str, Any]] = []
    for row in sorted(
        rows, key=lambda item: (str(item["table"]), int(item["ordered_position"]))
    ):
        table = str(row["table"])
        variable = str(row["variable"])
        key = (table, variable)
        categorical_rule = categorical.rule_for(table, variable)
        if categorical_rule is not None:
            physical_type = PHYSICAL_TYPE_OVERRIDES[categorical_rule.physical_type]
            unit = categorical_rule.unit
            eligibility = (
                "eligible" if categorical_rule.analysis_eligible else "ineligible"
            )
            caveat = categorical_rule.caveat
            definition = CATEGORICAL_DEFINITIONS[variable]
            definition_status = "reviewed_categorical_definition"
        else:
            physical_type = str(row.get("candidate_arrow_type"))
            if key in approved_units:
                unit = approved_units[key]
            elif key in conversion_units:
                unit = conversion_units[key]
            elif str(row.get("definition_status", "")).startswith(
                "reviewed_generated_representation"
            ):
                unit = str(row.get("candidate_unit"))
            elif str(row.get("candidate_unit")) == "not_applicable":
                unit = "not_applicable"
            else:
                unit = "unresolved"
            eligibility = "eligible" if unit != "unresolved" else "ineligible"
            caveat = (
                None
                if eligibility == "eligible"
                else "unit_or_source_definition_unresolved"
            )
            definition = str(row.get("definition"))
            definition_status = str(row.get("definition_status"))
        if variable in consolidated.all_missing_variables:
            eligibility = "ineligible"
            caveat = "globally_all_missing_in_reviewed_export"
        if variable in {"ie_ratio", "ie_ratio_set"}:
            eligibility = "ineligible"
            caveat = "ratio_direction_i_to_e_versus_e_to_i_unresolved"
        if variable in {"vt_per_kg", "vt_per_kg_ideal_body_weight"}:
            eligibility = "conditionally_ineligible"
            caveat = "asic_UK06_mixed_scale_requires_cleaned_layer_policy"
        if variable == "delta_p_reported":
            caveat = "keep_separate_from_future_derived_delta_p_computed"
        if key in overrides:
            unit, eligibility, caveat = overrides[key]
        result.append(
            {
                "table": table,
                "ordered_position": int(row["ordered_position"]),
                "variable": variable,
                "physical_type": physical_type,
                "unit": unit,
                "definition": definition,
                "definition_status": definition_status,
                "analysis_eligibility": eligibility,
                "analysis_caveat": caveat,
                "all_missing": variable in consolidated.all_missing_variables,
                "source_hospitals": row.get("source_hospitals"),
                "source_hospital_count": row.get("source_hospital_count"),
                "source_strategy": row.get("source_strategy"),
                "categorical_contract_version": (
                    categorical.version if categorical_rule is not None else None
                ),
                "publication_ready": False,
            }
        )
    return result


def _add_reviewed_generated_outputs(
    rows: list[dict[str, Any]],
    generated_outputs: tuple[
        tuple[str, str, str, str, str, str, str, str, str, str], ...
    ],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result = [dict(row) for row in rows]
    failures: list[dict[str, Any]] = []
    for (
        table,
        variable,
        insert_after,
        physical_type,
        unit,
        eligibility,
        definition,
        caveat,
        source_variable,
        anchor,
    ) in generated_outputs:
        table_rows = [row for row in result if str(row.get("table")) == table]
        variables = [str(row.get("variable")) for row in table_rows]
        if variable in variables or variables.count(source_variable) != 1:
            failures.append(
                {
                    "check": "reviewed_generated_output_inputs_are_complete",
                    "details": "Every reviewed generated output must be absent from the source-backed dictionary and have exactly one reviewed source variable.",
                }
            )
            continue
        if insert_after not in variables or insert_after != source_variable:
            failures.append(
                {
                    "check": "reviewed_generated_output_order_is_resolved",
                    "details": "Every reviewed generated output must have one deterministic insertion point equal to its source variable.",
                }
            )
            continue
        source_row = next(
            row for row in table_rows if str(row.get("variable")) == source_variable
        )
        source_position = int(source_row["ordered_position"])
        for row in result:
            if (
                str(row.get("table")) == table
                and int(row["ordered_position"]) > source_position
            ):
                row["ordered_position"] = int(row["ordered_position"]) + 1
        result.append(
            {
                "table": table,
                "ordered_position": source_position + 1,
                "variable": variable,
                "candidate_arrow_type": physical_type,
                "candidate_value_type": physical_type,
                "candidate_unit": unit,
                "definition": definition,
                "definition_status": "reviewed_generated_representation_requires_final_approval",
                "value_type_review_status": "reviewed_existing_v2_representation",
                "unit_review_status": "reviewed_existing_v2_representation",
                "source_hospitals": source_row.get("source_hospitals"),
                "source_hospital_count": source_row.get("source_hospital_count"),
                "source_review_item_ids": (),
                "source_raw_names": (),
                "source_occurrence_count": 0,
                "source_strategy": (
                    f"reviewed_generated_from:{source_variable};"
                    f"operation:anchor_plus_minutes;anchor:{anchor}"
                ),
                "candidate_stage": "proposed_harmonized_union_schema",
                "publication_ready": False,
            }
        )
    return result, failures


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 ordered-schema and variable-dictionary freeze review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input candidate: `{payload['candidate_run_id']}`",
        f"- Consolidated audit: `{payload['consolidated_audit_run_id']}`",
        f"- Categorical review: `{payload['categorical_review_run_id']}`",
        "- Candidate rows rescanned: `0`",
        "- Candidate data modified: `false`",
        "",
        "## Blocking findings",
        "",
    ]
    lines.extend(
        f"- `{item['check']}`: {item['details']}"
        for item in payload["blocking_findings"]
    )
    lines.extend(("", "## Review totals", ""))
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    for table in ("static", "dynamic"):
        lines.extend(("", f"## Proposed ordered {table} schema", ""))
        lines.append("| # | Variable | Physical type | Unit | Eligibility |")
        lines.append("|---:|---|---|---|---|")
        for row in payload["schemas"][table]:
            lines.append(
                f"| {row['ordered_position']} | `{row['variable']}` | `{row['physical_type']}` | `{row['unit']}` | `{row['analysis_eligibility']}` |"
            )
        lines.extend(("", "Operational provenance fields appended after clinical fields:", ""))
        lines.extend(
            f"- `{item['name']}`: `{item['physical_type']}`"
            for item in payload["operational_provenance_fields"]
        )
    lines.extend(
        (
            "",
            "## Human review gate",
            "",
            "Approve or revise the complete ordered schemas and dictionary. Unresolved units and definitions are preserved explicitly and make the affected variables analysis-ineligible; they do not authorize guessed conversions.",
            "",
            "## Limitations",
            "",
            "- This command reuses immutable report evidence and reads no candidate clinical rows.",
            "- Schema approval does not itself build, publish, clean, or derive data.",
            "- The existing candidate remains immutable; approved transformations require a new harmonized build.",
            "",
        )
    )
    return "\n".join(lines)


def run_schema_dictionary_review(
    config: SchemaDictionaryReviewConfig,
    run_id: str | None = None,
) -> SchemaDictionaryReviewResult:
    selected_run = default_run_id() if run_id is None else run_id
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid schema/dictionary review run ID")
    policy = load_schema_dictionary_review_policy(config.policy_path)
    if sha256_file(policy.consolidated_decisions_path) != policy.consolidated_decisions_sha256:
        raise HarmonizationError("Reviewed consolidated decisions changed")
    if sha256_file(policy.categorical_decisions_path) != policy.categorical_decisions_sha256:
        raise HarmonizationError("Reviewed categorical decisions changed")
    consolidated = load_reviewed_consolidated_decisions(
        policy.consolidated_decisions_path
    )
    categorical = load_reviewed_categorical_decisions(
        policy.categorical_decisions_path
    )
    run_triplet = (
        policy.candidate_run_id,
        policy.consolidated_audit_run_id,
        policy.categorical_review_run_id,
    )
    if run_triplet != (
        consolidated.candidate_run_id,
        consolidated.consolidated_audit_run_id,
        categorical.categorical_review_run_id,
    ) or run_triplet != (
        categorical.candidate_run_id,
        categorical.consolidated_audit_run_id,
        categorical.categorical_review_run_id,
    ):
        raise HarmonizationError("Reviewed decision evidence runs disagree")

    consolidated_dir = (
        config.reports_root
        / "private"
        / policy.consolidated_private_directory_name
        / policy.consolidated_audit_run_id
    )
    categorical_dir = (
        config.reports_root
        / "private"
        / policy.categorical_private_directory_name
        / policy.categorical_review_run_id
    )
    consolidated_manifest_path = consolidated_dir / "consolidated_audit_manifest.json"
    categorical_manifest_path = categorical_dir / "categorical_review_manifest.json"
    consolidated_manifest = _read_json(
        consolidated_manifest_path, "Consolidated audit manifest"
    )
    categorical_manifest = _read_json(
        categorical_manifest_path, "Categorical review manifest"
    )
    if (
        consolidated_manifest.get("artifact_version")
        != policy.consolidated_private_version
        or consolidated_manifest.get("run_id") != policy.consolidated_audit_run_id
        or consolidated_manifest.get("candidate_run_id") != policy.candidate_run_id
        or categorical_manifest.get("artifact_version")
        != policy.categorical_private_version
        or categorical_manifest.get("run_id") != policy.categorical_review_run_id
        or categorical_manifest.get("consolidated_audit_run_id")
        != policy.consolidated_audit_run_id
        or categorical_manifest.get("candidate_run_id") != policy.candidate_run_id
    ):
        raise HarmonizationError("Immutable review manifest lineage changed")
    consolidated_tables = consolidated_manifest.get("table_sha256")
    if not isinstance(consolidated_tables, dict):
        raise HarmonizationError("Consolidated table hashes are absent")
    dictionary_path = consolidated_dir / "candidate_variable_dictionary.parquet"
    list_path = consolidated_dir / "list_profiles.parquet"
    domain_path = categorical_dir / "categorical_domain_review.parquet"
    for path in (dictionary_path, list_path):
        if consolidated_tables.get(path.name) != sha256_file(path):
            raise HarmonizationError(f"Immutable consolidated evidence changed: {path.name}")
    if categorical_manifest.get("categorical_domain_review_sha256") != sha256_file(
        domain_path
    ):
        raise HarmonizationError("Immutable categorical domain evidence changed")

    dictionary_source_rows = pq.read_table(dictionary_path).to_pylist()
    list_rows = pq.read_table(list_path).to_pylist()
    domain_rows = pq.read_table(domain_path).to_pylist()
    categorical_evidence = validate_categorical_domain_evidence(
        categorical, domain_rows, list_rows
    )
    technical: list[dict[str, Any]] = []
    expected_dictionary_count = int(
        consolidated_manifest.get("table_row_counts", {}).get(
            "candidate_variable_dictionary.parquet", -1
        )
    )
    dictionary_keys = {
        (str(row.get("table")), str(row.get("variable")))
        for row in dictionary_source_rows
    }
    categorical_keys = {(rule.table, rule.variable) for rule in categorical.rules}
    if (
        expected_dictionary_count != len(dictionary_source_rows)
        or len(dictionary_keys) != len(dictionary_source_rows)
        or not categorical_keys.issubset(dictionary_keys)
    ):
        technical.append(
            {
                "check": "candidate_dictionary_coverage_is_complete",
                "details": "The immutable candidate dictionary must retain its complete unique row count and cover every approved categorical variable.",
            }
        )
    if any(not bool(row["passes"]) for row in categorical_evidence):
        technical.append(
            {
                "check": "approved_categorical_contract_matches_complete_evidence",
                "details": "Every observed categorical token and count must resolve under the approved complete contract.",
            }
        )
    augmented_dictionary_rows, generated_failures = _add_reviewed_generated_outputs(
        dictionary_source_rows, policy.generated_outputs
    )
    technical.extend(generated_failures)
    dictionary_rows = _dictionary_overlay(
        augmented_dictionary_rows,
        consolidated,
        categorical,
        policy.representation_overrides,
    )
    keys = [(row["table"], row["variable"]) for row in dictionary_rows]
    order_valid = len(keys) == len(set(keys)) and all(
        [row["ordered_position"] for row in dictionary_rows if row["table"] == table]
        == list(
            range(
                1,
                1
                + sum(row["table"] == table for row in dictionary_rows),
            )
        )
        for table in ("static", "dynamic")
    )
    if not order_valid:
        technical.append(
            {
                "check": "ordered_dictionary_is_unique_and_contiguous",
                "details": "Every table-variable must occur once in a contiguous deterministic order.",
            }
        )
    human = [
        {
            "check": "ordered_harmonized_union_schema_approved",
            "details": "The complete ordered static and dynamic schemas require explicit freeze approval.",
        },
        {
            "check": "variable_dictionary_approved",
            "details": "The complete definitions, types, units, availability, eligibility, caveats, and provenance require explicit freeze approval.",
        },
    ]
    blockers = technical + human
    schemas = {
        table: [
            {
                "ordered_position": row["ordered_position"],
                "variable": row["variable"],
                "physical_type": row["physical_type"],
                "unit": row["unit"],
                "analysis_eligibility": row["analysis_eligibility"],
            }
            for row in dictionary_rows
            if row["table"] == table
        ]
        for table in ("static", "dynamic")
    }
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_schema_dictionary_freeze_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "candidate_run_id": policy.candidate_run_id,
        "consolidated_audit_run_id": policy.consolidated_audit_run_id,
        "categorical_review_run_id": policy.categorical_review_run_id,
        "overall_status": "fail" if blockers else "pass",
        "blocking_findings": blockers,
        "metrics": {
            "clinical_variable_count": len(dictionary_rows),
            "source_backed_clinical_variable_count": len(dictionary_source_rows),
            "reviewed_generated_variable_count": len(policy.generated_outputs),
            "static_variable_count": len(schemas["static"]),
            "dynamic_variable_count": len(schemas["dynamic"]),
            "operational_provenance_field_count_per_table": len(
                policy.provenance_fields
            ),
            "categorical_rule_count": len(categorical.rules),
            "categorical_evidence_failure_count": sum(
                not bool(row["passes"]) for row in categorical_evidence
            ),
            "approved_named_unit_count": len(consolidated.approved_units),
            "approved_conversion_output_unit_variable_count": len(
                {(item.table, item.variable) for item in consolidated.conversions}
            ),
            "unresolved_unit_variable_count": sum(
                row["unit"] == "unresolved" for row in dictionary_rows
            ),
            "analysis_eligible_variable_count": sum(
                row["analysis_eligibility"] == "eligible" for row in dictionary_rows
            ),
            "analysis_ineligible_variable_count": sum(
                row["analysis_eligibility"] == "ineligible" for row in dictionary_rows
            ),
            "conditionally_ineligible_variable_count": sum(
                row["analysis_eligibility"] == "conditionally_ineligible"
                for row in dictionary_rows
            ),
            "globally_all_missing_variable_count": sum(
                bool(row["all_missing"]) for row in dictionary_rows
            ),
            "technical_blocking_finding_count": len(technical),
            "candidate_rows_rescanned": 0,
        },
        "schemas": schemas,
        "operational_provenance_fields": [
            {"name": name, "physical_type": physical_type}
            for name, physical_type in policy.provenance_fields
        ],
        "publication": {
            "candidate_mutated": False,
            "schema_frozen": False,
            "dictionary_frozen": False,
            "harmonized_artifact_generated": False,
            "publication_ready": False,
        },
    }
    assert_review_payload_is_safe(review_payload)
    private_dir = (
        config.reports_root / "private" / policy.private_directory_name / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Schema/dictionary review run already exists")
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    dictionary_output = private_dir / "proposed_variable_dictionary.parquet"
    categorical_output = private_dir / "categorical_contract_validation.parquet"
    _write_private_parquet(dictionary_output, tuple(dictionary_rows))
    _write_private_parquet(categorical_output, tuple(categorical_evidence))
    dictionary_output.chmod(0o600)
    categorical_output.chmod(0o600)
    _write_json(
        private_dir / "schema_dictionary_review_manifest.json",
        {
            "artifact": "asic_v3_schema_dictionary_freeze_review_private",
            "artifact_version": policy.private_artifact_version,
            "dataset_context": config.dataset_context,
            "generated_at_utc": generated,
            "run_id": selected_run,
            "candidate_run_id": policy.candidate_run_id,
            "decision_source_sha256": {
                "consolidated": policy.consolidated_decisions_sha256,
                "categorical": policy.categorical_decisions_sha256,
            },
            "source_manifest_sha256": {
                "consolidated": sha256_file(consolidated_manifest_path),
                "categorical": sha256_file(categorical_manifest_path),
            },
            "output_sha256": {
                dictionary_output.name: sha256_file(dictionary_output),
                categorical_output.name: sha256_file(categorical_output),
            },
            "publication_ready": False,
        },
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return SchemaDictionaryReviewResult(
        run_id=selected_run,
        overall_status=review_payload["overall_status"],
        blocking_findings=tuple(blockers),
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
