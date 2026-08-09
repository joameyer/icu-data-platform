from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    SchemaDictionaryFreezeConfig,
    SchemaDictionaryFreezePolicy,
    _schemas_payload,
    _validate_dictionary,
    load_schema_dictionary_freeze_config,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.audit import _write_json, _write_text
from asic_pipeline.unit_resolution.reviewed_decisions import (
    ReviewedUnitDecisions,
    load_reviewed_unit_decisions,
)
from asic_pipeline.unit_resolution.reviewed_medication_semantics import (
    ReviewedMedicationSemantics,
    load_reviewed_medication_semantics,
)


EXPECTED_BOUNDARY = {
    "read_frozen_contract_0_1": True,
    "read_unit_decision_audit_manifest": True,
    "read_clinical_rows": False,
    "modify_frozen_contract_0_1": False,
    "freeze_contract_0_2": False,
    "build_harmonized_data": False,
    "build_cleaned_data": False,
    "build_derived_data": False,
    "write_reports_only": True,
    "filter_rows_or_stays": False,
    "drop_source_values": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class UnitSchemaAmendmentPolicy:
    version: str
    reviewed_decisions_path: Path
    reviewed_decisions_sha256: str
    reviewed_medication_semantics_path: Path
    reviewed_medication_semantics_sha256: str
    baseline_freeze_policy_path: Path
    baseline_freeze_policy_sha256: str
    baseline_contract_version: str
    proposed_contract_version: str
    audit_run_id: str
    expected: dict[str, int]
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class UnitSchemaAmendmentConfig:
    freeze: SchemaDictionaryFreezeConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.freeze.dataset_context

    @property
    def data_root(self) -> Path:
        return self.freeze.data_root

    @property
    def reports_root(self) -> Path:
        return self.freeze.reports_root


@dataclass(frozen=True)
class UnitSchemaAmendmentResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _read_json_value(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc


def load_unit_schema_amendment_config(
    path: str | Path,
) -> UnitSchemaAmendmentConfig:
    source = Path(path).expanduser().resolve()
    freeze = load_schema_dictionary_freeze_config(source)
    raw = load_yaml_mapping(source, "Unit schema/dictionary amendment configuration")
    policy_path = resolve_path(
        required_string(
            raw, "unit_schema_dictionary_amendment_policy", "config"
        ),
        source,
    )
    return UnitSchemaAmendmentConfig(freeze=freeze, policy_path=policy_path)


def load_unit_schema_amendment_policy(
    path: str | Path,
) -> UnitSchemaAmendmentPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Unit schema/dictionary amendment policy")
    if (
        raw.get("unit_schema_dictionary_amendment_policy_version") != "0.1"
        or raw.get("status")
        != "approved_for_report_only_schema_dictionary_0_2_planning"
    ):
        raise ConfigurationError("Unit schema/dictionary amendment policy is invalid")
    inputs = _mapping(raw.get("inputs"), "inputs")
    expected_raw = _mapping(raw.get("expected"), "expected")
    reporting = _mapping(raw.get("reporting"), "reporting")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Unit schema/dictionary amendment boundary changed")

    reviewed_path = resolve_path(
        required_string(inputs, "reviewed_unit_decisions", "inputs"), source
    )
    reviewed_sha = required_string(
        inputs, "reviewed_unit_decisions_sha256", "inputs"
    )
    medication_path = resolve_path(
        required_string(
            inputs,
            "reviewed_medication_value_semantics",
            "inputs",
        ),
        source,
    )
    medication_sha = required_string(
        inputs,
        "reviewed_medication_value_semantics_sha256",
        "inputs",
    )
    freeze_path = resolve_path(
        required_string(inputs, "baseline_freeze_policy", "inputs"), source
    )
    freeze_sha = required_string(
        inputs, "baseline_freeze_policy_sha256", "inputs"
    )
    if sha256_file(reviewed_path) != reviewed_sha:
        raise ConfigurationError("Reviewed unit decisions changed after approval")
    if sha256_file(medication_path) != medication_sha:
        raise ConfigurationError(
            "Reviewed medication value semantics changed after approval"
        )
    if sha256_file(freeze_path) != freeze_sha:
        raise ConfigurationError("Baseline frozen-contract policy changed")

    expected = {
        key: _positive_int(value, f"expected.{key}")
        for key, value in expected_raw.items()
    }
    required_expected = {
        "baseline_static_variable_count": 23,
        "baseline_dynamic_variable_count": 130,
        "reviewed_unit_update_count": 72,
        "approved_conversion_count": 9,
        "semantic_split_count": 9,
        "resolved_semantic_split_count": 6,
        "unresolved_source_split_count": 3,
        "proposed_static_variable_count": 23,
        "proposed_dynamic_variable_count": 139,
        "proposed_clinical_variable_count": 162,
        "baseline_medication_variable_count": 25,
        "semantic_split_medication_variable_count": 9,
        "proposed_medication_variable_count": 34,
        "approved_general_negative_medication_rule_count": 1,
        "audited_negative_medication_value_count": 201,
    }
    if expected != required_expected:
        raise ConfigurationError("Unit schema/dictionary amendment scope changed")
    audit_run_id = required_string(inputs, "unit_decision_audit_run_id", "inputs")
    if audit_run_id != "20260807T083117Z" or not RUN_ID_PATTERN.fullmatch(
        audit_run_id
    ):
        raise ConfigurationError("Unit schema amendment audit lineage changed")
    return UnitSchemaAmendmentPolicy(
        version="0.1",
        reviewed_decisions_path=reviewed_path,
        reviewed_decisions_sha256=reviewed_sha,
        reviewed_medication_semantics_path=medication_path,
        reviewed_medication_semantics_sha256=medication_sha,
        baseline_freeze_policy_path=freeze_path,
        baseline_freeze_policy_sha256=freeze_sha,
        baseline_contract_version=required_string(
            inputs, "baseline_contract_version", "inputs"
        ),
        proposed_contract_version=required_string(
            inputs, "proposed_contract_version", "inputs"
        ),
        audit_run_id=audit_run_id,
        expected=expected,
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
        ),
        source_path=source,
    )


def build_amended_schema_and_dictionary(
    baseline_schemas: dict[str, list[dict[str, Any]]],
    baseline_dictionary: list[dict[str, Any]],
    reviewed: ReviewedUnitDecisions,
    medication_semantics: ReviewedMedicationSemantics,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    if set(baseline_schemas) != {"static", "dynamic"}:
        raise HarmonizationError("Baseline schema tables changed")
    baseline_by_key = {
        (str(row["table"]), str(row["variable"])): dict(row)
        for row in baseline_dictionary
    }
    if len(baseline_by_key) != len(baseline_dictionary):
        raise HarmonizationError("Baseline dictionary contains duplicate variables")

    split_by_source = {
        (str(row["hospital"]), str(row["source_variable"])): row
        for row in reviewed.semantic_splits
    }
    split_caveats: dict[str, list[str]] = {}
    for (hospital, source), row in split_by_source.items():
        split_caveats.setdefault(source, []).append(
            f"{hospital} uses parallel variable {row['target_variable']}."
        )
    medication_variables = set(reviewed.medication_variables)
    split_medication_variables = {
        str(row["target_variable"]) for row in reviewed.semantic_splits
    }
    proposed_medication_variables = medication_variables | split_medication_variables

    schemas = {
        table: [dict(row) for row in baseline_schemas[table]]
        for table in ("static", "dynamic")
    }
    for field in schemas["dynamic"]:
        variable = str(field["variable"])
        decision = reviewed.base_candidate.variables.get(variable)
        if decision is not None:
            field["unit"] = decision["unit"]
            field["analysis_eligibility"] = "eligible"

    next_position = len(schemas["dynamic"]) + 1
    for split in reviewed.semantic_splits:
        eligibility = (
            "eligible"
            if split["analysis_eligibility"] == "eligible"
            else "ineligible"
        )
        schemas["dynamic"].append(
            {
                "ordered_position": next_position,
                "variable": split["target_variable"],
                "physical_type": "double",
                "unit": split["target_unit"],
                "analysis_eligibility": eligibility,
            }
        )
        next_position += 1

    dictionary: list[dict[str, Any]] = []
    for table in ("static", "dynamic"):
        for field in schemas[table]:
            key = (table, str(field["variable"]))
            if key not in baseline_by_key:
                continue
            row = dict(baseline_by_key[key])
            row.update(field)
            decision = reviewed.base_candidate.variables.get(str(field["variable"]))
            if decision is not None:
                row["definition"] = decision["description"]
                row["unit_provenance"] = "team_derived_not_hospital_supplied"
                row["unit_confidence"] = decision["confidence"]
                row["unit_evidence_basis"] = decision["evidence_basis"]
                existing_caveat = str(row.get("analysis_caveat") or "").strip()
                candidate_caveat = str(decision.get("caveat") or "").strip()
                additions = split_caveats.get(str(field["variable"]), [])
                row["analysis_caveat"] = " ".join(
                    part
                    for part in (existing_caveat, candidate_caveat, *additions)
                    if part
                )
            else:
                row["unit_provenance"] = "carried_from_frozen_contract_0.1"
                row["unit_confidence"] = None
                row["unit_evidence_basis"] = None
            row["analysis_eligibility_detail"] = row["analysis_eligibility"]
            row["semantic_split_hospital"] = None
            row["semantic_split_source_variable"] = None
            is_medication = str(field["variable"]) in proposed_medication_variables
            row["medication_or_therapy_variable"] = is_medication
            row["zero_semantics"] = (
                "explicit_observed_numeric_zero_preserved"
                if is_medication
                else None
            )
            row["null_semantics"] = (
                "unavailable_or_unobserved_not_assumed_zero"
                if is_medication
                else None
            )
            row["null_to_zero_imputation_allowed"] = (
                False if is_medication else None
            )
            row["zero_to_null_conversion_allowed"] = (
                False if is_medication else None
            )
            row["positive_exposure_candidate"] = (
                "value_greater_than_zero" if is_medication else None
            )
            row["carry_forward_policy"] = (
                "requires_separate_reviewed_contract" if is_medication else None
            )
            row["valid_min_inclusive"] = 0.0 if is_medication else None
            row["negative_value_cleaning_policy"] = (
                medication_semantics.negative_cleaning_rule["rule_id"]
                if is_medication
                else None
            )
            row["negative_values_preserved_in_harmonized"] = (
                True if is_medication else None
            )
            row["contract_version"] = "0.2-proposed"
            row["publication_ready"] = False
            dictionary.append(row)

    for split in reviewed.semantic_splits:
        eligibility = (
            "eligible"
            if split["analysis_eligibility"] == "eligible"
            else "ineligible"
        )
        dictionary.append(
            {
                "table": "dynamic",
                "ordered_position": next(
                    int(field["ordered_position"])
                    for field in schemas["dynamic"]
                    if field["variable"] == split["target_variable"]
                ),
                "variable": split["target_variable"],
                "physical_type": "double",
                "unit": split["target_unit"],
                "definition": split["description"],
                "definition_status": "reviewed_team_inferred_semantic_split",
                "analysis_eligibility": eligibility,
                "analysis_eligibility_detail": split["analysis_eligibility"],
                "analysis_caveat": (
                    "Source representation is hospital-scoped and must not be "
                    "automatically coalesced with the ordinary canonical variable."
                ),
                "all_missing": False,
                "source_hospitals": [split["hospital"]],
                "source_hospital_count": 1,
                "source_strategy": "hospital_semantic_split_preserve_unchanged",
                "unit_provenance": "team_derived_not_hospital_supplied",
                "unit_confidence": split["confidence"],
                "unit_evidence_basis": split["evidence_basis"],
                "semantic_split_hospital": split["hospital"],
                "semantic_split_source_variable": split["source_variable"],
                "medication_or_therapy_variable": True,
                "zero_semantics": "explicit_observed_numeric_zero_preserved",
                "null_semantics": "unavailable_or_unobserved_not_assumed_zero",
                "null_to_zero_imputation_allowed": False,
                "zero_to_null_conversion_allowed": False,
                "positive_exposure_candidate": "value_greater_than_zero",
                "carry_forward_policy": "requires_separate_reviewed_contract",
                "valid_min_inclusive": 0.0,
                "negative_value_cleaning_policy": (
                    medication_semantics.negative_cleaning_rule["rule_id"]
                ),
                "negative_values_preserved_in_harmonized": True,
                "contract_version": "0.2-proposed",
                "publication_ready": False,
            }
        )
    table_order = {"static": 0, "dynamic": 1}
    dictionary.sort(
        key=lambda row: (table_order[str(row["table"])], int(row["ordered_position"]))
    )
    if len(dictionary) != sum(len(values) for values in schemas.values()):
        raise HarmonizationError("Proposed dictionary and schemas differ in size")
    return schemas, dictionary


def _validate_baseline_contract(
    config: UnitSchemaAmendmentConfig,
    freeze_policy: SchemaDictionaryFreezePolicy,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], Path]:
    contract_dir = config.data_root / freeze_policy.output_directory
    manifest_path = contract_dir / "freeze_manifest.json"
    dictionary_path = contract_dir / "variable_dictionary.parquet"
    static_path = contract_dir / "static_schema.json"
    dynamic_path = contract_dir / "dynamic_schema.json"
    manifest = _read_json(manifest_path, "Frozen contract 0.1 manifest")
    expected_schemas = _schemas_payload(freeze_policy)
    static_value = _read_json_value(static_path, "Frozen static schema")
    dynamic_value = _read_json_value(dynamic_path, "Frozen dynamic schema")
    if (
        manifest.get("artifact") != "asic_v3_frozen_harmonized_schema_dictionary"
        or manifest.get("contract_version") != "0.1"
        or manifest.get("schema_frozen") is not True
        or manifest.get("dictionary_frozen") is not True
        or manifest.get("files", {}).get(dictionary_path.name)
        != sha256_file(dictionary_path)
        or manifest.get("files", {}).get(static_path.name) != sha256_file(static_path)
        or manifest.get("files", {}).get(dynamic_path.name)
        != sha256_file(dynamic_path)
        or static_value != expected_schemas["static"]
        or dynamic_value != expected_schemas["dynamic"]
    ):
        raise HarmonizationError("Frozen schema/dictionary contract 0.1 changed")
    _validate_dictionary(dictionary_path, expected_schemas)
    return expected_schemas, pq.read_table(dictionary_path).to_pylist(), manifest_path


def _validate_audit_manifest(
    config: UnitSchemaAmendmentConfig,
    policy: UnitSchemaAmendmentPolicy,
    reviewed: ReviewedUnitDecisions,
) -> Path:
    path = (
        config.reports_root
        / "private"
        / "unit_decision_audit"
        / policy.audit_run_id
        / "unit_decision_audit_manifest.json"
    )
    manifest = _read_json(path, "Reviewed unit-decision audit manifest")
    metrics = manifest.get("metrics")
    lineage = manifest.get("lineage")
    if (
        manifest.get("artifact") != "asic_v3_unit_decision_audit_private"
        or manifest.get("artifact_version") != "0.2"
        or manifest.get("run_id") != policy.audit_run_id
        or manifest.get("cleaned_release_id") != "20260806T114234Z"
        or not isinstance(metrics, dict)
        or metrics.get("conversion_count") != 9
        or metrics.get("mask_count") != 9
        or metrics.get("target_variable_count") != 16
        or not isinstance(lineage, dict)
        or lineage.get("candidate_unit_decisions_sha256")
        != reviewed.base_candidate_sha256
    ):
        raise HarmonizationError("Reviewed unit-decision audit lineage changed")
    return path


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 harmonized schema and dictionary 0.2 amendment review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input frozen contract: `{payload['baseline_contract_version']}`",
        f"- Proposed contract: `{payload['proposed_contract_version']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Clinical rows read: `0`",
        "- Clinical data written or modified: `false`",
        "",
        "## Proposed amendment",
        "",
        f"- Static variables: `{metrics['static_variable_count']}` (unchanged)",
        f"- Dynamic variables: `{metrics['dynamic_variable_count']}`",
        f"- Reviewed existing unit updates: `{metrics['reviewed_unit_update_count']}`",
        f"- New semantic-split variables: `{metrics['semantic_split_count']}`",
        f"- Eligible variables: `{metrics['analysis_eligible_variable_count']}`",
        f"- Ineligible variables: `{metrics['analysis_ineligible_variable_count']}`",
        f"- Conditionally ineligible variables: `{metrics['conditionally_ineligible_variable_count']}`",
        f"- Unresolved source-scale variables: `{metrics['unresolved_source_scale_variable_count']}`",
        f"- Medication/therapy variables with explicit zero/null semantics: `{metrics['medication_variable_count']}`",
        f"- General negative-medication cleaning rules approved: `{metrics['approved_general_negative_medication_rule_count']}`",
        f"- Current audited negative medication values: `{metrics['audited_negative_medication_value_count']}`",
        "",
        "## New variables",
        "",
    ]
    for row in payload["semantic_split_variables"]:
        lines.append(
            f"- `{row['variable']}`: `{row['unit']}`; {row['analysis_eligibility_detail']}; "
            f"source `{row['hospital']}.{row['source_variable']}`."
        )
    lines.extend(
        [
            "",
            "## Medication value semantics",
            "",
            "- Explicit numeric zero remains an observed zero.",
            "- Null remains unavailable or unobserved and is never assumed to mean zero or inactive treatment.",
            "- Harmonization conversions and semantic splits must conserve zero and null independently.",
            "- Positive exposure (`value > 0`) and any carry-forward state are downstream derived interpretations; carry-forward requires a separate reviewed contract.",
            "- Every finite negative medication/therapy dose, rate, or concentration is preserved in harmonized data and masked to null by the general cleaned-layer rule; current audited count: 201.",
            "",
            "## Human review gate",
            "",
            "Review the complete owner-only proposed dictionary and ordered schemas. Explicit approval is required before contract 0.2 can be frozen or any clinical data can be rebuilt.",
            "",
            "## Boundary",
            "",
            "The 0.1 contract and all released data remain unchanged. This command reads no clinical row, activates no cleaning range, writes reports only, and authorizes no external export.",
            "",
        ]
    )
    return "\n".join(lines)


def run_unit_schema_amendment_review(
    config: UnitSchemaAmendmentConfig,
    run_id: str | None = None,
) -> UnitSchemaAmendmentResult:
    if config.dataset_context != "production":
        raise HarmonizationError("Unit schema/dictionary amendment is production-only")
    policy = load_unit_schema_amendment_policy(config.policy_path)
    reviewed = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    medication_semantics = load_reviewed_medication_semantics(
        policy.reviewed_medication_semantics_path
    )
    freeze_policy = load_schema_dictionary_freeze_policy(
        policy.baseline_freeze_policy_path
    )
    if (
        freeze_policy.contract_version != policy.baseline_contract_version
        or policy.proposed_contract_version != "0.2"
    ):
        raise HarmonizationError("Unit schema amendment contract lineage changed")
    baseline_schemas, baseline_dictionary, baseline_manifest = (
        _validate_baseline_contract(config, freeze_policy)
    )
    audit_manifest = _validate_audit_manifest(config, policy, reviewed)
    schemas, dictionary = build_amended_schema_and_dictionary(
        baseline_schemas,
        baseline_dictionary,
        reviewed,
        medication_semantics,
    )
    expected = policy.expected
    if (
        len(schemas["static"]) != expected["proposed_static_variable_count"]
        or len(schemas["dynamic"]) != expected["proposed_dynamic_variable_count"]
        or len(dictionary) != expected["proposed_clinical_variable_count"]
    ):
        raise HarmonizationError("Proposed schema/dictionary counts changed")

    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid unit schema amendment run ID")
    private_dir = (
        config.reports_root
        / "private"
        / policy.private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Unit schema amendment run ID already exists")

    eligibility_counts = {
        value: sum(row["analysis_eligibility"] == value for row in dictionary)
        for value in ("eligible", "ineligible", "conditionally_ineligible")
    }
    metrics = {
        "static_variable_count": len(schemas["static"]),
        "dynamic_variable_count": len(schemas["dynamic"]),
        "clinical_variable_count": len(dictionary),
        "reviewed_unit_update_count": len(reviewed.base_candidate.variables),
        "approved_conversion_count": len(reviewed.approved_harmonization_action_ids),
        "semantic_split_count": len(reviewed.semantic_splits),
        "resolved_semantic_split_count": sum(
            row["analysis_eligibility"] == "eligible"
            for row in reviewed.semantic_splits
        ),
        "unresolved_source_scale_variable_count": sum(
            row["unit"] == "unresolved_source_scale" for row in dictionary
        ),
        "analysis_eligible_variable_count": eligibility_counts["eligible"],
        "analysis_ineligible_variable_count": eligibility_counts["ineligible"],
        "conditionally_ineligible_variable_count": eligibility_counts[
            "conditionally_ineligible"
        ],
        "medication_variable_count": sum(
            row.get("medication_or_therapy_variable") is True
            for row in dictionary
        ),
        "approved_general_negative_medication_rule_count": 1,
        "audited_negative_medication_value_count": medication_semantics.evidence[
            "negative_value_count"
        ],
        "clinical_rows_read": 0,
        "technical_blocking_finding_count": 0,
    }
    if (
        metrics["resolved_semantic_split_count"]
        != expected["resolved_semantic_split_count"]
        or metrics["unresolved_source_scale_variable_count"]
        != expected["unresolved_source_split_count"]
        or metrics["medication_variable_count"]
        != expected["proposed_medication_variable_count"]
        or metrics["approved_general_negative_medication_rule_count"]
        != expected["approved_general_negative_medication_rule_count"]
        or metrics["audited_negative_medication_value_count"]
        != expected["audited_negative_medication_value_count"]
    ):
        raise HarmonizationError("Proposed semantic-split accounting changed")
    split_summary = [
        {
            "variable": row["target_variable"],
            "unit": row["target_unit"],
            "analysis_eligibility_detail": row["analysis_eligibility"],
            "hospital": row["hospital"],
            "source_variable": row["source_variable"],
        }
        for row in reviewed.semantic_splits
    ]
    blocker = {
        "check": "ordered_schema_dictionary_0_2_approved",
        "details": "The complete ordered schema and variable dictionary amendment requires explicit data-owner approval before freeze.",
    }
    generated = utc_timestamp()
    payload = {
        "artifact": "asic_v3_unit_schema_dictionary_amendment_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "baseline_contract_version": policy.baseline_contract_version,
        "proposed_contract_version": policy.proposed_contract_version,
        "unit_decision_audit_run_id": policy.audit_run_id,
        "overall_status": "pending_human_review",
        "blocking_findings": [blocker],
        "technical_blocking_findings": [],
        "metrics": metrics,
        "semantic_split_variables": split_summary,
        "schema_frozen": False,
        "dictionary_frozen": False,
        "clinical_rows_read": 0,
        "clinical_data_written": False,
        "existing_releases_modified": False,
        "cleaning_ranges_activated": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
        "privacy": {
            "contains_patient_rows": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_headers": False,
        },
    }
    assert_review_payload_is_safe(payload)

    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    dictionary_path = private_dir / "proposed_variable_dictionary.parquet"
    static_path = private_dir / "proposed_static_schema.json"
    dynamic_path = private_dir / "proposed_dynamic_schema.json"
    _write_private_parquet(dictionary_path, tuple(dictionary))
    dictionary_path.chmod(0o600)
    _write_json(static_path, schemas["static"], 0o600)
    _write_json(dynamic_path, schemas["dynamic"], 0o600)
    manifest = {
        "artifact": "asic_v3_unit_schema_dictionary_amendment_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "baseline_contract_version": policy.baseline_contract_version,
        "proposed_contract_version": policy.proposed_contract_version,
        "lineage_sha256": {
            "baseline_freeze_manifest": sha256_file(baseline_manifest),
            "unit_decision_audit_manifest": sha256_file(audit_manifest),
            "reviewed_unit_decisions": sha256_file(reviewed.source_path),
            "reviewed_medication_value_semantics": sha256_file(
                medication_semantics.source_path
            ),
            "policy": sha256_file(policy.source_path),
        },
        "output_sha256": {
            dictionary_path.name: sha256_file(dictionary_path),
            static_path.name: sha256_file(static_path),
            dynamic_path.name: sha256_file(dynamic_path),
        },
        "metrics": metrics,
        "schema_frozen": False,
        "dictionary_frozen": False,
        "clinical_data_written": False,
        "publication_ready": False,
    }
    _write_json(private_dir / "schema_amendment_manifest.json", manifest, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, payload, 0o640)
    _write_text(review_md, _markdown(payload), 0o640)
    return UnitSchemaAmendmentResult(
        run_id=selected_run,
        overall_status="pending_human_review",
        blocking_findings=(blocker,),
        technical_blocking_findings=(),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
