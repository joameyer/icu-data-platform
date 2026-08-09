from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.schema_amendment import (
    UnitSchemaAmendmentConfig,
    load_unit_schema_amendment_config,
    load_unit_schema_amendment_policy,
)


APPROVAL_STATEMENT = (
    "I approve and freeze the ASIC v3 harmonized schema and variable dictionary "
    "contract 0.2 described by amendment review 20260807T101415Z, including the "
    "reviewed medication zero/null semantics and general negative medication "
    "cleaning rule."
)

EXPECTED_REVIEW_METRICS = {
    "static_variable_count": 23,
    "dynamic_variable_count": 139,
    "clinical_variable_count": 162,
    "reviewed_unit_update_count": 72,
    "approved_conversion_count": 9,
    "semantic_split_count": 9,
    "resolved_semantic_split_count": 6,
    "unresolved_source_scale_variable_count": 3,
    "analysis_eligible_variable_count": 152,
    "analysis_ineligible_variable_count": 9,
    "conditionally_ineligible_variable_count": 1,
    "medication_variable_count": 34,
    "approved_general_negative_medication_rule_count": 1,
    "audited_negative_medication_value_count": 201,
    "clinical_rows_read": 0,
    "technical_blocking_finding_count": 0,
}

EXPECTED_REFERENCE_CORRECTION = {
    "original_approval_reference": "20260807T101415Z",
    "original_reference_interpretation": (
        "generated_timestamp_without_punctuation"
    ),
    "corrected_source_review_run_id": "20260807T101321Z",
    "corrected_source_review_generated_at_utc": (
        "2026-08-07T10:14:15+00:00"
    ),
    "evidence": (
        "The data owner supplied the sanitized immutable-review index showing "
        "that run 20260807T101321Z was generated at the originally cited "
        "timestamp and has 139 dynamic variables, 34 medication variables, "
        "and 201 audited negative medication values. No contract 0.2 output "
        "existed."
    ),
}

EXPECTED_DICTIONARY_CONTRACT = {
    "static_variable_count": 23,
    "dynamic_variable_count": 139,
    "clinical_variable_count": 162,
    "operational_provenance_field_count_per_table": 5,
    "medication_variable_count": 34,
    "semantic_split_variable_count": 9,
    "unresolved_source_scale_variable_count": 3,
    "medication_valid_min_inclusive": 0.0,
    "medication_negative_cleaning_rule": (
        "mask_negative_medication_or_therapy_value"
    ),
    "expected_current_negative_medication_mask_count": 201,
}

EXPECTED_PROVENANCE_FIELDS = (
    ("__v3_source_file_id", "string"),
    ("__v3_source_file_order", "int32"),
    ("__v3_source_row_number", "int64"),
    ("__v3_source_order", "int64"),
    ("__v3_source_schema_variant_id", "string"),
)

EXPECTED_BOUNDARY = {
    "read_clinical_rows": False,
    "modify_frozen_contract_0_1": False,
    "modify_schema_amendment_evidence": False,
    "copy_approved_schema_dictionary_bytes_exactly": True,
    "generate_clinical_data": False,
    "activate_unit_conversions": False,
    "activate_semantic_splits": False,
    "activate_cleaning_rule": False,
    "authorize_harmonized_0_2_build_implementation": True,
    "authorize_publication": False,
    "authorize_external_data_export": False,
}

REQUIRED_DICTIONARY_COLUMNS = frozenset(
    {
        "table",
        "ordered_position",
        "variable",
        "physical_type",
        "unit",
        "definition",
        "definition_status",
        "analysis_eligibility",
        "analysis_eligibility_detail",
        "analysis_caveat",
        "source_strategy",
        "publication_ready",
        "contract_version",
        "medication_or_therapy_variable",
        "zero_semantics",
        "null_semantics",
        "null_to_zero_imputation_allowed",
        "zero_to_null_conversion_allowed",
        "positive_exposure_candidate",
        "carry_forward_policy",
        "valid_min_inclusive",
        "negative_value_cleaning_policy",
        "negative_values_preserved_in_harmonized",
        "semantic_split_hospital",
        "semantic_split_source_variable",
    }
)


@dataclass(frozen=True)
class UnitSchemaDictionaryFreezePolicy:
    version: str
    dataset_context: str
    approved_date: str
    reviewer_role: str
    source_review_run_id: str
    source_review_generated_at_utc: str
    approval_statement: str
    reference_correction: dict[str, str]
    amendment_policy_path: Path
    amendment_policy_sha256: str
    reviewed_unit_decisions_path: Path
    reviewed_unit_decisions_sha256: str
    reviewed_medication_semantics_path: Path
    reviewed_medication_semantics_sha256: str
    baseline_contract_version: str
    contract_version: str
    source_review_artifact_version: str
    source_private_artifact_version: str
    expected_metrics: dict[str, int]
    resolved_human_finding: str
    dictionary_contract: dict[str, Any]
    provenance_fields: tuple[tuple[str, str], ...]
    output_directory: Path
    review_directory: str
    freeze_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class UnitSchemaDictionaryFreezeConfig:
    amendment: UnitSchemaAmendmentConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.amendment.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.amendment.reports_root

    @property
    def data_root(self) -> Path:
        return self.amendment.data_root


@dataclass(frozen=True)
class UnitSchemaDictionaryFreezeResult:
    contract_version: str
    contract_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc


def _write_json_exclusive(path: Path, value: Any, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def _write_text_exclusive(path: Path, value: str, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)


def _copy_exclusive(source: Path, target: Path, mode: int) -> None:
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with source.open("rb") as input_stream, os.fdopen(
            descriptor, "wb"
        ) as output_stream:
            shutil.copyfileobj(input_stream, output_stream)
    except Exception:
        target.unlink(missing_ok=True)
        raise


def load_unit_schema_dictionary_freeze_config(
    path: str | Path,
) -> UnitSchemaDictionaryFreezeConfig:
    source = Path(path).expanduser().resolve()
    amendment = load_unit_schema_amendment_config(source)
    raw = load_yaml_mapping(source, "Unit schema/dictionary freeze configuration")
    policy_path = resolve_path(
        required_string(
            raw,
            "unit_schema_dictionary_freeze_policy",
            "config",
        ),
        source,
    )
    return UnitSchemaDictionaryFreezeConfig(
        amendment=amendment,
        policy_path=policy_path,
    )


def load_unit_schema_dictionary_freeze_policy(
    path: str | Path,
) -> UnitSchemaDictionaryFreezePolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed unit schema/dictionary freeze")
    if (
        raw.get("reviewed_unit_schema_dictionary_freeze_version") != "0.2"
        or raw.get("status") != "human_approved_exact_unit_schema_and_dictionary"
    ):
        raise ConfigurationError("Unit schema/dictionary freeze approval is invalid")
    approval = _mapping(raw.get("approval"), "approval")
    immutable = _mapping(raw.get("immutable_inputs"), "immutable_inputs")
    expected_metrics = _mapping(
        raw.get("expected_review_metrics"), "expected_review_metrics"
    )
    finding = _mapping(raw.get("resolved_human_finding"), "resolved_human_finding")
    dictionary_contract = _mapping(
        raw.get("dictionary_contract"), "dictionary_contract"
    )
    output = _mapping(raw.get("output"), "output")
    if _mapping(raw.get("freeze_boundary"), "freeze_boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Unit schema/dictionary freeze boundary changed")
    if expected_metrics != EXPECTED_REVIEW_METRICS:
        raise ConfigurationError("Approved schema amendment metrics changed")
    if dictionary_contract != EXPECTED_DICTIONARY_CONTRACT:
        raise ConfigurationError("Approved dictionary contract changed")
    if finding != {"check": "ordered_schema_dictionary_0_2_approved"}:
        raise ConfigurationError("Resolved schema amendment finding changed")
    statement = required_string(approval, "approval_statement", "approval")
    if statement != APPROVAL_STATEMENT:
        raise ConfigurationError("Data-owner contract 0.2 approval changed")
    reference_correction = _mapping(
        approval.get("reference_correction"),
        "approval.reference_correction",
    )
    if reference_correction != EXPECTED_REFERENCE_CORRECTION:
        raise ConfigurationError("Approved review-reference correction changed")
    source_run = required_string(approval, "source_review_run_id", "approval")
    if source_run != "20260807T101321Z":
        raise ConfigurationError("Approved schema amendment run changed")

    def immutable_file(path_key: str, sha_key: str) -> tuple[Path, str]:
        resolved = resolve_path(
            required_string(immutable, path_key, "immutable_inputs"), source
        )
        expected_sha = required_string(immutable, sha_key, "immutable_inputs")
        if sha256_file(resolved) != expected_sha:
            raise ConfigurationError(f"Immutable freeze input changed: {path_key}")
        return resolved, expected_sha

    amendment_path, amendment_sha = immutable_file(
        "schema_amendment_policy", "schema_amendment_policy_sha256"
    )
    unit_path, unit_sha = immutable_file(
        "reviewed_unit_decisions", "reviewed_unit_decisions_sha256"
    )
    medication_path, medication_sha = immutable_file(
        "reviewed_medication_value_semantics",
        "reviewed_medication_value_semantics_sha256",
    )
    provenance_raw = raw.get("operational_provenance_fields")
    if not isinstance(provenance_raw, list):
        raise ConfigurationError("Operational provenance fields must be a list")
    provenance = tuple(
        (
            required_string(
                _mapping(item, "operational_provenance_fields"),
                "name",
                "operational_provenance_fields",
            ),
            required_string(
                _mapping(item, "operational_provenance_fields"),
                "physical_type",
                "operational_provenance_fields",
            ),
        )
        for item in provenance_raw
    )
    if provenance != EXPECTED_PROVENANCE_FIELDS:
        raise ConfigurationError("Operational provenance contract changed")
    output_directory = Path(required_string(output, "directory", "output"))
    if output_directory.is_absolute() or ".." in output_directory.parts:
        raise ConfigurationError("Frozen contract output must be contained")
    policy = UnitSchemaDictionaryFreezePolicy(
        version="0.2",
        dataset_context=required_string(approval, "dataset_context", "approval"),
        approved_date=required_string(approval, "approved_utc_date", "approval"),
        reviewer_role=required_string(approval, "reviewer_role", "approval"),
        source_review_run_id=source_run,
        source_review_generated_at_utc=required_string(
            approval, "source_review_generated_at_utc", "approval"
        ),
        approval_statement=statement,
        reference_correction=dict(reference_correction),
        amendment_policy_path=amendment_path,
        amendment_policy_sha256=amendment_sha,
        reviewed_unit_decisions_path=unit_path,
        reviewed_unit_decisions_sha256=unit_sha,
        reviewed_medication_semantics_path=medication_path,
        reviewed_medication_semantics_sha256=medication_sha,
        baseline_contract_version=required_string(
            immutable, "baseline_contract_version", "immutable_inputs"
        ),
        contract_version=required_string(
            immutable, "contract_version", "immutable_inputs"
        ),
        source_review_artifact_version=required_string(
            immutable, "source_review_artifact_version", "immutable_inputs"
        ),
        source_private_artifact_version=required_string(
            immutable, "source_private_artifact_version", "immutable_inputs"
        ),
        expected_metrics=dict(expected_metrics),
        resolved_human_finding=str(finding["check"]),
        dictionary_contract=dict(dictionary_contract),
        provenance_fields=provenance,
        output_directory=output_directory,
        review_directory=required_string(output, "review_directory", "output"),
        freeze_artifact_version=required_string(
            output, "freeze_artifact_version", "output"
        ),
        source_path=source,
    )
    if (
        policy.dataset_context != "production"
        or policy.approved_date != "2026-08-07"
        or policy.reviewer_role != "data_owner"
        or policy.source_review_generated_at_utc != "2026-08-07T10:14:15+00:00"
        or policy.baseline_contract_version != "0.1"
        or policy.contract_version != "0.2"
        or policy.source_review_artifact_version != "0.1"
        or policy.source_private_artifact_version != "0.1"
        or policy.output_directory
        != Path("contracts/harmonized_schema_dictionary/0.2")
        or policy.freeze_artifact_version != "0.2"
    ):
        raise ConfigurationError("Approved contract 0.2 freeze scope changed")
    return policy


def _validate_schema_and_dictionary(
    dictionary_path: Path,
    static_path: Path,
    dynamic_path: Path,
    policy: UnitSchemaDictionaryFreezePolicy,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    static = _read_json(static_path, "Approved static schema")
    dynamic = _read_json(dynamic_path, "Approved dynamic schema")
    if not isinstance(static, list) or not isinstance(dynamic, list):
        raise HarmonizationError("Approved schema files must contain lists")
    schemas = {"static": static, "dynamic": dynamic}
    expected_counts = {
        "static": policy.dictionary_contract["static_variable_count"],
        "dynamic": policy.dictionary_contract["dynamic_variable_count"],
    }
    for table, fields in schemas.items():
        if len(fields) != expected_counts[table] or any(
            not isinstance(field, dict) for field in fields
        ):
            raise HarmonizationError("Approved ordered schema count changed")
        if [field.get("ordered_position") for field in fields] != list(
            range(1, len(fields) + 1)
        ) or len({field.get("variable") for field in fields}) != len(fields):
            raise HarmonizationError("Approved ordered schema is invalid")
    parquet = pq.ParquetFile(dictionary_path)
    if not REQUIRED_DICTIONARY_COLUMNS.issubset(parquet.schema_arrow.names):
        raise HarmonizationError("Approved dictionary columns are incomplete")
    rows = parquet.read().to_pylist()
    if len(rows) != policy.dictionary_contract["clinical_variable_count"]:
        raise HarmonizationError("Approved dictionary row count changed")
    observed: dict[str, list[dict[str, Any]]] = {"static": [], "dynamic": []}
    for row in rows:
        table = str(row.get("table"))
        if table not in observed:
            raise HarmonizationError("Approved dictionary contains unknown table")
        if (
            not isinstance(row.get("definition"), str)
            or not str(row["definition"]).strip()
            or not isinstance(row.get("source_strategy"), str)
            or not str(row["source_strategy"]).strip()
            or row.get("publication_ready") is not False
            or row.get("contract_version") != "0.2-proposed"
        ):
            raise HarmonizationError("Approved dictionary metadata changed")
        observed[table].append(
            {
                "ordered_position": int(row["ordered_position"]),
                "variable": str(row["variable"]),
                "physical_type": str(row["physical_type"]),
                "unit": str(row["unit"]),
                "analysis_eligibility": str(row["analysis_eligibility"]),
            }
        )
    for fields in observed.values():
        fields.sort(key=lambda item: item["ordered_position"])
    if observed != schemas:
        raise HarmonizationError("Approved dictionary differs from ordered schemas")
    medication_rows = [
        row for row in rows if row.get("medication_or_therapy_variable") is True
    ]
    if len(medication_rows) != policy.dictionary_contract["medication_variable_count"]:
        raise HarmonizationError("Approved medication dictionary scope changed")
    for row in medication_rows:
        if (
            row.get("zero_semantics")
            != "explicit_observed_numeric_zero_preserved"
            or row.get("null_semantics")
            != "unavailable_or_unobserved_not_assumed_zero"
            or row.get("null_to_zero_imputation_allowed") is not False
            or row.get("zero_to_null_conversion_allowed") is not False
            or row.get("positive_exposure_candidate") != "value_greater_than_zero"
            or row.get("carry_forward_policy")
            != "requires_separate_reviewed_contract"
            or float(row.get("valid_min_inclusive"))
            != policy.dictionary_contract["medication_valid_min_inclusive"]
            or row.get("negative_value_cleaning_policy")
            != policy.dictionary_contract["medication_negative_cleaning_rule"]
            or row.get("negative_values_preserved_in_harmonized") is not True
        ):
            raise HarmonizationError("Approved medication value semantics changed")
    if (
        sum(row.get("semantic_split_hospital") is not None for row in rows)
        != policy.dictionary_contract["semantic_split_variable_count"]
        or sum(row.get("unit") == "unresolved_source_scale" for row in rows)
        != policy.dictionary_contract["unresolved_source_scale_variable_count"]
        or any(row.get("unit") == "unresolved" for row in rows)
    ):
        raise HarmonizationError("Approved unit or semantic-split scope changed")
    return schemas, rows


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    return "\n".join(
        (
            "# ASIC v3 frozen harmonized schema and variable dictionary 0.2",
            "",
            f"- Frozen (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Contract version: `{payload['contract_version']}`",
            f"- Source amendment review: `{payload['source_review_run_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            "- Schema frozen: `true`",
            "- Dictionary frozen: `true`",
            "- Clinical rows read: `0`",
            "- Clinical data written or modified: `false`",
            "- Publication ready: `false`",
            "",
            "## Frozen contract",
            "",
            f"- Static clinical variables: `{metrics['static_variable_count']}`",
            f"- Dynamic clinical variables: `{metrics['dynamic_variable_count']}`",
            f"- Medication/therapy variables: `{metrics['medication_variable_count']}`",
            f"- Semantic-split variables: `{metrics['semantic_split_count']}`",
            "- Unresolved source-scale variables: "
            f"`{metrics['unresolved_source_scale_variable_count']}`",
            "- General negative-medication cleaning rule count: "
            f"`{metrics['approved_general_negative_medication_rule_count']}`",
            "- Audited current negative medication values: "
            f"`{metrics['audited_negative_medication_value_count']}`",
            "- Five operational provenance fields remain appended per table.",
            "",
            "## Boundary",
            "",
            "This freeze authorizes implementation of a new harmonized build "
            "under exact contract 0.2. It does not read or generate clinical "
            "data, activate conversions, semantic splits, or cleaning, modify "
            "contract 0.1 or any release, authorize publication, or authorize "
            "external export.",
            "",
        )
    )


def freeze_unit_schema_dictionary(
    config: UnitSchemaDictionaryFreezeConfig,
) -> UnitSchemaDictionaryFreezeResult:
    policy = load_unit_schema_dictionary_freeze_policy(config.policy_path)
    if config.dataset_context != policy.dataset_context:
        raise HarmonizationError("Unit schema/dictionary freeze context changed")
    review_dir = config.reports_root / "review/unit_schema_dictionary_amendment"
    private_dir = config.reports_root / "private/unit_schema_dictionary_amendment"
    review_json = review_dir / f"{policy.source_review_run_id}.json"
    source_private_dir = private_dir / policy.source_review_run_id
    source_manifest_path = source_private_dir / "schema_amendment_manifest.json"
    dictionary_path = source_private_dir / "proposed_variable_dictionary.parquet"
    static_path = source_private_dir / "proposed_static_schema.json"
    dynamic_path = source_private_dir / "proposed_dynamic_schema.json"
    source_review = _read_json(review_json, "Approved schema amendment review")
    source_manifest = _read_json(
        source_manifest_path, "Approved schema amendment private manifest"
    )
    if not isinstance(source_review, dict) or not isinstance(source_manifest, dict):
        raise HarmonizationError("Approved schema amendment evidence is invalid")
    amendment_policy = load_unit_schema_amendment_policy(
        policy.amendment_policy_path
    )
    blockers = source_review.get("blocking_findings")
    if (
        source_review.get("artifact")
        != "asic_v3_unit_schema_dictionary_amendment_review"
        or source_review.get("artifact_version")
        != policy.source_review_artifact_version
        or source_review.get("dataset_context") != policy.dataset_context
        or source_review.get("run_id") != policy.source_review_run_id
        or source_review.get("generated_at_utc")
        != policy.source_review_generated_at_utc
        or source_review.get("baseline_contract_version")
        != policy.baseline_contract_version
        or source_review.get("proposed_contract_version")
        != policy.contract_version
        or source_review.get("unit_decision_audit_run_id")
        != amendment_policy.audit_run_id
        or source_review.get("overall_status") != "pending_human_review"
        or not isinstance(blockers, list)
        or [item.get("check") for item in blockers if isinstance(item, dict)]
        != [policy.resolved_human_finding]
        or source_review.get("technical_blocking_findings") != []
        or source_review.get("metrics") != policy.expected_metrics
        or source_review.get("clinical_rows_read") != 0
        or source_review.get("clinical_data_written") is not False
        or source_review.get("schema_frozen") is not False
        or source_review.get("dictionary_frozen") is not False
        or source_review.get("existing_releases_modified") is not False
        or source_review.get("cleaning_ranges_activated") is not False
        or source_review.get("publication_ready") is not False
        or source_review.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved schema amendment review changed")
    baseline_manifest_path = (
        config.data_root
        / "contracts/harmonized_schema_dictionary"
        / policy.baseline_contract_version
        / "freeze_manifest.json"
    )
    unit_audit_manifest_path = (
        config.reports_root
        / "private/unit_decision_audit"
        / amendment_policy.audit_run_id
        / "unit_decision_audit_manifest.json"
    )
    baseline_manifest = _read_json(
        baseline_manifest_path, "Frozen baseline contract manifest"
    )
    _read_json(unit_audit_manifest_path, "Reviewed unit-decision audit manifest")
    if (
        not isinstance(baseline_manifest, dict)
        or baseline_manifest.get("contract_version")
        != policy.baseline_contract_version
        or baseline_manifest.get("schema_frozen") is not True
        or baseline_manifest.get("dictionary_frozen") is not True
    ):
        raise HarmonizationError("Frozen baseline contract changed")

    output_hashes = source_manifest.get("output_sha256")
    lineage = source_manifest.get("lineage_sha256")
    if (
        source_manifest.get("artifact")
        != "asic_v3_unit_schema_dictionary_amendment_private"
        or source_manifest.get("artifact_version")
        != policy.source_private_artifact_version
        or source_manifest.get("dataset_context") != policy.dataset_context
        or source_manifest.get("generated_at_utc")
        != policy.source_review_generated_at_utc
        or source_manifest.get("run_id") != policy.source_review_run_id
        or source_manifest.get("baseline_contract_version")
        != policy.baseline_contract_version
        or source_manifest.get("proposed_contract_version")
        != policy.contract_version
        or source_manifest.get("metrics") != policy.expected_metrics
        or not isinstance(output_hashes, dict)
        or output_hashes.get(dictionary_path.name) != sha256_file(dictionary_path)
        or output_hashes.get(static_path.name) != sha256_file(static_path)
        or output_hashes.get(dynamic_path.name) != sha256_file(dynamic_path)
        or not isinstance(lineage, dict)
        or lineage.get("baseline_freeze_manifest")
        != sha256_file(baseline_manifest_path)
        or lineage.get("unit_decision_audit_manifest")
        != sha256_file(unit_audit_manifest_path)
        or lineage.get("reviewed_unit_decisions")
        != policy.reviewed_unit_decisions_sha256
        or lineage.get("reviewed_medication_value_semantics")
        != policy.reviewed_medication_semantics_sha256
        or lineage.get("policy") != policy.amendment_policy_sha256
        or source_manifest.get("schema_frozen") is not False
        or source_manifest.get("dictionary_frozen") is not False
        or source_manifest.get("clinical_data_written") is not False
        or source_manifest.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved schema amendment private evidence changed")
    _validate_schema_and_dictionary(
        dictionary_path, static_path, dynamic_path, policy
    )

    contract_directory = config.data_root / policy.output_directory
    staging_directory = (
        contract_directory.parent / f".{contract_directory.name}.incomplete"
    )
    freeze_review_dir = config.reports_root / "review" / policy.review_directory
    freeze_review_json = freeze_review_dir / f"{policy.contract_version}.json"
    freeze_review_md = freeze_review_dir / f"{policy.contract_version}.md"
    if (
        contract_directory.exists()
        or staging_directory.exists()
        or freeze_review_json.exists()
        or freeze_review_md.exists()
    ):
        raise HarmonizationError("Frozen schema/dictionary contract 0.2 already exists")
    staging_directory.mkdir(parents=True, mode=0o750)
    staging_directory.chmod(0o750)
    freeze_review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    freeze_review_dir.chmod(0o750)
    copied = {
        "variable_dictionary.parquet": dictionary_path,
        "static_schema.json": static_path,
        "dynamic_schema.json": dynamic_path,
    }
    frozen_hashes: dict[str, str] = {}
    for name, source in copied.items():
        target = staging_directory / name
        _copy_exclusive(source, target, 0o640)
        frozen_hashes[name] = sha256_file(target)
        if frozen_hashes[name] != sha256_file(source):
            raise HarmonizationError("Frozen contract byte identity changed")
    generated = utc_timestamp()
    manifest_path = staging_directory / "freeze_manifest.json"
    manifest = {
        "artifact": "asic_v3_frozen_harmonized_schema_dictionary",
        "artifact_version": policy.freeze_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "contract_version": policy.contract_version,
        "approval": {
            "approved_utc_date": policy.approved_date,
            "reviewer_role": policy.reviewer_role,
            "source_review_run_id": policy.source_review_run_id,
            "approval_statement": policy.approval_statement,
            "reference_correction": policy.reference_correction,
        },
        "lineage": {
            "baseline_contract_version": policy.baseline_contract_version,
            "source_review_json_sha256": sha256_file(review_json),
            "source_private_manifest_sha256": sha256_file(source_manifest_path),
            "schema_amendment_policy_sha256": policy.amendment_policy_sha256,
            "reviewed_unit_decisions_sha256": policy.reviewed_unit_decisions_sha256,
            "reviewed_medication_value_semantics_sha256": (
                policy.reviewed_medication_semantics_sha256
            ),
            "freeze_policy_sha256": sha256_file(policy.source_path),
        },
        "files": frozen_hashes,
        "metrics": policy.expected_metrics,
        "operational_provenance_fields": [
            {"name": name, "physical_type": physical_type}
            for name, physical_type in policy.provenance_fields
        ],
        "schema_frozen": True,
        "dictionary_frozen": True,
        "harmonized_build_0_2_implementation_authorized": True,
        "unit_conversions_activated": False,
        "semantic_splits_activated": False,
        "cleaning_rule_activated": False,
        "clinical_rows_read": 0,
        "clinical_data_artifacts_generated": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json_exclusive(manifest_path, manifest, 0o640)
    staging_directory.replace(contract_directory)
    manifest_path = contract_directory / manifest_path.name
    review_payload = {
        "artifact": "asic_v3_unit_schema_dictionary_freeze_review",
        "artifact_version": policy.freeze_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "contract_version": policy.contract_version,
        "source_review_run_id": policy.source_review_run_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "metrics": policy.expected_metrics,
        "schema_frozen": True,
        "dictionary_frozen": True,
        "clinical_rows_read": 0,
        "clinical_data_written": False,
        "existing_contract_0_1_modified": False,
        "existing_releases_modified": False,
        "unit_conversions_activated": False,
        "semantic_splits_activated": False,
        "cleaning_rule_activated": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    _write_json_exclusive(freeze_review_json, review_payload, 0o640)
    _write_text_exclusive(freeze_review_md, _markdown(review_payload), 0o640)
    return UnitSchemaDictionaryFreezeResult(
        contract_version=policy.contract_version,
        contract_directory=contract_directory,
        manifest_path=manifest_path,
        review_json_path=freeze_review_json,
        review_markdown_path=freeze_review_md,
    )
