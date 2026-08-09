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
from asic_pipeline.harmonization.schema_dictionary_review import (
    SchemaDictionaryReviewConfig,
    load_schema_dictionary_review_config,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


RESOLVED_FINDINGS = frozenset(
    {
        "ordered_harmonized_union_schema_approved",
        "variable_dictionary_approved",
    }
)
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
        "analysis_caveat",
        "all_missing",
        "source_hospitals",
        "source_hospital_count",
        "source_strategy",
        "publication_ready",
    }
)


@dataclass(frozen=True)
class FrozenSchemaField:
    position: int
    name: str
    physical_type: str
    unit: str
    eligibility: str


@dataclass(frozen=True)
class SchemaDictionaryFreezePolicy:
    version: str
    dataset_context: str
    approved_date: str
    reviewer_role: str
    source_review_run_id: str
    approval_statement: str
    candidate_run_id: str
    consolidated_audit_run_id: str
    categorical_review_run_id: str
    review_artifact_version: str
    private_artifact_version: str
    expected_metrics: tuple[tuple[str, int], ...]
    schemas: tuple[tuple[str, tuple[FrozenSchemaField, ...]], ...]
    provenance_fields: tuple[tuple[str, str], ...]
    contract_version: str
    output_directory: Path
    review_directory: str
    source_path: Path


@dataclass(frozen=True)
class SchemaDictionaryFreezeConfig:
    review: SchemaDictionaryReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.review.reports_root

    @property
    def data_root(self) -> Path:
        return self.review.consolidated.data_root


@dataclass(frozen=True)
class SchemaDictionaryFreezeResult:
    contract_version: str
    contract_directory: Path
    manifest_path: Path
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


def load_schema_dictionary_freeze_config(
    path: str | Path,
) -> SchemaDictionaryFreezeConfig:
    source = Path(path).expanduser().resolve()
    review = load_schema_dictionary_review_config(source)
    raw = load_yaml_mapping(source, "Schema/dictionary freeze configuration")
    policy_path = resolve_path(
        required_string(raw, "schema_dictionary_freeze_policy", "config"), source
    )
    return SchemaDictionaryFreezeConfig(review, policy_path)


def load_schema_dictionary_freeze_policy(
    path: str | Path,
) -> SchemaDictionaryFreezePolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed schema/dictionary freeze")
    if raw.get("reviewed_schema_dictionary_freeze_version") != "0.1" or raw.get(
        "status"
    ) != "human_approved_exact_schema_and_dictionary":
        raise ConfigurationError("Schema/dictionary freeze approval is invalid")

    approval = _mapping(raw.get("approval"), "approval")
    statement = required_string(approval, "approval_statement", "approval")
    source_run = required_string(approval, "source_review_run_id", "approval")
    if source_run not in statement:
        raise ConfigurationError("Approval statement does not bind its review run")

    lineage = _mapping(raw.get("required_lineage"), "required_lineage")
    raw_metrics = _mapping(raw.get("expected_metrics"), "expected_metrics")
    metrics = tuple(
        sorted(
            (
                str(name),
                _positive_int(value, f"expected_metrics.{name}")
                if value != 0
                else 0,
            )
            for name, value in raw_metrics.items()
        )
    )
    if not metrics or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in raw_metrics.values()
    ):
        raise ConfigurationError("Expected freeze metrics are invalid")

    resolved = raw.get("resolved_human_findings")
    if not isinstance(resolved, list) or frozenset(resolved) != RESOLVED_FINDINGS:
        raise ConfigurationError("Resolved human findings changed")

    schema_root = _mapping(raw.get("approved_schema"), "approved_schema")
    schemas: list[tuple[str, tuple[FrozenSchemaField, ...]]] = []
    for table in ("static", "dynamic"):
        values = schema_root.get(table)
        if not isinstance(values, list) or not values:
            raise ConfigurationError(f"approved_schema.{table} must be a list")
        fields: list[FrozenSchemaField] = []
        for index, item in enumerate(values, start=1):
            location = f"approved_schema.{table}[{index - 1}]"
            value = _mapping(item, location)
            position = _positive_int(value.get("position"), f"{location}.position")
            eligibility = required_string(value, "eligibility", location)
            if eligibility not in {
                "eligible",
                "ineligible",
                "conditionally_ineligible",
            }:
                raise ConfigurationError(f"{location}.eligibility is invalid")
            fields.append(
                FrozenSchemaField(
                    position=position,
                    name=required_string(value, "name", location),
                    physical_type=required_string(value, "physical_type", location),
                    unit=required_string(value, "unit", location),
                    eligibility=eligibility,
                )
            )
        if [item.position for item in fields] != list(range(1, len(fields) + 1)):
            raise ConfigurationError(f"approved_schema.{table} is not contiguous")
        if len({item.name for item in fields}) != len(fields):
            raise ConfigurationError(f"approved_schema.{table} has duplicate names")
        schemas.append((table, tuple(fields)))

    provenance_values = raw.get("operational_provenance_fields")
    if not isinstance(provenance_values, list) or len(provenance_values) != 5:
        raise ConfigurationError("Operational provenance contract changed")
    provenance: list[tuple[str, str]] = []
    for index, item in enumerate(provenance_values):
        value = _mapping(item, f"operational_provenance_fields[{index}]")
        provenance.append(
            (
                required_string(value, "name", "operational_provenance_fields"),
                required_string(
                    value, "physical_type", "operational_provenance_fields"
                ),
            )
        )

    boundary = _mapping(raw.get("freeze_boundary"), "freeze_boundary")
    if boundary != {
        "read_clinical_candidate_rows": False,
        "modify_candidate_data": False,
        "generate_clinical_data": False,
        "freeze_exact_reviewed_dictionary": True,
        "authorize_harmonized_build_implementation": True,
        "authorize_publication": False,
    }:
        raise ConfigurationError("Schema/dictionary freeze boundary changed")

    output = _mapping(raw.get("output"), "output")
    output_directory = Path(required_string(output, "directory", "output"))
    if output_directory.is_absolute() or ".." in output_directory.parts:
        raise ConfigurationError("Freeze output directory must be relative and contained")

    return SchemaDictionaryFreezePolicy(
        version="0.1",
        dataset_context=required_string(
            approval, "dataset_context", "approval"
        ),
        approved_date=required_string(approval, "approved_utc_date", "approval"),
        reviewer_role=required_string(approval, "reviewer_role", "approval"),
        source_review_run_id=source_run,
        approval_statement=statement,
        candidate_run_id=required_string(
            lineage, "candidate_run_id", "required_lineage"
        ),
        consolidated_audit_run_id=required_string(
            lineage, "consolidated_audit_run_id", "required_lineage"
        ),
        categorical_review_run_id=required_string(
            lineage, "categorical_review_run_id", "required_lineage"
        ),
        review_artifact_version=required_string(
            lineage, "schema_review_artifact_version", "required_lineage"
        ),
        private_artifact_version=required_string(
            lineage, "schema_review_private_artifact_version", "required_lineage"
        ),
        expected_metrics=metrics,
        schemas=tuple(schemas),
        provenance_fields=tuple(provenance),
        contract_version=required_string(output, "contract_version", "output"),
        output_directory=output_directory,
        review_directory=required_string(output, "review_directory", "output"),
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


def _write_json_exclusive(path: Path, value: Any, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


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


def _schemas_payload(
    policy: SchemaDictionaryFreezePolicy,
) -> dict[str, list[dict[str, Any]]]:
    return {
        table: [
            {
                "ordered_position": item.position,
                "variable": item.name,
                "physical_type": item.physical_type,
                "unit": item.unit,
                "analysis_eligibility": item.eligibility,
            }
            for item in fields
        ]
        for table, fields in policy.schemas
    }


def _validate_dictionary(
    dictionary_path: Path,
    expected_schemas: dict[str, list[dict[str, Any]]],
) -> None:
    parquet = pq.ParquetFile(dictionary_path)
    if not REQUIRED_DICTIONARY_COLUMNS.issubset(parquet.schema_arrow.names):
        raise HarmonizationError("Reviewed variable dictionary columns are incomplete")
    rows = parquet.read().to_pylist()
    observed: dict[str, list[dict[str, Any]]] = {"static": [], "dynamic": []}
    for row in rows:
        table = str(row.get("table"))
        if table not in observed:
            raise HarmonizationError("Reviewed dictionary contains an unknown table")
        if (
            not isinstance(row.get("definition"), str)
            or not row["definition"].strip()
            or not isinstance(row.get("definition_status"), str)
            or not row["definition_status"].strip()
            or not isinstance(row.get("source_strategy"), str)
            or not row["source_strategy"].strip()
            or bool(row.get("publication_ready"))
        ):
            raise HarmonizationError(
                "Reviewed dictionary definition, provenance, or publication state changed"
            )
        observed[table].append(
            {
                "ordered_position": int(row["ordered_position"]),
                "variable": str(row["variable"]),
                "physical_type": str(row["physical_type"]),
                "unit": str(row["unit"]),
                "analysis_eligibility": str(row["analysis_eligibility"]),
            }
        )
    for values in observed.values():
        values.sort(key=lambda item: item["ordered_position"])
    if observed != expected_schemas:
        raise HarmonizationError("Reviewed dictionary differs from the approved schemas")


def _markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 frozen harmonized schema and variable dictionary",
            "",
            f"- Frozen (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Contract version: `{payload['contract_version']}`",
            f"- Source schema review: `{payload['source_review_run_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            "- Schema frozen: `true`",
            "- Dictionary frozen: `true`",
            "- Candidate rows rescanned: `0`",
            "- Candidate data modified: `false`",
            "- Production clinical data artifacts generated: `false`",
            "- Publication ready: `false`",
            "",
            "## Frozen contract",
            "",
            f"- Static clinical variables: `{payload['metrics']['static_variable_count']}`",
            f"- Dynamic clinical variables: `{payload['metrics']['dynamic_variable_count']}`",
            f"- Reviewed generated variables: `{payload['metrics']['reviewed_generated_variable_count']}`",
            f"- Unresolved-unit variables retained as analysis-ineligible: `{payload['metrics']['unresolved_unit_variable_count']}`",
            "- Five operational provenance fields remain appended per table.",
            "",
            "## Boundary",
            "",
            "This freeze authorizes implementation of a new harmonized build under this exact contract. It does not generate clinical data, apply cleaning or derivation, or authorize publication.",
            "",
        )
    )


def freeze_schema_dictionary(
    config: SchemaDictionaryFreezeConfig,
) -> SchemaDictionaryFreezeResult:
    policy = load_schema_dictionary_freeze_policy(config.policy_path)
    if config.dataset_context != policy.dataset_context or config.dataset_context != "production":
        raise HarmonizationError("Reviewed schema/dictionary freeze is production-only")

    review_dir = config.reports_root / "review" / "schema_dictionary_review"
    private_dir = config.reports_root / "private" / "schema_dictionary_review"
    review_json_path = review_dir / f"{policy.source_review_run_id}.json"
    source_private_dir = private_dir / policy.source_review_run_id
    source_manifest_path = source_private_dir / "schema_dictionary_review_manifest.json"
    dictionary_path = source_private_dir / "proposed_variable_dictionary.parquet"
    source_review = _read_json(review_json_path, "Approved schema review")
    source_manifest = _read_json(source_manifest_path, "Approved private manifest")
    expected_schemas = _schemas_payload(policy)

    blocker_names = {
        str(item.get("check"))
        for item in source_review.get("blocking_findings", [])
        if isinstance(item, dict)
    }
    expected_metrics = dict(policy.expected_metrics)
    if (
        source_review.get("artifact_version") != policy.review_artifact_version
        or source_review.get("run_id") != policy.source_review_run_id
        or source_review.get("candidate_run_id") != policy.candidate_run_id
        or source_review.get("consolidated_audit_run_id")
        != policy.consolidated_audit_run_id
        or source_review.get("categorical_review_run_id")
        != policy.categorical_review_run_id
        or source_review.get("overall_status") != "fail"
        or blocker_names != RESOLVED_FINDINGS
        or source_review.get("metrics") != expected_metrics
        or source_review.get("schemas") != expected_schemas
        or source_review.get("operational_provenance_fields")
        != [
            {"name": name, "physical_type": physical_type}
            for name, physical_type in policy.provenance_fields
        ]
    ):
        raise HarmonizationError("Approved schema review content or lineage changed")

    output_hashes = source_manifest.get("output_sha256")
    if (
        source_manifest.get("artifact_version") != policy.private_artifact_version
        or source_manifest.get("run_id") != policy.source_review_run_id
        or source_manifest.get("candidate_run_id") != policy.candidate_run_id
        or not isinstance(output_hashes, dict)
        or output_hashes.get(dictionary_path.name) != sha256_file(dictionary_path)
    ):
        raise HarmonizationError("Approved private dictionary evidence changed")
    _validate_dictionary(dictionary_path, expected_schemas)

    contract_directory = config.data_root / policy.output_directory
    staging_directory = (
        contract_directory.parent / f".{contract_directory.name}.incomplete"
    )
    freeze_review_directory = config.reports_root / "review" / policy.review_directory
    freeze_review_json = freeze_review_directory / f"{policy.contract_version}.json"
    freeze_review_md = freeze_review_directory / f"{policy.contract_version}.md"
    if (
        contract_directory.exists()
        or staging_directory.exists()
        or freeze_review_json.exists()
        or freeze_review_md.exists()
    ):
        raise HarmonizationError("Frozen schema/dictionary contract already exists")

    staging_directory.mkdir(parents=True, mode=0o750)
    staging_directory.chmod(0o750)
    freeze_review_directory.mkdir(parents=True, mode=0o750, exist_ok=True)
    freeze_review_directory.chmod(0o750)
    frozen_dictionary = staging_directory / "variable_dictionary.parquet"
    static_schema = staging_directory / "static_schema.json"
    dynamic_schema = staging_directory / "dynamic_schema.json"
    _copy_exclusive(dictionary_path, frozen_dictionary, 0o640)
    _write_json_exclusive(static_schema, expected_schemas["static"], 0o640)
    _write_json_exclusive(dynamic_schema, expected_schemas["dynamic"], 0o640)

    generated = utc_timestamp()
    manifest_path = staging_directory / "freeze_manifest.json"
    manifest = {
        "artifact": "asic_v3_frozen_harmonized_schema_dictionary",
        "artifact_version": "0.1",
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "contract_version": policy.contract_version,
        "approval": {
            "approved_utc_date": policy.approved_date,
            "reviewer_role": policy.reviewer_role,
            "source_review_run_id": policy.source_review_run_id,
            "approval_statement": policy.approval_statement,
        },
        "lineage": {
            "candidate_run_id": policy.candidate_run_id,
            "consolidated_audit_run_id": policy.consolidated_audit_run_id,
            "categorical_review_run_id": policy.categorical_review_run_id,
            "source_review_json_sha256": sha256_file(review_json_path),
            "source_private_manifest_sha256": sha256_file(source_manifest_path),
        },
        "files": {
            frozen_dictionary.name: sha256_file(frozen_dictionary),
            static_schema.name: sha256_file(static_schema),
            dynamic_schema.name: sha256_file(dynamic_schema),
        },
        "metrics": expected_metrics,
        "schema_frozen": True,
        "dictionary_frozen": True,
        "harmonized_build_implementation_authorized": True,
        "clinical_data_artifacts_generated": False,
        "publication_ready": False,
    }
    _write_json_exclusive(manifest_path, manifest, 0o640)
    staging_directory.replace(contract_directory)
    frozen_dictionary = contract_directory / frozen_dictionary.name
    static_schema = contract_directory / static_schema.name
    dynamic_schema = contract_directory / dynamic_schema.name
    manifest_path = contract_directory / manifest_path.name

    review_payload = {
        "artifact": "asic_v3_schema_dictionary_freeze_review",
        "artifact_version": "0.1",
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "contract_version": policy.contract_version,
        "source_review_run_id": policy.source_review_run_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "metrics": expected_metrics,
        "schema_frozen": True,
        "dictionary_frozen": True,
        "candidate_rows_rescanned": 0,
        "candidate_data_modified": False,
        "production_data_artifacts_generated": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    _write_json_exclusive(freeze_review_json, review_payload, 0o640)
    descriptor = os.open(freeze_review_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    return SchemaDictionaryFreezeResult(
        contract_version=policy.contract_version,
        contract_directory=contract_directory,
        manifest_path=manifest_path,
        review_json_path=freeze_review_json,
        review_markdown_path=freeze_review_md,
    )
