from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build import (
    HarmonizedBuildConfig,
    HarmonizedBuildPolicy,
    _anchored_time_array,
    _categorical_array,
    _convert_numeric,
    _load_frozen_schemas,
    _read_json,
    load_harmonized_build_config,
    load_harmonized_build_policy,
)
from asic_pipeline.harmonization.categorical_decisions import (
    load_reviewed_categorical_decisions,
)
from asic_pipeline.harmonization.consolidated_decisions import (
    load_reviewed_consolidated_decisions,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


@dataclass(frozen=True)
class HarmonizedBuildAuditPolicy:
    version: str
    build_policy_path: Path
    build_policy_sha256: str
    rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class HarmonizedBuildAuditConfig:
    build: HarmonizedBuildConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.build.dataset_context

    @property
    def data_root(self) -> Path:
        return self.build.data_root

    @property
    def reports_root(self) -> Path:
        return self.build.reports_root


@dataclass(frozen=True)
class HarmonizedBuildAuditResult:
    run_id: str
    harmonized_build_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def load_harmonized_build_audit_config(
    path: str | Path,
) -> HarmonizedBuildAuditConfig:
    source = Path(path).expanduser().resolve()
    build = load_harmonized_build_config(source)
    raw = load_yaml_mapping(source, "Harmonized build audit configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_build_audit_policy", "config"), source
    )
    return HarmonizedBuildAuditConfig(build, policy_path)


def load_harmonized_build_audit_policy(
    path: str | Path,
) -> HarmonizedBuildAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized build audit policy")
    if raw.get("harmonized_build_audit_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_complete_read_only_build_audit":
        raise ConfigurationError("Harmonized build audit policy is invalid")
    build_path = resolve_path(
        required_string(raw, "build_policy", "audit policy"), source
    )
    build_hash = required_string(raw, "build_policy_sha256", "audit policy")
    if sha256_file(build_path) != build_hash:
        raise ConfigurationError("Approved harmonized build policy changed")
    audit = _mapping(raw.get("audit"), "audit")
    expected_audit = {
        "rows_per_batch": 50000,
        "verify_input_and_output_hashes": True,
        "verify_frozen_schema_exactly": True,
        "verify_all_rows_and_hospital_order": True,
        "recompute_every_categorical_transformation": True,
        "recompute_every_hospital_unit_conversion": True,
        "recompute_every_artificial_timestamp": True,
        "compare_every_unchanged_value": True,
        "compare_all_five_provenance_fields": True,
        "verify_rule_level_counts": True,
    }
    if audit != expected_audit:
        raise ConfigurationError("Complete harmonized build audit scope changed")
    boundary = _mapping(raw.get("boundary"), "boundary")
    if boundary != {
        "read_candidate_clinical_rows": True,
        "read_harmonized_clinical_rows": True,
        "modify_clinical_data": False,
        "write_reports_only": True,
        "apply_cleaning": False,
        "apply_derivation": False,
        "approve_release_automatically": False,
        "publish": False,
    }:
        raise ConfigurationError("Harmonized build audit boundary changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    return HarmonizedBuildAuditPolicy(
        version="0.1",
        build_policy_path=build_path,
        build_policy_sha256=build_hash,
        rows_per_batch=50000,
        private_directory_name=required_string(
            outputs, "private_directory_name", "outputs"
        ),
        review_directory_name=required_string(
            outputs, "review_directory_name", "outputs"
        ),
        private_artifact_version=required_string(
            outputs, "private_artifact_version", "outputs"
        ),
        review_artifact_version=required_string(
            outputs, "review_artifact_version", "outputs"
        ),
        source_path=source,
    )


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


def _parquet_schema_matches(observed: pa.Schema, expected: pa.Schema) -> bool:
    # Parquet readers may normalize field nullability even though order, Arrow
    # physical types, and all contract metadata are preserved.
    return (
        observed.names == expected.names
        and observed.metadata == expected.metadata
        and all(
            left.type == right.type and left.metadata == right.metadata
            for left, right in zip(observed, expected, strict=True)
        )
    )


def _next_slice(
    iterator: Iterator[pa.RecordBatch],
    state: list[Any],
    length: int,
) -> pa.RecordBatch:
    parts: list[pa.RecordBatch] = []
    remaining = length
    while remaining:
        batch = state[0]
        offset = int(state[1])
        if batch is None or offset == batch.num_rows:
            try:
                batch = next(iterator)
            except StopIteration as exc:
                raise HarmonizationError("Harmonized output ended before candidate") from exc
            offset = 0
        take = min(remaining, batch.num_rows - offset)
        parts.append(batch.slice(offset, take))
        state[0] = batch
        state[1] = offset + take
        remaining -= take
    if len(parts) == 1:
        return parts[0]
    table = pa.Table.from_batches(parts).combine_chunks()
    return table.to_batches(max_chunksize=length)[0]


def _require_output_exhausted(
    iterator: Iterator[pa.RecordBatch], state: list[Any]
) -> None:
    batch = state[0]
    offset = int(state[1])
    if batch is not None and offset < batch.num_rows:
        raise HarmonizationError("Harmonized output contains extra rows")
    try:
        next(iterator)
    except StopIteration:
        return
    raise HarmonizationError("Harmonized output contains extra batches")


def _expected_array(
    candidate: pa.RecordBatch,
    field: pa.Field,
    hospital: str,
    table: str,
    build_policy: HarmonizedBuildPolicy,
    categorical: Any,
    conversion_by_key: dict[tuple[str, str, str], Any],
    category_counts: dict[tuple[str, str], Counter[str]],
    conversion_counts: Counter[str],
) -> pa.Array:
    name = field.name
    if name == build_policy.time_target:
        index = candidate.schema.get_field_index(build_policy.time_source)
        if index < 0:
            raise HarmonizationError("Audit cannot find artificial-time source")
        return _anchored_time_array(
            candidate.column(index), build_policy.time_anchor
        )
    index = candidate.schema.get_field_index(name)
    if index < 0:
        raise HarmonizationError(f"Audit cannot find candidate field: {table}.{name}")
    source = candidate.column(index)
    rule = categorical.rule_for(table, name)
    if rule is not None and rule.parser != "previously_reviewed_fixed_position_composite":
        return _categorical_array(
            source,
            rule,
            hospital,
            field.type,
            category_counts[(table, name)],
        )
    conversion = conversion_by_key.get((hospital, table, name))
    if conversion is not None:
        output = _convert_numeric(source, conversion)
        conversion_counts[conversion.decision_id] += len(output) - output.null_count
        return output
    return pc.cast(source, field.type, safe=True)


def _markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 harmonized build audit review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Harmonized build: `{payload['harmonized_build_run_id']}`",
            "- Technical audit status: **PASS**",
            "- Overall status: **PENDING HUMAN REVIEW**",
            "- Technical blocking findings: `0`",
            "- Human blocking findings: `1`",
            f"- Compared static rows: `{payload['metrics']['compared_static_rows']}`",
            f"- Compared dynamic rows: `{payload['metrics']['compared_dynamic_rows']}`",
            f"- Compared output cells: `{payload['metrics']['compared_output_cell_count']}`",
            "- Candidate data modified: `false`",
            "- Harmonized data modified: `false`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Human review gate",
            "",
            "The frozen schema, every row and unchanged value, every categorical transformation, all four unit conversions, artificial anchored time, and all provenance fields agree exactly. Explicit release approval is still required before promotion from the run-scoped harmonized candidate.",
            "",
        )
    )


def run_harmonized_build_audit(
    config: HarmonizedBuildAuditConfig,
    harmonized_build_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> HarmonizedBuildAuditResult:
    audit_policy = load_harmonized_build_audit_policy(config.policy_path)
    build_policy = load_harmonized_build_policy(audit_policy.build_policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved harmonized build audit is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run) or not RUN_ID_PATTERN.fullmatch(
        harmonized_build_run_id
    ):
        raise HarmonizationError("Invalid harmonized build audit run ID")
    consolidated = load_reviewed_consolidated_decisions(
        build_policy.consolidated_decisions_path
    )
    categorical = load_reviewed_categorical_decisions(
        build_policy.categorical_decisions_path
    )
    freeze_policy = load_schema_dictionary_freeze_policy(
        build_policy.freeze_policy_path
    )
    frozen_dir = config.data_root / build_policy.frozen_contract_directory
    frozen_schemas, _ = _load_frozen_schemas(frozen_dir, freeze_policy)

    build_dir = (
        config.data_root / build_policy.output_directory_name / harmonized_build_run_id
    )
    build_manifest_path = build_dir / "harmonized_build_manifest.json"
    build_manifest = _read_json(build_manifest_path, "Harmonized build manifest")
    if not isinstance(build_manifest, dict):
        raise HarmonizationError("Harmonized build manifest is not an object")
    if (
        build_manifest.get("artifact") != "asic_v3_harmonized_build_candidate"
        or build_manifest.get("artifact_version") != build_policy.artifact_version
        or build_manifest.get("run_id") != harmonized_build_run_id
        or build_manifest.get("status") != build_policy.output_status
        or build_manifest.get("publication_ready") is not False
        or build_manifest.get("cleaning_applied") is not False
        or build_manifest.get("derivation_applied") is not False
    ):
        raise HarmonizationError("Harmonized build manifest changed")
    candidate_root = (
        config.data_root
        / "harmonization_candidates"
        / build_policy.candidate_run_id
    )
    candidate_manifest_path = candidate_root / "candidate_run_manifest.json"
    candidate_manifest = _read_json(candidate_manifest_path, "Candidate manifest")
    if not isinstance(candidate_manifest, dict):
        raise HarmonizationError("Candidate manifest is not an object")
    candidate_hospitals = {
        str(row.get("hospital")): row
        for row in candidate_manifest.get("hospitals", [])
        if isinstance(row, dict)
    }
    if tuple(candidate_hospitals) != build_policy.hospitals:
        raise HarmonizationError("Candidate hospital order changed before audit")

    private_dir = (
        config.reports_root
        / "private"
        / audit_policy.private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / audit_policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Harmonized build audit run already exists")

    conversion_by_key = {
        (item.hospital, item.table, item.variable): item
        for item in consolidated.conversions
    }
    category_counts = {
        (rule.table, rule.variable): Counter() for rule in categorical.rules
    }
    conversion_counts: Counter[str] = Counter()
    compared_rows: Counter[str] = Counter()
    compared_cells = 0
    anchored_non_null = 0
    outputs = build_manifest.get("outputs", {})
    for table in ("static", "dynamic"):
        output_path = build_dir / f"{table}.parquet"
        output_summary = outputs.get(table, {}) if isinstance(outputs, dict) else {}
        if (
            not output_path.is_file()
            or output_summary.get("sha256") != sha256_file(output_path)
        ):
            raise HarmonizationError("Harmonized output hash changed")
        output_parquet = pq.ParquetFile(output_path)
        expected_schema = frozen_schemas[table]
        if not _parquet_schema_matches(output_parquet.schema_arrow, expected_schema):
            raise HarmonizationError("Harmonized output differs from frozen schema")
        output_iterator = output_parquet.iter_batches(
            batch_size=audit_policy.rows_per_batch, use_threads=True
        )
        output_state: list[Any] = [None, 0]
        for hospital in build_policy.hospitals:
            candidate_summary = candidate_hospitals[hospital]["outputs"][table]
            candidate_path = candidate_root / hospital / f"{table}.parquet"
            if (
                not candidate_path.is_file()
                or candidate_summary.get("sha256") != sha256_file(candidate_path)
            ):
                raise HarmonizationError("Candidate input hash changed during audit")
            candidate_parquet = pq.ParquetFile(candidate_path)
            for candidate_batch in candidate_parquet.iter_batches(
                batch_size=audit_policy.rows_per_batch, use_threads=True
            ):
                output_batch = _next_slice(
                    output_iterator, output_state, candidate_batch.num_rows
                )
                for field in expected_schema:
                    expected = _expected_array(
                        candidate_batch,
                        field,
                        hospital,
                        table,
                        build_policy,
                        categorical,
                        conversion_by_key,
                        category_counts,
                        conversion_counts,
                    )
                    observed = output_batch.column(
                        output_batch.schema.get_field_index(field.name)
                    )
                    if expected.type != field.type:
                        expected = pc.cast(expected, field.type, safe=True)
                    if field.name == build_policy.time_target:
                        anchored_non_null += len(expected) - expected.null_count
                    if not expected.equals(observed):
                        raise HarmonizationError(
                            f"Harmonized value mismatch: {hospital}.{table}.{field.name}"
                        )
                    compared_cells += candidate_batch.num_rows
                compared_rows[table] += candidate_batch.num_rows
                if (
                    progress is not None
                    and compared_rows[table]
                    % (audit_policy.rows_per_batch * 20)
                    == 0
                ):
                    progress(
                        f"harmonized_build_audit_progress table={table} "
                        f"rows={compared_rows[table]}"
                    )
            if progress is not None:
                progress(
                    f"harmonized_build_audit_table_complete hospital={hospital} table={table}"
                )
        _require_output_exhausted(output_iterator, output_state)
        if compared_rows[table] != dict(build_policy.expected_rows)[table]:
            raise HarmonizationError("Audited row count differs from approved total")

    categorical_unresolved = sum(
        counts["unresolved"] for counts in category_counts.values()
    )
    if categorical_unresolved:
        raise HarmonizationError("Audit recomputation found unresolved categorical cells")
    manifest_accounting = build_manifest.get("transformation_accounting", {})
    expected_categorical_accounting = {
        f"{table}.{variable}": dict(sorted(counts.items()))
        for (table, variable), counts in sorted(category_counts.items())
    }
    if (
        manifest_accounting.get("categorical") != expected_categorical_accounting
        or manifest_accounting.get("hospital_conversions")
        != dict(sorted(conversion_counts.items()))
        or manifest_accounting.get("anchored_time_non_null_count")
        != anchored_non_null
    ):
        raise HarmonizationError("Build rule-level accounting differs from audit")
    for rule in categorical.rules:
        if rule.parser == "previously_reviewed_fixed_position_composite":
            continue
        counts = category_counts[(rule.table, rule.variable)]
        if (
            rule.expected_non_null_count is not None
            and counts["output_non_null"] != rule.expected_non_null_count
        ) or (
            rule.expected_null_count is not None
            and counts["output_null"] != rule.expected_null_count
        ):
            raise HarmonizationError("Audited categorical counts changed")

    generated = utc_timestamp()
    blockers = (
        {
            "check": "harmonized_candidate_release_approved",
            "details": "The technically passing run-scoped harmonized candidate requires explicit human release approval before promotion.",
        },
    )
    metrics = {
        "compared_static_rows": compared_rows["static"],
        "compared_dynamic_rows": compared_rows["dynamic"],
        "compared_output_cell_count": compared_cells,
        "categorical_unresolved_count": categorical_unresolved,
        "conversion_rule_count": len(conversion_counts),
        "provenance_field_count": len(freeze_policy.provenance_fields),
    }
    private_payload = {
        "artifact": "asic_v3_harmonized_build_audit_private",
        "artifact_version": audit_policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "harmonized_build_run_id": harmonized_build_run_id,
        "build_manifest_sha256": sha256_file(build_manifest_path),
        "metrics": metrics,
        "categorical_accounting": expected_categorical_accounting,
        "conversion_accounting": dict(sorted(conversion_counts.items())),
        "clinical_data_modified": False,
        "publication_ready": False,
    }
    review_payload = {
        "artifact": "asic_v3_harmonized_build_audit_review",
        "artifact_version": audit_policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "harmonized_build_run_id": harmonized_build_run_id,
        "overall_status": "pending_human_review",
        "technical_status": "pass",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "candidate_data_modified": False,
        "harmonized_data_modified": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(private_dir / "harmonized_build_audit_manifest.json", private_payload, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return HarmonizedBuildAuditResult(
        run_id=selected_run,
        harmonized_build_run_id=harmonized_build_run_id,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
