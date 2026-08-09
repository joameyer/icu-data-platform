from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.cleaning.cleaning_0_2 import (
    Cleaning02AuditConfig,
    _cleaned_schema,
    _load_input,
    _read_json,
    _schema_digest,
    load_cleaning_0_2_audit_config,
    load_cleaning_0_2_audit_policy,
    load_cleaning_0_2_policy,
)
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVAL_STATEMENT = (
    "i approve promotion of cleaned 0.2 candidate 20260807T154100Z under "
    "cleaning policy 0.2."
)
EXPECTED_METRIC_KEYS = (
    "compared_static_rows",
    "compared_dynamic_rows",
    "compared_output_cells",
    "baseline_scalar_rule_count",
    "priority_rule_count",
    "medication_variable_count",
    "priority_power_of_ten_correction_count",
    "priority_mask_count",
    "priority_nonpositive_mask_count",
    "priority_above_upper_mask_count",
    "positive_upper_values_preserved",
    "negative_medication_mask_count",
    "albumin_above_1000_mask_count",
    "post_clean_nonfinite_count",
    "post_clean_baseline_invalid_count",
    "post_clean_priority_invalid_count",
    "post_clean_negative_medication_count",
)
EXPECTED_BOUNDARY = {
    "copy_all_audited_candidate_bytes_unchanged": True,
    "preserve_candidate_artifacts": True,
    "preserve_all_previous_releases": True,
    "preserve_previous_current_pointer_bytes_in_new_release": True,
    "overwrite_existing_release": False,
    "require_exact_previous_current_pointer": True,
    "replace_current_pointer_atomically": True,
    "rerun_cleaning": False,
    "apply_harmonization": False,
    "apply_derivation": False,
    "filter_rows": False,
    "filter_stays": False,
    "drop_columns": False,
    "authorize_cleaned_layer": True,
    "authorize_derived_input": True,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class Cleaned02PromotionPolicy:
    version: str
    build_run_id: str
    audit_run_id: str
    harmonized_release_id: str
    harmonized_contract_version: str
    cleaning_policy_version: str
    candidate_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    cleaning_policy_path: Path
    cleaning_policy_sha256: str
    audit_policy_path: Path
    audit_policy_sha256: str
    approval_role: str
    approval_date: str
    approval_statement: str
    expected_metrics: dict[str, int]
    resolved_blocker: str
    previous_release_id: str
    previous_cleaning_policy_version: str
    current_release_pointer: Path
    release_id: str
    releases_directory: Path
    previous_pointer_snapshot_name: str
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class Cleaned02PromotionConfig:
    audit: Cleaning02AuditConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.audit.dataset_context

    @property
    def data_root(self) -> Path:
        return self.audit.data_root

    @property
    def reports_root(self) -> Path:
        return self.audit.reports_root


@dataclass(frozen=True)
class Cleaned02PromotionResult:
    release_id: str
    release_directory: Path
    release_manifest_path: Path
    current_release_pointer_path: Path
    previous_pointer_snapshot_path: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _nonnegative_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a nonnegative integer")
    return value


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_cleaned_0_2_promotion_config(
    path: str | Path,
) -> Cleaned02PromotionConfig:
    source = Path(path).expanduser().resolve()
    audit = load_cleaning_0_2_audit_config(source)
    raw = load_yaml_mapping(source, "Cleaned 0.2 promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "cleaned_0_2_promotion_policy", "config"), source
    )
    return Cleaned02PromotionConfig(audit=audit, policy_path=policy_path)


def load_cleaned_0_2_promotion_policy(
    path: str | Path,
) -> Cleaned02PromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaned 0.2 promotion policy")
    if (
        raw.get("cleaned_0_2_promotion_policy_version") != "0.1"
        or raw.get("status")
        != "explicitly_approved_for_immutable_cleaned_0_2_release"
    ):
        raise ConfigurationError("Cleaned 0.2 promotion approval is invalid")
    approved = _mapping(raw.get("approved_input"), "approved_input")
    immutable = _mapping(raw.get("immutable_policy"), "immutable_policy")
    human = _mapping(raw.get("human_approval"), "human_approval")
    expected = _mapping(raw.get("expected_audit"), "expected_audit")
    previous = _mapping(
        raw.get("previous_current_release"), "previous_current_release"
    )
    release = _mapping(raw.get("release"), "release")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Cleaned 0.2 promotion boundary changed")
    if (
        expected.get("technical_status") != "pass"
        or expected.get("overall_status_before_approval")
        != "pending_human_review"
        or expected.get("technical_blocking_finding_count") != 0
        or expected.get("human_blocking_finding_count_before_approval") != 1
    ):
        raise ConfigurationError("Expected cleaned 0.2 audit status changed")
    cleaning_path = resolve_path(
        required_string(
            immutable, "cleaning_0_2_policy", "immutable_policy"
        ),
        source,
    )
    cleaning_hash = required_string(
        immutable, "cleaning_0_2_policy_sha256", "immutable_policy"
    )
    audit_path = resolve_path(
        required_string(
            immutable, "cleaning_0_2_audit_policy", "immutable_policy"
        ),
        source,
    )
    audit_hash = required_string(
        immutable, "cleaning_0_2_audit_policy_sha256", "immutable_policy"
    )
    if (
        sha256_file(cleaning_path) != cleaning_hash
        or sha256_file(audit_path) != audit_hash
    ):
        raise ConfigurationError("An immutable cleaned 0.2 policy changed")
    statement = required_string(human, "statement", "human_approval")
    if statement != APPROVAL_STATEMENT:
        raise ConfigurationError("Data-owner cleaned 0.2 approval changed")
    expected_metrics = {
        key: _nonnegative_int(expected.get(key), f"expected_audit.{key}")
        for key in EXPECTED_METRIC_KEYS
    }
    policy = Cleaned02PromotionPolicy(
        version="0.1",
        build_run_id=required_string(approved, "build_run_id", "approved_input"),
        audit_run_id=required_string(approved, "audit_run_id", "approved_input"),
        harmonized_release_id=required_string(
            approved, "harmonized_release_id", "approved_input"
        ),
        harmonized_contract_version=required_string(
            approved, "harmonized_contract_version", "approved_input"
        ),
        cleaning_policy_version=required_string(
            approved, "cleaning_policy_version", "approved_input"
        ),
        candidate_artifact_version=required_string(
            approved, "candidate_artifact_version", "approved_input"
        ),
        audit_private_artifact_version=required_string(
            approved, "audit_private_artifact_version", "approved_input"
        ),
        audit_review_artifact_version=required_string(
            approved, "audit_review_artifact_version", "approved_input"
        ),
        cleaning_policy_path=cleaning_path,
        cleaning_policy_sha256=cleaning_hash,
        audit_policy_path=audit_path,
        audit_policy_sha256=audit_hash,
        approval_role=required_string(human, "role", "human_approval"),
        approval_date=required_string(
            human, "recorded_date", "human_approval"
        ),
        approval_statement=statement,
        expected_metrics=expected_metrics,
        resolved_blocker=required_string(
            expected,
            "human_blocking_check_resolved_by_this_approval",
            "expected_audit",
        ),
        previous_release_id=required_string(
            previous, "release_id", "previous_current_release"
        ),
        previous_cleaning_policy_version=required_string(
            previous, "cleaning_policy_version", "previous_current_release"
        ),
        current_release_pointer=_contained_relative_path(
            required_string(
                previous, "current_release_pointer", "previous_current_release"
            ),
            "previous_current_release.current_release_pointer",
        ),
        release_id=required_string(release, "release_id", "release"),
        releases_directory=_contained_relative_path(
            required_string(release, "releases_directory", "release"),
            "release.releases_directory",
        ),
        previous_pointer_snapshot_name=required_string(
            release, "prior_pointer_snapshot_name", "release"
        ),
        release_artifact_version=required_string(
            release, "artifact_version", "release"
        ),
        review_directory_name=required_string(
            release, "review_directory_name", "release"
        ),
        source_path=source,
    )
    if (
        policy.build_run_id != "20260807T154100Z"
        or policy.audit_run_id != policy.build_run_id
        or policy.release_id != policy.build_run_id
        or policy.harmonized_release_id != "20260807T112402Z"
        or policy.harmonized_contract_version != "0.2"
        or policy.cleaning_policy_version != "0.2"
        or policy.candidate_artifact_version != "0.2"
        or policy.audit_private_artifact_version != "0.2"
        or policy.audit_review_artifact_version != "0.2"
        or policy.approval_role != "data_owner"
        or policy.approval_date != "2026-08-08"
        or policy.resolved_blocker != "cleaned_0_2_candidate_release_approved"
        or policy.previous_release_id != "20260806T114234Z"
        or policy.previous_cleaning_policy_version != "0.1"
        or policy.release_artifact_version != "0.2"
        or release.get("current_release_pointer")
        != previous.get("current_release_pointer")
        or previous.get("require_ready_for_derivation") is not True
        or Path(policy.previous_pointer_snapshot_name).name
        != policy.previous_pointer_snapshot_name
    ):
        raise ConfigurationError("Approved cleaned 0.2 promotion scope changed")
    return policy


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


def _write_text(path: Path, value: str, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(value)
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["audit_metrics"]
    return "\n".join(
        (
            "# ASIC v3 cleaned 0.2 release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Cleaning policy: `{payload['cleaning_policy_version']}`",
            f"- Previous current release: `{payload['previous_release_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Compared output cells: `{metrics['compared_output_cells']}`",
            "- Audited Parquet and dictionary bytes preserved exactly: `true`",
            "- Previous current-pointer bytes preserved exactly: `true`",
            "- Previous releases preserved: `true`",
            "- Cleaned layer ready: `true`",
            "- Approved as derived-layer input: `true`",
            "- Cleaning rerun during promotion: `false`",
            "- Derivation applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human release gate for the exactly audited cleaned 0.2 candidate. Static and dynamic Parquet plus both cleaned-dictionary artifacts are copied byte-for-byte. The candidate, cleaned 0.1 release, harmonized releases, and derived 0.1 release remain preserved. Only the recoverable current-cleaned pointer advances.",
            "",
        )
    )


def _copy_exact(source: Path, destination: Path, expected_hash: str) -> str:
    before_size = source.stat().st_size
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
    observed_hash = sha256_file(destination)
    if (
        observed_hash != expected_hash
        or destination.stat().st_size != before_size
        or sha256_file(source) != expected_hash
    ):
        raise HarmonizationError("Audited candidate bytes changed during promotion")
    return observed_hash


def promote_cleaned_0_2_release(
    config: Cleaned02PromotionConfig,
) -> Cleaned02PromotionResult:
    policy = load_cleaned_0_2_promotion_policy(config.policy_path)
    audit_policy = load_cleaning_0_2_audit_policy(policy.audit_policy_path)
    cleaning_policy = load_cleaning_0_2_policy(policy.cleaning_policy_path)
    if (
        config.dataset_context != "production"
        or config.audit.policy_path.resolve() != policy.audit_policy_path.resolve()
        or config.audit.build.policy_path.resolve()
        != policy.cleaning_policy_path.resolve()
        or audit_policy.build_policy_path.resolve()
        != policy.cleaning_policy_path.resolve()
        or cleaning_policy.version != policy.cleaning_policy_version
        or cleaning_policy.release_id != policy.harmonized_release_id
        or cleaning_policy.contract_version != policy.harmonized_contract_version
        or cleaning_policy.artifact_version != policy.candidate_artifact_version
        or audit_policy.private_artifact_version
        != policy.audit_private_artifact_version
        or audit_policy.review_artifact_version
        != policy.audit_review_artifact_version
    ):
        raise HarmonizationError("Approved cleaned 0.2 promotion lineage changed")

    source = _load_input(config.audit.build, cleaning_policy)
    candidate_dir = (
        config.data_root
        / cleaning_policy.candidate_directory_name
        / policy.build_run_id
    )
    candidate_manifest_path = candidate_dir / "cleaning_0_2_manifest.json"
    candidate_manifest = _read_json(
        candidate_manifest_path, "Approved cleaning 0.2 candidate manifest"
    )
    if not isinstance(candidate_manifest, dict):
        raise HarmonizationError("Approved cleaned 0.2 candidate is invalid")
    lineage = candidate_manifest.get("lineage")
    if not isinstance(lineage, dict) or (
        candidate_manifest.get("artifact") != "asic_v3_cleaning_0_2_candidate"
        or candidate_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or candidate_manifest.get("dataset_context") != config.dataset_context
        or candidate_manifest.get("run_id") != policy.build_run_id
        or candidate_manifest.get("status") != cleaning_policy.candidate_status
        or lineage.get("harmonized_release_id") != policy.harmonized_release_id
        or lineage.get("harmonized_contract_version")
        != policy.harmonized_contract_version
        or lineage.get("cleaning_0_2_policy_sha256")
        != policy.cleaning_policy_sha256
        or candidate_manifest.get("row_filtering_applied") is not False
        or candidate_manifest.get("stay_filtering_applied") is not False
        or candidate_manifest.get("columns_dropped") is not False
        or candidate_manifest.get("height_aggregation_applied") is not False
        or candidate_manifest.get("cleaning_applied") is not True
        or candidate_manifest.get("derivation_applied") is not False
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved cleaned 0.2 candidate manifest changed")

    review_path = (
        config.reports_root
        / "review"
        / audit_policy.review_directory_name
        / f"{policy.audit_run_id}.json"
    )
    private_path = (
        config.reports_root
        / "private"
        / audit_policy.private_directory_name
        / policy.audit_run_id
        / "cleaning_0_2_audit_manifest.json"
    )
    review = _read_json(review_path, "Approved cleaned 0.2 audit review")
    private = _read_json(private_path, "Approved private cleaned 0.2 audit")
    if not isinstance(review, dict) or not isinstance(private, dict):
        raise HarmonizationError("Approved cleaned 0.2 audit evidence is invalid")
    metrics = review.get("metrics")
    blockers = review.get("blocking_findings")
    if (
        review.get("artifact") != "asic_v3_cleaning_0_2_audit_review"
        or review.get("artifact_version") != policy.audit_review_artifact_version
        or review.get("dataset_context") != config.dataset_context
        or review.get("run_id") != policy.audit_run_id
        or review.get("build_run_id") != policy.build_run_id
        or review.get("harmonized_release_id") != policy.harmonized_release_id
        or review.get("cleaning_policy_version") != policy.cleaning_policy_version
        or review.get("technical_status") != "pass"
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or not isinstance(blockers[0], dict)
        or blockers[0].get("check") != policy.resolved_blocker
        or not isinstance(metrics, dict)
        or any(
            metrics.get(key) != expected
            for key, expected in policy.expected_metrics.items()
        )
        or review.get("every_output_cell_recomputed") is not True
        or review.get("cleaned_dictionary_recomputed") is not True
        or review.get("uncertain_positive_extremes_preserved") is not True
        or review.get("rows_or_stays_filtered") is not False
        or review.get("columns_dropped") is not False
        or review.get("derivation_applied") is not False
        or review.get("candidate_data_modified") is not False
        or review.get("publication_ready") is not False
        or review.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved cleaned 0.2 audit review changed")
    candidate_manifest_hash = sha256_file(candidate_manifest_path)
    if (
        private.get("artifact") != "asic_v3_cleaning_0_2_audit_private"
        or private.get("artifact_version")
        != policy.audit_private_artifact_version
        or private.get("dataset_context") != config.dataset_context
        or private.get("run_id") != policy.audit_run_id
        or private.get("build_run_id") != policy.build_run_id
        or private.get("build_manifest_sha256") != candidate_manifest_hash
        or private.get("metrics") != metrics
        or private.get("rule_accounting")
        != candidate_manifest.get("rule_accounting")
        or private.get("source_data_modified") is not False
        or private.get("candidate_data_modified") is not False
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved private cleaned 0.2 audit changed")
    candidate_metrics = candidate_manifest.get("metrics")
    if not isinstance(candidate_metrics, dict) or any(
        metrics.get(key) != value for key, value in candidate_metrics.items()
    ):
        raise HarmonizationError("Cleaned 0.2 build and audit accounting differ")

    outputs = candidate_manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Approved cleaned 0.2 outputs are unavailable")
    expected_rows = {
        "static": policy.expected_metrics["compared_static_rows"],
        "dynamic": policy.expected_metrics["compared_dynamic_rows"],
    }
    candidate_files: dict[str, dict[str, Any]] = {}
    output_schemas = {
        table: _cleaned_schema(
            source.source_schemas[table], policy.harmonized_contract_version
        )
        for table in ("static", "dynamic")
    }
    for table in ("static", "dynamic"):
        path = candidate_dir / f"{table}.parquet"
        summary = outputs.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("An approved cleaned 0.2 table is unavailable")
        observed_hash = sha256_file(path)
        parquet = pq.ParquetFile(path)
        if (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(output_schemas[table])
            or not _parquet_schema_matches(
                parquet.schema_arrow, output_schemas[table]
            )
        ):
            raise HarmonizationError("An approved cleaned 0.2 table changed")
        candidate_files[table] = {
            "path": path,
            "sha256": observed_hash,
            "size_bytes": path.stat().st_size,
            "row_count": expected_rows[table],
            "schema_sha256": str(summary["schema_sha256"]),
        }

    dictionary_specs = (
        (
            "cleaned_variable_dictionary",
            cleaning_policy.dictionary_filename,
            True,
        ),
        (
            "cleaned_variable_dictionary_markdown",
            cleaning_policy.dictionary_markdown_filename,
            False,
        ),
    )
    for key, filename, is_parquet in dictionary_specs:
        path = candidate_dir / filename
        summary = outputs.get(key)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("An approved cleaned dictionary is unavailable")
        observed_hash = sha256_file(path)
        expected_dictionary_rows = (
            cleaning_policy.expected["static_clinical_variables"]
            + cleaning_policy.expected["dynamic_clinical_variables"]
        )
        if summary.get("sha256") != observed_hash or (
            is_parquet
            and (
                summary.get("row_count") != expected_dictionary_rows
                or pq.ParquetFile(path).metadata.num_rows != expected_dictionary_rows
            )
        ):
            raise HarmonizationError("An approved cleaned dictionary changed")
        candidate_files[key] = {
            "path": path,
            "filename": filename,
            "sha256": observed_hash,
            "size_bytes": path.stat().st_size,
            **({"row_count": expected_dictionary_rows} if is_parquet else {}),
        }

    current_pointer = config.data_root / policy.current_release_pointer
    previous_pointer_hash = sha256_file(current_pointer)
    previous_pointer = _read_json(current_pointer, "Previous cleaned pointer")
    previous_release = (
        config.data_root / policy.releases_directory / policy.previous_release_id
    )
    previous_manifest_path = previous_release / "release_manifest.json"
    previous_manifest = _read_json(
        previous_manifest_path, "Previous cleaned release manifest"
    )
    if not isinstance(previous_pointer, dict) or (
        previous_pointer.get("artifact") != "asic_v3_current_cleaned_release"
        or previous_pointer.get("dataset_context") != config.dataset_context
        or previous_pointer.get("release_id") != policy.previous_release_id
        or previous_pointer.get("cleaning_policy_version")
        != policy.previous_cleaning_policy_version
        or Path(str(previous_pointer.get("release_manifest", ""))).resolve()
        != previous_manifest_path.resolve()
        or previous_pointer.get("release_manifest_sha256")
        != sha256_file(previous_manifest_path)
        or previous_pointer.get("cleaned_layer_ready") is not True
        or previous_pointer.get("derived_input_approved") is not True
        or previous_pointer.get("publication_ready") is not True
        or previous_pointer.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Previous current cleaned pointer changed")
    if not isinstance(previous_manifest, dict) or (
        previous_manifest.get("artifact") != "asic_v3_cleaned_release"
        or previous_manifest.get("release_id") != policy.previous_release_id
        or previous_manifest.get("status") != "released_cleaned_layer"
        or previous_manifest.get("cleaning_policy_version")
        != policy.previous_cleaning_policy_version
        or previous_manifest.get("cleaned_layer_ready") is not True
        or previous_manifest.get("derived_input_approved") is not True
        or previous_manifest.get("publication_ready") is not True
        or previous_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Previous cleaned release changed")

    releases_root = config.data_root / policy.releases_directory
    release_dir = releases_root / policy.release_id
    staging_dir = releases_root / f".{policy.release_id}.incomplete"
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{policy.release_id}.json"
    review_markdown = review_dir / f"{policy.release_id}.md"
    if any(
        path.exists()
        for path in (
            release_dir,
            staging_dir,
            review_json,
            review_markdown,
        )
    ):
        raise HarmonizationError("Cleaned 0.2 release target already exists")
    releases_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    releases_root.chmod(0o700)
    staging_dir.mkdir(mode=0o700)
    staging_dir.chmod(0o700)

    released_files: dict[str, dict[str, Any]] = {}
    for key, source_summary in candidate_files.items():
        filename = (
            f"{key}.parquet"
            if key in {"static", "dynamic"}
            else str(source_summary["filename"])
        )
        destination = staging_dir / filename
        destination_hash = _copy_exact(
            source_summary["path"], destination, source_summary["sha256"]
        )
        released_files[key] = {
            "path": str(release_dir / filename),
            "sha256": destination_hash,
            "size_bytes": destination.stat().st_size,
            "byte_identical_to_audited_candidate": True,
            **(
                {"row_count": source_summary["row_count"]}
                if "row_count" in source_summary
                else {}
            ),
            **(
                {"schema_sha256": source_summary["schema_sha256"]}
                if "schema_sha256" in source_summary
                else {}
            ),
        }

    previous_snapshot = staging_dir / policy.previous_pointer_snapshot_name
    shutil.copyfile(current_pointer, previous_snapshot)
    previous_snapshot.chmod(0o600)
    if sha256_file(previous_snapshot) != previous_pointer_hash:
        raise HarmonizationError("Previous cleaned-pointer snapshot changed")

    generated = utc_timestamp()
    release_manifest_path = staging_dir / "release_manifest.json"
    release_manifest = {
        "artifact": "asic_v3_cleaned_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "status": "released_cleaned_layer",
        "harmonized_release_id": policy.harmonized_release_id,
        "harmonized_contract_version": policy.harmonized_contract_version,
        "cleaning_policy_version": policy.cleaning_policy_version,
        "previous_current_release_id": policy.previous_release_id,
        "lineage": {
            "cleaning_0_2_build_run_id": policy.build_run_id,
            "cleaning_0_2_build_manifest_sha256": candidate_manifest_hash,
            "cleaning_0_2_audit_run_id": policy.audit_run_id,
            "cleaning_0_2_audit_review_sha256": sha256_file(review_path),
            "cleaning_0_2_audit_private_manifest_sha256": sha256_file(
                private_path
            ),
            "cleaning_0_2_policy_sha256": policy.cleaning_policy_sha256,
            "cleaning_0_2_audit_policy_sha256": policy.audit_policy_sha256,
            "harmonized_release_manifest_sha256": sha256_file(
                source.release_manifest_path
            ),
            "previous_current_pointer_sha256": previous_pointer_hash,
            "previous_release_manifest_sha256": sha256_file(
                previous_manifest_path
            ),
            "promotion_policy_sha256": sha256_file(policy.source_path),
        },
        "human_approval": {
            "role": policy.approval_role,
            "recorded_date": policy.approval_date,
            "statement": policy.approval_statement,
            "resolved_blocker": policy.resolved_blocker,
        },
        "audit_metrics": metrics,
        "files": released_files,
        "previous_current_pointer_snapshot": {
            "path": str(release_dir / policy.previous_pointer_snapshot_name),
            "sha256": previous_pointer_hash,
            "byte_identical_to_previous_pointer": True,
        },
        "payload_byte_identity_preserved": True,
        "dictionary_bytes_preserved": True,
        "candidate_artifacts_preserved": True,
        "previous_releases_preserved": True,
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "columns_dropped": False,
        "cleaning_rerun_during_promotion": False,
        "cleaning_applied_in_candidate": True,
        "derivation_applied": False,
        "cleaned_layer_ready": True,
        "cleaned_dictionary_ready": True,
        "derived_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(release_manifest_path, release_manifest, 0o600)
    staging_dir.replace(release_dir)
    release_dir.chmod(0o700)
    release_manifest_path = release_dir / release_manifest_path.name
    previous_snapshot = release_dir / previous_snapshot.name

    if sha256_file(current_pointer) != previous_pointer_hash:
        raise HarmonizationError("Current cleaned pointer changed during promotion")
    current_payload = {
        "artifact": "asic_v3_current_cleaned_release",
        "artifact_version": "0.2",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "cleaning_policy_version": policy.cleaning_policy_version,
        "harmonized_release_id": policy.harmonized_release_id,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "previous_release_id": policy.previous_release_id,
        "previous_current_pointer_snapshot": str(previous_snapshot),
        "previous_current_pointer_sha256": previous_pointer_hash,
        "cleaned_layer_ready": True,
        "cleaned_dictionary_ready": True,
        "derived_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_cleaned_0_2_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "cleaning_policy_version": policy.cleaning_policy_version,
        "harmonized_release_id": policy.harmonized_release_id,
        "previous_release_id": policy.previous_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "audit_metrics": metrics,
        "payload_byte_identity_preserved": True,
        "dictionary_bytes_preserved": True,
        "previous_current_pointer_bytes_preserved": True,
        "previous_releases_preserved": True,
        "current_pointer_advanced_atomically": True,
        "cleaned_layer_ready": True,
        "cleaned_dictionary_ready": True,
        "derived_input_approved": True,
        "cleaning_rerun_during_promotion": False,
        "derivation_applied": False,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_markdown, _markdown(review_payload), 0o640)
    return Cleaned02PromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        previous_pointer_snapshot_path=previous_snapshot,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
