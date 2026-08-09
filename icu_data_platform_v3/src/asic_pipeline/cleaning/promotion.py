from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.cleaning.pipeline import (
    CleaningConfig,
    _cleaned_schema,
    _read_json,
    _schema_digest,
    load_cleaning_config,
    load_cleaning_policy,
    load_harmonized_release_input,
)
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVAL_STATEMENT = "i approve"
APPROVAL_CONTEXT = (
    "Promotion of cleaned candidate 20260806T114234Z under cleaning policy 0.1, "
    "as proposed in the immediately preceding cleaned-candidate audit review."
)

EXPECTED_METRIC_KEYS = (
    "compared_static_rows",
    "compared_dynamic_rows",
    "compared_output_cell_count",
    "applied_scalar_rule_count",
    "unit_unresolved_audit_only_rule_count",
    "power_of_ten_correction_count",
    "range_mask_count",
    "nonfinite_mask_count",
    "height_element_removal_count",
    "hospital_mask_count",
    "unit_unresolved_audit_only_outside_range_count",
    "post_clean_range_violation_count",
    "post_clean_nonfinite_count",
)


@dataclass(frozen=True)
class CleanedPromotionPolicy:
    version: str
    cleaning_run_id: str
    audit_run_id: str
    harmonized_release_id: str
    harmonized_contract_version: str
    cleaning_policy_path: Path
    cleaning_policy_sha256: str
    candidate_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    approval_role: str
    approval_date: str
    approval_statement: str
    approval_context: str
    expected_metrics: tuple[tuple[str, int], ...]
    resolved_blocker: str
    release_id: str
    releases_directory: Path
    current_release_pointer: Path
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class CleanedPromotionConfig:
    cleaning: CleaningConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.cleaning.dataset_context

    @property
    def data_root(self) -> Path:
        return self.cleaning.data_root

    @property
    def reports_root(self) -> Path:
        return self.cleaning.reports_root


@dataclass(frozen=True)
class CleanedPromotionResult:
    release_id: str
    release_directory: Path
    release_manifest_path: Path
    current_release_pointer_path: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _nonnegative_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a non-negative integer")
    return value


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_cleaned_promotion_config(path: str | Path) -> CleanedPromotionConfig:
    source = Path(path).expanduser().resolve()
    cleaning = load_cleaning_config(source)
    raw = load_yaml_mapping(source, "Cleaned promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "cleaned_promotion_policy", "config"), source
    )
    return CleanedPromotionConfig(cleaning=cleaning, policy_path=policy_path)


def load_cleaned_promotion_policy(path: str | Path) -> CleanedPromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaned promotion policy")
    if raw.get("cleaned_promotion_policy_version") != "0.1" or raw.get(
        "status"
    ) != "explicitly_approved_for_immutable_cleaned_release":
        raise ConfigurationError("Cleaned promotion approval is invalid")
    approved = _mapping(raw.get("approved_input"), "approved_input")
    immutable = _mapping(raw.get("immutable_policy"), "immutable_policy")
    approval = _mapping(raw.get("human_approval"), "human_approval")
    expected = _mapping(raw.get("expected_audit"), "expected_audit")
    release = _mapping(raw.get("release"), "release")
    boundary = _mapping(raw.get("boundary"), "boundary")
    expected_boundary = {
        "copy_audited_parquet_bytes_unchanged": True,
        "preserve_candidate_artifacts": True,
        "overwrite_existing_release": False,
        "overwrite_existing_current_pointer": False,
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
    if boundary != expected_boundary:
        raise ConfigurationError("Cleaned promotion boundary changed")
    if (
        expected.get("technical_status") != "pass"
        or expected.get("overall_status_before_approval")
        != "pending_human_review"
        or expected.get("technical_blocking_finding_count") != 0
        or expected.get("human_blocking_finding_count_before_approval") != 1
    ):
        raise ConfigurationError("Expected cleaned audit status changed")
    statement = required_string(approval, "statement", "human_approval")
    context = required_string(approval, "context", "human_approval")
    if statement != APPROVAL_STATEMENT or context != APPROVAL_CONTEXT:
        raise ConfigurationError("Data-owner cleaned promotion approval changed")
    cleaning_policy_path = resolve_path(
        required_string(immutable, "cleaning_policy", "immutable_policy"), source
    )
    cleaning_policy_sha256 = required_string(
        immutable, "cleaning_policy_sha256", "immutable_policy"
    )
    if sha256_file(cleaning_policy_path) != cleaning_policy_sha256:
        raise ConfigurationError("Approved cleaning policy changed")
    expected_metrics = tuple(
        (
            key,
            _nonnegative_int(expected.get(key), f"expected_audit.{key}"),
        )
        for key in EXPECTED_METRIC_KEYS
    )
    policy = CleanedPromotionPolicy(
        version="0.1",
        cleaning_run_id=required_string(
            approved, "cleaning_candidate_run_id", "approved_input"
        ),
        audit_run_id=required_string(
            approved, "cleaning_audit_run_id", "approved_input"
        ),
        harmonized_release_id=required_string(
            approved, "harmonized_release_id", "approved_input"
        ),
        harmonized_contract_version=required_string(
            approved, "harmonized_contract_version", "approved_input"
        ),
        cleaning_policy_path=cleaning_policy_path,
        cleaning_policy_sha256=cleaning_policy_sha256,
        candidate_artifact_version=required_string(
            approved, "candidate_artifact_version", "approved_input"
        ),
        audit_private_artifact_version=required_string(
            approved, "audit_private_artifact_version", "approved_input"
        ),
        audit_review_artifact_version=required_string(
            approved, "audit_review_artifact_version", "approved_input"
        ),
        approval_role=required_string(approval, "role", "human_approval"),
        approval_date=required_string(
            approval, "recorded_date", "human_approval"
        ),
        approval_statement=statement,
        approval_context=context,
        expected_metrics=expected_metrics,
        resolved_blocker=required_string(
            expected,
            "human_blocking_check_resolved_by_this_approval",
            "expected_audit",
        ),
        release_id=required_string(release, "release_id", "release"),
        releases_directory=_contained_relative_path(
            required_string(release, "releases_directory", "release"),
            "release.releases_directory",
        ),
        current_release_pointer=_contained_relative_path(
            required_string(release, "current_release_pointer", "release"),
            "release.current_release_pointer",
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
        policy.cleaning_run_id != policy.release_id
        or policy.audit_run_id != policy.cleaning_run_id
        or policy.approval_role != "data_owner"
        or policy.resolved_blocker != "cleaned_candidate_release_approved"
    ):
        raise ConfigurationError("Approved cleaned promotion lineage is inconsistent")
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


def _write_markdown(path: Path, value: str, mode: int) -> None:
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
            "# ASIC v3 cleaned release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Cleaning policy: `{payload['cleaning_policy_version']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Compared output cells: `{metrics['compared_output_cell_count']}`",
            "- Audited Parquet bytes preserved exactly: `true`",
            "- Cleaned layer ready: `true`",
            "- Approved as derived-layer input: `true`",
            "- Cleaning rerun during promotion: `false`",
            "- Derivation applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human release gate for the completely audited cleaned candidate. The immutable candidate remains preserved; no clinical value, row, schema, provenance field, or Parquet byte is changed during promotion.",
            "",
        )
    )


def promote_cleaned_release(
    config: CleanedPromotionConfig,
) -> CleanedPromotionResult:
    policy = load_cleaned_promotion_policy(config.policy_path)
    cleaning_policy = load_cleaning_policy(policy.cleaning_policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved cleaned promotion is production-only")
    if config.cleaning.policy_path.resolve() != policy.cleaning_policy_path:
        raise HarmonizationError("Configured cleaning policy differs from approval")
    if (
        cleaning_policy.harmonized_release_id != policy.harmonized_release_id
        or cleaning_policy.harmonized_contract_version
        != policy.harmonized_contract_version
        or cleaning_policy.candidate_artifact_version
        != policy.candidate_artifact_version
        or cleaning_policy.audit_private_artifact_version
        != policy.audit_private_artifact_version
        or cleaning_policy.audit_review_artifact_version
        != policy.audit_review_artifact_version
    ):
        raise HarmonizationError("Approved cleaned promotion contract lineage changed")
    harmonized = load_harmonized_release_input(config.cleaning, cleaning_policy)

    candidate_dir = (
        config.data_root
        / cleaning_policy.candidate_directory_name
        / policy.cleaning_run_id
    )
    candidate_manifest_path = candidate_dir / "cleaning_manifest.json"
    candidate_manifest = _read_json(
        candidate_manifest_path, "Cleaning candidate manifest"
    )
    if not isinstance(candidate_manifest, dict):
        raise HarmonizationError("Cleaning candidate manifest is not an object")
    lineage = candidate_manifest.get("lineage")
    if (
        candidate_manifest.get("artifact") != "asic_v3_cleaning_candidate"
        or candidate_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or candidate_manifest.get("run_id") != policy.cleaning_run_id
        or candidate_manifest.get("status")
        != "nonpublishable_requires_complete_cleaning_audit"
        or not isinstance(lineage, dict)
        or lineage.get("harmonized_release_id") != policy.harmonized_release_id
        or lineage.get("harmonized_contract_version")
        != policy.harmonized_contract_version
        or lineage.get("cleaning_policy_sha256")
        != policy.cleaning_policy_sha256
        or candidate_manifest.get("row_filtering_applied") is not False
        or candidate_manifest.get("stay_filtering_applied") is not False
        or candidate_manifest.get("columns_dropped") is not False
        or candidate_manifest.get("cleaning_applied") is not True
        or candidate_manifest.get("derivation_applied") is not False
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved cleaning candidate manifest changed")

    review_path = (
        config.reports_root
        / "review"
        / cleaning_policy.audit_review_directory_name
        / f"{policy.audit_run_id}.json"
    )
    private_path = (
        config.reports_root
        / "private"
        / cleaning_policy.audit_private_directory_name
        / policy.audit_run_id
        / "cleaning_audit_manifest.json"
    )
    review = _read_json(review_path, "Cleaning audit review")
    private = _read_json(private_path, "Private cleaning audit manifest")
    if not isinstance(review, dict) or not isinstance(private, dict):
        raise HarmonizationError("Approved cleaning audit evidence is invalid")
    expected_metrics = dict(policy.expected_metrics)
    blockers = review.get("blocking_findings")
    if (
        review.get("artifact") != "asic_v3_cleaning_audit_review"
        or review.get("artifact_version")
        != policy.audit_review_artifact_version
        or review.get("run_id") != policy.audit_run_id
        or review.get("cleaning_run_id") != policy.cleaning_run_id
        or review.get("harmonized_release_id") != policy.harmonized_release_id
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_status") != "pass"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or blockers[0].get("check") != policy.resolved_blocker
        or review.get("metrics") != expected_metrics
        or review.get("harmonized_data_modified") is not False
        or review.get("cleaning_candidate_modified") is not False
        or review.get("rows_or_stays_filtered") is not False
        or review.get("columns_dropped") is not False
        or review.get("derivation_applied") is not False
        or review.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved cleaning audit review changed")
    candidate_manifest_sha256 = sha256_file(candidate_manifest_path)
    if (
        private.get("artifact") != "asic_v3_cleaning_audit_private"
        or private.get("artifact_version")
        != policy.audit_private_artifact_version
        or private.get("run_id") != policy.audit_run_id
        or private.get("cleaning_run_id") != policy.cleaning_run_id
        or private.get("cleaning_manifest_sha256")
        != candidate_manifest_sha256
        or private.get("metrics") != expected_metrics
        or private.get("rule_accounting")
        != candidate_manifest.get("rule_accounting")
        or private.get("harmonized_data_modified") is not False
        or private.get("cleaning_candidate_modified") is not False
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved private cleaning audit evidence changed")

    output_summaries = candidate_manifest.get("outputs")
    if not isinstance(output_summaries, dict):
        raise HarmonizationError("Cleaning candidate outputs are unavailable")
    expected_rows = {
        "static": expected_metrics["compared_static_rows"],
        "dynamic": expected_metrics["compared_dynamic_rows"],
    }
    source_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        source_path = candidate_dir / f"{table}.parquet"
        summary = output_summaries.get(table)
        expected_schema = _cleaned_schema(
            harmonized.frozen_schemas[table], policy.harmonized_contract_version
        )
        if not isinstance(summary, dict) or not source_path.is_file():
            raise HarmonizationError("An approved cleaned output is unavailable")
        observed_hash = sha256_file(source_path)
        parquet = pq.ParquetFile(source_path)
        if (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(expected_schema)
            or not _parquet_schema_matches(parquet.schema_arrow, expected_schema)
        ):
            raise HarmonizationError("An approved cleaned output changed")
        source_files[table] = {
            "path": source_path,
            "sha256": observed_hash,
            "row_count": expected_rows[table],
            "schema_sha256": str(summary["schema_sha256"]),
            "size_bytes": source_path.stat().st_size,
        }

    releases_root = config.data_root / policy.releases_directory
    release_dir = releases_root / policy.release_id
    staging_dir = releases_root / f".{policy.release_id}.incomplete"
    current_pointer = config.data_root / policy.current_release_pointer
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{policy.release_id}.json"
    review_markdown = review_dir / f"{policy.release_id}.md"
    if any(
        path.exists()
        for path in (
            release_dir,
            staging_dir,
            current_pointer,
            review_json,
            review_markdown,
        )
    ):
        raise HarmonizationError("Approved cleaned release target already exists")
    releases_root.mkdir(parents=True, mode=0o700)
    releases_root.chmod(0o700)
    current_pointer.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    current_pointer.parent.chmod(0o700)
    staging_dir.mkdir(mode=0o700)
    staging_dir.chmod(0o700)

    released_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        source = source_files[table]
        destination = staging_dir / f"{table}.parquet"
        shutil.copyfile(source["path"], destination)
        destination.chmod(0o600)
        destination_hash = sha256_file(destination)
        if (
            destination_hash != source["sha256"]
            or destination.stat().st_size != source["size_bytes"]
            or sha256_file(source["path"]) != source["sha256"]
        ):
            raise HarmonizationError("Audited Parquet bytes changed during promotion")
        released_files[table] = {
            "path": str(release_dir / destination.name),
            "sha256": destination_hash,
            "size_bytes": destination.stat().st_size,
            "row_count": source["row_count"],
            "schema_sha256": source["schema_sha256"],
            "byte_identical_to_audited_candidate": True,
        }

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
        "cleaning_policy_version": cleaning_policy.version,
        "lineage": {
            "cleaning_candidate_run_id": policy.cleaning_run_id,
            "cleaning_candidate_manifest_sha256": candidate_manifest_sha256,
            "cleaning_audit_run_id": policy.audit_run_id,
            "cleaning_audit_review_sha256": sha256_file(review_path),
            "cleaning_audit_private_manifest_sha256": sha256_file(private_path),
            "cleaning_policy_sha256": policy.cleaning_policy_sha256,
            "harmonized_release_manifest_sha256": sha256_file(
                harmonized.release_manifest_path
            ),
            "promotion_policy_sha256": sha256_file(policy.source_path),
        },
        "human_approval": {
            "role": policy.approval_role,
            "recorded_date": policy.approval_date,
            "statement": policy.approval_statement,
            "context": policy.approval_context,
            "resolved_blocker": policy.resolved_blocker,
        },
        "audit_metrics": expected_metrics,
        "files": released_files,
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "columns_dropped": False,
        "cleaning_rerun_during_promotion": False,
        "cleaning_applied_in_candidate": True,
        "derivation_applied": False,
        "cleaned_layer_ready": True,
        "derived_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(release_manifest_path, release_manifest, 0o600)
    staging_dir.replace(release_dir)
    release_dir.chmod(0o700)
    release_manifest_path = release_dir / release_manifest_path.name

    current_payload = {
        "artifact": "asic_v3_current_cleaned_release",
        "artifact_version": "0.1",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "cleaning_policy_version": cleaning_policy.version,
        "harmonized_release_id": policy.harmonized_release_id,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "cleaned_layer_ready": True,
        "derived_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_cleaned_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "cleaning_policy_version": cleaning_policy.version,
        "harmonized_release_id": policy.harmonized_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "audit_metrics": expected_metrics,
        "payload_byte_identity_preserved": True,
        "cleaned_layer_ready": True,
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
    _write_markdown(review_markdown, _markdown(review_payload), 0o640)
    return CleanedPromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
