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
from asic_pipeline.harmonization.harmonized_build import _read_json, _schema_digest
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.harmonized_0_2 import (
    Harmonized02AuditConfig,
    _target_schemas,
    load_harmonized_0_2_audit_config,
    load_harmonized_0_2_audit_policy,
    load_harmonized_0_2_build_policy,
)


APPROVAL_STATEMENT = (
    "I approve promotion of harmonized 0.2 candidate 20260807T112402Z "
    "under frozen contract 0.2."
)

EXPECTED_BOUNDARY = {
    "copy_audited_parquet_bytes_unchanged": True,
    "preserve_candidate_artifacts": True,
    "preserve_all_previous_releases": True,
    "preserve_previous_current_pointer_bytes_in_new_release": True,
    "overwrite_existing_release": False,
    "require_exact_previous_current_pointer": True,
    "replace_current_pointer_atomically": True,
    "apply_harmonization": False,
    "apply_cleaning": False,
    "apply_derivation": False,
    "filter_rows": False,
    "filter_stays": False,
    "authorize_harmonized_layer": True,
    "authorize_cleaning_input": True,
    "authorize_external_data_export": False,
}

EXPECTED_AUDIT_METRICS = {
    "compared_static_rows": 16054,
    "compared_dynamic_rows": 24069379,
    "compared_output_cells": 3466440088,
    "unit_conversion_rules": 9,
    "semantic_split_rules": 9,
    "negative_medication_values_preserved": 201,
    "missing_or_invalid_weight_links": 0,
}


@dataclass(frozen=True)
class Harmonized02PromotionPolicy:
    version: str
    build_run_id: str
    audit_run_id: str
    contract_version: str
    candidate_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    audit_policy_path: Path
    audit_policy_sha256: str
    approval_role: str
    approval_date: str
    approval_statement: str
    resolved_blocker: str
    expected_metrics: dict[str, int]
    previous_release_id: str
    previous_contract_version: str
    previous_current_pointer: Path
    release_id: str
    releases_directory: Path
    current_release_pointer: Path
    prior_pointer_snapshot_name: str
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class Harmonized02PromotionConfig:
    audit: Harmonized02AuditConfig
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
class Harmonized02PromotionResult:
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


def _contained(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def _nonnegative_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a nonnegative integer")
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


def load_harmonized_0_2_promotion_config(
    path: str | Path,
) -> Harmonized02PromotionConfig:
    source = Path(path).expanduser().resolve()
    audit = load_harmonized_0_2_audit_config(source)
    raw = load_yaml_mapping(source, "Harmonized 0.2 promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_0_2_promotion_policy", "config"),
        source,
    )
    return Harmonized02PromotionConfig(audit=audit, policy_path=policy_path)


def load_harmonized_0_2_promotion_policy(
    path: str | Path,
) -> Harmonized02PromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized 0.2 promotion policy")
    if (
        raw.get("harmonized_0_2_promotion_policy_version") != "0.1"
        or raw.get("status")
        != "explicitly_approved_for_immutable_harmonized_0_2_release"
    ):
        raise ConfigurationError("Harmonized 0.2 promotion approval is invalid")
    approved = _mapping(raw.get("approved_input"), "approved_input")
    immutable = _mapping(raw.get("immutable_policy"), "immutable_policy")
    approval = _mapping(raw.get("human_approval"), "human_approval")
    expected = _mapping(raw.get("expected_audit"), "expected_audit")
    previous = _mapping(
        raw.get("previous_current_release"), "previous_current_release"
    )
    release = _mapping(raw.get("release"), "release")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Harmonized 0.2 promotion boundary changed")
    audit_policy_path = resolve_path(
        required_string(
            immutable,
            "harmonized_0_2_audit",
            "immutable_policy",
        ),
        source,
    )
    audit_policy_sha256 = required_string(
        immutable,
        "harmonized_0_2_audit_sha256",
        "immutable_policy",
    )
    if sha256_file(audit_policy_path) != audit_policy_sha256:
        raise ConfigurationError("Approved harmonized 0.2 audit policy changed")
    statement = required_string(approval, "statement", "human_approval")
    if statement != APPROVAL_STATEMENT:
        raise ConfigurationError("Data-owner harmonized 0.2 approval changed")
    expected_status = {
        "technical_status": "pass",
        "overall_status_before_approval": "pending_human_review",
        "technical_blocking_finding_count": 0,
        "human_blocking_finding_count_before_approval": 1,
        "human_blocking_check_resolved_by_this_approval": (
            "harmonized_0_2_candidate_release_approved"
        ),
    }
    for key, value in expected_status.items():
        if expected.get(key) != value:
            raise ConfigurationError("Approved harmonized 0.2 audit status changed")
    metrics = {
        key: _nonnegative_int(expected.get(key), f"expected_audit.{key}")
        for key in EXPECTED_AUDIT_METRICS
    }
    if metrics != EXPECTED_AUDIT_METRICS:
        raise ConfigurationError("Approved harmonized 0.2 audit metrics changed")
    if previous.get("require_ready_for_cleaning") is not True:
        raise ConfigurationError("Previous current-release precondition changed")
    snapshot_name = required_string(
        release,
        "prior_pointer_snapshot_name",
        "release",
    )
    if Path(snapshot_name).name != snapshot_name:
        raise ConfigurationError("Prior-pointer snapshot name is invalid")
    policy = Harmonized02PromotionPolicy(
        version="0.1",
        build_run_id=required_string(approved, "build_run_id", "approved_input"),
        audit_run_id=required_string(approved, "audit_run_id", "approved_input"),
        contract_version=required_string(
            approved,
            "frozen_contract_version",
            "approved_input",
        ),
        candidate_artifact_version=required_string(
            approved,
            "candidate_artifact_version",
            "approved_input",
        ),
        audit_private_artifact_version=required_string(
            approved,
            "audit_private_artifact_version",
            "approved_input",
        ),
        audit_review_artifact_version=required_string(
            approved,
            "audit_review_artifact_version",
            "approved_input",
        ),
        audit_policy_path=audit_policy_path,
        audit_policy_sha256=audit_policy_sha256,
        approval_role=required_string(approval, "role", "human_approval"),
        approval_date=required_string(
            approval,
            "recorded_date",
            "human_approval",
        ),
        approval_statement=statement,
        resolved_blocker=str(
            expected["human_blocking_check_resolved_by_this_approval"]
        ),
        expected_metrics=metrics,
        previous_release_id=required_string(
            previous,
            "release_id",
            "previous_current_release",
        ),
        previous_contract_version=required_string(
            previous,
            "frozen_contract_version",
            "previous_current_release",
        ),
        previous_current_pointer=_contained(
            required_string(
                previous,
                "current_release_pointer",
                "previous_current_release",
            ),
            "previous_current_release.current_release_pointer",
        ),
        release_id=required_string(release, "release_id", "release"),
        releases_directory=_contained(
            required_string(release, "releases_directory", "release"),
            "release.releases_directory",
        ),
        current_release_pointer=_contained(
            required_string(release, "current_release_pointer", "release"),
            "release.current_release_pointer",
        ),
        prior_pointer_snapshot_name=snapshot_name,
        release_artifact_version=required_string(
            release,
            "artifact_version",
            "release",
        ),
        review_directory_name=required_string(
            release,
            "review_directory_name",
            "release",
        ),
        source_path=source,
    )
    if (
        policy.build_run_id != "20260807T112402Z"
        or policy.audit_run_id != policy.build_run_id
        or policy.release_id != policy.build_run_id
        or policy.contract_version != "0.2"
        or policy.candidate_artifact_version != "0.1"
        or policy.approval_role != "data_owner"
        or policy.approval_date != "2026-08-07"
        or policy.previous_release_id != "20260806T111156Z"
        or policy.previous_contract_version != "0.1"
        or policy.previous_current_pointer != policy.current_release_pointer
        or policy.release_artifact_version != "0.2"
        or policy.review_directory_name != "harmonized_0_2_promotion"
    ):
        raise ConfigurationError("Approved harmonized 0.2 promotion scope changed")
    return policy


def _markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 harmonized 0.2 release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Frozen contract: `{payload['frozen_contract_version']}`",
            f"- Previous current release: `{payload['previous_release_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Compared output cells: `{payload['compared_output_cells']}`",
            "- Audited Parquet bytes preserved exactly: `true`",
            "- Previous pointer bytes preserved: `true`",
            "- Previous releases preserved: `true`",
            "- Harmonized layer ready: `true`",
            "- Approved as cleaning 0.2 input: `true`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human gate for the exactly audited harmonized 0.2 candidate. Static and dynamic Parquet bytes are copied unchanged; the candidate, harmonized 0.1 release, cleaned 0.1 release, and derived 0.1 release remain preserved. Only the recoverable current-harmonized pointer advances to 0.2.",
            "",
        )
    )


def promote_harmonized_0_2_release(
    config: Harmonized02PromotionConfig,
) -> Harmonized02PromotionResult:
    policy = load_harmonized_0_2_promotion_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("Harmonized 0.2 promotion is production-only")
    audit_policy = load_harmonized_0_2_audit_policy(policy.audit_policy_path)
    build_policy = load_harmonized_0_2_build_policy(audit_policy.build_policy_path)
    if (
        config.audit.policy_path.resolve() != policy.audit_policy_path.resolve()
        or config.audit.build.policy_path.resolve()
        != audit_policy.build_policy_path.resolve()
        or build_policy.target_contract_version != policy.contract_version
        or build_policy.artifact_version != policy.candidate_artifact_version
    ):
        raise HarmonizationError("Harmonized 0.2 promotion lineage changed")
    target_schemas, target_manifest = _target_schemas(config.audit.build, build_policy)
    target_manifest_path = (
        config.data_root
        / build_policy.target_contract_directory
        / "freeze_manifest.json"
    )
    if target_manifest.get("contract_version") != policy.contract_version:
        raise HarmonizationError("Frozen harmonized 0.2 contract changed")

    candidate = (
        config.data_root
        / build_policy.candidate_directory_name
        / policy.build_run_id
    )
    build_manifest_path = candidate / "harmonized_0_2_build_manifest.json"
    build_manifest = _read_json(
        build_manifest_path,
        "Harmonized 0.2 candidate manifest",
    )
    if not isinstance(build_manifest, dict) or (
        build_manifest.get("artifact") != "asic_v3_harmonized_0_2_candidate"
        or build_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or build_manifest.get("run_id") != policy.build_run_id
        or build_manifest.get("status") != build_policy.output_status
        or build_manifest.get("row_filtering_applied") is not False
        or build_manifest.get("stay_filtering_applied") is not False
        or build_manifest.get("input_columns_dropped") is not False
        or build_manifest.get("cleaning_applied") is not False
        or build_manifest.get("derivation_applied") is not False
        or build_manifest.get("publication_ready") is not False
        or build_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved harmonized 0.2 candidate changed")
    lineage = build_manifest.get("lineage")
    if not isinstance(lineage, dict) or (
        lineage.get("target_contract_version") != policy.contract_version
        or lineage.get("target_contract_manifest_sha256")
        != sha256_file(target_manifest_path)
        or lineage.get("build_policy_sha256")
        != sha256_file(build_policy.source_path)
    ):
        raise HarmonizationError("Approved harmonized 0.2 candidate lineage changed")

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
        / "harmonized_0_2_audit_manifest.json"
    )
    review = _read_json(review_path, "Harmonized 0.2 audit review")
    private = _read_json(private_path, "Private harmonized 0.2 audit manifest")
    blockers = review.get("blocking_findings") if isinstance(review, dict) else None
    metrics = review.get("metrics") if isinstance(review, dict) else None
    if not isinstance(review, dict) or (
        review.get("artifact") != "asic_v3_harmonized_0_2_audit_review"
        or review.get("artifact_version") != policy.audit_review_artifact_version
        or review.get("run_id") != policy.audit_run_id
        or review.get("build_run_id") != policy.build_run_id
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_status") != "pass"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or blockers[0].get("check") != policy.resolved_blocker
        or not isinstance(metrics, dict)
        or any(
            metrics.get(key) != value
            for key, value in policy.expected_metrics.items()
        )
        or len(review.get("unit_conversion_summaries", [])) != 9
        or len(review.get("semantic_split_summaries", [])) != 9
        or review.get("every_output_cell_recomputed") is not True
        or review.get("rows_or_stays_filtered") is not False
        or review.get("cleaning_applied") is not False
        or review.get("derivation_applied") is not False
        or review.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved harmonized 0.2 audit review changed")
    build_manifest_sha256 = sha256_file(build_manifest_path)
    if not isinstance(private, dict) or (
        private.get("artifact") != "asic_v3_harmonized_0_2_audit_private"
        or private.get("artifact_version")
        != policy.audit_private_artifact_version
        or private.get("run_id") != policy.audit_run_id
        or private.get("build_run_id") != policy.build_run_id
        or private.get("build_manifest_sha256") != build_manifest_sha256
        or private.get("metrics") != metrics
        or private.get("source_data_modified") is not False
        or private.get("candidate_data_modified") is not False
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved private harmonized 0.2 audit changed")

    outputs = build_manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Approved harmonized 0.2 outputs are absent")
    source_files: dict[str, dict[str, Any]] = {}
    expected_rows = {
        "static": policy.expected_metrics["compared_static_rows"],
        "dynamic": policy.expected_metrics["compared_dynamic_rows"],
    }
    for table in ("static", "dynamic"):
        path = candidate / f"{table}.parquet"
        summary = outputs.get(table)
        parquet = pq.ParquetFile(path)
        observed_hash = sha256_file(path)
        if not isinstance(summary, dict) or (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256")
            != _schema_digest(target_schemas[table])
            or not _parquet_schema_matches(
                parquet.schema_arrow,
                target_schemas[table],
            )
        ):
            raise HarmonizationError("An approved harmonized 0.2 output changed")
        source_files[table] = {
            "path": path,
            "sha256": observed_hash,
            "row_count": expected_rows[table],
            "schema_sha256": str(summary["schema_sha256"]),
            "size_bytes": path.stat().st_size,
        }

    current_pointer = config.data_root / policy.current_release_pointer
    previous_pointer_hash = sha256_file(current_pointer)
    previous_pointer = _read_json(current_pointer, "Previous current release pointer")
    previous_release_dir = (
        config.data_root / policy.releases_directory / policy.previous_release_id
    )
    previous_manifest_path = previous_release_dir / "release_manifest.json"
    previous_manifest = _read_json(
        previous_manifest_path,
        "Previous harmonized release manifest",
    )
    if not isinstance(previous_pointer, dict) or (
        previous_pointer.get("artifact") != "asic_v3_current_harmonized_release"
        or previous_pointer.get("dataset_context") != config.dataset_context
        or previous_pointer.get("release_id") != policy.previous_release_id
        or previous_pointer.get("frozen_contract_version")
        != policy.previous_contract_version
        or Path(str(previous_pointer.get("release_manifest"))).resolve()
        != previous_manifest_path.resolve()
        or previous_pointer.get("release_manifest_sha256")
        != sha256_file(previous_manifest_path)
        or previous_pointer.get("harmonized_layer_ready") is not True
        or previous_pointer.get("cleaning_input_approved") is not True
        or previous_pointer.get("publication_ready") is not True
    ):
        raise HarmonizationError("Previous current harmonized pointer changed")
    if not isinstance(previous_manifest, dict) or (
        previous_manifest.get("artifact") != "asic_v3_harmonized_release"
        or previous_manifest.get("release_id") != policy.previous_release_id
        or previous_manifest.get("frozen_contract_version")
        != policy.previous_contract_version
        or previous_manifest.get("status") != "released_harmonized_layer"
        or previous_manifest.get("harmonized_layer_ready") is not True
        or previous_manifest.get("cleaning_input_approved") is not True
        or previous_manifest.get("publication_ready") is not True
    ):
        raise HarmonizationError("Previous harmonized release changed")

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
        raise HarmonizationError("Harmonized 0.2 release target already exists")
    releases_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    releases_root.chmod(0o700)
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

    previous_snapshot = staging_dir / policy.prior_pointer_snapshot_name
    shutil.copyfile(current_pointer, previous_snapshot)
    previous_snapshot.chmod(0o600)
    if sha256_file(previous_snapshot) != previous_pointer_hash:
        raise HarmonizationError("Previous current-pointer snapshot changed")

    generated = utc_timestamp()
    release_manifest_path = staging_dir / "release_manifest.json"
    release_manifest = {
        "artifact": "asic_v3_harmonized_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "status": "released_harmonized_layer",
        "frozen_contract_version": policy.contract_version,
        "previous_current_release_id": policy.previous_release_id,
        "lineage": {
            "harmonized_0_2_build_run_id": policy.build_run_id,
            "harmonized_0_2_build_manifest_sha256": build_manifest_sha256,
            "harmonized_0_2_audit_run_id": policy.audit_run_id,
            "harmonized_0_2_audit_review_sha256": sha256_file(review_path),
            "harmonized_0_2_audit_private_manifest_sha256": sha256_file(
                private_path
            ),
            "frozen_contract_manifest_sha256": sha256_file(target_manifest_path),
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
        "audit_metrics": policy.expected_metrics,
        "files": released_files,
        "previous_current_pointer_snapshot": {
            "path": str(release_dir / policy.prior_pointer_snapshot_name),
            "sha256": previous_pointer_hash,
            "byte_identical_to_previous_pointer": True,
        },
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
        "previous_releases_preserved": True,
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "harmonized_layer_ready": True,
        "cleaning_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(release_manifest_path, release_manifest, 0o600)
    staging_dir.replace(release_dir)
    release_dir.chmod(0o700)
    release_manifest_path = release_dir / release_manifest_path.name
    previous_snapshot = release_dir / policy.prior_pointer_snapshot_name

    if sha256_file(current_pointer) != previous_pointer_hash:
        raise HarmonizationError(
            "Current harmonized pointer changed during promotion"
        )
    current_payload = {
        "artifact": "asic_v3_current_harmonized_release",
        "artifact_version": "0.2",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "frozen_contract_version": policy.contract_version,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "previous_release_id": policy.previous_release_id,
        "previous_current_pointer_snapshot": str(previous_snapshot),
        "previous_current_pointer_sha256": previous_pointer_hash,
        "harmonized_layer_ready": True,
        "cleaning_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_harmonized_0_2_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "frozen_contract_version": policy.contract_version,
        "previous_release_id": policy.previous_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "compared_output_cells": policy.expected_metrics[
            "compared_output_cells"
        ],
        "payload_byte_identity_preserved": True,
        "previous_current_pointer_bytes_preserved": True,
        "previous_releases_preserved": True,
        "current_pointer_advanced_atomically": True,
        "harmonized_layer_ready": True,
        "cleaning_input_approved": True,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_markdown, _markdown(review_payload), 0o640)
    return Harmonized02PromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        previous_pointer_snapshot_path=previous_snapshot,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
