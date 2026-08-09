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
from asic_pipeline.harmonization.harmonized_build import (
    _load_frozen_schemas,
    _read_json,
    _schema_digest,
    load_harmonized_build_policy,
)
from asic_pipeline.harmonization.harmonized_build_audit import (
    HarmonizedBuildAuditConfig,
    _parquet_schema_matches,
    load_harmonized_build_audit_config,
    load_harmonized_build_audit_policy,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVAL_STATEMENT = (
    "I approve promotion of harmonized candidate 20260806T111156Z "
    "under frozen contract 0.1"
)


@dataclass(frozen=True)
class HarmonizedPromotionPolicy:
    version: str
    build_run_id: str
    audit_run_id: str
    frozen_contract_version: str
    build_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    audit_policy_path: Path
    audit_policy_sha256: str
    approval_role: str
    approval_date: str
    approval_statement: str
    expected_static_rows: int
    expected_dynamic_rows: int
    expected_output_cells: int
    resolved_blocker: str
    release_id: str
    releases_directory: Path
    current_release_pointer: Path
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class HarmonizedPromotionConfig:
    audit: HarmonizedBuildAuditConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.audit.dataset_context

    @property
    def data_root(self) -> Path:
        return self.audit.build.data_root

    @property
    def reports_root(self) -> Path:
        return self.audit.build.reports_root


@dataclass(frozen=True)
class HarmonizedPromotionResult:
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


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_harmonized_promotion_config(
    path: str | Path,
) -> HarmonizedPromotionConfig:
    source = Path(path).expanduser().resolve()
    audit = load_harmonized_build_audit_config(source)
    raw = load_yaml_mapping(source, "Harmonized promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_promotion_policy", "config"), source
    )
    return HarmonizedPromotionConfig(audit=audit, policy_path=policy_path)


def load_harmonized_promotion_policy(
    path: str | Path,
) -> HarmonizedPromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized promotion policy")
    if raw.get("harmonized_promotion_policy_version") != "0.1" or raw.get(
        "status"
    ) != "explicitly_approved_for_immutable_harmonized_release":
        raise ConfigurationError("Harmonized promotion approval is invalid")
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
        "apply_harmonization": False,
        "apply_cleaning": False,
        "apply_derivation": False,
        "filter_rows": False,
        "filter_stays": False,
        "authorize_harmonized_layer": True,
        "authorize_cleaning_input": True,
        "authorize_external_data_export": False,
    }
    if boundary != expected_boundary:
        raise ConfigurationError("Harmonized promotion boundary changed")
    if (
        expected.get("technical_status") != "pass"
        or expected.get("overall_status_before_approval")
        != "pending_human_review"
        or expected.get("technical_blocking_finding_count") != 0
        or expected.get("human_blocking_finding_count_before_approval") != 1
    ):
        raise ConfigurationError("Expected promotion audit status changed")
    audit_policy_path = resolve_path(
        required_string(immutable, "harmonized_build_audit", "immutable_policy"),
        source,
    )
    audit_policy_sha256 = required_string(
        immutable, "harmonized_build_audit_sha256", "immutable_policy"
    )
    if sha256_file(audit_policy_path) != audit_policy_sha256:
        raise ConfigurationError("Approved harmonized audit policy changed")
    statement = required_string(approval, "statement", "human_approval")
    if statement != APPROVAL_STATEMENT:
        raise ConfigurationError("Data-owner promotion statement changed")
    policy = HarmonizedPromotionPolicy(
        version="0.1",
        build_run_id=required_string(
            approved, "harmonized_build_run_id", "approved_input"
        ),
        audit_run_id=required_string(
            approved, "harmonized_build_audit_run_id", "approved_input"
        ),
        frozen_contract_version=required_string(
            approved, "frozen_contract_version", "approved_input"
        ),
        build_artifact_version=required_string(
            approved, "build_artifact_version", "approved_input"
        ),
        audit_private_artifact_version=required_string(
            approved, "audit_private_artifact_version", "approved_input"
        ),
        audit_review_artifact_version=required_string(
            approved, "audit_review_artifact_version", "approved_input"
        ),
        audit_policy_path=audit_policy_path,
        audit_policy_sha256=audit_policy_sha256,
        approval_role=required_string(approval, "role", "human_approval"),
        approval_date=required_string(approval, "recorded_date", "human_approval"),
        approval_statement=statement,
        expected_static_rows=_positive_int(
            expected.get("compared_static_rows"), "expected_audit.compared_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            expected.get("compared_dynamic_rows"),
            "expected_audit.compared_dynamic_rows",
        ),
        expected_output_cells=_positive_int(
            expected.get("compared_output_cells"),
            "expected_audit.compared_output_cells",
        ),
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
        policy.build_run_id != policy.release_id
        or policy.audit_run_id != policy.build_run_id
        or policy.approval_role != "data_owner"
        or policy.resolved_blocker != "harmonized_candidate_release_approved"
    ):
        raise ConfigurationError("Approved promotion lineage is inconsistent")
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


def _write_markdown(path: Path, text: str, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 harmonized release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Frozen contract: `{payload['frozen_contract_version']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            "- Audited Parquet bytes preserved exactly: `true`",
            "- Harmonized layer ready: `true`",
            "- Approved as cleaning input: `true`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human release gate for the exactly audited harmonized candidate. The immutable candidate remains preserved; no clinical value, row, schema, or provenance field is changed during promotion.",
            "",
        )
    )


def promote_harmonized_release(
    config: HarmonizedPromotionConfig,
) -> HarmonizedPromotionResult:
    policy = load_harmonized_promotion_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved harmonized promotion is production-only")
    audit_policy = load_harmonized_build_audit_policy(policy.audit_policy_path)
    build_policy = load_harmonized_build_policy(audit_policy.build_policy_path)
    freeze_policy = load_schema_dictionary_freeze_policy(build_policy.freeze_policy_path)
    if (
        build_policy.frozen_contract_version != policy.frozen_contract_version
        or freeze_policy.contract_version != policy.frozen_contract_version
        or build_policy.artifact_version != policy.build_artifact_version
    ):
        raise HarmonizationError("Approved promotion contract lineage changed")

    build_dir = (
        config.data_root / build_policy.output_directory_name / policy.build_run_id
    )
    build_manifest_path = build_dir / "harmonized_build_manifest.json"
    build_manifest = _read_json(build_manifest_path, "Harmonized build manifest")
    if not isinstance(build_manifest, dict):
        raise HarmonizationError("Harmonized build manifest is not an object")
    if (
        build_manifest.get("artifact") != "asic_v3_harmonized_build_candidate"
        or build_manifest.get("artifact_version") != policy.build_artifact_version
        or build_manifest.get("run_id") != policy.build_run_id
        or build_manifest.get("status") != build_policy.output_status
        or build_manifest.get("publication_ready") is not False
        or build_manifest.get("cleaning_applied") is not False
        or build_manifest.get("derivation_applied") is not False
    ):
        raise HarmonizationError("Approved harmonized build manifest changed")

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
        / "harmonized_build_audit_manifest.json"
    )
    review = _read_json(review_path, "Harmonized build audit review")
    private = _read_json(private_path, "Private harmonized build audit manifest")
    if not isinstance(review, dict) or not isinstance(private, dict):
        raise HarmonizationError("Approved harmonized audit evidence is invalid")
    expected_metrics = {
        "compared_static_rows": policy.expected_static_rows,
        "compared_dynamic_rows": policy.expected_dynamic_rows,
        "compared_output_cell_count": policy.expected_output_cells,
    }
    observed_metrics = review.get("metrics")
    blockers = review.get("blocking_findings")
    if (
        review.get("artifact") != "asic_v3_harmonized_build_audit_review"
        or review.get("artifact_version") != policy.audit_review_artifact_version
        or review.get("run_id") != policy.audit_run_id
        or review.get("harmonized_build_run_id") != policy.build_run_id
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_status") != "pass"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or blockers[0].get("check") != policy.resolved_blocker
        or not isinstance(observed_metrics, dict)
        or any(observed_metrics.get(key) != value for key, value in expected_metrics.items())
        or review.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved harmonized audit review changed")
    build_manifest_sha256 = sha256_file(build_manifest_path)
    if (
        private.get("artifact") != "asic_v3_harmonized_build_audit_private"
        or private.get("artifact_version") != policy.audit_private_artifact_version
        or private.get("run_id") != policy.audit_run_id
        or private.get("harmonized_build_run_id") != policy.build_run_id
        or private.get("build_manifest_sha256") != build_manifest_sha256
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved private audit evidence changed")

    frozen_dir = config.data_root / build_policy.frozen_contract_directory
    frozen_schemas, freeze_manifest = _load_frozen_schemas(
        frozen_dir, freeze_policy
    )
    if (
        build_manifest.get("lineage", {}).get("frozen_contract_version")
        != policy.frozen_contract_version
        or build_manifest.get("lineage", {}).get("freeze_manifest_sha256")
        != sha256_file(frozen_dir / "freeze_manifest.json")
        or freeze_manifest.get("contract_version") != policy.frozen_contract_version
    ):
        raise HarmonizationError("Frozen release contract changed")

    output_summaries = build_manifest.get("outputs")
    if not isinstance(output_summaries, dict):
        raise HarmonizationError("Harmonized build outputs are unavailable")
    expected_rows = {
        "static": policy.expected_static_rows,
        "dynamic": policy.expected_dynamic_rows,
    }
    source_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        source_path = build_dir / f"{table}.parquet"
        summary = output_summaries.get(table)
        if not isinstance(summary, dict) or not source_path.is_file():
            raise HarmonizationError("An approved harmonized output is unavailable")
        observed_hash = sha256_file(source_path)
        parquet = pq.ParquetFile(source_path)
        if (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(frozen_schemas[table])
            or not _parquet_schema_matches(
                parquet.schema_arrow, frozen_schemas[table]
            )
        ):
            raise HarmonizationError("An approved harmonized output changed")
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
        raise HarmonizationError("Approved harmonized release target already exists")
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
        "artifact": "asic_v3_harmonized_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "status": "released_harmonized_layer",
        "frozen_contract_version": policy.frozen_contract_version,
        "lineage": {
            "harmonized_build_run_id": policy.build_run_id,
            "harmonized_build_manifest_sha256": build_manifest_sha256,
            "harmonized_build_audit_run_id": policy.audit_run_id,
            "harmonized_build_audit_review_sha256": sha256_file(review_path),
            "harmonized_build_audit_private_manifest_sha256": sha256_file(
                private_path
            ),
            "frozen_contract_manifest_sha256": sha256_file(
                frozen_dir / "freeze_manifest.json"
            ),
            "promotion_policy_sha256": sha256_file(policy.source_path),
        },
        "human_approval": {
            "role": policy.approval_role,
            "recorded_date": policy.approval_date,
            "statement": policy.approval_statement,
            "resolved_blocker": policy.resolved_blocker,
        },
        "files": released_files,
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
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

    current_payload = {
        "artifact": "asic_v3_current_harmonized_release",
        "artifact_version": "0.1",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "frozen_contract_version": policy.frozen_contract_version,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "harmonized_layer_ready": True,
        "cleaning_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_harmonized_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "frozen_contract_version": policy.frozen_contract_version,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "payload_byte_identity_preserved": True,
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
    _write_markdown(review_markdown, _markdown(review_payload), 0o640)
    return HarmonizedPromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
