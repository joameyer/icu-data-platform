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
from asic_pipeline.derivation.pipeline import (
    CoreDerivedConfig,
    _candidate_lineage,
    _load_core_derived_source,
    _read_json,
    _schema_digest,
    derived_schema,
    load_core_derived_0_2_config,
    load_core_derived_policy,
    load_reviewed_evidence,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVAL_STATEMENT = (
    "I approve promotion of core-derived 0.2 candidate 20260808T074305Z "
    "under core-derived contract 0.2."
)
APPROVAL_CONTEXT = (
    "Explicit approval immediately following the complete core-derived 0.2 "
    "candidate audit review."
)
EXPECTED_METRICS = {
    "compared_static_rows": 16_054,
    "compared_dynamic_rows": 24_069_379,
    "compared_output_cell_count": 3_538_792_711,
    "expected_output_cell_count": 3_538_792_711,
    "static_output_column_count": 37,
    "dynamic_output_column_count": 147,
}
EXPECTED_ACCOUNTING = {
    "dynamic_row_count": 24_069_379,
    "hours_non_missing_count": 24_069_379,
    "hours_negative_count": 247_977,
    "driving_pressure_computable_count": 6_386_755,
    "driving_pressure_in_range_count": 6_383_119,
    "driving_pressure_out_of_range_count": 3_636,
    "static_row_count": 16_054,
    "hospital_mortality_false": 9_757,
    "hospital_mortality_true": 4_726,
    "hospital_mortality_missing": 1_571,
    "hospital_mortality_conflict_count": 0,
    "icu_mortality_false": 10_457,
    "icu_mortality_true": 2_146,
    "icu_mortality_missing": 3_451,
    "icu_mortality_conflict_count": 0,
    "ventilation_supported_timestamp_count_total": 8_715_616,
    "ventilation_episode_count_total": 53_535,
    "observed_mechanical_ventilation_ge_24h_true": 12_775,
}
EXPECTED_BOUNDARY = {
    "copy_audited_parquet_bytes_unchanged": True,
    "preserve_candidate_artifacts": True,
    "preserve_all_previous_releases": True,
    "preserve_previous_current_pointer_bytes_in_new_release": True,
    "overwrite_existing_release": False,
    "require_exact_previous_current_pointer": True,
    "replace_current_pointer_atomically": True,
    "rerun_derivation": False,
    "rerun_cleaning": False,
    "filter_rows_or_stays": False,
    "drop_or_modify_source_columns": False,
    "create_cohort": False,
    "apply_time_blocking": False,
    "authorize_core_derived_layer": True,
    "authorize_analysis_input": True,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class CoreDerived02PromotionPolicy:
    version: str
    derived_run_id: str
    audit_run_id: str
    cleaned_release_id: str
    contract_version: str
    candidate_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    contract_path: Path
    contract_sha256: str
    approval_role: str
    approval_date: str
    approval_statement: str
    approval_context: str
    resolved_blocker: str
    previous_release_id: str
    previous_contract_version: str
    previous_cleaned_release_id: str
    current_release_pointer: Path
    release_id: str
    releases_directory: Path
    previous_pointer_snapshot_name: str
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class CoreDerived02PromotionConfig:
    derived: CoreDerivedConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.derived.dataset_context

    @property
    def data_root(self) -> Path:
        return self.derived.data_root

    @property
    def reports_root(self) -> Path:
        return self.derived.reports_root


@dataclass(frozen=True)
class CoreDerived02PromotionResult:
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


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_core_derived_0_2_promotion_config(
    path: str | Path,
) -> CoreDerived02PromotionConfig:
    source = Path(path).expanduser().resolve()
    derived = load_core_derived_0_2_config(source)
    raw = load_yaml_mapping(source, "Core-derived 0.2 promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "core_derived_0_2_promotion_policy", "config"),
        source,
    )
    return CoreDerived02PromotionConfig(derived=derived, policy_path=policy_path)


def load_core_derived_0_2_promotion_policy(
    path: str | Path,
) -> CoreDerived02PromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Core-derived 0.2 promotion policy")
    if (
        raw.get("core_derived_0_2_promotion_policy_version") != "0.1"
        or raw.get("status")
        != "explicitly_approved_for_immutable_core_derived_0_2_release"
    ):
        raise ConfigurationError("Core-derived 0.2 promotion approval is invalid")
    approved = _mapping(raw.get("approved_input"), "approved_input")
    immutable = _mapping(raw.get("immutable_contract"), "immutable_contract")
    approval = _mapping(raw.get("human_approval"), "human_approval")
    expected = _mapping(raw.get("expected_audit"), "expected_audit")
    previous = _mapping(
        raw.get("previous_current_release"), "previous_current_release"
    )
    release = _mapping(raw.get("release"), "release")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Core-derived 0.2 promotion boundary changed")
    if (
        expected.get("technical_status") != "pass"
        or expected.get("overall_status_before_approval")
        != "pending_human_review"
        or expected.get("technical_blocking_finding_count") != 0
        or expected.get("human_blocking_finding_count_before_approval") != 1
        or _mapping(expected.get("metrics"), "expected_audit.metrics")
        != EXPECTED_METRICS
        or _mapping(
            expected.get("derivation_accounting"),
            "expected_audit.derivation_accounting",
        )
        != EXPECTED_ACCOUNTING
    ):
        raise ConfigurationError("Expected core-derived 0.2 audit changed")
    statement = required_string(approval, "statement", "human_approval")
    context = required_string(approval, "context", "human_approval")
    if statement != APPROVAL_STATEMENT or context != APPROVAL_CONTEXT:
        raise ConfigurationError("Data-owner core-derived 0.2 approval changed")
    contract_path = resolve_path(
        required_string(immutable, "core_derived_contract", "immutable_contract"),
        source,
    )
    contract_hash = required_string(
        immutable, "core_derived_contract_sha256", "immutable_contract"
    )
    if sha256_file(contract_path) != contract_hash:
        raise ConfigurationError("Immutable core-derived 0.2 contract changed")
    policy = CoreDerived02PromotionPolicy(
        version="0.1",
        derived_run_id=required_string(
            approved, "derived_candidate_run_id", "approved_input"
        ),
        audit_run_id=required_string(
            approved, "derived_audit_run_id", "approved_input"
        ),
        cleaned_release_id=required_string(
            approved, "cleaned_release_id", "approved_input"
        ),
        contract_version=required_string(
            approved, "core_derived_contract_version", "approved_input"
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
        contract_path=contract_path,
        contract_sha256=contract_hash,
        approval_role=required_string(approval, "role", "human_approval"),
        approval_date=required_string(
            approval, "recorded_date", "human_approval"
        ),
        approval_statement=statement,
        approval_context=context,
        resolved_blocker=required_string(
            expected,
            "human_blocking_check_resolved_by_this_approval",
            "expected_audit",
        ),
        previous_release_id=required_string(
            previous, "release_id", "previous_current_release"
        ),
        previous_contract_version=required_string(
            previous,
            "core_derived_contract_version",
            "previous_current_release",
        ),
        previous_cleaned_release_id=required_string(
            previous, "cleaned_release_id", "previous_current_release"
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
        policy.derived_run_id != "20260808T074305Z"
        or policy.audit_run_id != policy.derived_run_id
        or policy.release_id != policy.derived_run_id
        or policy.cleaned_release_id != "20260807T154100Z"
        or policy.contract_version != "0.2"
        or policy.candidate_artifact_version != "0.2"
        or policy.audit_private_artifact_version != "0.2"
        or policy.audit_review_artifact_version != "0.2"
        or policy.approval_role != "data_owner"
        or policy.approval_date != "2026-08-08"
        or policy.resolved_blocker != "core_derived_candidate_release_approved"
        or policy.previous_release_id != "20260806T170134Z"
        or policy.previous_contract_version != "0.1"
        or policy.previous_cleaned_release_id != "20260806T114234Z"
        or previous.get("require_analysis_input_approved") is not True
        or release.get("current_release_pointer")
        != previous.get("current_release_pointer")
        or policy.release_artifact_version != "0.2"
        or Path(policy.previous_pointer_snapshot_name).name
        != policy.previous_pointer_snapshot_name
    ):
        raise ConfigurationError("Approved core-derived 0.2 scope changed")
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


def _copy_exact(source: Path, destination: Path, expected_hash: str) -> None:
    size = source.stat().st_size
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
    if (
        sha256_file(destination) != expected_hash
        or destination.stat().st_size != size
        or sha256_file(source) != expected_hash
    ):
        raise HarmonizationError("Audited derived bytes changed during promotion")


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["audit_metrics"]
    return "\n".join(
        (
            "# ASIC v3 core-derived 0.2 release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Core-derived contract: `{payload['core_derived_contract_version']}`",
            f"- Previous current release: `{payload['previous_release_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Compared output cells: `{metrics['compared_output_cell_count']}`",
            "- Audited Parquet bytes preserved exactly: `true`",
            "- Previous current-pointer bytes preserved exactly: `true`",
            "- Previous releases preserved: `true`",
            "- Core-derived layer ready: `true`",
            "- Approved as analysis input: `true`",
            "- Derivation rerun during promotion: `false`",
            "- Cohort generated: `false`",
            "- Time blocking applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human gate for the exactly audited core-derived 0.2 candidate. Static and dynamic Parquet bytes are copied unchanged. The candidate and core-derived 0.1 release remain preserved; only the recoverable current-derived pointer advances.",
            "",
        )
    )


def promote_core_derived_0_2_release(
    config: CoreDerived02PromotionConfig,
) -> CoreDerived02PromotionResult:
    if config.dataset_context != "production":
        raise HarmonizationError("Core-derived 0.2 promotion is production-only")
    policy = load_core_derived_0_2_promotion_policy(config.policy_path)
    core_policy = load_core_derived_policy(config.derived.policy_path)
    if (
        config.derived.policy_path.resolve() != policy.contract_path.resolve()
        or sha256_file(config.derived.policy_path) != policy.contract_sha256
        or core_policy.version != policy.contract_version
        or core_policy.cleaned_release_id != policy.cleaned_release_id
        or core_policy.candidate_artifact_version
        != policy.candidate_artifact_version
        or core_policy.audit_private_artifact_version
        != policy.audit_private_artifact_version
        or core_policy.audit_review_artifact_version
        != policy.audit_review_artifact_version
    ):
        raise HarmonizationError("Approved core-derived 0.2 contract changed")
    source = _load_core_derived_source(config.derived, core_policy)
    evidence = load_reviewed_evidence(config.derived, core_policy)
    candidate_dir = (
        config.data_root
        / core_policy.candidate_directory_name
        / policy.derived_run_id
    )
    candidate_manifest_path = candidate_dir / "derived_manifest.json"
    candidate_manifest = _read_json(
        candidate_manifest_path, "Approved core-derived 0.2 manifest"
    )
    expected_lineage = _candidate_lineage(source, evidence, core_policy)
    if not isinstance(candidate_manifest, dict) or (
        candidate_manifest.get("artifact") != "asic_v3_core_derived_candidate"
        or candidate_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or candidate_manifest.get("dataset_context") != config.dataset_context
        or candidate_manifest.get("run_id") != policy.derived_run_id
        or candidate_manifest.get("status")
        != "nonpublishable_requires_complete_derived_audit"
        or candidate_manifest.get("core_derived_contract_version")
        != policy.contract_version
        or candidate_manifest.get("lineage") != expected_lineage
        or candidate_manifest.get("derivation_accounting")
        != EXPECTED_ACCOUNTING
        or candidate_manifest.get("cleaning_applied") is not True
        or candidate_manifest.get("derivation_applied") is not True
        or candidate_manifest.get("rows_or_stays_filtered") is not False
        or candidate_manifest.get("source_columns_changed_or_dropped") is not False
        or candidate_manifest.get("cohort_generated") is not False
        or candidate_manifest.get("time_blocking_applied") is not False
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved core-derived 0.2 candidate changed")

    review_path = (
        config.reports_root
        / "review"
        / core_policy.audit_review_directory_name
        / f"{policy.audit_run_id}.json"
    )
    private_path = (
        config.reports_root
        / "private"
        / core_policy.audit_private_directory_name
        / policy.audit_run_id
        / "derived_audit_manifest.json"
    )
    review = _read_json(review_path, "Core-derived 0.2 audit review")
    private = _read_json(private_path, "Private core-derived 0.2 audit")
    blockers = review.get("blocking_findings") if isinstance(review, dict) else None
    if not isinstance(review, dict) or (
        review.get("artifact") != "asic_v3_core_derived_audit_review"
        or review.get("artifact_version") != policy.audit_review_artifact_version
        or review.get("dataset_context") != config.dataset_context
        or review.get("run_id") != policy.audit_run_id
        or review.get("derived_run_id") != policy.derived_run_id
        or review.get("cleaned_release_id") != policy.cleaned_release_id
        or review.get("core_derived_contract_version") != policy.contract_version
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_status") != "pass"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or not isinstance(blockers[0], dict)
        or blockers[0].get("check") != policy.resolved_blocker
        or review.get("metrics") != EXPECTED_METRICS
        or review.get("derivation_accounting") != EXPECTED_ACCOUNTING
        or review.get("every_input_column_preserved_exactly") is not True
        or review.get("every_derived_value_recomputed_exactly") is not True
        or review.get("candidate_data_modified") is not False
        or review.get("cleaned_release_modified") is not False
        or review.get("rows_or_stays_filtered") is not False
        or review.get("cohort_generated") is not False
        or review.get("time_blocking_applied") is not False
        or review.get("publication_ready") is not False
        or review.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved core-derived 0.2 audit changed")
    candidate_manifest_hash = sha256_file(candidate_manifest_path)
    if not isinstance(private, dict) or (
        private.get("artifact") != "asic_v3_core_derived_audit_private"
        or private.get("artifact_version") != policy.audit_private_artifact_version
        or private.get("dataset_context") != config.dataset_context
        or private.get("run_id") != policy.audit_run_id
        or private.get("derived_run_id") != policy.derived_run_id
        or private.get("derived_manifest_sha256") != candidate_manifest_hash
        or private.get("metrics") != EXPECTED_METRICS
        or private.get("derivation_accounting") != EXPECTED_ACCOUNTING
        or private.get("candidate_data_modified") is not False
        or private.get("cleaned_release_modified") is not False
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Private core-derived 0.2 audit changed")

    output_summaries = candidate_manifest.get("outputs")
    if not isinstance(output_summaries, dict):
        raise HarmonizationError("Core-derived 0.2 outputs are unavailable")
    expected_rows = {
        "static": EXPECTED_METRICS["compared_static_rows"],
        "dynamic": EXPECTED_METRICS["compared_dynamic_rows"],
    }
    source_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        path = candidate_dir / f"{table}.parquet"
        summary = output_summaries.get(table)
        expected_schema = derived_schema(
            source.schemas[table], table, policy.contract_version
        )
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Approved core-derived 0.2 output is missing")
        observed_hash = sha256_file(path)
        parquet = pq.ParquetFile(path)
        if (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(expected_schema)
            or not _parquet_schema_matches(parquet.schema_arrow, expected_schema)
        ):
            raise HarmonizationError(
                f"Approved core-derived 0.2 {table} output changed"
            )
        source_files[table] = {
            "path": path,
            "sha256": observed_hash,
            "row_count": expected_rows[table],
            "schema_sha256": str(summary["schema_sha256"]),
            "size_bytes": path.stat().st_size,
        }

    releases_root = config.data_root / policy.releases_directory
    release_dir = releases_root / policy.release_id
    staging_dir = releases_root / f".{policy.release_id}.incomplete"
    current_pointer = config.data_root / policy.current_release_pointer
    previous_pointer_hash = sha256_file(current_pointer)
    previous_pointer = _read_json(current_pointer, "Previous derived pointer")
    previous_release_dir = releases_root / policy.previous_release_id
    previous_manifest_path = previous_release_dir / "release_manifest.json"
    previous_manifest = _read_json(
        previous_manifest_path, "Previous derived release manifest"
    )
    if not isinstance(previous_pointer, dict) or (
        previous_pointer.get("artifact")
        != "asic_v3_current_core_derived_release"
        or previous_pointer.get("release_id") != policy.previous_release_id
        or previous_pointer.get("core_derived_contract_version")
        != policy.previous_contract_version
        or previous_pointer.get("cleaned_release_id")
        != policy.previous_cleaned_release_id
        or Path(str(previous_pointer.get("release_manifest", ""))).resolve()
        != previous_manifest_path.resolve()
        or previous_pointer.get("release_manifest_sha256")
        != sha256_file(previous_manifest_path)
        or previous_pointer.get("core_derived_layer_ready") is not True
        or previous_pointer.get("analysis_input_approved") is not True
        or previous_pointer.get("publication_ready") is not True
        or previous_pointer.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Previous current derived pointer changed")
    if not isinstance(previous_manifest, dict) or (
        previous_manifest.get("artifact") != "asic_v3_core_derived_release"
        or previous_manifest.get("release_id") != policy.previous_release_id
        or previous_manifest.get("status") != "released_core_derived_layer"
        or previous_manifest.get("core_derived_contract_version")
        != policy.previous_contract_version
        or previous_manifest.get("cleaned_release_id")
        != policy.previous_cleaned_release_id
        or previous_manifest.get("core_derived_layer_ready") is not True
        or previous_manifest.get("analysis_input_approved") is not True
        or previous_manifest.get("publication_ready") is not True
        or previous_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Previous derived release changed")
    previous_file_hashes: dict[str, str] = {}
    previous_files = previous_manifest.get("files")
    if not isinstance(previous_files, dict):
        raise HarmonizationError("Previous derived release files are unavailable")
    for table in ("static", "dynamic"):
        path = previous_release_dir / f"{table}.parquet"
        summary = previous_files.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Previous derived release file is missing")
        observed_hash = sha256_file(path)
        if summary.get("sha256") != observed_hash:
            raise HarmonizationError("Previous derived release file changed")
        previous_file_hashes[table] = observed_hash

    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{policy.release_id}.json"
    review_markdown = review_dir / f"{policy.release_id}.md"
    if any(
        path.exists()
        for path in (release_dir, staging_dir, review_json, review_markdown)
    ):
        raise HarmonizationError("Core-derived 0.2 promotion target already exists")
    releases_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    releases_root.chmod(0o700)
    staging_dir.mkdir(mode=0o700)
    staging_dir.chmod(0o700)

    released_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        item = source_files[table]
        destination = staging_dir / f"{table}.parquet"
        _copy_exact(item["path"], destination, item["sha256"])
        released_files[table] = {
            "path": str(release_dir / destination.name),
            "sha256": item["sha256"],
            "size_bytes": destination.stat().st_size,
            "row_count": item["row_count"],
            "schema_sha256": item["schema_sha256"],
            "byte_identical_to_audited_candidate": True,
        }

    previous_snapshot = staging_dir / policy.previous_pointer_snapshot_name
    shutil.copyfile(current_pointer, previous_snapshot)
    previous_snapshot.chmod(0o600)
    if sha256_file(previous_snapshot) != previous_pointer_hash:
        raise HarmonizationError("Previous derived-pointer snapshot changed")

    generated = utc_timestamp()
    release_manifest_path = staging_dir / "release_manifest.json"
    release_manifest = {
        "artifact": "asic_v3_core_derived_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "status": "released_core_derived_layer",
        "cleaned_release_id": policy.cleaned_release_id,
        "core_derived_contract_version": policy.contract_version,
        "previous_current_release_id": policy.previous_release_id,
        "lineage": {
            "derived_candidate_run_id": policy.derived_run_id,
            "derived_candidate_manifest_sha256": candidate_manifest_hash,
            "derived_audit_run_id": policy.audit_run_id,
            "derived_audit_review_sha256": sha256_file(review_path),
            "derived_audit_private_manifest_sha256": sha256_file(private_path),
            "core_derived_contract_sha256": policy.contract_sha256,
            "cleaned_release_manifest_sha256": sha256_file(
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
            "context": policy.approval_context,
            "resolved_blocker": policy.resolved_blocker,
        },
        "audit_metrics": EXPECTED_METRICS,
        "derivation_accounting": EXPECTED_ACCOUNTING,
        "files": released_files,
        "previous_current_pointer_snapshot": {
            "path": str(release_dir / policy.previous_pointer_snapshot_name),
            "sha256": previous_pointer_hash,
            "byte_identical_to_previous_pointer": True,
        },
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
        "previous_releases_preserved": True,
        "cleaning_rerun_during_promotion": False,
        "derivation_rerun_during_promotion": False,
        "rows_or_stays_filtered": False,
        "source_columns_changed_or_dropped": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "core_derived_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(release_manifest_path, release_manifest, 0o600)
    staging_dir.replace(release_dir)
    release_dir.chmod(0o700)
    release_manifest_path = release_dir / "release_manifest.json"
    previous_snapshot = release_dir / policy.previous_pointer_snapshot_name

    if sha256_file(current_pointer) != previous_pointer_hash:
        raise HarmonizationError("Current derived pointer changed during promotion")
    for table, expected_hash in previous_file_hashes.items():
        if sha256_file(previous_release_dir / f"{table}.parquet") != expected_hash:
            raise HarmonizationError("Previous derived release changed during promotion")
    current_payload = {
        "artifact": "asic_v3_current_core_derived_release",
        "artifact_version": "0.2",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "core_derived_contract_version": policy.contract_version,
        "cleaned_release_id": policy.cleaned_release_id,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "previous_release_id": policy.previous_release_id,
        "previous_current_pointer_snapshot": str(previous_snapshot),
        "previous_current_pointer_sha256": previous_pointer_hash,
        "core_derived_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_core_derived_0_2_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "core_derived_contract_version": policy.contract_version,
        "cleaned_release_id": policy.cleaned_release_id,
        "previous_release_id": policy.previous_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "audit_metrics": EXPECTED_METRICS,
        "derivation_accounting": EXPECTED_ACCOUNTING,
        "payload_byte_identity_preserved": True,
        "previous_current_pointer_bytes_preserved": True,
        "previous_releases_preserved": True,
        "current_pointer_advanced_atomically": True,
        "core_derived_layer_ready": True,
        "analysis_input_approved": True,
        "derivation_rerun_during_promotion": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_markdown, _markdown(review_payload), 0o640)
    return CoreDerived02PromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        previous_pointer_snapshot_path=previous_snapshot,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
