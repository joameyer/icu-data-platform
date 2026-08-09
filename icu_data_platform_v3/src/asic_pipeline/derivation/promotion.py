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
    _read_json,
    _schema_digest,
    derived_schema,
    load_core_derived_config,
    load_core_derived_policy,
    load_reviewed_evidence,
)
from asic_pipeline.derivation.review import (
    load_cleaned_release_input,
    load_derivation_contract_review_policy,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVAL_STATEMENT = (
    "I approve promotion of core-derived candidate 20260806T170134Z under "
    "core-derived contract 0.1."
)
APPROVAL_CONTEXT = (
    "Explicit approval immediately following the complete core-derived "
    "candidate audit review."
)
EXPECTED_METRICS = {
    "compared_static_rows": 16_054,
    "compared_dynamic_rows": 24_069_379,
    "compared_output_cell_count": 3_322_168_300,
    "expected_output_cell_count": 3_322_168_300,
    "static_output_column_count": 37,
    "dynamic_output_column_count": 138,
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
    "overwrite_existing_release": False,
    "overwrite_existing_current_pointer": False,
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
class CoreDerivedPromotionPolicy:
    version: str
    derived_run_id: str
    audit_run_id: str
    cleaned_release_id: str
    core_derived_contract_version: str
    candidate_artifact_version: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    approval_role: str
    approval_date: str
    approval_statement: str
    approval_context: str
    expected_metrics: dict[str, int]
    expected_accounting: dict[str, int]
    resolved_blocker: str
    release_id: str
    releases_directory: Path
    current_release_pointer: Path
    release_artifact_version: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class CoreDerivedPromotionConfig:
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
class CoreDerivedPromotionResult:
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


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_core_derived_promotion_config(
    path: str | Path,
) -> CoreDerivedPromotionConfig:
    source = Path(path).expanduser().resolve()
    derived = load_core_derived_config(source)
    raw = load_yaml_mapping(source, "Core-derived promotion configuration")
    policy_path = resolve_path(
        required_string(raw, "core_derived_promotion_policy", "config"), source
    )
    return CoreDerivedPromotionConfig(derived=derived, policy_path=policy_path)


def load_core_derived_promotion_policy(
    path: str | Path,
) -> CoreDerivedPromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Core-derived promotion policy")
    if raw.get("core_derived_promotion_policy_version") != "0.1" or raw.get(
        "status"
    ) != "explicitly_approved_for_immutable_core_derived_release":
        raise ConfigurationError("Core-derived promotion approval is invalid")
    approved = _mapping(raw.get("approved_input"), "approved_input")
    approval = _mapping(raw.get("human_approval"), "human_approval")
    expected = _mapping(raw.get("expected_audit"), "expected_audit")
    release = _mapping(raw.get("release"), "release")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Core-derived promotion boundary changed")
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
        raise ConfigurationError("Expected core-derived audit changed")
    statement = required_string(approval, "statement", "human_approval")
    context = required_string(approval, "context", "human_approval")
    if statement != APPROVAL_STATEMENT or context != APPROVAL_CONTEXT:
        raise ConfigurationError("Data-owner core-derived approval changed")
    policy = CoreDerivedPromotionPolicy(
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
        core_derived_contract_version=required_string(
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
        approval_role=required_string(approval, "role", "human_approval"),
        approval_date=required_string(
            approval, "recorded_date", "human_approval"
        ),
        approval_statement=statement,
        approval_context=context,
        expected_metrics=dict(EXPECTED_METRICS),
        expected_accounting=dict(EXPECTED_ACCOUNTING),
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
        policy.derived_run_id != policy.release_id
        or policy.audit_run_id != policy.derived_run_id
        or policy.cleaned_release_id != "20260806T114234Z"
        or policy.core_derived_contract_version != "0.1"
        or policy.approval_role != "data_owner"
        or policy.resolved_blocker != "core_derived_candidate_release_approved"
    ):
        raise ConfigurationError("Approved core-derived promotion lineage is inconsistent")
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
            "# ASIC v3 core-derived release promotion",
            "",
            f"- Promoted (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Release ID: `{payload['release_id']}`",
            f"- Core-derived contract: `{payload['core_derived_contract_version']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Compared output cells: `{metrics['compared_output_cell_count']}`",
            "- Audited Parquet bytes preserved exactly: `true`",
            "- Core-derived layer ready: `true`",
            "- Approved as analysis input: `true`",
            "- Derivation rerun during promotion: `false`",
            "- Cohort generated: `false`",
            "- Time blocking applied: `false`",
            "- External data export authorized: `false`",
            "",
            "## Boundary",
            "",
            "This promotion resolves the sole human release gate for the completely audited core-derived candidate. The immutable candidate remains preserved; no clinical value, row, schema, provenance field, or Parquet byte is changed during promotion.",
            "",
        )
    )


def promote_core_derived_release(
    config: CoreDerivedPromotionConfig,
) -> CoreDerivedPromotionResult:
    if config.dataset_context != "production":
        raise HarmonizationError("The approved core-derived promotion is production-only")
    policy = load_core_derived_promotion_policy(config.policy_path)
    core_policy = load_core_derived_policy(config.derived.policy_path)
    base_policy = load_derivation_contract_review_policy(
        config.derived.derivation_review.policy_path
    )
    if (
        core_policy.version != policy.core_derived_contract_version
        or core_policy.cleaned_release_id != policy.cleaned_release_id
        or core_policy.candidate_artifact_version
        != policy.candidate_artifact_version
        or core_policy.audit_private_artifact_version
        != policy.audit_private_artifact_version
        or core_policy.audit_review_artifact_version
        != policy.audit_review_artifact_version
    ):
        raise HarmonizationError("Approved core-derived promotion contract changed")
    source = load_cleaned_release_input(config.derived.derivation_review, base_policy)
    evidence = load_reviewed_evidence(config.derived, core_policy)
    candidate_dir = (
        config.data_root
        / core_policy.candidate_directory_name
        / policy.derived_run_id
    )
    candidate_manifest_path = candidate_dir / "derived_manifest.json"
    candidate_manifest = _read_json(
        candidate_manifest_path, "Core-derived candidate manifest"
    )
    expected_lineage = {
        "cleaned_release_id": policy.cleaned_release_id,
        "cleaned_release_manifest_sha256": sha256_file(source.release_manifest_path),
        "core_derived_contract_sha256": sha256_file(core_policy.source_path),
        "consolidated_derivation_review_run_id": evidence.core_review_payload[
            "run_id"
        ],
        "consolidated_derivation_review_sha256": sha256_file(
            evidence.core_review_path
        ),
        "driving_pressure_semantics_review_run_id": core_policy.driving_pressure_review_run_id,
        "driving_pressure_semantics_review_sha256": sha256_file(
            evidence.driving_review_path
        ),
    }
    if (
        not isinstance(candidate_manifest, dict)
        or candidate_manifest.get("artifact") != "asic_v3_core_derived_candidate"
        or candidate_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or candidate_manifest.get("run_id") != policy.derived_run_id
        or candidate_manifest.get("status")
        != "nonpublishable_requires_complete_derived_audit"
        or candidate_manifest.get("core_derived_contract_version")
        != policy.core_derived_contract_version
        or candidate_manifest.get("lineage") != expected_lineage
        or candidate_manifest.get("derivation_accounting")
        != policy.expected_accounting
        or candidate_manifest.get("cleaning_applied") is not True
        or candidate_manifest.get("derivation_applied") is not True
        or candidate_manifest.get("rows_or_stays_filtered") is not False
        or candidate_manifest.get("source_columns_changed_or_dropped") is not False
        or candidate_manifest.get("cohort_generated") is not False
        or candidate_manifest.get("time_blocking_applied") is not False
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved core-derived candidate changed")

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
    review = _read_json(review_path, "Core-derived audit review")
    private = _read_json(private_path, "Private core-derived audit manifest")
    blockers = review.get("blocking_findings") if isinstance(review, dict) else None
    if (
        not isinstance(review, dict)
        or not isinstance(private, dict)
        or review.get("artifact") != "asic_v3_core_derived_audit_review"
        or review.get("artifact_version") != policy.audit_review_artifact_version
        or review.get("run_id") != policy.audit_run_id
        or review.get("derived_run_id") != policy.derived_run_id
        or review.get("cleaned_release_id") != policy.cleaned_release_id
        or review.get("core_derived_contract_version")
        != policy.core_derived_contract_version
        or review.get("overall_status") != "pending_human_review"
        or review.get("technical_status") != "pass"
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or len(blockers) != 1
        or blockers[0].get("check") != policy.resolved_blocker
        or review.get("metrics") != policy.expected_metrics
        or review.get("derivation_accounting") != policy.expected_accounting
        or review.get("every_input_column_preserved_exactly") is not True
        or review.get("every_derived_value_recomputed_exactly") is not True
        or review.get("candidate_data_modified") is not False
        or review.get("cleaned_release_modified") is not False
        or review.get("rows_or_stays_filtered") is not False
        or review.get("cohort_generated") is not False
        or review.get("time_blocking_applied") is not False
        or review.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved core-derived audit review changed")
    candidate_manifest_sha256 = sha256_file(candidate_manifest_path)
    if (
        private.get("artifact") != "asic_v3_core_derived_audit_private"
        or private.get("artifact_version") != policy.audit_private_artifact_version
        or private.get("run_id") != policy.audit_run_id
        or private.get("derived_run_id") != policy.derived_run_id
        or private.get("derived_manifest_sha256") != candidate_manifest_sha256
        or private.get("metrics") != policy.expected_metrics
        or private.get("derivation_accounting") != policy.expected_accounting
        or private.get("candidate_data_modified") is not False
        or private.get("cleaned_release_modified") is not False
        or private.get("publication_ready") is not False
    ):
        raise HarmonizationError("Approved private core-derived audit evidence changed")

    output_summaries = candidate_manifest.get("outputs")
    if not isinstance(output_summaries, dict):
        raise HarmonizationError("Core-derived candidate outputs are unavailable")
    expected_rows = {
        "static": policy.expected_metrics["compared_static_rows"],
        "dynamic": policy.expected_metrics["compared_dynamic_rows"],
    }
    source_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        source_path = candidate_dir / f"{table}.parquet"
        summary = output_summaries.get(table)
        expected_schema = derived_schema(source.schemas[table], table)
        if not isinstance(summary, dict) or not source_path.is_file():
            raise HarmonizationError("An approved core-derived output is unavailable")
        observed_hash = sha256_file(source_path)
        parquet = pq.ParquetFile(source_path)
        if (
            summary.get("sha256") != observed_hash
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(expected_schema)
            or not _parquet_schema_matches(parquet.schema_arrow, expected_schema)
        ):
            raise HarmonizationError("An approved core-derived output changed")
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
        raise HarmonizationError("Approved core-derived release target already exists")
    releases_root.mkdir(parents=True, mode=0o700)
    releases_root.chmod(0o700)
    current_pointer.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    current_pointer.parent.chmod(0o700)
    staging_dir.mkdir(mode=0o700)
    staging_dir.chmod(0o700)

    released_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        source_file = source_files[table]
        destination = staging_dir / f"{table}.parquet"
        shutil.copyfile(source_file["path"], destination)
        destination.chmod(0o600)
        destination_hash = sha256_file(destination)
        if (
            destination_hash != source_file["sha256"]
            or destination.stat().st_size != source_file["size_bytes"]
            or sha256_file(source_file["path"]) != source_file["sha256"]
        ):
            raise HarmonizationError("Audited Parquet bytes changed during promotion")
        released_files[table] = {
            "path": str(release_dir / destination.name),
            "sha256": destination_hash,
            "size_bytes": destination.stat().st_size,
            "row_count": source_file["row_count"],
            "schema_sha256": source_file["schema_sha256"],
            "byte_identical_to_audited_candidate": True,
        }

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
        "core_derived_contract_version": policy.core_derived_contract_version,
        "lineage": {
            "derived_candidate_run_id": policy.derived_run_id,
            "derived_candidate_manifest_sha256": candidate_manifest_sha256,
            "derived_audit_run_id": policy.audit_run_id,
            "derived_audit_review_sha256": sha256_file(review_path),
            "derived_audit_private_manifest_sha256": sha256_file(private_path),
            "core_derived_contract_sha256": sha256_file(core_policy.source_path),
            "cleaned_release_manifest_sha256": sha256_file(
                source.release_manifest_path
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
        "audit_metrics": policy.expected_metrics,
        "derivation_accounting": policy.expected_accounting,
        "files": released_files,
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
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
    release_manifest_path = release_dir / release_manifest_path.name

    current_payload = {
        "artifact": "asic_v3_current_core_derived_release",
        "artifact_version": "0.1",
        "dataset_context": config.dataset_context,
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "core_derived_contract_version": policy.core_derived_contract_version,
        "cleaned_release_id": policy.cleaned_release_id,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "core_derived_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(current_pointer, current_payload, 0o600)

    review_payload = {
        "artifact": "asic_v3_core_derived_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "core_derived_contract_version": policy.core_derived_contract_version,
        "cleaned_release_id": policy.cleaned_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": expected_rows,
        "audit_metrics": policy.expected_metrics,
        "derivation_accounting": policy.expected_accounting,
        "payload_byte_identity_preserved": True,
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
    _write_markdown(review_markdown, _markdown(review_payload), 0o640)
    return CoreDerivedPromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
