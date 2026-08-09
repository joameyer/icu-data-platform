from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
from typing import Any

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import (
    load_yaml_mapping,
    required_string,
    resolve_path,
    validate_path_within,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.time_blocking.contract import (
    TimeBlockingContract,
    load_time_blocking_contract,
)
from asic_pipeline.time_blocking.engine import (
    TimeBlockingBuildConfig,
    _source_input,
    load_time_blocking_build_config,
    load_time_blocking_implementation_policy,
    require_production_execution_authorization,
)


EXPECTED_BOUNDARY = {
    "copy_audited_candidate_bytes_unchanged": True,
    "preserve_candidate_artifacts": True,
    "require_core_derived_pointer_unchanged": True,
    "require_core_derived_release_bytes_unchanged": True,
    "copy_static_table": False,
    "repeat_static_values_on_blocks": False,
    "overwrite_existing_release": False,
    "create_only_time_blocking_8h_current_pointer": True,
    "replace_core_derived_current_pointer": False,
    "rerun_time_blocking": False,
    "rerun_derivation": False,
    "filter_rows_or_stays": False,
    "create_analysis_cohort": False,
    "apply_carry_forward_or_imputation": False,
    "emit_medication_dose_totals": False,
    "authorize_analysis_input": True,
    "authorize_external_data_export": False,
}

EXPECTED_CANDIDATE_METRIC_KEYS = frozenset(
    {
        "source_dynamic_row_count",
        "assigned_source_row_count",
        "stay_summary_row_count",
        "block_row_count",
        "empty_block_count",
        "terminal_partial_block_count",
        "output_feature_count",
    }
)

EXPECTED_AUDIT_METRIC_KEYS = frozenset(
    {
        "assigned_source_rows",
        "compared_block_rows",
        "compared_feature_cells",
        "compared_stays",
    }
)


@dataclass(frozen=True)
class TimeBlockingPromotionPolicy:
    candidate_run_id: str
    audit_run_id: str
    core_derived_release_id: str
    contract_version: str
    candidate_artifact_version: str
    audit_artifact_version: str
    contract_path: Path
    contract_sha256: str
    implementation_policy_path: Path
    implementation_policy_sha256: str
    execution_authorization_path: Path
    execution_authorization_sha256: str
    approval_role: str
    approval_date: str
    approval_statement: str
    approval_context: str
    candidate_manifest_sha256: str
    expected_candidate_metrics: dict[str, int]
    expected_audit_metrics: dict[str, int]
    resolved_blocker: str
    release_id: str
    releases_directory: Path
    current_release_pointer: Path
    release_manifest_name: str
    review_directory: Path
    release_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class TimeBlockingPromotionConfig:
    build: TimeBlockingBuildConfig
    policy_path: Path


@dataclass(frozen=True)
class TimeBlockingPromotionResult:
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


def _contained_path(value: Any, location: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{location} must be a non-empty path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def _positive_metrics(
    value: Any,
    location: str,
    expected_keys: frozenset[str],
) -> dict[str, int]:
    mapping = _mapping(value, location)
    if set(mapping) != expected_keys or any(
        not isinstance(item, int) or isinstance(item, bool) or item <= 0
        for item in mapping.values()
    ):
        raise ConfigurationError(f"{location} has invalid metrics")
    return {key: int(mapping[key]) for key in sorted(mapping)}


def _expected_approval_statement(
    candidate_run_id: str,
    contract_version: str,
    release_id: str,
) -> str:
    return (
        "I approve promotion of ASIC v3 8-hour time-blocking candidate "
        f"{candidate_run_id} under time-blocking contract {contract_version} "
        f"as immutable release {release_id}, and authorize operator-executed "
        "promotion and atomic creation of only the 8-hour time-blocking "
        "current-release pointer. External export remains unauthorized."
    )


def load_time_blocking_promotion_policy(
    path: str | Path,
) -> TimeBlockingPromotionPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed time-blocking promotion policy")
    if (
        raw.get("time_blocking_promotion_policy_version") != "0.1"
        or raw.get("status")
        != "explicitly_approved_for_immutable_time_blocking_release"
        or _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY
    ):
        raise ConfigurationError("Time-blocking promotion approval changed")

    approved = _mapping(raw.get("approved_input"), "approved_input")
    candidate_run_id = required_string(
        approved, "candidate_run_id", "approved_input"
    )
    audit_run_id = required_string(approved, "audit_run_id", "approved_input")
    core_release_id = required_string(
        approved, "core_derived_release_id", "approved_input"
    )
    contract_version = required_string(
        approved, "time_blocking_contract_version", "approved_input"
    )
    if (
        not RUN_ID_PATTERN.fullmatch(candidate_run_id)
        or not RUN_ID_PATTERN.fullmatch(audit_run_id)
        or candidate_run_id != audit_run_id
    ):
        raise ConfigurationError("Approved promotion run IDs are invalid")

    contract = _mapping(raw.get("immutable_contract"), "immutable_contract")
    workflow = _mapping(raw.get("immutable_workflow"), "immutable_workflow")
    approval = _mapping(raw.get("human_approval"), "human_approval")
    release = _mapping(raw.get("release"), "release")
    release_id = required_string(release, "release_id", "release")
    statement = required_string(approval, "statement", "human_approval")
    if (
        release_id != candidate_run_id
        or approval.get("role") != "data_owner"
        or not isinstance(approval.get("recorded_date"), str)
        or statement
        != _expected_approval_statement(
            candidate_run_id, contract_version, release_id
        )
        or release.get("require_initial_current_release_pointer_absent") is not True
    ):
        raise ConfigurationError("Exact time-blocking promotion approval changed")

    expected_candidate = _mapping(
        raw.get("expected_candidate"), "expected_candidate"
    )
    expected_audit = _mapping(raw.get("expected_audit"), "expected_audit")
    resolved_blocker = required_string(
        expected_audit,
        "human_blocking_check_resolved_by_this_approval",
        "expected_audit",
    )
    if (
        expected_audit.get("overall_status_before_approval")
        != "pending_human_review"
        or expected_audit.get("technical_blocking_finding_count") != 0
        or expected_audit.get("human_blocking_finding_count_before_approval") != 1
        or resolved_blocker != "time_blocking_candidate_promotion_approved"
    ):
        raise ConfigurationError("Approved audit gate changed")

    policy = TimeBlockingPromotionPolicy(
        candidate_run_id=candidate_run_id,
        audit_run_id=audit_run_id,
        core_derived_release_id=core_release_id,
        contract_version=contract_version,
        candidate_artifact_version=required_string(
            approved, "candidate_artifact_version", "approved_input"
        ),
        audit_artifact_version=required_string(
            approved, "audit_artifact_version", "approved_input"
        ),
        contract_path=resolve_path(
            required_string(contract, "path", "immutable_contract"), source
        ),
        contract_sha256=required_string(
            contract, "sha256", "immutable_contract"
        ),
        implementation_policy_path=resolve_path(
            required_string(
                workflow, "implementation_policy", "immutable_workflow"
            ),
            source,
        ),
        implementation_policy_sha256=required_string(
            workflow, "implementation_policy_sha256", "immutable_workflow"
        ),
        execution_authorization_path=resolve_path(
            required_string(
                workflow, "execution_authorization", "immutable_workflow"
            ),
            source,
        ),
        execution_authorization_sha256=required_string(
            workflow, "execution_authorization_sha256", "immutable_workflow"
        ),
        approval_role="data_owner",
        approval_date=str(approval["recorded_date"]),
        approval_statement=statement,
        approval_context=required_string(
            approval, "context", "human_approval"
        ),
        candidate_manifest_sha256=required_string(
            expected_candidate, "manifest_sha256", "expected_candidate"
        ),
        expected_candidate_metrics=_positive_metrics(
            expected_candidate.get("metrics"),
            "expected_candidate.metrics",
            EXPECTED_CANDIDATE_METRIC_KEYS,
        ),
        expected_audit_metrics=_positive_metrics(
            expected_audit.get("metrics"),
            "expected_audit.metrics",
            EXPECTED_AUDIT_METRIC_KEYS,
        ),
        resolved_blocker=resolved_blocker,
        release_id=release_id,
        releases_directory=_contained_path(
            release.get("releases_directory"), "release.releases_directory"
        ),
        current_release_pointer=_contained_path(
            release.get("current_release_pointer"),
            "release.current_release_pointer",
        ),
        release_manifest_name=required_string(
            release, "release_manifest", "release"
        ),
        review_directory=_contained_path(
            release.get("review_directory"), "release.review_directory"
        ),
        release_artifact_version=required_string(
            release, "artifact_version", "release"
        ),
        source_path=source,
    )
    if (
        policy.expected_audit_metrics["compared_feature_cells"]
        != policy.expected_candidate_metrics["block_row_count"]
        * policy.expected_candidate_metrics["output_feature_count"]
        or policy.expected_audit_metrics["assigned_source_rows"]
        != policy.expected_candidate_metrics["assigned_source_row_count"]
        or policy.expected_audit_metrics["compared_block_rows"]
        != policy.expected_candidate_metrics["block_row_count"]
        or policy.expected_audit_metrics["compared_stays"]
        != policy.expected_candidate_metrics["stay_summary_row_count"]
    ):
        raise ConfigurationError("Candidate and audit metrics disagree")
    return policy


def load_time_blocking_promotion_config(
    path: str | Path,
) -> TimeBlockingPromotionConfig:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Time-blocking promotion configuration")
    build = load_time_blocking_build_config(source)
    policy_path = resolve_path(
        required_string(
            raw, "time_blocking_8h_promotion_policy", "config"
        ),
        source,
    )
    asic_root = source.parents[3] / "asic"
    validate_path_within(
        policy_path, asic_root, "time-blocking promotion policy"
    )
    return TimeBlockingPromotionConfig(build=build, policy_path=policy_path)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be an object")
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


def _copy_exact(source: Path, destination: Path, expected_hash: str) -> None:
    size = source.stat().st_size
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
    if (
        sha256_file(source) != expected_hash
        or sha256_file(destination) != expected_hash
        or destination.stat().st_size != size
    ):
        raise HarmonizationError("Audited time-blocking bytes changed during promotion")


def _promotion_markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 8-hour time-blocking release promotion",
            "",
            f"- Release ID: `{payload['release_id']}`",
            f"- Candidate run: `{payload['candidate_run_id']}`",
            f"- Audit run: `{payload['audit_run_id']}`",
            f"- Block rows: `{payload['metrics']['compared_block_rows']}`",
            f"- Compared feature cells: `{payload['metrics']['compared_feature_cells']}`",
            "- Overall status: **PASS**",
            "- Technical blockers: `0`",
            "- Audited candidate bytes preserved exactly: `true`",
            "- Core-derived release and pointer unchanged: `true`",
            "- Static table copied or repeated: `false`",
            "- Cohort generated: `false`",
            "- Carry-forward or imputation applied: `false`",
            "- External data export authorized: `false`",
            "",
        )
    )


def _validate_contract_binding(
    policy: TimeBlockingPromotionPolicy,
    contract: TimeBlockingContract,
) -> None:
    if (
        contract.source_path.resolve() != policy.contract_path.resolve()
        or sha256_file(policy.contract_path) != policy.contract_sha256
        or contract.version != policy.contract_version
        or contract.core_derived_release_id != policy.core_derived_release_id
        or contract.release_directory != policy.releases_directory
        or contract.current_release_pointer != policy.current_release_pointer
        or policy.release_manifest_name != "release_manifest.json"
    ):
        raise HarmonizationError("Approved time-blocking contract changed")


def _validate_candidate_outputs(
    candidate_dir: Path,
    contract: TimeBlockingContract,
    manifest: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    outputs = manifest.get("outputs")
    expected_names = (
        contract.block_file,
        contract.stay_summary_file,
        contract.dictionary_parquet,
        contract.dictionary_markdown,
    )
    if not isinstance(outputs, dict) or set(outputs) != set(expected_names):
        raise HarmonizationError("Approved candidate output set changed")
    validated: dict[str, dict[str, Any]] = {}
    for name in expected_names:
        summary = outputs.get(name)
        path = candidate_dir / name
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError(f"Approved candidate output is missing: {name}")
        observed_hash = sha256_file(path)
        if summary.get("sha256") != observed_hash:
            raise HarmonizationError(f"Approved candidate output changed: {name}")
        if name.endswith(".parquet"):
            row_count = summary.get("row_count")
            schema_sha256 = summary.get("schema_sha256")
            if (
                not isinstance(row_count, int)
                or isinstance(row_count, bool)
                or row_count < 0
                or not isinstance(schema_sha256, str)
                or len(schema_sha256) != 64
            ):
                raise HarmonizationError(
                    f"Approved candidate Parquet manifest is invalid: {name}"
                )
        elif (
            summary.get("row_count") is not None
            or summary.get("schema_sha256") is not None
        ):
            raise HarmonizationError("Candidate Markdown metadata changed")
        validated[name] = {
            "source_path": path,
            "sha256": observed_hash,
            "size_bytes": path.stat().st_size,
            "row_count": summary.get("row_count"),
            "schema_sha256": summary.get("schema_sha256"),
        }
    return validated


def _unchanged_source_and_candidate(
    source: Any,
    candidate_hashes: dict[Path, str],
) -> bool:
    return (
        sha256_file(source.pointer_path) == source.pointer_sha256
        and sha256_file(source.release_manifest_path)
        == source.release_manifest_sha256
        and all(
            sha256_file(source.table_paths[table]) == expected
            for table, expected in source.table_hashes.items()
        )
        and all(sha256_file(path) == expected for path, expected in candidate_hashes.items())
    )


def promote_time_blocking_release(
    config: TimeBlockingPromotionConfig,
) -> TimeBlockingPromotionResult:
    if config.build.dataset_context != "production":
        raise HarmonizationError("Time-blocking release promotion is production-only")
    policy = load_time_blocking_promotion_policy(config.policy_path)
    implementation = load_time_blocking_implementation_policy(
        config.build.implementation_policy_path
    )
    if (
        implementation.source_path.resolve()
        != policy.implementation_policy_path.resolve()
        or implementation.source_sha256 != policy.implementation_policy_sha256
        or implementation.execution_authorization_path is None
        or implementation.execution_authorization_path.resolve()
        != policy.execution_authorization_path.resolve()
        or sha256_file(policy.execution_authorization_path)
        != policy.execution_authorization_sha256
    ):
        raise HarmonizationError("Approved time-blocking workflow changed")
    contract = load_time_blocking_contract(implementation.contract_path)
    _validate_contract_binding(policy, contract)
    execution_lineage = require_production_execution_authorization(
        implementation,
        contract,
        policy.candidate_run_id,
        policy.audit_run_id,
    )
    source = _source_input(config.build, contract)

    candidate_dir = (
        config.build.data_root
        / contract.candidate_directory
        / policy.candidate_run_id
    )
    candidate_manifest_path = candidate_dir / contract.candidate_manifest
    candidate_manifest = _read_json(
        candidate_manifest_path, "Approved time-blocking candidate manifest"
    )
    if sha256_file(candidate_manifest_path) != policy.candidate_manifest_sha256:
        raise HarmonizationError("Approved candidate manifest hash changed")
    lineage_value = candidate_manifest.get("lineage")
    lineage = lineage_value if isinstance(lineage_value, dict) else {}
    if (
        candidate_manifest.get("artifact") != "asic_v3_time_blocking_candidate"
        or candidate_manifest.get("artifact_version")
        != policy.candidate_artifact_version
        or candidate_manifest.get("dataset_context") != "production"
        or candidate_manifest.get("run_id") != policy.candidate_run_id
        or candidate_manifest.get("status")
        != "nonpublishable_requires_independent_audit_and_human_promotion"
        or candidate_manifest.get("resolution") != contract.resolution_label
        or candidate_manifest.get("time_blocking_contract_version")
        != contract.version
        or candidate_manifest.get("metrics")
        != policy.expected_candidate_metrics
        or lineage.get("contract_sha256") != policy.contract_sha256
        or lineage.get("core_derived_release_id")
        != policy.core_derived_release_id
        or lineage.get("core_derived_release_manifest_sha256")
        != source.release_manifest_sha256
        or lineage.get("core_derived_file_hashes") != source.table_hashes
        or Path(str(lineage.get("source_static_path", ""))).resolve()
        != source.table_paths["static"].resolve()
        or any(lineage.get(key) != value for key, value in execution_lineage.items())
        or candidate_manifest.get("static_table_copied") is not False
        or candidate_manifest.get("static_table_repeated_on_blocks") is not False
        or candidate_manifest.get("rows_or_stays_filtered") is not False
        or candidate_manifest.get("analysis_cohort_created") is not False
        or candidate_manifest.get("carry_forward_or_imputation_applied") is not False
        or candidate_manifest.get("medication_dose_totals_emitted") is not False
        or candidate_manifest.get("release_created") is not False
        or candidate_manifest.get("current_release_pointer_modified") is not False
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved time-blocking candidate changed")
    candidate_outputs = _validate_candidate_outputs(
        candidate_dir, contract, candidate_manifest
    )
    if (
        candidate_outputs[contract.block_file]["row_count"]
        != policy.expected_candidate_metrics["block_row_count"]
        or candidate_outputs[contract.stay_summary_file]["row_count"]
        != policy.expected_candidate_metrics["stay_summary_row_count"]
    ):
        raise HarmonizationError("Candidate row counts changed")

    build_review = (
        config.build.reports_root
        / "review"
        / implementation.build_review_directory
        / f"{policy.candidate_run_id}.json"
    )
    build_private = (
        config.build.reports_root
        / "private"
        / implementation.build_private_directory
        / policy.candidate_run_id
        / "build_manifest.json"
    )
    audit_review = (
        config.build.reports_root
        / "review"
        / implementation.audit_review_directory
        / f"{policy.audit_run_id}.json"
    )
    audit_private = (
        config.build.reports_root
        / "private"
        / implementation.audit_private_directory
        / policy.audit_run_id
        / "audit_manifest.json"
    )
    build_review_payload = _read_json(build_review, "Candidate build review")
    build_private_payload = _read_json(build_private, "Private candidate build evidence")
    review = _read_json(audit_review, "Candidate audit review")
    private = _read_json(audit_private, "Private candidate audit evidence")
    blockers = review.get("blocking_findings")
    if (
        build_review_payload.get("artifact")
        != "asic_v3_time_blocking_candidate_build_review"
        or build_review_payload.get("artifact_version")
        != policy.candidate_artifact_version
        or build_review_payload.get("dataset_context") != "production"
        or build_review_payload.get("run_id") != policy.candidate_run_id
        or build_review_payload.get("time_blocking_contract_version")
        != contract.version
        or build_review_payload.get("candidate_manifest_sha256")
        != policy.candidate_manifest_sha256
        or build_review_payload.get("metrics")
        != policy.expected_candidate_metrics
        or build_review_payload.get("overall_status")
        != "nonpublishable_requires_independent_audit"
        or build_review_payload.get("source_release_modified") is not False
        or build_review_payload.get("release_created") is not False
        or build_review_payload.get("current_release_pointer_modified") is not False
        or build_review_payload.get("rows_or_stays_filtered") is not False
        or build_review_payload.get("carry_forward_or_imputation_applied") is not False
        or build_review_payload.get("publication_ready") is not False
        or build_review_payload.get("external_data_export_authorized") is not False
        or build_private_payload != candidate_manifest
    ):
        raise HarmonizationError("Approved candidate build evidence changed")
    if (
        review.get("artifact")
        != "asic_v3_time_blocking_candidate_audit_review"
        or review.get("artifact_version") != policy.audit_artifact_version
        or review.get("dataset_context") != "production"
        or review.get("run_id") != policy.audit_run_id
        or review.get("build_run_id") != policy.candidate_run_id
        or review.get("overall_status") != "pending_human_review"
        or review.get("time_blocking_contract_version") != contract.version
        or review.get("candidate_manifest_sha256")
        != policy.candidate_manifest_sha256
        or review.get("metrics") != policy.expected_audit_metrics
        or review.get("technical_blocking_findings") != []
        or not isinstance(blockers, list)
        or blockers
        != [
            {
                "check": policy.resolved_blocker,
                "details": "A technically passing candidate would still require explicit human promotion approval.",
            }
        ]
        or review.get("candidate_modified") is not False
        or review.get("source_release_modified") is not False
        or review.get("release_created") is not False
        or review.get("current_release_pointer_modified") is not False
        or review.get("rows_or_stays_filtered") is not False
        or review.get("carry_forward_or_imputation_applied") is not False
        or review.get("publication_ready") is not False
        or review.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved time-blocking audit changed")
    if (
        any(private.get(key) != value for key, value in review.items())
        or Path(str(private.get("candidate_directory", ""))).resolve()
        != candidate_dir.resolve()
        or private.get("source_dynamic_sha256")
        != source.table_hashes["dynamic"]
    ):
        raise HarmonizationError("Private time-blocking audit changed")

    release_root = config.build.data_root / policy.releases_directory
    release_dir = release_root / policy.release_id
    staging_dir = release_root / f".{policy.release_id}.incomplete"
    current_pointer = config.build.data_root / policy.current_release_pointer
    pointer_temporary = current_pointer.with_suffix(f"{current_pointer.suffix}.tmp")
    review_dir = config.build.reports_root / "review" / policy.review_directory
    review_json = review_dir / f"{policy.release_id}.json"
    review_markdown = review_dir / f"{policy.release_id}.md"
    if any(
        path.exists()
        for path in (
            release_dir,
            staging_dir,
            current_pointer,
            pointer_temporary,
            review_json,
            review_markdown,
        )
    ):
        raise HarmonizationError(
            "Time-blocking promotion target or initial pointer already exists"
        )

    candidate_hashes = {
        candidate_manifest_path: policy.candidate_manifest_sha256,
        **{
            item["source_path"]: item["sha256"]
            for item in candidate_outputs.values()
        },
    }
    release_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    release_root.chmod(0o700)
    staging_dir.mkdir(mode=0o700)
    released_files: dict[str, dict[str, Any]] = {}
    for name, item in candidate_outputs.items():
        destination = staging_dir / name
        _copy_exact(item["source_path"], destination, item["sha256"])
        released_files[name] = {
            "path": str(release_dir / name),
            "sha256": item["sha256"],
            "size_bytes": item["size_bytes"],
            "row_count": item["row_count"],
            "schema_sha256": item["schema_sha256"],
            "byte_identical_to_audited_candidate": True,
        }
    copied_candidate_manifest = staging_dir / contract.candidate_manifest
    _copy_exact(
        candidate_manifest_path,
        copied_candidate_manifest,
        policy.candidate_manifest_sha256,
    )
    released_files[contract.candidate_manifest] = {
        "path": str(release_dir / contract.candidate_manifest),
        "sha256": policy.candidate_manifest_sha256,
        "size_bytes": copied_candidate_manifest.stat().st_size,
        "row_count": None,
        "schema_sha256": None,
        "byte_identical_to_audited_candidate": True,
    }

    generated = utc_timestamp()
    release_manifest_staging = staging_dir / policy.release_manifest_name
    release_manifest = {
        "artifact": "asic_v3_time_blocking_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": "production",
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "status": "released_time_blocking_recipe",
        "resolution": contract.resolution_label,
        "time_blocking_contract_version": contract.version,
        "core_derived_release_id": contract.core_derived_release_id,
        "lineage": {
            "candidate_run_id": policy.candidate_run_id,
            "candidate_manifest_sha256": policy.candidate_manifest_sha256,
            "candidate_build_review_sha256": sha256_file(build_review),
            "candidate_build_private_manifest_sha256": sha256_file(build_private),
            "audit_run_id": policy.audit_run_id,
            "audit_review_sha256": sha256_file(audit_review),
            "audit_private_manifest_sha256": sha256_file(audit_private),
            "time_blocking_contract_sha256": policy.contract_sha256,
            "production_workflow_policy_sha256": policy.implementation_policy_sha256,
            "production_execution_authorization_sha256": policy.execution_authorization_sha256,
            "promotion_policy_sha256": sha256_file(policy.source_path),
            "core_derived_current_pointer_sha256": source.pointer_sha256,
            "core_derived_release_manifest_sha256": source.release_manifest_sha256,
            "core_derived_file_hashes": source.table_hashes,
        },
        "human_approval": {
            "role": policy.approval_role,
            "recorded_date": policy.approval_date,
            "statement": policy.approval_statement,
            "context": policy.approval_context,
            "resolved_blocker": policy.resolved_blocker,
        },
        "candidate_metrics": policy.expected_candidate_metrics,
        "audit_metrics": policy.expected_audit_metrics,
        "files": released_files,
        "source_static": {
            "representation": "core_derived_release_lineage_reference_only",
            "path": str(source.table_paths["static"]),
            "sha256": source.table_hashes["static"],
            "copied": False,
            "repeated_on_blocks": False,
        },
        "initial_time_blocking_current_pointer_was_absent": True,
        "payload_byte_identity_preserved": True,
        "candidate_artifacts_preserved": True,
        "core_derived_release_modified": False,
        "core_derived_current_pointer_modified": False,
        "static_table_copied": False,
        "static_table_repeated_on_blocks": False,
        "time_blocking_rerun_during_promotion": False,
        "derivation_rerun_during_promotion": False,
        "rows_or_stays_filtered": False,
        "analysis_cohort_created": False,
        "carry_forward_or_imputation_applied": False,
        "medication_dose_totals_emitted": False,
        "time_blocking_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    _write_json(release_manifest_staging, release_manifest, 0o600)
    staging_dir.replace(release_dir)
    release_dir.chmod(0o700)
    release_manifest_path = release_dir / policy.release_manifest_name

    if not _unchanged_source_and_candidate(source, candidate_hashes):
        raise HarmonizationError("Source or candidate changed during promotion")
    if current_pointer.exists() or pointer_temporary.exists():
        raise HarmonizationError("Time-blocking current pointer appeared during promotion")
    current_payload = {
        "artifact": "asic_v3_current_time_blocking_release",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": "production",
        "updated_at_utc": generated,
        "release_id": policy.release_id,
        "resolution": contract.resolution_label,
        "time_blocking_contract_version": contract.version,
        "core_derived_release_id": contract.core_derived_release_id,
        "release_manifest": str(release_manifest_path),
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "source_static_representation": "core_derived_release_lineage_reference_only",
        "time_blocking_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    current_pointer.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    _write_json(current_pointer, current_payload, 0o600)

    if not _unchanged_source_and_candidate(source, candidate_hashes):
        raise HarmonizationError("Source or candidate changed after promotion")
    for name, item in released_files.items():
        if sha256_file(release_dir / name) != item["sha256"]:
            raise HarmonizationError("Released payload is not byte-identical")
    observed_current = _read_json(current_pointer, "Time-blocking current pointer")
    if observed_current != current_payload:
        raise HarmonizationError("Time-blocking current pointer changed")

    review_payload = {
        "artifact": "asic_v3_time_blocking_promotion_review",
        "artifact_version": policy.release_artifact_version,
        "dataset_context": "production",
        "generated_at_utc": generated,
        "release_id": policy.release_id,
        "candidate_run_id": policy.candidate_run_id,
        "audit_run_id": policy.audit_run_id,
        "overall_status": "pass",
        "technical_blocking_findings": [],
        "blocking_findings": [],
        "metrics": policy.expected_audit_metrics,
        "candidate_manifest_sha256": policy.candidate_manifest_sha256,
        "release_manifest_sha256": sha256_file(release_manifest_path),
        "audited_candidate_bytes_preserved": True,
        "candidate_preserved": True,
        "core_derived_release_modified": False,
        "core_derived_current_pointer_modified": False,
        "static_table_copied": False,
        "static_table_repeated_on_blocks": False,
        "time_blocking_rerun_during_promotion": False,
        "rows_or_stays_filtered": False,
        "analysis_cohort_created": False,
        "carry_forward_or_imputation_applied": False,
        "medication_dose_totals_emitted": False,
        "time_blocking_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_markdown, _promotion_markdown(review_payload), 0o640)
    return TimeBlockingPromotionResult(
        release_id=policy.release_id,
        release_directory=release_dir,
        release_manifest_path=release_manifest_path,
        current_release_pointer_path=current_pointer,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
