from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import struct
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import (
    ALLOWED_DATASET_CONTEXTS,
    load_yaml_mapping,
    required_string,
    resolve_path,
    validate_context_path,
    validate_path_within,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


EXPECTED_BOUNDARY = {
    "read_released_core_derived_data": True,
    "modify_core_derived_release": False,
    "write_blocked_rows": False,
    "create_time_blocking_candidate": False,
    "create_time_blocking_release": False,
    "modify_current_release_pointer": False,
    "filter_rows_or_stays": False,
    "apply_carry_forward_or_imputation": False,
    "create_analysis_cohort": False,
    "expose_identifiers_or_row_examples": False,
    "authorize_external_data_export": False,
}

EXPECTED_TIME_POLICY = {
    "resolution_minutes": 480,
    "resolution_label": "8h",
    "admission_label": "0h",
    "admission_interval": "singleton_zero",
    "post_admission_intervals": "right_closed",
    "post_admission_examples": ["(0,8]", "(8,16]", "(16,24]"],
    "pre_admission_intervals": "left_closed_right_open",
    "pre_admission_examples": ["[-8,0)", "[-16,-8)"],
    "retain_negative_rows": True,
    "retain_admission_row_for_every_static_stay": True,
    "retain_empty_intervening_blocks": True,
    "retain_terminal_partial_block": True,
    "terminal_completeness_name": "is_full_by_recording_extent_proxy",
    "prediction_time_field": "omitted",
    "carry_forward": "prohibited",
}

ASSIGNMENT_COLUMNS = (
    "stay_id_global",
    "hospital_id",
    "minutes_since_icu_admission",
    "hours_since_icu_admission",
    "__v3_source_file_order",
    "__v3_source_row_number",
    "__v3_source_order",
)


@dataclass(frozen=True)
class TimeBlockingEvidencePolicy:
    version: str
    core_derived_release_id: str
    core_derived_contract_version: str
    cleaned_release_id: str
    harmonized_release_id: str
    current_pointer: Path
    releases_directory: Path
    expected_static_rows: int
    expected_dynamic_rows: int
    expected_static_columns: int
    expected_dynamic_columns: int
    expected_negative_time_rows: int
    lineage_contracts: dict[str, tuple[Path, str]]
    resolution_minutes: int
    registry_groups: dict[str, tuple[str, ...]]
    analysis_ineligible: tuple[str, ...]
    conditionally_ineligible: tuple[str, ...]
    observed_dose_total_candidates: tuple[str, ...]
    canonical_last_observation_outputs: tuple[str, ...]
    primary_rows_per_batch: int
    verification_rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path

    @property
    def registry_variables(self) -> tuple[str, ...]:
        return tuple(
            variable
            for variables in self.registry_groups.values()
            for variable in variables
        )

    @property
    def group_by_variable(self) -> dict[str, str]:
        return {
            variable: group
            for group, variables in self.registry_groups.items()
            for variable in variables
        }


@dataclass(frozen=True)
class TimeBlockingEvidenceConfig:
    dataset_context: str
    data_root: Path
    reports_root: Path
    policy_path: Path
    source_path: Path


@dataclass(frozen=True)
class TimeBlockingEvidenceResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class CoreDerivedReleaseInput:
    pointer_path: Path
    pointer_sha256: str
    release_directory: Path
    release_manifest_path: Path
    release_manifest_sha256: str
    table_paths: dict[str, Path]
    table_hashes: dict[str, str]
    schemas: dict[str, pa.Schema]


@dataclass
class StaticStay:
    hospital_id: str
    recording_extent_hours: float | None


@dataclass
class StayAssignmentState:
    stay_id: str
    hospital_id: str
    row_count: int = 0
    finite_time_row_count: int = 0
    minimum_time_minutes: float | None = None
    maximum_time_minutes: float | None = None
    maximum_positive_time_minutes: float | None = None
    minimum_negative_time_minutes: float | None = None
    block_row_counts: Counter[int] = field(default_factory=Counter)
    timestamp_counts: Counter[float] = field(default_factory=Counter)
    bolus_positive_observations: dict[str, list[tuple[float, int, float]]] = field(
        default_factory=dict
    )


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


def load_time_blocking_evidence_config(
    path: str | Path,
) -> TimeBlockingEvidenceConfig:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Time-blocking evidence configuration")
    context = required_string(raw, "dataset_context", "config")
    if context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError(
            f"config.dataset_context must be one of {sorted(ALLOWED_DATASET_CONTEXTS)}"
        )
    policy_path = resolve_path(
        required_string(
            raw,
            "time_blocking_contract_evidence_policy",
            "config",
        ),
        source,
    )
    paths = _mapping(raw.get("paths"), "config.paths")
    data_root = resolve_path(required_string(paths, "data", "config.paths"), source)
    reports_root = resolve_path(
        required_string(paths, "reports", "config.paths"), source
    )
    validate_context_path(data_root, context, "config.paths.data")
    validate_context_path(reports_root, context, "config.paths.reports")
    if len(source.parents) < 4 or source.parents[2].name != "asic":
        raise ConfigurationError(
            "Time-blocking configuration must remain under asic/config/datasets"
        )
    project_root = source.parents[3]
    asic_root = project_root / "asic"
    validate_path_within(data_root, asic_root, "config.paths.data")
    validate_path_within(reports_root, asic_root, "config.paths.reports")
    validate_path_within(policy_path, asic_root, "time-blocking evidence policy")
    return TimeBlockingEvidenceConfig(
        dataset_context=context,
        data_root=data_root,
        reports_root=reports_root,
        policy_path=policy_path,
        source_path=source,
    )


def load_time_blocking_evidence_policy(
    path: str | Path,
) -> TimeBlockingEvidencePolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Time-blocking contract evidence policy")
    if (
        raw.get("time_blocking_contract_evidence_policy_version") != "0.1"
        or raw.get("status") != "human_approved_for_read_only_contract_evidence"
    ):
        raise ConfigurationError("Time-blocking evidence approval is invalid")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Time-blocking evidence boundary changed")
    time_policy = _mapping(raw.get("time_policy"), "time_policy")
    if time_policy != EXPECTED_TIME_POLICY:
        raise ConfigurationError("Approved time-index evidence policy changed")

    input_raw = _mapping(raw.get("input"), "input")
    lineage_raw = _mapping(raw.get("lineage_contracts"), "lineage_contracts")
    lineage: dict[str, tuple[Path, str]] = {}
    for name, item_value in lineage_raw.items():
        item = _mapping(item_value, f"lineage_contracts.{name}")
        contract_path = resolve_path(
            required_string(item, "path", f"lineage_contracts.{name}"), source
        )
        expected_hash = required_string(
            item, "sha256", f"lineage_contracts.{name}"
        )
        if sha256_file(contract_path) != expected_hash:
            raise ConfigurationError(f"Immutable lineage contract changed: {name}")
        lineage[name] = (contract_path, expected_hash)

    registry_raw = _mapping(raw.get("registry"), "registry")
    groups_raw = _mapping(registry_raw.get("groups"), "registry.groups")
    groups: dict[str, tuple[str, ...]] = {}
    flattened: list[str] = []
    for group, values in groups_raw.items():
        if not isinstance(values, list) or any(
            not isinstance(value, str) or not value for value in values
        ):
            raise ConfigurationError(
                f"registry.groups.{group} must be a list of variable names"
            )
        groups[group] = tuple(values)
        flattened.extend(values)
    if len(flattened) != len(set(flattened)):
        raise ConfigurationError("Time-blocking registry contains duplicate variables")

    def string_tuple(key: str) -> tuple[str, ...]:
        value = registry_raw.get(key)
        if not isinstance(value, list) or any(
            not isinstance(item, str) or not item for item in value
        ):
            raise ConfigurationError(f"registry.{key} must be a list of names")
        return tuple(value)

    ineligible = string_tuple("analysis_ineligible")
    conditional = string_tuple("conditionally_ineligible")
    bolus = string_tuple("observed_dose_total_candidates")
    canonical_last = string_tuple("canonical_last_observation_outputs")
    if set(ineligible) - set(flattened):
        raise ConfigurationError("Ineligible registry variables are unknown")
    if set(conditional) - set(flattened):
        raise ConfigurationError("Conditionally ineligible variables are unknown")
    if set(bolus) - set(groups.get("medication_or_therapy", ())):
        raise ConfigurationError("Dose-total candidates must be medication variables")
    if canonical_last != (
        "last_observation_value",
        "last_observation_time_h",
        "last_observation_age_h",
    ):
        raise ConfigurationError("Canonical last-observation terminology changed")

    streaming = _mapping(raw.get("streaming"), "streaming")
    if (
        streaming.get("require_contiguous_dynamic_stays") is not True
        or streaming.get(
            "require_hospital_scoped_contiguous_source_order_from_one"
        )
        is not True
    ):
        raise ConfigurationError("Streaming determinism requirements changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    policy = TimeBlockingEvidencePolicy(
        version="0.1",
        core_derived_release_id=required_string(
            input_raw, "core_derived_release_id", "input"
        ),
        core_derived_contract_version=required_string(
            input_raw, "core_derived_contract_version", "input"
        ),
        cleaned_release_id=required_string(
            input_raw, "cleaned_release_id", "input"
        ),
        harmonized_release_id=required_string(
            input_raw, "harmonized_release_id", "input"
        ),
        current_pointer=_contained_relative_path(
            required_string(input_raw, "current_pointer", "input"),
            "input.current_pointer",
        ),
        releases_directory=_contained_relative_path(
            required_string(input_raw, "releases_directory", "input"),
            "input.releases_directory",
        ),
        expected_static_rows=_positive_int(
            input_raw.get("expected_static_rows"), "input.expected_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            input_raw.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        expected_static_columns=_positive_int(
            input_raw.get("expected_static_columns"),
            "input.expected_static_columns",
        ),
        expected_dynamic_columns=_positive_int(
            input_raw.get("expected_dynamic_columns"),
            "input.expected_dynamic_columns",
        ),
        expected_negative_time_rows=_positive_int(
            input_raw.get("expected_negative_time_rows"),
            "input.expected_negative_time_rows",
        ),
        lineage_contracts=lineage,
        resolution_minutes=_positive_int(
            time_policy.get("resolution_minutes"),
            "time_policy.resolution_minutes",
        ),
        registry_groups=groups,
        analysis_ineligible=ineligible,
        conditionally_ineligible=conditional,
        observed_dose_total_candidates=bolus,
        canonical_last_observation_outputs=canonical_last,
        primary_rows_per_batch=_positive_int(
            streaming.get("primary_rows_per_batch"),
            "streaming.primary_rows_per_batch",
        ),
        verification_rows_per_batch=_positive_int(
            streaming.get("verification_rows_per_batch"),
            "streaming.verification_rows_per_batch",
        ),
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
    if (
        len(policy.registry_variables) != policy.expected_dynamic_columns
        or len(set(policy.registry_variables)) != policy.expected_dynamic_columns
    ):
        raise ConfigurationError(
            "Registry width does not match the approved dynamic schema width"
        )
    return policy


def assign_time_block(
    minutes_since_icu_admission: float,
    resolution_minutes: int = 480,
) -> tuple[str, int]:
    """Return the reviewed time domain and block index for one finite timestamp."""

    value = float(minutes_since_icu_admission)
    if not math.isfinite(value):
        raise ValueError("Time assignment requires a finite elapsed minute value")
    if value < 0:
        return "pre_admission", math.floor(value / resolution_minutes)
    if value == 0:
        return "admission", 0
    return "post_admission", math.ceil(value / resolution_minutes)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid: {path}") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _contract_list_type(data_type: pa.DataType) -> pa.DataType:
    """Restore contract list child names after a Parquet round trip.

    Reviewed v3 schemas are constructed with ``pa.list_(value_type)``, whose
    child field is named ``item``. Parquet reconstructs the equivalent Arrow
    type with a child named ``element``. Arrow type equality ignores that
    implementation label, but serialized schema bytes do not. Core-derived
    manifests hash the pre-Parquet schema, so restore its construction form
    before recomputing that exact digest.
    """

    if pa.types.is_fixed_size_list(data_type):
        return pa.list_(
            _contract_list_type(data_type.value_type),
            data_type.list_size,
        )
    if pa.types.is_large_list(data_type):
        return pa.large_list(_contract_list_type(data_type.value_type))
    if pa.types.is_list(data_type):
        return pa.list_(_contract_list_type(data_type.value_type))
    return data_type


def _contract_schema_digest_from_parquet(schema: pa.Schema) -> str:
    normalized = pa.schema(
        [
            pa.field(
                field.name,
                _contract_list_type(field.type),
                nullable=field.nullable,
                metadata=field.metadata,
            )
            for field in schema
        ],
        metadata=schema.metadata,
    )
    return _schema_digest(normalized)


def _load_core_derived_release(
    config: TimeBlockingEvidenceConfig,
    policy: TimeBlockingEvidencePolicy,
) -> CoreDerivedReleaseInput:
    pointer_path = config.data_root / policy.current_pointer
    pointer_hash = sha256_file(pointer_path)
    release_dir = (
        config.data_root
        / policy.releases_directory
        / policy.core_derived_release_id
    )
    manifest_path = release_dir / "release_manifest.json"
    manifest_hash = sha256_file(manifest_path)
    pointer = _read_json(pointer_path, "Current core-derived release pointer")
    if (
        pointer.get("artifact") != "asic_v3_current_core_derived_release"
        or pointer.get("artifact_version") != "0.2"
        or pointer.get("dataset_context") != config.dataset_context
        or pointer.get("release_id") != policy.core_derived_release_id
        or pointer.get("core_derived_contract_version")
        != policy.core_derived_contract_version
        or pointer.get("cleaned_release_id") != policy.cleaned_release_id
        or Path(str(pointer.get("release_manifest", ""))).resolve()
        != manifest_path.resolve()
        or pointer.get("release_manifest_sha256") != manifest_hash
        or pointer.get("core_derived_layer_ready") is not True
        or pointer.get("analysis_input_approved") is not True
        or pointer.get("publication_ready") is not True
        or pointer.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Current core-derived 0.2 pointer changed")
    manifest = _read_json(manifest_path, "Core-derived release manifest")
    if (
        manifest.get("artifact") != "asic_v3_core_derived_release"
        or manifest.get("artifact_version") != "0.2"
        or manifest.get("dataset_context") != config.dataset_context
        or manifest.get("release_id") != policy.core_derived_release_id
        or manifest.get("status") != "released_core_derived_layer"
        or manifest.get("cleaned_release_id") != policy.cleaned_release_id
        or manifest.get("core_derived_contract_version")
        != policy.core_derived_contract_version
        or manifest.get("payload_byte_identity_preserved") is not True
        or manifest.get("rows_or_stays_filtered") is not False
        or manifest.get("cohort_generated") is not False
        or manifest.get("time_blocking_applied") is not False
        or manifest.get("analysis_input_approved") is not True
        or manifest.get("publication_ready") is not True
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved core-derived 0.2 release changed")
    summaries = manifest.get("files")
    if not isinstance(summaries, dict):
        raise HarmonizationError("Core-derived release file manifest is missing")
    expected_rows = {
        "static": policy.expected_static_rows,
        "dynamic": policy.expected_dynamic_rows,
    }
    expected_columns = {
        "static": policy.expected_static_columns,
        "dynamic": policy.expected_dynamic_columns,
    }
    paths: dict[str, Path] = {}
    hashes: dict[str, str] = {}
    schemas: dict[str, pa.Schema] = {}
    for table in ("static", "dynamic"):
        path = release_dir / f"{table}.parquet"
        summary = summaries.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError(f"Released {table} table is unavailable")
        parquet = pq.ParquetFile(path)
        observed_hash = sha256_file(path)
        schema = parquet.schema_arrow
        checks = {
            "hash": summary.get("sha256") == observed_hash,
            "manifest_rows": summary.get("row_count") == expected_rows[table],
            "parquet_rows": parquet.metadata.num_rows == expected_rows[table],
            "column_count": len(schema) == expected_columns[table],
            "schema_hash": summary.get("schema_sha256")
            == _contract_schema_digest_from_parquet(schema),
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise HarmonizationError(
                f"Core-derived {table} validation failed: {', '.join(failed)}"
            )
        paths[table] = path
        hashes[table] = observed_hash
        schemas[table] = schema
    if set(schemas["dynamic"].names) != set(policy.registry_variables):
        missing = sorted(set(policy.registry_variables) - set(schemas["dynamic"].names))
        unexpected = sorted(set(schemas["dynamic"].names) - set(policy.registry_variables))
        raise HarmonizationError(
            "Dynamic schema and reviewed evidence registry disagree; "
            f"missing={missing}, unexpected={unexpected}"
        )
    return CoreDerivedReleaseInput(
        pointer_path=pointer_path,
        pointer_sha256=pointer_hash,
        release_directory=release_dir,
        release_manifest_path=manifest_path,
        release_manifest_sha256=manifest_hash,
        table_paths=paths,
        table_hashes=hashes,
        schemas=schemas,
    )


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _load_static_stays(path: Path) -> tuple[dict[str, StaticStay], dict[str, int]]:
    required = ("stay_id_global", "hospital_id", "icu_recording_extent_hours")
    parquet = pq.ParquetFile(path)
    if not set(required).issubset(parquet.schema_arrow.names):
        raise HarmonizationError("Static release lacks time-blocking evidence fields")
    stays: dict[str, StaticStay] = {}
    metrics = Counter()
    for batch in parquet.iter_batches(batch_size=50_000, columns=list(required)):
        stay_values = batch.column(0).to_pylist()
        hospital_values = batch.column(1).to_pylist()
        extent_values = batch.column(2).to_pylist()
        for stay, hospital, extent in zip(
            stay_values, hospital_values, extent_values, strict=True
        ):
            metrics["row_count"] += 1
            if stay is None or hospital is None:
                metrics["missing_key_count"] += 1
                continue
            key = str(stay)
            if key in stays:
                metrics["duplicate_stay_count"] += 1
                continue
            stays[key] = StaticStay(
                hospital_id=str(hospital),
                recording_extent_hours=_finite_or_none(extent),
            )
    return stays, dict(metrics)


def _quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {name: None for name in ("minimum", "p25", "median", "p75", "p95", "maximum")}
    ordered = sorted(values)

    def value_at(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return float(ordered[lower])
        weight = position - lower
        return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)

    return {
        "minimum": float(ordered[0]),
        "p25": value_at(0.25),
        "median": value_at(0.5),
        "p75": value_at(0.75),
        "p95": value_at(0.95),
        "maximum": float(ordered[-1]),
    }


def _new_assignment_metrics(static_stay_count: int) -> Counter[str]:
    metrics: Counter[str] = Counter()
    for name in (
        "dynamic_row_count",
        "dynamic_stay_count",
        "missing_key_row_count",
        "invalid_or_missing_time_row_count",
        "hours_minutes_mismatch_row_count",
        "pre_admission_source_row_count",
        "admission_source_row_count",
        "post_admission_source_row_count",
        "positive_exact_boundary_source_row_count",
        "positive_near_boundary_source_row_count",
        "negative_exact_boundary_source_row_count",
        "pre_admission_grid_block_count",
        "pre_admission_observed_block_count",
        "pre_admission_empty_block_count",
        "admission_observed_stay_count",
        "post_admission_grid_block_count",
        "post_admission_observed_block_count",
        "post_admission_empty_block_count",
        "post_admission_proxy_full_block_count",
        "terminal_partial_block_count",
        "terminal_partial_source_row_count",
        "completed_only_excluded_source_row_count",
        "stay_count_with_terminal_partial_block",
        "stay_count_ending_exactly_on_boundary",
        "stay_count_with_negative_time",
        "observed_block_count",
        "maximum_rows_in_observed_block",
        "maximum_rows_in_stay",
        "tied_timestamp_group_count",
        "rows_on_tied_timestamps",
        "stay_count_with_tied_timestamps",
        "maximum_rows_at_same_timestamp",
        "dynamic_stay_missing_from_static_count",
        "static_stay_missing_from_dynamic_count",
        "static_dynamic_hospital_conflict_count",
        "within_stay_hospital_conflict_row_count",
        "recording_extent_mismatch_count",
        "noncontiguous_stay_reappearance_count",
        "missing_tie_break_provenance_row_count",
        "source_order_hospital_start_failure_count",
        "nonincreasing_source_order_within_hospital_row_count",
        "noncontiguous_source_order_within_hospital_row_count",
    ):
        metrics[name] = 0
    metrics["admission_grid_block_count"] = static_stay_count
    return metrics


def _finalize_assignment_stay(
    state: StayAssignmentState,
    static_stays: dict[str, StaticStay],
    metrics: Counter[str],
    extent_values: list[float],
    bolus_metrics: dict[str, Counter[str]],
    resolution: int,
) -> None:
    metrics["dynamic_stay_count"] += 1
    metrics["maximum_rows_in_stay"] = max(
        metrics["maximum_rows_in_stay"], state.row_count
    )
    observed_blocks = set(state.block_row_counts)
    metrics["observed_block_count"] += len(observed_blocks)
    if state.block_row_counts:
        metrics["maximum_rows_in_observed_block"] = max(
            metrics["maximum_rows_in_observed_block"],
            max(state.block_row_counts.values()),
        )

    negative_blocks = {index for index in observed_blocks if index < 0}
    if state.minimum_negative_time_minutes is not None:
        first_index = math.floor(state.minimum_negative_time_minutes / resolution)
        grid_count = -first_index
        metrics["pre_admission_grid_block_count"] += grid_count
        metrics["pre_admission_observed_block_count"] += len(negative_blocks)
        metrics["pre_admission_empty_block_count"] += grid_count - len(negative_blocks)
        metrics["stay_count_with_negative_time"] += 1

    admission_rows = state.block_row_counts.get(0, 0)
    if admission_rows:
        metrics["admission_observed_stay_count"] += 1

    positive_blocks = {index for index in observed_blocks if index > 0}
    if state.maximum_positive_time_minutes is not None:
        maximum = state.maximum_positive_time_minutes
        terminal_index = math.ceil(maximum / resolution)
        full_count = math.floor(maximum / resolution)
        exact_boundary = (maximum / resolution).is_integer()
        partial_count = 0 if exact_boundary else 1
        metrics["post_admission_grid_block_count"] += terminal_index
        metrics["post_admission_observed_block_count"] += len(positive_blocks)
        metrics["post_admission_empty_block_count"] += terminal_index - len(positive_blocks)
        metrics["post_admission_proxy_full_block_count"] += full_count
        metrics["terminal_partial_block_count"] += partial_count
        if partial_count:
            terminal_rows = state.block_row_counts.get(terminal_index, 0)
            metrics["terminal_partial_source_row_count"] += terminal_rows
            metrics["completed_only_excluded_source_row_count"] += terminal_rows
            metrics["stay_count_with_terminal_partial_block"] += 1
        else:
            metrics["stay_count_ending_exactly_on_boundary"] += 1

    duplicate_groups = [count for count in state.timestamp_counts.values() if count > 1]
    metrics["tied_timestamp_group_count"] += len(duplicate_groups)
    metrics["rows_on_tied_timestamps"] += sum(duplicate_groups)
    if duplicate_groups:
        metrics["stay_count_with_tied_timestamps"] += 1
        metrics["maximum_rows_at_same_timestamp"] = max(
            metrics["maximum_rows_at_same_timestamp"], max(duplicate_groups)
        )

    static = static_stays.get(state.stay_id)
    if static is None:
        metrics["dynamic_stay_missing_from_static_count"] += 1
    else:
        if static.hospital_id != state.hospital_id:
            metrics["static_dynamic_hospital_conflict_count"] += 1
        if state.maximum_time_minutes is not None:
            dynamic_extent = state.maximum_time_minutes / 60.0
            extent_values.append(dynamic_extent)
            if static.recording_extent_hours is None or not math.isclose(
                static.recording_extent_hours,
                dynamic_extent,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                metrics["recording_extent_mismatch_count"] += 1
        elif static.recording_extent_hours is not None:
            metrics["recording_extent_mismatch_count"] += 1

    for variable, observations in state.bolus_positive_observations.items():
        if not observations:
            continue
        ordered = sorted(observations, key=lambda item: (item[0], item[1]))
        evidence = bolus_metrics[variable]
        evidence["positive_observation_count"] += len(ordered)
        time_counts = Counter(item[0] for item in ordered)
        tied = [count for count in time_counts.values() if count > 1]
        evidence["tied_positive_timestamp_group_count"] += len(tied)
        evidence["positive_observations_on_tied_timestamps"] += sum(tied)
        for previous, current in zip(ordered, ordered[1:]):
            if previous[2] == current[2]:
                evidence["consecutive_equal_positive_value_count"] += 1


def _scan_assignment(
    path: Path,
    static_stays: dict[str, StaticStay],
    policy: TimeBlockingEvidencePolicy,
    rows_per_batch: int,
    progress: Callable[[str], None] | None,
) -> tuple[dict[str, Any], str, dict[str, dict[str, int]]]:
    columns = [*ASSIGNMENT_COLUMNS, *policy.observed_dose_total_candidates]
    parquet = pq.ParquetFile(path)
    missing = sorted(set(columns) - set(parquet.schema_arrow.names))
    if missing:
        raise HarmonizationError(f"Assignment evidence fields are missing: {missing}")
    metrics = _new_assignment_metrics(len(static_stays))
    closed_stays: set[str] = set()
    dynamic_stays: set[str] = set()
    extent_values: list[float] = []
    bolus_metrics: dict[str, Counter[str]] = {
        variable: Counter() for variable in policy.observed_dose_total_candidates
    }
    current: StayAssignmentState | None = None
    previous_source_order_by_hospital: dict[str, int] = {}
    digest = hashlib.sha256()
    row_offset = 0

    for batch_index, batch in enumerate(
        parquet.iter_batches(batch_size=rows_per_batch, columns=columns)
    ):
        values = {name: batch.column(index).to_pylist() for index, name in enumerate(columns)}
        for row_index in range(batch.num_rows):
            metrics["dynamic_row_count"] += 1
            stay_value = values["stay_id_global"][row_index]
            hospital_value = values["hospital_id"][row_index]
            minutes_value = values["minutes_since_icu_admission"][row_index]
            hours_value = values["hours_since_icu_admission"][row_index]
            source_order_value = values["__v3_source_order"][row_index]
            file_order_value = values["__v3_source_file_order"][row_index]
            source_row_value = values["__v3_source_row_number"][row_index]
            if stay_value is None or hospital_value is None:
                metrics["missing_key_row_count"] += 1
                continue
            stay = str(stay_value)
            hospital = str(hospital_value)
            if current is None or current.stay_id != stay:
                if current is not None:
                    _finalize_assignment_stay(
                        current,
                        static_stays,
                        metrics,
                        extent_values,
                        bolus_metrics,
                        policy.resolution_minutes,
                    )
                    closed_stays.add(current.stay_id)
                if stay in closed_stays:
                    metrics["noncontiguous_stay_reappearance_count"] += 1
                current = StayAssignmentState(
                    stay_id=stay,
                    hospital_id=hospital,
                    bolus_positive_observations={
                        variable: []
                        for variable in policy.observed_dose_total_candidates
                    },
                )
                dynamic_stays.add(stay)
            elif current.hospital_id != hospital:
                metrics["within_stay_hospital_conflict_row_count"] += 1
            current.row_count += 1

            if (
                source_order_value is None
                or file_order_value is None
                or source_row_value is None
            ):
                metrics["missing_tie_break_provenance_row_count"] += 1
                source_order = row_offset
            else:
                source_order = int(source_order_value)
                previous_source_order = previous_source_order_by_hospital.get(
                    hospital
                )
                if previous_source_order is None:
                    if source_order != 1:
                        metrics[
                            "source_order_hospital_start_failure_count"
                        ] += 1
                else:
                    if source_order <= previous_source_order:
                        metrics[
                            "nonincreasing_source_order_within_hospital_row_count"
                        ] += 1
                    if source_order != previous_source_order + 1:
                        metrics[
                            "noncontiguous_source_order_within_hospital_row_count"
                        ] += 1
                previous_source_order_by_hospital[hospital] = source_order

            minutes = _finite_or_none(minutes_value)
            hours = _finite_or_none(hours_value)
            if minutes is None or hours is None:
                metrics["invalid_or_missing_time_row_count"] += 1
                row_offset += 1
                continue
            if not math.isclose(hours, minutes / 60.0, rel_tol=0.0, abs_tol=1e-12):
                metrics["hours_minutes_mismatch_row_count"] += 1
            current.finite_time_row_count += 1
            current.minimum_time_minutes = (
                minutes
                if current.minimum_time_minutes is None
                else min(current.minimum_time_minutes, minutes)
            )
            current.maximum_time_minutes = (
                minutes
                if current.maximum_time_minutes is None
                else max(current.maximum_time_minutes, minutes)
            )
            current.timestamp_counts[minutes] += 1
            domain, block_index = assign_time_block(
                minutes, policy.resolution_minutes
            )
            current.block_row_counts[block_index] += 1
            metrics[f"{domain}_source_row_count"] += 1
            quotient = minutes / policy.resolution_minutes
            if minutes > 0:
                current.maximum_positive_time_minutes = (
                    minutes
                    if current.maximum_positive_time_minutes is None
                    else max(current.maximum_positive_time_minutes, minutes)
                )
                if quotient.is_integer():
                    metrics["positive_exact_boundary_source_row_count"] += 1
                elif abs(quotient - round(quotient)) <= 1e-9:
                    metrics["positive_near_boundary_source_row_count"] += 1
            elif minutes < 0:
                current.minimum_negative_time_minutes = (
                    minutes
                    if current.minimum_negative_time_minutes is None
                    else min(current.minimum_negative_time_minutes, minutes)
                )
                if quotient.is_integer():
                    metrics["negative_exact_boundary_source_row_count"] += 1

            digest.update(struct.pack(">qq", source_order, block_index))
            for variable in policy.observed_dose_total_candidates:
                raw_value = values[variable][row_index]
                numeric = _finite_or_none(raw_value)
                if numeric is not None and numeric > 0:
                    current.bolus_positive_observations[variable].append(
                        (minutes, source_order, numeric)
                    )
            row_offset += 1
        if progress and (batch_index + 1) % 25 == 0:
            progress(
                "time_blocking_assignment_rows_scanned="
                f"{metrics['dynamic_row_count']} batch_size={rows_per_batch}"
            )
    if current is not None:
        _finalize_assignment_stay(
            current,
            static_stays,
            metrics,
            extent_values,
            bolus_metrics,
            policy.resolution_minutes,
        )
        closed_stays.add(current.stay_id)

    metrics["admission_empty_stay_count"] = (
        len(static_stays) - metrics["admission_observed_stay_count"]
    )
    metrics["static_stay_missing_from_dynamic_count"] = len(
        set(static_stays) - dynamic_stays
    )
    metrics["total_grid_block_count"] = (
        metrics["pre_admission_grid_block_count"]
        + metrics["admission_grid_block_count"]
        + metrics["post_admission_grid_block_count"]
    )
    metrics["total_empty_grid_block_count"] = (
        metrics["pre_admission_empty_block_count"]
        + metrics["admission_empty_stay_count"]
        + metrics["post_admission_empty_block_count"]
    )
    metrics["all_finite_rows_assigned_count"] = (
        metrics["pre_admission_source_row_count"]
        + metrics["admission_source_row_count"]
        + metrics["post_admission_source_row_count"]
    )
    metrics["source_order_hospital_count"] = len(
        previous_source_order_by_hospital
    )
    metrics["source_order_terminal_value_sum"] = sum(
        previous_source_order_by_hospital.values()
    )
    payload = {
        "row_and_stay_accounting": dict(sorted(metrics.items())),
        "recording_extent_hours_distribution": _quantiles(extent_values),
    }
    bolus_payload = {
        variable: dict(sorted(values.items()))
        for variable, values in bolus_metrics.items()
    }
    return payload, digest.hexdigest(), bolus_payload


def _count_true(array: pa.Array) -> int:
    result = pc.sum(pc.cast(array, pa.int64()))
    value = result.as_py()
    return int(value) if value is not None else 0


def _masked_valid_count(array: pa.Array, mask: pa.Array) -> int:
    return _count_true(pc.and_kleene(pc.is_valid(array), mask))


def _safe_scalar(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _scan_variable_profiles(
    path: Path,
    schema: pa.Schema,
    policy: TimeBlockingEvidencePolicy,
    progress: Callable[[str], None] | None,
) -> dict[str, dict[str, Any]]:
    group_by_variable = policy.group_by_variable
    profiles: dict[str, dict[str, Any]] = {}
    categorical_distinct_values: dict[str, set[Any]] = {
        "ards_diagnosis_app": set(),
        "position_therapy": set(),
        "severity_read_confirmation": set(),
    }
    for field_value in schema:
        metadata = field_value.metadata or {}
        eligibility = "eligible"
        if field_value.name in policy.analysis_ineligible:
            eligibility = "analysis_ineligible"
        elif field_value.name in policy.conditionally_ineligible:
            eligibility = "conditionally_ineligible"
        profiles[field_value.name] = {
            "registry_group": group_by_variable[field_value.name],
            "physical_type": str(field_value.type),
            "unit": metadata.get(b"asic_v3_unit", b"unavailable").decode(
                "utf-8", errors="replace"
            ),
            "eligibility": eligibility,
            "non_missing_count": 0,
            "pre_admission_non_missing_count": 0,
            "admission_non_missing_count": 0,
            "post_admission_non_missing_count": 0,
        }
    parquet = pq.ParquetFile(path)
    for batch_index, batch in enumerate(
        parquet.iter_batches(batch_size=policy.primary_rows_per_batch)
    ):
        minutes = batch.column(schema.get_field_index("minutes_since_icu_admission"))
        finite_time = pc.and_kleene(pc.is_valid(minutes), pc.is_finite(minutes))
        negative = pc.and_kleene(finite_time, pc.less(minutes, 0.0))
        admission = pc.and_kleene(finite_time, pc.equal(minutes, 0.0))
        positive = pc.and_kleene(finite_time, pc.greater(minutes, 0.0))
        for index, field_value in enumerate(schema):
            array = batch.column(index)
            profile = profiles[field_value.name]
            profile["non_missing_count"] += len(array) - array.null_count
            profile["pre_admission_non_missing_count"] += _masked_valid_count(
                array, negative
            )
            profile["admission_non_missing_count"] += _masked_valid_count(
                array, admission
            )
            profile["post_admission_non_missing_count"] += _masked_valid_count(
                array, positive
            )
            if pa.types.is_boolean(field_value.type):
                valid = pc.is_valid(array)
                profile["true_count"] = profile.get("true_count", 0) + _count_true(
                    pc.and_kleene(valid, array)
                )
                profile["false_count"] = profile.get("false_count", 0) + _count_true(
                    pc.and_kleene(valid, pc.invert(array))
                )
            elif pa.types.is_integer(field_value.type) or pa.types.is_floating(
                field_value.type
            ):
                valid = pc.is_valid(array)
                profile["zero_count"] = profile.get("zero_count", 0) + _count_true(
                    pc.and_kleene(valid, pc.equal(array, 0))
                )
                profile["positive_count"] = profile.get(
                    "positive_count", 0
                ) + _count_true(pc.and_kleene(valid, pc.greater(array, 0)))
                profile["negative_count"] = profile.get(
                    "negative_count", 0
                ) + _count_true(pc.and_kleene(valid, pc.less(array, 0)))
                batch_min = pc.min(array).as_py()
                batch_max = pc.max(array).as_py()
                if batch_min is not None:
                    profile["minimum"] = _safe_scalar(
                        batch_min
                        if "minimum" not in profile
                        else min(profile["minimum"], batch_min)
                    )
                if batch_max is not None:
                    profile["maximum"] = _safe_scalar(
                        batch_max
                        if "maximum" not in profile
                        else max(profile["maximum"], batch_max)
                    )
                if field_value.name in policy.observed_dose_total_candidates:
                    value = pc.sum(array).as_py()
                    if value is not None:
                        profile["observed_value_sum_for_semantics_review"] = float(
                            profile.get(
                                "observed_value_sum_for_semantics_review", 0.0
                            )
                            + float(value)
                        )
                if field_value.name in categorical_distinct_values:
                    categorical_distinct_values[field_value.name].update(
                        value
                        for value in pc.unique(array).to_pylist()
                        if value is not None
                    )
            elif pa.types.is_string(field_value.type) or pa.types.is_large_string(
                field_value.type
            ):
                if field_value.name in categorical_distinct_values:
                    categorical_distinct_values[field_value.name].update(
                        value
                        for value in pc.unique(array).to_pylist()
                        if value is not None
                    )
            elif pa.types.is_fixed_size_list(field_value.type):
                states = profile.setdefault(
                    "pair_state_counts",
                    {
                        "false_false": 0,
                        "false_true": 0,
                        "false_null": 0,
                        "true_false": 0,
                        "true_true": 0,
                        "true_null": 0,
                        "null_false": 0,
                        "null_true": 0,
                        "null_null": 0,
                    },
                )
                labels = {False: "false", True: "true", None: "null"}
                for pair in array.to_pylist():
                    if pair is None:
                        states["null_null"] += 1
                    else:
                        states[f"{labels[pair[0]]}_{labels[pair[1]]}"] += 1
        if progress and (batch_index + 1) % 25 == 0:
            progress(
                "time_blocking_variable_profile_rows_scanned="
                f"{min((batch_index + 1) * policy.primary_rows_per_batch, parquet.metadata.num_rows)}"
            )
    for variable, values in categorical_distinct_values.items():
        profiles[variable]["distinct_non_missing_count"] = len(values)
    return profiles


def _technical_finding(check: str, details: str) -> dict[str, Any]:
    return {"check": check, "details": details}


def _write_json(path: Path, value: Any, mode: int) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _write_text(path: Path, value: str, mode: int) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
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
    assignment = payload["assignment_evidence"]["row_and_stay_accounting"]
    lines = [
        "# ASIC v3 8-hour time-blocking contract evidence",
        "",
        f"- Run ID: `{payload['run_id']}`",
        f"- Overall status: `{payload['overall_status']}`",
        f"- Core-derived release: `{payload['core_derived_release_id']}`",
        f"- Dynamic rows scanned: `{assignment['dynamic_row_count']}`",
        f"- Negative-time rows assigned: `{assignment['pre_admission_source_row_count']}`",
        f"- Exact-admission rows assigned: `{assignment['admission_source_row_count']}`",
        f"- Positive-time rows assigned: `{assignment['post_admission_source_row_count']}`",
        f"- Exact positive 8-hour boundary rows: `{assignment['positive_exact_boundary_source_row_count']}`",
        f"- Total proposed grid blocks: `{assignment['total_grid_block_count']}`",
        f"- Empty proposed grid blocks: `{assignment['total_empty_grid_block_count']}`",
        f"- Terminal partial blocks: `{assignment['terminal_partial_block_count']}`",
        f"- Rows retained only by terminal-partial policy: `{assignment['terminal_partial_source_row_count']}`",
        f"- Cross-batch assignment reproducible: `{str(payload['cross_batch_assignment_reproducible']).lower()}`",
        "",
        "## Contract interpretation",
        "",
        "- Pre-admission windows use `[-8,0)`, `[-16,-8)`, and so on.",
        "- The `0h` row is the singleton `{0}`.",
        "- Post-admission windows are right-labelled and right-closed: `(0,8]`, `(8,16]`, and so on.",
        "- Empty intervening blocks and terminal partial blocks are retained.",
        "- Completeness is recording-extent-proxy completeness, not confirmed ICU discharge or continuous coverage.",
        "- No carry-forward, imputation, cohort filtering, blocked data, candidate, release, or pointer was created.",
        "",
        "## Findings",
        "",
    ]
    findings = [*payload["technical_blocking_findings"], *payload["blocking_findings"]]
    if findings:
        lines.extend(f"- `{item['check']}`: {item['details']}" for item in findings)
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Observed-dose-total candidate evidence",
            "",
            "These aggregates do not establish that repeated rows are distinct administrations; that remains a human contract decision.",
            "",
            "| Variable | Positive observations | Tied-time positive observations | Consecutive equal positive values |",
            "|---|---:|---:|---:|",
        ]
    )
    for variable, evidence in payload["observed_dose_total_candidate_evidence"].items():
        lines.append(
            f"| `{variable}` | {evidence.get('positive_observation_count', 0)} | "
            f"{evidence.get('positive_observations_on_tied_timestamps', 0)} | "
            f"{evidence.get('consecutive_equal_positive_value_count', 0)} |"
        )
    lines.extend(
        [
            "",
            "## Complete variable profile",
            "",
            "| Variable | Registry group | Eligibility | Non-missing | Pre-admission | Admission | Post-admission | Zero | Positive |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for variable, profile in payload["variable_profiles"].items():
        lines.append(
            f"| `{variable}` | `{profile['registry_group']}` | `{profile['eligibility']}` | "
            f"{profile['non_missing_count']} | {profile['pre_admission_non_missing_count']} | "
            f"{profile['admission_non_missing_count']} | {profile['post_admission_non_missing_count']} | "
            f"{profile.get('zero_count', '')} | {profile.get('positive_count', '')} |"
        )
    lines.extend(
        [
            "",
            "## Gate",
            "",
            "Review this evidence and explicitly freeze or revise the 8-hour contract before any blocking engine or candidate is implemented or run.",
            "",
        ]
    )
    return "\n".join(lines)


def run_time_blocking_contract_evidence(
    config: TimeBlockingEvidenceConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> TimeBlockingEvidenceResult:
    policy = load_time_blocking_evidence_policy(config.policy_path)
    effective_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(effective_run_id):
        raise HarmonizationError("Invalid immutable time-blocking evidence run ID")
    private_dir = (
        config.reports_root
        / "private"
        / policy.private_directory_name
        / effective_run_id
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{effective_run_id}.json"
    review_markdown = review_dir / f"{effective_run_id}.md"
    private_json = private_dir / "aggregate_evidence.json"
    private_manifest = private_dir / "private_manifest.json"
    if any(
        path.exists()
        for path in (private_dir, review_json, review_markdown)
    ):
        raise HarmonizationError("Time-blocking evidence output already exists")

    if progress:
        progress("validating_core_derived_release=true")
    source = _load_core_derived_release(config, policy)
    static_stays, static_metrics = _load_static_stays(source.table_paths["static"])
    if progress:
        progress("scanning_primary_time_assignment=true")
    assignment, primary_digest, bolus_evidence = _scan_assignment(
        source.table_paths["dynamic"],
        static_stays,
        policy,
        policy.primary_rows_per_batch,
        progress,
    )
    if progress:
        progress("scanning_verification_time_assignment=true")
    verification, verification_digest, verification_bolus = _scan_assignment(
        source.table_paths["dynamic"],
        static_stays,
        policy,
        policy.verification_rows_per_batch,
        progress,
    )
    reproducible = (
        assignment == verification
        and primary_digest == verification_digest
        and bolus_evidence == verification_bolus
    )
    if progress:
        progress("scanning_complete_variable_profiles=true")
    profiles = _scan_variable_profiles(
        source.table_paths["dynamic"],
        source.schemas["dynamic"],
        policy,
        progress,
    )

    after_hashes = {
        table: sha256_file(path) for table, path in source.table_paths.items()
    }
    pointer_after_hash = sha256_file(source.pointer_path)
    manifest_after_hash = sha256_file(source.release_manifest_path)
    accounting = assignment["row_and_stay_accounting"]
    technical: list[dict[str, Any]] = []
    checks = {
        "static_row_count_matches": static_metrics.get("row_count", 0)
        == policy.expected_static_rows,
        "static_keys_are_valid": static_metrics.get("missing_key_count", 0) == 0
        and static_metrics.get("duplicate_stay_count", 0) == 0,
        "dynamic_row_count_matches": accounting["dynamic_row_count"]
        == policy.expected_dynamic_rows,
        "negative_time_count_matches": accounting["pre_admission_source_row_count"]
        == policy.expected_negative_time_rows,
        "all_finite_rows_assigned": accounting["all_finite_rows_assigned_count"]
        + accounting["invalid_or_missing_time_row_count"]
        == accounting["dynamic_row_count"],
        "time_fields_are_complete_and_consistent": accounting[
            "invalid_or_missing_time_row_count"
        ]
        == 0
        and accounting["hours_minutes_mismatch_row_count"] == 0,
        "stay_set_conserved": accounting["static_stay_missing_from_dynamic_count"]
        == 0
        and accounting["dynamic_stay_missing_from_static_count"] == 0,
        "stay_rows_are_contiguous": accounting[
            "noncontiguous_stay_reappearance_count"
        ]
        == 0,
        "hospital_identity_is_consistent": accounting[
            "within_stay_hospital_conflict_row_count"
        ]
        == 0
        and accounting["static_dynamic_hospital_conflict_count"] == 0,
        "source_order_is_deterministic": accounting[
            "missing_tie_break_provenance_row_count"
        ]
        == 0
        and accounting["source_order_hospital_start_failure_count"] == 0
        and accounting[
            "nonincreasing_source_order_within_hospital_row_count"
        ]
        == 0
        and accounting[
            "noncontiguous_source_order_within_hospital_row_count"
        ]
        == 0
        and accounting["source_order_terminal_value_sum"]
        == accounting["dynamic_row_count"],
        "recording_extent_reproduced": accounting[
            "recording_extent_mismatch_count"
        ]
        == 0,
        "registry_covers_schema": set(profiles) == set(policy.registry_variables)
        and len(profiles) == policy.expected_dynamic_columns,
        "cross_batch_assignment_reproducible": reproducible,
        "core_derived_bytes_unchanged": after_hashes == source.table_hashes
        and pointer_after_hash == source.pointer_sha256
        and manifest_after_hash == source.release_manifest_sha256,
    }
    for name, passed in checks.items():
        if not passed:
            technical.append(
                _technical_finding(name, "Observed evidence failed the approved invariant")
            )
    blocking = (
        {
            "check": "time_blocking_8h_contract_freeze_approved",
            "details": (
                "The consolidated evidence requires explicit human review before "
                "the 8-hour contract can be frozen or any blocking implementation can proceed."
            ),
        },
    )
    overall = "fail" if technical else "pending_human_review"
    generated = utc_timestamp()
    lineage_hashes = {
        name: expected_hash
        for name, (_, expected_hash) in policy.lineage_contracts.items()
    }
    review_payload: dict[str, Any] = {
        "artifact": "asic_v3_time_blocking_8h_contract_evidence_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": effective_run_id,
        "overall_status": overall,
        "core_derived_release_id": policy.core_derived_release_id,
        "core_derived_contract_version": policy.core_derived_contract_version,
        "cleaned_release_id": policy.cleaned_release_id,
        "harmonized_release_id": policy.harmonized_release_id,
        "input_pointer_sha256": source.pointer_sha256,
        "input_release_manifest_sha256": source.release_manifest_sha256,
        "input_file_hashes": dict(sorted(source.table_hashes.items())),
        "lineage_contract_hashes": dict(sorted(lineage_hashes.items())),
        "static_accounting": static_metrics,
        "assignment_evidence": assignment,
        "cross_batch_assignment_reproducible": reproducible,
        "variable_profiles": {
            name: profiles[name] for name in policy.registry_variables
        },
        "observed_dose_total_candidate_evidence": bolus_evidence,
        "technical_checks": checks,
        "technical_blocking_findings": technical,
        "blocking_findings": list(blocking),
        "canonical_last_observation_outputs": list(
            policy.canonical_last_observation_outputs
        ),
        "clinical_data_written": False,
        "blocked_rows_written": False,
        "time_blocking_candidate_created": False,
        "time_blocking_release_created": False,
        "current_release_pointer_modified": False,
        "core_derived_release_modified": False,
        "rows_or_stays_filtered": False,
        "carry_forward_or_imputation_applied": False,
        "analysis_cohort_created": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_payload = {
        "artifact": "asic_v3_time_blocking_8h_contract_evidence_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": effective_run_id,
        "core_derived_release_id": policy.core_derived_release_id,
        "assignment_evidence": assignment,
        "verification_assignment_evidence": verification,
        "cross_batch_assignment_reproducible": reproducible,
        "observed_dose_total_candidate_evidence": bolus_evidence,
        "contains_identifiers": False,
        "contains_patient_rows": False,
        "contains_raw_tokens": False,
        "contains_source_filenames": False,
        "clinical_data_written": False,
    }

    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(private_json, private_payload, 0o600)
    private_manifest_payload = {
        "artifact": "asic_v3_time_blocking_8h_contract_evidence_private_manifest",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": effective_run_id,
        "aggregate_evidence_sha256": sha256_file(private_json),
        "contains_identifiers": False,
        "contains_patient_rows": False,
        "contains_raw_tokens": False,
        "contains_source_filenames": False,
    }
    _write_json(private_manifest, private_manifest_payload, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_markdown, _markdown(review_payload), 0o640)
    return TimeBlockingEvidenceResult(
        run_id=effective_run_id,
        overall_status=overall,
        blocking_findings=blocking,
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
