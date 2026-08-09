from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
from typing import Any, Callable, Iterable

import pyarrow as pa
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
from asic_pipeline.time_blocking.contract import (
    OutputSpec,
    TimeBlockingContract,
    build_output_specs,
    load_and_validate_frozen_evidence,
    load_time_blocking_contract,
)
from asic_pipeline.time_blocking.evidence import (
    CoreDerivedReleaseInput,
    TimeBlockingEvidenceConfig,
    _load_core_derived_release,
    assign_time_block,
)


EXPECTED_LOCAL_IMPLEMENTATION_BOUNDARY = {
    "create_run_scoped_demo_candidate": True,
    "overwrite_candidate": False,
    "create_or_modify_release": False,
    "create_or_modify_current_release_pointer": False,
    "modify_core_derived_release": False,
    "filter_rows_or_stays": False,
    "create_analysis_cohort": False,
    "apply_carry_forward_or_imputation": False,
    "authorize_external_data_export": False,
}

EXPECTED_PRODUCTION_WORKFLOW_IMPLEMENTATION_BOUNDARY = {
    "implement_run_scoped_production_candidate": True,
    "implement_independent_production_audit": True,
    "execute_production_candidate_or_audit": False,
    "overwrite_candidate": False,
    "create_or_modify_release": False,
    "create_or_modify_current_release_pointer": False,
    "modify_core_derived_release": False,
    "filter_rows_or_stays": False,
    "create_analysis_cohort": False,
    "apply_carry_forward_or_imputation": False,
    "authorize_external_data_export": False,
}

EXPECTED_PRODUCTION_EXECUTION_ACTIONS = {
    "build_exact_run_scoped_candidate": True,
    "run_exact_independent_audit": True,
    "promote_release": False,
    "modify_current_release_pointer": False,
    "modify_core_derived_release": False,
    "filter_rows_or_stays": False,
    "create_analysis_cohort": False,
    "apply_carry_forward_or_imputation": False,
    "authorize_external_data_export": False,
}

OPERATIONAL_PROVENANCE_DEFINITIONS = {
    "__v3_source_file_id": (
        "Opaque deterministic source-file identifier assigned by the reviewed "
        "raw inventory and retained unchanged as operational provenance."
    ),
    "__v3_source_file_order": (
        "One-based position of the selected source file within a hospital and "
        "table after ordering the ingestion inputs by source-file identifier."
    ),
    "__v3_source_row_number": (
        "CSV data-record ordinal within the source file, with the first data "
        "record represented as 2 because the header occupies ordinal 1."
    ),
    "__v3_source_order": (
        "One-based row sequence across selected source files within one "
        "hospital and table; it follows ingestion file order and resets for "
        "each hospital and table."
    ),
    "__v3_source_schema_variant_id": (
        "Opaque schema-variant identifier derived by the reviewed inventory "
        "from source encoding, delimiter, and ordered CSV header."
    ),
}

HEADER_FIELDS = (
    ("hospital_id", pa.large_string(), False, "not_applicable"),
    ("stay_id_global", pa.large_string(), False, "not_applicable"),
    ("time_domain", pa.string(), False, "not_applicable"),
    ("block_index", pa.int32(), False, "index"),
    ("block_label", pa.string(), False, "not_applicable"),
    ("interval_semantics", pa.string(), False, "not_applicable"),
    ("block_start_h", pa.float64(), False, "h"),
    ("block_end_h", pa.float64(), False, "h"),
    ("block_start_anchored_time", pa.timestamp("ns"), False, "artificial_time"),
    ("block_end_anchored_time", pa.timestamp("ns"), False, "artificial_time"),
    ("available_through_h", pa.float64(), False, "h"),
    ("information_cutoff_h", pa.float64(), False, "h"),
    (
        "information_cutoff_anchored_time",
        pa.timestamp("ns"),
        False,
        "artificial_time",
    ),
    ("is_terminal_by_recording_extent_proxy", pa.bool_(), False, "not_applicable"),
    ("is_full_by_recording_extent_proxy", pa.bool_(), True, "not_applicable"),
    ("source_row_count", pa.int64(), False, "count"),
    ("has_source_rows", pa.bool_(), False, "not_applicable"),
)

STAY_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("hospital_id", pa.large_string(), nullable=False),
        pa.field("stay_id_global", pa.large_string(), nullable=False),
        pa.field("source_row_count", pa.int64(), nullable=False),
        pa.field("pre_admission_source_row_count", pa.int64(), nullable=False),
        pa.field("admission_source_row_count", pa.int64(), nullable=False),
        pa.field("post_admission_source_row_count", pa.int64(), nullable=False),
        pa.field("first_block_index", pa.int32(), nullable=False),
        pa.field("last_block_index", pa.int32(), nullable=False),
        pa.field("grid_block_count", pa.int32(), nullable=False),
        pa.field("observed_block_count", pa.int32(), nullable=False),
        pa.field("empty_block_count", pa.int32(), nullable=False),
        pa.field("proxy_full_post_admission_block_count", pa.int32(), nullable=False),
        pa.field("terminal_partial_block_count", pa.int32(), nullable=False),
        pa.field("icu_recording_extent_hours", pa.float64(), nullable=False),
    ],
    metadata={
        b"asic_v3_stage": b"time_blocking_candidate",
        b"asic_v3_time_blocking_contract_version": b"0.1",
        b"source_static_represented_by_lineage_only": b"true",
    },
)


@dataclass(frozen=True)
class TimeBlockingImplementationPolicy:
    contract_path: Path
    contract_sha256: str
    allowed_dataset_contexts: tuple[str, ...]
    input_rows_per_batch: int
    output_rows_per_batch: int
    compression: str
    build_private_directory: Path
    build_review_directory: Path
    audit_private_directory: Path
    audit_review_directory: Path
    candidate_artifact_version: str
    audit_artifact_version: str
    workflow_mode: str
    execution_authorization_path: Path | None
    source_path: Path
    source_sha256: str


@dataclass(frozen=True)
class TimeBlockingBuildConfig:
    dataset_context: str
    data_root: Path
    reports_root: Path
    implementation_policy_path: Path
    source_path: Path


@dataclass(frozen=True)
class TimeBlockingBuildResult:
    run_id: str
    candidate_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path
    block_row_count: int


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _contained_path(value: Any, location: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{location} must be a non-empty path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def load_time_blocking_build_config(
    path: str | Path,
) -> TimeBlockingBuildConfig:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Time-blocking build configuration")
    context = required_string(raw, "dataset_context", "config")
    if context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError("Unsupported time-blocking dataset context")
    implementation = resolve_path(
        required_string(raw, "time_blocking_implementation_policy", "config"),
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
    asic_root = source.parents[3] / "asic"
    validate_path_within(data_root, asic_root, "config.paths.data")
    validate_path_within(reports_root, asic_root, "config.paths.reports")
    validate_path_within(
        implementation, asic_root, "time-blocking implementation policy"
    )
    return TimeBlockingBuildConfig(
        dataset_context=context,
        data_root=data_root,
        reports_root=reports_root,
        implementation_policy_path=implementation,
        source_path=source,
    )


def load_time_blocking_implementation_policy(
    path: str | Path,
) -> TimeBlockingImplementationPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Time-blocking implementation policy")
    if raw.get("time_blocking_implementation_policy_version") != "0.1":
        raise ConfigurationError("Time-blocking implementation policy changed")
    status = raw.get("status")
    boundary = _mapping(raw.get("boundary"), "boundary")
    if status == "local_implementation_and_tests_only":
        if boundary != EXPECTED_LOCAL_IMPLEMENTATION_BOUNDARY:
            raise ConfigurationError("Local time-blocking scope changed")
        workflow_mode = "local_demo"
    elif (
        status
        == "production_candidate_and_audit_workflow_implemented_execution_pending"
    ):
        if boundary != EXPECTED_PRODUCTION_WORKFLOW_IMPLEMENTATION_BOUNDARY:
            raise ConfigurationError(
                "Production time-blocking workflow scope changed"
            )
        approval = _mapping(raw.get("approval"), "approval")
        if (
            approval.get("role") != "data_owner"
            or approval.get("recorded_date") != "2026-08-08"
            or required_string(approval, "statement", "approval")
            != "I authorize implementation of the run-scoped ASIC v3 8-hour production candidate and independent audit workflow. Do not run it, promote a release, or modify any pointer."
        ):
            raise ConfigurationError(
                "Production workflow implementation authorization changed"
            )
        workflow_mode = "production_execution_pending"
    else:
        raise ConfigurationError("Time-blocking implementation scope changed")
    contract_raw = _mapping(raw.get("contract"), "contract")
    contract_path = resolve_path(
        required_string(contract_raw, "path", "contract"), source
    )
    contract_hash = required_string(contract_raw, "sha256", "contract")
    if sha256_file(contract_path) != contract_hash:
        raise ConfigurationError("Frozen time-blocking contract changed")
    execution = _mapping(raw.get("execution"), "execution")
    allowed = execution.get("allowed_dataset_contexts")
    execution_authorization_path: Path | None = None
    if workflow_mode == "local_demo":
        if (
            allowed != ["demo"]
            or execution.get("production_candidate_authorized") is not False
            or execution.get("production_audit_authorized") is not False
        ):
            raise ConfigurationError(
                "Production time-blocking remains unauthorized"
            )
    else:
        if (
            allowed != ["production"]
            or execution.get("production_candidate_implementation_available")
            is not True
            or execution.get("production_audit_implementation_available")
            is not True
            or execution.get("production_candidate_execution_authorized")
            is not False
            or execution.get("production_audit_execution_authorized") is not False
        ):
            raise ConfigurationError(
                "Production execution must remain pending explicit authorization"
            )
        execution_authorization_path = resolve_path(
            required_string(
                execution,
                "execution_authorization_path",
                "execution",
            ),
            source,
        )
    if execution.get("compression") != "zstd":
        raise ConfigurationError("Time-blocking compression contract changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    return TimeBlockingImplementationPolicy(
        contract_path=contract_path,
        contract_sha256=contract_hash,
        allowed_dataset_contexts=tuple(allowed),
        input_rows_per_batch=_positive_int(
            execution.get("input_rows_per_batch"),
            "execution.input_rows_per_batch",
        ),
        output_rows_per_batch=_positive_int(
            execution.get("output_rows_per_batch"),
            "execution.output_rows_per_batch",
        ),
        compression="zstd",
        build_private_directory=_contained_path(
            outputs.get("build_private_directory"),
            "outputs.build_private_directory",
        ),
        build_review_directory=_contained_path(
            outputs.get("build_review_directory"),
            "outputs.build_review_directory",
        ),
        audit_private_directory=_contained_path(
            outputs.get("audit_private_directory"),
            "outputs.audit_private_directory",
        ),
        audit_review_directory=_contained_path(
            outputs.get("audit_review_directory"),
            "outputs.audit_review_directory",
        ),
        candidate_artifact_version=required_string(
            outputs, "candidate_artifact_version", "outputs"
        ),
        audit_artifact_version=required_string(
            outputs, "audit_artifact_version", "outputs"
        ),
        workflow_mode=workflow_mode,
        execution_authorization_path=execution_authorization_path,
        source_path=source,
        source_sha256=sha256_file(source),
    )


def require_production_execution_authorization(
    implementation: TimeBlockingImplementationPolicy,
    contract: TimeBlockingContract,
    candidate_run_id: str,
    audit_run_id: str | None,
) -> dict[str, str]:
    if implementation.workflow_mode == "local_demo":
        return {}
    path = implementation.execution_authorization_path
    if path is None or not path.is_file():
        raise HarmonizationError(
            "Production time-blocking execution is not authorized"
        )
    raw = load_yaml_mapping(path, "Production time-blocking execution authorization")
    approval = _mapping(raw.get("approval"), "approval")
    lineage = _mapping(raw.get("lineage"), "lineage")
    run_scope = _mapping(raw.get("run_scope"), "run_scope")
    actions = _mapping(raw.get("authorized_actions"), "authorized_actions")
    statement = required_string(approval, "statement", "approval")
    expected_audit_run_id = required_string(
        run_scope, "audit_run_id", "run_scope"
    )
    if (
        raw.get("artifact")
        != "asic_v3_time_blocking_8h_production_execution_authorization"
        or raw.get("artifact_version") != "0.1"
        or raw.get("status") != "human_approved_for_exact_candidate_and_audit_run"
        or raw.get("dataset_context") != "production"
        or raw.get("resolution") != contract.resolution_label
        or raw.get("core_derived_release_id")
        != contract.core_derived_release_id
        or approval.get("role") != "data_owner"
        or not isinstance(approval.get("recorded_date"), str)
        or candidate_run_id not in statement
        or expected_audit_run_id not in statement
        or lineage.get("time_blocking_contract_sha256")
        != implementation.contract_sha256
        or lineage.get("production_workflow_policy_sha256")
        != implementation.source_sha256
        or run_scope.get("candidate_run_id") != candidate_run_id
        or (audit_run_id is not None and expected_audit_run_id != audit_run_id)
        or actions != EXPECTED_PRODUCTION_EXECUTION_ACTIONS
    ):
        raise HarmonizationError(
            "Production time-blocking execution authorization is invalid"
        )
    return {
        "production_workflow_policy_sha256": implementation.source_sha256,
        "production_execution_authorization_sha256": sha256_file(path),
        "authorized_candidate_run_id": candidate_run_id,
        "authorized_audit_run_id": expected_audit_run_id,
    }


def block_schema(
    contract: TimeBlockingContract,
    specs: tuple[OutputSpec, ...],
) -> pa.Schema:
    fields = [
        pa.field(
            name,
            data_type,
            nullable=nullable,
            metadata={b"asic_v3_unit": unit.encode()},
        )
        for name, data_type, nullable, unit in HEADER_FIELDS
    ]
    fields.extend(
        pa.field(
            spec.output_name,
            spec.output_type,
            nullable=spec.nullable,
            metadata={
                b"asic_v3_unit": spec.output_unit.encode(),
                b"asic_v3_source_variable": spec.source_variable.encode(),
                b"asic_v3_aggregation_operation": spec.operation.encode(),
                b"asic_v3_analysis_eligibility": spec.eligibility.encode(),
            },
        )
        for spec in specs
    )
    return pa.schema(
        fields,
        metadata={
            b"asic_v3_stage": b"time_blocking_candidate",
            b"asic_v3_time_blocking_contract_version": contract.version.encode(),
            b"asic_v3_resolution": contract.resolution_label.encode(),
            b"publication_ready": b"false",
            b"carry_forward_applied": b"false",
            b"cohort_filtering_applied": b"false",
        },
    )


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _anchor_ns(anchor: str, hours: float) -> int:
    try:
        parsed = datetime.strptime(anchor, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise HarmonizationError("Artificial time anchor is invalid") from exc
    base = int(parsed.timestamp()) * 1_000_000_000
    offset = hours * 3_600_000_000_000
    rounded = round(offset)
    if not math.isfinite(offset) or abs(offset - rounded) > 0.25:
        raise HarmonizationError("Blocked time cannot be represented exactly in ns")
    return base + rounded


def _block_coordinates(index: int, resolution_h: float) -> tuple[str, str, float, float]:
    def label(value: float) -> str:
        return f"{value:g}h"

    if index < 0:
        return (
            "pre_admission",
            f"pre_{label(abs(index) * resolution_h)}",
            index * resolution_h,
            (index + 1) * resolution_h,
        )
    if index == 0:
        return "admission", "0h", 0.0, 0.0
    return (
        "post_admission",
        label(index * resolution_h),
        (index - 1) * resolution_h,
        index * resolution_h,
    )


def _last_observation(
    observations: list[tuple[float, int, Any]],
) -> tuple[Any, float | None]:
    if not observations:
        return None, None
    time_h, _, value = max(observations, key=lambda item: (item[0], item[1]))
    return value, time_h


def _numeric_summary(
    rows: list[dict[str, Any]],
    variable: str,
    cutoff_h: float,
) -> dict[str, Any]:
    observations: list[tuple[float, int, float]] = []
    for row in rows:
        value = row.get(variable)
        if value is None:
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            raise HarmonizationError(f"Non-finite blocked input: {variable}")
        observations.append(
            (
                float(row["minutes_since_icu_admission"]) / 60.0,
                int(row["__v3_source_order"]),
                numeric,
            )
        )
    values = [item[2] for item in observations]
    last, last_time = _last_observation(observations)
    count = len(values)
    return {
        "observation_count": count,
        "observed_zero_count": sum(value == 0 for value in values),
        "observed_positive_count": sum(value > 0 for value in values),
        "mean": sum(values) / count if count else None,
        "median": float(statistics.median(values)) if count else None,
        "minimum": min(values) if count else None,
        "maximum": max(values) if count else None,
        "last_observation_value": last,
        "last_observation_time_h": last_time,
        "last_observation_age_h": (
            cutoff_h - last_time if last_time is not None else None
        ),
    }


def _boolean_summary(
    rows: list[dict[str, Any]],
    variable: str,
    cutoff_h: float,
    component_index: int | None = None,
) -> dict[str, Any]:
    observations: list[tuple[float, int, bool]] = []
    for row in rows:
        value = row.get(variable)
        if component_index is not None:
            if value is None:
                value = None
            else:
                pair = list(value)
                value = pair[component_index]
        if value is None:
            continue
        if not isinstance(value, bool):
            raise HarmonizationError(f"Non-boolean blocked input: {variable}")
        observations.append(
            (
                float(row["minutes_since_icu_admission"]) / 60.0,
                int(row["__v3_source_order"]),
                value,
            )
        )
    values = [item[2] for item in observations]
    last, last_time = _last_observation(observations)
    return {
        "observation_count": len(values),
        "observed_true_count": sum(values),
        "observed_false_count": len(values) - sum(values),
        "any_true": any(values) if values else None,
        "all_true": all(values) if values else None,
        "last_observation_value": last,
        "last_observation_time_h": last_time,
        "last_observation_age_h": (
            cutoff_h - last_time if last_time is not None else None
        ),
    }


PAIR_LABEL = {False: "false", True: "true", None: "null"}


def _pair_state_counts(rows: list[dict[str, Any]], variable: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = row.get(variable)
        pair = [None, None] if value is None else list(value)
        if len(pair) != 2:
            raise HarmonizationError("Read-confirmation pair width changed")
        counts[f"{PAIR_LABEL[pair[0]]}_{PAIR_LABEL[pair[1]]}"] += 1
    return counts


def _aggregate_features(
    rows: list[dict[str, Any]],
    cutoff_h: float,
    specs: tuple[OutputSpec, ...],
) -> dict[str, Any]:
    by_source: dict[str, list[OutputSpec]] = defaultdict(list)
    for spec in specs:
        by_source[spec.source_variable].append(spec)
    output: dict[str, Any] = {}
    for variable, source_specs in by_source.items():
        group = source_specs[0].source_group
        if variable == "therapy_read_confirmation_utc":
            component_summaries = {
                component: _boolean_summary(
                    rows, variable, cutoff_h, component
                )
                for component in (0, 1)
            }
            pair_counts = _pair_state_counts(rows, variable)
            for spec in source_specs:
                if spec.component_index is not None:
                    output[spec.output_name] = component_summaries[
                        spec.component_index
                    ][spec.operation]
                else:
                    assert spec.pair_state is not None
                    output[spec.output_name] = pair_counts[spec.pair_state]
        elif group == "boolean":
            summary = _boolean_summary(rows, variable, cutoff_h)
            for spec in source_specs:
                output[spec.output_name] = summary[spec.operation]
        else:
            summary = _numeric_summary(rows, variable, cutoff_h)
            for spec in source_specs:
                output[spec.output_name] = summary[spec.operation]
    return output


def aggregate_stay(
    stay_id: str,
    hospital_id: str,
    recording_extent_hours: float,
    rows: list[dict[str, Any]],
    contract: TimeBlockingContract,
    specs: tuple[OutputSpec, ...],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not rows:
        raise HarmonizationError("A dynamic stay cannot be empty")
    resolution = contract.resolution_minutes
    resolution_h = resolution / 60.0
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    minimum_negative: float | None = None
    maximum_positive: float | None = None
    domain_counts: Counter[str] = Counter()
    maximum_time: float | None = None
    for row in rows:
        if row.get("stay_id_global") != stay_id or row.get("hospital_id") != hospital_id:
            raise HarmonizationError("Stay aggregation received mixed keys")
        raw_time = row.get("minutes_since_icu_admission")
        if raw_time is None or not math.isfinite(float(raw_time)):
            raise HarmonizationError("Time-blocking input time is unavailable")
        minutes = float(raw_time)
        domain, index = assign_time_block(minutes, resolution)
        grouped[index].append(row)
        domain_counts[domain] += 1
        maximum_time = minutes if maximum_time is None else max(maximum_time, minutes)
        if minutes < 0:
            minimum_negative = (
                minutes if minimum_negative is None else min(minimum_negative, minutes)
            )
        elif minutes > 0:
            maximum_positive = (
                minutes if maximum_positive is None else max(maximum_positive, minutes)
            )
    if maximum_time is None or not math.isclose(
        maximum_time / 60.0,
        recording_extent_hours,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise HarmonizationError("Recording extent does not match dynamic maximum")

    indices: list[int] = []
    if minimum_negative is not None:
        indices.extend(range(math.floor(minimum_negative / resolution), 0))
    indices.append(0)
    terminal_index: int | None = None
    if maximum_positive is not None:
        terminal_index = math.ceil(maximum_positive / resolution)
        indices.extend(range(1, terminal_index + 1))
    block_rows: list[dict[str, Any]] = []
    full_count = 0
    partial_count = 0
    for index in indices:
        domain, label, start_h, end_h = _block_coordinates(index, resolution_h)
        source_rows = grouped.get(index, [])
        terminal = index > 0 and index == terminal_index
        full: bool | None
        if index <= 0:
            full = None
        else:
            full = recording_extent_hours >= end_h
            full_count += int(full)
        if terminal and full is False:
            available_through_h = recording_extent_hours
            partial_count = 1
        else:
            available_through_h = end_h
        cutoff_h = available_through_h
        row = {
            "hospital_id": hospital_id,
            "stay_id_global": stay_id,
            "time_domain": domain,
            "block_index": index,
            "block_label": label,
            "interval_semantics": (
                "left_closed_right_open"
                if index < 0
                else "singleton_zero"
                if index == 0
                else "left_open_right_closed"
            ),
            "block_start_h": start_h,
            "block_end_h": end_h,
            "block_start_anchored_time": _anchor_ns(
                contract.artificial_anchor, start_h
            ),
            "block_end_anchored_time": _anchor_ns(
                contract.artificial_anchor, end_h
            ),
            "available_through_h": available_through_h,
            "information_cutoff_h": cutoff_h,
            "information_cutoff_anchored_time": _anchor_ns(
                contract.artificial_anchor, cutoff_h
            ),
            "is_terminal_by_recording_extent_proxy": terminal,
            "is_full_by_recording_extent_proxy": full,
            "source_row_count": len(source_rows),
            "has_source_rows": bool(source_rows),
        }
        row.update(_aggregate_features(source_rows, cutoff_h, specs))
        block_rows.append(row)
    observed = sum(bool(grouped.get(index)) for index in indices)
    summary = {
        "hospital_id": hospital_id,
        "stay_id_global": stay_id,
        "source_row_count": len(rows),
        "pre_admission_source_row_count": domain_counts["pre_admission"],
        "admission_source_row_count": domain_counts["admission"],
        "post_admission_source_row_count": domain_counts["post_admission"],
        "first_block_index": indices[0],
        "last_block_index": indices[-1],
        "grid_block_count": len(indices),
        "observed_block_count": observed,
        "empty_block_count": len(indices) - observed,
        "proxy_full_post_admission_block_count": full_count,
        "terminal_partial_block_count": partial_count,
        "icu_recording_extent_hours": recording_extent_hours,
    }
    return block_rows, summary


class _BufferedWriter:
    def __init__(
        self,
        path: Path,
        schema: pa.Schema,
        rows_per_batch: int,
        compression: str,
    ) -> None:
        self.schema = schema
        self.rows_per_batch = rows_per_batch
        self.rows: list[dict[str, Any]] = []
        self.writer = pq.ParquetWriter(path, schema, compression=compression)
        self.row_count = 0

    def append_many(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self.rows.append(row)
            if len(self.rows) >= self.rows_per_batch:
                self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        self.writer.write_table(table)
        self.row_count += table.num_rows
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        self.writer.close()


def _read_static(path: Path) -> dict[str, tuple[str, float]]:
    columns = ["stay_id_global", "hospital_id", "icu_recording_extent_hours"]
    output: dict[str, tuple[str, float]] = {}
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=50_000, columns=columns
    ):
        values = batch.to_pydict()
        for stay, hospital, extent in zip(
            values[columns[0]],
            values[columns[1]],
            values[columns[2]],
            strict=True,
        ):
            if stay is None or hospital is None or extent is None:
                raise HarmonizationError("Static blocking keys or extent are missing")
            key = str(stay)
            numeric = float(extent)
            if key in output or not math.isfinite(numeric):
                raise HarmonizationError("Static stay contract changed")
            output[key] = (str(hospital), numeric)
    return output


def _dictionary_schema() -> pa.Schema:
    names = (
        "source_variable",
        "source_definition",
        "source_physical_type",
        "source_unit",
        "source_eligibility",
        "disposition",
        "aggregation_operation",
        "output_name",
        "output_physical_type",
        "output_unit",
        "interval_origin",
        "interval_semantics",
        "last_value_semantics",
        "missingness_meaning",
        "zero_meaning",
        "false_meaning",
        "carry_forward_allowed",
        "analysis_eligibility",
        "caveats",
        "source_core_derived_contract_version",
        "source_core_derived_release_id",
        "time_blocking_contract_version",
    )
    return pa.schema(
        [
            pa.field(name, pa.bool_() if name == "carry_forward_allowed" else pa.string())
            for name in names
        ],
        metadata={
            b"asic_v3_artifact": b"blocked_variable_dictionary",
            b"asic_v3_time_blocking_contract_version": b"0.1",
        },
    )


def _read_mapping_json(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be an object")
    return value


def load_source_definitions(
    data_root: Path,
    source_schema: pa.Schema,
) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve exact source definitions and bind them to the frozen 0.2 dictionary."""

    contract_dir = data_root / "contracts/harmonized_schema_dictionary/0.2"
    manifest_path = contract_dir / "freeze_manifest.json"
    dictionary_path = contract_dir / "variable_dictionary.parquet"
    manifest = _read_mapping_json(
        manifest_path, "Frozen harmonized 0.2 dictionary manifest"
    )
    files = manifest.get("files")
    if (
        manifest.get("artifact")
        != "asic_v3_frozen_harmonized_schema_dictionary"
        or manifest.get("contract_version") != "0.2"
        or manifest.get("schema_frozen") is not True
        or manifest.get("dictionary_frozen") is not True
        or not isinstance(files, dict)
        or not dictionary_path.is_file()
        or files.get(dictionary_path.name) != sha256_file(dictionary_path)
    ):
        raise HarmonizationError("Frozen harmonized 0.2 dictionary changed")
    parquet = pq.ParquetFile(dictionary_path)
    required = {"table", "variable", "definition"}
    if not required.issubset(parquet.schema_arrow.names):
        raise HarmonizationError("Frozen source dictionary columns are incomplete")
    frozen: dict[str, str] = {}
    for batch in parquet.iter_batches(
        batch_size=1_024, columns=sorted(required)
    ):
        values = batch.to_pydict()
        for table, variable, definition in zip(
            values["table"],
            values["variable"],
            values["definition"],
            strict=True,
        ):
            if table != "dynamic":
                continue
            if (
                not isinstance(variable, str)
                or not variable
                or not isinstance(definition, str)
                or not definition.strip()
                or variable in frozen
            ):
                raise HarmonizationError(
                    "Frozen dynamic source dictionary is invalid"
                )
            frozen[variable] = definition.strip()
    resolved: dict[str, str] = {}
    for field in source_schema:
        metadata = field.metadata or {}
        derived = metadata.get(b"asic_v3_definition")
        if derived is not None and derived.decode(
            "utf-8", errors="replace"
        ).strip():
            resolved[field.name] = derived.decode(
                "utf-8", errors="replace"
            ).strip()
        elif field.name in frozen:
            resolved[field.name] = frozen[field.name]
        elif field.name in OPERATIONAL_PROVENANCE_DEFINITIONS:
            resolved[field.name] = OPERATIONAL_PROVENANCE_DEFINITIONS[
                field.name
            ]
        else:
            raise HarmonizationError(
                f"Source definition is unavailable: {field.name}"
            )
    return resolved, {
        "harmonized_dictionary_contract_version": "0.2",
        "harmonized_dictionary_manifest_sha256": sha256_file(manifest_path),
        "harmonized_variable_dictionary_sha256": sha256_file(dictionary_path),
    }


def dictionary_rows(
    contract: TimeBlockingContract,
    source_schema: pa.Schema,
    specs: tuple[OutputSpec, ...],
    source_definitions: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_source: dict[str, list[OutputSpec]] = defaultdict(list)
    for spec in specs:
        by_source[spec.source_variable].append(spec)
    for field in source_schema:
        metadata = field.metadata or {}
        unit = metadata.get(b"asic_v3_unit", b"unavailable").decode(
            "utf-8", errors="replace"
        )
        eligibility = (
            "analysis_ineligible"
            if field.name in contract.evidence_policy.analysis_ineligible
            else "conditionally_ineligible"
            if field.name in contract.evidence_policy.conditionally_ineligible
            else "eligible"
        )
        source_specs = by_source.get(field.name, [])
        if not source_specs:
            rows.append(
                {
                    "source_variable": field.name,
                    "source_definition": source_definitions[field.name],
                    "source_physical_type": str(field.type),
                    "source_unit": unit,
                    "source_eligibility": eligibility,
                    "disposition": "excluded_from_block_features_retained_in_accounting",
                    "aggregation_operation": "none",
                    "output_name": None,
                    "output_physical_type": None,
                    "output_unit": None,
                    "interval_origin": "exact_icu_admission",
                    "interval_semantics": "resolution_specific",
                    "last_value_semantics": None,
                    "missingness_meaning": "Unavailable or unobserved; never converted to zero.",
                    "zero_meaning": "Observed zero where applicable.",
                    "false_meaning": "Observed false where applicable.",
                    "carry_forward_allowed": False,
                    "analysis_eligibility": eligibility,
                    "caveats": "No blocked feature output under contract 0.1.",
                    "source_core_derived_contract_version": contract.core_derived_contract_version,
                    "source_core_derived_release_id": contract.core_derived_release_id,
                    "time_blocking_contract_version": contract.version,
                }
            )
            continue
        for spec in source_specs:
            rows.append(
                {
                    "source_variable": field.name,
                    "source_definition": source_definitions[field.name],
                    "source_physical_type": str(field.type),
                    "source_unit": unit,
                    "source_eligibility": eligibility,
                    "disposition": "blocked_feature",
                    "aggregation_operation": spec.operation,
                    "output_name": spec.output_name,
                    "output_physical_type": str(spec.output_type),
                    "output_unit": spec.output_unit,
                    "interval_origin": "exact_icu_admission",
                    "interval_semantics": "pre=[start,end); admission={0}; post=(start,end]",
                    "last_value_semantics": (
                        "Final non-missing observation by time then hospital-scoped source order."
                        if spec.operation.startswith("last_observation")
                        else None
                    ),
                    "missingness_meaning": "Null aggregate means no observed non-missing value in this block.",
                    "zero_meaning": "Observed zero is retained as an observation.",
                    "false_meaning": "Observed false is retained as an observation.",
                    "carry_forward_allowed": False,
                    "analysis_eligibility": spec.eligibility,
                    "caveats": spec.caveat,
                    "source_core_derived_contract_version": contract.core_derived_contract_version,
                    "source_core_derived_release_id": contract.core_derived_release_id,
                    "time_blocking_contract_version": contract.version,
                }
            )
    return rows


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


def _dictionary_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# ASIC v3 blocked variable dictionary",
        "",
        "No carry-forward, imputation, dose total, administration count, or cohort filtering is applied.",
        "",
        "| Source | Operation | Output | Type | Unit | Eligibility |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['source_variable']}` | `{row['aggregation_operation']}` | "
            f"`{row['output_name'] or ''}` | `{row['output_physical_type'] or ''}` | "
            f"`{row['output_unit'] or ''}` | `{row['analysis_eligibility']}` |"
        )
    lines.append("")
    return "\n".join(lines)


def _build_review_markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 time-blocking candidate build",
            "",
            f"- Run ID: `{payload['run_id']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Contract: `{payload['time_blocking_contract_version']}`",
            f"- Block rows: `{payload['metrics']['block_row_count']}`",
            f"- Static stays represented: `{payload['metrics']['stay_summary_row_count']}`",
            f"- Assigned source rows: `{payload['metrics']['assigned_source_row_count']}`",
            "- Overall status: `nonpublishable_requires_independent_audit`",
            "",
            "No release or pointer was created or modified. Production candidate execution remains unauthorized by the shipped implementation policy.",
            "",
        )
    )


def _source_input(
    config: TimeBlockingBuildConfig,
    contract: TimeBlockingContract,
) -> CoreDerivedReleaseInput:
    evidence_config = TimeBlockingEvidenceConfig(
        dataset_context=config.dataset_context,
        data_root=config.data_root,
        reports_root=config.reports_root,
        policy_path=contract.evidence_policy.source_path,
        source_path=config.source_path,
    )
    return _load_core_derived_release(evidence_config, contract.evidence_policy)


def run_time_blocking_candidate_build(
    config: TimeBlockingBuildConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> TimeBlockingBuildResult:
    implementation = load_time_blocking_implementation_policy(
        config.implementation_policy_path
    )
    if config.dataset_context not in implementation.allowed_dataset_contexts:
        raise HarmonizationError(
            "Production time-blocking candidate creation is not authorized"
        )
    contract = load_time_blocking_contract(implementation.contract_path)
    effective_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(effective_run_id):
        raise HarmonizationError("Invalid time-blocking candidate run ID")
    execution_lineage = require_production_execution_authorization(
        implementation,
        contract,
        effective_run_id,
        None,
    )
    load_and_validate_frozen_evidence(config.reports_root, contract)
    source = _source_input(config, contract)
    candidate_root = config.data_root / contract.candidate_directory
    final_dir = candidate_root / effective_run_id
    staging_dir = candidate_root / f".{effective_run_id}.incomplete"
    private_dir = (
        config.reports_root
        / "private"
        / implementation.build_private_directory
        / effective_run_id
    )
    review_dir = (
        config.reports_root / "review" / implementation.build_review_directory
    )
    review_json = review_dir / f"{effective_run_id}.json"
    review_md = review_dir / f"{effective_run_id}.md"
    if any(
        path.exists()
        for path in (final_dir, staging_dir, private_dir, review_json, review_md)
    ):
        raise HarmonizationError("Time-blocking candidate target already exists")

    specs = build_output_specs(contract, source.schemas["dynamic"])
    source_definitions, dictionary_lineage = load_source_definitions(
        config.data_root, source.schemas["dynamic"]
    )
    output_schema = block_schema(contract, specs)
    static = _read_static(source.table_paths["static"])
    candidate_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    staging_dir.mkdir(mode=0o700)
    block_path = staging_dir / contract.block_file
    summary_path = staging_dir / contract.stay_summary_file
    dictionary_path = staging_dir / contract.dictionary_parquet
    dictionary_md_path = staging_dir / contract.dictionary_markdown
    block_writer = _BufferedWriter(
        block_path,
        output_schema,
        implementation.output_rows_per_batch,
        implementation.compression,
    )
    summary_writer = _BufferedWriter(
        summary_path,
        STAY_SUMMARY_SCHEMA,
        implementation.output_rows_per_batch,
        implementation.compression,
    )
    required_sources = sorted({spec.source_variable for spec in specs})
    columns = [
        "stay_id_global",
        "hospital_id",
        "minutes_since_icu_admission",
        "__v3_source_file_order",
        "__v3_source_row_number",
        "__v3_source_order",
        *required_sources,
    ]
    columns = list(dict.fromkeys(columns))
    current_stay: str | None = None
    current_hospital: str | None = None
    current_rows: list[dict[str, Any]] = []
    seen_stays: set[str] = set()
    metrics: Counter[str] = Counter()

    def finish_stay() -> None:
        nonlocal current_rows
        if current_stay is None or current_hospital is None:
            return
        static_value = static.get(current_stay)
        if static_value is None or static_value[0] != current_hospital:
            raise HarmonizationError("Dynamic/static stay lineage changed")
        blocks, summary = aggregate_stay(
            current_stay,
            current_hospital,
            static_value[1],
            current_rows,
            contract,
            specs,
        )
        block_writer.append_many(blocks)
        summary_writer.append_many([summary])
        metrics["stay_count"] += 1
        metrics["assigned_source_row_count"] += len(current_rows)
        metrics["empty_block_count"] += summary["empty_block_count"]
        metrics["terminal_partial_block_count"] += summary[
            "terminal_partial_block_count"
        ]
        seen_stays.add(current_stay)
        current_rows = []

    try:
        parquet = pq.ParquetFile(source.table_paths["dynamic"])
        for batch_index, batch in enumerate(
            parquet.iter_batches(
                batch_size=implementation.input_rows_per_batch,
                columns=columns,
            )
        ):
            values = batch.to_pydict()
            for index in range(batch.num_rows):
                row = {name: values[name][index] for name in columns}
                stay_value = row["stay_id_global"]
                hospital_value = row["hospital_id"]
                if stay_value is None or hospital_value is None:
                    raise HarmonizationError("Dynamic blocking key is missing")
                stay, hospital = str(stay_value), str(hospital_value)
                if current_stay is None:
                    current_stay, current_hospital = stay, hospital
                elif stay != current_stay:
                    finish_stay()
                    if stay in seen_stays:
                        raise HarmonizationError("Dynamic stay rows are not contiguous")
                    current_stay, current_hospital = stay, hospital
                elif hospital != current_hospital:
                    raise HarmonizationError("Hospital changes within a stay")
                current_rows.append(row)
                metrics["dynamic_row_count"] += 1
            if progress and (batch_index + 1) % 100 == 0:
                progress(
                    f"time_blocking_build_rows_scanned={metrics['dynamic_row_count']}"
                )
        finish_stay()
        block_writer.close()
        summary_writer.close()
    except Exception:
        block_writer.writer.close()
        summary_writer.writer.close()
        raise
    if (
        metrics["dynamic_row_count"] != contract.evidence.expected_dynamic_rows
        or metrics["assigned_source_row_count"] != metrics["dynamic_row_count"]
        or metrics["stay_count"] != contract.evidence.expected_static_rows
        or seen_stays != set(static)
    ):
        raise HarmonizationError("Time-blocking candidate conservation failed")
    if config.dataset_context == "production" and (
        block_writer.row_count != contract.evidence.expected_total_grid_blocks
        or metrics["empty_block_count"]
        != contract.evidence.expected_empty_grid_blocks
        or metrics["terminal_partial_block_count"]
        != contract.evidence.expected_terminal_partial_blocks
    ):
        raise HarmonizationError("Time-blocking candidate evidence counts changed")

    dictionary = dictionary_rows(
        contract, source.schemas["dynamic"], specs, source_definitions
    )
    pq.write_table(
        pa.Table.from_pylist(dictionary, schema=_dictionary_schema()),
        dictionary_path,
        compression=implementation.compression,
    )
    _write_text(dictionary_md_path, _dictionary_markdown(dictionary), 0o600)
    for path in (block_path, summary_path, dictionary_path):
        path.chmod(0o600)
    output_files = {
        contract.block_file: {
            "sha256": sha256_file(block_path),
            "row_count": block_writer.row_count,
            "schema_sha256": _schema_digest(output_schema),
        },
        contract.stay_summary_file: {
            "sha256": sha256_file(summary_path),
            "row_count": summary_writer.row_count,
            "schema_sha256": _schema_digest(STAY_SUMMARY_SCHEMA),
        },
        contract.dictionary_parquet: {
            "sha256": sha256_file(dictionary_path),
            "row_count": len(dictionary),
            "schema_sha256": _schema_digest(_dictionary_schema()),
        },
        contract.dictionary_markdown: {
            "sha256": sha256_file(dictionary_md_path),
            "row_count": None,
            "schema_sha256": None,
        },
    }
    manifest_path = staging_dir / contract.candidate_manifest
    manifest = {
        "artifact": "asic_v3_time_blocking_candidate",
        "artifact_version": implementation.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "run_id": effective_run_id,
        "status": "nonpublishable_requires_independent_audit_and_human_promotion",
        "resolution": contract.resolution_label,
        "time_blocking_contract_version": contract.version,
        "lineage": {
            "contract_sha256": implementation.contract_sha256,
            "evidence_run_id": contract.evidence.run_id,
            "evidence_review_sha256": contract.evidence.review_sha256,
            "core_derived_release_id": contract.core_derived_release_id,
            "core_derived_release_manifest_sha256": source.release_manifest_sha256,
            "core_derived_file_hashes": source.table_hashes,
            "source_static_path": str(source.table_paths["static"]),
            **dictionary_lineage,
            **execution_lineage,
        },
        "outputs": output_files,
        "metrics": {
            "source_dynamic_row_count": metrics["dynamic_row_count"],
            "assigned_source_row_count": metrics["assigned_source_row_count"],
            "stay_summary_row_count": summary_writer.row_count,
            "block_row_count": block_writer.row_count,
            "empty_block_count": metrics["empty_block_count"],
            "terminal_partial_block_count": metrics[
                "terminal_partial_block_count"
            ],
            "output_feature_count": len(specs),
        },
        "static_table_copied": False,
        "static_table_repeated_on_blocks": False,
        "rows_or_stays_filtered": False,
        "analysis_cohort_created": False,
        "carry_forward_or_imputation_applied": False,
        "medication_dose_totals_emitted": False,
        "release_created": False,
        "current_release_pointer_modified": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging_dir.replace(final_dir)
    manifest_path = final_dir / contract.candidate_manifest

    review_payload = {
        "artifact": "asic_v3_time_blocking_candidate_build_review",
        "artifact_version": implementation.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "run_id": effective_run_id,
        "time_blocking_contract_version": contract.version,
        "candidate_manifest_sha256": sha256_file(manifest_path),
        "metrics": manifest["metrics"],
        "overall_status": "nonpublishable_requires_independent_audit",
        "source_release_modified": False,
        "release_created": False,
        "current_release_pointer_modified": False,
        "rows_or_stays_filtered": False,
        "carry_forward_or_imputation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    _write_json(private_dir / "build_manifest.json", manifest, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _build_review_markdown(review_payload), 0o640)
    return TimeBlockingBuildResult(
        run_id=effective_run_id,
        candidate_directory=final_dir,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_md,
        block_row_count=block_writer.row_count,
    )
