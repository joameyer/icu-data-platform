from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import statistics
from typing import Any, Callable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.time_blocking.contract import (
    OutputSpec,
    build_output_specs,
    load_and_validate_frozen_evidence,
    load_time_blocking_contract,
)
from asic_pipeline.time_blocking.engine import (
    HEADER_FIELDS,
    STAY_SUMMARY_SCHEMA,
    TimeBlockingBuildConfig,
    _anchor_ns,
    _dictionary_schema,
    _read_static,
    _schema_digest,
    _source_input,
    block_schema,
    dictionary_rows,
    load_source_definitions,
    load_time_blocking_implementation_policy,
    require_production_execution_authorization,
)


@dataclass(frozen=True)
class TimeBlockingAuditResult:
    run_id: str
    build_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


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


def _row_iterator(path: Path, batch_size: int) -> Iterator[dict[str, Any]]:
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
        yield from batch.to_pylist()


def _source_stays(
    path: Path,
    columns: list[str],
    batch_size: int,
) -> Iterator[tuple[str, str, list[dict[str, Any]]]]:
    current_stay: str | None = None
    current_hospital: str | None = None
    rows: list[dict[str, Any]] = []
    closed: set[str] = set()
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=batch_size, columns=columns
    ):
        values = batch.to_pydict()
        for index in range(batch.num_rows):
            row = {name: values[name][index] for name in columns}
            stay_value, hospital_value = row["stay_id_global"], row["hospital_id"]
            if stay_value is None or hospital_value is None:
                raise HarmonizationError("Independent audit found missing stay keys")
            stay, hospital = str(stay_value), str(hospital_value)
            if current_stay is None:
                current_stay, current_hospital = stay, hospital
            elif stay != current_stay:
                assert current_hospital is not None
                yield current_stay, current_hospital, rows
                closed.add(current_stay)
                if stay in closed:
                    raise HarmonizationError(
                        "Independent audit found noncontiguous stay rows"
                    )
                current_stay, current_hospital, rows = stay, hospital, []
            elif hospital != current_hospital:
                raise HarmonizationError("Independent audit found hospital conflict")
            rows.append(row)
    if current_stay is not None:
        assert current_hospital is not None
        yield current_stay, current_hospital, rows


def _candidate_stay_rows(
    iterator: Iterator[dict[str, Any]],
    state: list[dict[str, Any] | None],
    stay: str,
    hospital: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pending = state[0]
    while True:
        if pending is None:
            try:
                pending = next(iterator)
            except StopIteration:
                state[0] = None
                break
        candidate_stay = pending.get("stay_id_global")
        candidate_hospital = pending.get("hospital_id")
        if candidate_stay != stay or candidate_hospital != hospital:
            state[0] = pending
            break
        rows.append(pending)
        pending = None
    return rows


def _expected_indices(rows: list[dict[str, Any]], resolution: int) -> list[int]:
    negative: list[float] = []
    positive: list[float] = []
    for row in rows:
        value = row.get("minutes_since_icu_admission")
        if value is None or not math.isfinite(float(value)):
            raise HarmonizationError("Independent audit found invalid elapsed time")
        numeric = float(value)
        if numeric < 0:
            negative.append(numeric)
        elif numeric > 0:
            positive.append(numeric)
    indices: list[int] = []
    if negative:
        indices.extend(range(math.floor(min(negative) / resolution), 0))
    indices.append(0)
    if positive:
        indices.extend(range(1, math.ceil(max(positive) / resolution) + 1))
    return indices


def _assigned_rows(
    rows: list[dict[str, Any]], index: int, resolution: int
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        minutes = float(row["minutes_since_icu_admission"])
        if minutes < 0:
            observed_index = math.floor(minutes / resolution)
        elif minutes == 0:
            observed_index = 0
        else:
            observed_index = math.ceil(minutes / resolution)
        if observed_index == index:
            output.append(row)
    return output


def _coordinates(index: int, resolution_h: float) -> tuple[str, str, str, float, float]:
    label_value = lambda value: f"{value:g}h"
    if index < 0:
        return (
            "pre_admission",
            f"pre_{label_value(abs(index) * resolution_h)}",
            "left_closed_right_open",
            index * resolution_h,
            (index + 1) * resolution_h,
        )
    if index == 0:
        return "admission", "0h", "singleton_zero", 0.0, 0.0
    return (
        "post_admission",
        label_value(index * resolution_h),
        "left_open_right_closed",
        (index - 1) * resolution_h,
        index * resolution_h,
    )


def _observations(
    rows: list[dict[str, Any]],
    spec: OutputSpec,
) -> list[tuple[float, int, Any]]:
    output: list[tuple[float, int, Any]] = []
    for row in rows:
        value = row.get(spec.source_variable)
        if spec.component_index is not None:
            pair = [None, None] if value is None else list(value)
            if len(pair) != 2:
                raise HarmonizationError("Independent pair audit found invalid width")
            value = pair[spec.component_index]
        if value is None:
            continue
        output.append(
            (
                float(row["minutes_since_icu_admission"]) / 60.0,
                int(row["__v3_source_order"]),
                value,
            )
        )
    return output


def _expected_feature(
    spec: OutputSpec,
    rows: list[dict[str, Any]],
    cutoff_h: float,
) -> Any:
    if spec.pair_state is not None:
        count = 0
        for row in rows:
            value = row.get(spec.source_variable)
            pair = [None, None] if value is None else list(value)
            labels = [
                "null" if item is None else "true" if item is True else "false"
                for item in pair
            ]
            count += int("_".join(labels) == spec.pair_state)
        return count
    observations = _observations(rows, spec)
    values = [item[2] for item in observations]
    operation = spec.operation
    if operation == "observation_count":
        return len(values)
    if operation == "observed_zero_count":
        return sum(float(value) == 0 for value in values)
    if operation == "observed_positive_count":
        return sum(float(value) > 0 for value in values)
    if operation == "observed_true_count":
        return sum(value is True for value in values)
    if operation == "observed_false_count":
        return sum(value is False for value in values)
    if operation == "any_true":
        return any(values) if values else None
    if operation == "all_true":
        return all(values) if values else None
    if not values:
        return None
    if operation == "mean":
        return sum(float(value) for value in values) / len(values)
    if operation == "median":
        return float(statistics.median(float(value) for value in values))
    if operation == "minimum":
        return min(float(value) for value in values)
    if operation == "maximum":
        return max(float(value) for value in values)
    last_time, _, last_value = max(observations, key=lambda item: (item[0], item[1]))
    if operation == "last_observation_value":
        return last_value
    if operation == "last_observation_time_h":
        return last_time
    if operation == "last_observation_age_h":
        return cutoff_h - last_time
    raise HarmonizationError(f"Independent audit operation is unsupported: {operation}")


def _equal(observed: Any, expected: Any) -> bool:
    if observed is None or expected is None:
        return observed is expected
    if isinstance(expected, float):
        try:
            return math.isclose(
                float(observed), expected, rel_tol=1e-13, abs_tol=1e-12
            )
        except (TypeError, ValueError):
            return False
    return observed == expected


def _audit_block_row(
    candidate: dict[str, Any],
    source_rows: list[dict[str, Any]],
    index: int,
    recording_extent_h: float,
    terminal_index: int | None,
    resolution: int,
    anchor: str,
    specs: tuple[OutputSpec, ...],
    differences: Counter[str],
) -> None:
    resolution_h = resolution / 60.0
    domain, label, interval, start_h, end_h = _coordinates(index, resolution_h)
    terminal = index > 0 and index == terminal_index
    full = None if index <= 0 else recording_extent_h >= end_h
    available = recording_extent_h if terminal and full is False else end_h
    expected_header = {
        "time_domain": domain,
        "block_index": index,
        "block_label": label,
        "interval_semantics": interval,
        "block_start_h": start_h,
        "block_end_h": end_h,
        "available_through_h": available,
        "information_cutoff_h": available,
        "is_terminal_by_recording_extent_proxy": terminal,
        "is_full_by_recording_extent_proxy": full,
        "source_row_count": len(source_rows),
        "has_source_rows": bool(source_rows),
    }
    for name, expected in expected_header.items():
        if not _equal(candidate.get(name), expected):
            differences[f"header:{name}"] += 1
    anchored_expected = {
        "block_start_anchored_time": _anchor_ns(anchor, start_h),
        "block_end_anchored_time": _anchor_ns(anchor, end_h),
        "information_cutoff_anchored_time": _anchor_ns(anchor, available),
    }
    for name, expected_ns in anchored_expected.items():
        observed = candidate.get(name)
        observed_ns = None
        if observed is not None:
            observed_ns = int(pa.scalar(observed, type=pa.timestamp("ns")).cast(pa.int64()).as_py())
        if observed_ns != expected_ns:
            differences[f"header:{name}"] += 1
    for spec in specs:
        expected = _expected_feature(spec, source_rows, available)
        if not _equal(candidate.get(spec.output_name), expected):
            differences[f"feature:{spec.output_name}"] += 1


def _review_markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        (
            "# ASIC v3 time-blocking candidate audit",
            "",
            f"- Audit run: `{payload['run_id']}`",
            f"- Candidate run: `{payload['build_run_id']}`",
            f"- Overall status: `{payload['overall_status']}`",
            f"- Compared block rows: `{payload['metrics']['compared_block_rows']}`",
            f"- Compared feature cells: `{payload['metrics']['compared_feature_cells']}`",
            f"- Technical blockers: `{len(payload['technical_blocking_findings'])}`",
            "",
            "The candidate remains non-publishable and requires explicit production execution authorization and later promotion approval.",
            "",
        )
    )


def run_time_blocking_candidate_audit(
    config: TimeBlockingBuildConfig,
    build_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> TimeBlockingAuditResult:
    implementation = load_time_blocking_implementation_policy(
        config.implementation_policy_path
    )
    if config.dataset_context not in implementation.allowed_dataset_contexts:
        raise HarmonizationError("Production time-blocking audit is not authorized")
    contract = load_time_blocking_contract(implementation.contract_path)
    effective_run_id = run_id or default_run_id()
    if (
        not RUN_ID_PATTERN.fullmatch(effective_run_id)
        or not RUN_ID_PATTERN.fullmatch(build_run_id)
    ):
        raise HarmonizationError("Invalid time-blocking audit or build run ID")
    execution_lineage = require_production_execution_authorization(
        implementation,
        contract,
        build_run_id,
        effective_run_id,
    )
    load_and_validate_frozen_evidence(config.reports_root, contract)
    source = _source_input(config, contract)
    candidate_dir = config.data_root / contract.candidate_directory / build_run_id
    manifest_path = candidate_dir / contract.candidate_manifest
    manifest = _read_json(manifest_path, "Time-blocking candidate manifest")
    if (
        manifest.get("artifact") != "asic_v3_time_blocking_candidate"
        or manifest.get("artifact_version")
        != implementation.candidate_artifact_version
        or manifest.get("dataset_context") != config.dataset_context
        or manifest.get("run_id") != build_run_id
        or manifest.get("time_blocking_contract_version") != contract.version
        or manifest.get("rows_or_stays_filtered") is not False
        or manifest.get("carry_forward_or_imputation_applied") is not False
        or manifest.get("medication_dose_totals_emitted") is not False
        or manifest.get("release_created") is not False
        or manifest.get("current_release_pointer_modified") is not False
    ):
        raise HarmonizationError("Time-blocking candidate manifest changed")
    specs = build_output_specs(contract, source.schemas["dynamic"])
    expected_schemas = {
        contract.block_file: block_schema(contract, specs),
        contract.stay_summary_file: STAY_SUMMARY_SCHEMA,
        contract.dictionary_parquet: _dictionary_schema(),
    }
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Time-blocking candidate outputs are missing")
    technical: list[dict[str, Any]] = []
    for name, schema in expected_schemas.items():
        path = candidate_dir / name
        item = outputs.get(name)
        parquet = pq.ParquetFile(path) if path.is_file() else None
        if (
            not isinstance(item, dict)
            or parquet is None
            or item.get("sha256") != sha256_file(path)
            or item.get("row_count") != parquet.metadata.num_rows
            or item.get("schema_sha256") != _schema_digest(schema)
            or parquet.schema_arrow != schema
        ):
            technical.append(
                {"check": f"candidate_output_integrity:{name}", "details": "Candidate output hash, rows, or schema changed."}
            )
    markdown_item = outputs.get(contract.dictionary_markdown)
    markdown_path = candidate_dir / contract.dictionary_markdown
    if (
        not isinstance(markdown_item, dict)
        or not markdown_path.is_file()
        or markdown_item.get("sha256") != sha256_file(markdown_path)
    ):
        technical.append(
            {"check": "candidate_dictionary_markdown_integrity", "details": "Candidate Markdown dictionary changed."}
        )
    source_definitions, dictionary_lineage = load_source_definitions(
        config.data_root, source.schemas["dynamic"]
    )
    lineage_value = manifest.get("lineage")
    lineage = lineage_value if isinstance(lineage_value, dict) else {}
    if not isinstance(lineage_value, dict) or any(
        lineage.get(key) != value for key, value in dictionary_lineage.items()
    ):
        technical.append(
            {
                "check": "source_dictionary_lineage",
                "details": "Candidate source dictionary lineage changed.",
            }
        )
    if any(
        lineage.get(key) != value for key, value in execution_lineage.items()
    ):
        technical.append(
            {
                "check": "production_execution_lineage",
                "details": "Candidate execution-authorization lineage changed.",
            }
        )
    dictionary_expected = dictionary_rows(
        contract, source.schemas["dynamic"], specs, source_definitions
    )
    dictionary_observed = pq.read_table(
        candidate_dir / contract.dictionary_parquet
    ).to_pylist()
    if dictionary_observed != dictionary_expected:
        technical.append(
            {"check": "dictionary_reconstruction", "details": "Blocked dictionary differs from independent reconstruction."}
        )

    required_sources = sorted({spec.source_variable for spec in specs})
    source_columns = list(
        dict.fromkeys(
            [
                "stay_id_global",
                "hospital_id",
                "minutes_since_icu_admission",
                "__v3_source_file_order",
                "__v3_source_row_number",
                "__v3_source_order",
                *required_sources,
            ]
        )
    )
    static = _read_static(source.table_paths["static"])
    candidate_iterator = _row_iterator(
        candidate_dir / contract.block_file,
        max(1, implementation.output_rows_per_batch + 17),
    )
    candidate_state: list[dict[str, Any] | None] = [None]
    differences: Counter[str] = Counter()
    metrics: Counter[str] = Counter()
    summaries = _row_iterator(
        candidate_dir / contract.stay_summary_file,
        max(1, implementation.output_rows_per_batch + 11),
    )
    summary_iterator = iter(summaries)
    for stay, hospital, rows in _source_stays(
        source.table_paths["dynamic"],
        source_columns,
        max(1, implementation.input_rows_per_batch + 31),
    ):
        static_value = static.get(stay)
        if static_value is None or static_value[0] != hospital:
            raise HarmonizationError("Independent audit found static lineage drift")
        candidate_rows = _candidate_stay_rows(
            candidate_iterator, candidate_state, stay, hospital
        )
        indices = _expected_indices(rows, contract.resolution_minutes)
        if [row.get("block_index") for row in candidate_rows] != indices:
            differences["block_grid"] += 1
            continue
        positive = [
            float(row["minutes_since_icu_admission"])
            for row in rows
            if float(row["minutes_since_icu_admission"]) > 0
        ]
        terminal_index = (
            math.ceil(max(positive) / contract.resolution_minutes)
            if positive
            else None
        )
        for candidate, index in zip(candidate_rows, indices, strict=True):
            assigned = _assigned_rows(rows, index, contract.resolution_minutes)
            _audit_block_row(
                candidate,
                assigned,
                index,
                static_value[1],
                terminal_index,
                contract.resolution_minutes,
                contract.artificial_anchor,
                specs,
                differences,
            )
            metrics["compared_block_rows"] += 1
            metrics["compared_feature_cells"] += len(specs)
            metrics["assigned_source_rows"] += len(assigned)
        try:
            summary = next(summary_iterator)
        except StopIteration:
            differences["stay_summary_ended_early"] += 1
            break
        if (
            summary.get("stay_id_global") != stay
            or summary.get("hospital_id") != hospital
            or summary.get("source_row_count") != len(rows)
            or summary.get("grid_block_count") != len(indices)
            or summary.get("empty_block_count")
            != sum(not _assigned_rows(rows, index, contract.resolution_minutes) for index in indices)
        ):
            differences["stay_summary"] += 1
        metrics["compared_stays"] += 1
        if progress and metrics["compared_stays"] % 1000 == 0:
            progress(f"time_blocking_audit_stays={metrics['compared_stays']}")
    if candidate_state[0] is not None:
        differences["candidate_has_extra_rows"] += 1
    else:
        try:
            next(candidate_iterator)
            differences["candidate_has_extra_rows"] += 1
        except StopIteration:
            pass
    try:
        next(summary_iterator)
        differences["stay_summary_has_extra_rows"] += 1
    except StopIteration:
        pass
    if differences:
        technical.append(
            {
                "check": "independent_every_cell_recomputation",
                "details": f"Independent differences by class: {dict(sorted(differences.items()))}",
            }
        )
    if (
        metrics["assigned_source_rows"] != contract.evidence.expected_dynamic_rows
        or metrics["compared_stays"] != contract.evidence.expected_static_rows
    ) and config.dataset_context == "production":
        technical.append(
            {"check": "independent_conservation", "details": "Production source row or stay conservation failed."}
        )
    blocking = (
        {
            "check": "time_blocking_candidate_promotion_approved",
            "details": "A technically passing candidate would still require explicit human promotion approval.",
        },
    )
    overall = "fail" if technical else "pending_human_review"
    private_dir = (
        config.reports_root
        / "private"
        / implementation.audit_private_directory
        / effective_run_id
    )
    review_dir = (
        config.reports_root / "review" / implementation.audit_review_directory
    )
    review_json = review_dir / f"{effective_run_id}.json"
    review_md = review_dir / f"{effective_run_id}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Time-blocking audit target already exists")
    review_payload = {
        "artifact": "asic_v3_time_blocking_candidate_audit_review",
        "artifact_version": implementation.audit_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "run_id": effective_run_id,
        "build_run_id": build_run_id,
        "overall_status": overall,
        "time_blocking_contract_version": contract.version,
        "candidate_manifest_sha256": sha256_file(manifest_path),
        "metrics": dict(sorted(metrics.items())),
        "technical_blocking_findings": technical,
        "blocking_findings": list(blocking),
        "candidate_modified": False,
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
    _write_json(
        private_dir / "audit_manifest.json",
        {
            **review_payload,
            "candidate_directory": str(candidate_dir),
            "source_dynamic_sha256": source.table_hashes["dynamic"],
        },
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _review_markdown(review_payload), 0o640)
    return TimeBlockingAuditResult(
        run_id=effective_run_id,
        build_run_id=build_run_id,
        overall_status=overall,
        blocking_findings=blocking,
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
