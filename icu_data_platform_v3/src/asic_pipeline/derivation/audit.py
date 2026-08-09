from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.derivation.pipeline import (
    CoreDerivedConfig,
    VentilationTracker,
    _candidate_lineage,
    _expected_accounting,
    _load_core_derived_source,
    _read_json,
    _schema_digest,
    derived_schema,
    load_core_derived_policy,
    load_reviewed_evidence,
    transform_dynamic_batch,
    transform_static_batch,
    update_ventilation_tracker,
)
from asic_pipeline.derivation.review import (
    load_cleaned_release_input,  # historical monkeypatch surface
    load_derivation_contract_review_policy,  # historical monkeypatch surface
)
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _next_slice,
    _parquet_schema_matches,
    _require_output_exhausted,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


@dataclass(frozen=True)
class CoreDerivedAuditResult:
    run_id: str
    derived_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


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
    metrics = payload["metrics"]
    counts = payload["derivation_accounting"]
    return "\n".join(
        (
            "# ASIC v3 core-derived candidate audit review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Derived candidate: `{payload['derived_run_id']}`",
            f"- Input cleaned release: `{payload['cleaned_release_id']}`",
            f"- Core-derived contract: `{payload['core_derived_contract_version']}`",
            "- Technical audit status: **PASS**",
            "- Overall status: **PENDING HUMAN REVIEW**",
            "- Technical blocking findings: `0`",
            "- Human blocking findings: `1`",
            f"- Compared static rows: `{metrics['compared_static_rows']}`",
            f"- Compared dynamic rows: `{metrics['compared_dynamic_rows']}`",
            f"- Compared output cells: `{metrics['compared_output_cell_count']}`",
            "- Every input column preserved exactly: `true`",
            "- Every derived value recomputed exactly: `true`",
            "- Rows or stays filtered: `false`",
            "- Cohort generated: `false`",
            "- Time blocking applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Derived accounting",
            "",
            f"- Exact elapsed hours: non-missing `{counts['hours_non_missing_count']}`; negative retained `{counts['hours_negative_count']}`",
            f"- Computed driving pressure: in range retained `{counts['driving_pressure_in_range_count']}`; out of range masked and flagged `{counts['driving_pressure_out_of_range_count']}`",
            f"- Hospital mortality: true `{counts['hospital_mortality_true']}`; false `{counts['hospital_mortality_false']}`; missing `{counts['hospital_mortality_missing']}`; conflicts `{counts['hospital_mortality_conflict_count']}`",
            f"- ICU mortality: true `{counts['icu_mortality_true']}`; false `{counts['icu_mortality_false']}`; missing `{counts['icu_mortality_missing']}`; conflicts `{counts['icu_mortality_conflict_count']}`",
            f"- Observed-support timestamps at all elapsed times: `{counts['ventilation_supported_timestamp_count_total']}`",
            f"- Nonnegative-time observed-support episodes: `{counts['ventilation_episode_count_total']}`",
            f"- Stays with an observed-support episode ≥24 hours: `{counts['observed_mechanical_ventilation_ge_24h_true']}`",
            "",
            "## Human review gate",
            "",
            "Review this complete audit and explicitly approve the exact candidate before immutable derived-release promotion. Promotion will not rerun derivation or change any Parquet byte.",
            "",
            "## Boundary",
            "",
            "- Computed and reported driving pressure remain separate.",
            "- Mortality source fields remain present alongside conflict-aware derived outcomes.",
            "- The ventilation flag is an observed-support proxy, not ground truth and not a cohort filter.",
            "- SOFA and iSOFA variants remain unchanged and separate.",
            "- No clinical artifact outside the run-scoped candidate is published.",
            "",
        )
    )


def _compare_batches(
    expected: pa.RecordBatch,
    observed: pa.RecordBatch,
    table: str,
) -> int:
    if expected.num_rows != observed.num_rows or expected.schema != observed.schema:
        raise HarmonizationError(f"Derived {table} batch shape or schema changed")
    compared = 0
    for field in expected.schema:
        index = expected.schema.get_field_index(field.name)
        if not expected.column(index).equals(observed.column(index)):
            raise HarmonizationError(f"Derived value mismatch: {table}.{field.name}")
        compared += expected.num_rows
    return compared


def run_core_derived_candidate_audit(
    config: CoreDerivedConfig,
    derived_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> CoreDerivedAuditResult:
    if config.dataset_context != "production":
        raise HarmonizationError("The approved core-derived audit is production-only")
    policy = load_core_derived_policy(config.policy_path)
    source = _load_core_derived_source(config, policy)
    evidence = load_reviewed_evidence(config, policy)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run) or not RUN_ID_PATTERN.fullmatch(
        derived_run_id
    ):
        raise HarmonizationError("Invalid core-derived audit run ID")
    candidate_dir = config.data_root / policy.candidate_directory_name / derived_run_id
    manifest_path = candidate_dir / "derived_manifest.json"
    manifest = _read_json(manifest_path, "Core-derived candidate manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Core-derived candidate manifest is invalid")
    expected_lineage = _candidate_lineage(source, evidence, policy)
    if (
        manifest.get("artifact") != "asic_v3_core_derived_candidate"
        or manifest.get("artifact_version") != policy.candidate_artifact_version
        or manifest.get("run_id") != derived_run_id
        or manifest.get("status")
        != "nonpublishable_requires_complete_derived_audit"
        or manifest.get("core_derived_contract_version") != policy.version
        or manifest.get("lineage") != expected_lineage
        or manifest.get("cleaning_applied") is not True
        or manifest.get("derivation_applied") is not True
        or manifest.get("rows_or_stays_filtered") is not False
        or manifest.get("source_columns_changed_or_dropped") is not False
        or manifest.get("cohort_generated") is not False
        or manifest.get("time_blocking_applied") is not False
        or manifest.get("publication_ready") is not False
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Core-derived candidate manifest changed")
    private_dir = (
        config.reports_root
        / "private"
        / policy.audit_private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / policy.audit_review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_markdown = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_markdown)):
        raise HarmonizationError("Core-derived audit run already exists")
    output_schemas = {
        table: derived_schema(source.schemas[table], table, policy.version)
        for table in ("static", "dynamic")
    }
    output_summaries = manifest.get("outputs")
    if not isinstance(output_summaries, dict):
        raise HarmonizationError("Core-derived candidate outputs are unavailable")
    output_paths: dict[str, Path] = {}
    for table in ("static", "dynamic"):
        path = candidate_dir / f"{table}.parquet"
        summary = output_summaries.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Core-derived candidate table is unavailable")
        parquet = pq.ParquetFile(path)
        expected_rows = (
            policy.expected_static_rows if table == "static" else policy.expected_dynamic_rows
        )
        if (
            summary.get("sha256") != sha256_file(path)
            or summary.get("row_count") != expected_rows
            or parquet.metadata.num_rows != expected_rows
            or summary.get("schema_sha256") != _schema_digest(output_schemas[table])
            or not _parquet_schema_matches(parquet.schema_arrow, output_schemas[table])
        ):
            raise HarmonizationError("Core-derived candidate table changed")
        output_paths[table] = path

    tracker = VentilationTracker()
    dynamic_counts: Counter[str] = Counter()
    compared_rows: Counter[str] = Counter()
    compared_cells = 0
    input_dynamic = pq.ParquetFile(source.source_files["dynamic"])
    output_dynamic = pq.ParquetFile(output_paths["dynamic"])
    output_iterator = output_dynamic.iter_batches(
        batch_size=policy.rows_per_batch, use_threads=True
    )
    output_state: list[Any] = [None, 0]
    for input_batch in input_dynamic.iter_batches(
        batch_size=policy.rows_per_batch, use_threads=True
    ):
        observed = _next_slice(output_iterator, output_state, input_batch.num_rows)
        update_ventilation_tracker(input_batch, tracker)
        expected = transform_dynamic_batch(
            input_batch, output_schemas["dynamic"], dynamic_counts
        )
        compared_cells += _compare_batches(expected, observed, "dynamic")
        computed = observed.column(
            observed.schema.get_field_index("delta_p_computed")
        )
        finite = pc.fill_null(pc.is_finite(computed), False)
        invalid = pc.and_(
            finite,
            pc.or_(pc.less(computed, 0.0), pc.greater(computed, 60.0)),
        )
        if int(pc.sum(pc.cast(pc.fill_null(invalid, False), pa.int64())).as_py() or 0):
            raise HarmonizationError("Derived output retains invalid driving pressure")
        compared_rows["dynamic"] += input_batch.num_rows
        if progress is not None and compared_rows["dynamic"] % (
            policy.rows_per_batch * 20
        ) == 0:
            progress(
                f"derived_audit_progress table=dynamic rows={compared_rows['dynamic']}"
            )
    _require_output_exhausted(output_iterator, output_state)
    summaries = tracker.finish()
    if (
        compared_rows["dynamic"] != policy.expected_dynamic_rows
        or tracker.missing_identifier_count
        or tracker.hospital_conflict_count
        or tracker.supported_missing_time_count
        or tracker.nonfinite_time_count
    ):
        raise HarmonizationError("Dynamic derived audit input contract changed")
    if progress is not None:
        progress("derived_audit_table_complete=dynamic")

    static_counts: Counter[str] = Counter()
    seen_stays: set[str] = set()
    input_static = pq.ParquetFile(source.source_files["static"])
    output_static = pq.ParquetFile(output_paths["static"])
    output_iterator = output_static.iter_batches(
        batch_size=policy.rows_per_batch, use_threads=True
    )
    output_state = [None, 0]
    for input_batch in input_static.iter_batches(
        batch_size=policy.rows_per_batch, use_threads=True
    ):
        observed = _next_slice(output_iterator, output_state, input_batch.num_rows)
        expected = transform_static_batch(
            input_batch,
            output_schemas["static"],
            summaries,
            static_counts,
            seen_stays,
        )
        compared_cells += _compare_batches(expected, observed, "static")
        compared_rows["static"] += input_batch.num_rows
    _require_output_exhausted(output_iterator, output_state)
    if (
        compared_rows["static"] != policy.expected_static_rows
        or seen_stays != set(summaries)
    ):
        raise HarmonizationError("Static derived audit stay contract changed")
    if progress is not None:
        progress("derived_audit_table_complete=static")

    accounting = {
        "dynamic_row_count": dynamic_counts["row_count"],
        "hours_non_missing_count": dynamic_counts["hours_non_missing_count"],
        "hours_negative_count": dynamic_counts["hours_negative_count"],
        "driving_pressure_computable_count": dynamic_counts[
            "driving_pressure_computable_count"
        ],
        "driving_pressure_in_range_count": dynamic_counts[
            "driving_pressure_in_range_count"
        ],
        "driving_pressure_out_of_range_count": dynamic_counts[
            "driving_pressure_out_of_range_count"
        ],
        "static_row_count": static_counts["row_count"],
        **{
            key: static_counts[key]
            for key in (
                "hospital_mortality_false",
                "hospital_mortality_true",
                "hospital_mortality_missing",
                "hospital_mortality_conflict_count",
                "icu_mortality_false",
                "icu_mortality_true",
                "icu_mortality_missing",
                "icu_mortality_conflict_count",
                "ventilation_supported_timestamp_count_total",
                "ventilation_episode_count_total",
                "observed_mechanical_ventilation_ge_24h_true",
            )
        },
    }
    if (
        accounting != _expected_accounting(policy)
        or manifest.get("derivation_accounting") != accounting
    ):
        raise HarmonizationError("Derived accounting differs from build or contract")
    expected_cells = sum(
        compared_rows[table] * len(output_schemas[table])
        for table in ("static", "dynamic")
    )
    if compared_cells != expected_cells:
        raise HarmonizationError("Derived cell comparison accounting changed")
    for table in ("static", "dynamic"):
        if (
            sha256_file(output_paths[table])
            != output_summaries[table]["sha256"]
            or sha256_file(source.source_files[table])
            != source.release_manifest["files"][table]["sha256"]
        ):
            raise HarmonizationError("Derived candidate or cleaned input changed during audit")

    generated = utc_timestamp()
    blockers = (
        {
            "check": "core_derived_candidate_release_approved",
            "details": "The completely audited derived candidate requires explicit human approval before immutable release promotion.",
        },
    )
    metrics = {
        "compared_static_rows": compared_rows["static"],
        "compared_dynamic_rows": compared_rows["dynamic"],
        "compared_output_cell_count": compared_cells,
        "expected_output_cell_count": expected_cells,
        "static_output_column_count": len(output_schemas["static"]),
        "dynamic_output_column_count": len(output_schemas["dynamic"]),
    }
    private_payload = {
        "artifact": "asic_v3_core_derived_audit_private",
        "artifact_version": policy.audit_private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "derived_run_id": derived_run_id,
        "derived_manifest_sha256": sha256_file(manifest_path),
        "cleaned_release_manifest_sha256": sha256_file(source.release_manifest_path),
        "metrics": metrics,
        "derivation_accounting": accounting,
        "candidate_data_modified": False,
        "cleaned_release_modified": False,
        "publication_ready": False,
    }
    review_payload = {
        "artifact": "asic_v3_core_derived_audit_review",
        "artifact_version": policy.audit_review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "derived_run_id": derived_run_id,
        "cleaned_release_id": policy.cleaned_release_id,
        "core_derived_contract_version": policy.version,
        "overall_status": "pending_human_review",
        "technical_status": "pass",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "derivation_accounting": accounting,
        "every_input_column_preserved_exactly": True,
        "every_derived_value_recomputed_exactly": True,
        "candidate_data_modified": False,
        "cleaned_release_modified": False,
        "rows_or_stays_filtered": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(private_dir / "derived_audit_manifest.json", private_payload, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_markdown(review_markdown, _markdown(review_payload), 0o640)
    return CoreDerivedAuditResult(
        run_id=selected_run,
        derived_run_id=derived_run_id,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
