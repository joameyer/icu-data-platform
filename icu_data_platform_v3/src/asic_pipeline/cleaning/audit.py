from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.cleaning.pipeline import (
    CleaningConfig,
    _bool_count,
    _cleaned_schema,
    _inside_range,
    _load_rule_registry,
    _read_json,
    _schema_digest,
    load_cleaning_policy,
    load_harmonized_release_input,
    transform_cleaning_batch,
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
class CleaningAuditResult:
    run_id: str
    cleaning_run_id: str
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


def _rule_accounting(
    rule_counts: dict[str, Counter[str]],
    deferred_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    mask_counts: dict[str, Counter[str]],
) -> dict[str, Any]:
    return {
        "applied_scalar_rules": {
            key: dict(sorted(value.items())) for key, value in sorted(rule_counts.items())
        },
        "unit_unresolved_audit_only_rules": {
            key: dict(sorted(value.items())) for key, value in sorted(deferred_counts.items())
        },
        "nonfinite_only_fields": dict(sorted(nonfinite_counts.items())),
        "list_rules": {
            key: dict(sorted(value.items())) for key, value in sorted(list_counts.items())
        },
        "hospital_masks": {
            key: dict(sorted(value.items())) for key, value in sorted(mask_counts.items())
        },
    }


def _summary_metrics(
    rule_counts: dict[str, Counter[str]],
    deferred_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    mask_counts: dict[str, Counter[str]],
) -> dict[str, int]:
    return {
        "power_of_ten_correction_count": sum(
            count
            for counts in rule_counts.values()
            for key, count in counts.items()
            if key.startswith("corrected_factor_")
        ),
        "range_mask_count": sum(
            counts["masked_out_of_range"]
            + counts["masked_ambiguous_power_of_ten"]
            for counts in rule_counts.values()
        ),
        "nonfinite_mask_count": sum(
            counts["masked_nonfinite"] for counts in rule_counts.values()
        )
        + sum(nonfinite_counts.values()),
        "height_element_removal_count": sum(
            counts["removed_null_element_count"]
            + counts["removed_nonfinite_element_count"]
            + counts["removed_out_of_range_element_count"]
            for counts in list_counts.values()
        ),
        "hospital_mask_count": sum(
            counts["site_non_null_values_masked"] for counts in mask_counts.values()
        ),
        "unit_unresolved_audit_only_outside_range_count": sum(
            counts["audit_only_outside_legacy_range_count"]
            for counts in deferred_counts.values()
        ),
    }


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
            "# ASIC v3 cleaned candidate audit review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Cleaning candidate: `{payload['cleaning_run_id']}`",
            f"- Input harmonized release: `{payload['harmonized_release_id']}`",
            "- Technical audit status: **PASS**",
            "- Overall status: **PENDING HUMAN REVIEW**",
            "- Technical blocking findings: `0`",
            "- Human blocking findings: `1`",
            f"- Compared static rows: `{metrics['compared_static_rows']}`",
            f"- Compared dynamic rows: `{metrics['compared_dynamic_rows']}`",
            f"- Compared output cells: `{metrics['compared_output_cell_count']}`",
            f"- Power-of-ten corrections: `{metrics['power_of_ten_correction_count']}`",
            f"- Out-of-range values masked: `{metrics['range_mask_count']}`",
            f"- Nonfinite values masked: `{metrics['nonfinite_mask_count']}`",
            f"- Height elements removed: `{metrics['height_element_removal_count']}`",
            f"- UK06 mixed-scale values masked: `{metrics['hospital_mask_count']}`",
            f"- Unresolved-unit values outside legacy ranges, preserved audit-only: `{metrics['unit_unresolved_audit_only_outside_range_count']}`",
            "- Post-clean range violations: `0`",
            "- Rows or stays filtered: `false`",
            "- Columns dropped: `false`",
            "- Derived variables created: `false`",
            "- Publication ready: `false`",
            "",
            "## Human review gate",
            "",
            "Review the complete aggregate rule accounting, especially every power-of-ten correction factor and the unresolved-unit audit-only findings, before approving promotion to a cleaned release.",
            "",
            "## Applied scalar-rule accounting",
            "",
    ]
    for row in payload["applied_rule_summaries"]:
        factors = ", ".join(
            f"{factor}={count}" for factor, count in row["corrections_by_factor"].items()
        ) or "none"
        lines.append(
            f"- `{row['table']}.{row['variable']}`: kept `{row['kept_in_range']}`; "
            f"corrected `{row['corrected_count']}` ({factors}); masked no-match "
            f"`{row['masked_out_of_range']}`; masked ambiguous "
            f"`{row['masked_ambiguous_power_of_ten']}`; nonfinite "
            f"`{row['masked_nonfinite']}`"
        )
    lines.extend(("", "## Unresolved-unit audit-only accounting", ""))
    for row in payload["deferred_rule_summaries"]:
        lines.append(
            f"- `{row['table']}.{row['variable']}`: finite "
            f"`{row['finite_value_count']}`; outside legacy range but preserved "
            f"`{row['audit_only_outside_legacy_range_count']}`"
        )
    lines.extend(("", "## List and hospital-scoped accounting", ""))
    for row in payload["list_rule_summaries"]:
        lines.append(
            f"- `{row['table']}.{row['variable']}`: retained elements "
            f"`{row['retained_element_count']}`; removed null "
            f"`{row['removed_null_element_count']}`; removed nonfinite "
            f"`{row['removed_nonfinite_element_count']}`; removed out-of-range "
            f"`{row['removed_out_of_range_element_count']}`; emptied lists "
            f"`{row['cells_empty_after_cleaning']}`"
        )
    for row in payload["hospital_mask_summaries"]:
        lines.append(
            f"- `{row['hospital']}` `{row['table']}.{row['variable']}`: "
            f"site rows `{row['site_row_count']}`; non-missing values masked "
            f"`{row['site_non_null_values_masked']}`"
        )
    lines.append("")
    return "\n".join(lines)


def run_cleaning_candidate_audit(
    config: CleaningConfig,
    cleaning_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> CleaningAuditResult:
    policy = load_cleaning_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved cleaning audit is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run) or not RUN_ID_PATTERN.fullmatch(
        cleaning_run_id
    ):
        raise HarmonizationError("Invalid cleaning audit run ID")
    source = load_harmonized_release_input(config, policy)
    applied, deferred = _load_rule_registry(policy, source.frozen_schemas)
    output_schemas = {
        table: _cleaned_schema(source.frozen_schemas[table], policy.harmonized_contract_version)
        for table in ("static", "dynamic")
    }
    candidate_dir = config.data_root / policy.candidate_directory_name / cleaning_run_id
    manifest_path = candidate_dir / "cleaning_manifest.json"
    manifest = _read_json(manifest_path, "Cleaning candidate manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Cleaning candidate manifest is invalid")
    if (
        manifest.get("artifact") != "asic_v3_cleaning_candidate"
        or manifest.get("artifact_version") != policy.candidate_artifact_version
        or manifest.get("run_id") != cleaning_run_id
        or manifest.get("status") != "nonpublishable_requires_complete_cleaning_audit"
        or manifest.get("lineage", {}).get("harmonized_release_id")
        != policy.harmonized_release_id
        or manifest.get("lineage", {}).get("harmonized_release_manifest_sha256")
        != sha256_file(source.release_manifest_path)
        or manifest.get("lineage", {}).get("cleaning_policy_sha256")
        != sha256_file(policy.source_path)
        or manifest.get("cleaning_applied") is not True
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not False
        or manifest.get("columns_dropped") is not False
    ):
        raise HarmonizationError("Cleaning candidate manifest changed")
    private_dir = (
        config.reports_root
        / "private"
        / policy.audit_private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / policy.audit_review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Cleaning audit run already exists")
    rule_counts = {rule.rule_id: Counter() for rule in applied.values()}
    deferred_counts = {rule.rule_id: Counter() for rule in deferred.values()}
    nonfinite_counts: Counter[str] = Counter()
    list_counts = {rule.rule_id: Counter() for rule in policy.list_rules}
    mask_counts = {rule.rule_id: Counter() for rule in policy.hospital_masks}
    compared_rows: Counter[str] = Counter()
    compared_cells = 0
    all_missing_non_null: Counter[str] = Counter()
    post_clean_invalid_count = 0
    post_clean_nonfinite_count = 0
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Cleaning candidate outputs are unavailable")
    for table in ("static", "dynamic"):
        output_path = candidate_dir / f"{table}.parquet"
        output_summary = outputs.get(table)
        if not isinstance(output_summary, dict) or not output_path.is_file():
            raise HarmonizationError("Cleaning candidate table is unavailable")
        output_parquet = pq.ParquetFile(output_path)
        if (
            output_summary.get("sha256") != sha256_file(output_path)
            or output_summary.get("row_count") != dict(policy.expected_rows)[table]
            or output_summary.get("schema_sha256") != _schema_digest(output_schemas[table])
            or not _parquet_schema_matches(output_parquet.schema_arrow, output_schemas[table])
        ):
            raise HarmonizationError("Cleaning candidate table changed")
        input_path = source.source_files[table]
        expected_source_hash = source.release_manifest["files"][table]["sha256"]
        if sha256_file(input_path) != expected_source_hash:
            raise HarmonizationError("Harmonized release changed before cleaning audit")
        input_parquet = pq.ParquetFile(input_path)
        input_iterator = input_parquet.iter_batches(
            batch_size=policy.rows_per_batch, use_threads=True
        )
        output_iterator = output_parquet.iter_batches(
            batch_size=policy.rows_per_batch, use_threads=True
        )
        output_state: list[Any] = [None, 0]
        for input_batch in input_iterator:
            output_batch = _next_slice(
                output_iterator, output_state, input_batch.num_rows
            )
            expected_batch = transform_cleaning_batch(
                input_batch,
                table,
                output_schemas[table],
                policy,
                applied,
                deferred,
                rule_counts,
                deferred_counts,
                nonfinite_counts,
                list_counts,
                mask_counts,
            )
            for field in output_schemas[table]:
                index = output_batch.schema.get_field_index(field.name)
                expected_array = expected_batch.column(index)
                observed_array = output_batch.column(index)
                if not expected_array.equals(observed_array):
                    raise HarmonizationError(
                        f"Cleaned value mismatch: {table}.{field.name}"
                    )
                compared_cells += input_batch.num_rows
                if field.type == pa.float64():
                    post_clean_nonfinite_count += _bool_count(
                        pc.and_(pc.is_valid(observed_array), pc.invert(pc.fill_null(pc.is_finite(observed_array), False)))
                    )
                rule = applied.get((table, field.name))
                if rule is not None:
                    finite = pc.fill_null(pc.is_finite(observed_array), False)
                    post_clean_invalid_count += _bool_count(
                        pc.and_(finite, pc.invert(_inside_range(observed_array, rule)))
                    )
                if field.name in policy.globally_all_missing:
                    all_missing_non_null[field.name] += len(observed_array) - observed_array.null_count
            compared_rows[table] += input_batch.num_rows
            if progress is not None and compared_rows[table] % (policy.rows_per_batch * 20) == 0:
                progress(f"cleaning_audit_progress table={table} rows={compared_rows[table]}")
        _require_output_exhausted(output_iterator, output_state)
        if compared_rows[table] != dict(policy.expected_rows)[table]:
            raise HarmonizationError("Cleaning audit row count changed")
        if sha256_file(input_path) != expected_source_hash:
            raise HarmonizationError("Harmonized release changed during cleaning audit")
        if progress is not None:
            progress(f"cleaning_audit_table_complete={table}")
    if any(all_missing_non_null.values()):
        raise HarmonizationError("A globally all-missing field changed during cleaning")
    if post_clean_invalid_count or post_clean_nonfinite_count:
        raise HarmonizationError("Cleaned output retains a governed invalid numeric value")
    accounting = _rule_accounting(
        rule_counts,
        deferred_counts,
        nonfinite_counts,
        list_counts,
        mask_counts,
    )
    if manifest.get("rule_accounting") != accounting:
        raise HarmonizationError("Cleaning rule accounting differs from complete audit")
    summaries = _summary_metrics(
        rule_counts,
        deferred_counts,
        nonfinite_counts,
        list_counts,
        mask_counts,
    )
    applied_rule_summaries = []
    for (table, variable), rule in sorted(applied.items()):
        counts = rule_counts[rule.rule_id]
        factor_counts = {
            key.removeprefix("corrected_factor_"): value
            for key, value in sorted(counts.items())
            if key.startswith("corrected_factor_") and value
        }
        applied_rule_summaries.append(
            {
                "rule_id": rule.rule_id,
                "table": table,
                "variable": variable,
                "kept_in_range": counts["kept_in_range"],
                "corrected_count": sum(factor_counts.values()),
                "corrections_by_factor": factor_counts,
                "masked_out_of_range": counts["masked_out_of_range"],
                "masked_ambiguous_power_of_ten": counts[
                    "masked_ambiguous_power_of_ten"
                ],
                "masked_nonfinite": counts["masked_nonfinite"],
            }
        )
    deferred_rule_summaries = [
        {
            "rule_id": rule.rule_id,
            "table": table,
            "variable": variable,
            "finite_value_count": deferred_counts[rule.rule_id][
                "finite_value_count"
            ],
            "audit_only_outside_legacy_range_count": deferred_counts[
                rule.rule_id
            ]["audit_only_outside_legacy_range_count"],
        }
        for (table, variable), rule in sorted(deferred.items())
    ]
    list_rule_summaries = [
        {
            "rule_id": rule.rule_id,
            "table": rule.table,
            "variable": rule.variable,
            **{
                key: list_counts[rule.rule_id][key]
                for key in (
                    "retained_element_count",
                    "removed_null_element_count",
                    "removed_nonfinite_element_count",
                    "removed_out_of_range_element_count",
                    "cells_empty_after_cleaning",
                )
            },
        }
        for rule in policy.list_rules
    ]
    hospital_mask_summaries = [
        {
            "rule_id": rule.rule_id,
            "hospital": rule.hospital,
            "table": rule.table,
            "variable": rule.variable,
            "site_row_count": mask_counts[rule.rule_id]["site_row_count"],
            "site_non_null_values_masked": mask_counts[rule.rule_id][
                "site_non_null_values_masked"
            ],
        }
        for rule in policy.hospital_masks
    ]
    generated = utc_timestamp()
    blockers = (
        {
            "check": "cleaned_candidate_release_approved",
            "details": "The completely audited cleaned candidate requires explicit human approval before promotion to a cleaned release.",
        },
    )
    metrics = {
        "compared_static_rows": compared_rows["static"],
        "compared_dynamic_rows": compared_rows["dynamic"],
        "compared_output_cell_count": compared_cells,
        "applied_scalar_rule_count": len(applied),
        "unit_unresolved_audit_only_rule_count": len(deferred),
        **summaries,
        "post_clean_range_violation_count": post_clean_invalid_count,
        "post_clean_nonfinite_count": post_clean_nonfinite_count,
    }
    private_payload = {
        "artifact": "asic_v3_cleaning_audit_private",
        "artifact_version": policy.audit_private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaning_run_id": cleaning_run_id,
        "cleaning_manifest_sha256": sha256_file(manifest_path),
        "harmonized_release_manifest_sha256": sha256_file(source.release_manifest_path),
        "metrics": metrics,
        "rule_accounting": accounting,
        "harmonized_data_modified": False,
        "cleaning_candidate_modified": False,
        "publication_ready": False,
    }
    review_payload = {
        "artifact": "asic_v3_cleaning_audit_review",
        "artifact_version": policy.audit_review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaning_run_id": cleaning_run_id,
        "harmonized_release_id": policy.harmonized_release_id,
        "overall_status": "pending_human_review",
        "technical_status": "pass",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "applied_rule_summaries": applied_rule_summaries,
        "deferred_rule_summaries": deferred_rule_summaries,
        "list_rule_summaries": list_rule_summaries,
        "hospital_mask_summaries": hospital_mask_summaries,
        "harmonized_data_modified": False,
        "cleaning_candidate_modified": False,
        "rows_or_stays_filtered": False,
        "columns_dropped": False,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(private_dir / "cleaning_audit_manifest.json", private_payload, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_markdown(review_md, _markdown(review_payload), 0o640)
    return CleaningAuditResult(
        run_id=selected_run,
        cleaning_run_id=cleaning_run_id,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
