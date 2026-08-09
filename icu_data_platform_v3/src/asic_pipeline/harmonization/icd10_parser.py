from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.icd10_contract import ReviewedICD10Contract


APPROVED_HOSPITALS = frozenset(
    {
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    }
)


@dataclass(frozen=True)
class ReviewedICD10PrivateEvidence:
    audit_run_id: str
    incomplete_components: tuple[tuple[str, str], ...]
    source_path: Path

    def approves_incomplete_component(self, hospital: str, value: str) -> bool:
        return (hospital, value) in self.incomplete_components


@dataclass
class ICD10HarmonizationMetrics:
    hospital: str
    input_cell_count: int = 0
    source_schema_absent_count: int = 0
    literal_empty_missing_count: int = 0
    approved_textual_missing_count: int = 0
    parsed_cell_count: int = 0
    unresolved_cell_count: int = 0
    single_code_cell_count: int = 0
    comma_delimited_cell_count: int = 0
    source_component_count: int = 0
    valid_component_count_before_deduplication: int = 0
    retained_unique_valid_component_count: int = 0
    trailing_empty_component_removed_count: int = 0
    cells_with_trailing_empty_removed_count: int = 0
    duplicate_component_removed_count: int = 0
    cells_with_duplicates_removed_count: int = 0
    reviewed_incomplete_component_removed_count: int = 0
    cells_with_reviewed_incomplete_removed_count: int = 0
    unresolved_source_component_count: int = 0
    unapproved_empty_component_count: int = 0
    unapproved_noncode_component_count: int = 0
    empty_list_after_rules_count: int = 0

    @property
    def cell_accounting_is_conserved(self) -> bool:
        return self.input_cell_count == (
            self.source_schema_absent_count
            + self.literal_empty_missing_count
            + self.approved_textual_missing_count
            + self.parsed_cell_count
            + self.unresolved_cell_count
        )

    @property
    def component_accounting_is_conserved(self) -> bool:
        return self.source_component_count == (
            self.retained_unique_valid_component_count
            + self.duplicate_component_removed_count
            + self.trailing_empty_component_removed_count
            + self.reviewed_incomplete_component_removed_count
            + self.unresolved_source_component_count
        )


@dataclass(frozen=True)
class HarmonizedStaticICD10Batch:
    contract: ReviewedICD10Contract
    hospital: str
    source_text: pa.LargeStringArray
    codes: pa.ListArray
    parse_status: pa.StringArray
    metrics: ICD10HarmonizationMetrics

    @property
    def publication_blocked(self) -> bool:
        return self.metrics.unresolved_cell_count > 0

    def require_resolved(self) -> None:
        if self.publication_blocked:
            raise HarmonizationError(
                "Reviewed ICD-10 parser has unresolved source components"
            )
        if not self.metrics.cell_accounting_is_conserved:
            raise HarmonizationError("Reviewed ICD-10 parser failed cell accounting")
        if not self.metrics.component_accounting_is_conserved:
            raise HarmonizationError(
                "Reviewed ICD-10 parser failed component accounting"
            )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def load_reviewed_icd10_private_evidence(
    path: str | Path,
    contract: ReviewedICD10Contract,
) -> ReviewedICD10PrivateEvidence:
    contract.require_parser_active()
    if contract.version != "0.2" or not contract.allow_private_review_evidence_reads:
        raise HarmonizationError("ICD-10 private evidence reads are not approved")
    if contract.detail_audit_run_id is None:
        raise HarmonizationError("ICD-10 detail-audit evidence is not bound")
    source = Path(path).expanduser().resolve()
    audit_run_id = contract.detail_audit_run_id
    if (
        source.name != "examples.parquet"
        or source.parent.name != audit_run_id
        or source.parent.parent.name != "icd10_component_detail_audit"
    ):
        raise HarmonizationError("ICD-10 private evidence path is not approved")
    manifest_path = source.parent / "icd10_component_detail_audit_manifest.json"
    if len(source.parents) < 4:
        raise HarmonizationError("ICD-10 private evidence location is invalid")
    review_path = (
        source.parents[3]
        / "review"
        / "icd10_component_detail_audit"
        / f"{audit_run_id}.json"
    )
    if not source.is_file() or not manifest_path.is_file() or not review_path.is_file():
        raise HarmonizationError("ICD-10 private review evidence is incomplete")
    manifest = _read_json(manifest_path, "ICD-10 detail private manifest")
    review = _read_json(review_path, "ICD-10 detail sanitized review")
    dataset_context = source.parents[3].name
    expected_class = contract.incomplete_component_private_detail_class
    expected_hospital = contract.incomplete_component_hospital
    expected_count = contract.expected_reviewed_incomplete_component_count
    if (
        manifest.get("artifact") != "asic_v3_icd10_component_detail_audit_private"
        or manifest.get("artifact_version") != "0.1"
        or dataset_context not in {"demo", "production"}
        or manifest.get("dataset_context") != dataset_context
        or manifest.get("inputs", {}).get("icd10_component_audit_run_id")
        != "20260805T134245Z"
        or review.get("artifact") != "asic_v3_icd10_component_detail_audit_review"
        or review.get("artifact_version") != "0.1"
        or review.get("dataset_context") != dataset_context
        or review.get("inputs", {}).get("icd10_component_audit_run_id")
        != "20260805T134245Z"
        or review.get("metrics", {}).get("technical_blocking_finding_count") != 0
        or review.get("metrics", {}).get("incomplete_component_candidate_count")
        != expected_count
        or expected_class is None
        or expected_hospital is None
        or expected_count != 1
    ):
        raise HarmonizationError("ICD-10 private review evidence is invalid")
    try:
        rows = pq.read_table(source).to_pylist()
    except Exception as exc:
        raise HarmonizationError("ICD-10 private examples cannot be read") from exc
    selected = [
        row
        for row in rows
        if row.get("detail_class") == expected_class
        and row.get("hospital") == expected_hospital
    ]
    if len(selected) != 1:
        raise HarmonizationError("ICD-10 private incomplete evidence is ambiguous")
    raw_component = selected[0].get("raw_component")
    cell_count = selected[0].get("cell_count")
    if (
        not isinstance(raw_component, str)
        or not raw_component
        or not isinstance(cell_count, int)
        or isinstance(cell_count, bool)
        or cell_count != expected_count
    ):
        raise HarmonizationError("ICD-10 private incomplete evidence is invalid")
    return ReviewedICD10PrivateEvidence(
        audit_run_id=audit_run_id,
        incomplete_components=((expected_hospital, raw_component),),
        source_path=source,
    )


def _status(
    raw: str,
    delimiter: str,
    trailing_removed: int,
    incomplete_removed: int,
    duplicates_removed: int,
) -> str:
    parts = ["comma_split" if delimiter in raw else "single_code_list"]
    if trailing_removed:
        parts.append("trailing_empty_removed")
    if incomplete_removed:
        parts.append("reviewed_incomplete_removed")
    if duplicates_removed:
        parts.append("ordered_duplicates_removed")
    return "+".join(parts)


def harmonize_reviewed_static_icd10_batch(
    source: pa.Array | pa.ChunkedArray,
    hospital: str,
    contract: ReviewedICD10Contract,
    private_evidence: ReviewedICD10PrivateEvidence,
) -> HarmonizedStaticICD10Batch:
    contract.require_parser_active()
    if contract.version != "0.2" or not contract.allow_synthetic_batch_transform:
        raise HarmonizationError("ICD-10 bounded parser execution is not approved")
    if hospital not in APPROVED_HOSPITALS:
        raise HarmonizationError("ICD-10 parser hospital scope is not approved")
    if private_evidence.audit_run_id != contract.detail_audit_run_id:
        raise HarmonizationError("ICD-10 private evidence run does not match contract")
    if isinstance(source, pa.ChunkedArray):
        source = source.combine_chunks()
    if not pa.types.is_string(source.type) and not pa.types.is_large_string(source.type):
        raise HarmonizationError("ICD-10 parser requires lossless raw strings")
    if contract.component_code_candidate_regex is None:
        raise HarmonizationError("ICD-10 component grammar is unavailable")
    code_pattern = re.compile(contract.component_code_candidate_regex)
    metrics = ICD10HarmonizationMetrics(
        hospital=hospital,
        input_cell_count=len(source),
    )
    source_text: list[str | None] = []
    codes: list[list[str] | None] = []
    statuses: list[str] = []
    missing = contract.missing_decision_for(hospital)
    maximum_trailing = contract.maximum_trailing_empty_components_per_cell
    for raw in source.to_pylist():
        if raw is None:
            metrics.source_schema_absent_count += 1
            source_text.append(None)
            codes.append(None)
            statuses.append("source_schema_absent")
            continue
        if not isinstance(raw, str):
            raise HarmonizationError("ICD-10 parser encountered a non-string source cell")
        if raw == "":
            metrics.literal_empty_missing_count += 1
            source_text.append(None)
            codes.append(None)
            statuses.append("literal_empty_missing")
            continue
        if missing is not None and missing.matches(raw):
            metrics.approved_textual_missing_count += 1
            source_text.append(None)
            codes.append(None)
            statuses.append("approved_textual_missing")
            continue
        source_text.append(raw)
        original = raw.split(contract.delimiter)
        components = [item.strip() for item in original]
        metrics.source_component_count += len(components)
        unapproved_empty = 0
        unapproved_noncode = 0
        trailing_removed = 0
        incomplete_removed = 0
        valid: list[str] = []
        for index, component in enumerate(components):
            if component == "":
                approved_trailing = bool(
                    index == len(components) - 1
                    and contract.approved_trailing_empty_count(hospital) > 0
                    and maximum_trailing == 1
                    and trailing_removed == 0
                )
                if approved_trailing:
                    trailing_removed += 1
                else:
                    unapproved_empty += 1
                continue
            if code_pattern.fullmatch(component):
                valid.append(component)
                continue
            if private_evidence.approves_incomplete_component(hospital, component):
                incomplete_removed += 1
                continue
            unapproved_noncode += 1
        if unapproved_empty or unapproved_noncode:
            metrics.unresolved_cell_count += 1
            metrics.unresolved_source_component_count += len(components)
            metrics.unapproved_empty_component_count += unapproved_empty
            metrics.unapproved_noncode_component_count += unapproved_noncode
            codes.append(None)
            statuses.append("unresolved")
            continue
        seen: set[str] = set()
        retained: list[str] = []
        for component in valid:
            if component not in seen:
                seen.add(component)
                retained.append(component)
        duplicates_removed = len(valid) - len(retained)
        if not retained:
            metrics.unresolved_cell_count += 1
            metrics.unresolved_source_component_count += len(components)
            metrics.empty_list_after_rules_count += 1
            codes.append(None)
            statuses.append("unresolved")
            continue
        metrics.parsed_cell_count += 1
        metrics.single_code_cell_count += int(contract.delimiter not in raw)
        metrics.comma_delimited_cell_count += int(contract.delimiter in raw)
        metrics.valid_component_count_before_deduplication += len(valid)
        metrics.retained_unique_valid_component_count += len(retained)
        metrics.trailing_empty_component_removed_count += trailing_removed
        metrics.cells_with_trailing_empty_removed_count += int(trailing_removed > 0)
        metrics.duplicate_component_removed_count += duplicates_removed
        metrics.cells_with_duplicates_removed_count += int(duplicates_removed > 0)
        metrics.reviewed_incomplete_component_removed_count += incomplete_removed
        metrics.cells_with_reviewed_incomplete_removed_count += int(
            incomplete_removed > 0
        )
        codes.append(retained)
        statuses.append(
            _status(
                raw,
                contract.delimiter,
                trailing_removed,
                incomplete_removed,
                duplicates_removed,
            )
        )
    result = HarmonizedStaticICD10Batch(
        contract=contract,
        hospital=hospital,
        source_text=pa.array(source_text, type=pa.large_string()),
        codes=pa.array(codes, type=pa.list_(pa.large_string())),
        parse_status=pa.array(statuses, type=pa.string()),
        metrics=metrics,
    )
    if not metrics.cell_accounting_is_conserved:
        raise HarmonizationError("Reviewed ICD-10 parser failed cell accounting")
    if not metrics.component_accounting_is_conserved:
        raise HarmonizationError("Reviewed ICD-10 parser failed component accounting")
    return result


def require_full_reviewed_icd10_evidence_counts(
    metrics: Iterable[ICD10HarmonizationMetrics],
    contract: ReviewedICD10Contract,
) -> None:
    contract.require_parser_active()
    values = tuple(metrics)
    trailing_by_hospital = Counter(
        {
            hospital: sum(
                item.trailing_empty_component_removed_count
                for item in values
                if item.hospital == hospital
            )
            for hospital in APPROVED_HOSPITALS
        }
    )
    observed = {
        "input_cell_count": sum(item.input_cell_count for item in values),
        "approved_textual_missing_count": sum(
            item.approved_textual_missing_count for item in values
        ),
        "source_component_count": sum(item.source_component_count for item in values),
        "valid_component_count_before_deduplication": sum(
            item.valid_component_count_before_deduplication for item in values
        ),
        "reviewed_incomplete_component_removed_count": sum(
            item.reviewed_incomplete_component_removed_count for item in values
        ),
        "duplicate_component_removed_count": sum(
            item.duplicate_component_removed_count for item in values
        ),
        "retained_unique_valid_component_count": sum(
            item.retained_unique_valid_component_count for item in values
        ),
        "unresolved_cell_count": sum(item.unresolved_cell_count for item in values),
        "cells_with_trailing_empty_removed_count": sum(
            item.cells_with_trailing_empty_removed_count for item in values
        ),
    }
    expected = {
        "input_cell_count": contract.scanned_row_count,
        "approved_textual_missing_count": contract.approved_textual_missing_cell_count,
        "source_component_count": contract.expected_source_component_count,
        "valid_component_count_before_deduplication": (
            contract.expected_valid_component_count_before_deduplication
        ),
        "reviewed_incomplete_component_removed_count": (
            contract.expected_reviewed_incomplete_component_count
        ),
        "duplicate_component_removed_count": contract.expected_duplicate_component_count,
        "retained_unique_valid_component_count": contract.expected_retained_component_count,
        "unresolved_cell_count": 0,
        "cells_with_trailing_empty_removed_count": (
            contract.expected_trailing_empty_cell_count
        ),
    }
    if observed != expected:
        raise HarmonizationError(
            "Full ICD-10 harmonization counts do not match reviewed evidence"
        )
    if trailing_by_hospital != Counter(
        dict(contract.trailing_empty_evidence_by_hospital)
    ):
        raise HarmonizationError(
            "Full ICD-10 trailing-empty counts do not match reviewed hospital scopes"
        )
    if any(
        not item.cell_accounting_is_conserved
        or not item.component_accounting_is_conserved
        for item in values
    ):
        raise HarmonizationError("Full ICD-10 harmonization accounting failed")
