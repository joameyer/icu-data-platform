from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.icd10_contract import (
    ReviewedICD10Contract,
    load_reviewed_icd10_contract,
)
from asic_pipeline.harmonization.review_policy import (
    HarmonizationReviewConfig,
    load_harmonization_review_config,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


EXPECTED_HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)
TECHNICAL_CHECKS = frozenset(
    {
        "icd10_notation_evidence_valid",
        "reviewed_icd10_contract_valid",
        "icd10_source_occurrences_complete",
        "ingested_static_hashes_match_manifests",
        "icd10_source_bindings_valid",
        "icd10_source_cells_are_strings",
        "icd10_row_accounting_conserved",
        "icd10_notation_counts_reproduced",
        "icd10_approved_missing_evidence_reproduced",
        "icd10_component_accounting_conserved",
        "no_production_data_artifacts_generated",
    }
)
CHECK_DETAILS = {
    "icd10_notation_evidence_valid": (
        "The immutable private and sanitized notation-audit artifacts must match the approved run, context, versions, and evidence counts."
    ),
    "reviewed_icd10_contract_valid": (
        "Reviewed ICD-10 contract 0.1 and its approved file hash must remain unchanged and fail closed."
    ),
    "icd10_source_occurrences_complete": (
        "Exactly one immutable static ICD-10 source occurrence is required for every approved hospital."
    ),
    "ingested_static_hashes_match_manifests": (
        "Every scanned static Parquet must match its immutable ingestion-manifest hash."
    ),
    "icd10_source_bindings_valid": (
        "Each selected physical field must match its lossless raw-name and occurrence metadata."
    ),
    "icd10_source_cells_are_strings": (
        "Every non-null source cell must remain a lossless raw string."
    ),
    "icd10_row_accounting_conserved": (
        "Every source row must be counted exactly once as source-null, literal-empty, or non-empty."
    ),
    "icd10_notation_counts_reproduced": (
        "The complete scan must reproduce the approved notation audit's row, non-empty, missing, comma, and single-code counts."
    ),
    "icd10_approved_missing_evidence_reproduced": (
        "The approved UK03 textual-missing rule must match exactly 24 cells and no other hospital-scoped missing rule may appear."
    ),
    "icd10_component_accounting_conserved": (
        "Every diagnosis-bearing cell and every proposed list component must be accounted for exactly once."
    ),
    "icd10_empty_components_resolved": (
        "Leading, trailing, or repeated delimiters that produce empty components require explicit resolution before parser activation."
    ),
    "icd10_unexpected_component_syntax_reviewed": (
        "Components outside the bounded code-candidate grammar require private review; the audit does not assign diagnosis semantics."
    ),
    "icd10_component_evidence_approved": (
        "The complete component-count, whitespace, duplicate, and list-length evidence requires explicit human approval."
    ),
    "icd10_canonical_list_contract_activated": (
        "The ordered list representation remains disabled until this audit is reviewed and a new activation decision is recorded."
    ),
    "no_production_data_artifacts_generated": (
        "The audit may write reports only and must not generate harmonized, cleaned, derived, or pooled data."
    ),
}


@dataclass(frozen=True)
class ICD10ComponentAuditPolicy:
    version: str
    notation_audit_basis_run_id: str
    required_notation_private_artifact_version: str
    required_notation_review_artifact_version: str
    required_static_contract_private_artifact_version: str
    required_ingestion_manifest_version: str
    reviewed_contract_path: Path
    reviewed_contract_sha256: str
    component_code_candidate_regex: str
    maximum_private_examples_per_hospital_class: int
    rows_per_batch: int
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class ICD10ComponentAuditConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root

    @property
    def ingested_root(self) -> Path:
        return self.harmonization_review.schema_tokens.ingestion.output_root


@dataclass(frozen=True)
class ICD10ComponentAuditResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_patterns: tuple[dict[str, Any], ...]
    private_examples: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass
class _HospitalStats:
    row_count: int = 0
    source_null_count: int = 0
    literal_empty_count: int = 0
    nonempty_count: int = 0
    nonstring_count: int = 0
    approved_missing_count: int = 0
    parsed_cell_count: int = 0
    comma_cell_count: int = 0
    single_component_cell_count: int = 0
    multi_component_cell_count: int = 0
    component_count: int = 0
    code_candidate_component_count: int = 0
    unexpected_component_count: int = 0
    empty_component_count: int = 0
    cells_with_empty_components: int = 0
    components_with_trimmed_whitespace: int = 0
    cells_with_duplicate_components: int = 0
    duplicate_component_count: int = 0
    list_lengths: Counter[int] | None = None
    component_classes: Counter[str] | None = None
    examples: dict[str, Counter[str]] | None = None
    source_stream_hash: Any = None
    proposed_source_stream_hash: Any = None

    @classmethod
    def create(cls) -> _HospitalStats:
        return cls(
            list_lengths=Counter(),
            component_classes=Counter(),
            examples=defaultdict(Counter),
            source_stream_hash=hashlib.sha256(),
            proposed_source_stream_hash=hashlib.sha256(),
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(mapping: dict[str, Any], key: str, location: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location}.{key} must be a positive integer")
    return value


def load_icd10_component_audit_config(
    path: str | Path,
) -> ICD10ComponentAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "ICD-10 component-audit configuration")
    policy_path = resolve_path(
        required_string(raw, "icd10_component_audit_policy", "config"), source
    )
    return ICD10ComponentAuditConfig(harmonization, policy_path)


def load_icd10_component_audit_policy(
    path: str | Path,
) -> ICD10ComponentAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "ICD-10 component-audit policy")
    if raw.get("icd10_component_audit_policy_version") != "0.1":
        raise ConfigurationError("ICD-10 component-audit policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_icd10_component_evidence":
        raise ConfigurationError("ICD-10 component-audit policy is not approved")
    basis = required_string(raw, "icd10_notation_audit_basis_run_id", "ICD-10 component audit")
    if basis != "20260805T131735Z":
        raise ConfigurationError("ICD-10 notation-audit basis changed")
    contract_path = resolve_path(
        required_string(raw, "reviewed_icd10_contract", "ICD-10 component audit"),
        source,
    )
    contract_hash = required_string(
        raw, "reviewed_icd10_contract_sha256", "ICD-10 component audit"
    )
    if (
        contract_hash
        != "f6edf01c21587ae124fa0631cb5cc8ac4e297ef8a2e1e77a966a51ef83d24da1"
        or not contract_path.is_file()
        or sha256_file(contract_path) != contract_hash
    ):
        raise ConfigurationError("Reviewed ICD-10 contract hash changed")
    classification = _mapping(raw.get("classification"), "ICD-10 component classification")
    regex = required_string(
        classification,
        "component_code_candidate_regex",
        "ICD-10 component classification",
    )
    try:
        re.compile(regex)
    except re.error as exc:
        raise ConfigurationError("ICD-10 component regex is invalid") from exc
    scope = _mapping(raw.get("scope"), "ICD-10 component audit.scope")
    expected_scope = {
        "allow_notation_private_evidence_reads": True,
        "allow_static_contract_private_evidence_reads": True,
        "allow_ingested_static_icd10_reads": True,
        "allow_candidate_split_evidence": True,
        "allow_report_writes": True,
        "allow_raw_csv_reads": False,
        "allow_other_ingested_column_reads": False,
        "allow_value_transforms": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_parser_activation": False,
        "allow_union_schema_approval": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("ICD-10 component audit must remain read-only")
    scan = _mapping(raw.get("scan"), "ICD-10 component audit.scan")
    reporting = _mapping(raw.get("reporting"), "ICD-10 component audit.reporting")
    if reporting.get("private_directory_mode") != "0700" or reporting.get(
        "private_file_mode"
    ) != "0600":
        raise ConfigurationError("ICD-10 component private report modes changed")
    return ICD10ComponentAuditPolicy(
        version="0.1",
        notation_audit_basis_run_id=basis,
        required_notation_private_artifact_version=required_string(
            raw, "required_notation_private_artifact_version", "ICD-10 component audit"
        ),
        required_notation_review_artifact_version=required_string(
            raw, "required_notation_review_artifact_version", "ICD-10 component audit"
        ),
        required_static_contract_private_artifact_version=required_string(
            raw,
            "required_static_contract_private_artifact_version",
            "ICD-10 component audit",
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "ICD-10 component audit"
        ),
        reviewed_contract_path=contract_path,
        reviewed_contract_sha256=contract_hash,
        component_code_candidate_regex=regex,
        maximum_private_examples_per_hospital_class=_positive_int(
            classification,
            "maximum_private_examples_per_hospital_class",
            "ICD-10 component classification",
        ),
        rows_per_batch=_positive_int(scan, "rows_per_batch", "ICD-10 component audit.scan"),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "ICD-10 component audit.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "ICD-10 component audit.reporting"
        ),
        source_path=source,
    )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _update_hash(digest: Any, raw: str) -> None:
    encoded = raw.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def _consume(
    stats: _HospitalStats,
    raw: object,
    hospital: str,
    contract: ReviewedICD10Contract,
    code_pattern: re.Pattern[str],
) -> None:
    stats.row_count += 1
    if raw is None:
        stats.source_null_count += 1
        return
    if not isinstance(raw, str):
        stats.nonempty_count += 1
        stats.nonstring_count += 1
        return
    if raw == "":
        stats.literal_empty_count += 1
        return
    stats.nonempty_count += 1
    missing = contract.missing_decision_for(hospital)
    if missing is not None and missing.matches(raw):
        stats.approved_missing_count += 1
        return

    stats.parsed_cell_count += 1
    stats.comma_cell_count += int(contract.delimiter in raw)
    _update_hash(stats.source_stream_hash, raw)
    _update_hash(stats.proposed_source_stream_hash, raw)
    original_components = raw.split(contract.delimiter)
    components = [item.strip() for item in original_components]
    assert stats.list_lengths is not None
    assert stats.component_classes is not None
    assert stats.examples is not None
    stats.list_lengths[len(components)] += 1
    stats.component_count += len(components)
    if len(components) == 1:
        stats.single_component_cell_count += 1
    else:
        stats.multi_component_cell_count += 1
    empty_in_cell = False
    for original, component in zip(original_components, components, strict=True):
        stats.components_with_trimmed_whitespace += int(original != component)
        if component == "":
            stats.empty_component_count += 1
            stats.component_classes["empty_component"] += 1
            empty_in_cell = True
        elif code_pattern.fullmatch(component):
            stats.code_candidate_component_count += 1
            stats.component_classes["code_candidate"] += 1
        else:
            stats.unexpected_component_count += 1
            stats.component_classes["unexpected_component"] += 1
            stats.examples["unexpected_component"][component] += 1
    stats.cells_with_empty_components += int(empty_in_cell)
    duplicate_count = len(components) - len(set(components))
    if duplicate_count:
        stats.cells_with_duplicate_components += 1
        stats.duplicate_component_count += duplicate_count


def build_icd10_component_audit(
    config: ICD10ComponentAuditConfig,
    notation_audit_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> ICD10ComponentAuditResult:
    policy = load_icd10_component_audit_policy(config.policy_path)
    if notation_audit_run_id != policy.notation_audit_basis_run_id:
        raise HarmonizationError("Unexpected ICD-10 notation-audit basis run")
    if not RUN_ID_PATTERN.fullmatch(notation_audit_run_id):
        raise HarmonizationError("Invalid ICD-10 notation-audit run ID")
    contract = load_reviewed_icd10_contract(policy.reviewed_contract_path)
    contract_valid = bool(
        contract.notation_audit_run_id == notation_audit_run_id
        and contract.allow_component_audit_reads
        and contract.allow_report_writes
        and not contract.allow_value_transforms
        and not contract.allow_harmonized_artifact_writes
        and not contract.allow_hospital_concatenation
        and not contract.allow_union_schema_freeze
        and not contract.allow_parser_activation
    )

    notation_private = (
        config.reports_root / "private" / "icd10_notation_audit" / notation_audit_run_id
    )
    notation_manifest_path = notation_private / "icd10_notation_audit_manifest.json"
    notation_review_path = (
        config.reports_root
        / "review"
        / "icd10_notation_audit"
        / f"{notation_audit_run_id}.json"
    )
    static_private = (
        config.reports_root
        / "private"
        / "static_contract_audit"
        / contract.static_contract_audit_run_id
    )
    static_manifest_path = static_private / "static_contract_audit_manifest.json"
    occurrences_path = static_private / "occurrences.parquet"
    required = (
        notation_manifest_path,
        notation_review_path,
        static_manifest_path,
        occurrences_path,
        policy.reviewed_contract_path,
    )
    if any(not path.is_file() for path in required):
        raise HarmonizationError("ICD-10 component-audit input evidence is incomplete")
    notation_manifest = _read_json(notation_manifest_path, "ICD-10 notation private manifest")
    notation_review = _read_json(notation_review_path, "ICD-10 notation review")
    static_manifest = _read_json(static_manifest_path, "Static-contract private manifest")
    try:
        occurrence_rows = pq.read_table(occurrences_path).to_pylist()
    except Exception as exc:
        raise HarmonizationError("Static-contract occurrences cannot be read") from exc
    notation_metrics = notation_review.get("metrics", {})
    evidence_valid = bool(
        notation_manifest.get("artifact") == "asic_v3_icd10_notation_audit_private"
        and notation_manifest.get("artifact_version")
        == policy.required_notation_private_artifact_version
        and notation_manifest.get("dataset_context") == config.dataset_context
        and notation_manifest.get("inputs", {}).get("static_contract_audit_run_id")
        == contract.static_contract_audit_run_id
        and notation_review.get("artifact") == "asic_v3_icd10_notation_audit_review"
        and notation_review.get("artifact_version")
        == policy.required_notation_review_artifact_version
        and notation_review.get("dataset_context") == config.dataset_context
        and notation_metrics.get("technical_blocking_finding_count") == 0
        and notation_metrics.get("audited_hospital_count") == contract.audited_hospital_count
        and notation_metrics.get("scanned_row_count") == contract.scanned_row_count
        and notation_metrics.get("nonempty_cell_count") == contract.nonempty_cell_count
        and notation_metrics.get("textual_missing_candidate_count")
        == contract.approved_textual_missing_cell_count
        and notation_metrics.get("single_code_candidate_count")
        == contract.single_code_cell_count
        and notation_metrics.get("nonstring_source_cell_count") == 0
        and static_manifest.get("artifact") == "asic_v3_static_contract_audit_private"
        and static_manifest.get("artifact_version")
        == policy.required_static_contract_private_artifact_version
        and static_manifest.get("dataset_context") == config.dataset_context
    )

    selected = [
        row
        for row in occurrence_rows
        if row.get("table") == "static" and row.get("candidate_target") == "icd10_codes"
    ]
    by_hospital = {row.get("hospital"): row for row in selected}
    occurrences_complete = bool(
        len(selected) == len(EXPECTED_HOSPITALS)
        and tuple(sorted(by_hospital)) == tuple(sorted(EXPECTED_HOSPITALS))
    )
    if not occurrences_complete:
        raise HarmonizationError("ICD-10 source occurrence evidence is incomplete")

    code_pattern = re.compile(policy.component_code_candidate_regex)
    stats_by_hospital: dict[str, _HospitalStats] = {}
    input_hashes: dict[str, dict[str, str]] = {}
    hashes_match = True
    bindings_valid = True
    for hospital in EXPECTED_HOSPITALS:
        row = by_hospital[hospital]
        physical_name = row.get("physical_name")
        raw_name = row.get("raw_name")
        raw_occurrence = row.get("raw_occurrence")
        if (
            not isinstance(physical_name, str)
            or not isinstance(raw_name, str)
            or not isinstance(raw_occurrence, int)
        ):
            raise HarmonizationError("ICD-10 source occurrence identity is invalid")
        hospital_dir = config.ingested_root / hospital
        static_path = hospital_dir / "static.parquet"
        ingestion_manifest_path = hospital_dir / "ingestion_manifest.json"
        if not static_path.is_file() or not ingestion_manifest_path.is_file():
            raise HarmonizationError(f"ICD-10 ingested input is unavailable for {hospital}")
        ingestion_manifest = _read_json(
            ingestion_manifest_path, f"Ingestion manifest for {hospital}"
        )
        expected_hash = ingestion_manifest.get("outputs", {}).get("static", {}).get("sha256")
        observed_hash = sha256_file(static_path)
        hashes_match &= bool(
            ingestion_manifest.get("artifact") == "asic_v3_lossless_hospital_ingestion"
            and ingestion_manifest.get("artifact_version")
            == policy.required_ingestion_manifest_version
            and ingestion_manifest.get("hospital", {}).get("canonical_hospital_id")
            == hospital
            and isinstance(expected_hash, str)
            and expected_hash == observed_hash
        )
        input_hashes[hospital] = {
            "ingestion_manifest_sha256": sha256_file(ingestion_manifest_path),
            "static_parquet_sha256": observed_hash,
        }
        parquet = pq.ParquetFile(static_path)
        field_index = parquet.schema_arrow.get_field_index(physical_name)
        if field_index < 0:
            raise HarmonizationError(f"ICD-10 source field is unavailable for {hospital}")
        field = parquet.schema_arrow.field(field_index)
        metadata = field.metadata or {}
        bindings_valid &= bool(
            (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
            and metadata.get(b"raw_name", b"").decode("utf-8") == raw_name
            and int(metadata.get(b"raw_occurrence", b"0")) == raw_occurrence
        )
        stats = _HospitalStats.create()
        for batch in parquet.iter_batches(
            batch_size=policy.rows_per_batch,
            columns=[physical_name],
            use_threads=False,
        ):
            for raw in batch.column(0).to_pylist():
                _consume(stats, raw, hospital, contract, code_pattern)
        stats_by_hospital[hospital] = stats
        if progress is not None:
            progress(f"icd10_component_hospital_scanned={hospital} rows={stats.row_count}")

    notation_hospitals = {
        item.get("hospital"): item for item in notation_review.get("hospital_summaries", [])
    }
    expected_comma_cells = sum(
        int(item.get("cell_count", 0))
        for item in notation_review.get("syntax_patterns", [])
        if "comma" in str(item.get("syntax_signature", "")).split("+")
    )
    row_accounting = True
    notation_reproduced = True
    missing_reproduced = True
    component_accounting = True
    nonstring_count = 0
    sanitized_hospitals: list[dict[str, Any]] = []
    private_patterns: list[dict[str, Any]] = []
    private_examples: list[dict[str, Any]] = []
    for hospital in EXPECTED_HOSPITALS:
        stats = stats_by_hospital[hospital]
        prior = notation_hospitals.get(hospital, {})
        row_accounting &= stats.row_count == (
            stats.source_null_count + stats.literal_empty_count + stats.nonempty_count
        )
        notation_reproduced &= bool(
            stats.row_count == prior.get("row_count")
            and stats.nonempty_count == prior.get("nonempty_count")
            and stats.approved_missing_count
            == prior.get("textual_missing_candidate_count")
            and stats.single_component_cell_count
            == prior.get("single_code_candidate_count")
        )
        missing = contract.missing_decision_for(hospital)
        expected_missing = missing.evidence_cell_count if missing is not None else 0
        missing_reproduced &= stats.approved_missing_count == expected_missing
        component_accounting &= bool(
            stats.nonempty_count == stats.approved_missing_count + stats.parsed_cell_count
            and stats.parsed_cell_count
            == stats.single_component_cell_count + stats.multi_component_cell_count
            and stats.component_count
            == stats.code_candidate_component_count
            + stats.unexpected_component_count
            + stats.empty_component_count
            and stats.source_stream_hash.hexdigest()
            == stats.proposed_source_stream_hash.hexdigest()
        )
        nonstring_count += stats.nonstring_count
        assert stats.list_lengths is not None
        assert stats.component_classes is not None
        assert stats.examples is not None
        for component_class, count in sorted(stats.component_classes.items()):
            examples = stats.examples.get(component_class, Counter())
            selected_examples = examples.most_common(
                policy.maximum_private_examples_per_hospital_class
            )
            private_patterns.append(
                {
                    "hospital": hospital,
                    "component_class": component_class,
                    "component_count": count,
                    "distinct_private_example_count": len(examples),
                    "private_examples_truncated": len(examples) > len(selected_examples),
                }
            )
            for raw_component, count_for_component in selected_examples:
                private_examples.append(
                    {
                        "hospital": hospital,
                        "component_class": component_class,
                        "raw_component": raw_component,
                        "component_count": count_for_component,
                    }
                )
        sanitized_hospitals.append(
            {
                "hospital": hospital,
                "row_count": stats.row_count,
                "approved_missing_count": stats.approved_missing_count,
                "parsed_cell_count": stats.parsed_cell_count,
                "component_count": stats.component_count,
                "code_candidate_component_count": stats.code_candidate_component_count,
                "unexpected_component_count": stats.unexpected_component_count,
                "empty_component_count": stats.empty_component_count,
                "components_with_trimmed_whitespace": stats.components_with_trimmed_whitespace,
                "cells_with_duplicate_components": stats.cells_with_duplicate_components,
                "duplicate_component_count": stats.duplicate_component_count,
                "list_length_distribution": {
                    str(length): count for length, count in sorted(stats.list_lengths.items())
                },
            }
        )

    totals = {
        "audited_hospital_count": len(EXPECTED_HOSPITALS),
        "scanned_row_count": sum(item.row_count for item in stats_by_hospital.values()),
        "approved_textual_missing_cell_count": sum(
            item.approved_missing_count for item in stats_by_hospital.values()
        ),
        "parsed_cell_count": sum(item.parsed_cell_count for item in stats_by_hospital.values()),
        "comma_delimited_cell_count": sum(
            item.comma_cell_count for item in stats_by_hospital.values()
        ),
        "single_component_cell_count": sum(
            item.single_component_cell_count for item in stats_by_hospital.values()
        ),
        "multi_component_cell_count": sum(
            item.multi_component_cell_count for item in stats_by_hospital.values()
        ),
        "component_count": sum(item.component_count for item in stats_by_hospital.values()),
        "code_candidate_component_count": sum(
            item.code_candidate_component_count for item in stats_by_hospital.values()
        ),
        "unexpected_component_count": sum(
            item.unexpected_component_count for item in stats_by_hospital.values()
        ),
        "empty_component_count": sum(
            item.empty_component_count for item in stats_by_hospital.values()
        ),
        "cells_with_empty_components": sum(
            item.cells_with_empty_components for item in stats_by_hospital.values()
        ),
        "components_with_trimmed_whitespace": sum(
            item.components_with_trimmed_whitespace for item in stats_by_hospital.values()
        ),
        "cells_with_duplicate_components": sum(
            item.cells_with_duplicate_components for item in stats_by_hospital.values()
        ),
        "duplicate_component_count": sum(
            item.duplicate_component_count for item in stats_by_hospital.values()
        ),
        "nonstring_source_cell_count": nonstring_count,
    }
    notation_reproduced &= bool(
        totals["scanned_row_count"] == contract.scanned_row_count
        and totals["approved_textual_missing_cell_count"]
        == contract.approved_textual_missing_cell_count
        and totals["parsed_cell_count"]
        == contract.comma_delimited_cell_count + contract.single_code_cell_count
        and totals["comma_delimited_cell_count"] == expected_comma_cells
        and totals["comma_delimited_cell_count"] == contract.comma_delimited_cell_count
        and totals["single_component_cell_count"] == contract.single_code_cell_count
    )
    failure_counts = {
        "icd10_notation_evidence_valid": int(not evidence_valid),
        "reviewed_icd10_contract_valid": int(not contract_valid),
        "icd10_source_occurrences_complete": int(not occurrences_complete),
        "ingested_static_hashes_match_manifests": int(not hashes_match),
        "icd10_source_bindings_valid": int(not bindings_valid),
        "icd10_source_cells_are_strings": nonstring_count,
        "icd10_row_accounting_conserved": int(not row_accounting),
        "icd10_notation_counts_reproduced": int(not notation_reproduced),
        "icd10_approved_missing_evidence_reproduced": int(not missing_reproduced),
        "icd10_component_accounting_conserved": int(not component_accounting),
        "icd10_empty_components_resolved": totals["empty_component_count"],
        "icd10_unexpected_component_syntax_reviewed": totals[
            "unexpected_component_count"
        ],
        "icd10_component_evidence_approved": 1,
        "icd10_canonical_list_contract_activated": 1,
        "no_production_data_artifacts_generated": 0,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed=count,
            expected=0,
            details=CHECK_DETAILS[name],
        )
        for name, count in failure_counts.items()
    ]
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(item for item in blockers if item["check"] in TECHNICAL_CHECKS)
    totals["technical_blocking_finding_count"] = len(technical)
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_icd10_component_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": overall_status(checks),
        "inputs": {
            "icd10_notation_audit_run_id": notation_audit_run_id,
            "static_contract_audit_run_id": contract.static_contract_audit_run_id,
            "reviewed_icd10_contract_version": contract.version,
        },
        "metrics": totals,
        "hospital_summaries": sanitized_hospitals,
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "proposed_contract": {
            "source_text_target": contract.source_text_target,
            "source_text_value_type": contract.source_text_value_type,
            "canonical_list_target": contract.list_target,
            "canonical_list_value_type": contract.list_value_type,
            "delimiter": contract.delimiter,
            "component_normalization": contract.component_normalization,
            "single_code_handling": "one_element_list",
            "preserve_source_order": contract.preserve_source_order,
            "preserve_duplicates": contract.preserve_duplicates,
            "semantic_code_validation": "deferred",
            "parser_activation": "blocked_pending_human_review",
        },
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
        },
        "publication": {
            "source_data_modified": False,
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "parser_activated": False,
        },
        "limitations": [
            "Code-candidate syntax is structural evidence only and does not validate diagnoses or ICD-10 clinical semantics.",
            "Exact unexpected component examples remain only in the owner-only private bundle on the authorized cluster.",
            "Duplicate components are preserved and reported; they are not automatically removed.",
            "No parsed list column or source-text column is created by this audit.",
            "No harmonized, cleaned, derived, concatenated, or pooled production artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_icd10_component_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": review_payload["overall_status"],
        "inputs": {
            "icd10_notation_audit_run_id": notation_audit_run_id,
            "notation_manifest_sha256": sha256_file(notation_manifest_path),
            "notation_review_sha256": sha256_file(notation_review_path),
            "static_contract_occurrences_sha256": sha256_file(occurrences_path),
            "reviewed_icd10_contract_sha256": policy.reviewed_contract_sha256,
            "static_inputs": input_hashes,
            "nonmissing_source_stream_sha256": {
                hospital: stats_by_hospital[hospital].source_stream_hash.hexdigest()
                for hospital in EXPECTED_HOSPITALS
            },
            "proposed_source_text_stream_sha256": {
                hospital: stats_by_hospital[
                    hospital
                ].proposed_source_stream_hash.hexdigest()
                for hospital in EXPECTED_HOSPITALS
            },
        },
        "policy": {"version": policy.version, "path": str(policy.source_path)},
        "checks": [asdict(item) for item in checks],
        "private_pattern_count": len(private_patterns),
        "private_example_count": len(private_examples),
        "production_data_artifacts_generated": False,
    }
    return ICD10ComponentAuditResult(
        overall_status=review_payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_manifest=private_manifest,
        private_patterns=tuple(private_patterns),
        private_examples=tuple(private_examples),
        review_payload=review_payload,
    )


def _write_json(path: Path, value: dict[str, Any], mode: int) -> None:
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


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 ICD-10 component audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input notation-audit run: `{payload['inputs']['icd10_notation_audit_run_id']}`",
        "- Protected filenames, identifiers, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    if payload["blocking_findings"]:
        lines.extend(
            f"- `{item['check']}`: {item['details']}"
            for item in payload["blocking_findings"]
        )
    else:
        lines.append("- None")
    lines.extend(["", "## Audit totals", ""])
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    lines.extend(["", "## Hospital summaries", ""])
    for item in payload["hospital_summaries"]:
        lines.append(
            f"- `{item['hospital']}`: parsed cells `{item['parsed_cell_count']}`; "
            f"components `{item['component_count']}`; code candidates "
            f"`{item['code_candidate_component_count']}`; unexpected "
            f"`{item['unexpected_component_count']}`; empty "
            f"`{item['empty_component_count']}`; trimmed whitespace "
            f"`{item['components_with_trimmed_whitespace']}`; duplicate-bearing cells "
            f"`{item['cells_with_duplicate_components']}`"
        )
    lines.extend(["", "## Proposed contract", ""])
    lines.append(
        "Preserve exact non-missing source strings in nullable `icd10_codes_source_text`; represent diagnosis codes as ordered `list<large_string>` in `icd10_codes` by splitting on commas and trimming component-edge whitespace only. Preserve case, punctuation, order, and duplicates."
    )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review all component accounting, syntax, whitespace, duplicate, and list-length evidence before activating the canonical list parser."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_icd10_component_audit_bundle(
    result: ICD10ComponentAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> ICD10ComponentAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid ICD-10 component-audit run ID")
    private_dir = reports_root / "private" / "icd10_component_audit" / selected
    review_dir = reports_root / "review" / "icd10_component_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "ICD-10 component-audit run already exists and will not be overwritten"
        )
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(
        private_dir / "icd10_component_audit_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(private_dir / "component_patterns.parquet", result.private_patterns)
    _write_private_parquet(private_dir / "examples.parquet", result.private_examples)
    readme = private_dir / "README.md"
    descriptor = os.open(readme, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(
            "# Owner-only ICD-10 component evidence\n\n"
            "Keep this directory on the authorized cluster. Exact unexpected "
            "components, if any, must not be copied into committed or sanitized "
            "reports. This audit creates no parsed clinical field.\n"
        )
    readme.chmod(0o600)
    _write_json(review_json, result.review_payload, 0o640)
    temporary = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_markdown(result.review_payload))
        temporary.replace(review_md)
        review_md.chmod(0o640)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return replace(
        result,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
