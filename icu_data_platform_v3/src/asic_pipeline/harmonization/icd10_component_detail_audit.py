from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
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
        "icd10_component_audit_evidence_valid",
        "reviewed_icd10_contract_valid",
        "icd10_source_occurrences_complete",
        "ingested_static_hashes_match_manifests",
        "icd10_source_bindings_valid",
        "icd10_source_cells_are_strings",
        "icd10_prior_component_counts_reproduced",
        "icd10_empty_position_accounting_conserved",
        "icd10_duplicate_accounting_conserved",
        "no_production_data_artifacts_generated",
    }
)
CHECK_DETAILS = {
    "icd10_component_audit_evidence_valid": (
        "The immutable component-audit private and sanitized artifacts must match the approved run, versions, context, and reviewed counts."
    ),
    "reviewed_icd10_contract_valid": (
        "Reviewed ICD-10 contract 0.1 and its approved hash must remain unchanged and fail closed."
    ),
    "icd10_source_occurrences_complete": (
        "Exactly one immutable static ICD-10 source occurrence is required for every approved hospital."
    ),
    "ingested_static_hashes_match_manifests": (
        "Every rescanned static Parquet must match its immutable ingestion-manifest hash."
    ),
    "icd10_source_bindings_valid": (
        "Each selected physical field must match its lossless raw-name and occurrence metadata."
    ),
    "icd10_source_cells_are_strings": (
        "Every non-null source cell must remain a lossless raw string."
    ),
    "icd10_prior_component_counts_reproduced": (
        "The detail scan must reproduce every approved component-audit total and hospital count."
    ),
    "icd10_empty_position_accounting_conserved": (
        "Every empty component must be assigned exactly once to a leading, trailing, or interior position."
    ),
    "icd10_duplicate_accounting_conserved": (
        "Every duplicate occurrence must be assigned exactly once as adjacent or separated, and ordered deduplication counts must conserve components."
    ),
    "icd10_empty_component_policy_approved": (
        "The reviewed leading, trailing, and interior empty-component evidence requires an explicit keep, drop, or blocking policy."
    ),
    "icd10_duplicate_policy_approved": (
        "The reviewed duplicate evidence requires an explicit preserve or ordered-deduplicate policy for the canonical list."
    ),
    "icd10_incomplete_component_policy_approved": (
        "The one privately reviewed incomplete/input-error candidate requires an explicit canonical-list handling policy."
    ),
    "icd10_component_detail_evidence_approved": (
        "The complete empty-position, duplicate-adjacency, multiplicity, and list-length evidence requires explicit human approval."
    ),
    "icd10_canonical_list_contract_activated": (
        "The canonical list parser remains disabled until the detail evidence and final policies are recorded in a new reviewed contract."
    ),
    "no_production_data_artifacts_generated": (
        "The audit may write reports only and must not generate harmonized, cleaned, derived, or pooled data."
    ),
}


@dataclass(frozen=True)
class ICD10ComponentDetailAuditPolicy:
    version: str
    component_audit_basis_run_id: str
    required_component_private_artifact_version: str
    required_component_review_artifact_version: str
    required_static_contract_private_artifact_version: str
    required_ingestion_manifest_version: str
    reviewed_contract_path: Path
    reviewed_contract_sha256: str
    approved_input_evidence: tuple[tuple[str, int], ...]
    component_code_candidate_regex: str
    maximum_private_examples_per_hospital_class: int
    rows_per_batch: int
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class ICD10ComponentDetailAuditConfig:
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
class ICD10ComponentDetailAuditResult:
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
    nonstring_count: int = 0
    parsed_cell_count: int = 0
    component_count: int = 0
    code_candidate_component_count: int = 0
    unexpected_component_count: int = 0
    empty_component_count: int = 0
    cells_with_empty_components: int = 0
    leading_empty_component_count: int = 0
    trailing_empty_component_count: int = 0
    interior_empty_component_count: int = 0
    cells_empty_after_empty_removal: int = 0
    cells_with_duplicate_components: int = 0
    duplicate_component_count: int = 0
    adjacent_duplicate_component_count: int = 0
    separated_duplicate_component_count: int = 0
    maximum_component_multiplicity: int = 0
    component_count_after_empty_removal: int = 0
    component_count_after_ordered_deduplication: int = 0
    cells_changed_by_ordered_deduplication: int = 0
    empty_cell_examples: dict[str, Counter[str]] | None = None
    unexpected_examples: Counter[str] | None = None
    list_lengths_before: Counter[int] | None = None
    list_lengths_after_empty_removal: Counter[int] | None = None
    list_lengths_after_ordered_deduplication: Counter[int] | None = None

    @classmethod
    def create(cls) -> _HospitalStats:
        return cls(
            empty_cell_examples=defaultdict(Counter),
            unexpected_examples=Counter(),
            list_lengths_before=Counter(),
            list_lengths_after_empty_removal=Counter(),
            list_lengths_after_ordered_deduplication=Counter(),
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


def load_icd10_component_detail_audit_config(
    path: str | Path,
) -> ICD10ComponentDetailAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "ICD-10 component-detail configuration")
    policy_path = resolve_path(
        required_string(raw, "icd10_component_detail_audit_policy", "config"),
        source,
    )
    return ICD10ComponentDetailAuditConfig(harmonization, policy_path)


def load_icd10_component_detail_audit_policy(
    path: str | Path,
) -> ICD10ComponentDetailAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "ICD-10 component-detail policy")
    if raw.get("icd10_component_detail_audit_policy_version") != "0.1":
        raise ConfigurationError("ICD-10 component-detail policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_icd10_component_detail_evidence":
        raise ConfigurationError("ICD-10 component-detail policy is not approved")
    basis = required_string(
        raw, "icd10_component_audit_basis_run_id", "ICD-10 component detail"
    )
    if basis != "20260805T134245Z":
        raise ConfigurationError("ICD-10 component-audit basis changed")
    contract_path = resolve_path(
        required_string(raw, "reviewed_icd10_contract", "ICD-10 component detail"),
        source,
    )
    contract_hash = required_string(
        raw, "reviewed_icd10_contract_sha256", "ICD-10 component detail"
    )
    if (
        contract_hash
        != "f6edf01c21587ae124fa0631cb5cc8ac4e297ef8a2e1e77a966a51ef83d24da1"
        or not contract_path.is_file()
        or sha256_file(contract_path) != contract_hash
    ):
        raise ConfigurationError("Reviewed ICD-10 contract hash changed")
    evidence = _mapping(raw.get("approved_input_evidence"), "detail.approved_input_evidence")
    expected_evidence = {
        "scanned_row_count": 16054,
        "parsed_cell_count": 16029,
        "component_count": 466002,
        "code_candidate_component_count": 465975,
        "unexpected_component_count": 1,
        "empty_component_count": 26,
        "cells_with_empty_components": 26,
        "cells_with_duplicate_components": 9012,
        "duplicate_component_count": 96505,
        "nonstring_source_cell_count": 0,
    }
    if evidence != expected_evidence:
        raise ConfigurationError("Approved ICD-10 component evidence changed")
    classification = _mapping(raw.get("classification"), "detail.classification")
    regex = required_string(
        classification, "component_code_candidate_regex", "detail.classification"
    )
    try:
        re.compile(regex)
    except re.error as exc:
        raise ConfigurationError("ICD-10 component-detail regex is invalid") from exc
    scope = _mapping(raw.get("scope"), "ICD-10 component detail.scope")
    expected_scope = {
        "allow_component_private_evidence_reads": True,
        "allow_static_contract_private_evidence_reads": True,
        "allow_ingested_static_icd10_reads": True,
        "allow_detail_accounting": True,
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
        raise ConfigurationError("ICD-10 component-detail audit must remain read-only")
    scan = _mapping(raw.get("scan"), "ICD-10 component detail.scan")
    reporting = _mapping(raw.get("reporting"), "ICD-10 component detail.reporting")
    if reporting.get("private_directory_mode") != "0700" or reporting.get(
        "private_file_mode"
    ) != "0600":
        raise ConfigurationError("ICD-10 component-detail private modes changed")
    return ICD10ComponentDetailAuditPolicy(
        version="0.1",
        component_audit_basis_run_id=basis,
        required_component_private_artifact_version=required_string(
            raw, "required_component_private_artifact_version", "detail"
        ),
        required_component_review_artifact_version=required_string(
            raw, "required_component_review_artifact_version", "detail"
        ),
        required_static_contract_private_artifact_version=required_string(
            raw, "required_static_contract_private_artifact_version", "detail"
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "detail"
        ),
        reviewed_contract_path=contract_path,
        reviewed_contract_sha256=contract_hash,
        approved_input_evidence=tuple(expected_evidence.items()),
        component_code_candidate_regex=regex,
        maximum_private_examples_per_hospital_class=_positive_int(
            classification,
            "maximum_private_examples_per_hospital_class",
            "detail.classification",
        ),
        rows_per_batch=_positive_int(scan, "rows_per_batch", "detail.scan"),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "detail.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "detail.reporting"
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


def _consume(
    stats: _HospitalStats,
    raw: object,
    hospital: str,
    contract: ReviewedICD10Contract,
    code_pattern: re.Pattern[str],
) -> None:
    stats.row_count += 1
    if raw is None or raw == "":
        return
    if not isinstance(raw, str):
        stats.nonstring_count += 1
        return
    missing = contract.missing_decision_for(hospital)
    if missing is not None and missing.matches(raw):
        return
    stats.parsed_cell_count += 1
    original_components = raw.split(contract.delimiter)
    components = [item.strip() for item in original_components]
    assert stats.list_lengths_before is not None
    assert stats.list_lengths_after_empty_removal is not None
    assert stats.list_lengths_after_ordered_deduplication is not None
    assert stats.empty_cell_examples is not None
    assert stats.unexpected_examples is not None
    stats.list_lengths_before[len(components)] += 1
    stats.component_count += len(components)
    empty_positions: list[str] = []
    nonempty_components: list[str] = []
    for index, component in enumerate(components):
        if component == "":
            stats.empty_component_count += 1
            if index == 0:
                stats.leading_empty_component_count += 1
                empty_positions.append("leading")
            elif index == len(components) - 1:
                stats.trailing_empty_component_count += 1
                empty_positions.append("trailing")
            else:
                stats.interior_empty_component_count += 1
                empty_positions.append("interior")
            continue
        nonempty_components.append(component)
        if code_pattern.fullmatch(component):
            stats.code_candidate_component_count += 1
        else:
            stats.unexpected_component_count += 1
            stats.unexpected_examples[component] += 1
    if empty_positions:
        stats.cells_with_empty_components += 1
        signature = "+".join(sorted(set(empty_positions)))
        stats.empty_cell_examples[signature][raw] += 1
    stats.component_count_after_empty_removal += len(nonempty_components)
    stats.list_lengths_after_empty_removal[len(nonempty_components)] += 1
    stats.cells_empty_after_empty_removal += int(not nonempty_components)

    seen: set[str] = set()
    ordered_unique: list[str] = []
    adjacent = 0
    separated = 0
    for index, component in enumerate(nonempty_components):
        if component in seen:
            if index > 0 and nonempty_components[index - 1] == component:
                adjacent += 1
            else:
                separated += 1
        else:
            seen.add(component)
            ordered_unique.append(component)
    duplicate_count = adjacent + separated
    stats.cells_with_duplicate_components += int(duplicate_count > 0)
    stats.duplicate_component_count += duplicate_count
    stats.adjacent_duplicate_component_count += adjacent
    stats.separated_duplicate_component_count += separated
    counts = Counter(nonempty_components)
    stats.maximum_component_multiplicity = max(
        stats.maximum_component_multiplicity,
        max(counts.values(), default=0),
    )
    stats.component_count_after_ordered_deduplication += len(ordered_unique)
    stats.cells_changed_by_ordered_deduplication += int(duplicate_count > 0)
    stats.list_lengths_after_ordered_deduplication[len(ordered_unique)] += 1


def _distribution(counter: Counter[int]) -> dict[str, int]:
    return {str(key): value for key, value in sorted(counter.items())}


def build_icd10_component_detail_audit(
    config: ICD10ComponentDetailAuditConfig,
    component_audit_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> ICD10ComponentDetailAuditResult:
    policy = load_icd10_component_detail_audit_policy(config.policy_path)
    if component_audit_run_id != policy.component_audit_basis_run_id:
        raise HarmonizationError("Unexpected ICD-10 component-audit basis run")
    if not RUN_ID_PATTERN.fullmatch(component_audit_run_id):
        raise HarmonizationError("Invalid ICD-10 component-audit run ID")
    contract = load_reviewed_icd10_contract(policy.reviewed_contract_path)
    contract_valid = bool(
        not contract.allow_value_transforms
        and not contract.allow_harmonized_artifact_writes
        and not contract.allow_hospital_concatenation
        and not contract.allow_union_schema_freeze
        and not contract.allow_parser_activation
    )

    component_private = (
        config.reports_root
        / "private"
        / "icd10_component_audit"
        / component_audit_run_id
    )
    component_manifest_path = component_private / "icd10_component_audit_manifest.json"
    component_review_path = (
        config.reports_root
        / "review"
        / "icd10_component_audit"
        / f"{component_audit_run_id}.json"
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
        component_manifest_path,
        component_review_path,
        static_manifest_path,
        occurrences_path,
        policy.reviewed_contract_path,
    )
    if any(not path.is_file() for path in required):
        raise HarmonizationError("ICD-10 component-detail inputs are incomplete")
    component_manifest = _read_json(component_manifest_path, "Component private manifest")
    component_review = _read_json(component_review_path, "Component review")
    static_manifest = _read_json(static_manifest_path, "Static-contract private manifest")
    try:
        occurrence_rows = pq.read_table(occurrences_path).to_pylist()
    except Exception as exc:
        raise HarmonizationError("Static-contract occurrences cannot be read") from exc
    approved_evidence = dict(policy.approved_input_evidence)
    component_metrics = component_review.get("metrics", {})
    evidence_valid = bool(
        component_manifest.get("artifact") == "asic_v3_icd10_component_audit_private"
        and component_manifest.get("artifact_version")
        == policy.required_component_private_artifact_version
        and component_manifest.get("dataset_context") == config.dataset_context
        and component_review.get("artifact") == "asic_v3_icd10_component_audit_review"
        and component_review.get("artifact_version")
        == policy.required_component_review_artifact_version
        and component_review.get("dataset_context") == config.dataset_context
        and component_metrics.get("technical_blocking_finding_count") == 0
        and all(component_metrics.get(key) == value for key, value in approved_evidence.items())
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
            progress(f"icd10_component_detail_hospital_scanned={hospital} rows={stats.row_count}")

    prior_hospitals = {
        item.get("hospital"): item for item in component_review.get("hospital_summaries", [])
    }
    prior_reproduced = True
    empty_accounting = True
    duplicate_accounting = True
    private_patterns: list[dict[str, Any]] = []
    private_examples: list[dict[str, Any]] = []
    sanitized_hospitals: list[dict[str, Any]] = []
    for hospital in EXPECTED_HOSPITALS:
        stats = stats_by_hospital[hospital]
        prior = prior_hospitals.get(hospital, {})
        prior_reproduced &= bool(
            stats.row_count == prior.get("row_count")
            and stats.parsed_cell_count == prior.get("parsed_cell_count")
            and stats.component_count == prior.get("component_count")
            and stats.code_candidate_component_count
            == prior.get("code_candidate_component_count")
            and stats.unexpected_component_count == prior.get("unexpected_component_count")
            and stats.empty_component_count == prior.get("empty_component_count")
            and stats.cells_with_duplicate_components
            == prior.get("cells_with_duplicate_components")
            and stats.duplicate_component_count == prior.get("duplicate_component_count")
        )
        empty_accounting &= stats.empty_component_count == (
            stats.leading_empty_component_count
            + stats.trailing_empty_component_count
            + stats.interior_empty_component_count
        )
        duplicate_accounting &= bool(
            stats.duplicate_component_count
            == stats.adjacent_duplicate_component_count
            + stats.separated_duplicate_component_count
            and stats.component_count_after_empty_removal
            - stats.duplicate_component_count
            == stats.component_count_after_ordered_deduplication
            and stats.cells_changed_by_ordered_deduplication
            == stats.cells_with_duplicate_components
        )
        assert stats.empty_cell_examples is not None
        assert stats.unexpected_examples is not None
        assert stats.list_lengths_before is not None
        assert stats.list_lengths_after_empty_removal is not None
        assert stats.list_lengths_after_ordered_deduplication is not None
        for signature, examples in sorted(stats.empty_cell_examples.items()):
            selected_examples = examples.most_common(
                policy.maximum_private_examples_per_hospital_class
            )
            private_patterns.append(
                {
                    "hospital": hospital,
                    "detail_class": f"empty_{signature}",
                    "cell_count": sum(examples.values()),
                    "distinct_private_example_count": len(examples),
                    "private_examples_truncated": len(examples) > len(selected_examples),
                }
            )
            for raw_source_text, count in selected_examples:
                private_examples.append(
                    {
                        "hospital": hospital,
                        "detail_class": f"empty_{signature}",
                        "raw_source_text": raw_source_text,
                        "cell_count": count,
                    }
                )
        unexpected_selected = stats.unexpected_examples.most_common(
            policy.maximum_private_examples_per_hospital_class
        )
        if stats.unexpected_component_count:
            private_patterns.append(
                {
                    "hospital": hospital,
                    "detail_class": "incomplete_component_candidate",
                    "cell_count": stats.unexpected_component_count,
                    "distinct_private_example_count": len(stats.unexpected_examples),
                    "private_examples_truncated": len(stats.unexpected_examples)
                    > len(unexpected_selected),
                }
            )
            for raw_component, count in unexpected_selected:
                private_examples.append(
                    {
                        "hospital": hospital,
                        "detail_class": "incomplete_component_candidate",
                        "raw_component": raw_component,
                        "cell_count": count,
                    }
                )
        sanitized_hospitals.append(
            {
                "hospital": hospital,
                "parsed_cell_count": stats.parsed_cell_count,
                "empty_component_count": stats.empty_component_count,
                "leading_empty_component_count": stats.leading_empty_component_count,
                "trailing_empty_component_count": stats.trailing_empty_component_count,
                "interior_empty_component_count": stats.interior_empty_component_count,
                "cells_empty_after_empty_removal": stats.cells_empty_after_empty_removal,
                "cells_with_duplicate_components": stats.cells_with_duplicate_components,
                "duplicate_component_count": stats.duplicate_component_count,
                "adjacent_duplicate_component_count": stats.adjacent_duplicate_component_count,
                "separated_duplicate_component_count": stats.separated_duplicate_component_count,
                "maximum_component_multiplicity": stats.maximum_component_multiplicity,
                "component_count_after_empty_removal": stats.component_count_after_empty_removal,
                "component_count_after_ordered_deduplication": stats.component_count_after_ordered_deduplication,
                "list_length_distribution_before": _distribution(stats.list_lengths_before),
                "list_length_distribution_after_empty_removal": _distribution(
                    stats.list_lengths_after_empty_removal
                ),
                "list_length_distribution_after_ordered_deduplication": _distribution(
                    stats.list_lengths_after_ordered_deduplication
                ),
            }
        )

    totals = {
        "audited_hospital_count": len(EXPECTED_HOSPITALS),
        "scanned_row_count": sum(item.row_count for item in stats_by_hospital.values()),
        "parsed_cell_count": sum(item.parsed_cell_count for item in stats_by_hospital.values()),
        "component_count": sum(item.component_count for item in stats_by_hospital.values()),
        "code_candidate_component_count": sum(
            item.code_candidate_component_count for item in stats_by_hospital.values()
        ),
        "incomplete_component_candidate_count": sum(
            item.unexpected_component_count for item in stats_by_hospital.values()
        ),
        "empty_component_count": sum(
            item.empty_component_count for item in stats_by_hospital.values()
        ),
        "cells_with_empty_components": sum(
            item.cells_with_empty_components for item in stats_by_hospital.values()
        ),
        "leading_empty_component_count": sum(
            item.leading_empty_component_count for item in stats_by_hospital.values()
        ),
        "trailing_empty_component_count": sum(
            item.trailing_empty_component_count for item in stats_by_hospital.values()
        ),
        "interior_empty_component_count": sum(
            item.interior_empty_component_count for item in stats_by_hospital.values()
        ),
        "cells_empty_after_empty_removal": sum(
            item.cells_empty_after_empty_removal for item in stats_by_hospital.values()
        ),
        "cells_with_duplicate_components": sum(
            item.cells_with_duplicate_components for item in stats_by_hospital.values()
        ),
        "duplicate_component_count": sum(
            item.duplicate_component_count for item in stats_by_hospital.values()
        ),
        "adjacent_duplicate_component_count": sum(
            item.adjacent_duplicate_component_count for item in stats_by_hospital.values()
        ),
        "separated_duplicate_component_count": sum(
            item.separated_duplicate_component_count for item in stats_by_hospital.values()
        ),
        "maximum_component_multiplicity": max(
            item.maximum_component_multiplicity for item in stats_by_hospital.values()
        ),
        "component_count_after_empty_removal": sum(
            item.component_count_after_empty_removal for item in stats_by_hospital.values()
        ),
        "component_count_after_ordered_deduplication": sum(
            item.component_count_after_ordered_deduplication
            for item in stats_by_hospital.values()
        ),
        "cells_changed_by_ordered_deduplication": sum(
            item.cells_changed_by_ordered_deduplication
            for item in stats_by_hospital.values()
        ),
        "nonstring_source_cell_count": sum(
            item.nonstring_count for item in stats_by_hospital.values()
        ),
    }
    reproduced_input_totals = {
        **totals,
        "unexpected_component_count": totals[
            "incomplete_component_candidate_count"
        ],
    }
    prior_reproduced &= all(
        reproduced_input_totals.get(key) == value
        for key, value in approved_evidence.items()
    )
    empty_accounting &= totals["empty_component_count"] == (
        totals["leading_empty_component_count"]
        + totals["trailing_empty_component_count"]
        + totals["interior_empty_component_count"]
    )
    duplicate_accounting &= bool(
        totals["duplicate_component_count"]
        == totals["adjacent_duplicate_component_count"]
        + totals["separated_duplicate_component_count"]
        and totals["component_count_after_empty_removal"]
        - totals["duplicate_component_count"]
        == totals["component_count_after_ordered_deduplication"]
    )
    failure_counts = {
        "icd10_component_audit_evidence_valid": int(not evidence_valid),
        "reviewed_icd10_contract_valid": int(not contract_valid),
        "icd10_source_occurrences_complete": int(not occurrences_complete),
        "ingested_static_hashes_match_manifests": int(not hashes_match),
        "icd10_source_bindings_valid": int(not bindings_valid),
        "icd10_source_cells_are_strings": totals["nonstring_source_cell_count"],
        "icd10_prior_component_counts_reproduced": int(not prior_reproduced),
        "icd10_empty_position_accounting_conserved": int(not empty_accounting),
        "icd10_duplicate_accounting_conserved": int(not duplicate_accounting),
        "icd10_empty_component_policy_approved": 1,
        "icd10_duplicate_policy_approved": 1,
        "icd10_incomplete_component_policy_approved": 1,
        "icd10_component_detail_evidence_approved": 1,
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
        "artifact": "asic_v3_icd10_component_detail_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": overall_status(checks),
        "inputs": {
            "icd10_component_audit_run_id": component_audit_run_id,
            "reviewed_icd10_contract_version": contract.version,
        },
        "metrics": totals,
        "hospital_summaries": sanitized_hospitals,
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "reviewed_observation": {
            "unexpected_component_review_status": "incomplete_input_error_candidate",
            "exact_component_excluded_from_sanitized_report": True,
            "canonical_handling": "pending",
        },
        "decision_options": {
            "empty_components": (
                "retain_as_null_list_elements_or_remove_as_notation_artifacts"
            ),
            "duplicates": "preserve_or_ordered_first_occurrence_deduplicate",
            "incomplete_component": "retain_as_unresolved_or_remove_under_reviewed_rule",
            "parser_activation": "blocked",
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
            "The incomplete component is classified as a reviewed input-error candidate, but this audit does not remove it.",
            "Ordered-deduplication counts are counterfactual evidence only and do not alter source values.",
            "Exact empty-bearing source strings and incomplete components remain owner-only on the authorized cluster.",
            "No parsed list column or source-text column is created by this audit.",
            "No harmonized, cleaned, derived, concatenated, or pooled production artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_icd10_component_detail_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": review_payload["overall_status"],
        "inputs": {
            "icd10_component_audit_run_id": component_audit_run_id,
            "component_manifest_sha256": sha256_file(component_manifest_path),
            "component_review_sha256": sha256_file(component_review_path),
            "static_contract_occurrences_sha256": sha256_file(occurrences_path),
            "reviewed_icd10_contract_sha256": policy.reviewed_contract_sha256,
            "static_inputs": input_hashes,
        },
        "policy": {"version": policy.version, "path": str(policy.source_path)},
        "checks": [asdict(item) for item in checks],
        "private_pattern_count": len(private_patterns),
        "private_example_count": len(private_examples),
        "production_data_artifacts_generated": False,
    }
    return ICD10ComponentDetailAuditResult(
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
        "# ASIC v3 ICD-10 component-detail audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input component-audit run: `{payload['inputs']['icd10_component_audit_run_id']}`",
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
            f"- `{item['hospital']}`: empty components `{item['empty_component_count']}` "
            f"(leading `{item['leading_empty_component_count']}`, trailing "
            f"`{item['trailing_empty_component_count']}`, interior "
            f"`{item['interior_empty_component_count']}`); duplicate occurrences "
            f"`{item['duplicate_component_count']}` (adjacent "
            f"`{item['adjacent_duplicate_component_count']}`, separated "
            f"`{item['separated_duplicate_component_count']}`); maximum multiplicity "
            f"`{item['maximum_component_multiplicity']}`"
        )
    lines.extend(["", "## Reviewed observation", ""])
    lines.append(
        "The single unexpected component was privately reviewed as an incomplete/input-error candidate. Its exact value remains excluded; canonical handling is still pending."
    )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Approve explicit empty-component, duplicate, and incomplete-component policies before activating the canonical ICD-10 list parser."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_icd10_component_detail_audit_bundle(
    result: ICD10ComponentDetailAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> ICD10ComponentDetailAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid ICD-10 component-detail run ID")
    private_dir = reports_root / "private" / "icd10_component_detail_audit" / selected
    review_dir = reports_root / "review" / "icd10_component_detail_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "ICD-10 component-detail run already exists and will not be overwritten"
        )
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(
        private_dir / "icd10_component_detail_audit_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(private_dir / "detail_patterns.parquet", result.private_patterns)
    _write_private_parquet(private_dir / "examples.parquet", result.private_examples)
    readme = private_dir / "README.md"
    descriptor = os.open(readme, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(
            "# Owner-only ICD-10 component-detail evidence\n\n"
            "Keep exact empty-bearing source strings and incomplete components "
            "on the authorized cluster. This audit creates no parsed field.\n"
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
