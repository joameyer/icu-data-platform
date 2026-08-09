from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.composite_audit import (
    CompositeSourceAuditPolicy,
    load_composite_source_audit_policy,
)
from asic_pipeline.harmonization.icd10_contract import (
    ReviewedICD10Contract,
    load_reviewed_icd10_contract,
)
from asic_pipeline.harmonization.icd10_parser import (
    ReviewedICD10PrivateEvidence,
    harmonize_reviewed_static_icd10_batch,
    load_reviewed_icd10_private_evidence,
)
from asic_pipeline.harmonization.parsing import (
    parse_decimal_comma_numeric,
    parse_direct_numeric,
    parse_explicit_percentage_fraction,
    parse_numeric_list,
    parse_ratio_numeric,
    parse_threshold_boundary,
)
from asic_pipeline.harmonization.review_policy import (
    CandidateContract,
    CategoricalValueCandidate,
    HarmonizationReviewConfig,
    load_harmonization_review_config,
    load_harmonization_review_policy,
)
from asic_pipeline.harmonization.static_decisions import (
    ReviewedStaticDecisionRegistry,
    load_reviewed_static_decision_registry,
)
from asic_pipeline.harmonization.static_registry_completion_audit import (
    DRAFT_DEFINITIONS as STATIC_DRAFT_DEFINITIONS,
)
from asic_pipeline.ingestion.policy import IngestionPolicy, load_ingestion_policy
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import InventoryPolicy, load_inventory_policy
from asic_pipeline.inventory.report import RUN_ID_PATTERN, _write_private_parquet, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.schema_tokens.policy import SchemaTokenPolicy, load_schema_token_policy


PROVENANCE_FIELDS = (
    "__v3_source_file_id",
    "__v3_source_file_order",
    "__v3_source_row_number",
    "__v3_source_order",
    "__v3_source_schema_variant_id",
)
IDENTIFIER_RAW_NAMES = frozenset({"Pseudo-ID", "PseudoID"})
TECHNICAL_CHECKS = frozenset(
    {
        "immutable_evidence_and_decision_sources_valid",
        "ingested_inputs_match_manifests",
        "all_raw_occurrences_have_candidate_dispositions",
        "source_bindings_match_lossless_metadata",
        "row_conservation",
        "common_candidate_schemas",
        "run_scoped_outputs_are_isolated_and_nonpublishable",
    }
)
CHECK_DETAILS = {
    "immutable_evidence_and_decision_sources_valid": (
        "All immutable review bundles, configured run IDs, decision loaders, and SHA-256 bindings must agree."
    ),
    "ingested_inputs_match_manifests": (
        "Every ingested static and dynamic Parquet must match its immutable ingestion manifest hash and row count."
    ),
    "all_raw_occurrences_have_candidate_dispositions": (
        "Every raw occurrence must be transformed, used by an explicit composite, or retired only under an all-missing precondition."
    ),
    "source_bindings_match_lossless_metadata": (
        "Every source field must match its exact raw name and occurrence metadata from the registry-review workbook."
    ),
    "row_conservation": (
        "Candidate harmonization must preserve every input row without filtering rows or stays."
    ),
    "common_candidate_schemas": (
        "All hospital outputs for a table must use one identical ordered candidate Arrow schema."
    ),
    "candidate_tokens_resolved": (
        "Unresolved non-empty candidate tokens require review before this candidate run can be frozen."
    ),
    "candidate_column_mappings_approved": (
        "Candidate column mappings inherited only from v2 or legacy require one consolidated approval."
    ),
    "candidate_value_types_approved": (
        "Candidate physical value types require one consolidated approval."
    ),
    "candidate_parsing_and_missing_policies_approved": (
        "Candidate numeric, missing-token, ratio, percentage, threshold, categorical, and temporal parsing policies require one consolidated approval."
    ),
    "candidate_units_and_semantics_approved": (
        "Pending units, percentage/fraction scales, ratio direction, temporal semantics, and semantic splits require consolidated review."
    ),
    "candidate_categorical_domains_approved": (
        "Preserved or provisionally translated categorical domains require consolidated review."
    ),
    "ordered_harmonized_union_schema_approved": (
        "The common ordered static and dynamic candidate schemas require an explicit freeze decision."
    ),
    "variable_dictionary_approved": (
        "Candidate definitions, representations, units, availability, caveats, and provenance require explicit approval."
    ),
    "run_scoped_outputs_are_isolated_and_nonpublishable": (
        "Candidate outputs must be immutable, run-scoped under v3, and explicitly unavailable to cleaned, derived, or publication stages."
    ),
}


@dataclass(frozen=True)
class HarmonizationDryRunPolicy:
    version: str
    evidence_run_ids: dict[str, str]
    decision_paths: dict[str, Path]
    decision_hashes: dict[str, str]
    approved_hospitals: tuple[str, ...]
    rows_per_batch: int
    compression: str
    maximum_token_examples: int
    maximum_categorical_values: int
    output_directory_name: str
    artifact_version: str
    private_artifact_version: str
    review_artifact_version: str
    output_status: str
    source_path: Path


@dataclass(frozen=True)
class HarmonizationDryRunConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root

    @property
    def data_root(self) -> Path:
        return self.harmonization_review.schema_tokens.ingestion.data_root

    @property
    def ingested_root(self) -> Path:
        return self.harmonization_review.schema_tokens.ingestion.output_root


@dataclass(frozen=True)
class SourcePlan:
    review_item_id: str
    hospital: str
    table: str
    physical_name: str
    raw_name: str
    occurrence: int
    target: str | None
    kind: str
    mapping_review_status: str
    parser_review_status: str
    proposed_action: str
    semantic_split_candidate_id: str | None
    semantic_split_proposed_value_type: str | None
    semantic_split_proposed_unit: str | None
    semantic_split_review_status: str | None


@dataclass(frozen=True)
class TargetSpec:
    table: str
    target: str
    kind: str
    arrow_type: pa.DataType
    candidate_value_type: str
    value_type_review_status: str
    candidate_unit: str
    unit_review_status: str
    source_review_item_ids: tuple[str, ...]
    source_raw_names: tuple[str, ...]


@dataclass
class TargetMetrics:
    hospital: str
    table: str
    target: str
    status_counts: Counter[str]
    nonmissing_output_count: int = 0
    numeric_value_count: int = 0
    numeric_sum: float = 0.0
    numeric_min: float | None = None
    numeric_max: float | None = None

    def add_numeric(self, value: float, count: int) -> None:
        self.numeric_value_count += count
        self.numeric_sum += value * count
        self.numeric_min = value if self.numeric_min is None else min(self.numeric_min, value)
        self.numeric_max = value if self.numeric_max is None else max(self.numeric_max, value)

    def as_row(self) -> dict[str, Any]:
        return {
            "hospital": self.hospital,
            "table": self.table,
            "target": self.target,
            "input_cell_count": sum(self.status_counts.values()),
            "status_counts_json": json.dumps(dict(sorted(self.status_counts.items()))),
            "unresolved_count": self.status_counts["unresolved"],
            "nonmissing_output_count": self.nonmissing_output_count,
            "numeric_value_count": self.numeric_value_count,
            "numeric_min": self.numeric_min,
            "numeric_max": self.numeric_max,
            "numeric_mean": (
                self.numeric_sum / self.numeric_value_count
                if self.numeric_value_count
                else None
            ),
        }


@dataclass(frozen=True)
class HarmonizationDryRunResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    candidate_directory: Path
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass(frozen=True)
class ReviewedPhaseDecision:
    version: str
    rule_id: str
    hospital: str
    table: str
    raw_name: str
    occurrence: int
    target: str
    categorical: CategoricalValueCandidate
    evidence_candidate_run_id: str
    source_path: Path

    def applies_to(self, plan: SourcePlan) -> bool:
        return (
            plan.hospital == self.hospital
            and plan.table == self.table
            and plan.raw_name == self.raw_name
            and plan.occurrence == self.occurrence
            and plan.target == self.target
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must contain non-empty strings")
    result = tuple(value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def load_harmonization_dry_run_config(path: str | Path) -> HarmonizationDryRunConfig:
    source = Path(path).expanduser().resolve()
    review = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "Harmonization dry-run configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonization_dry_run_policy", "config"), source
    )
    return HarmonizationDryRunConfig(review, policy_path)


def load_harmonization_dry_run_policy(path: str | Path) -> HarmonizationDryRunPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonization dry-run policy")
    if raw.get("harmonization_dry_run_policy_version") != "0.3":
        raise ConfigurationError("Harmonization dry-run policy version must be 0.3")
    if raw.get("status") != "approved_for_nonpublishable_streaming_candidate_harmonization":
        raise ConfigurationError("Harmonization dry-run policy is not approved")
    evidence = _mapping(raw.get("evidence_basis"), "dry_run.evidence_basis")
    expected_runs = {
        "inventory_run_id": "20260804T193415Z",
        "ingestion_audit_run_id": "20260805T060046Z",
        "schema_token_run_id": "20260805T090942Z",
        "registry_review_run_id": "20260805T104417Z",
        "composite_audit_run_id": "20260805T111026Z",
    }
    if evidence != expected_runs:
        raise ConfigurationError("Harmonization dry-run evidence basis changed")
    sources = _mapping(raw.get("decision_sources"), "dry_run.decision_sources")
    source_names = (
        "inventory_policy",
        "ingestion_policy",
        "schema_token_policy",
        "harmonization_review_policy",
        "candidate_contract",
        "composite_source_policy",
        "reviewed_static_decisions",
        "reviewed_icd10_contract",
        "reviewed_phase_decisions",
    )
    paths = {
        name: resolve_path(required_string(sources, name, "decision_sources"), source)
        for name in source_names
    }
    hashes = {
        name: required_string(sources, f"{name}_sha256", "decision_sources")
        for name in source_names
    }
    parsing = _mapping(raw.get("candidate_parsing"), "dry_run.candidate_parsing")
    expected_parsing = {
        "direct_numeric": True,
        "decimal_comma": True,
        "ratio_notation": True,
        "explicit_percentage_to_fraction": True,
        "threshold_boundary_value": True,
        "reviewed_numeric_lists": True,
        "reviewed_missing_sentinels": True,
        "candidate_textual_missing_to_null": True,
        "unreviewed_categorical_values": "preserve_trimmed",
        "temporal_values": "preserve_exact_string",
        "distribution_inferred_unit_conversion": False,
    }
    semantics = _mapping(raw.get("candidate_semantics"), "dry_run.candidate_semantics")
    expected_semantics = {
        "apply_reviewed_static_decisions": True,
        "apply_reviewed_icd10_contract": True,
        "apply_reviewed_composite_sources": True,
        "apply_reviewed_study_phase_decision": True,
        "apply_candidate_uk00_vt_semantic_split": True,
        "apply_candidate_categorical_translations": True,
        "preserve_reported_and_computed_driving_pressure_separately": True,
        "preserve_sofa_and_isofa_separately": True,
        "filter_rows": False,
        "filter_stays": False,
        "apply_ventilation_cohort": False,
        "mask_physiologic_outliers": False,
    }
    if parsing != expected_parsing or semantics != expected_semantics:
        raise ConfigurationError("Candidate dry-run behavior changed unexpectedly")
    identifier_contract = _mapping(
        raw.get("identifier_contract"), "dry_run.identifier_contract"
    )
    if identifier_contract != {
        "static_local_source": "reviewed_raw_pseudo_id",
        "dynamic_local_source": "ingestion_filename_stay_id_provenance",
        "dynamic_in_file_identifier_policy": "if_available_must_agree",
        "dynamic_raw_identifier_policy": "if_available_must_corroborate",
        "dynamic_static_membership_required": True,
        "global_identifier_formula": "<stay_id_local>:<approved_hospital_suffix>",
    }:
        raise ConfigurationError("Candidate identifier contract changed unexpectedly")
    scope = _mapping(raw.get("scope"), "dry_run.scope")
    if scope != {
        "allow_ingested_production_reads": True,
        "allow_private_review_evidence_reads": True,
        "allow_run_scoped_candidate_artifact_writes": True,
        "allow_source_mutation": False,
        "allow_ingested_artifact_overwrite": False,
        "allow_candidate_artifact_overwrite": False,
        "allow_cleaned_artifact_writes": False,
        "allow_derived_artifact_writes": False,
        "allow_publication": False,
        "allow_release_pointer": False,
    }:
        raise ConfigurationError("Harmonization dry run must remain isolated and nonpublishable")
    streaming = _mapping(raw.get("streaming"), "dry_run.streaming")
    rows_per_batch = streaming.get("rows_per_batch")
    max_examples = streaming.get("maximum_private_token_examples_per_column")
    max_categories = streaming.get("maximum_private_categorical_values_per_column")
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value <= 0
        for value in (rows_per_batch, max_examples, max_categories)
    ):
        raise ConfigurationError("Harmonization dry-run streaming limits are invalid")
    outputs = _mapping(raw.get("outputs"), "dry_run.outputs")
    if outputs.get("directory_name") != "harmonization_candidates" or outputs.get(
        "status"
    ) != "candidate_nonpublishable_requires_consolidated_review":
        raise ConfigurationError("Harmonization dry-run output boundary changed")
    hospitals = _string_list(raw.get("approved_hospitals"), "approved_hospitals")
    expected_hospitals = (
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    )
    if hospitals != expected_hospitals:
        raise ConfigurationError("Approved dry-run hospital set changed")
    return HarmonizationDryRunPolicy(
        version="0.3",
        evidence_run_ids=expected_runs,
        decision_paths=paths,
        decision_hashes=hashes,
        approved_hospitals=hospitals,
        rows_per_batch=rows_per_batch,
        compression=required_string(streaming, "parquet_compression", "streaming"),
        maximum_token_examples=max_examples,
        maximum_categorical_values=max_categories,
        output_directory_name=outputs["directory_name"],
        artifact_version=required_string(outputs, "artifact_version", "outputs"),
        private_artifact_version=required_string(
            outputs, "private_report_artifact_version", "outputs"
        ),
        review_artifact_version=required_string(
            outputs, "review_report_artifact_version", "outputs"
        ),
        output_status=outputs["status"],
        source_path=source,
    )


def load_reviewed_phase_decision(path: str | Path) -> ReviewedPhaseDecision:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed study-phase decision")
    if raw.get("reviewed_phase_decision_version") != "0.1":
        raise ConfigurationError("Reviewed study-phase decision version must be 0.1")
    if raw.get("status") != "human_approved_raw_v3":
        raise ConfigurationError("Study-phase decision is not approved")
    review = _mapping(raw.get("review"), "study_phase.review")
    if review != {
        "approved_by_role": "project_data_owner",
        "approved_at": "2026-08-06",
        "evidence_candidate_run_id": "20260806T072541Z",
        "evidence_hospital": "asic_UK03",
        "evidence_static_row_count": 1360,
        "evidence_mapped_phase_label_count": 1357,
        "evidence_approved_missing_count": 3,
        "evidence_unknown_count": 0,
        "semantic_evidence": "phase_specific_time_since_study_start_ordering",
    }:
        raise ConfigurationError("Study-phase review evidence changed unexpectedly")
    decision = _mapping(raw.get("decision"), "study_phase.decision")
    expected = {
        "rule_id": "HARM-STATIC-STUDY-PHASE-UK03-001",
        "hospital": "asic_UK03",
        "table": "static",
        "raw_name": "Phase",
        "occurrence": 1,
        "target": "study_implementation_phase",
        "output_value_type": "large_string",
        "output_unit": "not_applicable",
        "normalization": "strip_upper",
        "literal_empty_handling": "null",
        "source_absence_handling": "null",
        "unknown_nonempty_handling": "block",
    }
    if {key: decision.get(key) for key in expected} != expected:
        raise ConfigurationError("Reviewed study-phase decision changed unexpectedly")
    values = _mapping(decision.get("values"), "study_phase.decision.values")
    if values != {
        "K": "calibration",
        "RI": "roll_in",
        "QS": "app_implementation",
        "NAN": None,
    }:
        raise ConfigurationError("Reviewed study-phase categorical mapping changed")
    scope = _mapping(raw.get("scope"), "study_phase.scope")
    if scope != {
        "allow_candidate_harmonization": True,
        "allow_cleaning": False,
        "allow_derivation": False,
        "allow_publication": False,
    }:
        raise ConfigurationError("Reviewed study-phase scope changed unexpectedly")
    return ReviewedPhaseDecision(
        version="0.1",
        rule_id=expected["rule_id"],
        hospital=expected["hospital"],
        table=expected["table"],
        raw_name=expected["raw_name"],
        occurrence=1,
        target=expected["target"],
        categorical=CategoricalValueCandidate(
            review_status="human_approved_raw_v3_hospital_scope",
            normalization="strip_upper",
            values=values,
        ),
        evidence_candidate_run_id=review["evidence_candidate_run_id"],
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


def _arrow_type(value_type: str, kind: str) -> pa.DataType:
    if value_type == "list_float64":
        return pa.list_(pa.float64())
    if value_type == "list_large_string":
        return pa.list_(pa.large_string())
    if value_type == "fixed_size_list_bool_2":
        return pa.list_(pa.bool_(), 2)
    if value_type == "bool":
        return pa.bool_()
    if value_type == "float64" or kind == "numeric":
        return pa.float64()
    # Candidate temporal parsers remain deliberately deferred in this run.
    return pa.large_string()


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _missing_array(length: int, arrow_type: pa.DataType) -> pa.Array:
    if pa.types.is_fixed_size_list(arrow_type):
        # A fixed-size list must retain its positional width even when the
        # hospital has no source column for the conceptual variable.
        return pa.array(
            [[None] * arrow_type.list_size for _ in range(length)],
            type=arrow_type,
        )
    return pa.nulls(length, type=arrow_type)


def _field_metadata(spec: TargetSpec) -> dict[bytes, bytes]:
    return {
        b"asic_v3_stage": b"harmonization_candidate_dry_run",
        b"candidate_target": spec.target.encode(),
        b"candidate_kind": spec.kind.encode(),
        b"candidate_value_type": spec.candidate_value_type.encode(),
        b"value_type_review_status": spec.value_type_review_status.encode(),
        b"candidate_unit": spec.candidate_unit.encode(),
        b"unit_review_status": spec.unit_review_status.encode(),
        b"source_review_item_ids": json.dumps(spec.source_review_item_ids).encode(),
        b"source_raw_names": json.dumps(spec.source_raw_names, ensure_ascii=False).encode(),
    }


def _candidate_normalize(value: str, normalization: str) -> str:
    if normalization == "strip":
        return value.strip()
    if normalization == "strip_upper":
        return value.strip().upper()
    if normalization == "strip_lower":
        return value.strip().lower()
    if normalization == "exact":
        return value
    if normalization == "integral_binary_numeric":
        numeric = parse_direct_numeric(value)
        if numeric in {0.0, 1.0}:
            return str(int(numeric))
        return value.strip()
    raise HarmonizationError("Candidate categorical normalization is unsupported")


def _dictionary_transform(
    source: pa.Array,
    spec: TargetSpec,
    candidate_contract: CandidateContract,
    schema_policy: SchemaTokenPolicy,
    missing_tokens: frozenset[str],
    numeric_list_missing_tokens: tuple[str, ...],
    allowed_tokens: frozenset[str],
    categorical_override: CategoricalValueCandidate | None,
    metrics: TargetMetrics,
    token_examples: dict[tuple[str, str, str], Counter[str]],
    categorical_values: dict[tuple[str, str, str], Counter[str]],
    maximum_examples: int,
    maximum_categories: int,
) -> pa.Array:
    if not (pa.types.is_string(source.type) or pa.types.is_large_string(source.type)):
        raise HarmonizationError("Candidate harmonization requires lossless raw strings")
    encoded = pc.dictionary_encode(source)
    dictionary = encoded.dictionary.to_pylist()
    counts_by_index = {
        row["values"]: row["counts"] for row in pc.value_counts(encoded.indices).to_pylist()
    }
    null_count = int(counts_by_index.get(None, 0))
    if null_count:
        metrics.status_counts["source_schema_absent"] += null_count
    parsed_dictionary: list[Any] = []
    categorical = categorical_override or candidate_contract.categorical_for(
        spec.table, spec.target
    )
    example_key = (metrics.hospital, spec.table, spec.target)
    for index, raw in enumerate(dictionary):
        count = int(counts_by_index.get(index, 0))
        if not isinstance(raw, str):
            raise HarmonizationError("Lossless string dictionary contains a non-string value")
        if raw == "":
            parsed_dictionary.append(None)
            metrics.status_counts["literal_empty"] += count
            continue
        stripped = raw.strip()
        if spec.kind == "numeric":
            if stripped.casefold() in missing_tokens or stripped.casefold() in (
                schema_policy.textual_missing_candidates
            ):
                parsed_dictionary.append(None)
                status = (
                    "reviewed_missing_sentinel"
                    if stripped.casefold() in missing_tokens
                    else "candidate_textual_missing"
                )
                metrics.status_counts[status] += count
                continue
            if pa.types.is_list(spec.arrow_type):
                parsed_list = parse_numeric_list(raw, numeric_list_missing_tokens)
                if parsed_list.resolved:
                    value = [item for item in parsed_list.values if item is not None]
                    parsed_dictionary.append(value)
                    metrics.status_counts["reviewed_numeric_list"] += count
                    metrics.nonmissing_output_count += count
                    for item in value:
                        metrics.add_numeric(float(item), count)
                    continue
                parsed_dictionary.append(None)
                metrics.status_counts["unresolved"] += count
            else:
                parsers = (
                    ("direct_numeric", parse_direct_numeric),
                    ("decimal_comma", parse_decimal_comma_numeric),
                    ("ratio_notation", parse_ratio_numeric),
                    ("explicit_percentage_to_fraction", parse_explicit_percentage_fraction),
                    ("threshold_boundary_value", parse_threshold_boundary),
                )
                parsed_value: float | None = None
                status = "unresolved"
                for candidate_status, parser in parsers:
                    parsed_value = parser(raw)
                    if parsed_value is not None and math.isfinite(parsed_value):
                        status = candidate_status
                        break
                parsed_dictionary.append(parsed_value)
                metrics.status_counts[status] += count
                if parsed_value is not None:
                    metrics.nonmissing_output_count += count
                    metrics.add_numeric(parsed_value, count)
                    continue
            examples = token_examples[example_key]
            if raw not in examples and len(examples) < maximum_examples:
                examples[raw] = 0
            if raw in examples:
                examples[raw] += count
            continue
        if spec.kind == "categorical":
            categories = categorical_values[example_key]
            if raw not in categories and len(categories) < maximum_categories:
                categories[raw] = 0
            if raw in categories:
                categories[raw] += count
            if allowed_tokens and stripped not in allowed_tokens:
                parsed_dictionary.append(None)
                metrics.status_counts["unresolved"] += count
                examples = token_examples[example_key]
                if raw not in examples and len(examples) < maximum_examples:
                    examples[raw] = 0
                if raw in examples:
                    examples[raw] += count
                continue
            if categorical is None:
                parsed_dictionary.append(stripped)
                metrics.status_counts[
                    "reviewed_categorical_token_preserved"
                    if allowed_tokens
                    else "candidate_categorical_preserved"
                ] += count
                metrics.nonmissing_output_count += count
                continue
            normalized = _candidate_normalize(raw, categorical.normalization)
            if normalized in categorical.values:
                mapped = categorical.values[normalized]
                parsed_dictionary.append(mapped)
                metrics.status_counts["candidate_categorical_translation"] += count
                metrics.nonmissing_output_count += int(mapped is not None) * count
                continue
            parsed_dictionary.append(None)
            metrics.status_counts["unresolved"] += count
            examples = token_examples[example_key]
            if raw not in examples and len(examples) < maximum_examples:
                examples[raw] = 0
            if raw in examples:
                examples[raw] += count
            continue
        parsed_dictionary.append(raw if spec.kind in {"free_text", "temporal", "identifier"} else stripped)
        metrics.status_counts[
            "candidate_temporal_preserved"
            if spec.kind == "temporal"
            else "source_string_preserved"
        ] += count
        metrics.nonmissing_output_count += count
    dictionary_array = pa.array(parsed_dictionary, type=spec.arrow_type)
    return pc.take(dictionary_array, encoded.indices)


def _identifier_arrays(
    source: pa.Array,
    suffix: str,
    local_metrics: TargetMetrics,
    global_metrics: TargetMetrics,
    valid_status: str,
) -> tuple[pa.Array, pa.Array]:
    encoded = pc.dictionary_encode(source)
    dictionary = encoded.dictionary.to_pylist()
    counts = {
        row["values"]: row["counts"] for row in pc.value_counts(encoded.indices).to_pylist()
    }
    null_count = int(counts.get(None, 0))
    local_metrics.status_counts["source_schema_absent"] += null_count
    global_metrics.status_counts["source_schema_absent"] += null_count
    local_values: list[str | None] = []
    global_values: list[str | None] = []
    for index, raw in enumerate(dictionary):
        count = int(counts.get(index, 0))
        valid = isinstance(raw, str) and raw != "" and raw.strip() == raw and ":" not in raw
        if valid:
            local_values.append(raw)
            global_values.append(f"{raw}:{suffix}")
            local_metrics.status_counts[valid_status] += count
            global_metrics.status_counts[valid_status] += count
            local_metrics.nonmissing_output_count += count
            global_metrics.nonmissing_output_count += count
        else:
            local_values.append(None)
            global_values.append(None)
            status = "literal_empty" if raw == "" else "unresolved"
            local_metrics.status_counts[status] += count
            global_metrics.status_counts[status] += count
    return (
        pc.take(pa.array(local_values, type=pa.large_string()), encoded.indices),
        pc.take(pa.array(global_values, type=pa.large_string()), encoded.indices),
    )


def _nonempty_string_count(source: pa.Array) -> int:
    nonempty = pc.and_kleene(pc.is_valid(source), pc.not_equal(source, ""))
    return int(pc.sum(pc.cast(pc.fill_null(nonempty, False), pa.int64())).as_py() or 0)


def _all_optional_values_agree(
    optional: pa.Array,
    required: pa.Array,
) -> bool:
    if len(optional) != len(required):
        return False
    unavailable = pc.or_kleene(pc.is_null(optional), pc.equal(optional, ""))
    agreement = pc.or_kleene(unavailable, pc.equal(optional, required))
    return pc.all(pc.fill_null(agreement, False)).as_py() is True


def _arrays_equal_including_null(left: pa.Array, right: pa.Array) -> bool:
    if len(left) != len(right):
        return False
    agreement = pc.or_kleene(
        pc.equal(left, right),
        pc.and_kleene(pc.is_null(left), pc.is_null(right)),
    )
    return pc.all(pc.fill_null(agreement, False)).as_py() is True


def _validate_dynamic_identifier_evidence(
    filename_source: pa.Array,
    in_file_source: pa.Array,
    raw_corroborating_source: pa.Array | None,
) -> dict[str, int]:
    if filename_source.null_count or _nonempty_string_count(filename_source) != len(
        filename_source
    ):
        raise HarmonizationError(
            "Dynamic filename-derived stay identifier provenance is incomplete"
        )
    if not _all_optional_values_agree(in_file_source, filename_source):
        raise HarmonizationError(
            "Dynamic in-file and filename-derived stay identifiers disagree"
        )
    if raw_corroborating_source is not None and not _arrays_equal_including_null(
        raw_corroborating_source, in_file_source
    ):
        raise HarmonizationError(
            "Dynamic raw and ingested in-file stay identifier evidence disagree"
        )
    return {
        "filename_nonempty_row_count": len(filename_source),
        "in_file_nonempty_row_count": _nonempty_string_count(in_file_source),
        "raw_corroborating_nonempty_row_count": (
            _nonempty_string_count(raw_corroborating_source)
            if raw_corroborating_source is not None
            else 0
        ),
    }


def _require_dynamic_identifiers_in_static(
    local_identifiers: pa.Array,
    static_local_stay_ids: set[str],
) -> None:
    if local_identifiers.null_count:
        raise HarmonizationError(
            "Dynamic filename-derived stay identifier is invalid"
        )
    unknown_count = sum(
        1
        for value in pc.unique(local_identifiers).to_pylist()
        if not isinstance(value, str) or value not in static_local_stay_ids
    )
    if unknown_count:
        raise HarmonizationError(
            "Dynamic filename-derived stay identifiers do not resolve to static stays"
        )


def _binary_value(raw: object) -> tuple[bool | None, str]:
    if raw is None:
        return None, "source_schema_absent"
    if raw == "":
        return None, "literal_empty"
    if not isinstance(raw, str):
        return None, "unresolved"
    value = parse_direct_numeric(raw)
    if value == 0.0:
        return False, "candidate_binary"
    if value == 1.0:
        return True, "candidate_binary"
    return None, "unresolved"


def _composite_array(
    left: pa.Array,
    right: pa.Array,
    metrics: TargetMetrics,
    token_examples: dict[tuple[str, str, str], Counter[str]],
    maximum_examples: int,
) -> pa.Array:
    pairs: list[list[bool | None]] = []
    for raw_left, raw_right in zip(left.to_pylist(), right.to_pylist(), strict=True):
        value_left, status_left = _binary_value(raw_left)
        value_right, status_right = _binary_value(raw_right)
        pairs.append([value_left, value_right])
        if status_left == "unresolved" or status_right == "unresolved":
            metrics.status_counts["unresolved"] += 1
            for position, raw in enumerate((raw_left, raw_right)):
                if isinstance(raw, str):
                    key = (metrics.hospital, metrics.table, metrics.target)
                    examples = token_examples[key]
                    protected = f"position_{position}:{raw}"
                    if protected not in examples and len(examples) < maximum_examples:
                        examples[protected] = 0
                    if protected in examples:
                        examples[protected] += 1
        else:
            metrics.status_counts[f"pair_{status_left}_{status_right}"] += 1
            metrics.nonmissing_output_count += 1
    return pa.array(pairs, type=pa.list_(pa.bool_(), 2))


def _merge_icd_metrics(target: TargetMetrics, statuses: pa.Array) -> None:
    for row in pc.value_counts(statuses).to_pylist():
        value = row["values"]
        count = int(row["counts"])
        status = "unresolved" if value == "unresolved" else f"reviewed_icd10:{value}"
        target.status_counts[status] += count
        if value not in {None, "unresolved", "source_schema_absent", "literal_empty_missing", "approved_textual_missing"}:
            target.nonmissing_output_count += count


def _plan_specs(
    occurrences: list[dict[str, Any]],
    contract: CandidateContract,
    composite: CompositeSourceAuditPolicy,
    icd10: ReviewedICD10Contract,
) -> tuple[
    dict[tuple[str, str], tuple[TargetSpec, ...]],
    dict[tuple[str, str, str], tuple[SourcePlan, ...]],
    dict[str, SourcePlan],
]:
    source_plans: list[SourcePlan] = []
    by_id: dict[str, SourcePlan] = {}
    retirement_ids = {item.review_item_id for item in composite.retirements}
    composite_ids = set(composite.composite.source_review_item_ids)
    target_sources: dict[tuple[str, str], list[SourcePlan]] = defaultdict(list)
    groups: dict[tuple[str, str, str], list[SourcePlan]] = defaultdict(list)
    for row in occurrences:
        required_values = {
            key: row.get(key)
            for key in (
                "review_item_id",
                "hospital",
                "table",
                "physical_name",
                "raw_name",
                "raw_occurrence",
                "candidate_kind",
                "mapping_review_status",
                "parser_review_status",
                "proposed_action",
            )
        }
        if any(value is None for value in required_values.values()):
            raise HarmonizationError("Registry-review occurrence plan is incomplete")
        plan = SourcePlan(
            review_item_id=str(row["review_item_id"]),
            hospital=str(row["hospital"]),
            table=str(row["table"]),
            physical_name=str(row["physical_name"]),
            raw_name=str(row["raw_name"]),
            occurrence=int(row["raw_occurrence"]),
            target=row.get("candidate_target"),
            kind=str(row["candidate_kind"]),
            mapping_review_status=str(row["mapping_review_status"]),
            parser_review_status=str(row["parser_review_status"]),
            proposed_action=str(row["proposed_action"]),
            semantic_split_candidate_id=row.get("semantic_split_candidate_id"),
            semantic_split_proposed_value_type=row.get(
                "semantic_split_proposed_value_type"
            ),
            semantic_split_proposed_unit=row.get("semantic_split_proposed_unit"),
            semantic_split_review_status=row.get("semantic_split_review_status"),
        )
        if plan.review_item_id in by_id:
            raise HarmonizationError("Registry-review item IDs are not unique")
        semantic = (
            contract.semantic_split_for(plan.hospital, plan.table, plan.target)
            if isinstance(plan.target, str)
            else None
        )
        if semantic is not None:
            plan = SourcePlan(**{**asdict(plan), "target": semantic.proposed_target})
        source_plans.append(plan)
        by_id[plan.review_item_id] = plan
        if plan.review_item_id in retirement_ids or plan.review_item_id in composite_ids:
            continue
        if plan.target is None:
            if plan.proposed_action != "drop_after_all_missing_precondition":
                raise HarmonizationError("A raw occurrence lacks a candidate disposition")
            continue
        groups[(plan.hospital, plan.table, plan.target)].append(plan)
        target_sources[(plan.table, plan.target)].append(plan)
    for key, plans in groups.items():
        if len(plans) != 1:
            raise HarmonizationError(f"Candidate alias group remains unresolved: {key}")

    specs: dict[tuple[str, str], list[TargetSpec]] = defaultdict(list)
    for (table, target), plans in target_sources.items():
        kinds = {item.kind for item in plans}
        if len(kinds) != 1:
            raise HarmonizationError("Candidate target kinds conflict")
        kind = next(iter(kinds))
        proposal = contract.proposal_for(table, target, kind)
        semantic_plans = [
            item for item in plans if item.semantic_split_candidate_id is not None
        ]
        if semantic_plans:
            semantic_types = {
                item.semantic_split_proposed_value_type for item in semantic_plans
            }
            semantic_units = {
                item.semantic_split_proposed_unit for item in semantic_plans
            }
            if len(semantic_plans) != len(plans) or len(semantic_types) != 1 or len(
                semantic_units
            ) != 1 or None in semantic_types or None in semantic_units:
                raise HarmonizationError("Candidate semantic-split contract conflicts")
            value_type = next(iter(semantic_types))
            unit = next(iter(semantic_units))
            value_type_status = "requires_human_review"
            unit_status = "requires_human_review"
        else:
            value_type = proposal.value_type
            unit = proposal.unit
            value_type_status = proposal.value_type_status
            unit_status = proposal.unit_status
        specs[(table, target)].append(
            TargetSpec(
                table=table,
                target=target,
                kind=kind,
                arrow_type=_arrow_type(value_type, kind),
                candidate_value_type=value_type,
                value_type_review_status=value_type_status,
                candidate_unit=unit,
                unit_review_status=unit_status,
                source_review_item_ids=tuple(sorted(item.review_item_id for item in plans)),
                source_raw_names=tuple(sorted({item.raw_name for item in plans})),
            )
        )
    additional = (
        TargetSpec("static", "stay_id_local", "identifier", pa.large_string(), "large_string", "approved", "not_applicable", "approved", (), ()),
        TargetSpec("static", "hospital_id", "identifier", pa.large_string(), "large_string", "approved", "not_applicable", "approved", (), ()),
        TargetSpec("dynamic", "stay_id_local", "identifier", pa.large_string(), "large_string", "human_approved_raw_v3", "not_applicable", "approved", (), ()),
        TargetSpec("dynamic", "hospital_id", "identifier", pa.large_string(), "large_string", "approved", "not_applicable", "approved", (), ()),
        TargetSpec("static", icd10.source_text_target, "free_text", pa.large_string(), "large_string", "approved", "not_applicable", "approved", (), ()),
    )
    for spec in additional:
        specs[(spec.table, spec.target)].append(spec)
    composite_spec = TargetSpec(
        "dynamic",
        composite.composite.target,
        "categorical",
        pa.list_(pa.bool_(), 2),
        "fixed_size_list_bool_2",
        "approved",
        "not_applicable",
        "approved",
        composite.composite.source_review_item_ids,
        tuple(by_id[item].raw_name for item in composite.composite.source_review_item_ids),
    )
    specs[("dynamic", composite_spec.target)] = [composite_spec]
    icd_source_plans = target_sources.get(("static", icd10.list_target), [])
    specs[("static", icd10.list_target)] = [
        TargetSpec(
            "static",
            icd10.list_target,
            "free_text",
            pa.list_(pa.large_string()),
            "list_large_string",
            "approved",
            "not_applicable",
            "approved",
            tuple(sorted(item.review_item_id for item in icd_source_plans)),
            tuple(sorted({item.raw_name for item in icd_source_plans})),
        )
    ]
    resolved_specs: dict[tuple[str, str], TargetSpec] = {}
    for key, candidates in specs.items():
        unique = {
            (str(item.arrow_type), item.kind, item.candidate_value_type, item.candidate_unit)
            for item in candidates
        }
        if len(unique) != 1:
            raise HarmonizationError(f"Candidate output contract conflicts for {key}")
        first = candidates[0]
        merged_ids = tuple(sorted({value for item in candidates for value in item.source_review_item_ids}))
        merged_names = tuple(sorted({value for item in candidates for value in item.source_raw_names}))
        resolved_specs[key] = TargetSpec(
            table=first.table,
            target=first.target,
            kind=first.kind,
            arrow_type=first.arrow_type,
            candidate_value_type=first.candidate_value_type,
            value_type_review_status=(
                "human_approved_raw_v3"
                if first.target in {"stay_id_global", "stay_id_local"}
                else first.value_type_review_status
            ),
            candidate_unit=first.candidate_unit,
            unit_review_status=(
                "approved"
                if first.target in {"stay_id_global", "stay_id_local"}
                else first.unit_review_status
            ),
            source_review_item_ids=merged_ids,
            source_raw_names=merged_names,
        )
    table_specs: dict[tuple[str, str], tuple[TargetSpec, ...]] = {}
    for table in ("static", "dynamic"):
        selected = [item for (item_table, _), item in resolved_specs.items() if item_table == table]
        leading = ("stay_id_global", "stay_id_local", "hospital_id")
        selected.sort(key=lambda item: (leading.index(item.target) if item.target in leading else len(leading), item.target))
        table_specs[(table, "all")] = tuple(selected)
    return table_specs, {key: tuple(value) for key, value in groups.items()}, by_id


def _output_schema(table: str, specs: tuple[TargetSpec, ...]) -> pa.Schema:
    fields = [
        pa.field(spec.target, spec.arrow_type, nullable=True, metadata=_field_metadata(spec))
        for spec in specs
    ]
    provenance_types = {
        "__v3_source_file_id": pa.string(),
        "__v3_source_file_order": pa.int32(),
        "__v3_source_row_number": pa.int64(),
        "__v3_source_order": pa.int64(),
        "__v3_source_schema_variant_id": pa.string(),
    }
    fields.extend(
        pa.field(
            name,
            provenance_types[name],
            nullable=False,
            metadata={b"asic_v3_role": b"source_provenance"},
        )
        for name in PROVENANCE_FIELDS
    )
    return pa.schema(
        fields,
        metadata={
            b"asic_v3_stage": b"harmonization_candidate_dry_run",
            b"asic_v3_table": table.encode(),
            b"publication_ready": b"false",
            b"analysis_ready": b"false",
        },
    )


def _candidate_dictionary_rows(
    table_specs: dict[tuple[str, str], tuple[TargetSpec, ...]],
    by_id: dict[str, SourcePlan],
    hospitals: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in ("static", "dynamic"):
        for position, spec in enumerate(table_specs[(table, "all")], start=1):
            source_hospitals = sorted(
                {
                    by_id[item].hospital
                    for item in spec.source_review_item_ids
                    if item in by_id
                }
            )
            if spec.target in {
                "stay_id_global",
                "stay_id_local",
                "hospital_id",
            }:
                source_hospitals = list(hospitals)
            elif not source_hospitals and spec.target in {
                "stay_id_local",
                "hospital_id",
                "icd10_codes_source_text",
            }:
                source_hospitals = list(hospitals)
            definition = STATIC_DRAFT_DEFINITIONS.get(spec.target)
            rows.append(
                {
                    "table": table,
                    "ordered_position": position,
                    "variable": spec.target,
                    "candidate_arrow_type": str(spec.arrow_type),
                    "candidate_value_type": spec.candidate_value_type,
                    "candidate_unit": spec.candidate_unit,
                    "definition": (
                        definition
                        if definition is not None
                        else (
                            f"Candidate harmonized representation of {spec.target}; "
                            "clinical definition requires consolidated review."
                        )
                    ),
                    "definition_status": (
                        "reviewed_static_draft_requires_final_approval"
                        if definition is not None
                        else "pending_consolidated_review"
                    ),
                    "value_type_review_status": spec.value_type_review_status,
                    "unit_review_status": spec.unit_review_status,
                    "source_hospitals": source_hospitals,
                    "source_hospital_count": len(source_hospitals),
                    "source_review_item_ids": spec.source_review_item_ids,
                    "source_raw_names": spec.source_raw_names,
                    "source_occurrence_count": len(spec.source_review_item_ids),
                    "source_strategy": (
                        "reviewed_static_raw_identifier"
                        if table == "static"
                        and spec.target in {"stay_id_global", "stay_id_local"}
                        else (
                            "reviewed_dynamic_filename_identifier_provenance"
                            if table == "dynamic"
                            and spec.target in {"stay_id_global", "stay_id_local"}
                            else "raw_occurrence_mapping_or_reviewed_composite"
                        )
                    ),
                    "candidate_stage": "harmonization_candidate_dry_run",
                    "publication_ready": False,
                }
            )
    return rows


def _source_missing_tokens(
    plan: SourcePlan,
    schema_policy: SchemaTokenPolicy,
    static_registry: ReviewedStaticDecisionRegistry,
) -> tuple[frozenset[str], tuple[str, ...], frozenset[str]]:
    missing: set[str] = set()
    list_missing: tuple[str, ...] = ()
    allowed: frozenset[str] = frozenset()
    rule = schema_policy.candidate_for(plan.table, plan.raw_name, plan.occurrence, plan.hospital)
    if rule is not None:
        missing.update(value.strip().casefold() for value in rule.approved_missing_sentinel_tokens)
        list_missing = rule.approved_list_missing_tokens
        allowed = frozenset(rule.allowed_tokens)
    if plan.table == "static":
        decision = static_registry.rule_for(plan.hospital, plan.table, plan.raw_name, plan.occurrence)
        if decision is not None and hasattr(decision, "approved_missing_tokens"):
            missing.update(
                value.strip().casefold()
                for value in getattr(decision, "approved_missing_tokens")
            )
    return frozenset(missing), list_missing, allowed


def _load_and_validate_evidence(
    config: HarmonizationDryRunConfig,
    policy: HarmonizationDryRunPolicy,
    registry_review_run_id: str,
    ingestion_audit_run_id: str,
) -> tuple[
    list[dict[str, Any]],
    InventoryPolicy,
    IngestionPolicy,
    SchemaTokenPolicy,
    CandidateContract,
    CompositeSourceAuditPolicy,
    ReviewedStaticDecisionRegistry,
    ReviewedICD10Contract,
    ReviewedICD10PrivateEvidence,
    ReviewedPhaseDecision,
    dict[str, str],
]:
    if registry_review_run_id != policy.evidence_run_ids["registry_review_run_id"]:
        raise HarmonizationError("Unexpected registry-review evidence run")
    if ingestion_audit_run_id != policy.evidence_run_ids["ingestion_audit_run_id"]:
        raise HarmonizationError("Unexpected ingestion-audit evidence run")
    if not RUN_ID_PATTERN.fullmatch(registry_review_run_id) or not RUN_ID_PATTERN.fullmatch(
        ingestion_audit_run_id
    ):
        raise HarmonizationError("Dry-run evidence run ID is invalid")
    observed_hashes = {
        name: sha256_file(path) if path.is_file() else "missing"
        for name, path in policy.decision_paths.items()
    }
    if observed_hashes != policy.decision_hashes:
        raise HarmonizationError("A dry-run decision source hash changed")
    inventory = load_inventory_policy(policy.decision_paths["inventory_policy"])
    ingestion = load_ingestion_policy(policy.decision_paths["ingestion_policy"])
    schema = load_schema_token_policy(policy.decision_paths["schema_token_policy"])
    review = load_harmonization_review_policy(
        policy.decision_paths["harmonization_review_policy"]
    )
    composite = load_composite_source_audit_policy(
        policy.decision_paths["composite_source_policy"]
    )
    static_registry = load_reviewed_static_decision_registry(
        policy.decision_paths["reviewed_static_decisions"]
    )
    icd10 = load_reviewed_icd10_contract(
        policy.decision_paths["reviewed_icd10_contract"]
    )
    phase_decision = load_reviewed_phase_decision(
        policy.decision_paths["reviewed_phase_decisions"]
    )
    private = (
        config.reports_root
        / "private"
        / "harmonization_registry_review"
        / registry_review_run_id
    )
    review_json = (
        config.reports_root
        / "review"
        / "harmonization_registry_review"
        / f"{registry_review_run_id}.json"
    )
    manifest_path = private / "harmonization_registry_review_manifest.json"
    occurrences_path = private / "occurrences.parquet"
    variables_path = private / "variables.parquet"
    required = (manifest_path, occurrences_path, variables_path, review_json)
    if any(not path.is_file() for path in required):
        raise HarmonizationError("Dry-run registry-review evidence is incomplete")
    manifest = _read_json(manifest_path, "Registry-review manifest")
    sanitized = _read_json(review_json, "Registry-review report")
    occurrences = pq.read_table(occurrences_path).to_pylist()
    if (
        manifest.get("artifact") != "asic_v3_harmonization_registry_review_private"
        or manifest.get("artifact_version") != "0.3"
        or manifest.get("dataset_context") != config.dataset_context
        or manifest.get("occurrence_review_row_count") != len(occurrences)
        or sanitized.get("metrics", {}).get("technical_blocking_finding_count") != 0
    ):
        raise HarmonizationError("Dry-run registry-review evidence is incompatible")
    ingestion_private = (
        config.reports_root / "private" / "ingestion_audit" / ingestion_audit_run_id
    )
    ingestion_review = (
        config.reports_root / "review" / "ingestion_audit" / f"{ingestion_audit_run_id}.json"
    )
    if not (ingestion_private / "ingestion_audit_manifest.json").is_file() or not ingestion_review.is_file():
        raise HarmonizationError("Dry-run ingestion-audit evidence is incomplete")
    ingestion_manifest = _read_json(
        ingestion_private / "ingestion_audit_manifest.json", "Ingestion-audit manifest"
    )
    if (
        ingestion_manifest.get("artifact") != "asic_v3_ingestion_audit_private"
        or ingestion_manifest.get("dataset_context") != config.dataset_context
        or set(ingestion_manifest.get("hospital_evidence", {}))
        != set(policy.approved_hospitals)
    ):
        raise HarmonizationError("Dry-run ingestion-audit evidence is incompatible")
    composite_review = (
        config.reports_root
        / "review"
        / "composite_source_audit"
        / f"{policy.evidence_run_ids['composite_audit_run_id']}.json"
    )
    if not composite_review.is_file():
        raise HarmonizationError("Passing composite-source audit is unavailable")
    composite_payload = _read_json(composite_review, "Composite-source audit review")
    if composite_payload.get("overall_status") != "pass":
        raise HarmonizationError("Composite-source audit did not pass")
    icd_examples = (
        config.reports_root
        / "private"
        / "icd10_component_detail_audit"
        / str(icd10.detail_audit_run_id)
        / "examples.parquet"
    )
    private_icd = load_reviewed_icd10_private_evidence(icd_examples, icd10)
    return (
        occurrences,
        inventory,
        ingestion,
        schema,
        review.candidate_contract,
        composite,
        static_registry,
        icd10,
        private_icd,
        phase_decision,
        observed_hashes,
    )


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 consolidated harmonization dry-run review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Candidate run ID: `{payload['run_id']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        "- Candidate data status: **NON-PUBLISHABLE**",
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
    lines.extend(["", "## Run totals", ""])
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    lines.extend(["", "## Hospital outputs", ""])
    for hospital, values in payload["hospital_summaries"].items():
        lines.append(
            f"- `{hospital}`: static rows `{values['static_rows']}`; dynamic rows "
            f"`{values['dynamic_rows']}`; unresolved cells `{values['unresolved_cells']}`"
        )
    lines.extend(["", "## Consolidated pending decisions", ""])
    for key, values in payload["pending_decisions"].items():
        lines.append(f"- {key}: `{len(values)}` — {', '.join(f'`{item}`' for item in values)}")
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the consolidated private exceptions, categorical domains, numeric profiles, and proposed schema. Candidate Parquet remains non-publishable until a final registry decision and conservation audit are approved."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def run_harmonization_dry_run(
    config: HarmonizationDryRunConfig,
    registry_review_run_id: str,
    ingestion_audit_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> HarmonizationDryRunResult:
    policy = load_harmonization_dry_run_policy(config.policy_path)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid harmonization dry-run ID")
    output_parent = config.data_root / policy.output_directory_name
    final_output = output_parent / selected_run
    staging_output = output_parent / f".{selected_run}.incomplete"
    private_dir = config.reports_root / "private" / "harmonization_dry_run" / selected_run
    review_dir = config.reports_root / "review" / "harmonization_dry_run"
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (final_output, staging_output, private_dir, review_json, review_md)):
        raise HarmonizationError("Harmonization dry-run ID already exists and will not be overwritten")
    (
        occurrences,
        inventory_policy,
        ingestion_policy,
        schema_policy,
        candidate_contract,
        composite_policy,
        static_registry,
        icd10_contract,
        icd10_private,
        phase_decision,
        decision_hashes,
    ) = _load_and_validate_evidence(
        config, policy, registry_review_run_id, ingestion_audit_run_id
    )
    table_specs, source_groups, by_id = _plan_specs(
        occurrences, candidate_contract, composite_policy, icd10_contract
    )
    dictionary_rows = _candidate_dictionary_rows(
        table_specs, by_id, policy.approved_hospitals
    )
    schemas = {
        table: _output_schema(table, table_specs[(table, "all")])
        for table in ("static", "dynamic")
    }
    mapped_hospitals = {
        mapping.canonical_hospital_id: mapping.source_folder
        for mapping in inventory_policy.hospital_mappings
        if mapping.cohort_action == "include"
    }
    if tuple(mapped_hospitals) != policy.approved_hospitals:
        raise HarmonizationError("Inventory hospital mapping differs from dry-run scope")
    if static_registry.identifier_decision is None:
        raise HarmonizationError("The reviewed stay-identifier contract is unavailable")
    suffixes = dict(static_registry.identifier_decision.hospital_suffixes)
    if set(suffixes) != set(policy.approved_hospitals):
        raise HarmonizationError("Reviewed stay-identifier suffixes are incomplete")
    filename_identifier_column = ingestion_policy.provenance_columns.get(
        "filename_stay_id"
    )
    in_file_identifier_column = ingestion_policy.provenance_columns.get(
        "in_file_stay_id"
    )
    if not isinstance(filename_identifier_column, str) or not isinstance(
        in_file_identifier_column, str
    ):
        raise HarmonizationError(
            "Approved ingestion identifier provenance columns are unavailable"
        )
    output_parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_parent.chmod(0o700)
    staging_output.mkdir(mode=0o700)
    token_examples: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    categorical_values: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    metrics_by_key: dict[tuple[str, str, str], TargetMetrics] = {}
    hospital_manifests: list[dict[str, Any]] = []
    input_hash_failures = 0
    binding_failures = 0
    disposition_failures = 0
    row_failures = 0
    retirement_nonempty_count = 0
    retirement_ids = {item.review_item_id for item in composite_policy.retirements}
    composite_ids = set(composite_policy.composite.source_review_item_ids)
    for hospital in policy.approved_hospitals:
        static_local_stay_ids: set[str] = set()
        input_dir = config.ingested_root / hospital
        manifest_path = input_dir / "ingestion_manifest.json"
        if not manifest_path.is_file():
            raise HarmonizationError("An approved hospital ingestion manifest is unavailable")
        input_manifest = _read_json(manifest_path, "Hospital ingestion manifest")
        if (
            input_manifest.get("artifact") != "asic_v3_lossless_hospital_ingestion"
            or input_manifest.get("artifact_version") != "0.2"
            or input_manifest.get("hospital", {}).get("canonical_hospital_id") != hospital
        ):
            raise HarmonizationError("Hospital ingestion manifest is incompatible")
        hospital_output = staging_output / hospital
        hospital_output.mkdir(mode=0o700)
        hospital_summary: dict[str, Any] = {
            "hospital": hospital,
            "inputs": {},
            "outputs": {},
            "publication_ready": False,
        }
        for table in ("static", "dynamic"):
            input_path = input_dir / f"{table}.parquet"
            expected_hash = input_manifest.get("outputs", {}).get(table, {}).get("sha256")
            observed_hash = sha256_file(input_path) if input_path.is_file() else "missing"
            input_hash_failures += int(observed_hash != expected_hash)
            parquet = pq.ParquetFile(input_path)
            expected_rows = int(input_manifest["tables"][table]["output_row_count"])
            if parquet.metadata.num_rows != expected_rows:
                raise HarmonizationError("Ingested row count changed after audit")
            table_occurrences = [
                plan
                for plan in by_id.values()
                if plan.hospital == hospital and plan.table == table
            ]
            raw_fields = [
                field
                for field in parquet.schema_arrow
                if (field.metadata or {}).get(b"asic_v3_role") == b"raw_clinical_string"
            ]
            if len(raw_fields) != len(table_occurrences):
                raise HarmonizationError("Registry-review occurrence coverage differs from ingested schema")
            for plan in table_occurrences:
                index = parquet.schema_arrow.get_field_index(plan.physical_name)
                if index < 0:
                    binding_failures += 1
                    continue
                field = parquet.schema_arrow.field(index)
                metadata = field.metadata or {}
                if (
                    metadata.get(b"raw_name", b"").decode("utf-8") != plan.raw_name
                    or int(metadata.get(b"raw_occurrence", b"0")) != plan.occurrence
                ):
                    binding_failures += 1
            output_path = hospital_output / f"{table}.parquet"
            schema = schemas[table]
            written_rows = 0
            target_id_plans = [
                plan
                for plan in table_occurrences
                if plan.target == "stay_id_global"
            ]
            if any(
                plan.raw_name not in IDENTIFIER_RAW_NAMES
                for plan in target_id_plans
            ):
                raise HarmonizationError(
                    "A stay identifier candidate uses an unapproved raw source"
                )
            if table == "static" and len(target_id_plans) != 1:
                raise HarmonizationError(
                    "Exactly one reviewed raw stay identifier source is required per static table"
                )
            if table == "dynamic" and len(target_id_plans) > 1:
                raise HarmonizationError(
                    "At most one corroborating raw stay identifier is allowed per dynamic table"
                )
            id_plan = target_id_plans[0] if target_id_plans else None
            identifier_evidence = {
                "source_strategy": (
                    "reviewed_static_raw_identifier"
                    if table == "static"
                    else "reviewed_dynamic_filename_identifier_provenance"
                ),
                "raw_identifier_occurrence_count": len(target_id_plans),
                "filename_nonempty_row_count": 0,
                "in_file_nonempty_row_count": 0,
                "raw_corroborating_nonempty_row_count": 0,
                "dynamic_identifier_not_in_static_count": 0,
            }
            icd_plans = [
                plan for plan in table_occurrences if plan.target == icd10_contract.list_target
            ]
            regular_groups = {
                target: plans
                for (group_hospital, group_table, target), plans in source_groups.items()
                if group_hospital == hospital and group_table == table
                and target != "stay_id_global"
                and target != icd10_contract.list_target
            }
            with pq.ParquetWriter(output_path, schema, compression=policy.compression) as writer:
                for batch in parquet.iter_batches(
                    batch_size=policy.rows_per_batch, use_threads=True
                ):
                    nrows = batch.num_rows
                    arrays: dict[str, pa.Array] = {}
                    local_metrics = metrics_by_key.setdefault(
                        (hospital, table, "stay_id_local"),
                        TargetMetrics(hospital, table, "stay_id_local", Counter()),
                    )
                    global_metrics = metrics_by_key.setdefault(
                        (hospital, table, "stay_id_global"),
                        TargetMetrics(hospital, table, "stay_id_global", Counter()),
                    )
                    if table == "static":
                        assert id_plan is not None
                        raw_identifier_index = batch.schema.get_field_index(
                            id_plan.physical_name
                        )
                        if raw_identifier_index < 0:
                            raise HarmonizationError(
                                "Reviewed static stay identifier source is unavailable"
                            )
                        id_source = batch.column(
                            raw_identifier_index
                        )
                        valid_identifier_status = "reviewed_static_raw_identifier"
                    else:
                        filename_index = batch.schema.get_field_index(
                            filename_identifier_column
                        )
                        in_file_index = batch.schema.get_field_index(
                            in_file_identifier_column
                        )
                        if filename_index < 0 or in_file_index < 0:
                            raise HarmonizationError(
                                "Dynamic ingested identifier provenance columns are unavailable"
                            )
                        filename_source = batch.column(filename_index)
                        in_file_source = batch.column(in_file_index)
                        raw_corroborating_source = None
                        if id_plan is not None:
                            corroborating_index = batch.schema.get_field_index(
                                id_plan.physical_name
                            )
                            if corroborating_index < 0:
                                raise HarmonizationError(
                                    "Corroborating dynamic raw stay identifier source is unavailable"
                                )
                            raw_corroborating_source = batch.column(
                                corroborating_index
                            )
                        evidence_counts = _validate_dynamic_identifier_evidence(
                            filename_source,
                            in_file_source,
                            raw_corroborating_source,
                        )
                        for key, value in evidence_counts.items():
                            identifier_evidence[key] += value
                        id_source = filename_source
                        valid_identifier_status = (
                            "reviewed_dynamic_filename_identifier"
                        )
                    local, global_id = _identifier_arrays(
                        id_source,
                        suffixes[hospital],
                        local_metrics,
                        global_metrics,
                        valid_identifier_status,
                    )
                    if table == "static":
                        static_local_stay_ids.update(
                            value
                            for value in pc.unique(local).to_pylist()
                            if isinstance(value, str)
                        )
                    else:
                        _require_dynamic_identifiers_in_static(
                            local, static_local_stay_ids
                        )
                    arrays["stay_id_local"] = local
                    arrays["stay_id_global"] = global_id
                    arrays["hospital_id"] = pa.repeat(pa.scalar(hospital, pa.large_string()), nrows)
                    hospital_metrics = metrics_by_key.setdefault(
                        (hospital, table, "hospital_id"),
                        TargetMetrics(hospital, table, "hospital_id", Counter()),
                    )
                    hospital_metrics.status_counts["approved_inventory_mapping"] += nrows
                    hospital_metrics.nonmissing_output_count += nrows
                    for target, plans in regular_groups.items():
                        if len(plans) != 1:
                            disposition_failures += 1
                            continue
                        plan = plans[0]
                        spec = next(item for item in table_specs[(table, "all")] if item.target == target)
                        source = batch.column(batch.schema.get_field_index(plan.physical_name))
                        target_metrics = metrics_by_key.setdefault(
                            (hospital, table, target),
                            TargetMetrics(hospital, table, target, Counter()),
                        )
                        missing_tokens, list_missing, allowed_tokens = _source_missing_tokens(
                            plan, schema_policy, static_registry
                        )
                        arrays[target] = _dictionary_transform(
                            source,
                            spec,
                            candidate_contract,
                            schema_policy,
                            missing_tokens,
                            list_missing,
                            allowed_tokens,
                            (
                                phase_decision.categorical
                                if phase_decision.applies_to(plan)
                                else None
                            ),
                            target_metrics,
                            token_examples,
                            categorical_values,
                            policy.maximum_token_examples,
                            policy.maximum_categorical_values,
                        )
                    if table == "dynamic" and hospital == composite_policy.composite.hospital:
                        source_plans = [by_id[item] for item in composite_policy.composite.source_review_item_ids]
                        composite_metrics = metrics_by_key.setdefault(
                            (hospital, table, composite_policy.composite.target),
                            TargetMetrics(hospital, table, composite_policy.composite.target, Counter()),
                        )
                        arrays[composite_policy.composite.target] = _composite_array(
                            batch.column(batch.schema.get_field_index(source_plans[0].physical_name)),
                            batch.column(batch.schema.get_field_index(source_plans[1].physical_name)),
                            composite_metrics,
                            token_examples,
                            policy.maximum_token_examples,
                        )
                    if table == "static":
                        if len(icd_plans) != 1:
                            raise HarmonizationError("Exactly one ICD-10 source is required per static hospital")
                        icd_source = batch.column(batch.schema.get_field_index(icd_plans[0].physical_name))
                        icd = harmonize_reviewed_static_icd10_batch(
                            icd_source, hospital, icd10_contract, icd10_private
                        )
                        arrays[icd10_contract.source_text_target] = icd.source_text
                        arrays[icd10_contract.list_target] = icd.codes
                        for target in (icd10_contract.source_text_target, icd10_contract.list_target):
                            target_metrics = metrics_by_key.setdefault(
                                (hospital, table, target),
                                TargetMetrics(hospital, table, target, Counter()),
                            )
                            _merge_icd_metrics(target_metrics, icd.parse_status)
                    for plan in table_occurrences:
                        if plan.review_item_id in retirement_ids or (
                            plan.target is None
                            and plan.proposed_action == "drop_after_all_missing_precondition"
                        ):
                            values = batch.column(batch.schema.get_field_index(plan.physical_name))
                            for row in pc.value_counts(values).to_pylist():
                                raw = row["values"]
                                if isinstance(raw, str) and raw != "":
                                    retirement_nonempty_count += int(row["counts"])
                    for spec in table_specs[(table, "all")]:
                        if spec.target not in arrays:
                            missing_metrics = metrics_by_key.setdefault(
                                (hospital, table, spec.target),
                                TargetMetrics(
                                    hospital, table, spec.target, Counter()
                                ),
                            )
                            missing_metrics.status_counts[
                                "hospital_source_unavailable"
                            ] += nrows
                            arrays[spec.target] = _missing_array(
                                nrows, spec.arrow_type
                            )
                    for name in PROVENANCE_FIELDS:
                        arrays[name] = batch.column(batch.schema.get_field_index(name))
                    output_batch = pa.RecordBatch.from_arrays(
                        [arrays[name] for name in schema.names], schema=schema
                    )
                    writer.write_batch(output_batch)
                    written_rows += nrows
                    if progress is not None and written_rows % (policy.rows_per_batch * 20) == 0:
                        progress(
                            f"harmonization_dry_run_progress hospital={hospital} table={table} rows={written_rows}"
                        )
            output_path.chmod(0o600)
            row_failures += int(written_rows != expected_rows)
            hospital_summary["inputs"][table] = {
                "path": str(input_path),
                "sha256": observed_hash,
                "row_count": expected_rows,
            }
            hospital_summary["outputs"][table] = {
                "path": str(final_output / hospital / f"{table}.parquet"),
                "sha256": sha256_file(output_path),
                "row_count": written_rows,
                "schema_sha256": _schema_digest(schema),
                "identifier_evidence": identifier_evidence,
            }
        _write_json(hospital_output / "candidate_manifest.json", hospital_summary, 0o600)
        hospital_manifests.append(hospital_summary)
        if progress is not None:
            progress(f"harmonization_dry_run_hospital_complete={hospital}")
    if any((input_hash_failures, binding_failures, disposition_failures, row_failures, retirement_nonempty_count)):
        # Preserve the explicitly named incomplete directory for owner diagnosis.
        raise HarmonizationError("Candidate harmonization encountered a technical conservation failure")
    generated = utc_timestamp()
    target_metrics_rows = [
        metrics.as_row()
        for _, metrics in sorted(metrics_by_key.items())
    ]
    unresolved_count = sum(row["unresolved_count"] for row in target_metrics_rows)
    unresolved_rows = [
        {
            "hospital": hospital,
            "table": table,
            "target": target,
            "raw_token": token,
            "count": count,
        }
        for (hospital, table, target), values in sorted(token_examples.items())
        for token, count in values.items()
    ]
    category_rows = [
        {
            "hospital": hospital,
            "table": table,
            "target": target,
            "raw_token": token,
            "count": count,
        }
        for (hospital, table, target), values in sorted(categorical_values.items())
        for token, count in values.items()
    ]
    all_specs = [*table_specs[("static", "all")], *table_specs[("dynamic", "all")]]
    pending_mappings = sorted(
        {
            plan.target
            for plan in by_id.values()
            if plan.target is not None
            and plan.mapping_review_status == "requires_human_review"
            and not (
                plan.table == "dynamic" and plan.target == "stay_id_global"
            )
        }
    )
    pending_units = sorted(
        {
            spec.target
            for spec in all_specs
            if spec.unit_review_status not in {"not_applicable", "approved", "human_approved_raw_v3"}
        }
    )
    pending_types = sorted(
        {
            spec.target
            for spec in all_specs
            if spec.value_type_review_status not in {"approved", "human_approved_raw_v3", "not_applicable"}
        }
    )
    pending_categories = sorted(
        {
            spec.target
            for spec in all_specs
            if spec.kind == "categorical"
        }
    )
    pending_parsers = sorted(
        {
            plan.target
            for plan in by_id.values()
            if plan.target is not None
            and plan.parser_review_status == "requires_human_review"
            and not (
                plan.table == "dynamic" and plan.target == "stay_id_global"
            )
        }
    )
    failure_counts = {
        "immutable_evidence_and_decision_sources_valid": 0,
        "ingested_inputs_match_manifests": 0,
        "all_raw_occurrences_have_candidate_dispositions": 0,
        "source_bindings_match_lossless_metadata": 0,
        "row_conservation": 0,
        "common_candidate_schemas": 0,
        "candidate_tokens_resolved": unresolved_count,
        "candidate_column_mappings_approved": len(pending_mappings),
        "candidate_value_types_approved": len(pending_types),
        "candidate_parsing_and_missing_policies_approved": len(pending_parsers),
        "candidate_units_and_semantics_approved": len(pending_units),
        "candidate_categorical_domains_approved": len(pending_categories),
        "ordered_harmonized_union_schema_approved": 2,
        "variable_dictionary_approved": len(dictionary_rows),
        "run_scoped_outputs_are_isolated_and_nonpublishable": 0,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed={"failure_count": count},
            expected={"failure_count": 0},
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
    hospital_summaries = {
        manifest["hospital"]: {
            "static_rows": manifest["outputs"]["static"]["row_count"],
            "dynamic_rows": manifest["outputs"]["dynamic"]["row_count"],
            "unresolved_cells": sum(
                row["unresolved_count"]
                for row in target_metrics_rows
                if row["hospital"] == manifest["hospital"]
            ),
        }
        for manifest in hospital_manifests
    }
    review_payload = {
        "artifact": "asic_v3_harmonization_dry_run_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "overall_status": overall_status(checks),
        "candidate_status": policy.output_status,
        "metrics": {
            "hospital_count": len(hospital_manifests),
            "static_row_count": sum(value["static_rows"] for value in hospital_summaries.values()),
            "dynamic_row_count": sum(value["dynamic_rows"] for value in hospital_summaries.values()),
            "static_candidate_column_count": len(table_specs[("static", "all")]),
            "dynamic_candidate_column_count": len(table_specs[("dynamic", "all")]),
            "candidate_variable_dictionary_row_count": len(dictionary_rows),
            "unresolved_candidate_cell_count": unresolved_count,
            "private_unresolved_example_count": len(unresolved_rows),
            "private_categorical_value_count": len(category_rows),
            "pending_mapping_variable_count": len(pending_mappings),
            "pending_type_variable_count": len(pending_types),
            "pending_parser_variable_count": len(pending_parsers),
            "pending_unit_variable_count": len(pending_units),
            "pending_categorical_variable_count": len(pending_categories),
            "dynamic_filename_identifier_row_count": sum(
                int(
                    manifest["outputs"]["dynamic"]["identifier_evidence"][
                        "filename_nonempty_row_count"
                    ]
                )
                for manifest in hospital_manifests
            ),
            "dynamic_in_file_identifier_corroboration_row_count": sum(
                int(
                    manifest["outputs"]["dynamic"]["identifier_evidence"][
                        "in_file_nonempty_row_count"
                    ]
                )
                for manifest in hospital_manifests
            ),
            "dynamic_raw_identifier_corroboration_occurrence_count": sum(
                int(
                    manifest["outputs"]["dynamic"]["identifier_evidence"][
                        "raw_identifier_occurrence_count"
                    ]
                )
                for manifest in hospital_manifests
            ),
            "dynamic_identifier_not_in_static_count": sum(
                int(
                    manifest["outputs"]["dynamic"]["identifier_evidence"][
                        "dynamic_identifier_not_in_static_count"
                    ]
                )
                for manifest in hospital_manifests
            ),
            "technical_blocking_finding_count": len(technical),
            "production_candidate_artifacts_generated": True,
            "publication_ready": False,
        },
        "hospital_summaries": hospital_summaries,
        "candidate_schemas": {
            table: [
                {
                    "name": spec.target,
                    "type": str(spec.arrow_type),
                    "candidate_unit": spec.candidate_unit,
                    "value_type_review_status": spec.value_type_review_status,
                    "unit_review_status": spec.unit_review_status,
                    "source_hospital_count": next(
                        row["source_hospital_count"]
                        for row in dictionary_rows
                        if row["table"] == table
                        and row["variable"] == spec.target
                    ),
                    "definition_status": next(
                        row["definition_status"]
                        for row in dictionary_rows
                        if row["table"] == table
                        and row["variable"] == spec.target
                    ),
                }
                for spec in table_specs[(table, "all")]
            ]
            for table in ("static", "dynamic")
        },
        "pending_decisions": {
            "mappings": pending_mappings,
            "types": pending_types,
            "parsers_and_missing": pending_parsers,
            "units_or_semantics": pending_units,
            "categorical_domains": pending_categories,
        },
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only harmonization dry-run report bundle on the authorized cluster",
        },
        "publication": {
            "candidate_directory": str(final_output),
            "harmonized_release_created": False,
            "cleaned_artifacts_generated": False,
            "derived_artifacts_generated": False,
            "publication_ready": False,
        },
        "limitations": [
            "These run-scoped Parquet files are candidate evidence, not an approved harmonized release.",
            "No row, stay, or variable is filtered; reviewed all-missing retirements and composite-source rules remain explicitly manifested.",
            "Explicit decimal-comma, ratio, percentage, threshold, list, sentinel, categorical, and ICD-10 notations are transformed and counted.",
            "Distribution-inferred unit conversions, physiologic outlier masking, individual scale-error correction, cleaning, and derivation are not applied.",
            "The lossless ingested Parquet remains the authoritative replay boundary for every candidate decision.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    run_manifest = {
        "artifact": "asic_v3_harmonization_candidate_run",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "status": policy.output_status,
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "ingestion_audit_run_id": ingestion_audit_run_id,
            "decision_hashes": decision_hashes,
        },
        "schemas": {table: _schema_digest(schema) for table, schema in schemas.items()},
        "hospitals": hospital_manifests,
        "checks": [asdict(item) for item in checks],
        "publication_ready": False,
        "cleaning_allowed": False,
        "derivation_allowed": False,
    }
    _write_json(staging_output / "candidate_run_manifest.json", run_manifest, 0o600)
    staging_output.replace(final_output)
    final_output.chmod(0o700)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(
        private_dir / "harmonization_dry_run_manifest.json",
        {
            "artifact": "asic_v3_harmonization_dry_run_private",
            "artifact_version": policy.private_artifact_version,
            "dataset_context": config.dataset_context,
            "generated_at_utc": generated,
            "run_id": selected_run,
            "candidate_run_manifest_sha256": sha256_file(
                final_output / "candidate_run_manifest.json"
            ),
            "column_metric_row_count": len(target_metrics_rows),
            "unresolved_example_row_count": len(unresolved_rows),
            "categorical_value_row_count": len(category_rows),
            "candidate_variable_dictionary_row_count": len(dictionary_rows),
            "publication_ready": False,
        },
        0o600,
    )
    _write_private_parquet(private_dir / "column_metrics.parquet", target_metrics_rows)
    _write_private_parquet(private_dir / "unresolved_tokens.parquet", unresolved_rows)
    _write_private_parquet(private_dir / "categorical_values.parquet", category_rows)
    _write_private_parquet(
        private_dir / "candidate_variable_dictionary.parquet",
        dictionary_rows,
    )
    _write_private_parquet(
        private_dir / "occurrence_plan.parquet",
        [
            {
                **asdict(plan),
                "target": plan.target,
                "effective_disposition": (
                    "reviewed_dynamic_identifier_corroboration"
                    if plan.table == "dynamic"
                    and plan.target == "stay_id_global"
                    else plan.proposed_action
                ),
            }
            for plan in by_id.values()
        ],
    )
    for path in private_dir.glob("*.parquet"):
        path.chmod(0o600)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return HarmonizationDryRunResult(
        run_id=selected_run,
        overall_status=review_payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        candidate_directory=final_output,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
