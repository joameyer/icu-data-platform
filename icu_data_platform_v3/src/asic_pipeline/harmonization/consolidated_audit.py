from __future__ import annotations

from array import array
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.dry_run import (
    PROVENANCE_FIELDS,
    HarmonizationDryRunConfig,
    load_harmonization_dry_run_config,
    load_harmonization_dry_run_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, _write_private_parquet, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


APPROVED_REVIEW_STATUSES = frozenset(
    {"approved", "human_approved_raw_v3", "not_applicable"}
)
TECHNICAL_CHECKS = frozenset(
    {
        "candidate_evidence_and_frozen_policies_valid",
        "candidate_and_ingested_hashes_valid",
        "row_and_provenance_conservation",
        "common_ordered_schemas_valid",
        "dictionary_schema_coverage_complete",
        "parser_accounting_complete",
        "identifier_contract_valid",
        "all_numeric_variables_profiled",
        "all_unresolved_unit_policies_audited",
        "all_migrated_quality_rules_accounted",
        "optional_old_version_comparison_non_authoritative",
        "audit_writes_reports_only",
    }
)
CHECK_DETAILS = {
    "candidate_evidence_and_frozen_policies_valid": "The candidate run, dry-run policy, private evidence, and v3-owned cross-hospital quality policy must match their immutable versions and hashes.",
    "candidate_and_ingested_hashes_valid": "Every candidate and ingested Parquet must match its immutable manifest hash and row count.",
    "row_and_provenance_conservation": "Every candidate row and all five lossless provenance columns must match ingestion exactly.",
    "common_ordered_schemas_valid": "All hospital outputs must retain one identical ordered static schema and one identical ordered dynamic schema.",
    "dictionary_schema_coverage_complete": "Every candidate clinical field must occur exactly once in the candidate dictionary and in the correct order.",
    "parser_accounting_complete": "Every hospital-variable parser metric must account for its complete row domain and unresolved tokens must remain explicitly counted.",
    "identifier_contract_valid": "Static global stay identifiers must be unique and every dynamic stay identifier must resolve to its hospital's static stay set.",
    "all_numeric_variables_profiled": "Every numeric or numeric-list candidate variable must receive a per-hospital production profile.",
    "all_unresolved_unit_policies_audited": "Every variable with a pending or unresolved unit must receive cross-hospital, legacy, scale, and comparison evidence plus an explicit recommendation.",
    "all_migrated_quality_rules_accounted": "Every selectively migrated invalid-range, targeted-scale, comparison, bucket, relationship, availability, and semantic rule must be evaluated or explicitly classified unavailable against v3 data.",
    "optional_old_version_comparison_non_authoritative": "Old-version artifacts are optional, read-only context; their absence must not block any v3 audit or release decision.",
    "candidate_tokens_resolved": "Every non-empty source token must have resolved during candidate harmonization.",
    "unit_and_scale_policies_approved": "Named candidate units, site-scale hypotheses, and unresolved units require one consolidated human decision before release.",
    "categorical_domains_approved": "Complete harmonized categorical domains require consolidated human approval.",
    "semantic_and_relationship_findings_approved": "Legacy semantic findings and production relationship evidence require consolidated human approval.",
    "legacy_invalid_ranges_reviewed_for_cleaning": "Invalid-range and isolated scale-entry findings require a separate cleaning-policy decision; they are not applied during harmonization.",
    "cross_version_differences_reviewed": "When optional old-version artifacts are available, their differences from raw-derived v3 require review but never establish v3 truth.",
    "ordered_schema_and_dictionary_approved": "The complete candidate schemas and variable dictionary require an explicit freeze decision.",
    "audit_writes_reports_only": "This audit must not modify candidate data or create released harmonized, cleaned, derived, or published artifacts.",
}


@dataclass(frozen=True)
class ConsolidatedAuditPolicy:
    version: str
    dry_run_policy_path: Path
    dry_run_policy_sha256: str
    candidate_artifact_version: str
    candidate_private_artifact_version: str
    candidate_review_artifact_version: str
    candidate_status: str
    hospital_count: int
    hospital_suffixes: tuple[tuple[str, str], ...]
    comparison_reference_path: Path | None
    comparison_reference_sha256: str | None
    quality_policy_path: Path
    quality_policy_sha256: str
    rows_per_batch: int
    sample_capacity: int
    sample_rows_per_batch: int
    maximum_examples: int
    categorical_domain_cap: int
    minimum_distribution_count: int
    minimum_distribution_hospitals: int
    iqr_fence_factor: float
    general_scale_factors: tuple[float, ...]
    peer_minimum_improvement_factor: float
    peer_maximum_residual_factor: float
    row_scale_factors: tuple[float, ...]
    private_artifact_version: str
    review_artifact_version: str
    private_directory_name: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class ConsolidatedAuditConfig:
    dry_run: HarmonizationDryRunConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.dry_run.dataset_context

    @property
    def data_root(self) -> Path:
        return self.dry_run.data_root

    @property
    def ingested_root(self) -> Path:
        return self.dry_run.ingested_root

    @property
    def reports_root(self) -> Path:
        return self.dry_run.reports_root


@dataclass(frozen=True)
class ConsolidatedAuditResult:
    run_id: str
    candidate_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _positive_number(value: Any, location: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive number")
    return float(value)


def _number_tuple(value: Any, location: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError(f"{location} must be a non-empty list")
    result = tuple(_positive_number(item, location) for item in value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicate values")
    return result


def load_consolidated_audit_config(path: str | Path) -> ConsolidatedAuditConfig:
    source = Path(path).expanduser().resolve()
    dry_run = load_harmonization_dry_run_config(source)
    raw = load_yaml_mapping(source, "Consolidated harmonization audit configuration")
    policy_path = resolve_path(
        required_string(
            raw,
            "consolidated_harmonization_audit_policy",
            "config",
        ),
        source,
    )
    return ConsolidatedAuditConfig(dry_run=dry_run, policy_path=policy_path)


def load_consolidated_audit_policy(path: str | Path) -> ConsolidatedAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Consolidated harmonization audit policy")
    if raw.get("consolidated_harmonization_audit_policy_version") != "0.2":
        raise ConfigurationError("Consolidated audit policy version must be 0.2")
    if raw.get("status") != "approved_for_read_only_consolidated_candidate_audit":
        raise ConfigurationError("Consolidated audit policy is not approved")
    candidate = _mapping(raw.get("required_candidate"), "policy.required_candidate")
    quality_policy = _mapping(raw.get("v3_quality_policy"), "policy.v3_quality_policy")
    old_comparison = _mapping(
        raw.get("optional_old_version_comparison"),
        "policy.optional_old_version_comparison",
    )
    scan = _mapping(raw.get("scan"), "policy.scan")
    coverage = _mapping(raw.get("coverage"), "policy.coverage")
    expected_coverage = {
        "verify_candidate_and_ingested_hashes": True,
        "verify_row_and_provenance_conservation": True,
        "verify_common_ordered_schemas": True,
        "verify_dictionary_schema_coverage": True,
        "verify_parser_accounting": True,
        "verify_static_identifier_uniqueness": True,
        "verify_dynamic_identifiers_exist_in_static": True,
        "profile_every_numeric_variable": True,
        "inventory_every_categorical_domain": True,
        "profile_every_list_variable": True,
        "audit_every_unresolved_unit_policy": True,
        "audit_legacy_invalid_ranges": True,
        "discover_row_level_scale_entry_candidates": True,
        "audit_all_migrated_targeted_scale_hypotheses": True,
        "audit_all_migrated_targeted_comparisons": True,
        "audit_all_migrated_targeted_buckets": True,
        "audit_all_migrated_relationships": True,
        "audit_known_legacy_semantic_findings": True,
        "audit_migrated_candidate_context_rules": True,
        "audit_static_missing_sentinels": True,
        "audit_predicted_body_weight_formula": True,
        "inventory_global_all_missing_columns": True,
        "generate_complete_decision_register": True,
        "audit_optional_old_version_differences_when_available": True,
        "require_old_version_artifacts": False,
        "apply_unit_or_cleaning_changes": False,
        "write_harmonized_data": False,
        "write_cleaned_data": False,
        "write_derived_data": False,
        "publish_data": False,
    }
    if coverage != expected_coverage:
        raise ConfigurationError("Consolidated audit coverage or write boundary changed")
    reporting = _mapping(raw.get("reporting"), "policy.reporting")
    if {
        key: reporting.get(key)
        for key in (
            "private_directory_mode",
            "private_file_mode",
            "review_directory_mode",
            "review_file_mode",
        )
    } != {
        "private_directory_mode": "0700",
        "private_file_mode": "0600",
        "review_directory_mode": "0750",
        "review_file_mode": "0640",
    }:
        raise ConfigurationError("Consolidated audit report permissions changed")
    dry_policy_path = resolve_path(
        required_string(candidate, "dry_run_policy", "required_candidate"), source
    )
    quality_path = resolve_path(
        required_string(quality_policy, "path", "v3_quality_policy"), source
    )
    if quality_policy.get("role") != "native_v3_reviewed_quality_rule_registry":
        raise ConfigurationError("V3 quality-policy role changed unexpectedly")
    comparison_value = old_comparison.get("reference")
    if comparison_value is None:
        comparison_path = None
        comparison_hash = None
    elif isinstance(comparison_value, str) and comparison_value:
        comparison_path = resolve_path(comparison_value, source)
        comparison_hash = required_string(
            old_comparison,
            "reference_sha256",
            "optional_old_version_comparison",
        )
    else:
        raise ConfigurationError("Optional old-version comparison reference is invalid")
    if {
        "availability": old_comparison.get("availability"),
        "role": old_comparison.get("role"),
    } != {
        "availability": "optional_nonblocking",
        "role": "untrusted_context_only_never_v3_input",
    }:
        raise ConfigurationError("Optional old-version comparison policy changed")
    suffixes_raw = _mapping(
        candidate.get("hospital_suffixes"), "required_candidate.hospital_suffixes"
    )
    expected_suffixes = {
        "asic_UK00": "0",
        "asic_UK01": "1",
        "asic_UK02": "2",
        "asic_UK03": "3",
        "asic_UK04": "4",
        "asic_UK06": "6",
        "asic_UK07": "7",
        "asic_UK08": "8",
    }
    if suffixes_raw != expected_suffixes:
        raise ConfigurationError("Approved hospital identifier suffixes changed")
    return ConsolidatedAuditPolicy(
        version="0.2",
        dry_run_policy_path=dry_policy_path,
        dry_run_policy_sha256=required_string(
            candidate, "dry_run_policy_sha256", "required_candidate"
        ),
        candidate_artifact_version=required_string(
            candidate, "candidate_artifact_version", "required_candidate"
        ),
        candidate_private_artifact_version=required_string(
            candidate, "candidate_private_artifact_version", "required_candidate"
        ),
        candidate_review_artifact_version=required_string(
            candidate, "candidate_review_artifact_version", "required_candidate"
        ),
        candidate_status=required_string(
            candidate, "candidate_status", "required_candidate"
        ),
        hospital_count=_positive_int(candidate.get("hospital_count"), "required_candidate.hospital_count"),
        hospital_suffixes=tuple(expected_suffixes.items()),
        comparison_reference_path=comparison_path,
        comparison_reference_sha256=comparison_hash,
        quality_policy_path=quality_path,
        quality_policy_sha256=required_string(
            quality_policy, "sha256", "v3_quality_policy"
        ),
        rows_per_batch=_positive_int(scan.get("rows_per_batch"), "scan.rows_per_batch"),
        sample_capacity=_positive_int(
            scan.get("numeric_sample_capacity_per_hospital_variable"),
            "scan.numeric_sample_capacity_per_hospital_variable",
        ),
        sample_rows_per_batch=_positive_int(
            scan.get("sample_rows_per_batch"), "scan.sample_rows_per_batch"
        ),
        maximum_examples=_positive_int(
            scan.get("maximum_private_examples_per_finding"),
            "scan.maximum_private_examples_per_finding",
        ),
        categorical_domain_cap=_positive_int(
            scan.get("categorical_domain_cap"), "scan.categorical_domain_cap"
        ),
        minimum_distribution_count=_positive_int(
            scan.get("minimum_nonmissing_for_distribution_comparison"),
            "scan.minimum_nonmissing_for_distribution_comparison",
        ),
        minimum_distribution_hospitals=_positive_int(
            scan.get("minimum_hospitals_for_distribution_comparison"),
            "scan.minimum_hospitals_for_distribution_comparison",
        ),
        iqr_fence_factor=_positive_number(
            scan.get("hospital_metric_iqr_fence_factor"),
            "scan.hospital_metric_iqr_fence_factor",
        ),
        general_scale_factors=_number_tuple(
            scan.get("general_scale_factors"), "scan.general_scale_factors"
        ),
        peer_minimum_improvement_factor=_positive_number(
            scan.get("peer_alignment_minimum_improvement_factor"),
            "scan.peer_alignment_minimum_improvement_factor",
        ),
        peer_maximum_residual_factor=_positive_number(
            scan.get("peer_alignment_maximum_residual_factor"),
            "scan.peer_alignment_maximum_residual_factor",
        ),
        row_scale_factors=_number_tuple(
            scan.get("row_scale_entry_factors"), "scan.row_scale_entry_factors"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
        ),
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
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


def _finite_number(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _scalar_number(value: pa.Scalar | None) -> float | None:
    return _finite_number(value.as_py()) if value is not None and value.is_valid else None


def _bool_count(value: pa.Array | pa.ChunkedArray) -> int:
    result = pc.sum(pc.cast(pc.fill_null(value, False), pa.int64())).as_py()
    return int(result or 0)


def _quantile(values: Iterable[float], probability: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return float(ordered[0])
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    fraction = position - lower
    return float(ordered[lower] * (1 - fraction) + ordered[upper] * fraction)


def _sample_positions(length: int, maximum: int, row_offset: int) -> pa.Int64Array:
    if length <= maximum:
        return pa.array(range(length), type=pa.int64())
    step = length / maximum
    offset = ((row_offset * 11400714819323198485) & ((1 << 64) - 1)) % 10_000
    fractional = offset / 10_000
    positions = [min(int((index + fractional) * step), length - 1) for index in range(maximum)]
    return pa.array(positions, type=pa.int64())


@dataclass
class NumericAccumulator:
    sample_capacity: int
    sample_seed: int
    row_count: int = 0
    null_count: int = 0
    nan_count: int = 0
    positive_infinity_count: int = 0
    negative_infinity_count: int = 0
    finite_count: int = 0
    zero_count: int = 0
    negative_count: int = 0
    minus_one_count: int = 0
    value_sum: float = 0.0
    squared_sum: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    sample_seen: int = 0
    sample: array = field(default_factory=lambda: array("d"))

    def _sample_add(self, value: float) -> None:
        self.sample_seen += 1
        if len(self.sample) < self.sample_capacity:
            self.sample.append(value)
            return
        mixed = (self.sample_seen * 6364136223846793005 + self.sample_seed) & ((1 << 64) - 1)
        position = mixed % self.sample_seen
        if position < self.sample_capacity:
            self.sample[position] = value

    def add(self, values: pa.Array, sample_positions: pa.Int64Array) -> None:
        if not (pa.types.is_integer(values.type) or pa.types.is_floating(values.type)):
            raise HarmonizationError("Numeric audit received a non-numeric Arrow array")
        self.row_count += len(values)
        self.null_count += values.null_count
        if pa.types.is_floating(values.type):
            self.nan_count += _bool_count(pc.is_nan(values))
            self.positive_infinity_count += _bool_count(
                pc.equal(values, pa.scalar(math.inf, type=values.type))
            )
            self.negative_infinity_count += _bool_count(
                pc.equal(values, pa.scalar(-math.inf, type=values.type))
            )
            finite_mask = pc.fill_null(pc.is_finite(values), False)
        else:
            finite_mask = pc.is_valid(values)
        finite = pc.filter(pc.cast(values, pa.float64()), finite_mask)
        count = len(finite)
        self.finite_count += count
        if count:
            self.zero_count += _bool_count(pc.equal(finite, 0.0))
            self.negative_count += _bool_count(pc.less(finite, 0.0))
            self.minus_one_count += _bool_count(pc.equal(finite, -1.0))
            batch_min_max = pc.min_max(finite).as_py()
            batch_min = _finite_number(batch_min_max["min"])
            batch_max = _finite_number(batch_min_max["max"])
            if batch_min is not None:
                self.minimum = batch_min if self.minimum is None else min(self.minimum, batch_min)
            if batch_max is not None:
                self.maximum = batch_max if self.maximum is None else max(self.maximum, batch_max)
            value_sum = _scalar_number(pc.sum(finite))
            square_sum = _scalar_number(pc.sum(pc.multiply(finite, finite)))
            if value_sum is not None:
                self.value_sum += value_sum
            if square_sum is not None:
                self.squared_sum += square_sum
        sampled = pc.take(values, sample_positions)
        if pa.types.is_floating(sampled.type):
            sampled = pc.filter(sampled, pc.fill_null(pc.is_finite(sampled), False))
        else:
            sampled = pc.drop_null(sampled)
        for value in sampled.to_pylist():
            if value is not None and math.isfinite(float(value)):
                self._sample_add(float(value))

    def profile(self, table: str, hospital: str, variable: str, source: str) -> dict[str, Any]:
        quantiles = {
            "q01": _quantile(self.sample, 0.01),
            "q05": _quantile(self.sample, 0.05),
            "q25": _quantile(self.sample, 0.25),
            "median": _quantile(self.sample, 0.50),
            "q75": _quantile(self.sample, 0.75),
            "q95": _quantile(self.sample, 0.95),
            "q99": _quantile(self.sample, 0.99),
        }
        mean = self.value_sum / self.finite_count if self.finite_count else None
        variance = None
        if mean is not None:
            variance = max(self.squared_sum / self.finite_count - mean * mean, 0.0)
        return {
            "source": source,
            "table": table,
            "hospital": hospital,
            "variable": variable,
            "row_count": self.row_count,
            "finite_count": self.finite_count,
            "finite_rate": self.finite_count / self.row_count if self.row_count else 0.0,
            "null_count": self.null_count,
            "nan_count": self.nan_count,
            "positive_infinity_count": self.positive_infinity_count,
            "negative_infinity_count": self.negative_infinity_count,
            "zero_count": self.zero_count,
            "negative_count": self.negative_count,
            "minus_one_count": self.minus_one_count,
            "min": self.minimum,
            **quantiles,
            "max": self.maximum,
            "mean": mean,
            "std": math.sqrt(variance) if variance is not None else None,
            "iqr": (
                quantiles["q75"] - quantiles["q25"]
                if quantiles["q25"] is not None and quantiles["q75"] is not None
                else None
            ),
            "quantile_sample_count": len(self.sample),
            "quantiles_are_exact": self.finite_count <= len(self.sample),
        }


@dataclass
class DifferenceAccumulator:
    sample_capacity: int
    sample_seed: int
    tolerance: float
    both_present_count: int = 0
    left_only_count: int = 0
    right_only_count: int = 0
    neither_present_count: int = 0
    within_tolerance_count: int = 0
    outside_tolerance_count: int = 0
    absolute_difference_sum: float = 0.0
    maximum_absolute_difference: float | None = None
    sample_seen: int = 0
    sample: array = field(default_factory=lambda: array("d"))

    def _sample_add(self, value: float) -> None:
        self.sample_seen += 1
        if len(self.sample) < self.sample_capacity:
            self.sample.append(value)
            return
        mixed = (self.sample_seen * 2862933555777941757 + self.sample_seed) & ((1 << 64) - 1)
        position = mixed % self.sample_seen
        if position < self.sample_capacity:
            self.sample[position] = value

    def add(self, left: pa.Array, right: pa.Array, sample_positions: pa.Int64Array) -> None:
        left = pc.cast(left, pa.float64())
        right = pc.cast(right, pa.float64())
        left_valid = pc.fill_null(pc.is_finite(left), False)
        right_valid = pc.fill_null(pc.is_finite(right), False)
        both = pc.and_(left_valid, right_valid)
        self.both_present_count += _bool_count(both)
        self.left_only_count += _bool_count(pc.and_(left_valid, pc.invert(right_valid)))
        self.right_only_count += _bool_count(pc.and_(pc.invert(left_valid), right_valid))
        self.neither_present_count += _bool_count(pc.and_(pc.invert(left_valid), pc.invert(right_valid)))
        differences = pc.filter(pc.subtract(left, right), both)
        if len(differences):
            absolute = pc.abs(differences)
            self.within_tolerance_count += _bool_count(pc.less_equal(absolute, self.tolerance))
            self.outside_tolerance_count += _bool_count(pc.greater(absolute, self.tolerance))
            difference_sum = _scalar_number(pc.sum(absolute))
            maximum = _scalar_number(pc.max(absolute))
            if difference_sum is not None:
                self.absolute_difference_sum += difference_sum
            if maximum is not None:
                self.maximum_absolute_difference = maximum if self.maximum_absolute_difference is None else max(self.maximum_absolute_difference, maximum)
        sampled_left = pc.take(left, sample_positions)
        sampled_right = pc.take(right, sample_positions)
        sampled_valid = pc.and_(
            pc.fill_null(pc.is_finite(sampled_left), False),
            pc.fill_null(pc.is_finite(sampled_right), False),
        )
        for value in pc.filter(pc.subtract(sampled_left, sampled_right), sampled_valid).to_pylist():
            self._sample_add(float(value))

    def result(self, audit_id: str, hospital: str, table: str, formula: str) -> dict[str, Any]:
        return {
            "audit_id": audit_id,
            "hospital": hospital,
            "table": table,
            "formula": formula,
            "both_present_count": self.both_present_count,
            "left_only_count": self.left_only_count,
            "right_only_count": self.right_only_count,
            "neither_present_count": self.neither_present_count,
            "within_tolerance_count": self.within_tolerance_count,
            "outside_tolerance_count": self.outside_tolerance_count,
            "mean_absolute_difference": self.absolute_difference_sum / self.both_present_count if self.both_present_count else None,
            "maximum_absolute_difference": self.maximum_absolute_difference,
            "difference_q01": _quantile(self.sample, 0.01),
            "difference_q25": _quantile(self.sample, 0.25),
            "difference_median": _quantile(self.sample, 0.50),
            "difference_q75": _quantile(self.sample, 0.75),
            "difference_q99": _quantile(self.sample, 0.99),
            "difference_sample_count": len(self.sample),
        }


def _stable_seed(*values: str) -> int:
    digest = hashlib.sha256("\x1f".join(values).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _schema_digest(schema: pa.Schema) -> str:
    # Parquet read-back uses ``element`` as a list-child field name whereas
    # Arrow's in-memory writer schema uses ``item``.  Canonicalize that harmless
    # representation difference before comparing with the immutable digest
    # recorded by the candidate writer.  Top-level names, nullability, types,
    # and metadata remain part of the digest.
    normalized_fields: list[pa.Field] = []
    for field_value in schema:
        data_type = field_value.type
        if pa.types.is_fixed_size_list(data_type):
            data_type = pa.list_(data_type.value_type, data_type.list_size)
        elif pa.types.is_list(data_type):
            data_type = pa.list_(data_type.value_type)
        elif pa.types.is_large_list(data_type):
            data_type = pa.large_list(data_type.value_type)
        normalized_fields.append(
            pa.field(
                field_value.name,
                data_type,
                nullable=field_value.nullable,
                metadata=field_value.metadata,
            )
        )
    normalized = pa.schema(normalized_fields, metadata=schema.metadata)
    return hashlib.sha256(normalized.serialize().to_pybytes()).hexdigest()


def _canonical_type_string(data_type: pa.DataType) -> str:
    if pa.types.is_fixed_size_list(data_type):
        return str(pa.list_(data_type.value_type, data_type.list_size))
    if pa.types.is_list(data_type):
        return str(pa.list_(data_type.value_type))
    if pa.types.is_large_list(data_type):
        return str(pa.large_list(data_type.value_type))
    return str(data_type)


def _clinical_fields(schema: pa.Schema) -> tuple[pa.Field, ...]:
    return tuple(field for field in schema if field.name not in PROVENANCE_FIELDS)


def _numeric_fields(schema: pa.Schema) -> tuple[pa.Field, ...]:
    return tuple(
        field
        for field in _clinical_fields(schema)
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type)
    )


def _categorical_fields(schema: pa.Schema) -> tuple[pa.Field, ...]:
    return tuple(
        field
        for field in _clinical_fields(schema)
        if (
            (field.metadata or {}).get(b"candidate_kind") == b"categorical"
            or pa.types.is_boolean(field.type)
        )
        and not pa.types.is_list(field.type)
        and not pa.types.is_fixed_size_list(field.type)
    )


def _read_private_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise HarmonizationError("Required private candidate evidence is absent")
    table = pq.read_table(path)
    if table.column_names == ["empty"]:
        return []
    return table.to_pylist()


def _comparison_paths(path: Path) -> dict[str, Path]:
    raw = load_yaml_mapping(path, "Optional old-version comparison reference")
    if raw.get("status") != "untrusted_read_only_comparison":
        raise HarmonizationError("Old-version comparison is not marked untrusted")
    paths = _mapping(raw.get("paths"), "comparison.paths")
    required = (
        "pooled_static",
        "pooled_dynamic",
        "translated_static",
        "translated_dynamic",
    )
    return {
        name: resolve_path(required_string(paths, name, "comparison.paths"), path)
        for name in required
    }


def _quality_rules(policy: dict[str, Any]) -> dict[str, Any]:
    if (
        policy.get("cross_hospital_quality_policy_version") != "0.1"
        or policy.get("status")
        != "approved_for_v3_read_only_cross_hospital_audit"
    ):
        raise HarmonizationError("V3 cross-hospital quality policy has an unexpected contract")
    provenance = _mapping(policy.get("provenance"), "quality_policy.provenance")
    if provenance.get("runtime_dependency_on_older_projects") is not False:
        raise HarmonizationError("V3 quality policy retains an older-project dependency")
    list_sections = (
        "legacy_invalid_value_rules",
        "known_legacy_semantic_findings",
        "targeted_scale_audits",
        "row_level_scale_entry_audits",
        "targeted_comparison_audits",
        "targeted_bucket_audits",
        "relationship_audits",
        "availability_audits",
    )
    result: dict[str, Any] = {}
    for section in list_sections:
        value = policy.get(section)
        if not isinstance(value, list):
            raise HarmonizationError(f"V3 quality section {section} is invalid")
        result[section] = [dict(item) for item in value if isinstance(item, dict)]
        if len(result[section]) != len(value):
            raise HarmonizationError(f"V3 quality section {section} is malformed")
    expected = _mapping(policy.get("targeted_review_coverage"), "targeted_review_coverage")
    observed = {
        "scale_hypothesis_count": len(result["targeted_scale_audits"]),
        "row_level_scale_entry_audit_count": len(result["row_level_scale_entry_audits"]),
        "comparison_audit_count": len(result["targeted_comparison_audits"]),
        "bucket_audit_count": len(result["targeted_bucket_audits"]),
        "cross_table_formula_audit_count": 1,
    }
    if expected != observed:
        raise HarmonizationError("V3 targeted-review coverage changed")
    for section in ("targeted_scale_audits", "targeted_bucket_audits"):
        for rule in result[section]:
            if rule.get("table") not in {"static", "dynamic"}:
                raise HarmonizationError(
                    f"V3 quality rule {rule.get('id')} lacks an explicit table scope"
                )
    return result


def _invalid_rules_by_column(rules: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_column: dict[str, dict[str, Any]] = {}
    for rule in rules:
        columns = rule.get("columns", [])
        if not isinstance(columns, list):
            raise HarmonizationError("Legacy invalid-value columns are malformed")
        for column in columns:
            if not isinstance(column, str) or not column:
                raise HarmonizationError("Legacy invalid-value column is invalid")
            definition = {
                "legacy_name": rule.get("legacy_name"),
                "hard_min": _finite_number(rule.get("hard_min")),
                "hard_max": _finite_number(rule.get("hard_max")),
                "invalid_zero": rule.get("invalid_zero") is True,
                "description": rule.get("description"),
            }
            previous = by_column.get(column)
            if previous is not None and previous != definition:
                raise HarmonizationError("Conflicting legacy invalid-value rules")
            by_column[column] = definition
    return by_column


def _unavailable_invalid_rule_rows(
    rules: list[dict[str, Any]],
    schemas: dict[str, pa.Schema],
    observed_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Account for migrated rules whose variables do not exist in v3 schemas."""
    by_column = _invalid_rules_by_column(rules)
    observed = {str(row.get("variable")) for row in observed_rows}
    schema_names = {
        field.name
        for schema in schemas.values()
        for field in _clinical_fields(schema)
    }
    rows: list[dict[str, Any]] = []
    for variable in sorted(set(by_column) - observed):
        # A present-but-unprofiled field indicates an incompatible physical
        # type and must remain a technical accounting failure. Only complete
        # absence from both approved union schemas is explicitly unavailable.
        if variable in schema_names:
            continue
        rule = by_column[variable]
        rows.append(
            {
                "table": "dynamic",
                "hospital": None,
                "variable": variable,
                "legacy_rule_name": rule["legacy_name"],
                "hard_min": rule["hard_min"],
                "hard_max": rule["hard_max"],
                "invalid_zero": rule["invalid_zero"],
                "finite_count": 0,
                "invalid_count": 0,
                "invalid_fraction": None,
                "recoverable_by_factor": {},
                "uniquely_recoverable_count": 0,
                "multiply_recoverable_count": 0,
                "status": "candidate_variable_unavailable_in_v3_union_schema",
                "action": "rule_retained_for_future_schema_or_derived_layer_review_no_values_changed",
            }
        )
    return rows


def _valid_numeric_mask(values: pa.Array) -> pa.Array:
    if pa.types.is_floating(values.type):
        return pc.fill_null(pc.is_finite(values), False)
    return pc.is_valid(values)


def _inside_range(
    values: pa.Array,
    minimum: float | None,
    maximum: float | None,
    invalid_zero: bool = False,
) -> pa.Array:
    valid = _valid_numeric_mask(values)
    if minimum is not None:
        valid = pc.and_(valid, pc.fill_null(pc.greater_equal(values, minimum), False))
    if maximum is not None:
        valid = pc.and_(valid, pc.fill_null(pc.less_equal(values, maximum), False))
    if invalid_zero:
        valid = pc.and_(valid, pc.fill_null(pc.not_equal(values, 0), False))
    return valid


def _transformed(values: pa.Array, rule: dict[str, Any]) -> pa.Array:
    values = pc.cast(values, pa.float64())
    factor = float(rule.get("factor"))
    operation = rule.get("operation")
    if operation == "multiply":
        return pc.multiply(values, factor)
    if operation == "divide":
        return pc.divide(values, factor)
    if operation == "divide_when_above":
        threshold = float(rule.get("threshold"))
        return pc.if_else(
            pc.fill_null(pc.greater(values, threshold), False),
            pc.divide(values, factor),
            values,
        )
    raise HarmonizationError("A frozen targeted-scale operation is unsupported")


@dataclass
class InvalidScaleState:
    rule: dict[str, Any]
    factors: tuple[float, ...]
    finite_count: int = 0
    invalid_count: int = 0
    recoverable_by_factor: Counter[float] = field(default_factory=Counter)
    uniquely_recoverable_count: int = 0
    multiply_recoverable_count: int = 0

    def add(self, values: pa.Array) -> None:
        minimum = self.rule["hard_min"]
        maximum = self.rule["hard_max"]
        invalid_zero = self.rule["invalid_zero"]
        finite = _valid_numeric_mask(values)
        inside = _inside_range(values, minimum, maximum, invalid_zero)
        invalid = pc.and_(finite, pc.invert(inside))
        self.finite_count += _bool_count(finite)
        self.invalid_count += _bool_count(invalid)
        candidate_masks: list[pa.Array] = []
        for factor in self.factors:
            candidate = pc.multiply(pc.cast(values, pa.float64()), factor)
            recovered = pc.and_(
                invalid,
                _inside_range(candidate, minimum, maximum, invalid_zero),
            )
            count = _bool_count(recovered)
            self.recoverable_by_factor[factor] += count
            candidate_masks.append(pc.cast(recovered, pa.int8()))
        if candidate_masks:
            recovery_count = candidate_masks[0]
            for candidate_mask in candidate_masks[1:]:
                recovery_count = pc.add(recovery_count, candidate_mask)
            self.uniquely_recoverable_count += _bool_count(pc.equal(recovery_count, 1))
            self.multiply_recoverable_count += _bool_count(pc.greater(recovery_count, 1))

    def result(self, table: str, hospital: str, variable: str) -> dict[str, Any]:
        return {
            "table": table,
            "hospital": hospital,
            "variable": variable,
            "legacy_rule_name": self.rule["legacy_name"],
            "hard_min": self.rule["hard_min"],
            "hard_max": self.rule["hard_max"],
            "invalid_zero": self.rule["invalid_zero"],
            "finite_count": self.finite_count,
            "invalid_count": self.invalid_count,
            "invalid_fraction": self.invalid_count / self.finite_count if self.finite_count else None,
            "recoverable_by_factor": dict(sorted(self.recoverable_by_factor.items())),
            "uniquely_recoverable_count": self.uniquely_recoverable_count,
            "multiply_recoverable_count": self.multiply_recoverable_count,
            "action": "review_for_cleaning_only_no_values_changed",
        }


@dataclass
class TargetedScaleState:
    rule: dict[str, Any]
    accumulator: NumericAccumulator
    values_considered_count: int = 0
    before_outside_expected_count: int = 0
    after_outside_expected_count: int = 0

    def add(self, values: pa.Array, positions: pa.Int64Array) -> None:
        candidate = _transformed(values, self.rule)
        finite = _valid_numeric_mask(values)
        self.values_considered_count += _bool_count(finite)
        minimum = _finite_number(self.rule.get("expected_min"))
        maximum = _finite_number(self.rule.get("expected_max"))
        if minimum is not None or maximum is not None:
            self.before_outside_expected_count += _bool_count(
                pc.and_(finite, pc.invert(_inside_range(values, minimum, maximum)))
            )
            self.after_outside_expected_count += _bool_count(
                pc.and_(finite, pc.invert(_inside_range(candidate, minimum, maximum)))
            )
        self.accumulator.add(candidate, positions)


@dataclass
class BucketState:
    rule: dict[str, Any]
    counts: Counter[str] = field(default_factory=Counter)

    def add(self, values: pa.Array) -> None:
        numeric = pc.cast(values, pa.float64())
        finite = _valid_numeric_mask(numeric)
        self.counts["missing_or_nonfinite"] += len(values) - _bool_count(finite)
        cuts = [float(item) for item in self.rule.get("cut_points", [])]
        if not cuts:
            raise HarmonizationError("Targeted bucket audit has no cut points")
        previous: float | None = None
        for cut in cuts:
            if previous is None:
                mask = pc.and_(finite, pc.less(numeric, cut))
                label = f"lt_{cut:g}"
            else:
                mask = pc.and_(
                    finite,
                    pc.and_(pc.greater_equal(numeric, previous), pc.less(numeric, cut)),
                )
                label = f"ge_{previous:g}_lt_{cut:g}"
            self.counts[label] += _bool_count(mask)
            previous = cut
        assert previous is not None
        self.counts[f"ge_{previous:g}"] += _bool_count(
            pc.and_(finite, pc.greater_equal(numeric, previous))
        )


@dataclass
class TableScanResult:
    numeric_profiles: list[dict[str, Any]]
    categorical_rows: list[dict[str, Any]]
    list_profiles: list[dict[str, Any]]
    invalid_scale_rows: list[dict[str, Any]]
    targeted_scale_rows: list[dict[str, Any]]
    bucket_rows: list[dict[str, Any]]
    relationship_rows: list[dict[str, Any]]
    identifier_summary: dict[str, Any]


@dataclass
class ListState:
    field: pa.Field
    sample_capacity: int
    seed: int
    row_count: int = 0
    null_list_count: int = 0
    empty_list_count: int = 0
    total_element_count: int = 0
    minimum_length: int | None = None
    maximum_length: int | None = None
    rows_with_duplicate_elements: int = 0
    fixed_position_counts: Counter[str] = field(default_factory=Counter)
    element_accumulator: NumericAccumulator | None = None

    def __post_init__(self) -> None:
        value_type = self.field.type.value_type
        if pa.types.is_integer(value_type) or pa.types.is_floating(value_type):
            self.element_accumulator = NumericAccumulator(self.sample_capacity, self.seed)

    def add(self, values: pa.Array, sample_rows: int, row_offset: int) -> None:
        self.row_count += len(values)
        self.null_list_count += values.null_count
        lengths = pc.list_value_length(values)
        valid_lengths = pc.drop_null(lengths)
        if len(valid_lengths):
            self.empty_list_count += _bool_count(pc.equal(valid_lengths, 0))
            self.total_element_count += int(pc.sum(valid_lengths).as_py() or 0)
            min_max = pc.min_max(valid_lengths).as_py()
            minimum = int(min_max["min"])
            maximum = int(min_max["max"])
            self.minimum_length = minimum if self.minimum_length is None else min(self.minimum_length, minimum)
            self.maximum_length = maximum if self.maximum_length is None else max(self.maximum_length, maximum)
        if pa.types.is_fixed_size_list(values.type) and values.type.list_size == 2:
            left = pc.list_element(values, 0)
            right = pc.list_element(values, 1)
            left_true = pc.fill_null(left, False)
            right_true = pc.fill_null(right, False)
            left_valid = pc.is_valid(left)
            right_valid = pc.is_valid(right)
            patterns = {
                "null_null": pc.and_(pc.invert(left_valid), pc.invert(right_valid)),
                "null_false": pc.and_(pc.invert(left_valid), pc.and_(right_valid, pc.invert(right_true))),
                "null_true": pc.and_(pc.invert(left_valid), pc.and_(right_valid, right_true)),
                "false_null": pc.and_(pc.and_(left_valid, pc.invert(left_true)), pc.invert(right_valid)),
                "false_false": pc.and_(pc.and_(left_valid, pc.invert(left_true)), pc.and_(right_valid, pc.invert(right_true))),
                "false_true": pc.and_(pc.and_(left_valid, pc.invert(left_true)), pc.and_(right_valid, right_true)),
                "true_null": pc.and_(pc.and_(left_valid, left_true), pc.invert(right_valid)),
                "true_false": pc.and_(pc.and_(left_valid, left_true), pc.and_(right_valid, pc.invert(right_true))),
                "true_true": pc.and_(pc.and_(left_valid, left_true), pc.and_(right_valid, right_true)),
            }
            for label, mask in patterns.items():
                self.fixed_position_counts[label] += _bool_count(mask)
            return
        # Static list fields are small enough for exact duplicate accounting.
        for value in values.to_pylist():
            if isinstance(value, list) and len(value) != len(set(value)):
                self.rows_with_duplicate_elements += 1
        if self.element_accumulator is not None:
            flattened = pc.list_flatten(values)
            positions = _sample_positions(len(flattened), sample_rows, row_offset)
            self.element_accumulator.add(flattened, positions)

    def result(self, table: str, hospital: str, variable: str) -> dict[str, Any]:
        row = {
            "table": table,
            "hospital": hospital,
            "variable": variable,
            "arrow_type": str(self.field.type),
            "row_count": self.row_count,
            "null_list_count": self.null_list_count,
            "empty_list_count": self.empty_list_count,
            "total_element_count": self.total_element_count,
            "minimum_list_length": self.minimum_length,
            "maximum_list_length": self.maximum_length,
            "rows_with_duplicate_elements": self.rows_with_duplicate_elements,
            "fixed_position_counts": dict(sorted(self.fixed_position_counts.items())),
        }
        if self.element_accumulator is not None:
            element = self.element_accumulator.profile(table, hospital, variable, "candidate_list_elements")
            row.update({f"element_{key}": value for key, value in element.items() if key not in {"source", "table", "hospital", "variable"}})
        return row


def _relationship_arrays(batch: pa.RecordBatch, rule: dict[str, Any]) -> tuple[pa.Array, pa.Array] | None:
    fields = rule.get("fields")
    if not isinstance(fields, list) or any(not isinstance(item, str) for item in fields):
        raise HarmonizationError("Frozen relationship field list is invalid")
    if set(fields) - set(batch.schema.names):
        return None
    arrays = [batch.column(batch.schema.get_field_index(name)) for name in fields]
    formula = rule.get("formula")
    if formula == "delta_p_computed_equals_insp_pressure_minus_peep":
        return arrays[0], pc.subtract(arrays[1], arrays[2])
    if formula == "isofa_total_equals_sum_of_six_components_when_all_present":
        total = pc.cast(arrays[1], pa.float64())
        for value in arrays[2:]:
            total = pc.add(total, pc.cast(value, pa.float64()))
        return arrays[0], total
    if len(arrays) >= 2:
        return arrays[0], arrays[1]
    return None


def _scan_candidate_table(
    candidate_path: Path,
    ingested_path: Path,
    hospital: str,
    table: str,
    suffix: str,
    policy: ConsolidatedAuditPolicy,
    quality: dict[str, Any],
    static_stays: set[str] | None,
    progress: Callable[[str], None] | None,
) -> tuple[TableScanResult, set[str]]:
    parquet = pq.ParquetFile(candidate_path)
    schema = parquet.schema_arrow
    numeric = _numeric_fields(schema)
    categorical = _categorical_fields(schema)
    list_fields = tuple(
        field
        for field in _clinical_fields(schema)
        if pa.types.is_list(field.type)
        or pa.types.is_large_list(field.type)
        or pa.types.is_fixed_size_list(field.type)
    )
    invalid_rules = _invalid_rules_by_column(quality["legacy_invalid_value_rules"])
    numeric_states = {
        field.name: NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("candidate", table, hospital, field.name),
        )
        for field in numeric
    }
    invalid_states = {
        field.name: InvalidScaleState(invalid_rules[field.name], policy.row_scale_factors)
        for field in numeric
        if field.name in invalid_rules
        and (
            invalid_rules[field.name]["hard_min"] is not None
            or invalid_rules[field.name]["hard_max"] is not None
            or invalid_rules[field.name]["invalid_zero"]
        )
    }
    targeted_states: dict[str, TargetedScaleState] = {}
    targeted_unavailable: list[dict[str, Any]] = []
    for rule in quality["targeted_scale_audits"]:
        if rule.get("table") != table:
            continue
        if rule.get("hospital") != hospital:
            continue
        variable = rule.get("column")
        if not isinstance(variable, str) or variable not in schema.names:
            targeted_unavailable.append({"audit_id": rule.get("id"), "hospital": hospital, "variable": variable, "status": "candidate_column_unavailable"})
            continue
        targeted_states[str(rule["id"])] = TargetedScaleState(
            rule,
            NumericAccumulator(
                policy.sample_capacity,
                _stable_seed("targeted", hospital, str(variable), str(rule["id"])),
            ),
        )
    bucket_states: dict[str, BucketState] = {}
    bucket_unavailable: list[dict[str, Any]] = []
    for rule in quality["targeted_bucket_audits"]:
        if rule.get("table") != table:
            continue
        if rule.get("hospital") != hospital:
            continue
        variable = rule.get("column")
        if not isinstance(variable, str) or variable not in schema.names:
            bucket_unavailable.append({"audit_id": rule.get("id"), "hospital": hospital, "variable": variable, "status": "candidate_column_unavailable"})
            continue
        bucket_states[str(rule["id"])] = BucketState(rule)
    relationship_states: dict[str, DifferenceAccumulator] = {}
    relationship_rules: dict[str, dict[str, Any]] = {}
    relationship_unavailable: list[dict[str, Any]] = []
    for rule in quality["relationship_audits"]:
        if rule.get("table") != table:
            continue
        scope = rule.get("hospital_scope")
        if isinstance(scope, list) and hospital not in scope:
            continue
        fields = rule.get("fields")
        if not isinstance(fields, list) or set(fields) - set(schema.names):
            relationship_unavailable.append({"audit_id": rule.get("id"), "hospital": hospital, "table": table, "status": "candidate_fields_unavailable"})
            continue
        audit_id = str(rule["id"])
        relationship_rules[audit_id] = rule
        relationship_states[audit_id] = DifferenceAccumulator(
            policy.sample_capacity,
            _stable_seed("relationship", hospital, audit_id),
            float(rule.get("absolute_tolerance", 0)),
        )
    list_states = {
        field.name: ListState(
            field,
            policy.sample_capacity,
            _stable_seed("list", table, hospital, field.name),
        )
        for field in list_fields
    }
    categorical_counts: dict[str, Counter[Any]] = {
        field.name: Counter() for field in categorical
    }
    categorical_truncated: set[str] = set()
    stay_values: set[str] = set()
    missing_identifier_count = 0
    identifier_formula_mismatch_count = 0
    hospital_id_mismatch_count = 0
    dynamic_not_in_static_count = 0
    adjacent_duplicate_time_key_count = 0
    time_decrease_within_stay_count = 0
    missing_time_count = 0
    previous_stay: str | None = None
    previous_time: float | None = None
    row_offset = 0
    ingested_provenance_batches = pq.ParquetFile(ingested_path).iter_batches(
        columns=list(PROVENANCE_FIELDS), batch_size=policy.rows_per_batch
    )
    provenance_matches = True
    ingested_provenance_rows = 0
    static_stay_array = (
        pa.array(sorted(static_stays), type=pa.large_string())
        if static_stays is not None
        else None
    )
    for batch in parquet.iter_batches(batch_size=policy.rows_per_batch, use_threads=True):
        ingested_provenance = next(ingested_provenance_batches, None)
        if ingested_provenance is None or len(ingested_provenance) != len(batch):
            provenance_matches = False
        else:
            ingested_provenance_rows += len(ingested_provenance)
            for name in PROVENANCE_FIELDS:
                left = batch.column(batch.schema.get_field_index(name))
                right = ingested_provenance.column(
                    ingested_provenance.schema.get_field_index(name)
                )
                provenance_matches = provenance_matches and left.equals(right)
        positions = _sample_positions(len(batch), policy.sample_rows_per_batch, row_offset)
        for field in numeric:
            values = batch.column(batch.schema.get_field_index(field.name))
            numeric_states[field.name].add(values, positions)
            state = invalid_states.get(field.name)
            if state is not None:
                state.add(values)
        for audit_id, state in targeted_states.items():
            variable = str(state.rule["column"])
            state.add(batch.column(batch.schema.get_field_index(variable)), positions)
        for state in bucket_states.values():
            variable = str(state.rule["column"])
            state.add(batch.column(batch.schema.get_field_index(variable)))
        for audit_id, state in relationship_states.items():
            values = _relationship_arrays(batch, relationship_rules[audit_id])
            if values is not None:
                state.add(values[0], values[1], positions)
        for field in categorical:
            values = batch.column(batch.schema.get_field_index(field.name))
            for item in pc.value_counts(values).to_pylist():
                categorical_counts[field.name][item["values"]] += int(item["counts"])
            if len(categorical_counts[field.name]) > policy.categorical_domain_cap:
                categorical_truncated.add(field.name)
                categorical_counts[field.name] = Counter(
                    dict(categorical_counts[field.name].most_common(policy.categorical_domain_cap))
                )
        for field in list_fields:
            values = batch.column(batch.schema.get_field_index(field.name))
            list_states[field.name].add(values, policy.sample_rows_per_batch, row_offset)
        local = batch.column(batch.schema.get_field_index("stay_id_local"))
        global_id = batch.column(batch.schema.get_field_index("stay_id_global"))
        hospital_values = batch.column(batch.schema.get_field_index("hospital_id"))
        missing_identifier_count += local.null_count + global_id.null_count
        expected_global = pc.binary_join_element_wise(
            pc.cast(local, pa.string()), pa.scalar(suffix), ":"
        )
        identifier_formula_mismatch_count += _bool_count(
            pc.and_(
                pc.and_(pc.is_valid(expected_global), pc.is_valid(global_id)),
                pc.not_equal(expected_global, pc.cast(global_id, pa.string())),
            )
        )
        hospital_id_mismatch_count += _bool_count(
            pc.and_(
                pc.is_valid(hospital_values),
                pc.not_equal(hospital_values, hospital),
            )
        ) + hospital_values.null_count
        unique_stays = [
            value for value in pc.unique(global_id).to_pylist() if isinstance(value, str)
        ]
        stay_values.update(unique_stays)
        if table == "dynamic" and static_stays is not None:
            assert static_stay_array is not None
            is_known = pc.is_in(global_id, value_set=static_stay_array)
            dynamic_not_in_static_count += _bool_count(
                pc.and_(pc.is_valid(global_id), pc.invert(pc.fill_null(is_known, False)))
            )
            if "minutes_since_icu_admission" in batch.schema.names:
                time_values = batch.column(batch.schema.get_field_index("minutes_since_icu_admission"))
                missing_time_count += len(time_values) - _bool_count(_valid_numeric_mask(time_values))
                if len(batch) > 1:
                    same_stay = pc.equal(global_id.slice(1), global_id.slice(0, len(batch) - 1))
                    left_time = time_values.slice(0, len(batch) - 1)
                    right_time = time_values.slice(1)
                    both_time = pc.and_(_valid_numeric_mask(left_time), _valid_numeric_mask(right_time))
                    adjacent_duplicate_time_key_count += _bool_count(
                        pc.and_(same_stay, pc.and_(both_time, pc.equal(left_time, right_time)))
                    )
                    time_decrease_within_stay_count += _bool_count(
                        pc.and_(same_stay, pc.and_(both_time, pc.less(right_time, left_time)))
                    )
                first_stay = global_id[0].as_py() if len(batch) else None
                first_time = time_values[0].as_py() if len(batch) and time_values[0].is_valid else None
                if previous_stay is not None and first_stay == previous_stay and previous_time is not None and first_time is not None:
                    adjacent_duplicate_time_key_count += int(float(first_time) == previous_time)
                    time_decrease_within_stay_count += int(float(first_time) < previous_time)
                if len(batch):
                    previous_stay = global_id[len(batch) - 1].as_py()
                    last_time = time_values[len(batch) - 1]
                    previous_time = float(last_time.as_py()) if last_time.is_valid and math.isfinite(float(last_time.as_py())) else None
        row_offset += len(batch)
        if progress is not None and row_offset % (policy.rows_per_batch * 20) == 0:
            progress(f"consolidated_audit_progress hospital={hospital} table={table} rows={row_offset}")
    provenance_matches = (
        provenance_matches
        and next(ingested_provenance_batches, None) is None
        and row_offset == ingested_provenance_rows
    )
    candidate_provenance_rows = row_offset
    numeric_profiles = [
        state.profile(table, hospital, variable, "v3_candidate")
        for variable, state in sorted(numeric_states.items())
    ]
    categorical_rows = [
        {
            "table": table,
            "hospital": hospital,
            "variable": variable,
            "value_repr": None if value is None else str(value),
            "value_is_null": value is None,
            "count": count,
            "domain_truncated": variable in categorical_truncated,
        }
        for variable, counts in sorted(categorical_counts.items())
        for value, count in sorted(counts.items(), key=lambda item: (str(item[0]), item[1]))
    ]
    list_profiles = [
        state.result(table, hospital, variable)
        for variable, state in sorted(list_states.items())
    ]
    invalid_rows = [
        state.result(table, hospital, variable)
        for variable, state in sorted(invalid_states.items())
    ]
    targeted_rows = list(targeted_unavailable)
    for audit_id, state in sorted(targeted_states.items()):
        variable = str(state.rule["column"])
        profile = state.accumulator.profile(table, hospital, variable, "targeted_scale_candidate")
        targeted_rows.append(
            {
                "audit_id": audit_id,
                "hospital": hospital,
                "table": table,
                "variable": variable,
                "operation": state.rule.get("operation"),
                "factor": state.rule.get("factor"),
                "threshold": state.rule.get("threshold"),
                "approval_status": state.rule.get("approval_status"),
                "values_considered_count": state.values_considered_count,
                "before_outside_expected_count": state.before_outside_expected_count,
                "after_outside_expected_count": state.after_outside_expected_count,
                "candidate_profile": profile,
                "action": "hypothesis_only_no_values_changed",
            }
        )
    bucket_rows = list(bucket_unavailable)
    bucket_rows.extend(
        {
            "audit_id": audit_id,
            "hospital": hospital,
            "table": table,
            "variable": state.rule.get("column"),
            "counts": dict(sorted(state.counts.items())),
            "reason": state.rule.get("reason"),
        }
        for audit_id, state in sorted(bucket_states.items())
    )
    relationship_rows = list(relationship_unavailable)
    relationship_rows.extend(
        state.result(
            audit_id,
            hospital,
            table,
            str(relationship_rules[audit_id].get("formula")),
        )
        for audit_id, state in sorted(relationship_states.items())
    )
    identifier_summary = {
        "hospital": hospital,
        "table": table,
        "row_count": row_offset,
        "unique_stay_count": len(stay_values),
        "missing_identifier_cell_count": missing_identifier_count,
        "identifier_formula_mismatch_count": identifier_formula_mismatch_count,
        "hospital_id_mismatch_count": hospital_id_mismatch_count,
        "dynamic_identifier_not_in_static_count": dynamic_not_in_static_count,
        "adjacent_duplicate_stay_time_count": adjacent_duplicate_time_key_count,
        "time_decrease_within_stay_count": time_decrease_within_stay_count,
        "missing_time_count": missing_time_count,
        "provenance_comparison": "cellwise_arrow_array_equality",
        "candidate_provenance_row_count": candidate_provenance_rows,
        "ingested_provenance_row_count": ingested_provenance_rows,
        "provenance_matches": provenance_matches,
    }
    return (
        TableScanResult(
            numeric_profiles=numeric_profiles,
            categorical_rows=categorical_rows,
            list_profiles=list_profiles,
            invalid_scale_rows=invalid_rows,
            targeted_scale_rows=targeted_rows,
            bucket_rows=bucket_rows,
            relationship_rows=relationship_rows,
            identifier_summary=identifier_summary,
        ),
        stay_values,
    )


def _scan_v2_translated_table(
    path: Path,
    table: str,
    candidate_numeric_variables: set[str],
    policy: ConsolidatedAuditPolicy,
    progress: Callable[[str], None] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, set[str]]]:
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    hospital_column = "hospital_id"
    stay_column = "stay_id_global"
    if hospital_column not in schema.names or stay_column not in schema.names:
        raise HarmonizationError("Frozen v2 translated identifiers are unavailable")
    numeric_names = sorted(
        field.name
        for field in schema
        if field.name in candidate_numeric_variables
        and (pa.types.is_integer(field.type) or pa.types.is_floating(field.type))
    )
    states: dict[tuple[str, str], NumericAccumulator] = {}
    row_counts: Counter[str] = Counter()
    stay_sets: dict[str, set[str]] = defaultdict(set)
    row_offset = 0
    columns = [hospital_column, stay_column, *numeric_names]
    for batch in parquet.iter_batches(
        columns=columns,
        batch_size=policy.rows_per_batch,
        use_threads=True,
    ):
        hospitals = batch.column(batch.schema.get_field_index(hospital_column))
        stays = batch.column(batch.schema.get_field_index(stay_column))
        for hospital_value in pc.unique(hospitals).to_pylist():
            if not isinstance(hospital_value, str):
                continue
            mask = pc.fill_null(pc.equal(hospitals, hospital_value), False)
            count = _bool_count(mask)
            row_counts[hospital_value] += count
            hospital_stays = pc.filter(stays, mask)
            stay_sets[hospital_value].update(
                value
                for value in pc.unique(hospital_stays).to_pylist()
                if isinstance(value, str)
            )
            positions = _sample_positions(
                count,
                policy.sample_rows_per_batch,
                row_offset + _stable_seed(hospital_value, table),
            )
            for variable in numeric_names:
                values = pc.filter(
                    batch.column(batch.schema.get_field_index(variable)), mask
                )
                state = states.setdefault(
                    (hospital_value, variable),
                    NumericAccumulator(
                        policy.sample_capacity,
                        _stable_seed("v2", table, hospital_value, variable),
                    ),
                )
                state.add(values, positions)
        row_offset += len(batch)
        if progress is not None and row_offset % (policy.rows_per_batch * 20) == 0:
            progress(f"consolidated_audit_v2_progress table={table} rows={row_offset}")
    profiles = [
        state.profile(table, hospital, variable, "frozen_v2_translated")
        for (hospital, variable), state in sorted(states.items())
    ]
    summary = {
        "table": table,
        "path": str(path),
        "sha256": sha256_file(path),
        "row_count": parquet.metadata.num_rows,
        "hospital_row_counts": dict(sorted(row_counts.items())),
        "hospital_unique_stay_counts": {
            hospital: len(values) for hospital, values in sorted(stay_sets.items())
        },
        "profiled_numeric_variable_count": len(numeric_names),
    }
    return profiles, summary, stay_sets


def _scan_v2_pooled_identity(
    path: Path,
    table: str,
    stay_column: str,
    hospital_column: str,
    rows_per_batch: int,
) -> tuple[dict[str, Any], dict[str, set[str]]]:
    parquet = pq.ParquetFile(path)
    if {stay_column, hospital_column} - set(parquet.schema_arrow.names):
        raise HarmonizationError("Frozen pooled identifier columns are unavailable")
    row_counts: Counter[str] = Counter()
    stay_sets: dict[str, set[str]] = defaultdict(set)
    for batch in parquet.iter_batches(
        columns=[stay_column, hospital_column], batch_size=rows_per_batch
    ):
        stays = batch.column(0)
        hospitals = batch.column(1)
        for hospital_value in pc.unique(hospitals).to_pylist():
            if hospital_value is None:
                continue
            try:
                hospital_key = f"asic_UK{int(float(hospital_value)):02d}"
            except (TypeError, ValueError, OverflowError):
                hospital_key = f"unrecognized:{hospital_value}"
            mask = pc.fill_null(pc.equal(hospitals, hospital_value), False)
            row_counts[hospital_key] += _bool_count(mask)
            stay_sets[hospital_key].update(
                str(value)
                for value in pc.unique(pc.filter(stays, mask)).to_pylist()
                if value is not None
            )
    return (
        {
            "table": table,
            "path": str(path),
            "sha256": sha256_file(path),
            "row_count": parquet.metadata.num_rows,
            "hospital_row_counts": dict(sorted(row_counts.items())),
            "hospital_unique_stay_counts": {
                hospital: len(values)
                for hospital, values in sorted(stay_sets.items())
            },
            "role": "untrusted_lossy_comparison_only",
        },
        stay_sets,
    )


def _median(values: Iterable[float]) -> float | None:
    return _quantile(values, 0.5)


def _scale_signals(
    profiles: list[dict[str, Any]], policy: ConsolidatedAuditPolicy
) -> list[dict[str, Any]]:
    by_variable: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in profiles:
        if row.get("source") == "v3_candidate":
            by_variable[(str(row["table"]), str(row["variable"]))].append(row)
    signals: list[dict[str, Any]] = []
    for (table, variable), rows in sorted(by_variable.items()):
        eligible = [
            row
            for row in rows
            if int(row.get("finite_count", 0)) >= policy.minimum_distribution_count
            and _finite_number(row.get("median")) not in {None, 0.0}
        ]
        if len(eligible) < policy.minimum_distribution_hospitals:
            continue
        for row in eligible:
            target = abs(float(row["median"]))
            peers = [
                abs(float(peer["median"]))
                for peer in eligible
                if peer["hospital"] != row["hospital"]
                and float(peer["median"]) != 0
            ]
            peer_median = _median(peers)
            if not target or peer_median is None or not peer_median:
                continue
            baseline = max(target / peer_median, peer_median / target)
            candidates = []
            for factor in policy.general_scale_factors:
                transformed = target * factor
                residual = max(transformed / peer_median, peer_median / transformed)
                candidates.append((residual, factor))
            residual, factor = min(candidates)
            if (
                baseline >= policy.peer_minimum_improvement_factor
                and residual <= policy.peer_maximum_residual_factor
            ):
                signals.append(
                    {
                        "table": table,
                        "variable": variable,
                        "hospital": row["hospital"],
                        "target_median": row["median"],
                        "peer_median_of_hospital_medians": peer_median,
                        "baseline_scale_ratio": baseline,
                        "candidate_multiplier": factor,
                        "post_multiplier_residual_ratio": residual,
                        "classification": "cross_hospital_scale_hypothesis_not_unit_proof",
                        "action": "human_unit_or_definition_review_required",
                    }
                )
    return signals


def _distribution_outliers(
    profiles: list[dict[str, Any]], policy: ConsolidatedAuditPolicy
) -> list[dict[str, Any]]:
    metrics = ("min", "median", "iqr", "max", "finite_rate")
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in profiles:
        if row.get("source") == "v3_candidate":
            grouped[(str(row["table"]), str(row["variable"]))].append(row)
    findings: list[dict[str, Any]] = []
    for (table, variable), rows in sorted(grouped.items()):
        eligible = [
            row
            for row in rows
            if int(row.get("finite_count", 0)) >= policy.minimum_distribution_count
        ]
        if len(eligible) < policy.minimum_distribution_hospitals:
            continue
        for metric in metrics:
            values = [
                float(row[metric])
                for row in eligible
                if _finite_number(row.get(metric)) is not None
            ]
            if len(values) < policy.minimum_distribution_hospitals:
                continue
            q1 = _quantile(values, 0.25)
            q3 = _quantile(values, 0.75)
            if q1 is None or q3 is None:
                continue
            iqr = q3 - q1
            lower = q1 - policy.iqr_fence_factor * iqr
            upper = q3 + policy.iqr_fence_factor * iqr
            for row in eligible:
                value = _finite_number(row.get(metric))
                if value is not None and (value < lower or value > upper):
                    findings.append(
                        {
                            "table": table,
                            "variable": variable,
                            "hospital": row["hospital"],
                            "metric": metric,
                            "value": value,
                            "lower_iqr_fence": lower,
                            "upper_iqr_fence": upper,
                            "classification": "distribution_screen_flag_not_unit_proof",
                        }
                    )
    return findings


def _cross_version_rows(
    candidate_profiles: list[dict[str, Any]],
    v2_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    old = {
        (row["table"], row["hospital"], row["variable"]): row
        for row in v2_profiles
    }
    rows: list[dict[str, Any]] = []
    for current in candidate_profiles:
        if current.get("source") != "v3_candidate":
            continue
        key = (current["table"], current["hospital"], current["variable"])
        previous = old.get(key)
        if previous is None:
            rows.append(
                {
                    "table": key[0],
                    "hospital": key[1],
                    "variable": key[2],
                    "comparison_status": "not_available_in_frozen_v2_translated_profile",
                    "v3_finite_count": current["finite_count"],
                    "v2_finite_count": None,
                    "finite_count_difference": None,
                    "v3_median": current["median"],
                    "v2_median": None,
                    "median_ratio_v3_over_v2": None,
                }
            )
            continue
        v3_median = _finite_number(current.get("median"))
        v2_median = _finite_number(previous.get("median"))
        ratio = (
            v3_median / v2_median
            if v3_median is not None and v2_median not in {None, 0.0}
            else None
        )
        rows.append(
            {
                "table": key[0],
                "hospital": key[1],
                "variable": key[2],
                "comparison_status": "untrusted_v2_comparison_complete",
                "v3_finite_count": current["finite_count"],
                "v2_finite_count": previous["finite_count"],
                "finite_count_difference": int(current["finite_count"]) - int(previous["finite_count"]),
                "v3_median": v3_median,
                "v2_median": v2_median,
                "median_ratio_v3_over_v2": ratio,
            }
        )
    return rows


def _optional_old_version_evidence(
    comparison_status: str,
    comparison_paths: dict[str, Path],
    candidate_profiles: list[dict[str, Any]],
    unit_rows: list[dict[str, Any]],
    static_stays: dict[str, set[str]],
    identifier_rows: list[dict[str, Any]],
    hospitals: tuple[str, ...],
    policy: ConsolidatedAuditPolicy,
    progress: Callable[[str], None] | None,
) -> tuple[
    str,
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Read optional historical context without making it a v3 prerequisite."""
    if comparison_status != "available_untrusted_read_only":
        return comparison_status, [], [], []
    profiles: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    stays_by_source: dict[str, dict[str, set[str]]] = {}
    try:
        numeric_variables = {str(row["variable"]) for row in unit_rows}
        for table in ("static", "dynamic"):
            table_profiles, summary, stays = _scan_v2_translated_table(
                comparison_paths[f"translated_{table}"],
                table,
                numeric_variables,
                policy,
                progress,
            )
            profiles.extend(table_profiles)
            summaries.append(summary)
            stays_by_source[f"translated_{table}"] = stays
            pooled_summary, pooled_stays = _scan_v2_pooled_identity(
                comparison_paths[f"pooled_{table}"],
                table,
                "Pseudo-ID",
                "hid",
                policy.rows_per_batch,
            )
            summaries.append(pooled_summary)
            stays_by_source[f"pooled_{table}"] = pooled_stays
    except (HarmonizationError, OSError, ValueError, KeyError, pa.ArrowException):
        return "skipped_scan_invalid", [], [], []

    numeric_rows = _cross_version_rows(candidate_profiles, profiles)
    identity_rows: list[dict[str, Any]] = []
    for table in ("static", "dynamic"):
        current = static_stays if table == "static" else {
            row["hospital"]: set()
            for row in identifier_rows
            if row["table"] == table
        }
        translated = stays_by_source[f"translated_{table}"]
        pooled = stays_by_source[f"pooled_{table}"]
        for hospital in hospitals:
            current_set = current.get(hospital, set())
            translated_set = translated.get(hospital, set())
            pooled_set = pooled.get(hospital, set())
            identity_rows.append(
                {
                    "table": table,
                    "hospital": hospital,
                    "v3_unique_stay_count": next(
                        row["unique_stay_count"]
                        for row in identifier_rows
                        if row["table"] == table and row["hospital"] == hospital
                    ),
                    "old_translated_unique_stay_count": len(translated_set),
                    "old_pooled_unique_local_stay_count": len(pooled_set),
                    "v3_static_vs_old_translated_overlap_count": (
                        len(current_set & translated_set)
                        if table == "static"
                        else None
                    ),
                    "role": "optional_untrusted_old_version_comparison_only",
                }
            )
    return comparison_status, summaries, numeric_rows, identity_rows


def _unit_review_rows(
    dictionary_rows: list[dict[str, Any]],
    candidate_profiles: list[dict[str, Any]],
    list_profiles: list[dict[str, Any]],
    scale_signals: list[dict[str, Any]],
    distribution_findings: list[dict[str, Any]],
    targeted_scale_rows: list[dict[str, Any]],
    quality: dict[str, Any],
) -> list[dict[str, Any]]:
    numeric_keys = {
        (str(row["table"]), str(row["variable"]))
        for row in candidate_profiles
        if row.get("source") == "v3_candidate"
    }
    numeric_keys.update(
        (str(row["table"]), str(row["variable"]))
        for row in list_profiles
        if row.get("element_finite_count") is not None
    )
    known_by_column: dict[str, list[str]] = defaultdict(list)
    for section in (
        "known_legacy_semantic_findings",
        "targeted_scale_audits",
        "targeted_comparison_audits",
        "targeted_bucket_audits",
    ):
        for rule in quality[section]:
            column = rule.get("column")
            if isinstance(column, str):
                known_by_column[column].append(str(rule.get("id")))
    result: list[dict[str, Any]] = []
    for dictionary in dictionary_rows:
        key = (str(dictionary["table"]), str(dictionary["variable"]))
        unit = str(dictionary.get("candidate_unit"))
        unit_status = str(dictionary.get("unit_review_status"))
        pending_non_numeric_unit = (
            unit == "unresolved"
            or unit_status not in APPROVED_REVIEW_STATUSES
            and unit_status != "not_applicable"
        )
        if key not in numeric_keys and not pending_non_numeric_unit:
            continue
        signals = [row for row in scale_signals if (row["table"], row["variable"]) == key]
        distributions = [row for row in distribution_findings if (row["table"], row["variable"]) == key]
        targeted = [row for row in targeted_scale_rows if row.get("variable") == key[1]]
        approved = unit_status in APPROVED_REVIEW_STATUSES and unit != "unresolved"
        known_ids = sorted(set(known_by_column.get(key[1], [])))
        if approved and not signals and not known_ids:
            recommendation = "retain_approved_unit_and_monitor"
            blocking = False
        elif any(item == "uk04_etco2_pa_scale" for item in known_ids):
            recommendation = "review_legacy_documented_conversion_against_complete_production_evidence"
            blocking = True
        elif signals:
            recommendation = "resolve_hospital_specific_scale_or_definition_before_schema_freeze"
            blocking = True
        elif unit == "unresolved":
            recommendation = "source_metadata_required_before_variable_is_analysis_eligible"
            blocking = True
        elif known_ids:
            recommendation = "resolve_known_legacy_unit_or_definition_finding"
            blocking = True
        else:
            recommendation = "candidate_unit_has_no_detected_scale_signal_but_requires_human_approval"
            blocking = not approved
        result.append(
            {
                "table": key[0],
                "variable": key[1],
                "candidate_unit": unit,
                "unit_review_status": unit_status,
                "source_hospital_count": dictionary.get("source_hospital_count"),
                "numeric_distribution_profiled": key in numeric_keys,
                "legacy_or_targeted_review_ids": known_ids,
                "cross_hospital_scale_signal_count": len(signals),
                "distribution_flag_count": len(distributions),
                "targeted_scale_audit_count": len(targeted),
                "recommendation": recommendation,
                "blocks_harmonized_release": blocking,
                "unit_policy_audited": True,
                "automatic_conversion_applied": False,
            }
        )
    return result


def _targeted_comparison_rows(
    quality: dict[str, Any],
    profiles: list[dict[str, Any]],
    scale_signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in quality["targeted_comparison_audits"]:
        variable = str(rule.get("column"))
        matching = [
            row
            for row in profiles
            if row.get("source") == "v3_candidate" and row.get("variable") == variable
        ]
        rows.append(
            {
                "audit_id": rule.get("id"),
                "variable": variable,
                "reason": rule.get("reason"),
                "prior_approval_status": rule.get("approval_status"),
                "hospital_profile_count": len(matching),
                "hospitals_with_values": sorted(
                    row["hospital"] for row in matching if int(row.get("finite_count", 0)) > 0
                ),
                "production_medians": {
                    row["hospital"]: row.get("median") for row in matching
                },
                "detected_scale_signal_count": sum(
                    item["variable"] == variable for item in scale_signals
                ),
                "resolution": "complete_production_evidence_generated_human_or_metadata_decision_pending",
            }
        )
    return rows


def _semantic_finding_rows(
    quality: dict[str, Any],
    profiles: list[dict[str, Any]],
    targeted_scale_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in quality["known_legacy_semantic_findings"]:
        variable = str(rule.get("column"))
        matching = [
            row
            for row in profiles
            if row.get("source") == "v3_candidate"
            and row.get("variable") == variable
            and (rule.get("hospital") == "all" or row.get("hospital") == rule.get("hospital"))
        ]
        targeted = [
            row
            for row in targeted_scale_rows
            if row.get("variable") == variable
            and (rule.get("hospital") == "all" or row.get("hospital") == rule.get("hospital"))
        ]
        rows.append(
            {
                "finding_id": rule.get("id"),
                "variable": variable,
                "hospital": rule.get("hospital"),
                "prior_classification": rule.get("prior_classification"),
                "prior_action": rule.get("prior_action"),
                "prior_decision": rule.get("decision"),
                "production_profile_count": len(matching),
                "production_finite_count": sum(int(row.get("finite_count", 0)) for row in matching),
                "production_medians": {row["hospital"]: row.get("median") for row in matching},
                "targeted_candidate_evidence_count": len(targeted),
                "resolution": "production_evidence_complete_manual_semantic_decision_pending",
            }
        )
    return rows


def _availability_rows(
    quality: dict[str, Any], profiles: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in quality["availability_audits"]:
        columns = rule.get("columns", [])
        expected = sorted(str(item) for item in rule.get("expected_non_missing_hospitals", []))
        observed = sorted(
            {
                str(row["hospital"])
                for row in profiles
                if row.get("source") == "v3_candidate"
                and row.get("variable") in columns
                and int(row.get("finite_count", 0)) > 0
            }
        )
        rows.append(
            {
                "audit_id": rule.get("id"),
                "columns": columns,
                "expected_non_missing_hospitals": expected,
                "observed_non_missing_hospitals": observed,
                "matches_expectation": observed == expected,
                "provenance": rule.get("provenance"),
            }
        )
    return rows


def _row_scale_entry_rows(
    quality: dict[str, Any], invalid_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rule in quality["row_level_scale_entry_audits"]:
        variable = str(rule.get("column"))
        operation = str(rule.get("operation"))
        factor = float(rule.get("factor"))
        multiplier = 1.0 / factor if operation == "divide" else factor
        matching = [row for row in invalid_rows if row.get("variable") == variable]
        rows.append(
            {
                "audit_id": rule.get("id"),
                "variable": variable,
                "operation": operation,
                "factor": factor,
                "recovery_min": rule.get("recovery_min"),
                "recovery_max": rule.get("recovery_max"),
                "approval_status": rule.get("approval_status"),
                "hospital_profile_count": len(matching),
                "invalid_value_count": sum(
                    int(row.get("invalid_count", 0)) for row in matching
                ),
                "candidate_recovery_count": sum(
                    int(row.get("recoverable_by_factor", {}).get(multiplier, 0))
                    for row in matching
                ),
                "action": "cleaning_candidate_only_no_value_changed",
            }
        )
    return rows


def _context_audit_rows(
    quality: dict[str, Any], invalid_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    context = quality.get("candidate_context_audit")
    if not isinstance(context, dict):
        return []
    windows = context.get("windows_hours")
    fields = context.get("fields")
    if not isinstance(windows, list) or not isinstance(fields, list):
        raise HarmonizationError("Frozen candidate-context audit is malformed")
    rows: list[dict[str, Any]] = []
    for item in fields:
        if not isinstance(item, dict) or not isinstance(item.get("column"), str):
            raise HarmonizationError("Frozen candidate-context field is malformed")
        variable = str(item["column"])
        excluded = set(str(value) for value in item.get("excluded_hospitals", []))
        matching = [
            row
            for row in invalid_rows
            if row.get("variable") == variable and row.get("hospital") not in excluded
        ]
        rows.append(
            {
                "variable": variable,
                "windows_hours": windows,
                "excluded_hospitals": sorted(excluded),
                "related_fields": item.get("related_fields", []),
                "relationship": item.get("relationship"),
                "eligible_hospital_profile_count": len(matching),
                "invalid_value_count": sum(
                    int(row.get("invalid_count", 0)) for row in matching
                ),
                "uniquely_scale_recoverable_count": sum(
                    int(row.get("uniquely_recoverable_count", 0))
                    for row in matching
                ),
                "status": "candidates_identified_contextual_cleaning_review_pending",
                "action": "no_temporal_correction_or_value_change_applied",
            }
        )
    return rows


def _static_sentinel_rows(
    quality: dict[str, Any],
    numeric_profiles: list[dict[str, Any]],
    categorical_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rules = quality.get("legacy_static_minus_one_sentinels")
    if not isinstance(rules, list):
        raise HarmonizationError("Frozen static sentinel rules are unavailable")
    rows: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict) or not isinstance(rule.get("column"), str):
            raise HarmonizationError("Frozen static sentinel rule is malformed")
        variable = str(rule["column"])
        if rule.get("value_kind") == "numeric":
            residual = sum(
                int(row.get("minus_one_count", 0))
                for row in numeric_profiles
                if row.get("table") == "static" and row.get("variable") == variable
            )
        else:
            residual = sum(
                int(row.get("count", 0))
                for row in categorical_rows
                if row.get("table") == "static"
                and row.get("variable") == variable
                and row.get("value_repr") == str(rule.get("value"))
            )
        rows.append(
            {
                "variable": variable,
                "sentinel": str(rule.get("value")),
                "value_kind": rule.get("value_kind"),
                "residual_sentinel_count_after_candidate_harmonization": residual,
                "passes": residual == 0,
            }
        )
    return rows


def _pbw_formula_audit(
    candidate_root: Path,
    policy: ConsolidatedAuditPolicy,
    quality: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate the frozen UK00 PBW hypothesis without choosing a height summary."""

    rule = quality.get("predicted_body_weight_tidal_volume_audit")
    if not isinstance(rule, dict):
        return {
            "audit_id": "uk00_ardsnet_pbw_tidal_volume_formula_review",
            "status": "frozen_rule_unavailable",
        }
    hospital = str(rule.get("hospital"))
    static_path = candidate_root / hospital / "static.parquet"
    dynamic_path = candidate_root / hospital / "dynamic.parquet"
    static_schema = pq.read_schema(static_path)
    dynamic_schema = pq.read_schema(dynamic_path)
    height_column = (
        "height_measurements_cm"
        if "height_measurements_cm" in static_schema.names
        else str(rule.get("height_column"))
    )
    required_static = {"stay_id_global", "sex", height_column, "weight_kg"}
    required_dynamic = {
        "stay_id_global",
        str(rule.get("vt_column")),
        str(rule.get("vt_per_kg_column")),
        str(rule.get("target_absolute_vt_column")),
    }
    missing = sorted(
        (required_static - set(static_schema.names))
        | (required_dynamic - set(dynamic_schema.names))
    )
    if missing:
        return {
            "audit_id": rule.get("id"),
            "hospital": hospital,
            "status": "candidate_fields_unavailable",
            "missing_candidate_fields": missing,
            "action": "explicitly_accounted_no_formula_inference",
        }
    male = str(rule.get("male_value"))
    female = str(rule.get("female_value"))
    height_min = float(rule.get("plausible_height_min_cm"))
    height_max = float(rule.get("plausible_height_max_cm"))
    weight_min = float(rule.get("plausible_actual_weight_min_kg"))
    weight_max = float(rule.get("plausible_actual_weight_max_kg"))
    height_center = float(rule.get("height_center_cm"))
    coefficient = float(rule.get("height_coefficient_kg_per_cm"))
    target_per_kg = float(rule.get("target_ml_per_kg_pbw"))
    male_intercept = float(rule.get("male_intercept_kg"))
    female_intercept = float(rule.get("female_intercept_kg"))
    formula_tolerance = float(rule.get("absolute_tolerance_ml"))
    normalized_tolerance = float(rule.get("normalized_tolerance_ml_per_kg"))
    lookup: dict[str, tuple[tuple[float, ...], float | None]] = {}
    static_rows = 0
    eligible_static_stays = 0
    multiple_height_stays = 0
    for batch in pq.ParquetFile(static_path).iter_batches(
        columns=["stay_id_global", "sex", height_column, "weight_kg"],
        batch_size=policy.rows_per_batch,
    ):
        for stay, sex, heights, weight in zip(
            *(column.to_pylist() for column in batch.columns), strict=True
        ):
            static_rows += 1
            if not isinstance(stay, str) or sex not in {male, female}:
                continue
            height_values = heights if isinstance(heights, list) else [heights]
            plausible = sorted(
                {
                    float(value)
                    for value in height_values
                    if value is not None
                    and math.isfinite(float(value))
                    and height_min <= float(value) <= height_max
                }
            )
            if not plausible:
                continue
            intercept = male_intercept if sex == male else female_intercept
            targets = tuple(
                target_per_kg
                * (intercept + coefficient * (height - height_center))
                for height in plausible
            )
            actual_weight = (
                float(weight)
                if weight is not None
                and math.isfinite(float(weight))
                and weight_min <= float(weight) <= weight_max
                else None
            )
            lookup[stay] = (targets, actual_weight)
            eligible_static_stays += 1
            multiple_height_stays += int(len(targets) > 1)
    counters: Counter[str] = Counter()
    vt_column = str(rule.get("vt_column"))
    vt_per_kg_column = str(rule.get("vt_per_kg_column"))
    target_column = str(rule.get("target_absolute_vt_column"))
    for batch in pq.ParquetFile(dynamic_path).iter_batches(
        columns=["stay_id_global", vt_column, vt_per_kg_column, target_column],
        batch_size=policy.rows_per_batch,
    ):
        for stay, vt, vt_per_kg, target in zip(
            *(column.to_pylist() for column in batch.columns), strict=True
        ):
            counters["dynamic_rows"] += 1
            static = lookup.get(stay) if isinstance(stay, str) else None
            if static is None:
                continue
            targets, actual_weight = static
            counters["rows_with_plausible_static_inputs"] += 1
            if target is not None and math.isfinite(float(target)):
                candidate = float(target)
                counters["target_absolute_present"] += 1
                counters["target_matches_any_height_pbw"] += int(
                    any(abs(candidate - expected) <= formula_tolerance for expected in targets)
                )
            if (
                vt is not None
                and vt_per_kg is not None
                and actual_weight is not None
                and math.isfinite(float(vt))
                and math.isfinite(float(vt_per_kg))
            ):
                counters["vt_weight_relationship_present"] += 1
                counters["vt_per_kg_matches_actual_weight"] += int(
                    abs(float(vt_per_kg) - float(vt) / actual_weight)
                    <= normalized_tolerance
                )
    return {
        "audit_id": rule.get("id"),
        "hospital": hospital,
        "status": "complete_hypothesis_evidence_no_semantic_assignment",
        "static_row_count": static_rows,
        "eligible_static_stay_count": eligible_static_stays,
        "eligible_stays_with_multiple_height_measurements": multiple_height_stays,
        **dict(sorted(counters.items())),
        "height_policy": "match_against_every_plausible_preserved_measurement_no_aggregation",
        "approval_status": rule.get("approval_status"),
        "automatic_semantic_assignment": False,
    }


def _private_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    _write_private_parquet(path, tuple(rows))
    path.chmod(0o600)


def _decision_level_evidence(
    relationship_rows: list[dict[str, Any]],
    invalid_rows: list[dict[str, Any]],
    row_scale_entry_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[tuple[str, str]], set[str]]:
    """Collapse hospital evidence to policy decisions without discarding evidence."""

    relationship_ids = {
        str(row.get("audit_id"))
        for row in relationship_rows
        if isinstance(row.get("audit_id"), str) and row.get("audit_id")
    }
    relationship_decisions = [
        {
            "audit_id": audit_id,
            "table": next(
                (
                    row.get("table")
                    for row in relationship_rows
                    if row.get("audit_id") == audit_id
                ),
                None,
            ),
            "variable": None,
            "hospital": None,
            "evidence_row_count": sum(
                row.get("audit_id") == audit_id for row in relationship_rows
            ),
        }
        for audit_id in sorted(relationship_ids)
    ]
    invalid_decisions = {
        (str(row.get("table")), str(row.get("variable")))
        for row in invalid_rows
        if int(row.get("invalid_count", 0)) > 0
    }
    row_scale_decisions = {
        str(row.get("audit_id"))
        for row in row_scale_entry_rows
        if isinstance(row.get("audit_id"), str)
        and row.get("audit_id")
        and int(row.get("candidate_recovery_count", 0)) > 0
    }
    return relationship_decisions, invalid_decisions, row_scale_decisions


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 consolidated harmonization audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input candidate run: `{payload['candidate_run_id']}`",
        "- Candidate data remain non-publishable",
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
    lines.extend(("", "## Audit totals", ""))
    lines.extend(f"- {key}: `{value}`" for key, value in metrics.items())
    comparison = payload["optional_old_version_comparison"]
    lines.extend(("", "## Optional older-version comparison", ""))
    lines.append(f"- Status: `{comparison['status']}`")
    lines.append("- Required for v3: `false`")
    lines.append("- Authoritative for v3: `false`")
    lines.extend(("", "## Unit-policy disposition", ""))
    unit = payload["unit_policy_summary"]
    lines.extend(f"- {key}: `{value}`" for key, value in unit.items())
    lines.extend(("", "## Highest-priority review groups", ""))
    for item in payload["priority_review_groups"]:
        lines.append(
            f"- `{item['group']}`: `{item['count']}` findings; {item['next_action']}"
        )
    lines.extend(("", "## Human review gate", ""))
    lines.append(
        "Review the complete owner-only decision register, especially every unit-policy row and every hospital-scoped scale or semantic finding. No distribution-derived conversion is approved automatically."
    )
    lines.extend(("", "## Limitations", ""))
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def run_consolidated_harmonization_audit(
    config: ConsolidatedAuditConfig,
    candidate_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> ConsolidatedAuditResult:
    policy = load_consolidated_audit_policy(config.policy_path)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(candidate_run_id) or not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid consolidated-audit or candidate run ID")
    private_dir = (
        config.reports_root / "private" / policy.private_directory_name / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Consolidated audit run ID already exists")
    immutable = {
        policy.dry_run_policy_path: policy.dry_run_policy_sha256,
        policy.quality_policy_path: policy.quality_policy_sha256,
    }
    for path, expected_hash in immutable.items():
        if not path.is_file() or sha256_file(path) != expected_hash:
            raise HarmonizationError(
                f"Immutable audit evidence is missing or changed: {path.name}"
            )
    dry_policy = load_harmonization_dry_run_policy(policy.dry_run_policy_path)
    suffixes = dict(policy.hospital_suffixes)
    hospitals = tuple(suffixes)
    if (
        len(hospitals) != policy.hospital_count
        or dry_policy.approved_hospitals != hospitals
    ):
        raise HarmonizationError("Candidate hospital scope differs from approved audit scope")
    quality = _quality_rules(
        load_yaml_mapping(policy.quality_policy_path, "V3 cross-hospital quality policy")
    )
    # The cross-table PBW audit is intentionally kept in addition to all list sections.
    raw_quality = load_yaml_mapping(
        policy.quality_policy_path, "V3 cross-hospital quality policy"
    )
    quality["predicted_body_weight_tidal_volume_audit"] = raw_quality.get(
        "predicted_body_weight_tidal_volume_audit"
    )
    quality["candidate_context_audit"] = raw_quality.get("candidate_context_audit")
    quality["legacy_static_minus_one_sentinels"] = raw_quality.get(
        "legacy_static_minus_one_sentinels"
    )
    comparison_paths: dict[str, Path] = {}
    comparison_status = "skipped_not_configured"
    if policy.comparison_reference_path is not None:
        if not policy.comparison_reference_path.is_file():
            comparison_status = "skipped_reference_config_unavailable"
        elif (
            policy.comparison_reference_sha256 is None
            or sha256_file(policy.comparison_reference_path)
            != policy.comparison_reference_sha256
        ):
            comparison_status = "skipped_reference_config_changed"
        else:
            try:
                comparison_paths = _comparison_paths(
                    policy.comparison_reference_path
                )
            except (ConfigurationError, HarmonizationError, OSError):
                comparison_status = "skipped_reference_config_invalid"
                comparison_paths = {}
            else:
                if all(path.is_file() for path in comparison_paths.values()):
                    comparison_status = "available_untrusted_read_only"
                else:
                    comparison_status = "skipped_artifacts_unavailable"
                    comparison_paths = {}

    candidate_root = config.data_root / dry_policy.output_directory_name / candidate_run_id
    candidate_manifest_path = candidate_root / "candidate_run_manifest.json"
    private_candidate = (
        config.reports_root / "private" / "harmonization_dry_run" / candidate_run_id
    )
    candidate_review_path = (
        config.reports_root
        / "review"
        / "harmonization_dry_run"
        / f"{candidate_run_id}.json"
    )
    required_candidate_paths = (
        candidate_manifest_path,
        private_candidate / "harmonization_dry_run_manifest.json",
        private_candidate / "column_metrics.parquet",
        private_candidate / "unresolved_tokens.parquet",
        private_candidate / "categorical_values.parquet",
        private_candidate / "candidate_variable_dictionary.parquet",
        private_candidate / "occurrence_plan.parquet",
        candidate_review_path,
    )
    if any(not path.is_file() for path in required_candidate_paths):
        raise HarmonizationError("Candidate harmonization evidence is incomplete")
    candidate_manifest = _read_json(candidate_manifest_path, "Candidate manifest")
    private_manifest = _read_json(
        private_candidate / "harmonization_dry_run_manifest.json",
        "Candidate private manifest",
    )
    candidate_review = _read_json(candidate_review_path, "Candidate review")
    if (
        candidate_manifest.get("artifact") != "asic_v3_harmonization_candidate_run"
        or candidate_manifest.get("artifact_version") != policy.candidate_artifact_version
        or candidate_manifest.get("run_id") != candidate_run_id
        or candidate_manifest.get("dataset_context") != config.dataset_context
        or candidate_manifest.get("status") != policy.candidate_status
        or candidate_manifest.get("publication_ready") is not False
        or candidate_manifest.get("cleaning_allowed") is not False
        or candidate_manifest.get("derivation_allowed") is not False
    ):
        raise HarmonizationError("Candidate run manifest is incompatible")
    if (
        private_manifest.get("artifact") != "asic_v3_harmonization_dry_run_private"
        or private_manifest.get("artifact_version") != policy.candidate_private_artifact_version
        or private_manifest.get("run_id") != candidate_run_id
        or private_manifest.get("candidate_run_manifest_sha256")
        != sha256_file(candidate_manifest_path)
        or candidate_review.get("artifact") != "asic_v3_harmonization_dry_run_review"
        or candidate_review.get("artifact_version") != policy.candidate_review_artifact_version
        or candidate_review.get("metrics", {}).get("technical_blocking_finding_count")
        != 0
    ):
        raise HarmonizationError("Candidate private or sanitized evidence is incompatible")
    dictionary_rows = _read_private_rows(
        private_candidate / "candidate_variable_dictionary.parquet"
    )
    metric_rows = _read_private_rows(private_candidate / "column_metrics.parquet")
    unresolved_rows = _read_private_rows(private_candidate / "unresolved_tokens.parquet")
    categorical_source_rows = _read_private_rows(
        private_candidate / "categorical_values.parquet"
    )
    occurrence_rows = _read_private_rows(private_candidate / "occurrence_plan.parquet")
    if (
        private_manifest.get("candidate_variable_dictionary_row_count")
        != len(dictionary_rows)
        or private_manifest.get("column_metric_row_count") != len(metric_rows)
        or private_manifest.get("unresolved_example_row_count") != len(unresolved_rows)
        or private_manifest.get("categorical_value_row_count")
        != len(categorical_source_rows)
    ):
        raise HarmonizationError("Candidate private evidence row counts changed")
    if not occurrence_rows or any(
        row.get("target") is None
        and row.get("proposed_action") != "drop_after_all_missing_precondition"
        for row in occurrence_rows
    ):
        raise HarmonizationError("Candidate occurrence dispositions are incomplete")
    manifest_hospitals = {
        str(item.get("hospital")): item
        for item in candidate_manifest.get("hospitals", [])
        if isinstance(item, dict)
    }
    if tuple(manifest_hospitals) != hospitals:
        raise HarmonizationError("Candidate manifest hospital order or scope changed")

    numeric_profiles: list[dict[str, Any]] = []
    categorical_rows: list[dict[str, Any]] = []
    list_profiles: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    targeted_scale_rows: list[dict[str, Any]] = []
    bucket_rows: list[dict[str, Any]] = []
    relationship_rows: list[dict[str, Any]] = []
    identifier_rows: list[dict[str, Any]] = []
    static_stays: dict[str, set[str]] = {}
    common_schemas: dict[str, pa.Schema] = {}
    hash_failure_count = 0
    row_failure_count = 0
    schema_failure_count = 0
    for table in ("static", "dynamic"):
        for hospital in hospitals:
            candidate_path = candidate_root / hospital / f"{table}.parquet"
            ingested_path = config.ingested_root / hospital / f"{table}.parquet"
            hospital_manifest_path = candidate_root / hospital / "candidate_manifest.json"
            ingestion_manifest_path = config.ingested_root / hospital / "ingestion_manifest.json"
            if any(
                not path.is_file()
                for path in (
                    candidate_path,
                    ingested_path,
                    hospital_manifest_path,
                    ingestion_manifest_path,
                )
            ):
                raise HarmonizationError(f"Candidate audit input is incomplete for {hospital}")
            hospital_manifest = _read_json(
                hospital_manifest_path, "Hospital candidate manifest"
            )
            ingestion_manifest = _read_json(
                ingestion_manifest_path, "Hospital ingestion manifest"
            )
            embedded = manifest_hospitals[hospital]
            if (
                hospital_manifest.get("hospital") != hospital
                or hospital_manifest.get("publication_ready") is not False
                or ingestion_manifest.get("artifact")
                != "asic_v3_lossless_hospital_ingestion"
                or ingestion_manifest.get("artifact_version") != "0.2"
                or ingestion_manifest.get("hospital", {}).get(
                    "canonical_hospital_id"
                )
                != hospital
            ):
                raise HarmonizationError(
                    f"Candidate or ingestion manifest is incompatible for {hospital}"
                )
            candidate_hash = sha256_file(candidate_path)
            hash_failure_count += int(
                candidate_hash
                != embedded.get("outputs", {}).get(table, {}).get("sha256")
            )
            hash_failure_count += int(
                candidate_hash
                != hospital_manifest.get("outputs", {}).get(table, {}).get("sha256")
            )
            ingested_hash = sha256_file(ingested_path)
            hash_failure_count += int(
                ingested_hash != embedded.get("inputs", {}).get(table, {}).get("sha256")
            )
            hash_failure_count += int(
                ingested_hash
                != ingestion_manifest.get("outputs", {}).get(table, {}).get("sha256")
            )
            candidate_parquet = pq.ParquetFile(candidate_path)
            ingested_parquet = pq.ParquetFile(ingested_path)
            expected_rows = int(embedded.get("outputs", {}).get(table, {}).get("row_count", -1))
            row_failure_count += int(candidate_parquet.metadata.num_rows != expected_rows)
            row_failure_count += int(ingested_parquet.metadata.num_rows != expected_rows)
            schema = candidate_parquet.schema_arrow
            expected_schema_hash = candidate_manifest.get("schemas", {}).get(table)
            schema_failure_count += int(_schema_digest(schema) != expected_schema_hash)
            if table not in common_schemas:
                common_schemas[table] = schema
            else:
                schema_failure_count += int(schema != common_schemas[table])
            scan, stays = _scan_candidate_table(
                candidate_path,
                ingested_path,
                hospital,
                table,
                suffixes[hospital],
                policy,
                quality,
                static_stays.get(hospital) if table == "dynamic" else None,
                progress,
            )
            if table == "static":
                static_stays[hospital] = stays
            numeric_profiles.extend(scan.numeric_profiles)
            categorical_rows.extend(scan.categorical_rows)
            list_profiles.extend(scan.list_profiles)
            invalid_rows.extend(scan.invalid_scale_rows)
            targeted_scale_rows.extend(scan.targeted_scale_rows)
            bucket_rows.extend(scan.bucket_rows)
            relationship_rows.extend(scan.relationship_rows)
            identifier_rows.append(scan.identifier_summary)
            if progress is not None:
                progress(f"consolidated_audit_table_complete hospital={hospital} table={table}")

    dictionary_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in dictionary_rows:
        dictionary_by_table[str(row.get("table"))].append(row)
    dictionary_failure_count = 0
    for table, schema in common_schemas.items():
        rows = sorted(
            dictionary_by_table[table], key=lambda item: int(item["ordered_position"])
        )
        expected_names = [field.name for field in _clinical_fields(schema)]
        dictionary_failure_count += int(
            [str(row.get("variable")) for row in rows] != expected_names
        )
        dictionary_failure_count += sum(
            str(row.get("candidate_arrow_type"))
            != _canonical_type_string(schema.field(name).type)
            for row, name in zip(rows, expected_names, strict=True)
        ) if len(rows) == len(expected_names) else 0
    invalid_rows.extend(
        _unavailable_invalid_rule_rows(
            quality["legacy_invalid_value_rules"],
            common_schemas,
            invalid_rows,
        )
    )
    expected_metric_rows = {
        (hospital, table, field.name)
        for hospital in hospitals
        for table, schema in common_schemas.items()
        for field in _clinical_fields(schema)
    }
    metric_keys = {
        (str(row.get("hospital")), str(row.get("table")), str(row.get("target")))
        for row in metric_rows
    }
    parser_failure_count = int(metric_keys != expected_metric_rows)
    rows_by_hospital_table = {
        (row["hospital"], row["table"]): int(row["row_count"])
        for row in identifier_rows
    }
    for row in metric_rows:
        parser_failure_count += int(
            int(row.get("input_cell_count", -1))
            != rows_by_hospital_table.get((row.get("hospital"), row.get("table")), -2)
        )
        try:
            status_counts = json.loads(str(row.get("status_counts_json")))
        except json.JSONDecodeError:
            parser_failure_count += 1
            continue
        parser_failure_count += int(
            sum(int(value) for value in status_counts.values())
            != int(row.get("input_cell_count", -1))
        )
    unresolved_count = sum(int(row.get("unresolved_count", 0)) for row in metric_rows)

    all_static: set[str] = set()
    cross_hospital_overlap_count = 0
    for hospital in hospitals:
        cross_hospital_overlap_count += len(all_static & static_stays[hospital])
        all_static.update(static_stays[hospital])
    identifier_failure_count = cross_hospital_overlap_count
    for row in identifier_rows:
        identifier_failure_count += int(row["missing_identifier_cell_count"])
        identifier_failure_count += int(row["identifier_formula_mismatch_count"])
        identifier_failure_count += int(row["hospital_id_mismatch_count"])
        identifier_failure_count += int(row["dynamic_identifier_not_in_static_count"])
        if row["table"] == "static":
            identifier_failure_count += int(row["row_count"]) - int(
                row["unique_stay_count"]
            )

    scale_signals = _scale_signals(numeric_profiles, policy)
    distribution_findings = _distribution_outliers(numeric_profiles, policy)
    unit_rows = _unit_review_rows(
        dictionary_rows,
        numeric_profiles,
        list_profiles,
        scale_signals,
        distribution_findings,
        targeted_scale_rows,
        quality,
    )
    audited_unit_keys = {
        (row["table"], row["variable"])
        for row in unit_rows
    }
    observed_numeric_keys = {
        (row["table"], row["variable"])
        for row in numeric_profiles
        if row.get("source") == "v3_candidate"
    } | {
        (row["table"], row["variable"])
        for row in list_profiles
        if "element_finite_count" in row
    }
    expected_unit_keys = set(observed_numeric_keys)
    expected_unit_keys.update(
        (str(row["table"]), str(row["variable"]))
        for row in dictionary_rows
        if str(row.get("candidate_unit")) == "unresolved"
        or (
            str(row.get("unit_review_status")) not in APPROVED_REVIEW_STATUSES
            and str(row.get("unit_review_status")) != "not_applicable"
        )
    )
    unit_coverage_failure_count = int(audited_unit_keys != expected_unit_keys)

    targeted_comparisons = _targeted_comparison_rows(
        quality, numeric_profiles, scale_signals
    )
    row_scale_entry_rows = _row_scale_entry_rows(quality, invalid_rows)
    context_rows = _context_audit_rows(quality, invalid_rows)
    static_sentinel_rows = _static_sentinel_rows(
        quality, numeric_profiles, categorical_rows
    )
    semantic_rows = _semantic_finding_rows(
        quality, numeric_profiles, targeted_scale_rows
    )
    availability_rows = _availability_rows(quality, numeric_profiles)
    pbw_row = _pbw_formula_audit(candidate_root, policy, quality)
    migrated_rule_expected = {
        "invalid_variable_count": len(_invalid_rules_by_column(quality["legacy_invalid_value_rules"])),
        "targeted_scale_count": len(quality["targeted_scale_audits"]),
        "row_scale_entry_count": len(quality["row_level_scale_entry_audits"]),
        "targeted_comparison_count": len(quality["targeted_comparison_audits"]),
        "targeted_bucket_count": len(quality["targeted_bucket_audits"]),
        "relationship_rule_count": len(quality["relationship_audits"]),
        "availability_rule_count": len(quality["availability_audits"]),
        "semantic_finding_count": len(quality["known_legacy_semantic_findings"]),
        "cross_table_formula_count": 1,
        "candidate_context_field_count": len(
            quality.get("candidate_context_audit", {}).get("fields", [])
        ),
        "static_sentinel_count": len(quality["legacy_static_minus_one_sentinels"]),
    }
    migrated_rule_observed = {
        "invalid_variable_count": len({row["variable"] for row in invalid_rows}),
        "targeted_scale_count": len({row.get("audit_id") for row in targeted_scale_rows}),
        "row_scale_entry_count": len(row_scale_entry_rows),
        "targeted_comparison_count": len(targeted_comparisons),
        "targeted_bucket_count": len({row.get("audit_id") for row in bucket_rows}),
        "relationship_rule_count": len({row.get("audit_id") for row in relationship_rows}),
        "availability_rule_count": len(availability_rows),
        "semantic_finding_count": len(semantic_rows),
        "cross_table_formula_count": 1 if pbw_row.get("status") else 0,
        "candidate_context_field_count": len(context_rows),
        "static_sentinel_count": len(static_sentinel_rows),
    }
    # Rules for absent candidate fields are still accounted explicitly by the
    # per-hospital unavailable rows; compare IDs rather than only profiled rows.
    migrated_rule_coverage_failure_count = sum(
        migrated_rule_observed[key] != value
        for key, value in migrated_rule_expected.items()
    )

    (
        comparison_status,
        old_version_summaries,
        cross_version_rows,
        cross_version_identity_rows,
    ) = _optional_old_version_evidence(
        comparison_status,
        comparison_paths,
        numeric_profiles,
        unit_rows,
        static_stays,
        identifier_rows,
        hospitals,
        policy,
        progress,
    )

    numeric_expected_profile_count = sum(
        len(_numeric_fields(schema)) * len(hospitals)
        for schema in common_schemas.values()
    )
    nonmissing_by_variable: Counter[tuple[str, str]] = Counter()
    for row in metric_rows:
        nonmissing_by_variable[(str(row["table"]), str(row["target"]))] += int(
            row.get("nonmissing_output_count", 0)
        )
    globally_all_missing = sorted(
        key
        for key in {
            (str(row["table"]), str(row["variable"])) for row in dictionary_rows
        }
        if nonmissing_by_variable[key] == 0
    )
    residual_reviewed_sentinel_count = sum(
        int(row["residual_sentinel_count_after_candidate_harmonization"])
        for row in static_sentinel_rows
    )
    technical_counts = {
        "candidate_evidence_and_frozen_policies_valid": 0,
        "candidate_and_ingested_hashes_valid": hash_failure_count,
        "row_and_provenance_conservation": row_failure_count
        + sum(not bool(row["provenance_matches"]) for row in identifier_rows),
        "common_ordered_schemas_valid": schema_failure_count,
        "dictionary_schema_coverage_complete": dictionary_failure_count,
        "parser_accounting_complete": parser_failure_count
        + residual_reviewed_sentinel_count,
        "identifier_contract_valid": identifier_failure_count,
        "all_numeric_variables_profiled": int(
            len(numeric_profiles) != numeric_expected_profile_count
        ),
        "all_unresolved_unit_policies_audited": unit_coverage_failure_count,
        "all_migrated_quality_rules_accounted": migrated_rule_coverage_failure_count,
        "optional_old_version_comparison_non_authoritative": 0,
        "audit_writes_reports_only": 0,
    }
    pending_mapping_count = int(
        candidate_review.get("metrics", {}).get("pending_mapping_variable_count", 0)
    )
    pending_type_count = int(
        candidate_review.get("metrics", {}).get("pending_type_variable_count", 0)
    )
    pending_parser_count = int(
        candidate_review.get("metrics", {}).get("pending_parser_variable_count", 0)
    )
    pending_categorical_count = int(
        candidate_review.get("metrics", {}).get("pending_categorical_variable_count", 0)
    )
    (
        relationship_decision_rows,
        invalid_decision_keys,
        active_row_scale_ids,
    ) = _decision_level_evidence(
        relationship_rows,
        invalid_rows,
        row_scale_entry_rows,
    )
    human_counts = {
        "candidate_tokens_resolved": unresolved_count,
        "unit_and_scale_policies_approved": sum(
            bool(row["blocks_harmonized_release"]) for row in unit_rows
        ),
        "categorical_domains_approved": pending_categorical_count
        + sum(bool(row["domain_truncated"]) for row in categorical_rows),
        "semantic_and_relationship_findings_approved": len(semantic_rows)
        + len(relationship_decision_rows),
        "legacy_invalid_ranges_reviewed_for_cleaning": len(invalid_decision_keys)
        + len(active_row_scale_ids),
        "cross_version_differences_reviewed": sum(
            row["finite_count_difference"] not in {None, 0}
            for row in cross_version_rows
        ),
        "ordered_schema_and_dictionary_approved": len(dictionary_rows)
        + pending_mapping_count
        + pending_type_count
        + pending_parser_count,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed={"finding_count": count},
            expected={"finding_count": 0},
            details=CHECK_DETAILS[name],
        )
        for name, count in {**technical_counts, **human_counts}.items()
    ]
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(item for item in blockers if item["check"] in TECHNICAL_CHECKS)
    decision_rows: list[dict[str, Any]] = []
    decision_rows.extend(
        {
            "decision_id": f"UNIT-{row['table']}-{row['variable']}",
            "domain": "unit_policy",
            "status": "requires_human_review" if row["blocks_harmonized_release"] else "evidence_supports_existing_candidate_unit",
            "table": row["table"],
            "variable": row["variable"],
            "hospital": None,
            "recommendation": row["recommendation"],
            "evidence_row": f"unit_review:{row['table']}:{row['variable']}",
            "automatic_approval": False,
        }
        for row in unit_rows
    )
    decision_rows.extend(
        {
            "decision_id": f"SCALE-{index:04d}",
            "domain": "hospital_scale_hypothesis",
            "status": "requires_human_review",
            "table": row["table"],
            "variable": row["variable"],
            "hospital": row["hospital"],
            "recommendation": row["action"],
            "evidence_row": f"scale_signals:{index}",
            "automatic_approval": False,
        }
        for index, row in enumerate(scale_signals, start=1)
    )
    pending_decisions = candidate_review.get("pending_decisions", {})
    if not isinstance(pending_decisions, dict):
        raise HarmonizationError("Candidate pending-decision register is malformed")
    for domain, variables in pending_decisions.items():
        if not isinstance(variables, list):
            raise HarmonizationError("Candidate pending-decision values are malformed")
        decision_rows.extend(
            {
                "decision_id": f"CANDIDATE-{domain}-{variable}",
                "domain": str(domain),
                "status": "requires_human_review",
                "table": None,
                "variable": str(variable),
                "hospital": None,
                "recommendation": "approve_or_revise_candidate_registry_policy",
                "evidence_row": "candidate_harmonization_dry_run_review",
                "automatic_approval": False,
            }
            for variable in variables
        )
    decision_sources = (
        (
            "TARGETED-SCALE",
            "hospital_targeted_scale_hypothesis",
            targeted_scale_rows,
            "resolve_from_source_metadata_and_complete_production_evidence",
        ),
        (
            "TARGETED-COMPARISON",
            "targeted_unit_or_definition_comparison",
            targeted_comparisons,
            "approve_unit_or_definition_policy_or_mark_analysis_ineligible",
        ),
        (
            "TARGETED-BUCKET",
            "targeted_distribution_bucket_review",
            bucket_rows,
            "review_complete_bucket_evidence_without_transforming_values",
        ),
        (
            "RELATIONSHIP",
            "semantic_relationship",
            relationship_decision_rows,
            "preserve_separate_variables_unless_equivalence_is_explicitly_approved",
        ),
        (
            "SEMANTIC",
            "known_legacy_semantic_finding",
            semantic_rows,
            "resolve_hospital_definition_or_mark_variable_analysis_ineligible",
        ),
        (
            "AVAILABILITY",
            "hospital_availability",
            availability_rows,
            "approve_or_revise_variable_availability_documentation",
        ),
        (
            "CONTEXT",
            "candidate_context_cleaning_evidence",
            context_rows,
            "review_temporal_context_before_any_cleaning_correction",
        ),
        (
            "STATIC-SENTINEL",
            "reviewed_missing_sentinel_conservation",
            static_sentinel_rows,
            "confirm_reviewed_sentinels_are_absent_after_harmonization",
        ),
    )
    for prefix, domain, rows, recommendation in decision_sources:
        for index, row in enumerate(rows, start=1):
            decision_rows.append(
                {
                    "decision_id": f"{prefix}-{index:04d}",
                    "domain": domain,
                    "status": "requires_human_review",
                    "table": row.get("table"),
                    "variable": row.get("variable"),
                    "hospital": row.get("hospital"),
                    "recommendation": recommendation,
                    "evidence_row": f"{domain}:{index}",
                    "automatic_approval": False,
                }
            )
    for index, (table, variable) in enumerate(sorted(invalid_decision_keys), start=1):
        decision_rows.append(
            {
                "decision_id": f"CLEANING-RANGE-{index:04d}",
                "domain": "physiologic_range_cleaning_candidate",
                "status": "requires_cleaning_policy_review",
                "table": table,
                "variable": variable,
                "hospital": None,
                "recommendation": "define_masking_or_uniquely_recoverable_row_rule_in_cleaned_layer",
                "evidence_row": "legacy_invalid_and_scale_entry:all_hospital_rows_for_variable",
                "automatic_approval": False,
            }
        )
    for index, row in enumerate(row_scale_entry_rows, start=1):
        decision_rows.append(
            {
                "decision_id": f"ROW-SCALE-{index:04d}",
                "domain": "row_level_scale_entry_cleaning_candidate",
                "status": "requires_cleaning_policy_review",
                "table": row.get("table"),
                "variable": row.get("variable"),
                "hospital": row.get("hospital"),
                "recommendation": "review_as_row_level_cleaning_rule_never_as_hospital_unit_conversion",
                "evidence_row": f"row_scale_entry_audits:{index}",
                "automatic_approval": False,
            }
        )
    for index, row in enumerate(cross_version_rows, start=1):
        if row.get("finite_count_difference") in {None, 0}:
            continue
        decision_rows.append(
            {
                "decision_id": f"CROSS-VERSION-{index:04d}",
                "domain": "optional_untrusted_old_version_comparison",
                "status": "requires_human_review",
                "table": row.get("table"),
                "variable": row.get("variable"),
                "hospital": row.get("hospital"),
                "recommendation": "confirm_expected_raw_recovery_or_hospital_coverage_difference",
                "evidence_row": f"cross_version_numeric:{index}",
                "automatic_approval": False,
            }
        )
    decision_rows.extend(
        {
            "decision_id": f"ALL-MISSING-{table}-{variable}",
            "domain": "global_all_missing_column",
            "status": "requires_explicit_preserve_or_retire_decision",
            "table": table,
            "variable": variable,
            "hospital": None,
            "recommendation": "preserve_for_union_compatibility_or_approve_evidence_bound_retirement_never_drop_silently",
            "evidence_row": "candidate_column_metrics",
            "automatic_approval": False,
        }
        for table, variable in globally_all_missing
    )
    decision_rows.extend(
        (
            {
                "decision_id": "UK00-PBW-FORMULA",
                "domain": "cross_table_semantic_hypothesis",
                "status": "requires_human_review",
                "table": "dynamic",
                "variable": "vt_per_ideal_bw_total",
                "hospital": "asic_UK00",
                "recommendation": "review_all_preserved_height_measurements_without_choosing_an_aggregation",
                "evidence_row": "pbw_formula_audit.json",
                "automatic_approval": False,
            },
            {
                "decision_id": "FREEZE-ORDERED-UNION-SCHEMAS",
                "domain": "schema_freeze",
                "status": "requires_human_approval",
                "table": None,
                "variable": None,
                "hospital": None,
                "recommendation": "approve_complete_ordered_static_and_dynamic_schemas",
                "evidence_row": "candidate_variable_dictionary",
                "automatic_approval": False,
            },
            {
                "decision_id": "FREEZE-VARIABLE-DICTIONARY",
                "domain": "dictionary_freeze",
                "status": "requires_human_approval",
                "table": None,
                "variable": None,
                "hospital": None,
                "recommendation": "approve_definitions_types_units_availability_caveats_and_provenance",
                "evidence_row": "candidate_variable_dictionary",
                "automatic_approval": False,
            },
        )
    )
    generated = utc_timestamp()
    unit_summary = {
        "unit_policy_variable_count": len(unit_rows),
        "numeric_or_numeric_list_variable_count": len(observed_numeric_keys),
        "unit_policy_rows_checked": sum(bool(row["unit_policy_audited"]) for row in unit_rows),
        "unresolved_candidate_unit_count": sum(row["candidate_unit"] == "unresolved" for row in unit_rows),
        "unit_rows_blocking_release": sum(bool(row["blocks_harmonized_release"]) for row in unit_rows),
        "cross_hospital_scale_signal_count": len(scale_signals),
        "automatic_unit_conversions_applied": 0,
    }
    review_payload = {
        "artifact": "asic_v3_consolidated_harmonization_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "candidate_run_id": candidate_run_id,
        "overall_status": overall_status(checks),
        "optional_old_version_comparison": {
            "status": comparison_status,
            "authoritative_for_v3": False,
            "required_for_v3": False,
        },
        "metrics": {
            "hospital_count": len(hospitals),
            "audited_table_count": len(identifier_rows),
            "candidate_row_count": sum(int(row["row_count"]) for row in identifier_rows),
            "candidate_variable_count": len(dictionary_rows),
            "numeric_profile_count": len(numeric_profiles),
            "numeric_list_profile_count": sum("element_finite_count" in row for row in list_profiles),
            "categorical_domain_row_count": len(categorical_rows),
            "unresolved_candidate_cell_count": unresolved_count,
            "legacy_invalid_range_profile_count": len(invalid_rows),
            "migrated_invalid_rule_unavailable_count": sum(
                row.get("status")
                == "candidate_variable_unavailable_in_v3_union_schema"
                for row in invalid_rows
            ),
            "targeted_scale_evidence_row_count": len(targeted_scale_rows),
            "row_scale_entry_evidence_row_count": len(row_scale_entry_rows),
            "candidate_context_evidence_row_count": len(context_rows),
            "static_sentinel_evidence_row_count": len(static_sentinel_rows),
            "globally_all_missing_variable_count": len(globally_all_missing),
            "relationship_evidence_row_count": len(relationship_rows),
            "cross_version_profile_count": len(cross_version_rows),
            "decision_register_row_count": len(decision_rows),
            "technical_blocking_finding_count": len(technical),
            "production_data_artifacts_generated": False,
        },
        "unit_policy_summary": unit_summary,
        "migrated_quality_policy_coverage": {
            "expected": migrated_rule_expected,
            "observed": migrated_rule_observed,
        },
        "priority_review_groups": [
            {
                "group": "unit_and_scale",
                "count": human_counts["unit_and_scale_policies_approved"],
                "next_action": "approve, revise, or mark analysis-ineligible each unit row",
            },
            {
                "group": "semantic_and_relationship",
                "count": human_counts["semantic_and_relationship_findings_approved"],
                "next_action": "resolve hospital definitions and relationship findings",
            },
            {
                "group": "cleaning_candidates",
                "count": human_counts["legacy_invalid_ranges_reviewed_for_cleaning"],
                "next_action": "review separately for the cleaned-layer policy",
            },
            {
                "group": "optional_old_version_differences",
                "count": human_counts["cross_version_differences_reviewed"],
                "next_action": "if comparison evidence is available, confirm expected raw-recovery and hospital-coverage differences",
            },
        ],
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only consolidated audit bundle on the authorized cluster",
        },
        "publication": {
            "candidate_mutated": False,
            "harmonized_release_created": False,
            "cleaned_artifacts_generated": False,
            "derived_artifacts_generated": False,
            "publication_ready": False,
        },
        "limitations": [
            "Distribution and power-of-ten alignment are screening evidence, not proof of a physical unit or permission to transform values.",
            "Legacy invalid-value and row-scale findings are cleaning candidates only and are not applied to the harmonization candidate.",
            "A migrated invalid-range rule whose variable is absent from both v3 union schemas is retained as explicit unavailable evidence; no derived value is manufactured during harmonization.",
            "Older-version artifacts are optional, untrusted read-only context. Their absence cannot block this v3 audit, and pooled artifacts cannot reconstruct source strings lost upstream.",
            "Quantiles use deterministic bounded samples; counts, minima, maxima, means, conservation, identifiers, and rule accounting are complete streaming results.",
            "The UK00 PBW hypothesis checks every plausible preserved height measurement and does not choose a patient-level height aggregation.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    private_tables = {
        "numeric_profiles.parquet": numeric_profiles,
        "list_profiles.parquet": list_profiles,
        "categorical_domains.parquet": categorical_rows,
        "unit_review.parquet": unit_rows,
        "scale_signals.parquet": scale_signals,
        "distribution_findings.parquet": distribution_findings,
        "legacy_invalid_and_scale_entry.parquet": invalid_rows,
        "targeted_scale_audits.parquet": targeted_scale_rows,
        "row_scale_entry_audits.parquet": row_scale_entry_rows,
        "candidate_context_audits.parquet": context_rows,
        "static_missing_sentinel_audits.parquet": static_sentinel_rows,
        "targeted_comparisons.parquet": targeted_comparisons,
        "targeted_buckets.parquet": bucket_rows,
        "relationships.parquet": relationship_rows,
        "availability_audits.parquet": availability_rows,
        "semantic_findings.parquet": semantic_rows,
        "cross_version_numeric.parquet": cross_version_rows,
        "cross_version_identifiers.parquet": cross_version_identity_rows,
        "old_version_reference_summaries.parquet": old_version_summaries,
        "identifier_integrity.parquet": identifier_rows,
        "decision_register.parquet": decision_rows,
        "candidate_variable_dictionary.parquet": dictionary_rows,
    }
    for filename, rows in private_tables.items():
        _private_parquet(private_dir / filename, rows)
    _write_json(private_dir / "pbw_formula_audit.json", pbw_row, 0o600)
    manifest_payload = {
        "artifact": "asic_v3_consolidated_harmonization_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "candidate_run_id": candidate_run_id,
        "candidate_manifest_sha256": sha256_file(candidate_manifest_path),
        "audit_policy_sha256": sha256_file(policy.source_path),
        "v3_cross_hospital_quality_policy_sha256": sha256_file(
            policy.quality_policy_path
        ),
        "optional_old_version_comparison": {
            "status": comparison_status,
            "authoritative_for_v3": False,
            "required_for_v3": False,
        },
        "table_row_counts": {name: len(rows) for name, rows in private_tables.items()},
        "table_sha256": {
            name: sha256_file(private_dir / name) for name in private_tables
        },
        "pbw_formula_audit_sha256": sha256_file(
            private_dir / "pbw_formula_audit.json"
        ),
        "pbw_formula_audit": pbw_row,
        "checks": [asdict(item) for item in checks],
        "publication_ready": False,
    }
    _write_json(private_dir / "consolidated_audit_manifest.json", manifest_payload, 0o600)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return ConsolidatedAuditResult(
        run_id=selected_run,
        candidate_run_id=candidate_run_id,
        overall_status=review_payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
