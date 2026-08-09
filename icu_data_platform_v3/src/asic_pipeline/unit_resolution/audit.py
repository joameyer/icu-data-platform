from __future__ import annotations

from collections import defaultdict
import csv
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.derivation.review import (
    CleanedReleaseInput,
    DerivationContractReviewConfig,
    load_cleaned_release_input,
    load_derivation_contract_review_config,
    load_derivation_contract_review_policy,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.consolidated_audit import (
    InvalidScaleState,
    NumericAccumulator,
    _bool_count,
    _invalid_rules_by_column,
    _quantile,
    _sample_positions,
    _stable_seed,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)

EXPECTED_BOUNDARY = {
    "read_released_cleaned_data": True,
    "read_existing_private_mapping_and_audit_evidence": True,
    "modify_harmonized_release": False,
    "modify_cleaned_release": False,
    "modify_derived_release": False,
    "write_clinical_data": False,
    "write_reports_only": True,
    "infer_units_automatically": False,
    "apply_unit_conversions": False,
    "apply_legacy_ranges": False,
    "mask_values": False,
    "filter_rows_or_stays": False,
    "authorize_external_data_export": False,
}

EXPECTED_DECISION_FRAMEWORK = {
    "distribution_evidence_can_establish_unit": False,
    "raw_header_can_establish_unit_without_source_documentation": False,
    "preserve_values_until_unit_is_resolved": True,
    "automatic_unit_conversion": False,
    "automatic_legacy_range_activation": False,
    "permitted_unit_dispositions": [
        "confirm_same_unit_all_hospitals",
        "approve_hospital_specific_conversion",
        "confirm_dimensionless_or_score_representation",
        "retain_unresolved_and_analysis_ineligible",
        "classify_mixed_or_unrecoverable",
    ],
    "permitted_range_dispositions": [
        "activate_legacy_range_after_unit_confirmation",
        "replace_with_reviewed_unit_specific_range",
        "preserve_audit_only",
        "mask_hospital_specific_unrecoverable_values",
        "retire_range_rule_as_inapplicable",
    ],
}


@dataclass(frozen=True)
class UnitResolutionAuditPolicy:
    version: str
    cleaned_release_id: str
    expected_dynamic_rows: int
    harmonized_contract_version: str
    frozen_contract_directory: Path
    frozen_schema_policy_path: Path
    frozen_schema_policy_sha256: str
    quality_policy_path: Path
    quality_policy_sha256: str
    dry_run_id: str
    consolidated_audit_run_id: str
    expected_unresolved_count: int
    expected_priority_count: int
    expected_priority_outside_counts: tuple[tuple[str, int], ...]
    rows_per_batch: int
    sample_capacity: int
    sample_rows_per_batch: int
    minimum_distribution_count: int
    minimum_distribution_hospitals: int
    iqr_fence_factor: float
    scale_factors: tuple[float, ...]
    peer_minimum_improvement_factor: float
    peer_maximum_residual_factor: float
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class UnitResolutionAuditConfig:
    derivation: DerivationContractReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.derivation.dataset_context

    @property
    def data_root(self) -> Path:
        return self.derivation.data_root

    @property
    def reports_root(self) -> Path:
        return self.derivation.reports_root


@dataclass(frozen=True)
class UnitContractEvidence:
    dictionary_path: Path
    dictionary_rows: tuple[dict[str, Any], ...]
    unresolved_rows: tuple[dict[str, Any], ...]
    source_mapping_path: Path
    source_mapping_rows: tuple[dict[str, Any], ...]
    prior_unit_review_path: Path
    prior_unit_rows: tuple[dict[str, Any], ...]
    prior_numeric_profiles_path: Path
    prior_numeric_profiles: tuple[dict[str, Any], ...]
    quality_rules: dict[str, Any]
    frozen_manifest_path: Path
    dry_run_manifest_path: Path
    consolidated_manifest_path: Path


@dataclass(frozen=True)
class UnitResolutionAuditResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _nonnegative_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a non-negative integer")
    return value


def _positive_number(value: Any, location: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive number")
    result = float(value)
    if not math.isfinite(result):
        raise ConfigurationError(f"{location} must be finite")
    return result


def _contained_relative_path(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def _number_tuple(value: Any, location: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError(f"{location} must be a non-empty list")
    values = tuple(_positive_number(item, location) for item in value)
    if len(values) != len(set(values)):
        raise ConfigurationError(f"{location} contains duplicate values")
    return values


def load_unit_resolution_audit_config(
    path: str | Path,
) -> UnitResolutionAuditConfig:
    source = Path(path).expanduser().resolve()
    derivation = load_derivation_contract_review_config(source)
    raw = load_yaml_mapping(source, "Unit-resolution audit configuration")
    policy_path = resolve_path(
        required_string(raw, "unit_resolution_audit_policy", "config"), source
    )
    return UnitResolutionAuditConfig(derivation=derivation, policy_path=policy_path)


def load_unit_resolution_audit_policy(
    path: str | Path,
) -> UnitResolutionAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Unit-resolution audit policy")
    if raw.get("unit_resolution_audit_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_consolidated_read_only_unit_and_range_review":
        raise ConfigurationError("Unit-resolution audit policy is invalid")
    inputs = _mapping(raw.get("input"), "input")
    scope = _mapping(raw.get("scope"), "scope")
    scan = _mapping(raw.get("scan"), "scan")
    reporting = _mapping(raw.get("reporting"), "reporting")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Unit-resolution audit boundary changed")
    if (
        _mapping(raw.get("decision_framework"), "decision_framework")
        != EXPECTED_DECISION_FRAMEWORK
    ):
        raise ConfigurationError("Unit-resolution decision framework changed")
    expected_scope_options = {
        "require_all_unresolved_variables_to_be_dynamic_float64": True,
        "require_source_mapping_evidence_for_every_variable": True,
        "require_prior_candidate_profile_for_every_hospital_variable": True,
        "compare_current_finite_counts_with_preclean_candidate": True,
    }
    if {key: scope.get(key) for key in expected_scope_options} != expected_scope_options:
        raise ConfigurationError("Unit-resolution evidence scope changed")
    counts_raw = _mapping(
        scope.get("priority_expected_outside_legacy_range_counts"),
        "scope.priority_expected_outside_legacy_range_counts",
    )
    expected_counts = tuple(
        sorted(
            (str(variable), _nonnegative_int(value, f"priority.{variable}"))
            for variable, value in counts_raw.items()
        )
    )
    if sum(value for _, value in expected_counts) != 10_021:
        raise ConfigurationError("Priority legacy-range accounting changed")
    reporting_modes = {
        "private_directory_mode": "0700",
        "private_file_mode": "0600",
        "review_directory_mode": "0750",
        "review_file_mode": "0640",
    }
    if {key: reporting.get(key) for key in reporting_modes} != reporting_modes:
        raise ConfigurationError("Unit-resolution report permissions changed")
    frozen_policy_path = resolve_path(
        required_string(inputs, "frozen_schema_policy", "input"), source
    )
    quality_policy_path = resolve_path(
        required_string(inputs, "quality_policy", "input"), source
    )
    frozen_hash = required_string(inputs, "frozen_schema_policy_sha256", "input")
    quality_hash = required_string(inputs, "quality_policy_sha256", "input")
    for evidence_path, expected_hash in (
        (frozen_policy_path, frozen_hash),
        (quality_policy_path, quality_hash),
    ):
        if not evidence_path.is_file() or sha256_file(evidence_path) != expected_hash:
            raise ConfigurationError(
                f"Immutable unit-resolution policy changed: {evidence_path.name}"
            )
    expected_unresolved = _positive_int(
        scope.get("expected_unresolved_unit_variable_count"),
        "scope.expected_unresolved_unit_variable_count",
    )
    expected_priority = _positive_int(
        scope.get("expected_priority_legacy_range_variable_count"),
        "scope.expected_priority_legacy_range_variable_count",
    )
    if expected_unresolved != 72 or expected_priority != 12:
        raise ConfigurationError("Approved unresolved-unit scope changed")
    if len(expected_counts) != expected_priority:
        raise ConfigurationError("Priority legacy-range variable list changed")
    return UnitResolutionAuditPolicy(
        version="0.1",
        cleaned_release_id=required_string(inputs, "cleaned_release_id", "input"),
        expected_dynamic_rows=_positive_int(
            inputs.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        harmonized_contract_version=required_string(
            inputs, "harmonized_contract_version", "input"
        ),
        frozen_contract_directory=_contained_relative_path(
            required_string(inputs, "frozen_contract_directory", "input"),
            "input.frozen_contract_directory",
        ),
        frozen_schema_policy_path=frozen_policy_path,
        frozen_schema_policy_sha256=frozen_hash,
        quality_policy_path=quality_policy_path,
        quality_policy_sha256=quality_hash,
        dry_run_id=required_string(inputs, "harmonization_dry_run_id", "input"),
        consolidated_audit_run_id=required_string(
            inputs, "consolidated_harmonization_audit_run_id", "input"
        ),
        expected_unresolved_count=expected_unresolved,
        expected_priority_count=expected_priority,
        expected_priority_outside_counts=expected_counts,
        rows_per_batch=_positive_int(scan.get("rows_per_batch"), "scan.rows_per_batch"),
        sample_capacity=_positive_int(
            scan.get("numeric_sample_capacity_per_hospital_variable"),
            "scan.numeric_sample_capacity_per_hospital_variable",
        ),
        sample_rows_per_batch=_positive_int(
            scan.get("sample_rows_per_batch"), "scan.sample_rows_per_batch"
        ),
        minimum_distribution_count=_positive_int(
            scan.get("minimum_nonmissing_for_cross_hospital_comparison"),
            "scan.minimum_nonmissing_for_cross_hospital_comparison",
        ),
        minimum_distribution_hospitals=_positive_int(
            scan.get("minimum_hospitals_for_cross_hospital_comparison"),
            "scan.minimum_hospitals_for_cross_hospital_comparison",
        ),
        iqr_fence_factor=_positive_number(
            scan.get("hospital_metric_iqr_fence_factor"),
            "scan.hospital_metric_iqr_fence_factor",
        ),
        scale_factors=_number_tuple(
            scan.get("scale_screen_factors"), "scan.scale_screen_factors"
        ),
        peer_minimum_improvement_factor=_positive_number(
            scan.get("peer_alignment_minimum_improvement_factor"),
            "scan.peer_alignment_minimum_improvement_factor",
        ),
        peer_maximum_residual_factor=_positive_number(
            scan.get("peer_alignment_maximum_residual_factor"),
            "scan.peer_alignment_maximum_residual_factor",
        ),
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
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


def _read_private_rows(path: Path, label: str) -> tuple[dict[str, Any], ...]:
    if not path.is_file():
        raise HarmonizationError(f"{label} is unavailable")
    table = pq.read_table(path)
    if table.column_names == ["empty"]:
        return ()
    return tuple(table.to_pylist())


def _load_contract_evidence(
    config: UnitResolutionAuditConfig,
    policy: UnitResolutionAuditPolicy,
) -> UnitContractEvidence:
    frozen_policy = load_schema_dictionary_freeze_policy(
        policy.frozen_schema_policy_path
    )
    if frozen_policy.contract_version != policy.harmonized_contract_version:
        raise HarmonizationError("Frozen dictionary contract version changed")
    contract_dir = config.data_root / policy.frozen_contract_directory
    frozen_manifest_path = contract_dir / "freeze_manifest.json"
    dictionary_path = contract_dir / "variable_dictionary.parquet"
    frozen_manifest = _read_json(frozen_manifest_path, "Frozen dictionary manifest")
    if (
        frozen_manifest.get("artifact")
        != "asic_v3_frozen_harmonized_schema_dictionary"
        or frozen_manifest.get("contract_version") != policy.harmonized_contract_version
        or frozen_manifest.get("schema_frozen") is not True
        or frozen_manifest.get("dictionary_frozen") is not True
        or frozen_manifest.get("files", {}).get(dictionary_path.name)
        != sha256_file(dictionary_path)
    ):
        raise HarmonizationError("Frozen variable dictionary changed")
    dictionary_rows = _read_private_rows(dictionary_path, "Frozen variable dictionary")
    unresolved = tuple(
        row for row in dictionary_rows if str(row.get("unit")) == "unresolved"
    )
    if len(unresolved) != policy.expected_unresolved_count:
        raise HarmonizationError("Frozen unresolved-unit variable count changed")
    for row in unresolved:
        if (
            row.get("table") != "dynamic"
            or row.get("physical_type") != "double"
            or row.get("analysis_eligibility") != "ineligible"
        ):
            raise HarmonizationError(
                "An unresolved unit no longer has the approved dynamic float64 boundary"
            )

    dry_private = (
        config.reports_root
        / "private"
        / "harmonization_dry_run"
        / policy.dry_run_id
    )
    dry_run_manifest_path = dry_private / "harmonization_dry_run_manifest.json"
    dry_manifest = _read_json(dry_run_manifest_path, "Harmonization dry-run manifest")
    source_mapping_path = dry_private / "occurrence_plan.parquet"
    source_mappings = _read_private_rows(
        source_mapping_path, "Harmonization occurrence plan"
    )
    if (
        dry_manifest.get("artifact") != "asic_v3_harmonization_dry_run_private"
        or dry_manifest.get("run_id") != policy.dry_run_id
        or not source_mappings
    ):
        raise HarmonizationError("Harmonization source-mapping evidence changed")
    unresolved_names = {str(row["variable"]) for row in unresolved}
    filtered_mappings = tuple(
        row
        for row in source_mappings
        if row.get("table") == "dynamic" and row.get("target") in unresolved_names
    )
    if {str(row.get("target")) for row in filtered_mappings} != unresolved_names:
        raise HarmonizationError(
            "Source-mapping evidence does not cover every unresolved-unit variable"
        )

    consolidated_private = (
        config.reports_root
        / "private"
        / "consolidated_harmonization_audit"
        / policy.consolidated_audit_run_id
    )
    consolidated_manifest_path = consolidated_private / "consolidated_audit_manifest.json"
    consolidated_manifest = _read_json(
        consolidated_manifest_path, "Consolidated harmonization manifest"
    )
    required_tables = {
        "unit_review.parquet": consolidated_private / "unit_review.parquet",
        "numeric_profiles.parquet": consolidated_private / "numeric_profiles.parquet",
    }
    hashes = consolidated_manifest.get("table_sha256")
    if (
        consolidated_manifest.get("artifact")
        != "asic_v3_consolidated_harmonization_audit_private"
        or consolidated_manifest.get("run_id") != policy.consolidated_audit_run_id
        or not isinstance(hashes, dict)
    ):
        raise HarmonizationError("Prior consolidated unit evidence changed")
    for name, path in required_tables.items():
        if hashes.get(name) != sha256_file(path):
            raise HarmonizationError(f"Prior unit evidence changed: {name}")
    prior_unit_review_path = required_tables["unit_review.parquet"]
    prior_numeric_profiles_path = required_tables["numeric_profiles.parquet"]
    prior_units = tuple(
        row
        for row in _read_private_rows(prior_unit_review_path, "Prior unit review")
        if row.get("table") == "dynamic" and row.get("variable") in unresolved_names
    )
    prior_profiles = tuple(
        row
        for row in _read_private_rows(
            prior_numeric_profiles_path, "Prior numeric profiles"
        )
        if row.get("table") == "dynamic"
        and row.get("variable") in unresolved_names
        and row.get("source") == "v3_candidate"
    )
    if {str(row.get("variable")) for row in prior_units} != unresolved_names:
        raise HarmonizationError("Prior unit review does not cover the frozen scope")
    expected_profile_keys = {
        (hospital, variable)
        for hospital in HOSPITALS
        for variable in unresolved_names
    }
    observed_profile_keys = {
        (str(row.get("hospital")), str(row.get("variable")))
        for row in prior_profiles
    }
    if observed_profile_keys != expected_profile_keys:
        raise HarmonizationError(
            "Prior hospital-variable profiles do not cover the frozen scope"
        )

    quality = load_yaml_mapping(policy.quality_policy_path, "V3 quality policy")
    if (
        quality.get("cross_hospital_quality_policy_version") != "0.1"
        or quality.get("status")
        != "approved_for_v3_read_only_cross_hospital_audit"
    ):
        raise HarmonizationError("V3 unit and range quality policy changed")
    return UnitContractEvidence(
        dictionary_path=dictionary_path,
        dictionary_rows=dictionary_rows,
        unresolved_rows=unresolved,
        source_mapping_path=source_mapping_path,
        source_mapping_rows=filtered_mappings,
        prior_unit_review_path=prior_unit_review_path,
        prior_unit_rows=prior_units,
        prior_numeric_profiles_path=prior_numeric_profiles_path,
        prior_numeric_profiles=prior_profiles,
        quality_rules=quality,
        frozen_manifest_path=frozen_manifest_path,
        dry_run_manifest_path=dry_run_manifest_path,
        consolidated_manifest_path=consolidated_manifest_path,
    )


def _finite_number(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _median(values: Iterable[float]) -> float | None:
    return _quantile(values, 0.5)


def _scale_signals(
    profiles: list[dict[str, Any]], policy: UnitResolutionAuditPolicy
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in profiles:
        grouped[str(row["variable"])].append(row)
    signals: list[dict[str, Any]] = []
    for variable, rows in sorted(grouped.items()):
        eligible = [
            row
            for row in rows
            if int(row["finite_count"]) >= policy.minimum_distribution_count
            and _finite_number(row.get("median")) not in {None, 0.0}
        ]
        if len(eligible) < policy.minimum_distribution_hospitals:
            continue
        for row in eligible:
            target = abs(float(row["median"]))
            peers = [
                abs(float(peer["median"]))
                for peer in eligible
                if peer["hospital"] != row["hospital"] and float(peer["median"]) != 0
            ]
            peer_median = _median(peers)
            if not target or peer_median in {None, 0.0}:
                continue
            assert peer_median is not None
            baseline = max(target / peer_median, peer_median / target)
            candidates = [
                (
                    max(
                        target * factor / peer_median,
                        peer_median / (target * factor),
                    ),
                    factor,
                )
                for factor in policy.scale_factors
            ]
            residual, factor = min(candidates)
            if (
                baseline >= policy.peer_minimum_improvement_factor
                and residual <= policy.peer_maximum_residual_factor
            ):
                signals.append(
                    {
                        "table": "dynamic",
                        "variable": variable,
                        "hospital": row["hospital"],
                        "target_median": row["median"],
                        "peer_median_of_hospital_medians": peer_median,
                        "baseline_scale_ratio": baseline,
                        "candidate_multiplier": factor,
                        "post_multiplier_residual_ratio": residual,
                        "classification": "screening_hypothesis_not_unit_proof",
                        "automatic_conversion_approved": False,
                    }
                )
    return signals


def _distribution_findings(
    profiles: list[dict[str, Any]], policy: UnitResolutionAuditPolicy
) -> list[dict[str, Any]]:
    metrics = ("min", "median", "iqr", "max", "finite_rate")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in profiles:
        grouped[str(row["variable"])].append(row)
    findings: list[dict[str, Any]] = []
    for variable, rows in sorted(grouped.items()):
        eligible = [
            row
            for row in rows
            if int(row["finite_count"]) >= policy.minimum_distribution_count
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
            assert q1 is not None and q3 is not None
            spread = q3 - q1
            lower = q1 - policy.iqr_fence_factor * spread
            upper = q3 + policy.iqr_fence_factor * spread
            for row in eligible:
                value = _finite_number(row.get(metric))
                if value is not None and (value < lower or value > upper):
                    findings.append(
                        {
                            "table": "dynamic",
                            "variable": variable,
                            "hospital": row["hospital"],
                            "metric": metric,
                            "value": value,
                            "lower_iqr_fence": lower,
                            "upper_iqr_fence": upper,
                            "classification": "screening_flag_not_unit_proof",
                        }
                    )
    return findings


def _review_group(variable: str) -> str:
    if variable.startswith("sofa_") or variable.startswith("isofa_"):
        return "severity_scores"
    if (
        variable.endswith("_iv_cont")
        or variable.endswith("_iv_bolus")
        or variable.endswith("_po_bolus")
        or variable.endswith("_inh")
        or variable.startswith("inhaled_")
    ):
        return "medication_or_therapy_exposure"
    if variable in {"evlwi", "gedvi", "pvri", "svri"}:
        return "advanced_hemodynamics"
    if variable in {"base_excess_art", "bicarbonate_art", "lactate_art"}:
        return "arterial_blood_gas"
    return "laboratory_or_biomarker"


def _scan_cleaned_dynamic(
    source: CleanedReleaseInput,
    policy: UnitResolutionAuditPolicy,
    unresolved_rows: tuple[dict[str, Any], ...],
    priority_rules: dict[str, dict[str, Any]],
    progress: Callable[[str], None] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    variables = tuple(sorted(str(row["variable"]) for row in unresolved_rows))
    schema = source.schemas["dynamic"]
    if "hospital_id" not in schema.names or any(name not in schema.names for name in variables):
        raise HarmonizationError("Cleaned dynamic schema lacks unit-review fields")
    for variable in variables:
        if not pa.types.is_float64(schema.field(variable).type):
            raise HarmonizationError("An unresolved-unit field is not float64")
    accumulators = {
        (hospital, variable): NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_resolution", hospital, variable),
        )
        for hospital in HOSPITALS
        for variable in variables
    }
    range_states = {
        (hospital, variable): InvalidScaleState(
            rule=priority_rules[variable], factors=policy.scale_factors
        )
        for hospital in HOSPITALS
        for variable in priority_rules
    }
    columns = ("hospital_id", *variables)
    rows = 0
    missing_hospital_count = 0
    unknown_hospital_count = 0
    hospital_rows = {hospital: 0 for hospital in HOSPITALS}
    next_progress = 1_000_000
    parquet = pq.ParquetFile(source.source_files["dynamic"])
    for batch in parquet.iter_batches(batch_size=policy.rows_per_batch, columns=columns):
        hospital_values = batch.column(0)
        missing_hospital_count += len(hospital_values) - int(
            pc.sum(pc.cast(pc.is_valid(hospital_values), pa.int64())).as_py() or 0
        )
        observed_hospitals = [
            str(value)
            for value in pc.unique(hospital_values).to_pylist()
            if value is not None
        ]
        for hospital in observed_hospitals:
            if hospital not in hospital_rows:
                unknown_hospital_count += _bool_count(pc.equal(hospital_values, hospital))
                continue
            mask = pc.fill_null(pc.equal(hospital_values, hospital), False)
            count = _bool_count(mask)
            hospital_rows[hospital] += count
            for column_index, variable in enumerate(variables, start=1):
                values = pc.filter(batch.column(column_index), mask)
                accumulator = accumulators[(hospital, variable)]
                positions = _sample_positions(
                    len(values), policy.sample_rows_per_batch, accumulator.row_count
                )
                accumulator.add(values, positions)
                state = range_states.get((hospital, variable))
                if state is not None:
                    state.add(values)
        rows += batch.num_rows
        if progress is not None and rows >= next_progress:
            progress(f"unit_resolution_audit_progress dynamic_rows={rows}")
            next_progress = ((rows // 1_000_000) + 1) * 1_000_000
    if (
        rows != policy.expected_dynamic_rows
        or missing_hospital_count
        or unknown_hospital_count
        or set(hospital for hospital, count in hospital_rows.items() if count) != set(HOSPITALS)
    ):
        raise HarmonizationError("Cleaned dynamic row or hospital accounting changed")
    profiles: list[dict[str, Any]] = []
    for hospital in HOSPITALS:
        for variable in variables:
            row = accumulators[(hospital, variable)].profile(
                "dynamic", hospital, variable, "cleaned_release"
            )
            profiles.append(row)
    ranges = [
        range_states[(hospital, variable)].result("dynamic", hospital, variable)
        for variable in sorted(priority_rules)
        for hospital in HOSPITALS
    ]
    return profiles, ranges, hospital_rows


def _aggregate_priority_ranges(
    range_rows: list[dict[str, Any]],
    priority_rules: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variable, rule in sorted(priority_rules.items()):
        matching = [row for row in range_rows if row["variable"] == variable]
        factor_counts: dict[str, int] = defaultdict(int)
        for row in matching:
            values = row.get("recoverable_by_factor") or {}
            for factor, count in values.items():
                factor_counts[str(factor)] += int(count)
        finite_count = sum(int(row["finite_count"]) for row in matching)
        invalid_count = sum(int(row["invalid_count"]) for row in matching)
        rows.append(
            {
                "table": "dynamic",
                "variable": variable,
                "legacy_rule_name": rule.get("legacy_name"),
                "legacy_hard_min": rule.get("hard_min"),
                "legacy_hard_max": rule.get("hard_max"),
                "legacy_invalid_zero": bool(rule.get("invalid_zero")),
                "legacy_description": rule.get("description"),
                "finite_count": finite_count,
                "outside_legacy_range_count": invalid_count,
                "outside_legacy_range_fraction": (
                    invalid_count / finite_count if finite_count else None
                ),
                "recoverable_by_factor": dict(sorted(factor_counts.items())),
                "uniquely_recoverable_count": sum(
                    int(row["uniquely_recoverable_count"]) for row in matching
                ),
                "multiply_recoverable_count": sum(
                    int(row["multiply_recoverable_count"]) for row in matching
                ),
                "all_values_missing": finite_count == 0,
                "current_action": "preserve_audit_only_until_unit_and_range_review",
                "unit_confirmed": False,
                "legacy_range_activated": False,
                "values_modified_by_this_audit": False,
            }
        )
    return rows


def _build_decision_workbook(
    evidence: UnitContractEvidence,
    profiles: list[dict[str, Any]],
    range_summaries: list[dict[str, Any]],
    scale_signals: list[dict[str, Any]],
    distribution_findings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    mappings: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in evidence.source_mapping_rows:
        variable = str(row.get("target"))
        hospital = str(row.get("hospital"))
        raw_name = row.get("raw_name")
        if isinstance(raw_name, str):
            mappings[variable][hospital].add(raw_name)
    prior_units = {str(row["variable"]): row for row in evidence.prior_unit_rows}
    priority = {str(row["variable"]): row for row in range_summaries}
    rows: list[dict[str, Any]] = []
    for dictionary in sorted(evidence.unresolved_rows, key=lambda row: str(row["variable"])):
        variable = str(dictionary["variable"])
        matching = [row for row in profiles if row["variable"] == variable]
        finite = sum(int(row["finite_count"]) for row in matching)
        nulls = sum(int(row["null_count"]) for row in matching)
        medians = {
            str(row["hospital"]): row.get("median")
            for row in matching
            if row.get("median") is not None
        }
        minimums = [float(row["min"]) for row in matching if row.get("min") is not None]
        maximums = [float(row["max"]) for row in matching if row.get("max") is not None]
        priority_row = priority.get(variable)
        rows.append(
            {
                "table": "dynamic",
                "variable": variable,
                "review_group": _review_group(variable),
                "current_physical_type": dictionary.get("physical_type"),
                "current_unit": dictionary.get("unit"),
                "current_analysis_eligibility": dictionary.get("analysis_eligibility"),
                "current_analysis_caveat": dictionary.get("analysis_caveat"),
                "definition": dictionary.get("definition"),
                "definition_status": dictionary.get("definition_status"),
                "source_strategy": dictionary.get("source_strategy"),
                "source_raw_names_by_hospital": {
                    hospital: sorted(names)
                    for hospital, names in sorted(mappings[variable].items())
                },
                "source_mapping_occurrence_count": sum(
                    row.get("table") == "dynamic"
                    and row.get("target") == variable
                    for row in evidence.source_mapping_rows
                ),
                "hospitals_with_finite_values": sum(
                    int(row["finite_count"]) > 0 for row in matching
                ),
                "finite_value_count": finite,
                "null_value_count": nulls,
                "all_values_missing": finite == 0,
                "global_minimum": min(minimums) if minimums else None,
                "hospital_medians": medians,
                "global_maximum": max(maximums) if maximums else None,
                "cross_hospital_scale_signal_count": sum(
                    row["variable"] == variable for row in scale_signals
                ),
                "distribution_screen_finding_count": sum(
                    row["variable"] == variable for row in distribution_findings
                ),
                "prior_recommendation": prior_units[variable].get("recommendation"),
                "prior_candidate_unit": prior_units[variable].get("candidate_unit"),
                "prior_unit_review_status": prior_units[variable].get(
                    "unit_review_status"
                ),
                "prior_source_hospital_count": prior_units[variable].get(
                    "source_hospital_count"
                ),
                "prior_legacy_or_targeted_review_ids": prior_units[variable].get(
                    "legacy_or_targeted_review_ids"
                ),
                "priority_legacy_range_target": priority_row is not None,
                "legacy_hard_min": (
                    priority_row.get("legacy_hard_min") if priority_row else None
                ),
                "legacy_hard_max": (
                    priority_row.get("legacy_hard_max") if priority_row else None
                ),
                "legacy_invalid_zero": (
                    priority_row.get("legacy_invalid_zero") if priority_row else None
                ),
                "outside_legacy_range_count": (
                    priority_row.get("outside_legacy_range_count")
                    if priority_row
                    else None
                ),
                "proposed_unit": "PENDING_HUMAN_REVIEW",
                "unit_evidence_source": "PENDING_HUMAN_REVIEW",
                "unit_disposition": "PENDING_HUMAN_REVIEW",
                "hospital_conversion_policy": "PENDING_HUMAN_REVIEW",
                "range_disposition": (
                    "PENDING_HUMAN_REVIEW" if priority_row else "not_applicable"
                ),
                "current_safe_action": "preserve_and_keep_analysis_ineligible",
                "clinical_source_documentation_required": True,
                "distribution_can_establish_unit": False,
                "automatic_conversion_approved": False,
                "values_modified_by_this_audit": False,
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


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        key: (
                            json.dumps(value, ensure_ascii=False, sort_keys=True)
                            if isinstance(value, (dict, list, tuple))
                            else value
                        )
                        for key, value in row.items()
                    }
                )
        temporary.replace(path)
        path.chmod(0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 unresolved-unit and legacy-range evidence review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input cleaned release: `{payload['cleaned_release_id']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Clinical data written or modified: `false`",
        "- Protected identifiers, patient rows, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    lines.extend(
        f"- `{item['check']}`: {item['details']}"
        for item in payload["blocking_findings"]
    )
    lines.extend(("", "## Audit totals", ""))
    lines.extend(f"- {key}: `{value}`" for key, value in metrics.items())
    lines.extend(("", "## Priority legacy-range targets", ""))
    for row in payload["priority_legacy_range_summary"]:
        lines.append(
            f"- `{row['variable']}`: finite `{row['finite_count']}`; outside legacy range and preserved `{row['outside_legacy_range_count']}`"
        )
    lines.extend(("", "## Human review gate", ""))
    lines.append(
        "Review the owner-only decision workbook and record a unit disposition for all 72 variables. For the 12 priority variables, record a separate range disposition only after the unit and definition are established."
    )
    lines.extend(("", "## Safety boundary", ""))
    lines.extend(
        (
            "- Distribution and scale alignment are screening evidence, not proof of a unit.",
            "- Exact raw headers appear only in the owner-only workbook on the authorized cluster.",
            "- Current values remain preserved and analysis-ineligible.",
            "- No conversion, masking, filtering, clinical-data write, contract update, or release rebuild occurs.",
            "- A later approved conversion requires harmonized contract 0.2; a range-only change requires cleaning policy 0.2.",
            "",
        )
    )
    return "\n".join(lines)


def run_unit_resolution_audit(
    config: UnitResolutionAuditConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> UnitResolutionAuditResult:
    if config.dataset_context != "production":
        raise HarmonizationError("The approved unit-resolution audit is production-only")
    policy = load_unit_resolution_audit_policy(config.policy_path)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid unit-resolution audit run ID")
    private_dir = (
        config.reports_root / "private" / policy.private_directory_name / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Unit-resolution audit run ID already exists")
    derivation_policy = load_derivation_contract_review_policy(
        config.derivation.policy_path
    )
    if derivation_policy.cleaned_release_id != policy.cleaned_release_id:
        raise HarmonizationError("Unit-resolution cleaned release lineage changed")
    source = load_cleaned_release_input(config.derivation, derivation_policy)
    evidence = _load_contract_evidence(config, policy)
    unresolved_names = {str(row["variable"]) for row in evidence.unresolved_rows}
    legacy_rules = _invalid_rules_by_column(
        [dict(row) for row in evidence.quality_rules["legacy_invalid_value_rules"]]
    )
    priority_names = unresolved_names & set(legacy_rules)
    expected_priority = dict(policy.expected_priority_outside_counts)
    if priority_names != set(expected_priority) or len(priority_names) != policy.expected_priority_count:
        raise HarmonizationError("Priority unresolved-unit legacy-range scope changed")
    priority_rules = {name: legacy_rules[name] for name in sorted(priority_names)}

    profiles, range_rows, hospital_rows = _scan_cleaned_dynamic(
        source, policy, evidence.unresolved_rows, priority_rules, progress
    )
    prior_profiles = {
        (str(row["hospital"]), str(row["variable"])): row
        for row in evidence.prior_numeric_profiles
    }
    finite_mismatches = [
        {
            "hospital": row["hospital"],
            "variable": row["variable"],
            "prior_finite_count": prior_profiles[
                (str(row["hospital"]), str(row["variable"]))
            ]["finite_count"],
            "current_finite_count": row["finite_count"],
        }
        for row in profiles
        if int(
            prior_profiles[(str(row["hospital"]), str(row["variable"]))][
                "finite_count"
            ]
        )
        != int(row["finite_count"])
    ]
    if finite_mismatches:
        raise HarmonizationError(
            "Cleaned finite-value counts differ from the preserved pre-clean candidate"
        )
    priority_summaries = _aggregate_priority_ranges(range_rows, priority_rules)
    observed_priority = {
        str(row["variable"]): int(row["outside_legacy_range_count"])
        for row in priority_summaries
    }
    if observed_priority != expected_priority:
        raise HarmonizationError(
            "Priority legacy-range counts differ from the approved cleaning audit"
        )
    scale_signals = _scale_signals(profiles, policy)
    distribution_findings = _distribution_findings(profiles, policy)
    workbook = _build_decision_workbook(
        evidence,
        profiles,
        priority_summaries,
        scale_signals,
        distribution_findings,
    )
    if len(workbook) != policy.expected_unresolved_count:
        raise HarmonizationError("Unit decision workbook coverage is incomplete")

    blockers = (
        {
            "check": "unresolved_unit_contract_decisions_approved",
            "details": "All 72 unresolved-unit variables require a source-supported unit or an explicit retain-ineligible/mixed-scale disposition.",
        },
        {
            "check": "priority_legacy_range_preservation_decisions_approved",
            "details": "The 12 legacy range rules require unit-aware activation, replacement, preservation, masking, or retirement decisions.",
        },
    )
    generated = utc_timestamp()
    total_finite = sum(int(row["finite_count"]) for row in profiles)
    metrics = {
        "dynamic_rows_scanned": policy.expected_dynamic_rows,
        "unresolved_unit_variable_count": len(workbook),
        "hospital_variable_profile_count": len(profiles),
        "variables_with_finite_values": sum(
            int(row["finite_value_count"]) > 0 for row in workbook
        ),
        "globally_all_missing_unresolved_variable_count": sum(
            bool(row["all_values_missing"]) for row in workbook
        ),
        "finite_values_profiled": total_finite,
        "preclean_to_cleaned_finite_count_mismatch_count": len(finite_mismatches),
        "cross_hospital_scale_screen_signal_count": len(scale_signals),
        "distribution_screen_finding_count": len(distribution_findings),
        "priority_legacy_range_variable_count": len(priority_summaries),
        "priority_outside_legacy_range_preserved_count": sum(
            int(row["outside_legacy_range_count"]) for row in priority_summaries
        ),
        "source_mapping_variable_count": len(
            {str(row["target"]) for row in evidence.source_mapping_rows}
        ),
        "technical_blocking_finding_count": 0,
        "clinical_data_artifacts_generated": False,
    }
    sanitized_priority = [
        {
            "variable": row["variable"],
            "finite_count": row["finite_count"],
            "outside_legacy_range_count": row["outside_legacy_range_count"],
            "all_values_missing": row["all_values_missing"],
        }
        for row in priority_summaries
    ]
    review_payload = {
        "artifact": "asic_v3_unit_resolution_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "harmonized_contract_version": policy.harmonized_contract_version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "hospital_row_counts": hospital_rows,
        "priority_legacy_range_summary": sanitized_priority,
        "clinical_data_written": False,
        "harmonized_release_modified": False,
        "cleaned_release_modified": False,
        "derived_release_modified": False,
        "values_converted": False,
        "values_masked": False,
        "rows_or_stays_filtered": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
        "privacy": {
            "contains_patient_rows": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_headers": False,
            "exact_raw_headers_location": "owner-only unit decision workbook on the authorized cluster",
        },
    }
    assert_review_payload_is_safe(review_payload)

    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    private_tables = {
        "unit_decision_workbook.parquet": workbook,
        "hospital_numeric_profiles.parquet": profiles,
        "source_mapping_evidence.parquet": list(evidence.source_mapping_rows),
        "cross_hospital_scale_signals.parquet": scale_signals,
        "distribution_screen_findings.parquet": distribution_findings,
        "priority_legacy_range_profiles.parquet": range_rows,
        "priority_legacy_range_summary.parquet": priority_summaries,
    }
    for filename, rows in private_tables.items():
        _write_private_parquet(private_dir / filename, tuple(rows))
        (private_dir / filename).chmod(0o600)
    workbook_csv = private_dir / "unit_decision_workbook.csv"
    _write_csv(workbook_csv, workbook)
    private_readme = private_dir / "README.md"
    _write_text(
        private_readme,
        "# Owner-only unit-resolution evidence\n\n"
        "Start with `unit_decision_workbook.csv`. Exact raw headers are included only there and in the private Parquet evidence. Fill the proposed unit, evidence source, unit disposition, conversion policy, and—where applicable—range disposition. Distribution screens are hypotheses only. No patient rows or stay identifiers are included.\n",
        0o600,
    )
    lineage = {
        "cleaned_release_manifest_sha256": sha256_file(source.release_manifest_path),
        "frozen_dictionary_manifest_sha256": sha256_file(
            evidence.frozen_manifest_path
        ),
        "frozen_dictionary_sha256": sha256_file(evidence.dictionary_path),
        "harmonization_dry_run_manifest_sha256": sha256_file(
            evidence.dry_run_manifest_path
        ),
        "source_mapping_evidence_sha256": sha256_file(evidence.source_mapping_path),
        "prior_consolidated_audit_manifest_sha256": sha256_file(
            evidence.consolidated_manifest_path
        ),
        "prior_unit_review_sha256": sha256_file(evidence.prior_unit_review_path),
        "prior_numeric_profiles_sha256": sha256_file(
            evidence.prior_numeric_profiles_path
        ),
        "unit_resolution_policy_sha256": sha256_file(policy.source_path),
        "quality_policy_sha256": sha256_file(policy.quality_policy_path),
    }
    manifest = {
        "artifact": "asic_v3_unit_resolution_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "lineage": lineage,
        "metrics": metrics,
        "table_row_counts": {
            name: len(rows) for name, rows in private_tables.items()
        },
        "file_sha256": {
            **{name: sha256_file(private_dir / name) for name in private_tables},
            workbook_csv.name: sha256_file(workbook_csv),
            private_readme.name: sha256_file(private_readme),
        },
        "clinical_data_written": False,
        "publication_ready": False,
    }
    _write_json(private_dir / "unit_resolution_audit_manifest.json", manifest, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _markdown(review_payload), 0o640)
    return UnitResolutionAuditResult(
        run_id=selected_run,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        technical_blocking_findings=(),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
