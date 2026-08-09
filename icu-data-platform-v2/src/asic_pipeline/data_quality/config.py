from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.config import ALLOWED_DATASET_CONTEXTS, validate_context_output_path
from asic_pipeline.contracts import TranslatedInputContract
from asic_pipeline.errors import ConfigurationError, ContractError


@dataclass(frozen=True)
class CrossHospitalAuditConfig:
    dataset_context: str
    translated_contract_path: Path
    policy_path: Path
    translated_dir: Path
    reports_dir: Path
    batch_size: int
    quantile_sample_size_per_hospital_variable: int
    max_examples: int
    source_path: Path


@dataclass(frozen=True)
class InvalidValueRule:
    legacy_name: str
    columns: tuple[str, ...]
    hard_min: float | None
    hard_max: float | None
    invalid_zero: bool
    description: str


@dataclass(frozen=True)
class KnownLegacyFinding:
    finding_id: str
    column: str
    hospital: str
    prior_classification: str
    prior_action: str
    current_verification: str
    decision: str
    conversion_divisor: float | None


@dataclass(frozen=True)
class TargetedScaleAudit:
    audit_id: str
    column: str
    hospital: str
    operation: str
    factor: float
    threshold: float | None
    expected_min: float | None
    expected_max: float | None
    evidence: str
    approval_status: str
    peer_hospitals: tuple[str, ...] | None


@dataclass(frozen=True)
class RowLevelScaleEntryAudit:
    audit_id: str
    column: str
    operation: str
    factor: float
    recovery_min: float
    recovery_max: float
    evidence: str
    approval_status: str


@dataclass(frozen=True)
class TargetedBucketAudit:
    audit_id: str
    column: str
    hospital: str
    cut_points: tuple[float, ...]
    reason: str


@dataclass(frozen=True)
class SentinelRule:
    column: str
    value: str | float
    value_kind: str


@dataclass(frozen=True)
class DistributionPolicy:
    minimum_non_missing_values: int
    minimum_hospitals: int
    hospital_metric_iqr_fence_factor: float
    metrics: tuple[str, ...]
    additional_metrics: tuple[str, ...]


@dataclass(frozen=True)
class ScaleEntryDiscoveryPolicy:
    source: str
    factors: tuple[float, ...]
    minimum_finite_values_for_pattern: int
    rare_candidate_max_fraction: float
    systematic_candidate_min_fraction: float


@dataclass(frozen=True)
class CandidateContextFieldPolicy:
    column: str
    excluded_hospitals: tuple[str, ...]
    related_fields: tuple[str, ...]
    relationship: str


@dataclass(frozen=True)
class CandidateContextAuditPolicy:
    windows_hours: tuple[float, ...]
    maximum_candidate_rows: int
    fields: tuple[CandidateContextFieldPolicy, ...]


@dataclass(frozen=True)
class PredictedBodyWeightTidalVolumeAudit:
    audit_id: str
    hospital: str
    sex_column: str
    height_column: str
    actual_weight_column: str
    vt_column: str
    vt_per_kg_column: str
    target_absolute_vt_column: str
    male_value: str
    female_value: str
    male_intercept_kg: float
    female_intercept_kg: float
    height_coefficient_kg_per_cm: float
    height_center_cm: float
    target_ml_per_kg_pbw: float
    plausible_height_min_cm: float
    plausible_height_max_cm: float
    plausible_actual_weight_min_kg: float
    plausible_actual_weight_max_kg: float
    absolute_tolerance_ml: float
    normalized_tolerance_ml_per_kg: float
    approval_status: str
    evidence: str


@dataclass(frozen=True)
class DataQualityPolicy:
    policy_version: str
    status: str
    translated_contract_version: str
    documentation: str
    hospital_column: str
    stay_id_column: str
    profile_exclusions: dict[str, tuple[str, ...]]
    distribution: DistributionPolicy
    scale_entry_discovery: ScaleEntryDiscoveryPolicy
    candidate_context_audit: CandidateContextAuditPolicy
    predicted_body_weight_tidal_volume_audit: (
        PredictedBodyWeightTidalVolumeAudit
    )
    legacy_sources: tuple[dict[str, Any], ...]
    legacy_coverage: dict[str, int]
    legacy_raw_numeric_parsing: dict[str, Any]
    sentinels: tuple[SentinelRule, ...]
    invalid_value_rules: tuple[InvalidValueRule, ...]
    known_legacy_findings: tuple[KnownLegacyFinding, ...]
    targeted_scale_audits: tuple[TargetedScaleAudit, ...]
    row_level_scale_entry_audits: tuple[RowLevelScaleEntryAudit, ...]
    targeted_comparison_audits: tuple[dict[str, Any], ...]
    targeted_bucket_audits: tuple[TargetedBucketAudit, ...]
    targeted_review_coverage: dict[str, int]
    relationship_audits: tuple[dict[str, Any], ...]
    availability_audits: tuple[dict[str, Any], ...]
    source_path: Path


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"{label} does not exist: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in {label} {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{label} must contain a YAML mapping: {path}")
    return raw


def _required_string(raw: dict[str, Any], key: str, location: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _resolve_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def load_cross_hospital_audit_config(
    path: str | Path,
) -> CrossHospitalAuditConfig:
    config_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(config_path, "Dataset configuration")
    context = _required_string(raw, "dataset_context", "config")
    if context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError(
            f"config.dataset_context must be one of {sorted(ALLOWED_DATASET_CONTEXTS)}"
        )
    contract_path = _resolve_path(
        _required_string(raw, "translated_contract", "config"), config_path
    )
    policy_path = _resolve_path(
        _required_string(raw, "data_quality_policy", "config"), config_path
    )
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigurationError("config.paths must be a mapping")
    translated_dir = _resolve_path(
        _required_string(paths, "translated", "config.paths"), config_path
    )
    reports_dir = _resolve_path(
        _required_string(paths, "reports", "config.paths"), config_path
    )
    validate_context_output_path(
        translated_dir, context, "config.paths.translated"
    )
    validate_context_output_path(reports_dir, context, "config.paths.reports")
    if translated_dir.name != "translated":
        raise ConfigurationError(
            "config.paths.translated must end in a 'translated' directory"
        )
    settings = raw.get("cross_hospital_audit", {})
    if not isinstance(settings, dict):
        raise ConfigurationError("config.cross_hospital_audit must be a mapping")
    return CrossHospitalAuditConfig(
        dataset_context=context,
        translated_contract_path=contract_path,
        policy_path=policy_path,
        translated_dir=translated_dir,
        reports_dir=reports_dir,
        batch_size=_positive_int(
            settings.get("batch_size", 100_000),
            "config.cross_hospital_audit.batch_size",
        ),
        quantile_sample_size_per_hospital_variable=_positive_int(
            settings.get("quantile_sample_size_per_hospital_variable", 20_000),
            "config.cross_hospital_audit.quantile_sample_size_per_hospital_variable",
        ),
        max_examples=_positive_int(
            settings.get("max_examples", 10),
            "config.cross_hospital_audit.max_examples",
        ),
        source_path=config_path,
    )


def _mapping_list(raw: Any, location: str) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
        raise ContractError(f"{location} must be a list of mappings")
    return raw


def _string_list(raw: Any, location: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or any(
        not isinstance(item, str) or not item.strip() for item in raw
    ):
        raise ContractError(f"{location} must be a list of non-empty strings")
    values = tuple(item.strip() for item in raw)
    if len(values) != len(set(values)):
        raise ContractError(f"{location} contains duplicates")
    return values


def _policy_string(raw: dict[str, Any], key: str, location: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _optional_number(raw: dict[str, Any], key: str, location: str) -> float | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ContractError(f"{location}.{key} must be numeric when provided")
    return float(value)


def _required_number(raw: dict[str, Any], key: str, location: str) -> float:
    value = _optional_number(raw, key, location)
    if value is None:
        raise ContractError(f"{location}.{key} is required")
    return value


def load_data_quality_policy(
    path: str | Path,
    contract: TranslatedInputContract,
) -> DataQualityPolicy:
    policy_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(policy_path, "Data-quality audit policy")
    location = "data_quality_policy"
    version = _policy_string(raw, "policy_version", location)
    contract_version = _policy_string(raw, "translated_contract_version", location)
    if contract_version != contract.contract_version:
        raise ContractError(
            "Data-quality policy translated_contract_version does not match the "
            f"loaded contract: {contract_version!r} != {contract.contract_version!r}"
        )
    scope = raw.get("scope")
    if not isinstance(scope, dict):
        raise ContractError(f"{location}.scope must be a mapping")
    if scope.get("profile_every_numeric_translated_field") is not True:
        raise ContractError(
            f"{location}.scope.profile_every_numeric_translated_field must be true"
        )
    exclusions_raw = scope.get("profile_exclusions", {})
    if not isinstance(exclusions_raw, dict):
        raise ContractError(f"{location}.scope.profile_exclusions must be a mapping")
    exclusions = {
        table: _string_list(exclusions_raw.get(table, []), f"{location}.scope.profile_exclusions.{table}")
        for table in ("static", "dynamic")
    }
    distribution_raw = raw.get("distribution_screen")
    if not isinstance(distribution_raw, dict):
        raise ContractError(f"{location}.distribution_screen must be a mapping")
    distribution = DistributionPolicy(
        minimum_non_missing_values=_positive_int(
            distribution_raw.get("minimum_non_missing_values"),
            f"{location}.distribution_screen.minimum_non_missing_values",
        ),
        minimum_hospitals=_positive_int(
            distribution_raw.get("minimum_hospitals"),
            f"{location}.distribution_screen.minimum_hospitals",
        ),
        hospital_metric_iqr_fence_factor=float(
            distribution_raw.get("hospital_metric_iqr_fence_factor", 1.5)
        ),
        metrics=_string_list(
            distribution_raw.get("metrics"),
            f"{location}.distribution_screen.metrics",
        ),
        additional_metrics=_string_list(
            distribution_raw.get("additional_metrics", []),
            f"{location}.distribution_screen.additional_metrics",
        ),
    )
    if distribution.hospital_metric_iqr_fence_factor <= 0:
        raise ContractError(
            f"{location}.distribution_screen.hospital_metric_iqr_fence_factor must be positive"
        )

    discovery_location = f"{location}.scale_entry_discovery"
    discovery_raw = raw.get("scale_entry_discovery")
    if not isinstance(discovery_raw, dict):
        raise ContractError(f"{discovery_location} must be a mapping")
    discovery_source = _policy_string(
        discovery_raw,
        "source",
        discovery_location,
    )
    if discovery_source != "bounded_legacy_invalid_value_rules":
        raise ContractError(
            f"{discovery_location}.source must be "
            "'bounded_legacy_invalid_value_rules'"
        )
    raw_factors = discovery_raw.get("factors")
    if (
        not isinstance(raw_factors, list)
        or not raw_factors
        or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or value <= 1
            for value in raw_factors
        )
    ):
        raise ContractError(
            f"{discovery_location}.factors must be a non-empty list of "
            "numbers greater than 1"
        )
    discovery_factors = tuple(float(value) for value in raw_factors)
    if tuple(sorted(set(discovery_factors))) != discovery_factors:
        raise ContractError(
            f"{discovery_location}.factors must be unique and strictly increasing"
        )
    rare_fraction = _optional_number(
        discovery_raw,
        "rare_candidate_max_fraction",
        discovery_location,
    )
    systematic_fraction = _optional_number(
        discovery_raw,
        "systematic_candidate_min_fraction",
        discovery_location,
    )
    if (
        rare_fraction is None
        or systematic_fraction is None
        or not 0 <= rare_fraction < systematic_fraction <= 1
    ):
        raise ContractError(
            f"{discovery_location} fractions must satisfy 0 <= "
            "rare_candidate_max_fraction < "
            "systematic_candidate_min_fraction <= 1"
        )
    scale_entry_discovery = ScaleEntryDiscoveryPolicy(
        source=discovery_source,
        factors=discovery_factors,
        minimum_finite_values_for_pattern=_positive_int(
            discovery_raw.get("minimum_finite_values_for_pattern"),
            f"{discovery_location}.minimum_finite_values_for_pattern",
        ),
        rare_candidate_max_fraction=rare_fraction,
        systematic_candidate_min_fraction=systematic_fraction,
    )

    rules = []
    for index, item in enumerate(
        _mapping_list(raw.get("legacy_invalid_value_rules"), f"{location}.legacy_invalid_value_rules")
    ):
        rule_location = f"{location}.legacy_invalid_value_rules[{index}]"
        invalid_zero = item.get("invalid_zero", False)
        if not isinstance(invalid_zero, bool):
            raise ContractError(f"{rule_location}.invalid_zero must be boolean")
        rules.append(
            InvalidValueRule(
                legacy_name=_policy_string(item, "legacy_name", rule_location),
                columns=_string_list(item.get("columns"), f"{rule_location}.columns"),
                hard_min=_optional_number(item, "hard_min", rule_location),
                hard_max=_optional_number(item, "hard_max", rule_location),
                invalid_zero=invalid_zero,
                description=_policy_string(item, "description", rule_location),
            )
        )
    legacy_names = [rule.legacy_name for rule in rules]
    if len(legacy_names) != len(set(legacy_names)):
        raise ContractError("Legacy invalid-value rule names must be unique")

    findings = []
    for index, item in enumerate(
        _mapping_list(raw.get("known_legacy_semantic_findings"), f"{location}.known_legacy_semantic_findings")
    ):
        finding_location = f"{location}.known_legacy_semantic_findings[{index}]"
        findings.append(
            KnownLegacyFinding(
                finding_id=_policy_string(item, "id", finding_location),
                column=_policy_string(item, "column", finding_location),
                hospital=_policy_string(item, "hospital", finding_location),
                prior_classification=_policy_string(
                    item, "prior_classification", finding_location
                ),
                prior_action=_policy_string(item, "prior_action", finding_location),
                current_verification=_policy_string(
                    item, "current_verification", finding_location
                ),
                decision=_policy_string(item, "decision", finding_location),
                conversion_divisor=_optional_number(
                    item, "conversion_divisor", finding_location
                ),
            )
        )

    sentinels = []
    for index, item in enumerate(
        _mapping_list(raw.get("legacy_static_minus_one_sentinels"), f"{location}.legacy_static_minus_one_sentinels")
    ):
        sentinel_location = f"{location}.legacy_static_minus_one_sentinels[{index}]"
        value_kind = _policy_string(item, "value_kind", sentinel_location)
        if value_kind not in {"numeric", "string"}:
            raise ContractError(f"{sentinel_location}.value_kind must be numeric or string")
        value = item.get("value")
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise ContractError(f"{sentinel_location}.value must be scalar")
        sentinels.append(
            SentinelRule(
                column=_policy_string(item, "column", sentinel_location),
                value=float(value) if value_kind == "numeric" else str(value),
                value_kind=value_kind,
            )
        )

    coverage = raw.get("legacy_coverage")
    if not isinstance(coverage, dict) or any(
        not isinstance(value, int) or isinstance(value, bool) or value <= 0
        for value in coverage.values()
    ):
        raise ContractError(f"{location}.legacy_coverage must contain positive integer counts")
    expanded_count = sum(len(rule.columns) for rule in rules)
    observed_coverage = {
        "invalid_value_rule_count": len(rules),
        "expanded_v2_invalid_value_check_count": expanded_count,
        "targeted_semantic_finding_count": len(findings),
        "static_minus_one_sentinel_count": len(sentinels),
    }
    mismatches = {
        key: {"declared": coverage.get(key), "observed": observed}
        for key, observed in observed_coverage.items()
        if coverage.get(key) != observed
    }
    if mismatches:
        raise ContractError(f"Legacy policy coverage counts do not match: {mismatches}")

    static_columns = {column.name for column in contract.static.columns}
    dynamic_columns = {column.name for column in contract.dynamic.columns}
    all_rule_columns = {column for rule in rules for column in rule.columns}
    unknown = sorted(all_rule_columns - dynamic_columns)
    if unknown:
        raise ContractError(f"Invalid-value policy references unknown dynamic columns: {unknown}")
    unknown_sentinels = sorted({item.column for item in sentinels} - static_columns)
    if unknown_sentinels:
        raise ContractError(f"Sentinel policy references unknown static columns: {unknown_sentinels}")
    unknown_findings = sorted({item.column for item in findings} - dynamic_columns)
    if unknown_findings:
        raise ContractError(f"Legacy findings reference unknown dynamic columns: {unknown_findings}")

    targeted_scale_audits: list[TargetedScaleAudit] = []
    expected_hospital_ids = {
        f"asic_UK{code:02d}" for code in contract.expected_hospital_codes
    }

    context_location = f"{location}.candidate_context_audit"
    context_raw = raw.get("candidate_context_audit")
    if not isinstance(context_raw, dict):
        raise ContractError(f"{context_location} must be a mapping")
    raw_windows = context_raw.get("windows_hours")
    if (
        not isinstance(raw_windows, list)
        or not raw_windows
        or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or value <= 0
            for value in raw_windows
        )
    ):
        raise ContractError(
            f"{context_location}.windows_hours must be a non-empty list of "
            "positive numbers"
        )
    context_windows = tuple(float(value) for value in raw_windows)
    if tuple(sorted(set(context_windows))) != context_windows:
        raise ContractError(
            f"{context_location}.windows_hours must be unique and strictly "
            "increasing"
        )
    rules_by_column = {
        column: rule for rule in rules for column in rule.columns
    }
    context_fields: list[CandidateContextFieldPolicy] = []
    allowed_relationships = {
        "none",
        "compare_related_field",
        "map_from_sbp_dbp",
    }
    for index, item in enumerate(
        _mapping_list(context_raw.get("fields"), f"{context_location}.fields")
    ):
        field_location = f"{context_location}.fields[{index}]"
        column = _policy_string(item, "column", field_location)
        if column not in dynamic_columns:
            raise ContractError(
                f"{field_location} references unknown dynamic column {column!r}"
            )
        invalid_rule = rules_by_column.get(column)
        if (
            invalid_rule is None
            or invalid_rule.hard_min is None
            or invalid_rule.hard_max is None
        ):
            raise ContractError(
                f"{field_location}.column must have a legacy invalid-value "
                "rule with both hard_min and hard_max"
            )
        excluded_hospitals = _string_list(
            item.get("excluded_hospitals", []),
            f"{field_location}.excluded_hospitals",
        )
        unknown_hospitals = sorted(
            set(excluded_hospitals) - expected_hospital_ids
        )
        if unknown_hospitals:
            raise ContractError(
                f"{field_location}.excluded_hospitals contains unknown "
                f"hospitals: {unknown_hospitals}"
            )
        related_fields = _string_list(
            item.get("related_fields", []),
            f"{field_location}.related_fields",
        )
        unknown_related = sorted(set(related_fields) - dynamic_columns)
        if unknown_related:
            raise ContractError(
                f"{field_location}.related_fields contains unknown dynamic "
                f"columns: {unknown_related}"
            )
        relationship = _policy_string(item, "relationship", field_location)
        if relationship not in allowed_relationships:
            raise ContractError(
                f"{field_location}.relationship must be one of "
                f"{sorted(allowed_relationships)}"
            )
        if relationship == "none" and related_fields:
            raise ContractError(
                f"{field_location}.related_fields must be empty when "
                "relationship is 'none'"
            )
        if relationship == "compare_related_field" and len(related_fields) != 1:
            raise ContractError(
                f"{field_location}.related_fields must contain exactly one "
                "field for compare_related_field"
            )
        if relationship == "map_from_sbp_dbp" and set(related_fields) != {
            "sbp",
            "dbp",
        }:
            raise ContractError(
                f"{field_location}.related_fields must contain sbp and dbp "
                "for map_from_sbp_dbp"
            )
        context_fields.append(
            CandidateContextFieldPolicy(
                column=column,
                excluded_hospitals=excluded_hospitals,
                related_fields=related_fields,
                relationship=relationship,
            )
        )
    context_columns = [item.column for item in context_fields]
    if len(context_columns) != len(set(context_columns)):
        raise ContractError(
            f"{context_location}.fields contains duplicate candidate columns"
        )
    candidate_context_audit = CandidateContextAuditPolicy(
        windows_hours=context_windows,
        maximum_candidate_rows=_positive_int(
            context_raw.get("maximum_candidate_rows"),
            f"{context_location}.maximum_candidate_rows",
        ),
        fields=tuple(context_fields),
    )

    pbw_location = (
        f"{location}.predicted_body_weight_tidal_volume_audit"
    )
    pbw_raw = raw.get("predicted_body_weight_tidal_volume_audit")
    if not isinstance(pbw_raw, dict):
        raise ContractError(f"{pbw_location} must be a mapping")
    pbw_hospital = _policy_string(pbw_raw, "hospital", pbw_location)
    if pbw_hospital not in expected_hospital_ids:
        raise ContractError(
            f"{pbw_location}.hospital must be one of "
            f"{sorted(expected_hospital_ids)}"
        )
    pbw_static_fields = {
        key: _policy_string(pbw_raw, key, pbw_location)
        for key in ("sex_column", "height_column", "actual_weight_column")
    }
    unknown_pbw_static = sorted(
        set(pbw_static_fields.values()) - static_columns
    )
    if unknown_pbw_static:
        raise ContractError(
            f"{pbw_location} references unknown static columns: "
            f"{unknown_pbw_static}"
        )
    pbw_dynamic_fields = {
        key: _policy_string(pbw_raw, key, pbw_location)
        for key in (
            "vt_column",
            "vt_per_kg_column",
            "target_absolute_vt_column",
        )
    }
    unknown_pbw_dynamic = sorted(
        set(pbw_dynamic_fields.values()) - dynamic_columns
    )
    if unknown_pbw_dynamic:
        raise ContractError(
            f"{pbw_location} references unknown dynamic columns: "
            f"{unknown_pbw_dynamic}"
        )
    male_value = _policy_string(pbw_raw, "male_value", pbw_location)
    female_value = _policy_string(pbw_raw, "female_value", pbw_location)
    if male_value == female_value:
        raise ContractError(
            f"{pbw_location}.male_value and female_value must differ"
        )
    numeric = {
        key: _required_number(pbw_raw, key, pbw_location)
        for key in (
            "male_intercept_kg",
            "female_intercept_kg",
            "height_coefficient_kg_per_cm",
            "height_center_cm",
            "target_ml_per_kg_pbw",
            "plausible_height_min_cm",
            "plausible_height_max_cm",
            "plausible_actual_weight_min_kg",
            "plausible_actual_weight_max_kg",
            "absolute_tolerance_ml",
            "normalized_tolerance_ml_per_kg",
        )
    }
    for key in (
        "height_coefficient_kg_per_cm",
        "target_ml_per_kg_pbw",
        "plausible_height_min_cm",
        "plausible_height_max_cm",
        "plausible_actual_weight_min_kg",
        "plausible_actual_weight_max_kg",
    ):
        if numeric[key] <= 0:
            raise ContractError(f"{pbw_location}.{key} must be positive")
    for key in (
        "absolute_tolerance_ml",
        "normalized_tolerance_ml_per_kg",
    ):
        if numeric[key] < 0:
            raise ContractError(f"{pbw_location}.{key} must be non-negative")
    if numeric["plausible_height_min_cm"] >= numeric["plausible_height_max_cm"]:
        raise ContractError(
            f"{pbw_location}.plausible_height_min_cm must be below "
            "plausible_height_max_cm"
        )
    if (
        numeric["plausible_actual_weight_min_kg"]
        >= numeric["plausible_actual_weight_max_kg"]
    ):
        raise ContractError(
            f"{pbw_location}.plausible_actual_weight_min_kg must be below "
            "plausible_actual_weight_max_kg"
        )
    predicted_body_weight_tidal_volume_audit = (
        PredictedBodyWeightTidalVolumeAudit(
            audit_id=_policy_string(pbw_raw, "id", pbw_location),
            hospital=pbw_hospital,
            sex_column=pbw_static_fields["sex_column"],
            height_column=pbw_static_fields["height_column"],
            actual_weight_column=pbw_static_fields["actual_weight_column"],
            vt_column=pbw_dynamic_fields["vt_column"],
            vt_per_kg_column=pbw_dynamic_fields["vt_per_kg_column"],
            target_absolute_vt_column=pbw_dynamic_fields[
                "target_absolute_vt_column"
            ],
            male_value=male_value,
            female_value=female_value,
            male_intercept_kg=numeric["male_intercept_kg"],
            female_intercept_kg=numeric["female_intercept_kg"],
            height_coefficient_kg_per_cm=numeric[
                "height_coefficient_kg_per_cm"
            ],
            height_center_cm=numeric["height_center_cm"],
            target_ml_per_kg_pbw=numeric["target_ml_per_kg_pbw"],
            plausible_height_min_cm=numeric["plausible_height_min_cm"],
            plausible_height_max_cm=numeric["plausible_height_max_cm"],
            plausible_actual_weight_min_kg=numeric[
                "plausible_actual_weight_min_kg"
            ],
            plausible_actual_weight_max_kg=numeric[
                "plausible_actual_weight_max_kg"
            ],
            absolute_tolerance_ml=numeric["absolute_tolerance_ml"],
            normalized_tolerance_ml_per_kg=numeric[
                "normalized_tolerance_ml_per_kg"
            ],
            approval_status=_policy_string(
                pbw_raw, "approval_status", pbw_location
            ),
            evidence=_policy_string(pbw_raw, "evidence", pbw_location),
        )
    )

    for index, item in enumerate(
        _mapping_list(
            raw.get("targeted_scale_audits"),
            f"{location}.targeted_scale_audits",
        )
    ):
        audit_location = f"{location}.targeted_scale_audits[{index}]"
        operation = _policy_string(item, "operation", audit_location)
        if operation not in {"multiply", "divide", "divide_when_above"}:
            raise ContractError(
                f"{audit_location}.operation must be multiply, divide, or divide_when_above"
            )
        factor = _optional_number(item, "factor", audit_location)
        if factor is None or factor <= 0:
            raise ContractError(f"{audit_location}.factor must be positive")
        threshold = _optional_number(item, "threshold", audit_location)
        if operation == "divide_when_above" and threshold is None:
            raise ContractError(
                f"{audit_location}.threshold is required for divide_when_above"
            )
        column = _policy_string(item, "column", audit_location)
        if column not in dynamic_columns:
            raise ContractError(
                f"{audit_location} references unknown dynamic column {column!r}"
            )
        hospital = _policy_string(item, "hospital", audit_location)
        if hospital not in expected_hospital_ids:
            raise ContractError(
                f"{audit_location}.hospital must be one of "
                f"{sorted(expected_hospital_ids)}, got {hospital!r}"
            )
        raw_peer_hospitals = item.get("peer_hospitals")
        peer_hospitals = (
            _string_list(
                raw_peer_hospitals,
                f"{audit_location}.peer_hospitals",
            )
            if raw_peer_hospitals is not None
            else None
        )
        if peer_hospitals is not None:
            unknown_peers = sorted(set(peer_hospitals) - expected_hospital_ids)
            if unknown_peers:
                raise ContractError(
                    f"{audit_location}.peer_hospitals contains unknown hospitals: "
                    f"{unknown_peers}"
                )
            if not peer_hospitals:
                raise ContractError(
                    f"{audit_location}.peer_hospitals must not be empty"
                )
            if hospital in peer_hospitals:
                raise ContractError(
                    f"{audit_location}.peer_hospitals must not include the target "
                    f"hospital {hospital!r}"
                )
        expected_min = _optional_number(item, "expected_min", audit_location)
        expected_max = _optional_number(item, "expected_max", audit_location)
        if (
            expected_min is not None
            and expected_max is not None
            and expected_min > expected_max
        ):
            raise ContractError(
                f"{audit_location}.expected_min cannot exceed expected_max"
            )
        targeted_scale_audits.append(
            TargetedScaleAudit(
                audit_id=_policy_string(item, "id", audit_location),
                column=column,
                hospital=hospital,
                operation=operation,
                factor=factor,
                threshold=threshold,
                expected_min=expected_min,
                expected_max=expected_max,
                evidence=_policy_string(item, "evidence", audit_location),
                approval_status=_policy_string(
                    item, "approval_status", audit_location
                ),
                peer_hospitals=peer_hospitals,
            )
        )
    targeted_scale_ids = [item.audit_id for item in targeted_scale_audits]
    if len(targeted_scale_ids) != len(set(targeted_scale_ids)):
        raise ContractError("Targeted scale audit IDs must be unique")

    row_level_scale_entry_audits: list[RowLevelScaleEntryAudit] = []
    for index, item in enumerate(
        _mapping_list(
            raw.get("row_level_scale_entry_audits"),
            f"{location}.row_level_scale_entry_audits",
        )
    ):
        audit_location = f"{location}.row_level_scale_entry_audits[{index}]"
        operation = _policy_string(item, "operation", audit_location)
        if operation not in {"multiply", "divide"}:
            raise ContractError(
                f"{audit_location}.operation must be multiply or divide"
            )
        factor = _optional_number(item, "factor", audit_location)
        if factor is None or factor <= 0:
            raise ContractError(f"{audit_location}.factor must be positive")
        recovery_min = _optional_number(item, "recovery_min", audit_location)
        recovery_max = _optional_number(item, "recovery_max", audit_location)
        if recovery_min is None or recovery_max is None:
            raise ContractError(
                f"{audit_location}.recovery_min and recovery_max are required"
            )
        if recovery_min >= recovery_max:
            raise ContractError(
                f"{audit_location}.recovery_min must be below recovery_max"
            )
        column = _policy_string(item, "column", audit_location)
        if column not in dynamic_columns:
            raise ContractError(
                f"{audit_location} references unknown dynamic column {column!r}"
            )
        row_level_scale_entry_audits.append(
            RowLevelScaleEntryAudit(
                audit_id=_policy_string(item, "id", audit_location),
                column=column,
                operation=operation,
                factor=factor,
                recovery_min=recovery_min,
                recovery_max=recovery_max,
                evidence=_policy_string(item, "evidence", audit_location),
                approval_status=_policy_string(
                    item, "approval_status", audit_location
                ),
            )
        )
    row_level_scale_ids = [
        item.audit_id for item in row_level_scale_entry_audits
    ]
    if len(row_level_scale_ids) != len(set(row_level_scale_ids)):
        raise ContractError("Row-level scale-entry audit IDs must be unique")

    targeted_comparisons = _mapping_list(
        raw.get("targeted_comparison_audits"),
        f"{location}.targeted_comparison_audits",
    )
    targeted_comparison_ids: list[str] = []
    for index, item in enumerate(targeted_comparisons):
        audit_location = f"{location}.targeted_comparison_audits[{index}]"
        targeted_comparison_ids.append(_policy_string(item, "id", audit_location))
        column = _policy_string(item, "column", audit_location)
        if column not in dynamic_columns:
            raise ContractError(
                f"{audit_location} references unknown dynamic column {column!r}"
            )
        _policy_string(item, "reason", audit_location)
        _policy_string(item, "approval_status", audit_location)
    if len(targeted_comparison_ids) != len(set(targeted_comparison_ids)):
        raise ContractError("Targeted comparison audit IDs must be unique")

    targeted_buckets: list[TargetedBucketAudit] = []
    for index, item in enumerate(
        _mapping_list(
            raw.get("targeted_bucket_audits"),
            f"{location}.targeted_bucket_audits",
        )
    ):
        audit_location = f"{location}.targeted_bucket_audits[{index}]"
        column = _policy_string(item, "column", audit_location)
        if column not in dynamic_columns:
            raise ContractError(
                f"{audit_location} references unknown dynamic column {column!r}"
            )
        raw_cut_points = item.get("cut_points")
        if (
            not isinstance(raw_cut_points, list)
            or not raw_cut_points
            or any(
                not isinstance(value, (int, float)) or isinstance(value, bool)
                for value in raw_cut_points
            )
        ):
            raise ContractError(
                f"{audit_location}.cut_points must be a non-empty numeric list"
            )
        cut_points = tuple(float(value) for value in raw_cut_points)
        if tuple(sorted(set(cut_points))) != cut_points:
            raise ContractError(
                f"{audit_location}.cut_points must be strictly increasing"
            )
        targeted_buckets.append(
            TargetedBucketAudit(
                audit_id=_policy_string(item, "id", audit_location),
                column=column,
                hospital=_policy_string(item, "hospital", audit_location),
                cut_points=cut_points,
                reason=_policy_string(item, "reason", audit_location),
            )
        )
    targeted_bucket_ids = [item.audit_id for item in targeted_buckets]
    if len(targeted_bucket_ids) != len(set(targeted_bucket_ids)):
        raise ContractError("Targeted bucket audit IDs must be unique")

    targeted_coverage = raw.get("targeted_review_coverage")
    if not isinstance(targeted_coverage, dict) or any(
        not isinstance(value, int) or isinstance(value, bool) or value <= 0
        for value in targeted_coverage.values()
    ):
        raise ContractError(
            f"{location}.targeted_review_coverage must contain positive integer counts"
        )
    observed_targeted_coverage = {
        "scale_hypothesis_count": len(targeted_scale_audits),
        "row_level_scale_entry_audit_count": len(
            row_level_scale_entry_audits
        ),
        "comparison_audit_count": len(targeted_comparisons),
        "bucket_audit_count": len(targeted_buckets),
        "cross_table_formula_audit_count": 1,
    }
    targeted_coverage_mismatches = {
        key: {"declared": targeted_coverage.get(key), "observed": observed}
        for key, observed in observed_targeted_coverage.items()
        if targeted_coverage.get(key) != observed
    }
    if targeted_coverage_mismatches:
        raise ContractError(
            "Targeted review policy coverage counts do not match: "
            f"{targeted_coverage_mismatches}"
        )

    legacy_sources = _mapping_list(raw.get("legacy_sources"), f"{location}.legacy_sources")
    raw_parsing = raw.get("legacy_raw_numeric_parsing")
    if not isinstance(raw_parsing, dict):
        raise ContractError(f"{location}.legacy_raw_numeric_parsing must be a mapping")
    relationships = _mapping_list(raw.get("relationship_audits"), f"{location}.relationship_audits")
    availability = _mapping_list(raw.get("availability_audits"), f"{location}.availability_audits")
    relationship_ids: list[str] = []
    table_columns = {"static": static_columns, "dynamic": dynamic_columns}
    for index, relationship in enumerate(relationships):
        relationship_location = f"{location}.relationship_audits[{index}]"
        relationship_ids.append(
            _policy_string(relationship, "id", relationship_location)
        )
        table_name = _policy_string(relationship, "table", relationship_location)
        if table_name not in table_columns:
            raise ContractError(f"{relationship_location}.table must be static or dynamic")
        fields = _string_list(
            relationship.get("fields"), f"{relationship_location}.fields"
        )
        unknown_fields = sorted(set(fields) - table_columns[table_name])
        if unknown_fields:
            raise ContractError(
                f"{relationship_location} references unknown {table_name} fields: {unknown_fields}"
            )
        _policy_string(relationship, "formula", relationship_location)
        hospital_scope = relationship.get("hospital_scope")
        if hospital_scope is not None:
            _string_list(
                hospital_scope,
                f"{relationship_location}.hospital_scope",
            )
        tolerance = relationship.get("absolute_tolerance", 0)
        if (
            not isinstance(tolerance, (int, float))
            or isinstance(tolerance, bool)
            or tolerance < 0
        ):
            raise ContractError(
                f"{relationship_location}.absolute_tolerance must be non-negative"
            )
    if len(relationship_ids) != len(set(relationship_ids)):
        raise ContractError("Relationship audit IDs must be unique")

    availability_ids: list[str] = []
    for index, audit in enumerate(availability):
        audit_location = f"{location}.availability_audits[{index}]"
        availability_ids.append(_policy_string(audit, "id", audit_location))
        fields = _string_list(audit.get("columns"), f"{audit_location}.columns")
        unknown_fields = sorted(set(fields) - dynamic_columns)
        if unknown_fields:
            raise ContractError(
                f"{audit_location} references unknown dynamic fields: {unknown_fields}"
            )
        _string_list(
            audit.get("expected_non_missing_hospitals"),
            f"{audit_location}.expected_non_missing_hospitals",
        )
    if len(availability_ids) != len(set(availability_ids)):
        raise ContractError("Availability audit IDs must be unique")

    hospital_column = _policy_string(scope, "hospital_column", f"{location}.scope")
    stay_id_column = _policy_string(scope, "stay_id_column", f"{location}.scope")
    for field_name, label in (
        (hospital_column, "hospital_column"),
        (stay_id_column, "stay_id_column"),
    ):
        if field_name not in static_columns or field_name not in dynamic_columns:
            raise ContractError(
                f"{location}.scope.{label} must exist in both translated tables"
            )

    return DataQualityPolicy(
        policy_version=version,
        status=_policy_string(raw, "status", location),
        translated_contract_version=contract_version,
        documentation=_policy_string(raw, "documentation", location),
        hospital_column=hospital_column,
        stay_id_column=stay_id_column,
        profile_exclusions=exclusions,
        distribution=distribution,
        scale_entry_discovery=scale_entry_discovery,
        candidate_context_audit=candidate_context_audit,
        predicted_body_weight_tidal_volume_audit=(
            predicted_body_weight_tidal_volume_audit
        ),
        legacy_sources=tuple(legacy_sources),
        legacy_coverage={key: int(value) for key, value in coverage.items()},
        legacy_raw_numeric_parsing=raw_parsing,
        sentinels=tuple(sentinels),
        invalid_value_rules=tuple(rules),
        known_legacy_findings=tuple(findings),
        targeted_scale_audits=tuple(targeted_scale_audits),
        row_level_scale_entry_audits=tuple(row_level_scale_entry_audits),
        targeted_comparison_audits=tuple(targeted_comparisons),
        targeted_bucket_audits=tuple(targeted_buckets),
        targeted_review_coverage={
            key: int(value) for key, value in targeted_coverage.items()
        },
        relationship_audits=tuple(relationships),
        availability_audits=tuple(availability),
        source_path=policy_path,
    )
