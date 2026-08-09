from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build import (
    _load_frozen_schemas,
    _read_json,
    load_harmonized_build_policy,
)
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
    load_harmonized_build_audit_policy,
)
from asic_pipeline.harmonization.harmonized_promotion import (
    HarmonizedPromotionConfig,
    load_harmonized_promotion_config,
    load_harmonized_promotion_policy,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


@dataclass(frozen=True)
class CleaningRule:
    rule_id: str
    table: str
    variable: str
    hard_min: float | None
    hard_max: float | None
    invalid_zero: bool
    allow_power_of_ten_repair: bool
    source: str


@dataclass(frozen=True)
class ListCleaningRule:
    rule_id: str
    table: str
    variable: str
    element_hard_min: float
    element_hard_max: float


@dataclass(frozen=True)
class HospitalMaskRule:
    rule_id: str
    hospital: str
    table: str
    variable: str


@dataclass(frozen=True)
class CleaningPolicy:
    version: str
    harmonized_release_id: str
    harmonized_contract_version: str
    promotion_policy_path: Path
    promotion_policy_sha256: str
    quality_policy_path: Path
    quality_policy_sha256: str
    expected_legacy_expanded_count: int
    expected_available_count: int
    unavailable_variables: tuple[str, ...]
    expected_applied_legacy_count: int
    expected_deferred_legacy_count: int
    repair_factors: tuple[float, ...]
    additional_rules: tuple[CleaningRule, ...]
    list_rules: tuple[ListCleaningRule, ...]
    hospital_masks: tuple[HospitalMaskRule, ...]
    globally_all_missing: tuple[str, ...]
    rows_per_batch: int
    compression: str
    expected_rows: tuple[tuple[str, int], ...]
    candidate_directory_name: str
    candidate_artifact_version: str
    build_review_directory_name: str
    audit_private_directory_name: str
    audit_review_directory_name: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class CleaningConfig:
    promotion: HarmonizedPromotionConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.promotion.dataset_context

    @property
    def data_root(self) -> Path:
        return self.promotion.data_root

    @property
    def reports_root(self) -> Path:
        return self.promotion.reports_root


@dataclass(frozen=True)
class CleaningResult:
    run_id: str
    candidate_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class HarmonizedReleaseInput:
    release_directory: Path
    release_manifest_path: Path
    release_manifest: dict[str, Any]
    frozen_schemas: dict[str, pa.Schema]
    source_files: dict[str, Path]


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _optional_number(value: Any, location: str) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigurationError(f"{location} must be numeric or null")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ConfigurationError(f"{location} must be finite")
    return numeric


def _cleaning_rule(value: Any, table: str, location: str) -> CleaningRule:
    row = _mapping(value, location)
    expected = {
        "id",
        "variable",
        "hard_min",
        "hard_max",
        "invalid_zero",
        "allow_power_of_ten_repair",
    }
    if set(row) - expected:
        raise ConfigurationError(f"{location} contains unknown fields")
    hard_min = _optional_number(row.get("hard_min"), f"{location}.hard_min")
    hard_max = _optional_number(row.get("hard_max"), f"{location}.hard_max")
    if hard_min is None and hard_max is None and row.get("invalid_zero") is not True:
        raise ConfigurationError(f"{location} has no cleaning predicate")
    if hard_min is not None and hard_max is not None and hard_min > hard_max:
        raise ConfigurationError(f"{location} has reversed bounds")
    if not isinstance(row.get("invalid_zero"), bool) or not isinstance(
        row.get("allow_power_of_ten_repair"), bool
    ):
        raise ConfigurationError(f"{location} Boolean fields are invalid")
    return CleaningRule(
        rule_id=required_string(row, "id", location),
        table=table,
        variable=required_string(row, "variable", location),
        hard_min=hard_min,
        hard_max=hard_max,
        invalid_zero=bool(row["invalid_zero"]),
        allow_power_of_ten_repair=bool(row["allow_power_of_ten_repair"]),
        source="v3_cleaning_extension",
    )


def load_cleaning_config(path: str | Path) -> CleaningConfig:
    source = Path(path).expanduser().resolve()
    promotion = load_harmonized_promotion_config(source)
    raw = load_yaml_mapping(source, "Cleaning configuration")
    policy_path = resolve_path(required_string(raw, "cleaning_policy", "config"), source)
    return CleaningConfig(promotion=promotion, policy_path=policy_path)


def load_cleaning_policy(path: str | Path) -> CleaningPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning policy")
    if raw.get("cleaning_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_nonpublishable_cleaning_candidate_and_complete_audit":
        raise ConfigurationError("Cleaning candidate policy is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    quality = _mapping(raw.get("native_v3_quality_registry"), "native_v3_quality_registry")
    numeric = _mapping(raw.get("numeric_policy"), "numeric_policy")
    repair = _mapping(numeric.get("power_of_ten_repair"), "numeric_policy.power_of_ten_repair")
    expected_numeric = {
        "mask_nonfinite_values_in_every_float64_field": True,
        "range_bounds_are_inclusive": True,
        "invalid_zero_is_applied_after_range_test": True,
        "unresolved_unit_rules_are_audit_only": True,
        "preserve_finite_values_when_unit_is_unresolved": True,
    }
    if {key: numeric.get(key) for key in expected_numeric} != expected_numeric:
        raise ConfigurationError("Cleaning numeric boundary changed")
    expected_repair = {
        "enabled_for_applied_rules_with_both_bounds": True,
        "require_original_value_outside_rule": True,
        "require_exactly_one_in_range_factor": True,
        "no_in_range_factor_action": "set_missing",
        "multiple_in_range_factors_action": "set_missing",
        "record_factor_level_counts": True,
    }
    if {key: repair.get(key) for key in expected_repair} != expected_repair:
        raise ConfigurationError("Power-of-ten cleaning boundary changed")
    factors_raw = repair.get("factors")
    if not isinstance(factors_raw, list) or factors_raw != [0.001, 0.01, 0.1, 10, 100, 1000]:
        raise ConfigurationError("Power-of-ten repair factors changed")
    additions = _mapping(raw.get("additional_scalar_rules"), "additional_scalar_rules")
    additional_rules: list[CleaningRule] = []
    for table in ("static", "dynamic"):
        rows = additions.get(table)
        if not isinstance(rows, list):
            raise ConfigurationError(f"additional_scalar_rules.{table} must be a list")
        additional_rules.extend(
            _cleaning_rule(row, table, f"additional_scalar_rules.{table}[{index}]")
            for index, row in enumerate(rows)
        )
    list_rows = raw.get("list_rules")
    if not isinstance(list_rows, list) or len(list_rows) != 1:
        raise ConfigurationError("Exactly one reviewed list cleaning rule is required")
    list_row = _mapping(list_rows[0], "list_rules[0]")
    expected_list_options = {
        "remove_null_elements": True,
        "remove_nonfinite_elements": True,
        "remove_out_of_range_elements": True,
        "preserve_source_list_order": True,
        "preserve_duplicates": True,
        "preserve_empty_list_when_all_elements_removed": True,
        "apply_power_of_ten_repair": False,
    }
    if {key: list_row.get(key) for key in expected_list_options} != expected_list_options:
        raise ConfigurationError("Height-list cleaning boundary changed")
    list_rule = ListCleaningRule(
        rule_id=required_string(list_row, "id", "list_rules[0]"),
        table=required_string(list_row, "table", "list_rules[0]"),
        variable=required_string(list_row, "variable", "list_rules[0]"),
        element_hard_min=float(list_row.get("element_hard_min")),
        element_hard_max=float(list_row.get("element_hard_max")),
    )
    if (
        list_rule.table != "static"
        or list_rule.variable != "height_measurements_cm"
        or list_rule.element_hard_min != 120
        or list_rule.element_hard_max != 220
    ):
        raise ConfigurationError("Height-list cleaning rule changed")
    mask_rows = raw.get("hospital_scoped_masks")
    if not isinstance(mask_rows, list) or len(mask_rows) != 1:
        raise ConfigurationError("Exactly one reviewed hospital mask is required")
    mask_row = _mapping(mask_rows[0], "hospital_scoped_masks[0]")
    if mask_row.get("action") != "set_all_site_values_missing":
        raise ConfigurationError("Hospital-scoped mask action changed")
    hospital_mask = HospitalMaskRule(
        rule_id=required_string(mask_row, "id", "hospital_scoped_masks[0]"),
        hospital=required_string(mask_row, "hospital", "hospital_scoped_masks[0]"),
        table=required_string(mask_row, "table", "hospital_scoped_masks[0]"),
        variable=required_string(mask_row, "variable", "hospital_scoped_masks[0]"),
    )
    if (
        hospital_mask.hospital,
        hospital_mask.table,
        hospital_mask.variable,
    ) != ("asic_UK06", "dynamic", "vt_per_kg_ideal_body_weight"):
        raise ConfigurationError("Hospital-scoped mask scope changed")
    preservation = _mapping(raw.get("preservation"), "preservation")
    all_missing = preservation.get("globally_all_missing_columns")
    expected_all_missing = [
        "feo2",
        "severity_read_confirmation",
        "sofa_score_without_gcs",
        "stroke_volume_bolus",
    ]
    if (
        not all(preservation.get(key) is True for key in (
            "preserve_all_rows_and_stays",
            "preserve_column_order_and_physical_types",
            "preserve_all_five_operational_provenance_fields",
            "preserve_globally_all_missing_columns",
            "do_not_drop_columns",
        ))
        or all_missing != expected_all_missing
    ):
        raise ConfigurationError("Cleaning preservation contract changed")
    boundary = _mapping(raw.get("boundary"), "boundary")
    expected_boundary = {
        "read_released_harmonized_data": True,
        "modify_harmonized_release": False,
        "overwrite_outputs": False,
        "filter_rows": False,
        "filter_stays": False,
        "aggregate_height_measurements": False,
        "derive_variables": False,
        "define_cohorts": False,
        "perform_time_blocking": False,
        "publish_cleaned_release": False,
        "authorize_external_data_export": False,
    }
    if boundary != expected_boundary:
        raise ConfigurationError("Cleaning stage boundary changed")
    promotion_path = resolve_path(
        required_string(input_raw, "harmonized_promotion_policy", "input"), source
    )
    promotion_hash = required_string(
        input_raw, "harmonized_promotion_policy_sha256", "input"
    )
    quality_path = resolve_path(required_string(quality, "path", "native_v3_quality_registry"), source)
    quality_hash = required_string(quality, "sha256", "native_v3_quality_registry")
    if sha256_file(promotion_path) != promotion_hash:
        raise ConfigurationError("Approved harmonized promotion policy changed")
    if sha256_file(quality_path) != quality_hash:
        raise ConfigurationError("Native v3 quality registry changed")
    streaming = _mapping(raw.get("streaming"), "streaming")
    rows = _mapping(raw.get("expected_rows"), "expected_rows")
    outputs = _mapping(raw.get("outputs"), "outputs")
    unavailable = quality.get("source_unavailable_variables")
    if not isinstance(unavailable, list) or any(not isinstance(item, str) for item in unavailable):
        raise ConfigurationError("Unavailable cleaning variables are invalid")
    policy = CleaningPolicy(
        version="0.1",
        harmonized_release_id=required_string(input_raw, "harmonized_release_id", "input"),
        harmonized_contract_version=required_string(input_raw, "harmonized_contract_version", "input"),
        promotion_policy_path=promotion_path,
        promotion_policy_sha256=promotion_hash,
        quality_policy_path=quality_path,
        quality_policy_sha256=quality_hash,
        expected_legacy_expanded_count=_positive_int(quality.get("legacy_expanded_rule_count"), "native_v3_quality_registry.legacy_expanded_rule_count"),
        expected_available_count=_positive_int(quality.get("source_available_rule_count"), "native_v3_quality_registry.source_available_rule_count"),
        unavailable_variables=tuple(unavailable),
        expected_applied_legacy_count=_positive_int(quality.get("approved_unit_rule_count"), "native_v3_quality_registry.approved_unit_rule_count"),
        expected_deferred_legacy_count=_positive_int(quality.get("unresolved_unit_deferred_rule_count"), "native_v3_quality_registry.unresolved_unit_deferred_rule_count"),
        repair_factors=tuple(float(item) for item in factors_raw),
        additional_rules=tuple(additional_rules),
        list_rules=(list_rule,),
        hospital_masks=(hospital_mask,),
        globally_all_missing=tuple(expected_all_missing),
        rows_per_batch=_positive_int(streaming.get("rows_per_batch"), "streaming.rows_per_batch"),
        compression=required_string(streaming, "parquet_compression", "streaming"),
        expected_rows=tuple((table, _positive_int(rows.get(table), f"expected_rows.{table}")) for table in ("static", "dynamic")),
        candidate_directory_name=required_string(outputs, "candidate_directory_name", "outputs"),
        candidate_artifact_version=required_string(outputs, "candidate_artifact_version", "outputs"),
        build_review_directory_name=required_string(outputs, "build_review_directory_name", "outputs"),
        audit_private_directory_name=required_string(outputs, "audit_private_directory_name", "outputs"),
        audit_review_directory_name=required_string(outputs, "audit_review_directory_name", "outputs"),
        audit_private_artifact_version=required_string(outputs, "audit_private_artifact_version", "outputs"),
        audit_review_artifact_version=required_string(outputs, "audit_review_artifact_version", "outputs"),
        source_path=source,
    )
    promotion = load_harmonized_promotion_policy(promotion_path)
    if (
        promotion.release_id != policy.harmonized_release_id
        or promotion.frozen_contract_version != policy.harmonized_contract_version
    ):
        raise ConfigurationError("Cleaning input differs from approved harmonized release")
    return policy


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


def _bool_count(mask: pa.Array) -> int:
    value = pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py()
    return int(value or 0)


def _inside_range(values: pa.Array, rule: CleaningRule) -> pa.Array:
    finite = pc.fill_null(pc.is_finite(values), False)
    inside = finite
    if rule.hard_min is not None:
        inside = pc.and_(inside, pc.greater_equal(values, rule.hard_min))
    if rule.hard_max is not None:
        inside = pc.and_(inside, pc.less_equal(values, rule.hard_max))
    if rule.invalid_zero:
        inside = pc.and_(inside, pc.not_equal(values, 0.0))
    return pc.fill_null(inside, False)


def _clean_numeric_array(
    source: pa.Array,
    rule: CleaningRule,
    factors: tuple[float, ...],
    counts: Counter[str],
) -> pa.Array:
    values = pc.cast(source, pa.float64(), safe=True)
    valid = pc.is_valid(values)
    finite = pc.fill_null(pc.is_finite(values), False)
    inside = _inside_range(values, rule)
    invalid = pc.and_(finite, pc.invert(inside))
    counts["input_null"] += source.null_count
    counts["masked_nonfinite"] += _bool_count(pc.and_(valid, pc.invert(finite)))
    counts["kept_in_range"] += _bool_count(inside)
    output = values
    uniquely_repaired = pa.repeat(pa.scalar(False, pa.bool_()), len(source))
    candidate_total = pa.repeat(pa.scalar(0, pa.int8()), len(source))
    candidate_masks: list[tuple[float, pa.Array, pa.Array]] = []
    if (
        rule.allow_power_of_ten_repair
        and rule.hard_min is not None
        and rule.hard_max is not None
    ):
        for factor in factors:
            transformed = pc.multiply(values, pa.scalar(factor, pa.float64()))
            mask = pc.and_(invalid, _inside_range(transformed, rule))
            candidate_masks.append((factor, mask, transformed))
            candidate_total = pc.add(candidate_total, pc.cast(mask, pa.int8()))
        uniquely_repaired = pc.equal(candidate_total, 1)
        for factor, mask, transformed in candidate_masks:
            selected = pc.and_(uniquely_repaired, mask)
            count = _bool_count(selected)
            if count:
                counts[f"corrected_factor_{factor:g}"] += count
            output = pc.if_else(selected, transformed, output)
    ambiguous = pc.and_(invalid, pc.greater(candidate_total, 1))
    no_repair = pc.and_(invalid, pc.equal(candidate_total, 0))
    counts["masked_ambiguous_power_of_ten"] += _bool_count(ambiguous)
    counts["masked_out_of_range"] += _bool_count(no_repair)
    keep = pc.or_(inside, uniquely_repaired)
    return pc.if_else(keep, output, pa.scalar(None, pa.float64()))


def _mask_nonfinite_only(
    source: pa.Array,
    counts: Counter[str],
    counter_key: str,
) -> pa.Array:
    values = pc.cast(source, pa.float64(), safe=True)
    finite = pc.fill_null(pc.is_finite(values), False)
    nonfinite = pc.and_(pc.is_valid(values), pc.invert(finite))
    counts[counter_key] += _bool_count(nonfinite)
    return pc.if_else(finite, values, pa.scalar(None, pa.float64()))


def _audit_deferred_rule(
    source: pa.Array,
    rule: CleaningRule,
    counts: Counter[str],
) -> None:
    values = pc.cast(source, pa.float64(), safe=True)
    finite = pc.fill_null(pc.is_finite(values), False)
    inside = _inside_range(values, rule)
    counts["finite_value_count"] += _bool_count(finite)
    counts["audit_only_outside_legacy_range_count"] += _bool_count(
        pc.and_(finite, pc.invert(inside))
    )


def _clean_height_list(
    source: pa.Array,
    rule: ListCleaningRule,
    counts: Counter[str],
) -> pa.Array:
    output: list[list[float] | None] = []
    for cell in source.to_pylist():
        if cell is None:
            counts["input_null_cells"] += 1
            output.append(None)
            continue
        cleaned: list[float] = []
        for item in cell:
            counts["input_element_count"] += 1
            if item is None:
                counts["removed_null_element_count"] += 1
                continue
            numeric = float(item)
            if not math.isfinite(numeric):
                counts["removed_nonfinite_element_count"] += 1
                continue
            if numeric < rule.element_hard_min or numeric > rule.element_hard_max:
                counts["removed_out_of_range_element_count"] += 1
                continue
            cleaned.append(numeric)
            counts["retained_element_count"] += 1
        if not cleaned and cell:
            counts["cells_empty_after_cleaning"] += 1
        output.append(cleaned)
    return pa.array(output, type=pa.list_(pa.float64()))


def _cleaned_schema(schema: pa.Schema, contract_version: str) -> pa.Schema:
    return pa.schema(
        list(schema),
        metadata={
            b"asic_v3_stage": b"cleaned_candidate",
            b"asic_v3_harmonized_contract_version": contract_version.encode(),
            b"asic_v3_cleaning_policy_version": b"0.1",
            b"publication_ready": b"false",
            b"cleaning_applied": b"true",
            b"derivation_applied": b"false",
        },
    )


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _load_rule_registry(
    policy: CleaningPolicy,
    schemas: dict[str, pa.Schema],
) -> tuple[dict[tuple[str, str], CleaningRule], dict[tuple[str, str], CleaningRule]]:
    quality = load_yaml_mapping(policy.quality_policy_path, "Native v3 quality registry")
    rows = quality.get("legacy_invalid_value_rules")
    if not isinstance(rows, list):
        raise HarmonizationError("Legacy cleaning rule evidence is unavailable")
    applied: dict[tuple[str, str], CleaningRule] = {}
    deferred: dict[tuple[str, str], CleaningRule] = {}
    unavailable: list[str] = []
    expanded_count = 0
    for index, value in enumerate(rows):
        row = _mapping(value, f"legacy_invalid_value_rules[{index}]")
        columns = row.get("columns")
        if not isinstance(columns, list) or any(not isinstance(item, str) for item in columns):
            raise HarmonizationError("Legacy cleaning columns are malformed")
        for variable in columns:
            expanded_count += 1
            field_index = schemas["dynamic"].get_field_index(variable)
            if field_index < 0:
                unavailable.append(variable)
                continue
            field = schemas["dynamic"].field(field_index)
            if field.type != pa.float64():
                raise HarmonizationError("A legacy cleaning target is not float64")
            unit = (field.metadata or {}).get(b"asic_v3_unit", b"").decode()
            rule = CleaningRule(
                rule_id=f"LEGACY-{required_string(row, 'legacy_name', 'legacy rule').upper().replace('_', '-')}-{variable.upper().replace('_', '-')}",
                table="dynamic",
                variable=variable,
                hard_min=_optional_number(row.get("hard_min"), "legacy hard_min"),
                hard_max=_optional_number(row.get("hard_max"), "legacy hard_max"),
                invalid_zero=row.get("invalid_zero") is True,
                allow_power_of_ten_repair=(
                    row.get("hard_min") is not None and row.get("hard_max") is not None
                ),
                source="selectively_migrated_legacy_rule_revalidated_in_v3",
            )
            target = deferred if unit == "unresolved" else applied
            key = (rule.table, rule.variable)
            if key in applied or key in deferred:
                raise HarmonizationError("Duplicate legacy cleaning target")
            target[key] = rule
    if (
        expanded_count != policy.expected_legacy_expanded_count
        or len(applied) + len(deferred) != policy.expected_available_count
        or len(applied) != policy.expected_applied_legacy_count
        or len(deferred) != policy.expected_deferred_legacy_count
        or tuple(unavailable) != policy.unavailable_variables
    ):
        raise HarmonizationError("Legacy cleaning rule accounting changed")
    for rule in policy.additional_rules:
        key = (rule.table, rule.variable)
        if key in applied or key in deferred:
            raise HarmonizationError("Cleaning extension duplicates another rule")
        index = schemas[rule.table].get_field_index(rule.variable)
        if index < 0 or schemas[rule.table].field(index).type != pa.float64():
            raise HarmonizationError("Cleaning extension target is unavailable")
        applied[key] = rule
    return applied, deferred


def load_harmonized_release_input(
    config: CleaningConfig,
    policy: CleaningPolicy,
) -> HarmonizedReleaseInput:
    if config.promotion.policy_path.resolve() != policy.promotion_policy_path.resolve():
        raise HarmonizationError("Dataset and cleaning promotion policies disagree")
    promotion = load_harmonized_promotion_policy(policy.promotion_policy_path)
    audit_policy = load_harmonized_build_audit_policy(promotion.audit_policy_path)
    build_policy = load_harmonized_build_policy(audit_policy.build_policy_path)
    freeze_policy = load_schema_dictionary_freeze_policy(build_policy.freeze_policy_path)
    frozen_dir = config.data_root / build_policy.frozen_contract_directory
    schemas, _ = _load_frozen_schemas(frozen_dir, freeze_policy)
    if freeze_policy.contract_version != policy.harmonized_contract_version:
        raise HarmonizationError("Cleaning frozen contract version changed")
    pointer_path = config.data_root / promotion.current_release_pointer
    pointer = _read_json(pointer_path, "Current harmonized release pointer")
    if not isinstance(pointer, dict):
        raise HarmonizationError("Current harmonized release pointer is invalid")
    release_dir = config.data_root / promotion.releases_directory / policy.harmonized_release_id
    manifest_path = release_dir / "release_manifest.json"
    if (
        pointer.get("artifact") != "asic_v3_current_harmonized_release"
        or pointer.get("release_id") != policy.harmonized_release_id
        or pointer.get("frozen_contract_version") != policy.harmonized_contract_version
        or pointer.get("harmonized_layer_ready") is not True
        or pointer.get("cleaning_input_approved") is not True
        or pointer.get("publication_ready") is not True
        or pointer.get("external_data_export_authorized") is not False
        or Path(str(pointer.get("release_manifest", ""))).resolve() != manifest_path.resolve()
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
    ):
        raise HarmonizationError("Current harmonized release pointer changed")
    manifest = _read_json(manifest_path, "Harmonized release manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Harmonized release manifest is invalid")
    if (
        manifest.get("artifact") != "asic_v3_harmonized_release"
        or manifest.get("release_id") != policy.harmonized_release_id
        or manifest.get("status") != "released_harmonized_layer"
        or manifest.get("frozen_contract_version") != policy.harmonized_contract_version
        or manifest.get("payload_byte_identity_preserved") is not True
        or manifest.get("cleaning_input_approved") is not True
        or manifest.get("publication_ready") is not True
        or manifest.get("cleaning_applied") is not False
        or manifest.get("derivation_applied") is not False
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved harmonized release changed")
    source_files: dict[str, Path] = {}
    summaries = manifest.get("files")
    if not isinstance(summaries, dict):
        raise HarmonizationError("Harmonized release files are unavailable")
    for table in ("static", "dynamic"):
        path = release_dir / f"{table}.parquet"
        summary = summaries.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Harmonized release table is unavailable")
        parquet = pq.ParquetFile(path)
        if (
            summary.get("sha256") != sha256_file(path)
            or summary.get("row_count") != dict(policy.expected_rows)[table]
            or parquet.metadata.num_rows != dict(policy.expected_rows)[table]
            or not _parquet_schema_matches(parquet.schema_arrow, schemas[table])
            or summary.get("schema_sha256") != _schema_digest(schemas[table])
        ):
            raise HarmonizationError("Harmonized release table changed")
        source_files[table] = path
    return HarmonizedReleaseInput(
        release_directory=release_dir,
        release_manifest_path=manifest_path,
        release_manifest=manifest,
        frozen_schemas=schemas,
        source_files=source_files,
    )


def transform_cleaning_batch(
    batch: pa.RecordBatch,
    table: str,
    output_schema: pa.Schema,
    policy: CleaningPolicy,
    applied_rules: dict[tuple[str, str], CleaningRule],
    deferred_rules: dict[tuple[str, str], CleaningRule],
    rule_counts: dict[str, Counter[str]],
    deferred_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    mask_counts: dict[str, Counter[str]],
) -> pa.RecordBatch:
    hospital_index = batch.schema.get_field_index("hospital_id")
    if hospital_index < 0:
        raise HarmonizationError("Cleaning input has no hospital identifier")
    hospitals = batch.column(hospital_index)
    arrays: list[pa.Array] = []
    masks_by_variable = {
        item.variable: item
        for item in policy.hospital_masks
        if item.table == table
    }
    list_by_variable = {
        item.variable: item for item in policy.list_rules if item.table == table
    }
    for field in output_schema:
        index = batch.schema.get_field_index(field.name)
        if index < 0:
            raise HarmonizationError(f"Cleaning input field is missing: {table}.{field.name}")
        source = batch.column(index)
        mask_rule = masks_by_variable.get(field.name)
        if mask_rule is not None:
            site = pc.fill_null(pc.equal(hospitals, mask_rule.hospital), False)
            masked_nonnull = pc.and_(site, pc.is_valid(source))
            mask_counts[mask_rule.rule_id]["site_non_null_values_masked"] += _bool_count(masked_nonnull)
            mask_counts[mask_rule.rule_id]["site_row_count"] += _bool_count(site)
            source = pc.if_else(site, pa.scalar(None, type=field.type), source)
        key = (table, field.name)
        if field.name in list_by_variable:
            transformed = _clean_height_list(
                source,
                list_by_variable[field.name],
                list_counts[list_by_variable[field.name].rule_id],
            )
        elif key in applied_rules:
            rule = applied_rules[key]
            transformed = _clean_numeric_array(
                source, rule, policy.repair_factors, rule_counts[rule.rule_id]
            )
        elif key in deferred_rules:
            rule = deferred_rules[key]
            _audit_deferred_rule(source, rule, deferred_counts[rule.rule_id])
            transformed = _mask_nonfinite_only(
                source, nonfinite_counts, f"{table}.{field.name}"
            )
        elif field.type == pa.float64():
            transformed = _mask_nonfinite_only(
                source, nonfinite_counts, f"{table}.{field.name}"
            )
        else:
            transformed = source
        if transformed.type != field.type:
            transformed = pc.cast(transformed, field.type, safe=True)
        arrays.append(transformed)
    return pa.RecordBatch.from_arrays(arrays, schema=output_schema)


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    return "\n".join(
        (
            "# ASIC v3 cleaned candidate build review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Cleaning run: `{payload['run_id']}`",
            f"- Input harmonized release: `{payload['harmonized_release_id']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Applied scalar rules: `{metrics['applied_scalar_rule_count']}`",
            f"- Unit-unresolved audit-only rules: `{metrics['deferred_rule_count']}`",
            f"- Power-of-ten corrections: `{metrics['power_of_ten_correction_count']}`",
            f"- Out-of-range values masked: `{metrics['range_mask_count']}`",
            f"- Nonfinite values masked: `{metrics['nonfinite_mask_count']}`",
            f"- Height elements removed: `{metrics['height_element_removal_count']}`",
            f"- UK06 mixed-scale values masked: `{metrics['hospital_mask_count']}`",
            "- Rows or stays filtered: `false`",
            "- Columns dropped: `false`",
            "- Derivation applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Human review gate",
            "",
            "Run the independent complete cleaning audit and review the rule-level counts before approving a cleaned release.",
            "",
        )
    )


def run_cleaning_candidate_build(
    config: CleaningConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> CleaningResult:
    policy = load_cleaning_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved cleaning candidate build is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid cleaning run ID")
    source = load_harmonized_release_input(config, policy)
    applied, deferred = _load_rule_registry(policy, source.frozen_schemas)
    output_schemas = {
        table: _cleaned_schema(source.frozen_schemas[table], policy.harmonized_contract_version)
        for table in ("static", "dynamic")
    }
    output_parent = config.data_root / policy.candidate_directory_name
    final_output = output_parent / selected_run
    staging_output = output_parent / f".{selected_run}.incomplete"
    review_dir = config.reports_root / "review" / policy.build_review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (final_output, staging_output, review_json, review_md)):
        raise HarmonizationError("Cleaning candidate run already exists")
    staging_output.mkdir(parents=True, mode=0o700)
    staging_output.chmod(0o700)
    rule_counts = {rule.rule_id: Counter() for rule in applied.values()}
    deferred_counts = {rule.rule_id: Counter() for rule in deferred.values()}
    nonfinite_counts: Counter[str] = Counter()
    list_counts = {rule.rule_id: Counter() for rule in policy.list_rules}
    mask_counts = {rule.rule_id: Counter() for rule in policy.hospital_masks}
    row_counts: Counter[str] = Counter()
    all_missing_non_null: Counter[str] = Counter()
    output_files: dict[str, dict[str, Any]] = {}
    for table in ("static", "dynamic"):
        input_path = source.source_files[table]
        input_parquet = pq.ParquetFile(input_path)
        output_path = staging_output / f"{table}.parquet"
        schema = output_schemas[table]
        with pq.ParquetWriter(output_path, schema, compression=policy.compression) as writer:
            for batch in input_parquet.iter_batches(
                batch_size=policy.rows_per_batch, use_threads=True
            ):
                output_batch = transform_cleaning_batch(
                    batch,
                    table,
                    schema,
                    policy,
                    applied,
                    deferred,
                    rule_counts,
                    deferred_counts,
                    nonfinite_counts,
                    list_counts,
                    mask_counts,
                )
                writer.write_batch(output_batch)
                row_counts[table] += batch.num_rows
                for variable in policy.globally_all_missing:
                    index = output_batch.schema.get_field_index(variable)
                    if index >= 0:
                        array = output_batch.column(index)
                        all_missing_non_null[variable] += len(array) - array.null_count
                if progress is not None and row_counts[table] % (policy.rows_per_batch * 20) == 0:
                    progress(f"cleaning_build_progress table={table} rows={row_counts[table]}")
        output_path.chmod(0o600)
        if row_counts[table] != dict(policy.expected_rows)[table]:
            raise HarmonizationError("Cleaning changed the approved row count")
        if (
            sha256_file(input_path)
            != source.release_manifest["files"][table]["sha256"]
        ):
            raise HarmonizationError(
                "Harmonized release changed during cleaning candidate build"
            )
        output_files[table] = {
            "path": str(final_output / output_path.name),
            "sha256": sha256_file(output_path),
            "row_count": row_counts[table],
            "schema_sha256": _schema_digest(schema),
        }
        if progress is not None:
            progress(f"cleaning_build_table_complete={table}")
    if any(all_missing_non_null.values()):
        raise HarmonizationError("A reviewed globally all-missing column changed")
    corrections = sum(
        count
        for counts in rule_counts.values()
        for key, count in counts.items()
        if key.startswith("corrected_factor_")
    )
    range_masks = sum(
        counts["masked_out_of_range"] + counts["masked_ambiguous_power_of_ten"]
        for counts in rule_counts.values()
    )
    nonfinite_masks = sum(counts["masked_nonfinite"] for counts in rule_counts.values()) + sum(nonfinite_counts.values())
    height_removals = sum(
        counts["removed_null_element_count"]
        + counts["removed_nonfinite_element_count"]
        + counts["removed_out_of_range_element_count"]
        for counts in list_counts.values()
    )
    hospital_masks_total = sum(
        counts["site_non_null_values_masked"] for counts in mask_counts.values()
    )
    generated = utc_timestamp()
    manifest_path = staging_output / "cleaning_manifest.json"
    manifest = {
        "artifact": "asic_v3_cleaning_candidate",
        "artifact_version": policy.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "status": "nonpublishable_requires_complete_cleaning_audit",
        "lineage": {
            "harmonized_release_id": policy.harmonized_release_id,
            "harmonized_release_manifest_sha256": sha256_file(source.release_manifest_path),
            "harmonized_contract_version": policy.harmonized_contract_version,
            "cleaning_policy_sha256": sha256_file(policy.source_path),
            "v3_quality_registry_sha256": policy.quality_policy_sha256,
        },
        "outputs": output_files,
        "rule_accounting": {
            "applied_scalar_rules": {key: dict(sorted(value.items())) for key, value in sorted(rule_counts.items())},
            "unit_unresolved_audit_only_rules": {key: dict(sorted(value.items())) for key, value in sorted(deferred_counts.items())},
            "nonfinite_only_fields": dict(sorted(nonfinite_counts.items())),
            "list_rules": {key: dict(sorted(value.items())) for key, value in sorted(list_counts.items())},
            "hospital_masks": {key: dict(sorted(value.items())) for key, value in sorted(mask_counts.items())},
        },
        "rule_registry": {
            "applied_scalar_rule_count": len(applied),
            "unit_unresolved_audit_only_rule_count": len(deferred),
            "unavailable_derived_variables": list(policy.unavailable_variables),
            "globally_all_missing_columns_preserved": list(policy.globally_all_missing),
        },
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "columns_dropped": False,
        "height_aggregation_applied": False,
        "cleaning_applied": True,
        "derivation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging_output.replace(final_output)
    final_output.chmod(0o700)
    manifest_path = final_output / manifest_path.name
    review_payload = {
        "artifact": "asic_v3_cleaning_build_review",
        "artifact_version": policy.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "harmonized_release_id": policy.harmonized_release_id,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": dict(row_counts),
        "metrics": {
            "applied_scalar_rule_count": len(applied),
            "deferred_rule_count": len(deferred),
            "power_of_ten_correction_count": corrections,
            "range_mask_count": range_masks,
            "nonfinite_mask_count": nonfinite_masks,
            "height_element_removal_count": height_removals,
            "hospital_mask_count": hospital_masks_total,
        },
        "rows_or_stays_filtered": False,
        "columns_dropped": False,
        "cleaning_applied": True,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_markdown(review_md, _markdown(review_payload), 0o640)
    return CleaningResult(
        run_id=selected_run,
        candidate_directory=final_output,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
