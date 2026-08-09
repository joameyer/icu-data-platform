"""Translate pooled ASIC column names and apply approved source aliases."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline import __version__
from asic_pipeline.contracts import InputContract, load_input_contract
from asic_pipeline.contracts.pooled import TableContract
from asic_pipeline.errors import InputReadError
from asic_pipeline.translations.config import TranslationConfig
from asic_pipeline.translations.registry import (
    CategoricalValueMapping,
    DropRule,
    HospitalSpecificSemanticSplitRule,
    MERGE_POLICY_BINARY_OR,
    MERGE_POLICY_FAIL_ON_UNEQUAL,
    MergeRule,
    NumericMissingSentinelRule,
    TranslationEntry,
    TranslationRegistry,
    load_translation_registry,
    normalize_categorical_source_value,
)


ProgressReporter = Callable[[str], None]


@dataclass(frozen=True)
class TranslationResult:
    static_path: Path
    dynamic_path: Path
    manifest_path: Path
    static_row_count: int
    dynamic_row_count: int
    static_column_count: int
    dynamic_column_count: int
    audit_status: str


@dataclass
class _MergeMetrics:
    source_non_missing: Counter[str] = field(default_factory=Counter)
    source_non_missing_by_hospital: dict[str, Counter[int]] = field(
        default_factory=dict
    )
    overlap_rows: int = 0
    conflict_rows: int = 0
    output_non_missing: int = 0
    disallowed_hospital_rows: Counter[str] = field(default_factory=Counter)
    invalid_binary_rows: Counter[str] = field(default_factory=Counter)
    resolved_conflict_rows: int = 0


@dataclass
class _ValueMappingMetrics:
    source_non_missing: int = 0
    output_non_missing: int = 0
    mapped_to_missing: int = 0
    normalized_source_counts: Counter[str] = field(default_factory=Counter)


@dataclass
class _NumericMissingSentinelMetrics:
    source_non_missing: int = 0
    output_non_missing: int = 0
    sentinel_counts: Counter[int | float] = field(default_factory=Counter)


@dataclass
class _HospitalSpecificSemanticSplitMetrics:
    source_non_missing: int = 0
    selected_rows: int = 0
    selected_source_non_missing: int = 0
    nonselected_source_non_missing: int = 0
    base_output_non_missing: int = 0
    base_output_non_missing_in_selected_hospitals: int = 0
    new_output_non_missing: int = 0
    new_output_non_missing_outside_selected_hospitals: int = 0


@dataclass
class _TableMetrics:
    input_rows: int = 0
    output_rows: int = 0
    merge: dict[str, _MergeMetrics] = field(default_factory=dict)
    value_mappings: dict[str, _ValueMappingMetrics] = field(default_factory=dict)
    numeric_missing_sentinels: dict[
        str,
        _NumericMissingSentinelMetrics,
    ] = field(default_factory=dict)
    hospital_specific_semantic_splits: dict[
        str,
        _HospitalSpecificSemanticSplitMetrics,
    ] = field(default_factory=dict)
    dropped_non_missing: Counter[str] = field(default_factory=Counter)


def report_progress(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _emit_progress(reporter: ProgressReporter | None, message: str) -> None:
    if reporter is not None:
        reporter(message)


def _assert_parquet_schema(
    path: Path,
    table_contract: TableContract,
    dataset_context: str,
    table_name: str,
) -> pq.ParquetFile:
    if not path.is_file():
        raise InputReadError(f"{table_name} Parquet file does not exist: {path}")
    parquet = pq.ParquetFile(path)
    observed = [(field.name, str(field.type)) for field in parquet.schema_arrow]
    expected = [
        (column.name, column.arrow_type_for(dataset_context))
        for column in table_contract.columns
    ]
    if observed != expected:
        observed_names = [name for name, _ in observed]
        expected_names = [name for name, _ in expected]
        observed_types = dict(observed)
        expected_types = dict(expected)
        missing = [name for name in expected_names if name not in observed_types]
        extra = [name for name in observed_names if name not in expected_types]
        type_mismatches = [
            (name, observed_types[name], expected_types[name])
            for name in expected_names
            if name in observed_types and observed_types[name] != expected_types[name]
        ]
        raise ValueError(
            f"{table_name} schema does not match contract context "
            f"{dataset_context!r}; missing={missing}, extra={extra}, "
            f"column_order_matches={observed_names == expected_names}, "
            f"type_mismatches={type_mismatches}"
        )
    return parquet


def _valid_value_mask(array: pa.Array | pa.ChunkedArray) -> pa.Array:
    values = array.combine_chunks() if isinstance(array, pa.ChunkedArray) else array
    valid = pc.is_valid(values)
    if pa.types.is_floating(values.type):
        valid = pc.and_(valid, pc.invert(pc.is_nan(values)))
    return pc.fill_null(valid, False)


def _normalize_missing(array: pa.Array | pa.ChunkedArray) -> pa.Array:
    values = array.combine_chunks() if isinstance(array, pa.ChunkedArray) else array
    return pc.if_else(
        _valid_value_mask(values),
        values,
        pa.scalar(None, type=values.type),
    )


def _true_count(mask: pa.Array | pa.ChunkedArray) -> int:
    value = pc.sum(pc.cast(mask, pa.int64())).as_py()
    return int(value or 0)


def _counts_by_hospital(
    hospital_codes: pa.Array,
    valid_mask: pa.Array,
) -> Counter[int]:
    filtered = pc.filter(hospital_codes, valid_mask)
    counts: Counter[int] = Counter()
    for item in pc.value_counts(filtered).to_pylist():
        if item["values"] is not None:
            counts[int(item["values"])] += int(item["counts"])
    return counts


def _derive_hospital_id(table: pa.Table, contract: InputContract) -> pa.Array:
    stay_ids = table.column(contract.stay_id_column).combine_chunks()
    hospital_codes = table.column(contract.hospital_id_column).combine_chunks()
    output: list[str] = []
    for stay_id, value in zip(
        stay_ids.to_pylist(),
        hospital_codes.to_pylist(),
        strict=True,
    ):
        if (
            value is None
            or (isinstance(value, float) and not math.isfinite(value))
            or int(value) != value
        ):
            raise ValueError(
                "Cannot derive hospital_id from a missing or non-integral hid value"
            )
        if not isinstance(stay_id, str) or ":" not in stay_id:
            raise ValueError(
                "Cannot derive hospital_id from a missing or malformed Pseudo-ID"
            )
        try:
            authoritative_code = int(stay_id.rsplit(":", 1)[1])
        except ValueError as exc:
            raise ValueError(
                f"Cannot derive hospital_id from malformed Pseudo-ID {stay_id!r}"
            ) from exc
        hospital_code = int(value)
        if authoritative_code != hospital_code:
            raise ValueError(
                "Cannot derive hospital_id because the authoritative Pseudo-ID "
                f"suffix {authoritative_code} disagrees with hid {hospital_code}"
            )
        if authoritative_code not in contract.expected_hospital_codes:
            raise ValueError(
                "Cannot derive hospital_id from unexpected Pseudo-ID suffix "
                f"{authoritative_code}"
            )
        output.append(f"asic_UK{authoritative_code:02d}")
    return pa.array(output, type=pa.string())


def _merge_arrays(
    table: pa.Table,
    rule: MergeRule,
    contract: InputContract,
    metrics: _MergeMetrics,
) -> pa.Array:
    arrays = [
        table.column(source).combine_chunks() for source in rule.source_columns
    ]
    types = {array.type for array in arrays}
    if len(types) != 1:
        raise ValueError(
            f"Approved merge {rule.name!r} has incompatible Arrow types: {types}"
        )
    valid_masks = [_valid_value_mask(array) for array in arrays]
    normalized = [_normalize_missing(array) for array in arrays]
    hospital_codes = table.column(contract.hospital_id_column).combine_chunks()

    for source, array, valid in zip(
        rule.source_columns,
        arrays,
        valid_masks,
        strict=True,
    ):
        non_missing = _true_count(valid)
        metrics.source_non_missing[source] += non_missing
        source_counts = metrics.source_non_missing_by_hospital.setdefault(
            source,
            Counter(),
        )
        source_counts.update(_counts_by_hospital(hospital_codes, valid))
        allowed = rule.allowed_hospitals_for(source)
        if allowed is not None:
            allowed_values = pa.array(allowed, type=hospital_codes.type)
            disallowed = pc.and_(
                valid,
                pc.invert(pc.is_in(hospital_codes, value_set=allowed_values)),
            )
            metrics.disallowed_hospital_rows[source] += _true_count(disallowed)
        if rule.conflict_policy == MERGE_POLICY_BINARY_OR:
            binary_values = pa.array([0, 1], type=arrays[0].type)
            invalid_binary = pc.and_(
                valid,
                pc.invert(pc.is_in(array, value_set=binary_values)),
            )
            metrics.invalid_binary_rows[source] += _true_count(invalid_binary)

    any_valid = valid_masks[0]
    for valid in valid_masks[1:]:
        any_valid = pc.or_(any_valid, valid)
    metrics.output_non_missing += _true_count(any_valid)

    batch_conflict_rows = 0
    if len(arrays) == 2:
        both_valid = pc.and_(valid_masks[0], valid_masks[1])
        equal = pc.fill_null(pc.equal(arrays[0], arrays[1]), False)
        conflicts = pc.and_(both_valid, pc.invert(equal))
        metrics.overlap_rows += _true_count(both_valid)
        batch_conflict_rows = _true_count(conflicts)
        metrics.conflict_rows += batch_conflict_rows
    else:
        for left_index in range(len(arrays) - 1):
            for right_index in range(left_index + 1, len(arrays)):
                both_valid = pc.and_(
                    valid_masks[left_index],
                    valid_masks[right_index],
                )
                equal = pc.fill_null(
                    pc.equal(arrays[left_index], arrays[right_index]),
                    False,
                )
                conflicts = pc.and_(both_valid, pc.invert(equal))
                metrics.overlap_rows += _true_count(both_valid)
                pair_conflicts = _true_count(conflicts)
                batch_conflict_rows += pair_conflicts
                metrics.conflict_rows += pair_conflicts

    if rule.conflict_policy == MERGE_POLICY_BINARY_OR:
        metrics.resolved_conflict_rows += batch_conflict_rows
        one = pa.scalar(1, type=arrays[0].type)
        zero = pa.scalar(0, type=arrays[0].type)
        missing = pa.scalar(None, type=arrays[0].type)
        any_one = pc.and_(
            valid_masks[0],
            pc.fill_null(pc.equal(arrays[0], one), False),
        )
        for array, valid in zip(arrays[1:], valid_masks[1:], strict=True):
            source_is_one = pc.and_(
                valid,
                pc.fill_null(pc.equal(array, one), False),
            )
            any_one = pc.or_(any_one, source_is_one)
        return pc.if_else(
            any_valid,
            pc.if_else(any_one, one, zero),
            missing,
        )
    return pc.coalesce(*normalized)


def _value_mapping_arrow_type(rule: CategoricalValueMapping) -> pa.DataType:
    if rule.output_arrow_type == "large_string":
        return pa.large_string()
    if rule.output_arrow_type == "boolean":
        return pa.bool_()
    raise ValueError(
        f"Unsupported categorical mapping output type: {rule.output_arrow_type}"
    )


def _map_categorical_values(
    array: pa.Array,
    rule: CategoricalValueMapping,
    metrics: _ValueMappingMetrics,
) -> pa.Array:
    if not (pa.types.is_string(array.type) or pa.types.is_large_string(array.type)):
        raise ValueError(
            f"Categorical mapping {rule.table}.{rule.target_name} requires a "
            f"string source, got {array.type}"
        )

    mapping = rule.mapping()
    output: list[str | bool | None] = []
    unexpected: Counter[str] = Counter()
    for value in array.to_pylist():
        if value is None:
            output.append(None)
            continue
        normalized = normalize_categorical_source_value(
            value,
            rule.normalization,
        )
        metrics.source_non_missing += 1
        metrics.normalized_source_counts[normalized] += 1
        if normalized not in mapping:
            unexpected[normalized] += 1
            output.append(None)
            continue
        target = mapping[normalized]
        output.append(target)
        if target is None:
            metrics.mapped_to_missing += 1
        else:
            metrics.output_non_missing += 1

    if unexpected:
        examples = [
            {"value": value, "count": int(count)}
            for value, count in unexpected.most_common(10)
        ]
        raise ValueError(
            f"Categorical mapping {rule.table}.{rule.target_name} found "
            f"{sum(unexpected.values())} unmapped non-missing values: {examples}"
        )
    return pa.array(output, type=_value_mapping_arrow_type(rule))


def _mask_numeric_missing_sentinels(
    array: pa.Array | pa.ChunkedArray,
    rule: NumericMissingSentinelRule,
    metrics: _NumericMissingSentinelMetrics,
) -> pa.Array:
    values = array.combine_chunks() if isinstance(array, pa.ChunkedArray) else array
    if not (
        pa.types.is_integer(values.type) or pa.types.is_floating(values.type)
    ):
        raise ValueError(
            f"Numeric missing sentinel {rule.table}.{rule.target_name} requires "
            f"a numeric source, got {values.type}"
        )

    valid = _valid_value_mask(values)
    metrics.source_non_missing += _true_count(valid)
    sentinel_mask: pa.Array | None = None
    for sentinel in rule.values:
        matches = pc.and_(
            valid,
            pc.fill_null(
                pc.equal(values, pa.scalar(sentinel, type=values.type)),
                False,
            ),
        )
        metrics.sentinel_counts[sentinel] += _true_count(matches)
        sentinel_mask = (
            matches if sentinel_mask is None else pc.or_(sentinel_mask, matches)
        )

    assert sentinel_mask is not None
    output = pc.if_else(
        sentinel_mask,
        pa.scalar(None, type=values.type),
        values,
    )
    metrics.output_non_missing += _true_count(_valid_value_mask(output))
    return output


def _apply_hospital_specific_semantic_split(
    array: pa.Array | pa.ChunkedArray,
    hospital_codes: pa.Array | pa.ChunkedArray,
    rule: HospitalSpecificSemanticSplitRule,
    metrics: _HospitalSpecificSemanticSplitMetrics,
) -> tuple[pa.Array, pa.Array]:
    values = array.combine_chunks() if isinstance(array, pa.ChunkedArray) else array
    hospitals = (
        hospital_codes.combine_chunks()
        if isinstance(hospital_codes, pa.ChunkedArray)
        else hospital_codes
    )
    selected = pc.fill_null(
        pc.is_in(
            hospitals,
            value_set=pa.array(
                rule.selected_hospital_codes,
                type=hospitals.type,
            ),
        ),
        False,
    )
    nonselected = pc.invert(selected)
    valid = _valid_value_mask(values)
    selected_valid = pc.and_(selected, valid)
    nonselected_valid = pc.and_(nonselected, valid)
    missing = pa.scalar(None, type=values.type)
    base_output = pc.if_else(selected, missing, values)
    new_output = pc.if_else(selected, values, missing)

    metrics.source_non_missing += _true_count(valid)
    metrics.selected_rows += _true_count(selected)
    metrics.selected_source_non_missing += _true_count(selected_valid)
    metrics.nonselected_source_non_missing += _true_count(nonselected_valid)
    metrics.base_output_non_missing += _true_count(
        _valid_value_mask(base_output)
    )
    metrics.base_output_non_missing_in_selected_hospitals += _true_count(
        pc.and_(selected, _valid_value_mask(base_output))
    )
    metrics.new_output_non_missing += _true_count(_valid_value_mask(new_output))
    metrics.new_output_non_missing_outside_selected_hospitals += _true_count(
        pc.and_(nonselected, _valid_value_mask(new_output))
    )
    return base_output, new_output


def _translated_schema(
    source_schema: pa.Schema,
    entries: tuple[TranslationEntry, ...],
    merge_rules: tuple[MergeRule, ...],
    value_mapping_rules: tuple[CategoricalValueMapping, ...],
    semantic_split_rules: tuple[HospitalSpecificSemanticSplitRule, ...],
    output_order: tuple[str, ...],
) -> pa.Schema:
    merge_by_target = {rule.target_name: rule for rule in merge_rules}
    value_mapping_by_target = {
        rule.target_name: rule for rule in value_mapping_rules
    }
    fields_by_name: dict[str, pa.Field] = {}
    for entry in entries:
        target = entry.english_name
        if target is None or target in fields_by_name:
            continue
        source_type = source_schema.field(entry.source_name).type
        rule = merge_by_target.get(target)
        if rule is not None:
            source_types = {
                source_schema.field(source).type for source in rule.source_columns
            }
            if len(source_types) != 1:
                raise ValueError(
                    f"Approved merge {rule.name!r} has incompatible source types: "
                    f"{source_types}"
                )
            source_type = next(iter(source_types))
        value_mapping = value_mapping_by_target.get(target)
        if value_mapping is not None:
            source_type = _value_mapping_arrow_type(value_mapping)
        fields_by_name[target] = pa.field(target, source_type)

    if "hospital_id" in fields_by_name:
        raise ValueError("hospital_id already exists before derivation")
    fields_by_name["hospital_id"] = pa.field("hospital_id", pa.string())
    for rule in semantic_split_rules:
        if rule.base_target_name not in fields_by_name:
            raise ValueError(
                f"Semantic split {rule.name!r} base target does not exist in "
                "the generated schema"
            )
        if rule.new_target_name in fields_by_name:
            raise ValueError(
                f"Semantic split {rule.name!r} new target already exists"
            )
        fields_by_name[rule.new_target_name] = pa.field(
            rule.new_target_name,
            fields_by_name[rule.base_target_name].type,
        )

    duplicate_names = sorted(
        {name for name in output_order if output_order.count(name) > 1}
    )
    missing_names = sorted(set(fields_by_name) - set(output_order))
    unknown_names = sorted(set(output_order) - set(fields_by_name))
    if duplicate_names or missing_names or unknown_names:
        raise ValueError(
            "Translated output order does not match the generated schema; "
            f"duplicates={duplicate_names}, missing={missing_names}, "
            f"unknown={unknown_names}"
        )
    return pa.schema([fields_by_name[name] for name in output_order])


def _transform_table(
    table: pa.Table,
    table_name: str,
    contract: InputContract,
    registry: TranslationRegistry,
    metrics: _TableMetrics,
    output_schema: pa.Schema,
) -> pa.Table:
    entries = registry.entries_for(table_name)
    merge_rules = registry.merge_rules_for(table_name)
    merge_by_target = {rule.target_name: rule for rule in merge_rules}
    drop_sources = {
        rule.source_column for rule in registry.drop_rules_for(table_name)
    }

    for source in drop_sources:
        metrics.dropped_non_missing[source] += _true_count(
            _valid_value_mask(table.column(source))
        )

    arrays: list[pa.Array] = []
    names: list[str] = []
    emitted: set[str] = set()
    for entry in entries:
        target = entry.english_name
        if target is None or target in emitted:
            continue
        rule = merge_by_target.get(target)
        if rule is None:
            output_array = table.column(entry.source_name).combine_chunks()
        else:
            merge_metrics = metrics.merge.setdefault(
                rule.name,
                _MergeMetrics(),
            )
            output_array = _merge_arrays(
                table,
                rule,
                contract,
                merge_metrics,
            )
        value_mapping = registry.value_mapping_for(table_name, target)
        if value_mapping is not None:
            mapping_metrics = metrics.value_mappings.setdefault(
                target,
                _ValueMappingMetrics(),
            )
            output_array = _map_categorical_values(
                output_array,
                value_mapping,
                mapping_metrics,
            )
        missing_sentinel_rule = registry.numeric_missing_sentinel_rule_for(
            table_name,
            target,
        )
        if missing_sentinel_rule is not None:
            sentinel_metrics = metrics.numeric_missing_sentinels.setdefault(
                target,
                _NumericMissingSentinelMetrics(),
            )
            output_array = _mask_numeric_missing_sentinels(
                output_array,
                missing_sentinel_rule,
                sentinel_metrics,
            )
        semantic_split = (
            registry.hospital_specific_semantic_split_for_base_target(
                table_name,
                target,
            )
        )
        split_output: pa.Array | None = None
        if semantic_split is not None:
            split_metrics = metrics.hospital_specific_semantic_splits.setdefault(
                semantic_split.name,
                _HospitalSpecificSemanticSplitMetrics(),
            )
            output_array, split_output = _apply_hospital_specific_semantic_split(
                output_array,
                table.column(contract.hospital_id_column),
                semantic_split,
                split_metrics,
            )
        arrays.append(output_array)
        names.append(target)
        emitted.add(target)
        if semantic_split is not None:
            assert split_output is not None
            arrays.append(split_output)
            names.append(semantic_split.new_target_name)
            emitted.add(semantic_split.new_target_name)
        if target == "stay_id_global":
            arrays.append(_derive_hospital_id(table, contract))
            names.append("hospital_id")
            emitted.add("hospital_id")

    transformed = pa.Table.from_arrays(arrays, names=names).select(
        output_schema.names
    )
    if transformed.schema != output_schema:
        raise ValueError(
            f"Translated {table_name} batch schema changed unexpectedly; "
            f"observed={transformed.schema}, expected={output_schema}"
        )
    metrics.input_rows += table.num_rows
    metrics.output_rows += transformed.num_rows
    return transformed


def _validate_transform_metrics(
    table_metrics: dict[str, _TableMetrics],
    registry: TranslationRegistry,
) -> None:
    failures: list[str] = []
    for table_name, metrics in table_metrics.items():
        if metrics.input_rows != metrics.output_rows:
            failures.append(
                f"{table_name} row count changed: "
                f"{metrics.input_rows} -> {metrics.output_rows}"
            )
        for rule in registry.merge_rules_for(table_name):
            merge = metrics.merge[rule.name]
            if (
                rule.conflict_policy == MERGE_POLICY_FAIL_ON_UNEQUAL
                and merge.conflict_rows
            ):
                failures.append(
                    f"merge {rule.name} has {merge.conflict_rows} conflicting rows"
                )
            for source, count in merge.disallowed_hospital_rows.items():
                if count:
                    failures.append(
                        f"merge {rule.name} source {source} has {count} values in "
                        "disallowed hospitals"
                    )
            for source, count in merge.invalid_binary_rows.items():
                if count:
                    failures.append(
                        f"merge {rule.name} source {source} has {count} "
                        "non-binary values"
                    )
            if (
                rule.conflict_policy == MERGE_POLICY_BINARY_OR
                and merge.resolved_conflict_rows != merge.conflict_rows
            ):
                failures.append(
                    f"merge {rule.name} resolved "
                    f"{merge.resolved_conflict_rows} of "
                    f"{merge.conflict_rows} conflicts"
                )
        for rule in registry.drop_rules_for(table_name):
            non_missing = metrics.dropped_non_missing[rule.source_column]
            if non_missing:
                failures.append(
                    f"drop {table_name}.{rule.source_column} requires all missing but "
                    f"found {non_missing} non-missing rows"
                )
        for rule in registry.value_mappings_for(table_name):
            mapping = metrics.value_mappings[rule.target_name]
            if (
                mapping.source_non_missing
                != mapping.output_non_missing + mapping.mapped_to_missing
            ):
                failures.append(
                    f"categorical mapping {table_name}.{rule.target_name} lost "
                    "row accounting"
                )
        for rule in registry.numeric_missing_sentinel_rules_for(table_name):
            sentinel = metrics.numeric_missing_sentinels[rule.target_name]
            normalized = sum(sentinel.sentinel_counts.values())
            if sentinel.source_non_missing != sentinel.output_non_missing + normalized:
                failures.append(
                    f"numeric missing sentinel {table_name}.{rule.target_name} "
                    "lost row accounting"
                )
        for rule in registry.hospital_specific_semantic_split_rules_for(
            table_name
        ):
            split = metrics.hospital_specific_semantic_splits[rule.name]
            if split.source_non_missing != (
                split.base_output_non_missing + split.new_output_non_missing
            ):
                failures.append(
                    f"semantic split {rule.name} lost non-missing source values"
                )
            if split.selected_source_non_missing != split.new_output_non_missing:
                failures.append(
                    f"semantic split {rule.name} did not move every selected "
                    "non-missing value"
                )
            if (
                split.base_output_non_missing_in_selected_hospitals
                or split.new_output_non_missing_outside_selected_hospitals
            ):
                failures.append(
                    f"semantic split {rule.name} leaked values across hospital "
                    "scopes"
                )
    if failures:
        raise ValueError("Column harmonization validation failed: " + "; ".join(failures))


def _merge_metrics_manifest(
    metrics: _MergeMetrics,
    rule: MergeRule,
) -> dict[str, Any]:
    return {
        "table": rule.table,
        "source_columns": list(rule.source_columns),
        "target_name": rule.target_name,
        "conflict_policy": rule.conflict_policy,
        "source_non_missing": dict(metrics.source_non_missing),
        "source_non_missing_by_hospital": {
            source: {
                f"asic_UK{hospital:02d}": int(count)
                for hospital, count in sorted(counts.items())
            }
            for source, counts in metrics.source_non_missing_by_hospital.items()
        },
        "overlap_rows": metrics.overlap_rows,
        "conflict_rows": metrics.conflict_rows,
        "resolved_conflict_rows": metrics.resolved_conflict_rows,
        "invalid_binary_rows": dict(metrics.invalid_binary_rows),
        "disallowed_hospital_rows": dict(metrics.disallowed_hospital_rows),
        "output_non_missing": metrics.output_non_missing,
    }


def _value_mapping_manifest(
    rule: CategoricalValueMapping,
    metrics: _ValueMappingMetrics,
) -> dict[str, Any]:
    return {
        "table": rule.table,
        "target_name": rule.target_name,
        "normalization": rule.normalization,
        "output_arrow_type": rule.output_arrow_type,
        "approved_values": [
            {"source": source, "output": target}
            for source, target in rule.source_to_target
        ],
        "source_non_missing": metrics.source_non_missing,
        "normalized_source_counts": dict(metrics.normalized_source_counts),
        "output_non_missing": metrics.output_non_missing,
        "mapped_to_missing": metrics.mapped_to_missing,
        "unmapped_non_missing": 0,
    }


def _numeric_missing_sentinel_manifest(
    rule: NumericMissingSentinelRule,
    metrics: _NumericMissingSentinelMetrics,
) -> dict[str, Any]:
    return {
        "table": rule.table,
        "target_name": rule.target_name,
        "approved_sentinels": list(rule.values),
        "reason": rule.reason,
        "source_non_missing": metrics.source_non_missing,
        "sentinel_counts": {
            str(value): int(metrics.sentinel_counts[value])
            for value in rule.values
        },
        "mapped_to_missing": int(sum(metrics.sentinel_counts.values())),
        "output_non_missing": metrics.output_non_missing,
    }


def _hospital_specific_semantic_split_manifest(
    rule: HospitalSpecificSemanticSplitRule,
    metrics: _HospitalSpecificSemanticSplitMetrics,
) -> dict[str, Any]:
    return {
        "table": rule.table,
        "source_column": rule.source_column,
        "base_target_name": rule.base_target_name,
        "new_target_name": rule.new_target_name,
        "selected_hospital_codes": list(rule.selected_hospital_codes),
        "selected_hospital_ids": [
            f"asic_UK{code:02d}" for code in rule.selected_hospital_codes
        ],
        "status": "approved_semantic_split",
        "action": "move_selected_hospital_values",
        "reason": rule.reason,
        "source_non_missing": metrics.source_non_missing,
        "selected_rows": metrics.selected_rows,
        "selected_source_non_missing": metrics.selected_source_non_missing,
        "nonselected_source_non_missing": (
            metrics.nonselected_source_non_missing
        ),
        "base_output_non_missing": metrics.base_output_non_missing,
        "base_output_non_missing_in_selected_hospitals": (
            metrics.base_output_non_missing_in_selected_hospitals
        ),
        "new_output_non_missing": metrics.new_output_non_missing,
        "new_output_non_missing_outside_selected_hospitals": (
            metrics.new_output_non_missing_outside_selected_hospitals
        ),
        "source_non_missing_values_preserved": (
            metrics.source_non_missing
            == metrics.base_output_non_missing + metrics.new_output_non_missing
        ),
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


def translate_and_write_pooled_data(
    config: TranslationConfig,
    *,
    overwrite: bool = False,
    reporter: ProgressReporter | None = report_progress,
) -> TranslationResult:
    """Apply reviewed translations, validate them, and publish outputs."""

    contract = load_input_contract(config.source_contract_path)
    registry = load_translation_registry(config.policy_path, contract)
    if registry.status != "approved_for_pooled_to_translated":
        raise ValueError(
            "Translation policy is not approved for pooled-to-translated use: "
            f"{registry.status!r}"
        )

    static_source = config.source_pooled_dir / contract.static.filename
    dynamic_source = config.source_pooled_dir / contract.dynamic.filename
    static_output = config.output_translated_dir / contract.static.filename
    dynamic_output = config.output_translated_dir / contract.dynamic.filename
    static_parquet = _assert_parquet_schema(
        static_source,
        contract.static,
        config.dataset_context,
        "Pooled static",
    )
    dynamic_parquet = _assert_parquet_schema(
        dynamic_source,
        contract.dynamic,
        config.dataset_context,
        "Pooled dynamic",
    )
    existing = [
        path
        for path in (static_output, dynamic_output, config.manifest_path)
        if path.exists()
    ]
    if existing and not overwrite:
        raise FileExistsError(
            "Translated outputs already exist. Rerun with --overwrite only after "
            "explicit replacement approval; "
            f"existing={[str(path) for path in existing]}"
        )

    static_schema = _translated_schema(
        static_parquet.schema_arrow,
        registry.static,
        registry.merge_rules_for("static"),
        registry.value_mappings_for("static"),
        registry.hospital_specific_semantic_split_rules_for("static"),
        registry.output_order_for("static"),
    )
    dynamic_schema = _translated_schema(
        dynamic_parquet.schema_arrow,
        registry.dynamic,
        registry.merge_rules_for("dynamic"),
        registry.value_mappings_for("dynamic"),
        registry.hospital_specific_semantic_split_rules_for("dynamic"),
        registry.output_order_for("dynamic"),
    )
    table_metrics = {"static": _TableMetrics(), "dynamic": _TableMetrics()}

    config.output_translated_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".asic-translate-build-",
        dir=config.output_translated_dir.parent,
    ) as temporary_directory:
        staging_dir = Path(temporary_directory) / "translated"
        staging_dir.mkdir()
        staged_static = staging_dir / contract.static.filename
        staged_dynamic = staging_dir / contract.dynamic.filename

        _emit_progress(reporter, f"Translating static columns: {static_source}")
        source_static = static_parquet.read()
        translated_static = _transform_table(
            source_static,
            "static",
            contract,
            registry,
            table_metrics["static"],
            static_schema,
        )
        pq.write_table(translated_static, staged_static)

        _emit_progress(
            reporter,
            f"Streaming {dynamic_parquet.metadata.num_rows:,} dynamic rows in "
            f"batches of {config.batch_size:,}.",
        )
        processed_rows = 0
        next_progress_fraction = 0.1
        with pq.ParquetWriter(staged_dynamic, dynamic_schema) as writer:
            for batch in dynamic_parquet.iter_batches(batch_size=config.batch_size):
                source_batch = pa.Table.from_batches([batch])
                translated_batch = _transform_table(
                    source_batch,
                    "dynamic",
                    contract,
                    registry,
                    table_metrics["dynamic"],
                    dynamic_schema,
                )
                writer.write_table(translated_batch)
                processed_rows += batch.num_rows
                fraction = processed_rows / dynamic_parquet.metadata.num_rows
                if (
                    fraction >= next_progress_fraction
                    or processed_rows == dynamic_parquet.metadata.num_rows
                ):
                    _emit_progress(
                        reporter,
                        f"Translated {processed_rows:,}/"
                        f"{dynamic_parquet.metadata.num_rows:,} dynamic rows.",
                    )
                    while next_progress_fraction <= fraction:
                        next_progress_fraction += 0.1

        _validate_transform_metrics(table_metrics, registry)
        staged_static_parquet = pq.ParquetFile(staged_static)
        staged_dynamic_parquet = pq.ParquetFile(staged_dynamic)
        if staged_static_parquet.schema_arrow != static_schema:
            raise ValueError("Written translated static schema does not match plan")
        if staged_dynamic_parquet.schema_arrow != dynamic_schema:
            raise ValueError("Written translated dynamic schema does not match plan")
        if staged_static_parquet.metadata.num_rows != static_parquet.metadata.num_rows:
            raise ValueError("Translated static Parquet row count changed")
        if staged_dynamic_parquet.metadata.num_rows != dynamic_parquet.metadata.num_rows:
            raise ValueError("Translated dynamic Parquet row count changed")

        manifest = {
            "artifact": "asic_translated",
            "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "generator_version": __version__,
            "dataset_context": config.dataset_context,
            "source_contract_version": contract.contract_version,
            "translation_policy_version": registry.policy_version,
            "documentation": registry.documentation,
            "configuration": str(config.source_path),
            "source": {
                "pooled_directory": str(config.source_pooled_dir),
                "static_rows": static_parquet.metadata.num_rows,
                "dynamic_rows": dynamic_parquet.metadata.num_rows,
                "static_columns": len(static_parquet.schema_arrow),
                "dynamic_columns": len(dynamic_parquet.schema_arrow),
            },
            "output": {
                "translated_directory": str(config.output_translated_dir),
                "static_rows": staged_static_parquet.metadata.num_rows,
                "dynamic_rows": staged_dynamic_parquet.metadata.num_rows,
                "static_columns": len(static_schema),
                "dynamic_columns": len(dynamic_schema),
                "column_groups": {
                    table_name: {
                        group.name: list(group.columns)
                        for group in registry.output_groups_for(table_name)
                    }
                    for table_name in ("static", "dynamic")
                },
            },
            "audit_status": "pass",
            "checks": {
                "input_schemas_match_contract": True,
                "row_counts_preserved": True,
                "source_order_preserved": True,
                "translated_column_order_matches_registry": True,
                "stay_id_hid_and_time_values_preserved": True,
                "translated_names_unique": True,
                "approved_merge_conflict_policies_applied": True,
                "approved_categorical_value_mappings_applied": True,
                "unmapped_non_missing_values_in_approved_mappings": 0,
                "approved_categorical_missing_sentinels_normalized": True,
                "approved_numeric_missing_sentinels_normalized": True,
                "approved_hospital_specific_semantic_splits_applied": True,
                "semantic_split_source_values_preserved": True,
                "categorical_values_recoded": True,
                "unit_corrections_applied": False,
                "invalid_values_masked": False,
            },
            "derived_columns": {
                "hospital_id": (
                    "Formatted as asic_UKNN from the authoritative Pseudo-ID "
                    "suffix after verifying agreement with hid"
                )
            },
            "approved_merges": {
                rule.name: _merge_metrics_manifest(
                    table_metrics[rule.table].merge[rule.name],
                    rule,
                )
                for rule in registry.merge_rules
            },
            "approved_drops": {
                f"{rule.table}.{rule.source_column}": {
                    "precondition": rule.precondition,
                    "reason": rule.reason,
                    "non_missing_rows": table_metrics[
                        rule.table
                    ].dropped_non_missing[rule.source_column],
                }
                for rule in registry.drop_rules
            },
            "approved_categorical_value_mappings": {
                f"{rule.table}.{rule.target_name}": _value_mapping_manifest(
                    rule,
                    table_metrics[rule.table].value_mappings[rule.target_name],
                )
                for rule in registry.categorical_value_mappings
            },
            "approved_numeric_missing_sentinels": {
                f"{rule.table}.{rule.target_name}": (
                    _numeric_missing_sentinel_manifest(
                        rule,
                        table_metrics[rule.table].numeric_missing_sentinels[
                            rule.target_name
                        ],
                    )
                )
                for rule in registry.numeric_missing_sentinel_rules
            },
            "approved_hospital_specific_semantic_splits": {
                rule.name: _hospital_specific_semantic_split_manifest(
                    rule,
                    table_metrics[
                        rule.table
                    ].hospital_specific_semantic_splits[rule.name],
                )
                for rule in registry.hospital_specific_semantic_split_rules
            },
            "unresolved_collision_groups_retained_separately": (
                registry.unresolved_collision_group_count
            ),
        }
        staged_manifest = staging_dir / config.manifest_path.name
        _write_json(staged_manifest, manifest)

        config.output_translated_dir.mkdir(parents=True, exist_ok=True)
        staged_static.replace(static_output)
        staged_dynamic.replace(dynamic_output)
        staged_manifest.replace(config.manifest_path)

    _emit_progress(
        reporter,
        "Reviewed translation finished: "
        f"static={static_parquet.metadata.num_rows:,} rows/"
        f"{len(static_schema)} columns, dynamic={dynamic_parquet.metadata.num_rows:,} "
        f"rows/{len(dynamic_schema)} columns.",
    )
    return TranslationResult(
        static_path=static_output,
        dynamic_path=dynamic_output,
        manifest_path=config.manifest_path,
        static_row_count=static_parquet.metadata.num_rows,
        dynamic_row_count=dynamic_parquet.metadata.num_rows,
        static_column_count=len(static_schema),
        dynamic_column_count=len(dynamic_schema),
        audit_status="pass",
    )
