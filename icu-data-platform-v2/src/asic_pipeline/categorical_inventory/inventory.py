"""Read-only categorical value inventories for pooled ASIC data."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline import __version__
from asic_pipeline.categorical_inventory.config import CategoricalInventoryConfig
from asic_pipeline.contracts import InputContract, load_input_contract
from asic_pipeline.contracts.pooled import TableContract
from asic_pipeline.errors import InputReadError
from asic_pipeline.translations import (
    MERGE_POLICY_BINARY_OR,
    TranslationRegistry,
    load_translation_registry,
)


ProgressReporter = Callable[[str], None]


@dataclass(frozen=True)
class CategoricalInventoryResult:
    report: dict[str, Any]
    static_row_count: int
    dynamic_row_count: int
    audit_status: str


@dataclass(frozen=True)
class _ValueKey:
    value_type: str
    identity: str


@dataclass(frozen=True)
class _ConflictKey:
    left_source: str
    right_source: str
    left_value: _ValueKey
    right_value: _ValueKey


@dataclass
class _MergeBatchResult:
    values: pa.Array
    conflict_policy: str | None = None
    overlap_rows: int = 0
    conflict_mask: pa.Array | None = None
    conflict_rows: int = 0
    resolved_conflict_rows: int = 0
    excluded_conflict_rows: int = 0
    conflict_rows_by_hospital: Counter[int] = field(default_factory=Counter)
    resolved_conflict_rows_by_hospital: Counter[int] = field(
        default_factory=Counter
    )
    excluded_conflict_rows_by_hospital: Counter[int] = field(
        default_factory=Counter
    )
    conflict_pair_counts: Counter[_ConflictKey] = field(default_factory=Counter)
    conflict_pair_counts_by_hospital: dict[
        _ConflictKey, Counter[int]
    ] = field(default_factory=dict)
    conflict_pair_serialized_values: dict[
        _ConflictKey, tuple[Any, Any]
    ] = field(default_factory=dict)


@dataclass
class _ColumnStats:
    output_name: str
    source_columns: tuple[str, ...]
    arrow_type: pa.DataType
    hospitals: tuple[int, ...]
    max_distinct_values: int
    scanned_rows: int = 0
    null_count: int = 0
    nan_count: int = 0
    missing_by_hospital: dict[int, Counter[str]] = field(default_factory=dict)
    counts: Counter[_ValueKey] = field(default_factory=Counter)
    counts_by_hospital: dict[int, Counter[_ValueKey]] = field(default_factory=dict)
    serialized_values: dict[_ValueKey, Any] = field(default_factory=dict)
    truncated: bool = False
    untracked_non_missing_rows: int = 0
    untracked_by_hospital: Counter[int] = field(default_factory=Counter)
    merge_overlap_rows: int = 0
    merge_conflict_policy: str | None = None
    merge_conflict_rows: int = 0
    merge_resolved_conflict_rows: int = 0
    merge_excluded_conflict_rows: int = 0
    merge_conflict_rows_by_hospital: Counter[int] = field(default_factory=Counter)
    merge_resolved_conflict_rows_by_hospital: Counter[int] = field(
        default_factory=Counter
    )
    merge_excluded_conflict_rows_by_hospital: Counter[int] = field(
        default_factory=Counter
    )
    merge_conflict_pair_counts: Counter[_ConflictKey] = field(
        default_factory=Counter
    )
    merge_conflict_pair_counts_by_hospital: dict[
        _ConflictKey, Counter[int]
    ] = field(default_factory=dict)
    merge_conflict_pair_serialized_values: dict[
        _ConflictKey, tuple[Any, Any]
    ] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.missing_by_hospital = {
            hospital: Counter() for hospital in self.hospitals
        }
        self.counts_by_hospital = {
            hospital: Counter() for hospital in self.hospitals
        }

    def _add_overall_counts(self, array: pa.Array) -> None:
        for item in _value_counts(array):
            key, serialized = _value_key(item["value"])
            count = item["count"]
            if key in self.counts:
                self.counts[key] += count
            elif len(self.counts) < self.max_distinct_values:
                self.counts[key] = count
                self.serialized_values[key] = serialized
            else:
                self.truncated = True
                self.untracked_non_missing_rows += count

    def _add_hospital_counts(self, hospital: int, array: pa.Array) -> None:
        for item in _value_counts(array):
            key, _ = _value_key(item["value"])
            count = item["count"]
            if key in self.counts:
                self.counts_by_hospital[hospital][key] += count
            else:
                self.untracked_by_hospital[hospital] += count

    def add_merge_batch(self, result: _MergeBatchResult) -> None:
        if result.conflict_policy is not None:
            if (
                self.merge_conflict_policy is not None
                and self.merge_conflict_policy != result.conflict_policy
            ):
                raise ValueError(
                    f"Merge policy changed while scanning {self.output_name}: "
                    f"{self.merge_conflict_policy!r} -> "
                    f"{result.conflict_policy!r}"
                )
            self.merge_conflict_policy = result.conflict_policy
        self.merge_overlap_rows += result.overlap_rows
        self.merge_conflict_rows += result.conflict_rows
        self.merge_resolved_conflict_rows += result.resolved_conflict_rows
        self.merge_excluded_conflict_rows += result.excluded_conflict_rows
        self.merge_conflict_rows_by_hospital.update(
            result.conflict_rows_by_hospital
        )
        self.merge_resolved_conflict_rows_by_hospital.update(
            result.resolved_conflict_rows_by_hospital
        )
        self.merge_excluded_conflict_rows_by_hospital.update(
            result.excluded_conflict_rows_by_hospital
        )
        self.merge_conflict_pair_counts.update(result.conflict_pair_counts)
        for key, counts in result.conflict_pair_counts_by_hospital.items():
            self.merge_conflict_pair_counts_by_hospital.setdefault(
                key,
                Counter(),
            ).update(counts)
        self.merge_conflict_pair_serialized_values.update(
            result.conflict_pair_serialized_values
        )

    def add_batch(
        self,
        array: pa.Array,
        hospital_ids: pa.Array,
        excluded_conflict_mask: pa.Array | None = None,
    ) -> None:
        self.scanned_rows += len(array)
        if excluded_conflict_mask is None:
            eligible_mask: pa.Array | None = None
        else:
            eligible_mask = pc.invert(excluded_conflict_mask)

        null_mask = pc.is_null(array)
        if eligible_mask is not None:
            null_mask = pc.and_(null_mask, eligible_mask)
        self.null_count += _true_count(null_mask)
        valid_mask = pc.is_valid(array)
        if pa.types.is_floating(array.type):
            nan_mask = pc.fill_null(pc.is_nan(array), False)
            if eligible_mask is not None:
                nan_mask = pc.and_(nan_mask, eligible_mask)
            non_missing_mask = pc.and_(valid_mask, pc.invert(nan_mask))
            self.nan_count += _true_count(nan_mask)
        else:
            nan_mask = None
            non_missing_mask = valid_mask
        if eligible_mask is not None:
            non_missing_mask = pc.and_(non_missing_mask, eligible_mask)

        non_missing = pc.filter(array, non_missing_mask)
        self._add_overall_counts(non_missing)

        for hospital in self.hospitals:
            hospital_mask = pc.fill_null(
                pc.equal(
                    hospital_ids,
                    pa.scalar(hospital, type=hospital_ids.type),
                ),
                False,
            )
            hospital_null = pc.and_(hospital_mask, null_mask)
            self.missing_by_hospital[hospital]["null"] += _true_count(
                hospital_null
            )
            if nan_mask is not None:
                hospital_nan = pc.and_(hospital_mask, nan_mask)
                self.missing_by_hospital[hospital]["nan"] += _true_count(
                    hospital_nan
                )
            hospital_non_missing = pc.and_(hospital_mask, non_missing_mask)
            self._add_hospital_counts(
                hospital,
                pc.filter(array, hospital_non_missing),
            )

    def to_dict(self, hospital_row_counts: Counter[int]) -> dict[str, Any]:
        missing_total = self.null_count + self.nan_count
        non_missing_count = (
            self.scanned_rows
            - missing_total
            - self.merge_excluded_conflict_rows
        )
        values = []
        ordered_keys = sorted(
            self.counts,
            key=lambda key: (
                -self.counts[key],
                key.value_type,
                key.identity,
            ),
        )
        for key in ordered_keys:
            values.append(
                {
                    "value": self.serialized_values[key],
                    "value_type": key.value_type,
                    "count": int(self.counts[key]),
                    "by_hospital": {
                        _hospital_label(hospital): int(
                            self.counts_by_hospital[hospital][key]
                        )
                        for hospital in self.hospitals
                    },
                }
            )

        complete = not self.truncated
        distinct_count = len(self.counts) if complete else None
        distinct_at_least = len(self.counts) + (1 if self.truncated else 0)
        conflict_pairs = []
        ordered_conflict_keys = sorted(
            self.merge_conflict_pair_counts,
            key=lambda key: (
                -self.merge_conflict_pair_counts[key],
                key.left_source,
                key.right_source,
                key.left_value.value_type,
                key.left_value.identity,
                key.right_value.value_type,
                key.right_value.identity,
            ),
        )
        for key in ordered_conflict_keys:
            left_value, right_value = (
                self.merge_conflict_pair_serialized_values[key]
            )
            by_hospital = self.merge_conflict_pair_counts_by_hospital[key]
            conflict_pairs.append(
                {
                    "left_source": key.left_source,
                    "left_value": left_value,
                    "left_value_type": key.left_value.value_type,
                    "right_source": key.right_source,
                    "right_value": right_value,
                    "right_value_type": key.right_value.value_type,
                    "count": int(self.merge_conflict_pair_counts[key]),
                    "by_hospital": {
                        _hospital_label(hospital): int(by_hospital[hospital])
                        for hospital in self.hospitals
                    },
                }
            )
        return {
            "source_columns": list(self.source_columns),
            "approved_source_merge": len(self.source_columns) > 1,
            "source_merge_conflict_policy": self.merge_conflict_policy,
            "source_merge_overlap_rows": self.merge_overlap_rows,
            "source_merge_conflict_rows": self.merge_conflict_rows,
            "source_merge_resolved_conflict_rows": (
                self.merge_resolved_conflict_rows
            ),
            "source_merge_conflicts_by_hospital": {
                _hospital_label(hospital): int(
                    self.merge_conflict_rows_by_hospital[hospital]
                )
                for hospital in self.hospitals
            },
            "source_merge_resolved_conflicts_by_hospital": {
                _hospital_label(hospital): int(
                    self.merge_resolved_conflict_rows_by_hospital[hospital]
                )
                for hospital in self.hospitals
            },
            "source_merge_conflicting_value_pairs": conflict_pairs,
            "conflicting_rows_excluded_from_value_inventory": (
                self.merge_excluded_conflict_rows
            ),
            "arrow_type": str(self.arrow_type),
            "scanned_rows": self.scanned_rows,
            "non_missing_count": non_missing_count,
            "non_missing_by_hospital": {
                _hospital_label(hospital): int(
                    hospital_row_counts[hospital]
                    - self.missing_by_hospital[hospital]["null"]
                    - self.missing_by_hospital[hospital]["nan"]
                    - self.merge_excluded_conflict_rows_by_hospital[hospital]
                )
                for hospital in self.hospitals
            },
            "missing": {
                "total": missing_total,
                "null": self.null_count,
                "nan": self.nan_count,
                "by_hospital": {
                    _hospital_label(hospital): {
                        "total": int(
                            self.missing_by_hospital[hospital]["null"]
                            + self.missing_by_hospital[hospital]["nan"]
                        ),
                        "null": int(
                            self.missing_by_hospital[hospital]["null"]
                        ),
                        "nan": int(self.missing_by_hospital[hospital]["nan"]),
                    }
                    for hospital in self.hospitals
                },
            },
            "inventory_complete": complete,
            "row_accounting": {
                "scanned_rows": self.scanned_rows,
                "inventoried_non_missing_rows": non_missing_count,
                "missing_rows": missing_total,
                "excluded_source_conflict_rows": (
                    self.merge_excluded_conflict_rows
                ),
                "sum_matches_scanned_rows": (
                    non_missing_count
                    + missing_total
                    + self.merge_excluded_conflict_rows
                    == self.scanned_rows
                ),
            },
            "distinct_non_missing_values": distinct_count,
            "distinct_non_missing_values_at_least": distinct_at_least,
            "max_distinct_values_per_column": self.max_distinct_values,
            "untracked_non_missing_rows": self.untracked_non_missing_rows,
            "untracked_non_missing_rows_by_hospital": {
                _hospital_label(hospital): int(
                    self.untracked_by_hospital[hospital]
                )
                for hospital in self.hospitals
            },
            "values": values,
        }


def report_progress(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _emit_progress(reporter: ProgressReporter | None, message: str) -> None:
    if reporter is not None:
        reporter(message)


def _true_count(mask: pa.Array) -> int:
    value = pc.sum(pc.cast(mask, pa.int64())).as_py()
    return int(value or 0)


def _value_counts(array: pa.Array) -> list[dict[str, Any]]:
    if len(array) == 0:
        return []
    records = [
        {"value": item["values"], "count": int(item["counts"])}
        for item in pc.value_counts(array).to_pylist()
    ]
    return sorted(
        records,
        key=lambda item: (
            _value_key(item["value"])[0].value_type,
            _value_key(item["value"])[0].identity,
        ),
    )


def _value_key(value: Any) -> tuple[_ValueKey, Any]:
    if isinstance(value, bool):
        return _ValueKey("boolean", "true" if value else "false"), value
    if isinstance(value, int):
        return _ValueKey("integer", str(value)), value
    if isinstance(value, float):
        if math.isnan(value):
            raise ValueError("IEEE NaN must be classified as missing before counting")
        if math.isinf(value):
            label = "Infinity" if value > 0 else "-Infinity"
            return _ValueKey("float_special", label), label
        return _ValueKey("float", repr(value)), value
    if isinstance(value, str):
        return _ValueKey("string", value), value
    raise ValueError(
        "Categorical inventory only supports boolean, integer, floating, and "
        f"string values, got {type(value).__name__}"
    )


def _hospital_label(hospital: int) -> str:
    return f"asic_UK{hospital:02d}"


def _open_pooled_table(
    path: Path,
    table_name: str,
    table_contract: TableContract,
    dataset_context: str,
) -> pq.ParquetFile:
    if not path.is_file():
        raise InputReadError(f"Pooled {table_name} file does not exist: {path}")
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
            f"Pooled {table_name} schema does not match contract context "
            f"{dataset_context!r}; missing={missing}, extra={extra}, "
            f"column_order_matches={observed_names == expected_names}, "
            f"type_mismatches={type_mismatches}"
        )
    return parquet


def _source_lineage(
    registry: TranslationRegistry,
    table_name: str,
    output_name: str,
) -> tuple[str, ...]:
    return tuple(
        entry.source_name
        for entry in registry.entries_for(table_name)
        if entry.english_name == output_name
    )


def _valid_value_mask(array: pa.Array) -> pa.Array:
    valid = pc.is_valid(array)
    if pa.types.is_floating(array.type):
        valid = pc.and_(valid, pc.invert(pc.fill_null(pc.is_nan(array), False)))
    return pc.fill_null(valid, False)


def _normalize_missing(array: pa.Array) -> pa.Array:
    return pc.if_else(
        _valid_value_mask(array),
        array,
        pa.scalar(None, type=array.type),
    )


def _harmonize_categorical_sources(
    batch: pa.RecordBatch,
    table_name: str,
    output_name: str,
    source_columns: tuple[str, ...],
    registry: TranslationRegistry,
    hospital_codes: pa.Array,
) -> _MergeBatchResult:
    arrays = [
        batch.column(batch.schema.get_field_index(source))
        for source in source_columns
    ]
    if len(arrays) == 1:
        return _MergeBatchResult(values=arrays[0])

    rule = next(
        (
            candidate
            for candidate in registry.merge_rules_for(table_name)
            if candidate.target_name == output_name
            and set(candidate.source_columns) == set(source_columns)
        ),
        None,
    )
    if rule is None:
        raise ValueError(
            f"Categorical target {table_name}.{output_name} has multiple source "
            "columns without an approved merge rule"
        )
    source_types = {array.type for array in arrays}
    if len(source_types) != 1:
        raise ValueError(
            f"Approved categorical merge {rule.name!r} has incompatible source "
            f"types: {source_types}"
        )

    valid_masks = [_valid_value_mask(array) for array in arrays]
    normalized = [_normalize_missing(array) for array in arrays]
    overlap_rows = 0
    conflict_mask: pa.Array | None = None
    conflict_pair_counts: Counter[_ConflictKey] = Counter()
    conflict_pair_counts_by_hospital: dict[
        _ConflictKey, Counter[int]
    ] = {}
    conflict_pair_serialized_values: dict[
        _ConflictKey, tuple[Any, Any]
    ] = {}
    for source, array, valid in zip(
        source_columns,
        arrays,
        valid_masks,
        strict=True,
    ):
        allowed = rule.allowed_hospitals_for(source)
        if allowed is not None:
            allowed_values = pa.array(allowed, type=hospital_codes.type)
            disallowed = pc.and_(
                valid,
                pc.invert(pc.is_in(hospital_codes, value_set=allowed_values)),
            )
            disallowed_count = _true_count(disallowed)
            if disallowed_count:
                raise ValueError(
                    f"Categorical merge {rule.name} source {source} has "
                    f"{disallowed_count} values in disallowed hospitals"
                )
        if rule.conflict_policy == MERGE_POLICY_BINARY_OR:
            binary_values = pa.array([0, 1], type=array.type)
            invalid_binary = pc.and_(
                valid,
                pc.invert(pc.is_in(array, value_set=binary_values)),
            )
            invalid_count = _true_count(invalid_binary)
            if invalid_count:
                raise ValueError(
                    f"Categorical merge {rule.name} source {source} has "
                    f"{invalid_count} non-binary values"
                )

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
            overlap_rows += _true_count(both_valid)
            conflict_mask = (
                conflicts
                if conflict_mask is None
                else pc.or_(conflict_mask, conflicts)
            )
            if not _true_count(conflicts):
                continue

            left_values = pc.filter(arrays[left_index], conflicts).to_pylist()
            right_values = pc.filter(arrays[right_index], conflicts).to_pylist()
            conflict_hospitals = pc.filter(
                hospital_codes,
                conflicts,
            ).to_pylist()
            for left, right, hospital_value in zip(
                left_values,
                right_values,
                conflict_hospitals,
                strict=True,
            ):
                left_key, left_serialized = _value_key(left)
                right_key, right_serialized = _value_key(right)
                key = _ConflictKey(
                    left_source=source_columns[left_index],
                    right_source=source_columns[right_index],
                    left_value=left_key,
                    right_value=right_key,
                )
                hospital = int(hospital_value)
                conflict_pair_counts[key] += 1
                conflict_pair_counts_by_hospital.setdefault(
                    key,
                    Counter(),
                )[hospital] += 1
                conflict_pair_serialized_values[key] = (
                    left_serialized,
                    right_serialized,
                )

    if conflict_mask is None:
        raise ValueError(
            f"Approved categorical merge {rule.name!r} has fewer than two sources"
        )
    conflict_rows = _true_count(conflict_mask)
    conflict_rows_by_hospital: Counter[int] = Counter()
    if conflict_rows:
        for item in pc.value_counts(
            pc.filter(hospital_codes, conflict_mask)
        ).to_pylist():
            conflict_rows_by_hospital[int(item["values"])] += int(item["counts"])

    if rule.conflict_policy == MERGE_POLICY_BINARY_OR:
        any_valid = valid_masks[0]
        any_one = pc.and_(
            valid_masks[0],
            pc.fill_null(pc.equal(arrays[0], 1), False),
        )
        for array, valid in zip(arrays[1:], valid_masks[1:], strict=True):
            any_valid = pc.or_(any_valid, valid)
            any_one = pc.or_(
                any_one,
                pc.and_(
                    valid,
                    pc.fill_null(pc.equal(array, 1), False),
                ),
            )
        values = pc.if_else(
            any_valid,
            pc.if_else(
                any_one,
                pa.scalar(1, type=arrays[0].type),
                pa.scalar(0, type=arrays[0].type),
            ),
            pa.scalar(None, type=arrays[0].type),
        )
        excluded_conflict_mask = None
        resolved_conflict_rows = conflict_rows
        resolved_conflict_rows_by_hospital = conflict_rows_by_hospital
        excluded_conflict_rows = 0
        excluded_conflict_rows_by_hospital: Counter[int] = Counter()
    else:
        values = pc.coalesce(*normalized)
        excluded_conflict_mask = conflict_mask
        resolved_conflict_rows = 0
        resolved_conflict_rows_by_hospital = Counter()
        excluded_conflict_rows = conflict_rows
        excluded_conflict_rows_by_hospital = conflict_rows_by_hospital

    return _MergeBatchResult(
        values=values,
        conflict_policy=rule.conflict_policy,
        overlap_rows=overlap_rows,
        conflict_mask=excluded_conflict_mask,
        conflict_rows=conflict_rows,
        resolved_conflict_rows=resolved_conflict_rows,
        excluded_conflict_rows=excluded_conflict_rows,
        conflict_rows_by_hospital=conflict_rows_by_hospital,
        resolved_conflict_rows_by_hospital=(
            resolved_conflict_rows_by_hospital
        ),
        excluded_conflict_rows_by_hospital=(
            excluded_conflict_rows_by_hospital
        ),
        conflict_pair_counts=conflict_pair_counts,
        conflict_pair_counts_by_hospital=conflict_pair_counts_by_hospital,
        conflict_pair_serialized_values=conflict_pair_serialized_values,
    )


def _validate_inventory_types(
    parquet: pq.ParquetFile,
    table_name: str,
    source_lineages: dict[str, tuple[str, ...]],
) -> None:
    supported = []
    unsupported = []
    for output_name, sources in source_lineages.items():
        for source in sources:
            arrow_type = parquet.schema_arrow.field(source).type
            is_supported = any(
                predicate(arrow_type)
                for predicate in (
                    pa.types.is_boolean,
                    pa.types.is_integer,
                    pa.types.is_floating,
                    pa.types.is_string,
                    pa.types.is_large_string,
                )
            )
            (supported if is_supported else unsupported).append(
                (output_name, source, str(arrow_type))
            )
    if unsupported:
        raise ValueError(
            f"Pooled {table_name} categorical columns have unsupported types: "
            f"{unsupported}; supported={supported}"
        )


def _scan_table(
    parquet: pq.ParquetFile,
    table_name: str,
    categorical_columns: tuple[str, ...],
    registry: TranslationRegistry,
    contract: InputContract,
    hospitals: tuple[int, ...],
    config: CategoricalInventoryConfig,
    reporter: ProgressReporter | None,
) -> dict[str, Any]:
    source_lineages = {
        column: _source_lineage(registry, table_name, column)
        for column in categorical_columns
    }
    _validate_inventory_types(parquet, table_name, source_lineages)
    source_columns = tuple(
        dict.fromkeys(
            source
            for column in categorical_columns
            for source in source_lineages[column]
        )
    )
    columns = (contract.hospital_id_column, *source_columns)
    stats = {
        column: _ColumnStats(
            output_name=column,
            source_columns=source_lineages[column],
            arrow_type=parquet.schema_arrow.field(
                source_lineages[column][0]
            ).type,
            hospitals=hospitals,
            max_distinct_values=config.max_distinct_values_per_column,
        )
        for column in categorical_columns
    }
    hospital_row_counts: Counter[int] = Counter()
    processed_rows = 0
    next_progress_fraction = 0.1
    for batch in parquet.iter_batches(
        batch_size=config.batch_size,
        columns=list(columns),
    ):
        hospital_codes = batch.column(0)
        for item in pc.value_counts(hospital_codes).to_pylist():
            value = item["values"]
            if (
                value is None
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or int(value) != value
            ):
                raise ValueError(
                    f"Pooled {table_name}.{contract.hospital_id_column} contains "
                    f"invalid value {value!r}"
                )
            hospital = int(value)
            if hospital not in hospitals:
                raise ValueError(
                    f"Pooled {table_name}.{contract.hospital_id_column} contains "
                    "unexpected "
                    f"value {hospital!r}"
                )
            hospital_row_counts[hospital] += int(item["counts"])

        for column in categorical_columns:
            merge_result = _harmonize_categorical_sources(
                batch,
                table_name,
                column,
                source_lineages[column],
                registry,
                hospital_codes,
            )
            stats[column].add_merge_batch(merge_result)
            stats[column].add_batch(
                merge_result.values,
                hospital_codes,
                merge_result.conflict_mask,
            )

        processed_rows += batch.num_rows
        fraction = processed_rows / parquet.metadata.num_rows
        if (
            fraction >= next_progress_fraction
            or processed_rows == parquet.metadata.num_rows
        ):
            _emit_progress(
                reporter,
                f"Inventoried {processed_rows:,}/{parquet.metadata.num_rows:,} "
                f"{table_name} rows.",
            )
            while next_progress_fraction <= fraction:
                next_progress_fraction += 0.1

    if processed_rows != parquet.metadata.num_rows:
        raise ValueError(
            f"Categorical inventory did not scan all {table_name} rows: "
            f"{processed_rows} != {parquet.metadata.num_rows}"
        )
    return {
        "row_count": parquet.metadata.num_rows,
        "scanned_rows": processed_rows,
        "categorical_column_count": len(categorical_columns),
        "hospital_row_counts": {
            _hospital_label(hospital): int(hospital_row_counts[hospital])
            for hospital in hospitals
        },
        "columns": {
            column: stats[column].to_dict(hospital_row_counts)
            for column in categorical_columns
        },
    }


def inventory_categorical_values(
    config: CategoricalInventoryConfig,
    *,
    reporter: ProgressReporter | None = report_progress,
) -> CategoricalInventoryResult:
    """Inventory reviewed pooled categorical fields without changing data."""

    contract = load_input_contract(config.source_contract_path)
    registry = load_translation_registry(config.policy_path, contract)
    static_path = config.source_pooled_dir / contract.static.filename
    dynamic_path = config.source_pooled_dir / contract.dynamic.filename
    static_parquet = _open_pooled_table(
        static_path,
        "static",
        contract.static,
        config.dataset_context,
    )
    dynamic_parquet = _open_pooled_table(
        dynamic_path,
        "dynamic",
        contract.dynamic,
        config.dataset_context,
    )
    hospitals = contract.expected_hospital_codes

    _emit_progress(reporter, f"Inventorying pooled static data: {static_path}")
    tables = {
        "static": _scan_table(
            static_parquet,
            "static",
            registry.categorical_columns_for("static"),
            registry,
            contract,
            hospitals,
            config,
            reporter,
        )
    }
    _emit_progress(reporter, f"Inventorying pooled dynamic data: {dynamic_path}")
    tables["dynamic"] = _scan_table(
        dynamic_parquet,
        "dynamic",
        registry.categorical_columns_for("dynamic"),
        registry,
        contract,
        hospitals,
        config,
        reporter,
    )

    incomplete_columns = [
        f"{table_name}.{column_name}"
        for table_name, table in tables.items()
        for column_name, inventory in table["columns"].items()
        if not inventory["inventory_complete"]
    ]
    merge_conflicts = [
        {
            "column": f"{table_name}.{column_name}",
            "conflict_rows": inventory["source_merge_conflict_rows"],
            "by_hospital": {
                hospital: count
                for hospital, count in inventory[
                    "source_merge_conflicts_by_hospital"
                ].items()
                if count
            },
        }
        for table_name, table in tables.items()
        for column_name, inventory in table["columns"].items()
        if inventory["conflicting_rows_excluded_from_value_inventory"]
    ]
    if merge_conflicts:
        audit_status = "fail"
    elif incomplete_columns:
        audit_status = "warning"
    else:
        audit_status = "pass"
    report = {
        "artifact": "asic_categorical_value_inventory",
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "generator_version": __version__,
        "dataset_context": config.dataset_context,
        "source_contract_version": contract.contract_version,
        "translation_policy_version": registry.policy_version,
        "documentation": registry.documentation,
        "configuration": str(config.source_path),
        "source": {
            "pooled_directory": str(config.source_pooled_dir),
            "static_file": str(static_path),
            "dynamic_file": str(dynamic_path),
        },
        "audit_status": audit_status,
        "checks": {
            "pooled_schemas_match_contract": True,
            "hospital_codes_match_contract": True,
            "approved_categorical_aliases_have_no_unresolved_conflicts": (
                not merge_conflicts
            ),
            "all_rows_scanned": True,
            "inventories_complete_within_limit": not incomplete_columns,
        },
        "blocking_findings": {
            "categorical_alias_merge_conflicts": merge_conflicts,
        },
        "inventory_limit": {
            "max_distinct_values_per_column": (
                config.max_distinct_values_per_column
            ),
            "incomplete_columns": incomplete_columns,
        },
        "tables": tables,
        "limitations": [
            "This report inventories pooled categorical values under their translated "
            "output names and combines only approved source aliases. It does "
            "not translate or otherwise recode values, impute, filter, derive, or "
            "write data tables.",
            "JSON null and IEEE NaN are counted separately. Literal strings such as "
            "'nan' remain visible raw inventory values; reviewed translated-stage "
            "mappings are not applied by this command.",
            "Counts are aggregate and contain no stay identifiers.",
            "Unequal non-missing approved aliases are reported as aggregate value "
            "pairs by hospital. The therapy-confirmation spelling variants use the "
            "approved binary-OR rule (0/1 becomes 1); non-binary values are "
            "blocking. Other unequal alias overlaps remain excluded and blocking.",
            (
                "Demo counts describe an unchanged sampled subset and may omit rare "
                "production categories."
                if config.dataset_context == "demo"
                else "Production counts do not by themselves establish clinical meaning."
            ),
        ],
    }
    return CategoricalInventoryResult(
        report=report,
        static_row_count=static_parquet.metadata.num_rows,
        dynamic_row_count=dynamic_parquet.metadata.num_rows,
        audit_status=audit_status,
    )


def write_categorical_inventory_report(
    report: dict[str, Any],
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    temporary_path.replace(path)
    return path
