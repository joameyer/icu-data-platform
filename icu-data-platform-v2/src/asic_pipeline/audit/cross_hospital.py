from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.pooled_input import _check, _schema_summary
from asic_pipeline.audit.report import (
    AuditReport,
    CheckResult,
    overall_status,
    utc_timestamp,
)
from asic_pipeline.contracts import TranslatedInputContract
from asic_pipeline.data_quality import (
    CandidateContextAuditPolicy,
    CandidateContextFieldPolicy,
    CrossHospitalAuditConfig,
    DataQualityPolicy,
    InvalidValueRule,
    PredictedBodyWeightTidalVolumeAudit,
    RowLevelScaleEntryAudit,
    ScaleEntryDiscoveryPolicy,
    TargetedBucketAudit,
    TargetedScaleAudit,
)
from asic_pipeline.errors import InputReadError
from asic_pipeline.io import PooledInput, PooledTable


_UINT64_MAX = np.iinfo(np.uint64).max


def _json_number(value: float | int | np.number | None) -> float | int | None:
    if value is None:
        return None
    numeric = float(value)
    if not np.isfinite(numeric):
        return None
    return numeric


@dataclass
class _BottomKSample:
    capacity: int
    priorities: np.ndarray = field(
        default_factory=lambda: np.empty(0, dtype=np.uint64)
    )
    values: np.ndarray = field(
        default_factory=lambda: np.empty(0, dtype=np.float64)
    )

    def add(self, priorities: np.ndarray, values: np.ndarray) -> None:
        if values.size == 0:
            return
        new_priorities = priorities.astype(np.uint64, copy=False)
        new_values = values.astype(np.float64, copy=False)
        if self.priorities.size >= self.capacity:
            threshold = self.priorities.max()
            keep = new_priorities < threshold
            if not keep.any():
                return
            new_priorities = new_priorities[keep]
            new_values = new_values[keep]
        combined_priorities = np.concatenate((self.priorities, new_priorities))
        combined_values = np.concatenate((self.values, new_values))
        if combined_priorities.size > self.capacity:
            selected = np.argpartition(
                combined_priorities, self.capacity - 1
            )[: self.capacity]
            combined_priorities = combined_priorities[selected]
            combined_values = combined_values[selected]
        self.priorities = combined_priorities
        self.values = combined_values


@dataclass
class _NumericAccumulator:
    sample_capacity: int
    max_examples: int
    total_rows: int = 0
    finite_count: int = 0
    missing_or_nan_count: int = 0
    positive_infinity_count: int = 0
    negative_infinity_count: int = 0
    zero_count: int = 0
    negative_count: int = 0
    minus_one_count: int = 0
    value_sum: float = 0.0
    squared_sum: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    invalid_count: int = 0
    invalid_examples: set[float] = field(default_factory=set)
    sample: _BottomKSample = field(init=False)

    def __post_init__(self) -> None:
        self.sample = _BottomKSample(self.sample_capacity)

    def add(
        self,
        values: np.ndarray,
        priorities: np.ndarray,
        rule: InvalidValueRule | None,
    ) -> None:
        values = values.astype(np.float64, copy=False)
        self.total_rows += int(values.size)
        nan_mask = np.isnan(values)
        pos_inf_mask = np.isposinf(values)
        neg_inf_mask = np.isneginf(values)
        finite_mask = np.isfinite(values)
        finite = values[finite_mask]
        self.missing_or_nan_count += int(nan_mask.sum())
        self.positive_infinity_count += int(pos_inf_mask.sum())
        self.negative_infinity_count += int(neg_inf_mask.sum())
        self.finite_count += int(finite.size)
        if finite.size == 0:
            return
        self.zero_count += int(np.count_nonzero(finite == 0))
        self.negative_count += int(np.count_nonzero(finite < 0))
        self.minus_one_count += int(np.count_nonzero(finite == -1))
        self.value_sum += float(finite.sum(dtype=np.float64))
        self.squared_sum += float(np.square(finite).sum(dtype=np.float64))
        batch_min = float(finite.min())
        batch_max = float(finite.max())
        self.minimum = batch_min if self.minimum is None else min(self.minimum, batch_min)
        self.maximum = batch_max if self.maximum is None else max(self.maximum, batch_max)
        self.sample.add(priorities[finite_mask], finite)

        if rule is None:
            return
        invalid = np.zeros(finite.size, dtype=bool)
        if rule.hard_min is not None:
            invalid |= finite < rule.hard_min
        if rule.hard_max is not None:
            invalid |= finite > rule.hard_max
        if rule.invalid_zero:
            invalid |= finite == 0
        self.invalid_count += int(invalid.sum())
        if invalid.any() and len(self.invalid_examples) < self.max_examples:
            for value in np.unique(finite[invalid]):
                self.invalid_examples.add(float(value))
                if len(self.invalid_examples) >= self.max_examples:
                    break

    def profile(self, table: str, hospital: str, column: str) -> dict[str, Any]:
        quantiles: dict[str, float | None] = {
            "q01": None,
            "q1": None,
            "median": None,
            "q3": None,
            "q99": None,
        }
        if self.sample.values.size:
            values = np.quantile(
                self.sample.values,
                [0.01, 0.25, 0.5, 0.75, 0.99],
            )
            quantiles = {
                name: _json_number(value)
                for name, value in zip(quantiles, values, strict=True)
            }
        mean = self.value_sum / self.finite_count if self.finite_count else None
        variance = None
        if self.finite_count:
            variance = max(
                self.squared_sum / self.finite_count - float(mean) ** 2,
                0.0,
            )
        q1 = quantiles["q1"]
        q3 = quantiles["q3"]
        return {
            "table": table,
            "column": column,
            "hospital": hospital,
            "row_count": self.total_rows,
            "finite_count": self.finite_count,
            "finite_rate": (
                self.finite_count / self.total_rows if self.total_rows else 0.0
            ),
            "missing_or_nan_count": self.missing_or_nan_count,
            "positive_infinity_count": self.positive_infinity_count,
            "negative_infinity_count": self.negative_infinity_count,
            "zero_count": self.zero_count,
            "negative_count": self.negative_count,
            "minus_one_count": self.minus_one_count,
            "min": _json_number(self.minimum),
            **quantiles,
            "max": _json_number(self.maximum),
            "mean": _json_number(mean),
            "std": _json_number(np.sqrt(variance) if variance is not None else None),
            "iqr": (
                _json_number(float(q3) - float(q1))
                if q1 is not None and q3 is not None
                else None
            ),
            "range_width": (
                _json_number(float(self.maximum) - float(self.minimum))
                if self.minimum is not None and self.maximum is not None
                else None
            ),
            "quantile_sample_count": int(self.sample.values.size),
            "quantiles_are_exact": self.finite_count <= self.sample_capacity,
        }


@dataclass
class _DifferenceAccumulator:
    sample_capacity: int
    tolerance: float
    both_present_count: int = 0
    left_only_count: int = 0
    right_only_count: int = 0
    neither_present_count: int = 0
    within_tolerance_count: int = 0
    outside_tolerance_count: int = 0
    absolute_difference_sum: float = 0.0
    maximum_absolute_difference: float | None = None
    sample: _BottomKSample = field(init=False)

    def __post_init__(self) -> None:
        self.sample = _BottomKSample(self.sample_capacity)

    def add(
        self,
        left: np.ndarray,
        right: np.ndarray,
        priorities: np.ndarray,
    ) -> None:
        left_finite = np.isfinite(left)
        right_finite = np.isfinite(right)
        both = left_finite & right_finite
        self.both_present_count += int(both.sum())
        self.left_only_count += int((left_finite & ~right_finite).sum())
        self.right_only_count += int((~left_finite & right_finite).sum())
        self.neither_present_count += int((~left_finite & ~right_finite).sum())
        if not both.any():
            return
        differences = left[both] - right[both]
        absolute = np.abs(differences)
        within = absolute <= self.tolerance
        self.within_tolerance_count += int(within.sum())
        self.outside_tolerance_count += int((~within).sum())
        self.absolute_difference_sum += float(absolute.sum(dtype=np.float64))
        batch_max = float(absolute.max())
        self.maximum_absolute_difference = (
            batch_max
            if self.maximum_absolute_difference is None
            else max(self.maximum_absolute_difference, batch_max)
        )
        self.sample.add(priorities[both], differences)

    def result(self, hospital: str) -> dict[str, Any]:
        quantiles = {"q01": None, "q1": None, "median": None, "q3": None, "q99": None}
        if self.sample.values.size:
            values = np.quantile(
                self.sample.values,
                [0.01, 0.25, 0.5, 0.75, 0.99],
            )
            quantiles = {
                key: _json_number(value)
                for key, value in zip(quantiles, values, strict=True)
            }
        return {
            "hospital": hospital,
            "both_present_count": self.both_present_count,
            "left_only_count": self.left_only_count,
            "right_only_count": self.right_only_count,
            "neither_present_count": self.neither_present_count,
            "within_tolerance_count": self.within_tolerance_count,
            "outside_tolerance_count": self.outside_tolerance_count,
            "mean_absolute_difference": (
                self.absolute_difference_sum / self.both_present_count
                if self.both_present_count
                else None
            ),
            "maximum_absolute_difference": _json_number(
                self.maximum_absolute_difference
            ),
            "difference_quantiles": quantiles,
            "difference_sample_count": int(self.sample.values.size),
        }


@dataclass
class _PredictedBodyWeightReference:
    policy: PredictedBodyWeightTidalVolumeAudit
    values_by_stay: dict[str, tuple[float, float]]
    target_hospital_static_rows: int
    duplicate_stay_count: int
    sex_value_counts: dict[str, int]
    unsupported_or_missing_sex_count: int
    implausible_or_missing_height_count: int
    valid_predicted_body_weight_count: int
    implausible_or_missing_actual_weight_count: int
    valid_actual_weight_count: int
    predicted_body_weight_profile: dict[str, Any]


@dataclass
class _PredictedBodyWeightTidalVolumeAccumulator:
    policy: PredictedBodyWeightTidalVolumeAudit
    sample_capacity: int
    max_examples: int
    target_absolute_vs_six_pbw: _DifferenceAccumulator = field(init=False)
    reported_vt_per_kg_vs_vt_div_pbw: _DifferenceAccumulator = field(
        init=False
    )
    target_absolute_vs_vt_scaled_pbw_over_actual_weight: (
        _DifferenceAccumulator
    ) = field(init=False)
    target_ml_per_kg_pbw: _NumericAccumulator = field(init=False)
    delivered_vt_ml_per_kg_pbw: _NumericAccumulator = field(init=False)
    delivered_vt_ml_per_kg_actual_weight: _NumericAccumulator = field(
        init=False
    )
    target_hospital_dynamic_rows: int = 0
    rows_without_static_reference: int = 0
    rows_without_valid_pbw: int = 0
    rows_without_valid_actual_weight: int = 0

    def __post_init__(self) -> None:
        self.target_absolute_vs_six_pbw = _DifferenceAccumulator(
            self.sample_capacity,
            self.policy.absolute_tolerance_ml,
        )
        self.reported_vt_per_kg_vs_vt_div_pbw = _DifferenceAccumulator(
            self.sample_capacity,
            self.policy.normalized_tolerance_ml_per_kg,
        )
        self.target_absolute_vs_vt_scaled_pbw_over_actual_weight = (
            _DifferenceAccumulator(
                self.sample_capacity,
                self.policy.absolute_tolerance_ml,
            )
        )
        self.target_ml_per_kg_pbw = _NumericAccumulator(
            self.sample_capacity,
            self.max_examples,
        )
        self.delivered_vt_ml_per_kg_pbw = _NumericAccumulator(
            self.sample_capacity,
            self.max_examples,
        )
        self.delivered_vt_ml_per_kg_actual_weight = _NumericAccumulator(
            self.sample_capacity,
            self.max_examples,
        )

    def add(
        self,
        frame: pd.DataFrame,
        positions: np.ndarray,
        priorities: np.ndarray,
        reference: _PredictedBodyWeightReference,
        stay_id_column: str,
    ) -> None:
        count = int(positions.size)
        self.target_hospital_dynamic_rows += count
        stay_ids = (
            frame.iloc[positions][stay_id_column]
            .astype("string")
            .fillna("")
            .tolist()
        )
        reference_values = [
            reference.values_by_stay.get(str(stay_id))
            for stay_id in stay_ids
        ]
        known = np.fromiter(
            (value is not None for value in reference_values),
            dtype=bool,
            count=count,
        )
        pbw = np.fromiter(
            (
                value[0] if value is not None else np.nan
                for value in reference_values
            ),
            dtype=np.float64,
            count=count,
        )
        actual_weight = np.fromiter(
            (
                value[1] if value is not None else np.nan
                for value in reference_values
            ),
            dtype=np.float64,
            count=count,
        )
        self.rows_without_static_reference += int((~known).sum())
        self.rows_without_valid_pbw += int((~np.isfinite(pbw)).sum())
        self.rows_without_valid_actual_weight += int(
            (~np.isfinite(actual_weight)).sum()
        )

        site_priorities = priorities[positions]
        vt = _array(frame, self.policy.vt_column, positions)
        reported_vt_per_kg = _array(
            frame,
            self.policy.vt_per_kg_column,
            positions,
        )
        target_absolute = _array(
            frame,
            self.policy.target_absolute_vt_column,
            positions,
        )
        expected_target = pbw * self.policy.target_ml_per_kg_pbw
        vt_div_pbw = np.divide(
            vt,
            pbw,
            out=np.full(vt.shape, np.nan, dtype=np.float64),
            where=np.isfinite(pbw) & (pbw > 0),
        )
        target_div_pbw = np.divide(
            target_absolute,
            pbw,
            out=np.full(target_absolute.shape, np.nan, dtype=np.float64),
            where=np.isfinite(pbw) & (pbw > 0),
        )
        vt_div_actual_weight = np.divide(
            vt,
            actual_weight,
            out=np.full(vt.shape, np.nan, dtype=np.float64),
            where=np.isfinite(actual_weight) & (actual_weight > 0),
        )
        vt_scaled_pbw_over_actual_weight = np.divide(
            vt * pbw,
            actual_weight,
            out=np.full(vt.shape, np.nan, dtype=np.float64),
            where=(
                np.isfinite(pbw)
                & (pbw > 0)
                & np.isfinite(actual_weight)
                & (actual_weight > 0)
            ),
        )

        self.target_absolute_vs_six_pbw.add(
            target_absolute,
            expected_target,
            site_priorities,
        )
        self.reported_vt_per_kg_vs_vt_div_pbw.add(
            reported_vt_per_kg,
            vt_div_pbw,
            site_priorities,
        )
        self.target_absolute_vs_vt_scaled_pbw_over_actual_weight.add(
            target_absolute,
            vt_scaled_pbw_over_actual_weight,
            site_priorities,
        )
        self.target_ml_per_kg_pbw.add(
            target_div_pbw,
            site_priorities,
            None,
        )
        self.delivered_vt_ml_per_kg_pbw.add(
            vt_div_pbw,
            site_priorities,
            None,
        )
        self.delivered_vt_ml_per_kg_actual_weight.add(
            vt_div_actual_weight,
            site_priorities,
            None,
        )


@dataclass
class _TargetedScaleAccumulator:
    rule: TargetedScaleAudit
    sample_capacity: int
    max_examples: int
    values_considered_for_transformation_count: int = 0
    numerically_changed_count: int = 0
    before_outside_expected_range_count: int = 0
    after_outside_expected_range_count: int = 0
    candidate: _NumericAccumulator = field(init=False)

    def __post_init__(self) -> None:
        self.candidate = _NumericAccumulator(
            self.sample_capacity, self.max_examples
        )

    def add(self, values: np.ndarray, priorities: np.ndarray) -> None:
        raw = values.astype(np.float64, copy=False)
        candidate = raw.copy()
        finite = np.isfinite(raw)
        transform = finite.copy()
        if self.rule.operation == "multiply":
            candidate[transform] *= self.rule.factor
        elif self.rule.operation == "divide":
            candidate[transform] /= self.rule.factor
        elif self.rule.operation == "divide_when_above":
            assert self.rule.threshold is not None
            transform &= raw > self.rule.threshold
            candidate[transform] /= self.rule.factor
        else:  # pragma: no cover - rejected by policy validation
            raise ValueError(
                f"Unknown targeted scale operation: {self.rule.operation}"
            )

        self.values_considered_for_transformation_count += int(transform.sum())
        self.numerically_changed_count += int(
            np.count_nonzero(candidate[transform] != raw[transform])
        )
        self.before_outside_expected_range_count += _outside_range_count(
            raw, self.rule.expected_min, self.rule.expected_max
        )
        self.after_outside_expected_range_count += _outside_range_count(
            candidate, self.rule.expected_min, self.rule.expected_max
        )
        self.candidate.add(candidate, priorities, None)


@dataclass
class _RowLevelScaleEntryAccumulator:
    rule: RowLevelScaleEntryAudit
    max_examples: int
    finite_count: int = 0
    already_within_recovery_window_count: int = 0
    outside_recovery_window_count: int = 0
    recoverable_candidate_count: int = 0
    unrecoverable_outside_count: int = 0
    candidate_examples: set[tuple[float, float]] = field(default_factory=set)

    def add(self, values: np.ndarray) -> None:
        raw = values.astype(np.float64, copy=False)
        finite = np.isfinite(raw)
        transformed = raw.copy()
        if self.rule.operation == "divide":
            transformed[finite] /= self.rule.factor
        elif self.rule.operation == "multiply":
            transformed[finite] *= self.rule.factor
        else:  # pragma: no cover - rejected by policy validation
            raise ValueError(
                f"Unknown row-level scale operation: {self.rule.operation}"
            )

        raw_within = (
            finite
            & (raw >= self.rule.recovery_min)
            & (raw <= self.rule.recovery_max)
        )
        transformed_within = (
            finite
            & (transformed >= self.rule.recovery_min)
            & (transformed <= self.rule.recovery_max)
        )
        recoverable = finite & ~raw_within & transformed_within
        outside = finite & ~raw_within

        self.finite_count += int(finite.sum())
        self.already_within_recovery_window_count += int(raw_within.sum())
        self.outside_recovery_window_count += int(outside.sum())
        self.recoverable_candidate_count += int(recoverable.sum())
        self.unrecoverable_outside_count += int((outside & ~recoverable).sum())

        if recoverable.any() and len(self.candidate_examples) < self.max_examples:
            pairs = np.column_stack((raw[recoverable], transformed[recoverable]))
            for raw_value, transformed_value in np.unique(pairs, axis=0):
                self.candidate_examples.add(
                    (float(raw_value), float(transformed_value))
                )
                if len(self.candidate_examples) >= self.max_examples:
                    break

    def result(self, hospital: str) -> dict[str, Any]:
        return {
            "hospital": hospital,
            "finite_count": self.finite_count,
            "already_within_recovery_window_count": (
                self.already_within_recovery_window_count
            ),
            "outside_recovery_window_count": self.outside_recovery_window_count,
            "recoverable_candidate_count": self.recoverable_candidate_count,
            "unrecoverable_outside_count": self.unrecoverable_outside_count,
            "candidate_examples": [
                {"raw": raw, "hypothetical_recovered": recovered}
                for raw, recovered in sorted(self.candidate_examples)
            ],
        }


def _scale_transformations(
    policy: ScaleEntryDiscoveryPolicy,
) -> tuple[tuple[str, float, str], ...]:
    transformations: list[tuple[str, float, str]] = []
    for factor in policy.factors:
        label = str(int(factor)) if factor.is_integer() else str(factor)
        transformations.extend(
            (
                ("multiply", factor, f"multiply_by_{label}"),
                ("divide", factor, f"divide_by_{label}"),
            )
        )
    return tuple(transformations)


@dataclass
class _ScaleEntryDiscoveryAccumulator:
    rule: InvalidValueRule
    policy: ScaleEntryDiscoveryPolicy
    sample_capacity: int
    max_examples: int
    finite_count: int = 0
    in_range_count: int = 0
    raw_outside_count: int = 0
    uniquely_recoverable_count: int = 0
    ambiguous_transform_count: int = 0
    unrecoverable_count: int = 0
    unique_counts: dict[str, int] = field(default_factory=dict)
    candidate_examples: dict[str, set[tuple[float, float]]] = field(
        default_factory=dict
    )
    ambiguous_examples: set[tuple[float, tuple[str, ...]]] = field(
        default_factory=set
    )
    valid_values: _NumericAccumulator = field(init=False)
    candidate_values: dict[str, _NumericAccumulator] = field(init=False)

    def __post_init__(self) -> None:
        self.valid_values = _NumericAccumulator(
            self.sample_capacity,
            self.max_examples,
        )
        self.candidate_values = {
            transformation_id: _NumericAccumulator(
                self.sample_capacity,
                self.max_examples,
            )
            for _, _, transformation_id in _scale_transformations(self.policy)
        }
        self.unique_counts = {
            transformation_id: 0
            for _, _, transformation_id in _scale_transformations(self.policy)
        }
        self.candidate_examples = {
            transformation_id: set()
            for _, _, transformation_id in _scale_transformations(self.policy)
        }

    def add(
        self,
        values: np.ndarray,
        priorities: np.ndarray,
        *,
        collect_hits: bool = False,
    ) -> list["_ScaleEntryCandidateHit"]:
        assert self.rule.hard_min is not None
        assert self.rule.hard_max is not None
        raw = values.astype(np.float64, copy=False)
        finite = np.isfinite(raw)
        in_range = (
            finite
            & (raw >= self.rule.hard_min)
            & (raw <= self.rule.hard_max)
        )
        outside = finite & ~in_range
        self.finite_count += int(finite.sum())
        self.in_range_count += int(in_range.sum())
        self.raw_outside_count += int(outside.sum())

        self.valid_values.add(raw[in_range], priorities[in_range], None)
        if not outside.any():
            return []

        transformations = _scale_transformations(self.policy)
        outside_positions = np.flatnonzero(outside)
        outside_raw = raw[outside]
        outside_priorities = priorities[outside]
        transformed_values: list[np.ndarray] = []
        candidate_masks: list[np.ndarray] = []
        candidate_counts = np.zeros(outside_raw.size, dtype=np.int8)
        for operation, factor, _ in transformations:
            transformed = outside_raw.copy()
            if operation == "multiply":
                transformed *= factor
            else:
                transformed /= factor
            candidate = (
                (transformed >= self.rule.hard_min)
                & (transformed <= self.rule.hard_max)
            )
            transformed_values.append(transformed)
            candidate_masks.append(candidate)
            candidate_counts += candidate.astype(np.int8)

        unique = candidate_counts == 1
        ambiguous = candidate_counts > 1
        unrecoverable = candidate_counts == 0
        self.uniquely_recoverable_count += int(unique.sum())
        self.ambiguous_transform_count += int(ambiguous.sum())
        self.unrecoverable_count += int(unrecoverable.sum())

        if ambiguous.any() and len(self.ambiguous_examples) < self.max_examples:
            for position in np.flatnonzero(ambiguous):
                possible = tuple(
                    transformation_id
                    for (_, _, transformation_id), candidate in zip(
                        transformations,
                        candidate_masks,
                        strict=True,
                    )
                    if bool(candidate[position])
                )
                self.ambiguous_examples.add(
                    (float(outside_raw[position]), possible)
                )
                if len(self.ambiguous_examples) >= self.max_examples:
                    break

        hits: list[_ScaleEntryCandidateHit] = []
        for (_, _, transformation_id), transformed, candidate in zip(
            transformations,
            transformed_values,
            candidate_masks,
            strict=True,
        ):
            unique_for_transform = unique & candidate
            count = int(unique_for_transform.sum())
            self.unique_counts[transformation_id] += count
            if count:
                self.candidate_values[transformation_id].add(
                    transformed[unique_for_transform],
                    outside_priorities[unique_for_transform],
                    None,
                )
            if count and (
                len(self.candidate_examples[transformation_id])
                < self.max_examples
            ):
                pairs = np.column_stack(
                    (
                        outside_raw[unique_for_transform],
                        transformed[unique_for_transform],
                    )
                )
                for raw_value, recovered_value in np.unique(pairs, axis=0):
                    self.candidate_examples[transformation_id].add(
                        (float(raw_value), float(recovered_value))
                    )
                    if (
                        len(self.candidate_examples[transformation_id])
                        >= self.max_examples
                    ):
                        break
            if collect_hits and count:
                for outside_position in np.flatnonzero(unique_for_transform):
                    hits.append(
                        _ScaleEntryCandidateHit(
                            relative_position=int(
                                outside_positions[outside_position]
                            ),
                            transformation_id=transformation_id,
                            raw_value=float(outside_raw[outside_position]),
                            recovered_value=float(
                                transformed[outside_position]
                            ),
                        )
                    )
        return hits


@dataclass(frozen=True)
class _ScaleEntryCandidateHit:
    relative_position: int
    transformation_id: str
    raw_value: float
    recovered_value: float


@dataclass
class _CandidateContextRecord:
    column: str
    hospital: str
    stay_id: str
    time_minutes: float
    transformation_id: str
    raw_value: float
    recovered_value: float
    previous_gap_hours: float | None = None
    previous_values: list[float] = field(default_factory=list)
    next_gap_hours: float | None = None
    next_values: list[float] = field(default_factory=list)
    related_values: dict[str, list[float]] = field(default_factory=dict)


@dataclass
class _CandidateContextCollector:
    policy: CandidateContextAuditPolicy
    stay_id_column: str
    time_column: str
    records: list[_CandidateContextRecord] = field(default_factory=list)
    selected_candidate_count: int = 0
    invalid_key_count: int = 0
    omitted_due_to_cap_count: int = 0
    second_pass_rows_scanned: int = 0

    def field_policy(
        self, column: str
    ) -> CandidateContextFieldPolicy | None:
        return next(
            (item for item in self.policy.fields if item.column == column),
            None,
        )

    def should_collect(self, column: str, hospital: str) -> bool:
        field_policy = self.field_policy(column)
        return bool(
            field_policy is not None
            and hospital not in field_policy.excluded_hospitals
        )

    def add(
        self,
        column: str,
        hospital: str,
        hits: list[_ScaleEntryCandidateHit],
        site_positions: np.ndarray,
        frame: pd.DataFrame,
    ) -> None:
        self.selected_candidate_count += len(hits)
        for hit in hits:
            frame_position = int(site_positions[hit.relative_position])
            stay_value = frame[self.stay_id_column].iloc[frame_position]
            time_value = pd.to_numeric(
                pd.Series(
                    [frame[self.time_column].iloc[frame_position]],
                    dtype="object",
                ),
                errors="coerce",
            ).iloc[0]
            if pd.isna(stay_value) or not np.isfinite(float(time_value)):
                self.invalid_key_count += 1
                continue
            if len(self.records) >= self.policy.maximum_candidate_rows:
                self.omitted_due_to_cap_count += 1
                continue
            self.records.append(
                _CandidateContextRecord(
                    column=column,
                    hospital=hospital,
                    stay_id=str(stay_value),
                    time_minutes=float(time_value),
                    transformation_id=hit.transformation_id,
                    raw_value=hit.raw_value,
                    recovered_value=hit.recovered_value,
                )
            )


@dataclass
class _TargetedBucketAccumulator:
    rule: TargetedBucketAudit
    finite_count: int = 0
    counts: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        self.counts = np.zeros(len(self.rule.cut_points) + 1, dtype=np.int64)

    def add(self, values: np.ndarray) -> None:
        finite = values[np.isfinite(values)].astype(np.float64, copy=False)
        self.finite_count += int(finite.size)
        if finite.size:
            buckets = np.searchsorted(self.rule.cut_points, finite, side="left")
            self.counts += np.bincount(
                buckets, minlength=len(self.rule.cut_points) + 1
            )


def _outside_range_count(
    values: np.ndarray,
    minimum: float | None,
    maximum: float | None,
) -> int:
    finite = values[np.isfinite(values)]
    outside = np.zeros(finite.size, dtype=bool)
    if minimum is not None:
        outside |= finite < minimum
    if maximum is not None:
        outside |= finite > maximum
    return int(outside.sum())


def _open_translated_input(
    directory: Path,
    contract: TranslatedInputContract,
) -> PooledInput:
    tables: dict[str, PooledTable] = {}
    for name, table_contract in (("static", contract.static), ("dynamic", contract.dynamic)):
        path = directory / table_contract.filename
        if not path.is_file():
            raise InputReadError(f"Translated {name} file does not exist: {path}")
        try:
            parquet = pq.ParquetFile(path)
        except Exception as exc:
            raise InputReadError(f"Could not open translated {name} Parquet file {path}: {exc}") from exc
        tables[name] = PooledTable(name=name, path=path, parquet=parquet)
    return PooledInput(static=tables["static"], dynamic=tables["dynamic"])


def _numeric_columns(table: PooledTable, excluded: set[str]) -> list[str]:
    columns = []
    for field in table.schema:
        if field.name in excluded:
            continue
        if (
            pa.types.is_integer(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_decimal(field.type)
        ) and not pa.types.is_boolean(field.type):
            columns.append(field.name)
    return columns


def _contract_numeric_columns(
    table_contract: Any,
    dataset_context: str,
    excluded: set[str],
) -> list[str]:
    columns = []
    for column in table_contract.columns:
        if column.name in excluded:
            continue
        type_name = column.arrow_type_for(dataset_context)
        arrow_type = pa.timestamp("us") if type_name == "timestamp[us]" else pa.type_for_alias(type_name)
        if (
            pa.types.is_integer(arrow_type)
            or pa.types.is_floating(arrow_type)
            or pa.types.is_decimal(arrow_type)
        ) and not pa.types.is_boolean(arrow_type):
            columns.append(column.name)
    return columns


def _rule_by_column(policy: DataQualityPolicy) -> dict[str, InvalidValueRule]:
    result: dict[str, InvalidValueRule] = {}
    for rule in policy.invalid_value_rules:
        for column in rule.columns:
            if column in result:
                raise ValueError(f"Multiple invalid-value rules target {column!r}")
            result[column] = rule
    return result


def _row_priorities(frame: pd.DataFrame, columns: list[str]) -> np.ndarray:
    return pd.util.hash_pandas_object(
        frame[columns], index=False, categorize=True
    ).to_numpy(dtype=np.uint64, copy=False)


def _sentinel_counts_for_batch(
    frame: pd.DataFrame,
    hospital: str,
    positions: np.ndarray,
    policy: DataQualityPolicy,
    counts: dict[tuple[str, str], int],
) -> None:
    subset = frame.iloc[positions]
    for rule in policy.sentinels:
        if rule.value_kind == "numeric":
            values = pd.to_numeric(subset[rule.column], errors="coerce")
            count = int(values.eq(float(rule.value)).sum())
        else:
            values = subset[rule.column].astype("string").str.strip()
            accepted = {str(rule.value)}
            if str(rule.value) == "-1":
                accepted.add("-1.0")
            count = int(values.isin(accepted).sum())
        counts[(rule.column, hospital)] = counts.get((rule.column, hospital), 0) + count


def _array(frame: pd.DataFrame, column: str, positions: np.ndarray) -> np.ndarray:
    return pd.to_numeric(frame[column], errors="coerce").to_numpy(
        dtype=np.float64, na_value=np.nan
    )[positions]


def _build_predicted_body_weight_reference(
    table: PooledTable,
    audit_policy: PredictedBodyWeightTidalVolumeAudit,
    policy: DataQualityPolicy,
    config: CrossHospitalAuditConfig,
) -> _PredictedBodyWeightReference:
    columns = [
        policy.stay_id_column,
        policy.hospital_column,
        audit_policy.sex_column,
        audit_policy.height_column,
        audit_policy.actual_weight_column,
    ]
    values_by_stay: dict[str, tuple[float, float]] = {}
    target_rows = 0
    duplicate_stays = 0
    sex_counts: dict[str, int] = {}
    unsupported_sex_count = 0
    invalid_height_count = 0
    valid_pbw_count = 0
    invalid_actual_weight_count = 0
    valid_actual_weight_count = 0
    pbw_profile = _NumericAccumulator(
        config.quantile_sample_size_per_hospital_variable,
        config.max_examples,
    )

    for batch in table.parquet.iter_batches(
        batch_size=config.batch_size,
        columns=columns,
    ):
        frame = batch.to_pandas()
        hospital = frame[policy.hospital_column].astype("string")
        positions = np.flatnonzero(
            hospital.to_numpy() == audit_policy.hospital
        )
        if positions.size == 0:
            continue
        target_rows += int(positions.size)
        subset = frame.iloc[positions]
        stay_ids = (
            subset[policy.stay_id_column]
            .astype("string")
            .fillna("")
            .tolist()
        )
        sex = (
            subset[audit_policy.sex_column]
            .astype("string")
            .str.strip()
            .str.lower()
            .fillna("")
            .to_numpy()
        )
        height = pd.to_numeric(
            subset[audit_policy.height_column], errors="coerce"
        ).to_numpy(dtype=np.float64, na_value=np.nan)
        actual_weight = pd.to_numeric(
            subset[audit_policy.actual_weight_column], errors="coerce"
        ).to_numpy(dtype=np.float64, na_value=np.nan, copy=True)
        for value in sex:
            label = str(value) if str(value) else "<missing>"
            sex_counts[label] = sex_counts.get(label, 0) + 1

        supported_sex = (sex == audit_policy.male_value) | (
            sex == audit_policy.female_value
        )
        plausible_height = (
            np.isfinite(height)
            & (height >= audit_policy.plausible_height_min_cm)
            & (height <= audit_policy.plausible_height_max_cm)
        )
        valid_pbw = supported_sex & plausible_height
        pbw = np.full(height.shape, np.nan, dtype=np.float64)
        intercept = np.where(
            sex == audit_policy.male_value,
            audit_policy.male_intercept_kg,
            audit_policy.female_intercept_kg,
        )
        pbw[valid_pbw] = intercept[valid_pbw] + (
            audit_policy.height_coefficient_kg_per_cm
            * (height[valid_pbw] - audit_policy.height_center_cm)
        )
        pbw[~np.isfinite(pbw) | (pbw <= 0)] = np.nan
        valid_pbw = np.isfinite(pbw)
        plausible_actual_weight = (
            np.isfinite(actual_weight)
            & (
                actual_weight
                >= audit_policy.plausible_actual_weight_min_kg
            )
            & (
                actual_weight
                <= audit_policy.plausible_actual_weight_max_kg
            )
        )
        actual_weight[~plausible_actual_weight] = np.nan

        unsupported_sex_count += int((~supported_sex).sum())
        invalid_height_count += int((~plausible_height).sum())
        valid_pbw_count += int(valid_pbw.sum())
        invalid_actual_weight_count += int(
            (~plausible_actual_weight).sum()
        )
        valid_actual_weight_count += int(plausible_actual_weight.sum())
        priorities = _row_priorities(subset, [policy.stay_id_column])
        pbw_profile.add(pbw, priorities, None)

        for stay_id, pbw_value, weight_value in zip(
            stay_ids,
            pbw,
            actual_weight,
            strict=True,
        ):
            if stay_id in values_by_stay:
                duplicate_stays += 1
            values_by_stay[str(stay_id)] = (
                float(pbw_value),
                float(weight_value),
            )

    return _PredictedBodyWeightReference(
        policy=audit_policy,
        values_by_stay=values_by_stay,
        target_hospital_static_rows=target_rows,
        duplicate_stay_count=duplicate_stays,
        sex_value_counts=dict(sorted(sex_counts.items())),
        unsupported_or_missing_sex_count=unsupported_sex_count,
        implausible_or_missing_height_count=invalid_height_count,
        valid_predicted_body_weight_count=valid_pbw_count,
        implausible_or_missing_actual_weight_count=(
            invalid_actual_weight_count
        ),
        valid_actual_weight_count=valid_actual_weight_count,
        predicted_body_weight_profile=pbw_profile.profile(
            "static_reference",
            audit_policy.hospital,
            "predicted_body_weight_kg",
        ),
    )


def _collect_predicted_body_weight_tidal_volume_audit(
    table: PooledTable,
    reference: _PredictedBodyWeightReference,
    policy: DataQualityPolicy,
    config: CrossHospitalAuditConfig,
) -> tuple[dict[str, Any], int]:
    audit_policy = reference.policy
    columns = [
        policy.stay_id_column,
        policy.hospital_column,
        "minutes_since_icu_admission",
        audit_policy.vt_column,
        audit_policy.vt_per_kg_column,
        audit_policy.target_absolute_vt_column,
    ]
    accumulator = _PredictedBodyWeightTidalVolumeAccumulator(
        audit_policy,
        config.quantile_sample_size_per_hospital_variable,
        config.max_examples,
    )
    rows_scanned = 0
    progress_interval = max(table.row_count // 10, config.batch_size)
    next_progress = progress_interval
    for batch in table.parquet.iter_batches(
        batch_size=config.batch_size,
        columns=columns,
    ):
        frame = batch.to_pandas()
        priorities = _row_priorities(
            frame,
            [policy.stay_id_column, "minutes_since_icu_admission"],
        )
        hospitals = frame[policy.hospital_column].astype("string").to_numpy()
        positions = np.flatnonzero(hospitals == audit_policy.hospital)
        if positions.size:
            accumulator.add(
                frame,
                positions,
                priorities,
                reference,
                policy.stay_id_column,
            )
        rows_scanned += batch.num_rows
        if rows_scanned >= next_progress or rows_scanned == table.row_count:
            print(
                "PBW formula-scanned "
                f"{rows_scanned:,}/{table.row_count:,} dynamic rows."
            )
            next_progress += progress_interval

    target_comparison = accumulator.target_absolute_vs_six_pbw.result(
        audit_policy.hospital
    )
    normalized_comparison = (
        accumulator.reported_vt_per_kg_vs_vt_div_pbw.result(
            audit_policy.hospital
        )
    )
    actual_weight_comparison = (
        accumulator.target_absolute_vs_vt_scaled_pbw_over_actual_weight.result(
            audit_policy.hospital
        )
    )
    result = {
        "id": audit_policy.audit_id,
        "status": "hypothesis_evidence_only_not_approved_for_cleaning",
        "hospital": audit_policy.hospital,
        "approval_status": audit_policy.approval_status,
        "interpretation_status": (
            "not_interpretable_mock_columns_were_independently_shuffled"
            if config.dataset_context == "mock"
            else "eligible_for_relationship_interpretation"
        ),
        "evidence": audit_policy.evidence,
        "formula_source": "NIH ARDS Network predicted-body-weight equation",
        "formula_reference_url": (
            "https://biolincc.nhlbi.nih.gov/media/studies/eden/"
            "Protocol%20EDEN%20only.pdf"
        ),
        "formula": {
            "male_pbw_kg": (
                f"{audit_policy.male_intercept_kg} + "
                f"{audit_policy.height_coefficient_kg_per_cm} * "
                f"(height_cm - {audit_policy.height_center_cm})"
            ),
            "female_pbw_kg": (
                f"{audit_policy.female_intercept_kg} + "
                f"{audit_policy.height_coefficient_kg_per_cm} * "
                f"(height_cm - {audit_policy.height_center_cm})"
            ),
            "hypothesized_target_absolute_vt_ml": (
                f"{audit_policy.target_ml_per_kg_pbw} * PBW_kg"
            ),
        },
        "configured_plausibility_windows": {
            "height_cm": [
                audit_policy.plausible_height_min_cm,
                audit_policy.plausible_height_max_cm,
            ],
            "actual_weight_kg": [
                audit_policy.plausible_actual_weight_min_kg,
                audit_policy.plausible_actual_weight_max_kg,
            ],
        },
        "static_reference": {
            "target_hospital_static_rows": (
                reference.target_hospital_static_rows
            ),
            "duplicate_stay_count": reference.duplicate_stay_count,
            "sex_value_counts": reference.sex_value_counts,
            "unsupported_or_missing_sex_count": (
                reference.unsupported_or_missing_sex_count
            ),
            "implausible_or_missing_height_count": (
                reference.implausible_or_missing_height_count
            ),
            "valid_predicted_body_weight_count": (
                reference.valid_predicted_body_weight_count
            ),
            "implausible_or_missing_actual_weight_count": (
                reference.implausible_or_missing_actual_weight_count
            ),
            "valid_actual_weight_count": (
                reference.valid_actual_weight_count
            ),
            "predicted_body_weight_profile": (
                reference.predicted_body_weight_profile
            ),
        },
        "dynamic_join": {
            "target_hospital_dynamic_rows": (
                accumulator.target_hospital_dynamic_rows
            ),
            "rows_without_static_reference": (
                accumulator.rows_without_static_reference
            ),
            "rows_without_valid_predicted_body_weight": (
                accumulator.rows_without_valid_pbw
            ),
            "rows_without_valid_actual_weight": (
                accumulator.rows_without_valid_actual_weight
            ),
            "all_dynamic_rows_scanned": rows_scanned == table.row_count,
            "rows_scanned": rows_scanned,
        },
        "comparisons": {
            "target_absolute_vt_vs_six_ml_per_kg_pbw": {
                "left": audit_policy.target_absolute_vt_column,
                "right": (
                    f"{audit_policy.target_ml_per_kg_pbw} * PBW_kg"
                ),
                "absolute_tolerance_ml": (
                    audit_policy.absolute_tolerance_ml
                ),
                **target_comparison,
            },
            "reported_vt_per_kg_vs_vt_div_pbw": {
                "left": audit_policy.vt_per_kg_column,
                "right": f"{audit_policy.vt_column} / PBW_kg",
                "absolute_tolerance_ml_per_kg": (
                    audit_policy.normalized_tolerance_ml_per_kg
                ),
                **normalized_comparison,
            },
            "target_absolute_vt_vs_vt_scaled_by_pbw_over_actual_weight": {
                "left": audit_policy.target_absolute_vt_column,
                "right": (
                    f"{audit_policy.vt_column} * PBW_kg / "
                    f"{audit_policy.actual_weight_column}"
                ),
                "absolute_tolerance_ml": (
                    audit_policy.absolute_tolerance_ml
                ),
                "interpretation": (
                    "Direct test of whether the target differs from delivered "
                    "vt only through replacing actual weight with PBW."
                ),
                **actual_weight_comparison,
            },
        },
        "normalized_profiles": {
            "target_absolute_vt_div_pbw": (
                accumulator.target_ml_per_kg_pbw.profile(
                    "cross_table_formula",
                    audit_policy.hospital,
                    "target_absolute_vt_div_pbw",
                )
            ),
            "vt_div_pbw": accumulator.delivered_vt_ml_per_kg_pbw.profile(
                "cross_table_formula",
                audit_policy.hospital,
                "vt_div_pbw",
            ),
            "vt_div_actual_weight": (
                accumulator.delivered_vt_ml_per_kg_actual_weight.profile(
                    "cross_table_formula",
                    audit_policy.hospital,
                    "vt_div_actual_weight",
                )
            ),
        },
        "identifiers_written_to_report": False,
        "values_written_to_data": False,
    }
    return result, rows_scanned


def _update_relationships(
    table_name: str,
    frame: pd.DataFrame,
    hospitals: np.ndarray,
    priorities: np.ndarray,
    policy: DataQualityPolicy,
    sample_capacity: int,
    accumulators: dict[tuple[str, str], _DifferenceAccumulator],
) -> None:
    relationships = [
        item for item in policy.relationship_audits if item.get("table") == table_name
    ]
    if not relationships:
        return
    for hospital in sorted(pd.unique(hospitals).tolist()):
        positions = np.flatnonzero(hospitals == hospital)
        site_priorities = priorities[positions]
        for relationship in relationships:
            hospital_scope = relationship.get("hospital_scope")
            if hospital_scope is not None and str(hospital) not in hospital_scope:
                continue
            relationship_id = str(relationship["id"])
            fields = list(relationship["fields"])
            tolerance = float(relationship.get("absolute_tolerance", 0.0))
            key = (relationship_id, str(hospital))
            accumulator = accumulators.setdefault(
                key,
                _DifferenceAccumulator(sample_capacity, tolerance),
            )
            formula = relationship["formula"]
            if formula == "delta_p_computed_equals_insp_pressure_minus_peep":
                left = _array(frame, fields[0], positions)
                right = _array(frame, fields[1], positions) - _array(
                    frame, fields[2], positions
                )
            elif formula == "isofa_total_equals_sum_of_six_components_when_all_present":
                left = _array(frame, fields[0], positions)
                components = np.column_stack(
                    [_array(frame, column, positions) for column in fields[1:]]
                )
                complete = np.isfinite(components).all(axis=1)
                right = np.full(left.shape, np.nan, dtype=np.float64)
                right[complete] = components[complete].sum(axis=1)
            else:
                left = _array(frame, fields[0], positions)
                right = _array(frame, fields[1], positions)
            accumulator.add(left, right, site_priorities)


def _scan_table(
    table_name: str,
    table: PooledTable,
    profile_columns: list[str],
    policy: DataQualityPolicy,
    config: CrossHospitalAuditConfig,
    invalid_rules: dict[str, InvalidValueRule],
    relationship_accumulators: dict[tuple[str, str], _DifferenceAccumulator],
    targeted_scale_accumulators: dict[str, _TargetedScaleAccumulator],
    row_level_scale_entry_accumulators: dict[
        tuple[str, str], _RowLevelScaleEntryAccumulator
    ],
    scale_entry_discovery_accumulators: dict[
        tuple[str, str], _ScaleEntryDiscoveryAccumulator
    ],
    targeted_bucket_accumulators: dict[str, _TargetedBucketAccumulator],
    candidate_context_collector: _CandidateContextCollector,
) -> tuple[
    dict[tuple[str, str], _NumericAccumulator],
    dict[tuple[str, str], int],
    dict[str, int],
    int,
]:
    required = {policy.hospital_column, policy.stay_id_column, *profile_columns}
    for item in policy.relationship_audits:
        if item.get("table") == table_name:
            required.update(str(column) for column in item["fields"])
    if table_name == "static":
        required.update(item.column for item in policy.sentinels)
    key_columns = [policy.stay_id_column]
    if table_name == "dynamic":
        key_columns.append("minutes_since_icu_admission")
        required.add("minutes_since_icu_admission")
    ordered_columns = [name for name in table.schema.names if name in required]
    accumulators: dict[tuple[str, str], _NumericAccumulator] = {}
    sentinel_counts: dict[tuple[str, str], int] = {}
    hospital_rows: dict[str, int] = {}
    scanned_rows = 0
    progress_interval = max(table.row_count // 10, config.batch_size)
    next_progress = progress_interval
    targeted_scales_by_site_column: dict[
        tuple[str, str], list[_TargetedScaleAccumulator]
    ] = {}
    for accumulator in targeted_scale_accumulators.values():
        targeted_scales_by_site_column.setdefault(
            (accumulator.rule.hospital, accumulator.rule.column), []
        ).append(accumulator)
    row_level_scale_rules_by_column: dict[
        str, list[RowLevelScaleEntryAudit]
    ] = {}
    for rule in policy.row_level_scale_entry_audits:
        row_level_scale_rules_by_column.setdefault(rule.column, []).append(rule)
    targeted_buckets_by_site_column: dict[
        tuple[str, str], list[_TargetedBucketAccumulator]
    ] = {}
    for accumulator in targeted_bucket_accumulators.values():
        targeted_buckets_by_site_column.setdefault(
            (accumulator.rule.hospital, accumulator.rule.column), []
        ).append(accumulator)
    for batch in table.parquet.iter_batches(
        batch_size=config.batch_size,
        columns=ordered_columns,
    ):
        frame = batch.to_pandas()
        priorities = _row_priorities(frame, key_columns)
        hospitals = frame[policy.hospital_column].astype("string").to_numpy()
        matrix = frame[profile_columns].to_numpy(dtype=np.float64, na_value=np.nan)
        for hospital in sorted(pd.unique(hospitals).tolist()):
            positions = np.flatnonzero(hospitals == hospital)
            hospital_name = str(hospital)
            hospital_rows[hospital_name] = hospital_rows.get(hospital_name, 0) + int(
                positions.size
            )
            site_priorities = priorities[positions]
            for column_index, column in enumerate(profile_columns):
                site_values = matrix[positions, column_index]
                key = (column, hospital_name)
                accumulator = accumulators.setdefault(
                    key,
                    _NumericAccumulator(
                        config.quantile_sample_size_per_hospital_variable,
                        config.max_examples,
                    ),
                )
                accumulator.add(
                    site_values,
                    site_priorities,
                    invalid_rules.get(column),
                )
                discovery_rule = invalid_rules.get(column)
                if (
                    discovery_rule is not None
                    and discovery_rule.hard_min is not None
                    and discovery_rule.hard_max is not None
                ):
                    discovery_key = (column, hospital_name)
                    discovery = scale_entry_discovery_accumulators.setdefault(
                        discovery_key,
                        _ScaleEntryDiscoveryAccumulator(
                            discovery_rule,
                            policy.scale_entry_discovery,
                            config.quantile_sample_size_per_hospital_variable,
                            config.max_examples,
                        ),
                    )
                    collect_context = (
                        table_name == "dynamic"
                        and candidate_context_collector.should_collect(
                            column,
                            hospital_name,
                        )
                    )
                    candidate_hits = discovery.add(
                        site_values,
                        site_priorities,
                        collect_hits=collect_context,
                    )
                    if candidate_hits:
                        candidate_context_collector.add(
                            column,
                            hospital_name,
                            candidate_hits,
                            positions,
                            frame,
                        )
                targeted_key = (hospital_name, column)
                for targeted in targeted_scales_by_site_column.get(targeted_key, []):
                    targeted.add(site_values, site_priorities)
                for rule in row_level_scale_rules_by_column.get(column, []):
                    row_level_key = (rule.audit_id, hospital_name)
                    row_level = row_level_scale_entry_accumulators.setdefault(
                        row_level_key,
                        _RowLevelScaleEntryAccumulator(
                            rule,
                            config.max_examples,
                        ),
                    )
                    row_level.add(site_values)
                for targeted in targeted_buckets_by_site_column.get(targeted_key, []):
                    targeted.add(site_values)
            if table_name == "static":
                _sentinel_counts_for_batch(
                    frame,
                    hospital_name,
                    positions,
                    policy,
                    sentinel_counts,
                )
        _update_relationships(
            table_name,
            frame,
            hospitals,
            priorities,
            policy,
            config.quantile_sample_size_per_hospital_variable,
            relationship_accumulators,
        )
        scanned_rows += batch.num_rows
        if scanned_rows >= next_progress or scanned_rows == table.row_count:
            print(
                f"Profiled {scanned_rows:,}/{table.row_count:,} {table_name} rows."
            )
            next_progress += progress_interval
    return accumulators, sentinel_counts, hospital_rows, scanned_rows


def _valid_context_values(
    values: np.ndarray,
    rule: InvalidValueRule | None,
) -> np.ndarray:
    valid = np.isfinite(values)
    if rule is None:
        return valid
    if rule.hard_min is not None:
        valid &= values >= rule.hard_min
    if rule.hard_max is not None:
        valid &= values <= rule.hard_max
    if rule.invalid_zero:
        valid &= values != 0
    return valid


def _update_nearest_context(
    record: _CandidateContextRecord,
    side: str,
    gap_hours: float,
    values: np.ndarray,
) -> None:
    gap_attribute = f"{side}_gap_hours"
    values_attribute = f"{side}_values"
    current_gap = getattr(record, gap_attribute)
    finite_values = [float(value) for value in values if np.isfinite(value)]
    if not finite_values:
        return
    if current_gap is None or gap_hours < current_gap - 1e-12:
        setattr(record, gap_attribute, float(gap_hours))
        setattr(record, values_attribute, finite_values)
    elif np.isclose(gap_hours, current_gap, rtol=0, atol=1e-12):
        getattr(record, values_attribute).extend(finite_values)


def _collect_candidate_context(
    table: PooledTable,
    collector: _CandidateContextCollector,
    policy: DataQualityPolicy,
    config: CrossHospitalAuditConfig,
    invalid_rules: dict[str, InvalidValueRule],
) -> None:
    if not collector.records:
        return
    records_by_stay: dict[str, list[_CandidateContextRecord]] = {}
    for record in collector.records:
        records_by_stay.setdefault(record.stay_id, []).append(record)
    candidate_stays = set(records_by_stay)
    candidate_columns = {record.column for record in collector.records}
    context_field_policies = {
        item.column: item for item in collector.policy.fields
    }
    related_columns = {
        related
        for column in candidate_columns
        for related in context_field_policies[column].related_fields
    }
    required = {
        policy.stay_id_column,
        policy.hospital_column,
        collector.time_column,
        *candidate_columns,
        *related_columns,
    }
    ordered_columns = [name for name in table.schema.names if name in required]
    maximum_gap_hours = max(collector.policy.windows_hours)
    progress_interval = max(table.row_count // 10, config.batch_size)
    next_progress = progress_interval
    scanned_rows = 0
    for batch in table.parquet.iter_batches(
        batch_size=config.batch_size,
        columns=ordered_columns,
    ):
        frame = batch.to_pandas()
        stay_series = frame[policy.stay_id_column].astype("string")
        relevant = stay_series.isin(candidate_stays).to_numpy(dtype=bool)
        if relevant.any():
            hospitals = frame[policy.hospital_column].astype("string")
            for stay_id in sorted(pd.unique(stay_series[relevant]).tolist()):
                stay_mask = stay_series.eq(stay_id).fillna(False).to_numpy(
                    dtype=bool
                )
                stay_records = records_by_stay[str(stay_id)]
                for record in stay_records:
                    positions = np.flatnonzero(
                        stay_mask
                        & hospitals.eq(record.hospital).fillna(False).to_numpy(
                            dtype=bool
                        )
                    )
                    if positions.size == 0:
                        continue
                    times = _array(
                        frame,
                        collector.time_column,
                        positions,
                    )
                    values = _array(frame, record.column, positions)
                    valid = _valid_context_values(
                        values,
                        invalid_rules.get(record.column),
                    ) & np.isfinite(times)
                    delta_hours = (times - record.time_minutes) / 60.0
                    previous = (
                        valid
                        & (delta_hours < 0)
                        & (delta_hours >= -maximum_gap_hours)
                    )
                    if previous.any():
                        gap = float(np.min(-delta_hours[previous]))
                        nearest = previous & np.isclose(
                            -delta_hours,
                            gap,
                            rtol=0,
                            atol=1e-12,
                        )
                        _update_nearest_context(
                            record,
                            "previous",
                            gap,
                            values[nearest],
                        )
                    following = (
                        valid
                        & (delta_hours > 0)
                        & (delta_hours <= maximum_gap_hours)
                    )
                    if following.any():
                        gap = float(np.min(delta_hours[following]))
                        nearest = following & np.isclose(
                            delta_hours,
                            gap,
                            rtol=0,
                            atol=1e-12,
                        )
                        _update_nearest_context(
                            record,
                            "next",
                            gap,
                            values[nearest],
                        )
                    exact_time = np.isfinite(times) & np.isclose(
                        times,
                        record.time_minutes,
                        rtol=0,
                        atol=1e-9,
                    )
                    if exact_time.any():
                        field_policy = context_field_policies[record.column]
                        for related in field_policy.related_fields:
                            related_values = _array(
                                frame,
                                related,
                                positions,
                            )
                            valid_related = _valid_context_values(
                                related_values,
                                invalid_rules.get(related),
                            )
                            selected = related_values[
                                exact_time & valid_related
                            ]
                            if selected.size:
                                record.related_values.setdefault(
                                    related,
                                    [],
                                ).extend(float(value) for value in selected)
        scanned_rows += batch.num_rows
        if scanned_rows >= next_progress or scanned_rows == table.row_count:
            print(
                f"Context-scanned {scanned_rows:,}/{table.row_count:,} "
                "dynamic rows."
            )
            next_progress += progress_interval
    collector.second_pass_rows_scanned = scanned_rows


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(np.median(np.asarray(values, dtype=np.float64)))


def _temporal_context_evidence(
    record: _CandidateContextRecord,
    window_hours: float,
) -> dict[str, Any]:
    previous_value = (
        _median_or_none(record.previous_values)
        if record.previous_gap_hours is not None
        and record.previous_gap_hours <= window_hours
        else None
    )
    next_value = (
        _median_or_none(record.next_values)
        if record.next_gap_hours is not None
        and record.next_gap_hours <= window_hours
        else None
    )
    neighbor_values = [
        value for value in (previous_value, next_value) if value is not None
    ]
    neighbor_median = _median_or_none(neighbor_values)
    raw_distance = (
        abs(record.raw_value - neighbor_median)
        if neighbor_median is not None
        else None
    )
    recovered_distance = (
        abs(record.recovered_value - neighbor_median)
        if neighbor_median is not None
        else None
    )
    if neighbor_median is None:
        classification = "no_temporal_context"
    elif recovered_distance is not None and raw_distance is not None and (
        recovered_distance < raw_distance
    ):
        classification = "recovered_is_closer"
    else:
        classification = "recovered_is_not_closer"
    return {
        "window_hours": window_hours,
        "classification": classification,
        "has_previous_neighbor": previous_value is not None,
        "has_next_neighbor": next_value is not None,
        "has_both_neighbors": (
            previous_value is not None and next_value is not None
        ),
        "previous_neighbor": (
            {
                "value": previous_value,
                "gap_hours": record.previous_gap_hours,
            }
            if previous_value is not None
            else None
        ),
        "next_neighbor": (
            {
                "value": next_value,
                "gap_hours": record.next_gap_hours,
            }
            if next_value is not None
            else None
        ),
        "neighbor_median": neighbor_median,
        "raw_absolute_distance": raw_distance,
        "recovered_absolute_distance": recovered_distance,
    }


def _related_context_evidence(
    record: _CandidateContextRecord,
    field_policy: CandidateContextFieldPolicy,
) -> dict[str, Any]:
    related_values = {
        column: _median_or_none(values)
        for column, values in sorted(record.related_values.items())
    }
    if field_policy.relationship == "none":
        return {
            "relationship": "none",
            "classification": "not_applicable",
            "related_values": {},
        }
    if field_policy.relationship == "compare_related_field":
        related_column = field_policy.related_fields[0]
        reference = related_values.get(related_column)
        if reference is None:
            classification = "insufficient_context"
            raw_distance = None
            recovered_distance = None
        else:
            raw_distance = abs(record.raw_value - reference)
            recovered_distance = abs(record.recovered_value - reference)
            classification = (
                "supports_recovery"
                if recovered_distance < raw_distance
                else "contradicts_recovery"
            )
        return {
            "relationship": "compare_related_field",
            "reference_field": related_column,
            "classification": classification,
            "related_values": related_values,
            "raw_absolute_distance": raw_distance,
            "recovered_absolute_distance": recovered_distance,
        }
    sbp = related_values.get("sbp")
    dbp = related_values.get("dbp")
    expected_map = (sbp + 2 * dbp) / 3 if sbp is not None and dbp is not None else None
    if expected_map is None:
        classification = "insufficient_context"
        raw_distance = None
        recovered_distance = None
        recovered_between = None
    else:
        raw_distance = abs(record.raw_value - expected_map)
        recovered_distance = abs(record.recovered_value - expected_map)
        recovered_between = min(sbp, dbp) <= record.recovered_value <= max(sbp, dbp)
        classification = (
            "supports_recovery"
            if recovered_between and recovered_distance < raw_distance
            else "contradicts_recovery"
        )
    return {
        "relationship": "map_from_sbp_dbp",
        "formula": "(sbp + 2 * dbp) / 3",
        "classification": classification,
        "related_values": related_values,
        "expected_map": expected_map,
        "raw_absolute_distance": raw_distance,
        "recovered_absolute_distance": recovered_distance,
        "recovered_between_dbp_and_sbp": recovered_between,
        "contradiction_reason": (
            "recovered_map_outside_dbp_sbp_interval"
            if recovered_between is False
            else None
        ),
    }


def _increment_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _candidate_context_results(
    collector: _CandidateContextCollector,
    dynamic_row_count: int,
    max_examples: int,
) -> dict[str, Any]:
    field_policies = {item.column: item for item in collector.policy.fields}
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    overall_temporal: dict[str, int] = {}
    overall_related: dict[str, int] = {}
    primary_window = max(collector.policy.windows_hours)
    for record in collector.records:
        key = (record.column, record.hospital, record.transformation_id)
        group = grouped.setdefault(
            key,
            {
                "column": record.column,
                "hospital": record.hospital,
                "transformation_id": record.transformation_id,
                "candidate_count": 0,
                "temporal_evidence_by_window": {
                    window: {
                        "window_hours": window,
                        "classification_counts": {},
                        "any_neighbor_count": 0,
                        "both_neighbors_count": 0,
                    }
                    for window in collector.policy.windows_hours
                },
                "related_evidence_classification_counts": {},
                "examples": [],
            },
        )
        group["candidate_count"] += 1
        temporal_evidence = [
            _temporal_context_evidence(record, window)
            for window in collector.policy.windows_hours
        ]
        for evidence in temporal_evidence:
            summary = group["temporal_evidence_by_window"][
                evidence["window_hours"]
            ]
            _increment_count(
                summary["classification_counts"],
                evidence["classification"],
            )
            if evidence["has_previous_neighbor"] or evidence["has_next_neighbor"]:
                summary["any_neighbor_count"] += 1
            if evidence["has_both_neighbors"]:
                summary["both_neighbors_count"] += 1
        primary_evidence = next(
            item
            for item in temporal_evidence
            if item["window_hours"] == primary_window
        )
        _increment_count(overall_temporal, primary_evidence["classification"])
        related_evidence = _related_context_evidence(
            record,
            field_policies[record.column],
        )
        _increment_count(
            group["related_evidence_classification_counts"],
            related_evidence["classification"],
        )
        _increment_count(overall_related, related_evidence["classification"])
        if len(group["examples"]) < max_examples:
            group["examples"].append(
                {
                    "raw": record.raw_value,
                    "hypothetical_recovered": record.recovered_value,
                    "temporal_evidence": temporal_evidence,
                    "same_row_related_evidence": related_evidence,
                }
            )
    groups = []
    for key in sorted(grouped):
        group = grouped[key]
        group["temporal_evidence_by_window"] = [
            group["temporal_evidence_by_window"][window]
            for window in collector.policy.windows_hours
        ]
        groups.append(group)
    requires_second_pass = bool(collector.records)
    full_second_pass = (
        collector.second_pass_rows_scanned == dynamic_row_count
        if requires_second_pass
        else True
    )
    collection_complete = (
        collector.invalid_key_count == 0
        and collector.omitted_due_to_cap_count == 0
        and full_second_pass
    )
    return {
        "status": "context_evidence_only_not_approved_for_cleaning",
        "eligibility_rule": (
            "uniquely recoverable candidates from configured bounded fields, "
            "excluding configured systematic hospital-field scopes"
        ),
        "candidate_values_are_hypothetical_and_not_written": True,
        "identifiers_written_to_report": False,
        "windows_hours": list(collector.policy.windows_hours),
        "primary_context_window_hours": primary_window,
        "maximum_candidate_rows": collector.policy.maximum_candidate_rows,
        "configured_fields": [
            {
                "column": item.column,
                "excluded_hospitals": list(item.excluded_hospitals),
                "related_fields": list(item.related_fields),
                "relationship": item.relationship,
            }
            for item in collector.policy.fields
        ],
        "selected_unique_candidate_count": (
            collector.selected_candidate_count
        ),
        "collected_candidate_count": len(collector.records),
        "invalid_candidate_key_count": collector.invalid_key_count,
        "omitted_due_to_cap_count": collector.omitted_due_to_cap_count,
        "second_pass_required": requires_second_pass,
        "second_pass_rows_scanned": collector.second_pass_rows_scanned,
        "expected_second_pass_rows": (
            dynamic_row_count if requires_second_pass else 0
        ),
        "collection_complete": collection_complete,
        "primary_temporal_classification_counts": overall_temporal,
        "related_evidence_classification_counts": overall_related,
        "groups": groups,
    }


def _distribution_issues(
    profiles: list[dict[str, Any]], policy: DataQualityPolicy
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    frame = pd.DataFrame(profiles)
    if frame.empty:
        return issues
    metrics = (*policy.distribution.metrics, *policy.distribution.additional_metrics)
    for (table, column), group in frame.groupby(["table", "column"], sort=True):
        eligible = group[
            group["finite_count"] >= policy.distribution.minimum_non_missing_values
        ].copy()
        if eligible["hospital"].nunique() < policy.distribution.minimum_hospitals:
            continue
        bounds: dict[str, tuple[float, float]] = {}
        for metric in metrics:
            values = pd.to_numeric(eligible[metric], errors="coerce").dropna()
            if values.shape[0] < policy.distribution.minimum_hospitals:
                continue
            q1 = float(values.quantile(0.25))
            q3 = float(values.quantile(0.75))
            metric_iqr = q3 - q1
            if metric_iqr == 0:
                bounds[metric] = (q1, q3)
            else:
                factor = policy.distribution.hospital_metric_iqr_fence_factor
                bounds[metric] = (q1 - factor * metric_iqr, q3 + factor * metric_iqr)
        for _, row in eligible.iterrows():
            flagged = []
            for metric, (lower, upper) in bounds.items():
                value = row[metric]
                if pd.notna(value) and (float(value) < lower or float(value) > upper):
                    flagged.append(metric)
            if flagged:
                issues.append(
                    {
                        "table": str(table),
                        "column": str(column),
                        "hospital": str(row["hospital"]),
                        "finite_count": int(row["finite_count"]),
                        "flagged_metrics": flagged,
                        "legacy_metrics_flagged": [
                            metric for metric in flagged if metric in policy.distribution.metrics
                        ],
                        "additional_metrics_flagged": [
                            metric
                            for metric in flagged
                            if metric in policy.distribution.additional_metrics
                        ],
                        "values": {
                            metric: _json_number(row[metric]) for metric in metrics
                        },
                        "bounds": {
                            metric: {"lower": lower, "upper": upper}
                            for metric, (lower, upper) in bounds.items()
                        },
                    }
                )
    return issues


def _invalid_value_findings(
    accumulators: dict[tuple[str, str], _NumericAccumulator],
    policy: DataQualityPolicy,
) -> list[dict[str, Any]]:
    by_column = _rule_by_column(policy)
    rows: list[dict[str, Any]] = []
    totals: dict[str, int] = {}
    for (column, _), accumulator in accumulators.items():
        if column in by_column:
            totals[column] = totals.get(column, 0) + accumulator.invalid_count
    for (column, hospital), accumulator in sorted(accumulators.items()):
        rule = by_column.get(column)
        if rule is None:
            continue
        total = totals.get(column, 0)
        rows.append(
            {
                "column": column,
                "legacy_name": rule.legacy_name,
                "hospital": hospital,
                "hard_min": rule.hard_min,
                "hard_max": rule.hard_max,
                "invalid_zero": rule.invalid_zero,
                "finite_count": accumulator.finite_count,
                "invalid_count": accumulator.invalid_count,
                "invalid_proportion": (
                    accumulator.invalid_count / accumulator.finite_count
                    if accumulator.finite_count
                    else 0.0
                ),
                "invalid_examples": sorted(accumulator.invalid_examples),
                "variable_total_invalid_count": total,
                "hospital_share_of_variable_invalids": (
                    accumulator.invalid_count / total if total else 0.0
                ),
                "description": rule.description,
            }
        )
    for column, total in totals.items():
        column_rows = [row for row in rows if row["column"] == column]
        dominant = max(
            column_rows,
            key=lambda row: (row["hospital_share_of_variable_invalids"], row["hospital"]),
        )
        for row in column_rows:
            row["dominant_invalid_hospital"] = dominant["hospital"] if total else None
            row["dominant_invalid_hospital_share"] = (
                dominant["hospital_share_of_variable_invalids"] if total else 0.0
            )
            row["invalidity_concentrated_in_specific_hospitals"] = bool(
                total >= 5
                and dominant["hospital_share_of_variable_invalids"] >= 0.5
            )
    return rows


def _known_legacy_finding_results(
    profiles: list[dict[str, Any]], policy: DataQualityPolicy
) -> list[dict[str, Any]]:
    lookup = {
        (row["column"], row["hospital"]): row
        for row in profiles
        if row["table"] == "dynamic"
    }
    results: list[dict[str, Any]] = []
    hospitals = sorted({row["hospital"] for row in profiles if row["table"] == "dynamic"})
    for finding in policy.known_legacy_findings:
        if finding.hospital == "all":
            site_results = []
            for hospital in hospitals:
                profile = lookup.get((finding.column, hospital))
                median = profile.get("median") if profile else None
                site_results.append(
                    {
                        "hospital": hospital,
                        "finite_count": profile.get("finite_count", 0) if profile else 0,
                        "median": median,
                        "candidate_reciprocal_median": (
                            1.0 / float(median) if median not in (None, 0) else None
                        ),
                    }
                )
            status = "manual_review_required"
            observed: Any = site_results
        else:
            profile = lookup.get((finding.column, finding.hospital))
            finite_count = profile.get("finite_count", 0) if profile else 0
            observed = {
                "profile": profile,
                "candidate_converted_profile": None,
            }
            if finding.conversion_divisor is not None and profile is not None:
                divisor = finding.conversion_divisor
                observed["candidate_converted_profile"] = {
                    metric: (
                        float(profile[metric]) / divisor
                        if profile.get(metric) is not None
                        else None
                    )
                    for metric in ("min", "q1", "median", "q3", "max")
                }
                observed["peer_medians"] = {
                    hospital: lookup[(finding.column, hospital)]["median"]
                    for hospital in hospitals
                    if hospital != finding.hospital
                    and (finding.column, hospital) in lookup
                    and lookup[(finding.column, hospital)]["median"] is not None
                }
                status = "manual_review_required"
            else:
                status = (
                    "appears_already_applied_all_target_values_missing"
                    if finite_count == 0
                    else "legacy_site_values_present_manual_review_required"
                )
        results.append(
            {
                "id": finding.finding_id,
                "column": finding.column,
                "hospital": finding.hospital,
                "prior_classification": finding.prior_classification,
                "prior_action": finding.prior_action,
                "current_verification": finding.current_verification,
                "decision": finding.decision,
                "verification_status": status,
                "observed": observed,
            }
        )
    return results


def _availability_results(
    profiles: list[dict[str, Any]], policy: DataQualityPolicy
) -> list[dict[str, Any]]:
    lookup = {
        (row["column"], row["hospital"]): row
        for row in profiles
        if row["table"] == "dynamic"
    }
    hospitals = sorted({row["hospital"] for row in profiles if row["table"] == "dynamic"})
    results = []
    for audit in policy.availability_audits:
        expected = set(audit["expected_non_missing_hospitals"])
        counts = {
            column: {
                hospital: int(lookup[(column, hospital)]["finite_count"])
                if (column, hospital) in lookup
                else 0
                for hospital in hospitals
            }
            for column in audit["columns"]
        }
        unexpected = {
            column: {
                hospital: count
                for hospital, count in hospital_counts.items()
                if hospital not in expected and count > 0
            }
            for column, hospital_counts in counts.items()
        }
        unexpected = {column: values for column, values in unexpected.items() if values}
        results.append(
            {
                "id": audit["id"],
                "expected_non_missing_hospitals": sorted(expected),
                "non_missing_counts": counts,
                "unexpected_non_missing_counts": unexpected,
                "matches_expected_availability_boundary": not unexpected,
                "provenance": audit.get("provenance"),
            }
        )
    return results


def _relationship_results(
    accumulators: dict[tuple[str, str], _DifferenceAccumulator],
    policy: DataQualityPolicy,
) -> list[dict[str, Any]]:
    results = []
    for audit in policy.relationship_audits:
        audit_id = str(audit["id"])
        by_hospital = [
            accumulator.result(hospital)
            for (relationship_id, hospital), accumulator in sorted(accumulators.items())
            if relationship_id == audit_id
        ]
        results.append(
            {
                "id": audit_id,
                "table": audit["table"],
                "fields": audit["fields"],
                "formula": audit["formula"],
                "hospital_scope": audit.get("hospital_scope", "all"),
                "absolute_tolerance": float(audit.get("absolute_tolerance", 0.0)),
                "by_hospital": by_hospital,
                "total_both_present_count": sum(
                    item["both_present_count"] for item in by_hospital
                ),
                "total_outside_tolerance_count": sum(
                    item["outside_tolerance_count"] for item in by_hospital
                ),
            }
        )
    return results


_COMPACT_PROFILE_FIELDS = (
    "finite_count",
    "finite_rate",
    "min",
    "q01",
    "q1",
    "median",
    "q3",
    "q99",
    "max",
    "zero_count",
    "negative_count",
)


def _compact_profile(profile: dict[str, Any]) -> dict[str, Any]:
    return {field: profile.get(field) for field in _COMPACT_PROFILE_FIELDS}


def _targeted_scale_results(
    accumulators: dict[str, _TargetedScaleAccumulator],
    profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    lookup = {
        (row["column"], row["hospital"]): row
        for row in profiles
        if row["table"] == "dynamic"
    }
    results: list[dict[str, Any]] = []
    for audit_id, accumulator in sorted(accumulators.items()):
        rule = accumulator.rule
        raw = lookup.get((rule.column, rule.hospital))
        candidate = accumulator.candidate.profile(
            "dynamic", rule.hospital, rule.column
        )
        peers = [
            {
                "hospital": hospital,
                **_compact_profile(profile),
            }
            for (column, hospital), profile in sorted(lookup.items())
            if column == rule.column
            and hospital != rule.hospital
            and (
                rule.peer_hospitals is None
                or hospital in rule.peer_hospitals
            )
        ]
        peer_medians = [
            float(item["median"])
            for item in peers
            if item["median"] is not None
        ]
        candidate_median = candidate.get("median")
        expected_range_defined = (
            rule.expected_min is not None or rule.expected_max is not None
        )
        results.append(
            {
                "id": audit_id,
                "column": rule.column,
                "hospital": rule.hospital,
                "operation": rule.operation,
                "factor": rule.factor,
                "threshold": rule.threshold,
                "expected_range": {
                    "min": rule.expected_min,
                    "max": rule.expected_max,
                },
                "evidence": rule.evidence,
                "approval_status": rule.approval_status,
                "peer_hospital_scope": (
                    list(rule.peer_hospitals)
                    if rule.peer_hospitals is not None
                    else "all_except_target"
                ),
                "candidate_is_hypothetical_and_not_written": True,
                "values_considered_for_transformation_count": (
                    accumulator.values_considered_for_transformation_count
                ),
                "numerically_changed_count": accumulator.numerically_changed_count,
                "before_outside_expected_range_count": (
                    accumulator.before_outside_expected_range_count
                    if expected_range_defined
                    else None
                ),
                "after_outside_expected_range_count": (
                    accumulator.after_outside_expected_range_count
                    if expected_range_defined
                    else None
                ),
                "raw_target_profile": _compact_profile(raw) if raw else None,
                "hypothetical_candidate_profile": _compact_profile(candidate),
                "peer_raw_profiles": peers,
                "peer_median_range": (
                    {"min": min(peer_medians), "max": max(peer_medians)}
                    if peer_medians
                    else None
                ),
                "candidate_median_within_peer_median_range": (
                    min(peer_medians) <= float(candidate_median) <= max(peer_medians)
                    if peer_medians and candidate_median is not None
                    else None
                ),
            }
        )
    return results


def _targeted_comparison_results(
    profiles: list[dict[str, Any]], policy: DataQualityPolicy
) -> list[dict[str, Any]]:
    dynamic_profiles = [row for row in profiles if row["table"] == "dynamic"]
    results: list[dict[str, Any]] = []
    for audit in policy.targeted_comparison_audits:
        column = str(audit["column"])
        by_hospital = [
            {"hospital": row["hospital"], **_compact_profile(row)}
            for row in dynamic_profiles
            if row["column"] == column
        ]
        by_hospital.sort(key=lambda row: row["hospital"])
        nonzero_absolute_medians = [
            (row["hospital"], abs(float(row["median"])))
            for row in by_hospital
            if row["median"] not in (None, 0)
        ]
        if nonzero_absolute_medians:
            minimum_site, minimum_median = min(
                nonzero_absolute_medians, key=lambda item: (item[1], item[0])
            )
            maximum_site, maximum_median = max(
                nonzero_absolute_medians, key=lambda item: (item[1], item[0])
            )
            ratio = maximum_median / minimum_median
        else:
            minimum_site = maximum_site = None
            minimum_median = maximum_median = ratio = None
        results.append(
            {
                "id": audit["id"],
                "column": column,
                "reason": audit["reason"],
                "approval_status": audit["approval_status"],
                "by_hospital": by_hospital,
                "absolute_nonzero_median_range": {
                    "minimum_hospital": minimum_site,
                    "minimum": _json_number(minimum_median),
                    "maximum_hospital": maximum_site,
                    "maximum": _json_number(maximum_median),
                    "maximum_to_minimum_ratio": _json_number(ratio),
                },
            }
        )
    return results


def _row_level_scale_entry_results(
    accumulators: dict[tuple[str, str], _RowLevelScaleEntryAccumulator],
    policy: DataQualityPolicy,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for rule in policy.row_level_scale_entry_audits:
        by_hospital = [
            accumulator.result(hospital)
            for (audit_id, hospital), accumulator in sorted(accumulators.items())
            if audit_id == rule.audit_id
        ]
        results.append(
            {
                "id": rule.audit_id,
                "column": rule.column,
                "scope": "individual_rows_within_each_hospital",
                "operation": rule.operation,
                "factor": rule.factor,
                "inclusive_recovery_window": {
                    "min": rule.recovery_min,
                    "max": rule.recovery_max,
                },
                "evidence": rule.evidence,
                "approval_status": rule.approval_status,
                "candidate_is_hypothetical_and_not_written": True,
                "total_finite_count": sum(
                    row["finite_count"] for row in by_hospital
                ),
                "total_outside_recovery_window_count": sum(
                    row["outside_recovery_window_count"]
                    for row in by_hospital
                ),
                "total_recoverable_candidate_count": sum(
                    row["recoverable_candidate_count"]
                    for row in by_hospital
                ),
                "total_unrecoverable_outside_count": sum(
                    row["unrecoverable_outside_count"]
                    for row in by_hospital
                ),
                "by_hospital": by_hospital,
            }
        )
    return results


def _scale_entry_pattern_classification(
    accumulator: _ScaleEntryDiscoveryAccumulator,
) -> str:
    policy = accumulator.policy
    if accumulator.finite_count < policy.minimum_finite_values_for_pattern:
        return "insufficient_finite_values_for_pattern_classification"
    if accumulator.uniquely_recoverable_count == 0:
        if accumulator.ambiguous_transform_count:
            return "ambiguous_factor_candidates_only"
        return "no_unique_scale_entry_candidates"
    fraction = accumulator.uniquely_recoverable_count / accumulator.finite_count
    if fraction <= policy.rare_candidate_max_fraction:
        return "rare_row_level_pattern"
    if fraction >= policy.systematic_candidate_min_fraction:
        return "possible_hospital_wide_or_systematic_scale_pattern"
    return "mixed_frequency_pattern_requires_review"


def _scale_entry_discovery_results(
    accumulators: dict[tuple[str, str], _ScaleEntryDiscoveryAccumulator],
    policy: DataQualityPolicy,
    dynamic_numeric_columns: list[str],
) -> dict[str, Any]:
    transformations = _scale_transformations(policy.scale_entry_discovery)
    explicit_columns = {
        rule.column for rule in policy.row_level_scale_entry_audits
    }
    eligible_columns = sorted({column for column, _ in accumulators})
    fields: list[dict[str, Any]] = []
    for column in eligible_columns:
        by_hospital: list[dict[str, Any]] = []
        column_accumulators = [
            (hospital, accumulator)
            for (candidate_column, hospital), accumulator in sorted(
                accumulators.items()
            )
            if candidate_column == column
        ]
        for hospital, accumulator in column_accumulators:
            valid_profile = _compact_profile(
                accumulator.valid_values.profile(
                    "dynamic",
                    hospital,
                    column,
                )
            )
            unique_transformations: list[dict[str, Any]] = []
            for operation, factor, transformation_id in transformations:
                count = accumulator.unique_counts[transformation_id]
                if count == 0:
                    continue
                candidate_profile = _compact_profile(
                    accumulator.candidate_values[transformation_id].profile(
                        "dynamic",
                        hospital,
                        column,
                    )
                )
                candidate_median = candidate_profile.get("median")
                valid_q01 = valid_profile.get("q01")
                valid_q99 = valid_profile.get("q99")
                unique_transformations.append(
                    {
                        "id": transformation_id,
                        "operation": operation,
                        "factor": factor,
                        "uniquely_recoverable_count": count,
                        "hypothetical_recovered_profile": candidate_profile,
                        "hypothetical_recovered_median_within_valid_q01_q99": (
                            float(valid_q01)
                            <= float(candidate_median)
                            <= float(valid_q99)
                            if candidate_median is not None
                            and valid_q01 is not None
                            and valid_q99 is not None
                            else None
                        ),
                        "candidate_examples": [
                            {
                                "raw": raw,
                                "hypothetical_recovered": recovered,
                            }
                            for raw, recovered in sorted(
                                accumulator.candidate_examples[
                                    transformation_id
                                ]
                            )
                        ],
                    }
                )
            by_hospital.append(
                {
                    "hospital": hospital,
                    "finite_count": accumulator.finite_count,
                    "in_range_count": accumulator.in_range_count,
                    "raw_outside_count": accumulator.raw_outside_count,
                    "uniquely_recoverable_count": (
                        accumulator.uniquely_recoverable_count
                    ),
                    "ambiguous_transform_count": (
                        accumulator.ambiguous_transform_count
                    ),
                    "unrecoverable_count": accumulator.unrecoverable_count,
                    "uniquely_recoverable_fraction_of_finite": (
                        accumulator.uniquely_recoverable_count
                        / accumulator.finite_count
                        if accumulator.finite_count
                        else None
                    ),
                    "pattern_classification": (
                        _scale_entry_pattern_classification(accumulator)
                    ),
                    "valid_raw_profile": valid_profile,
                    "unique_transformations": unique_transformations,
                    "ambiguous_examples": [
                        {
                            "raw": raw,
                            "possible_transformations": list(possible),
                        }
                        for raw, possible in sorted(
                            accumulator.ambiguous_examples
                        )
                    ],
                }
            )
        first_accumulator = column_accumulators[0][1]
        fields.append(
            {
                "column": column,
                "inclusive_validity_window": {
                    "min": first_accumulator.rule.hard_min,
                    "max": first_accumulator.rule.hard_max,
                },
                "legacy_rule": first_accumulator.rule.legacy_name,
                "has_explicit_row_level_recovery_audit": (
                    column in explicit_columns
                ),
                "total_finite_count": sum(
                    item["finite_count"] for item in by_hospital
                ),
                "total_raw_outside_count": sum(
                    item["raw_outside_count"] for item in by_hospital
                ),
                "total_uniquely_recoverable_count": sum(
                    item["uniquely_recoverable_count"]
                    for item in by_hospital
                ),
                "total_ambiguous_transform_count": sum(
                    item["ambiguous_transform_count"]
                    for item in by_hospital
                ),
                "total_unrecoverable_count": sum(
                    item["unrecoverable_count"] for item in by_hospital
                ),
                "by_hospital": by_hospital,
            }
        )

    unbounded_columns = sorted(
        {
            column
            for rule in policy.invalid_value_rules
            if rule.hard_min is None or rule.hard_max is None
            for column in rule.columns
        }
    )
    return {
        "status": "discovery_only_not_approved_for_cleaning",
        "source": policy.scale_entry_discovery.source,
        "eligibility_rule": (
            "raw value is outside the inclusive validity window and exactly "
            "one tested multiplication or division enters the window"
        ),
        "tested_transformations": [
            {"id": transformation_id, "operation": operation, "factor": factor}
            for operation, factor, transformation_id in transformations
        ],
        "pattern_thresholds": {
            "minimum_finite_values": (
                policy.scale_entry_discovery.minimum_finite_values_for_pattern
            ),
            "rare_candidate_max_fraction": (
                policy.scale_entry_discovery.rare_candidate_max_fraction
            ),
            "systematic_candidate_min_fraction": (
                policy.scale_entry_discovery.systematic_candidate_min_fraction
            ),
        },
        "audited_field_count": len(fields),
        "audited_fields": fields,
        "excluded_unbounded_rule_columns": unbounded_columns,
        "excluded_numeric_fields_without_complete_bounds": sorted(
            set(dynamic_numeric_columns) - set(eligible_columns)
        ),
        "total_uniquely_recoverable_count": sum(
            field["total_uniquely_recoverable_count"] for field in fields
        ),
        "total_ambiguous_transform_count": sum(
            field["total_ambiguous_transform_count"] for field in fields
        ),
        "candidate_values_are_hypothetical_and_not_written": True,
    }


def _scale_entry_candidate_review_coverage(
    accumulators: dict[tuple[str, str], _ScaleEntryDiscoveryAccumulator],
    policy: DataQualityPolicy,
) -> dict[str, Any]:
    """Require every unique candidate scope to have an explicit review route."""
    context_fields = {
        field.column: field for field in policy.candidate_context_audit.fields
    }
    targeted_scopes = {
        (rule.column, rule.hospital): rule.audit_id
        for rule in policy.targeted_scale_audits
    }
    explicit_columns = {
        rule.column: rule.audit_id
        for rule in policy.row_level_scale_entry_audits
    }

    scopes: list[dict[str, Any]] = []
    for (column, hospital), accumulator in sorted(accumulators.items()):
        candidate_count = accumulator.uniquely_recoverable_count
        if candidate_count == 0:
            continue

        context_field = context_fields.get(column)
        if (
            context_field is not None
            and hospital not in context_field.excluded_hospitals
        ):
            route = "candidate_context_audit"
            route_id = column
        elif (column, hospital) in targeted_scopes:
            route = "targeted_scale_audit"
            route_id = targeted_scopes[(column, hospital)]
        elif column in explicit_columns:
            route = "explicit_row_level_scale_entry_audit"
            route_id = explicit_columns[column]
        else:
            route = "uncovered"
            route_id = None

        scopes.append(
            {
                "column": column,
                "hospital": hospital,
                "uniquely_recoverable_count": candidate_count,
                "review_route": route,
                "review_route_id": route_id,
            }
        )

    uncovered_scopes = [
        scope for scope in scopes if scope["review_route"] == "uncovered"
    ]
    route_scope_counts: dict[str, int] = {}
    route_candidate_counts: dict[str, int] = {}
    for scope in scopes:
        route = str(scope["review_route"])
        route_scope_counts[route] = route_scope_counts.get(route, 0) + 1
        route_candidate_counts[route] = (
            route_candidate_counts.get(route, 0)
            + int(scope["uniquely_recoverable_count"])
        )

    return {
        "status": (
            "complete" if not uncovered_scopes else "incomplete_blocking"
        ),
        "scope_definition": "column and hospital",
        "unique_candidate_scope_count": len(scopes),
        "total_unique_candidate_count": sum(
            int(scope["uniquely_recoverable_count"]) for scope in scopes
        ),
        "review_route_scope_counts": route_scope_counts,
        "review_route_candidate_counts": route_candidate_counts,
        "uncovered_scope_count": len(uncovered_scopes),
        "uncovered_candidate_count": sum(
            int(scope["uniquely_recoverable_count"])
            for scope in uncovered_scopes
        ),
        "uncovered_scopes": uncovered_scopes,
        "scopes": scopes,
    }


def _bucket_labels(cut_points: tuple[float, ...]) -> list[str]:
    labels = [f"(-inf, {cut_points[0]}]"]
    labels.extend(
        f"({left}, {right}]"
        for left, right in zip(cut_points, cut_points[1:])
    )
    labels.append(f"({cut_points[-1]}, inf)")
    return labels


def _targeted_bucket_results(
    accumulators: dict[str, _TargetedBucketAccumulator],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for audit_id, accumulator in sorted(accumulators.items()):
        rule = accumulator.rule
        labels = _bucket_labels(rule.cut_points)
        results.append(
            {
                "id": audit_id,
                "column": rule.column,
                "hospital": rule.hospital,
                "reason": rule.reason,
                "cut_points": list(rule.cut_points),
                "finite_count": accumulator.finite_count,
                "buckets": [
                    {
                        "interval": label,
                        "count": int(count),
                        "proportion_of_finite": (
                            int(count) / accumulator.finite_count
                            if accumulator.finite_count
                            else None
                        ),
                    }
                    for label, count in zip(
                        labels, accumulator.counts.tolist(), strict=True
                    )
                ],
            }
        )
    return results


def _sentinel_results(
    counts: dict[tuple[str, str], int], policy: DataQualityPolicy, hospitals: list[str]
) -> list[dict[str, Any]]:
    rows = []
    for rule in policy.sentinels:
        by_hospital = {
            hospital: counts.get((rule.column, hospital), 0) for hospital in hospitals
        }
        rows.append(
            {
                "column": rule.column,
                "sentinel": rule.value,
                "value_kind": rule.value_kind,
                "count": sum(by_hospital.values()),
                "by_hospital": by_hospital,
            }
        )
    return rows


def audit_cross_hospital_data_quality(
    config: CrossHospitalAuditConfig,
    contract: TranslatedInputContract,
    policy: DataQualityPolicy,
) -> AuditReport:
    translated = _open_translated_input(config.translated_dir, contract)
    pbw_reference = _build_predicted_body_weight_reference(
        translated.static,
        policy.predicted_body_weight_tidal_volume_audit,
        policy,
        config,
    )
    exclusions = policy.profile_exclusions
    static_numeric = _numeric_columns(translated.static, set(exclusions["static"]))
    dynamic_numeric = _numeric_columns(translated.dynamic, set(exclusions["dynamic"]))
    expected_static_numeric = _contract_numeric_columns(
        contract.static,
        config.dataset_context,
        set(exclusions["static"]),
    )
    expected_dynamic_numeric = _contract_numeric_columns(
        contract.dynamic,
        config.dataset_context,
        set(exclusions["dynamic"]),
    )
    numeric_profile_mismatches = {
        "static": {
            "missing": sorted(set(expected_static_numeric) - set(static_numeric)),
            "extra": sorted(set(static_numeric) - set(expected_static_numeric)),
        },
        "dynamic": {
            "missing": sorted(set(expected_dynamic_numeric) - set(dynamic_numeric)),
            "extra": sorted(set(dynamic_numeric) - set(expected_dynamic_numeric)),
        },
    }
    rule_columns = _rule_by_column(policy)
    actual_dynamic_numeric = set(dynamic_numeric)
    non_numeric_rule_columns = sorted(set(rule_columns) - actual_dynamic_numeric)
    relationship_accumulators: dict[tuple[str, str], _DifferenceAccumulator] = {}
    targeted_scale_accumulators = {
        rule.audit_id: _TargetedScaleAccumulator(
            rule,
            config.quantile_sample_size_per_hospital_variable,
            config.max_examples,
        )
        for rule in policy.targeted_scale_audits
    }
    row_level_scale_entry_accumulators: dict[
        tuple[str, str], _RowLevelScaleEntryAccumulator
    ] = {}
    scale_entry_discovery_accumulators: dict[
        tuple[str, str], _ScaleEntryDiscoveryAccumulator
    ] = {}
    candidate_context_collector = _CandidateContextCollector(
        policy=policy.candidate_context_audit,
        stay_id_column=policy.stay_id_column,
        time_column="minutes_since_icu_admission",
    )
    targeted_bucket_accumulators = {
        rule.audit_id: _TargetedBucketAccumulator(rule)
        for rule in policy.targeted_bucket_audits
    }

    print(f"Profiling translated static data: {translated.static.path}")
    static_acc, sentinel_counts, static_hospital_rows, static_scanned = _scan_table(
        "static",
        translated.static,
        static_numeric,
        policy,
        config,
        {},
        relationship_accumulators,
        targeted_scale_accumulators,
        row_level_scale_entry_accumulators,
        scale_entry_discovery_accumulators,
        targeted_bucket_accumulators,
        candidate_context_collector,
    )
    print(f"Profiling translated dynamic data: {translated.dynamic.path}")
    dynamic_acc, _, dynamic_hospital_rows, dynamic_scanned = _scan_table(
        "dynamic",
        translated.dynamic,
        dynamic_numeric,
        policy,
        config,
        rule_columns,
        relationship_accumulators,
        targeted_scale_accumulators,
        row_level_scale_entry_accumulators,
        scale_entry_discovery_accumulators,
        targeted_bucket_accumulators,
        candidate_context_collector,
    )
    if candidate_context_collector.records:
        print(
            "Collecting neighboring-measurement context for "
            f"{len(candidate_context_collector.records):,} uniquely "
            "recoverable candidates."
        )
    _collect_candidate_context(
        translated.dynamic,
        candidate_context_collector,
        policy,
        config,
        rule_columns,
    )
    print(
        "Testing UK00 tidal-volume formulas against static height, sex, "
        "and weight."
    )
    pbw_tidal_volume_audit, pbw_dynamic_rows_scanned = (
        _collect_predicted_body_weight_tidal_volume_audit(
            translated.dynamic,
            pbw_reference,
            policy,
            config,
        )
    )
    profiles = [
        accumulator.profile("static", hospital, column)
        for (column, hospital), accumulator in sorted(static_acc.items())
    ] + [
        accumulator.profile("dynamic", hospital, column)
        for (column, hospital), accumulator in sorted(dynamic_acc.items())
    ]
    distribution_issues = _distribution_issues(profiles, policy)
    invalid_findings = _invalid_value_findings(dynamic_acc, policy)
    known_findings = _known_legacy_finding_results(profiles, policy)
    availability = _availability_results(profiles, policy)
    relationships = _relationship_results(relationship_accumulators, policy)
    targeted_scales = _targeted_scale_results(
        targeted_scale_accumulators, profiles
    )
    row_level_scale_entries = _row_level_scale_entry_results(
        row_level_scale_entry_accumulators,
        policy,
    )
    scale_entry_discovery = _scale_entry_discovery_results(
        scale_entry_discovery_accumulators,
        policy,
        dynamic_numeric,
    )
    candidate_review_coverage = _scale_entry_candidate_review_coverage(
        scale_entry_discovery_accumulators,
        policy,
    )
    candidate_context = _candidate_context_results(
        candidate_context_collector,
        translated.dynamic.row_count,
        config.max_examples,
    )
    targeted_comparisons = _targeted_comparison_results(profiles, policy)
    targeted_buckets = _targeted_bucket_results(targeted_bucket_accumulators)
    hospitals = sorted(set(static_hospital_rows) | set(dynamic_hospital_rows))
    sentinel_results = _sentinel_results(sentinel_counts, policy, hospitals)

    infinite_count = sum(
        row["positive_infinity_count"] + row["negative_infinity_count"]
        for row in profiles
    )
    invalid_count = sum(
        row["invalid_count"]
        for row in invalid_findings
        if row["hospital_share_of_variable_invalids"] >= 0
    )
    # Each site contributes once, so this is the exact total over site-variable rows.
    site_drop_unresolved = [
        row
        for row in known_findings
        if row["prior_action"] == "set_site_values_missing"
        and row["verification_status"]
        != "appears_already_applied_all_target_values_missing"
    ]
    manual_unit_or_definition = [
        row
        for row in known_findings
        if row["verification_status"] == "manual_review_required"
    ]
    formula_result = next(
        item
        for item in relationships
        if item["id"] == "computed_driving_pressure_formula"
    )
    availability_mismatches = [
        item for item in availability if not item["matches_expected_availability_boundary"]
    ]
    sentinel_count = sum(item["count"] for item in sentinel_results)
    unapproved_scale_hypotheses = [
        item
        for item in targeted_scales
        if item["approval_status"] != "approved_for_cleaning"
    ]
    row_level_recovery_candidate_count = sum(
        item["total_recoverable_candidate_count"]
        for item in row_level_scale_entries
    )
    discovery_unique_count = int(
        scale_entry_discovery["total_uniquely_recoverable_count"]
    )
    discovery_ambiguous_count = int(
        scale_entry_discovery["total_ambiguous_transform_count"]
    )
    expected_discovery_columns = sorted(
        {
            column
            for rule in policy.invalid_value_rules
            if rule.hard_min is not None and rule.hard_max is not None
            for column in rule.columns
        }
    )
    observed_discovery_columns = sorted(
        field["column"]
        for field in scale_entry_discovery["audited_fields"]
    )
    context_without_temporal_evidence_count = int(
        candidate_context["primary_temporal_classification_counts"].get(
            "no_temporal_context",
            0,
        )
    )
    pbw_target_comparison = pbw_tidal_volume_audit["comparisons"][
        "target_absolute_vt_vs_six_ml_per_kg_pbw"
    ]
    pbw_normalized_comparison = pbw_tidal_volume_audit["comparisons"][
        "reported_vt_per_kg_vs_vt_div_pbw"
    ]
    pbw_actual_weight_comparison = pbw_tidal_volume_audit["comparisons"][
        "target_absolute_vt_vs_vt_scaled_by_pbw_over_actual_weight"
    ]

    def _pbw_hypothesis_check(
        name: str,
        condition: bool,
        observed: dict[str, Any],
        expected: dict[str, Any],
        details: str,
    ) -> CheckResult:
        if config.dataset_context == "mock":
            return CheckResult(
                name=name,
                status="skipped",
                severity="advisory",
                observed={
                    "dataset_context": "mock",
                    "reason": "dynamic columns were independently shuffled",
                },
                expected={"dataset_context": ["demo", "production"]},
                details=(
                    "The formula metric is emitted for structural testing, "
                    "but mock relationships are intentionally destroyed and "
                    "must not be interpreted."
                ),
            )
        return _check(
            name,
            condition,
            observed,
            expected,
            details,
            severity="advisory",
        )

    checks: list[CheckResult] = [
        _check(
            "legacy_policy_coverage_is_complete",
            policy.legacy_coverage
            == {
                "invalid_value_rule_count": 44,
                "expanded_v2_invalid_value_check_count": 46,
                "targeted_semantic_finding_count": 5,
                "static_minus_one_sentinel_count": 6,
            },
            policy.legacy_coverage,
            {
                "invalid_value_rule_count": 44,
                "expanded_v2_invalid_value_check_count": 46,
                "targeted_semantic_finding_count": 5,
                "static_minus_one_sentinel_count": 6,
            },
            "The executable policy must retain every recovered legacy rule and finding.",
        ),
        _check(
            "targeted_review_policy_coverage_is_complete",
            policy.targeted_review_coverage
            == {
                "scale_hypothesis_count": 11,
                "row_level_scale_entry_audit_count": 1,
                "comparison_audit_count": 12,
                "bucket_audit_count": 4,
                "cross_table_formula_audit_count": 1,
            },
            policy.targeted_review_coverage,
            {
                "scale_hypothesis_count": 11,
                "row_level_scale_entry_audit_count": 1,
                "comparison_audit_count": 12,
                "bucket_audit_count": 4,
                "cross_table_formula_audit_count": 1,
            },
            "The focused review must retain every hypothesis selected after the demo audit.",
        ),
        _check(
            "numeric_profile_field_sets_match_translated_contract",
            all(
                not differences["missing"] and not differences["extra"]
                for differences in numeric_profile_mismatches.values()
            ),
            numeric_profile_mismatches,
            {
                "static": {"missing": [], "extra": []},
                "dynamic": {"missing": [], "extra": []},
            },
            "Every non-excluded numeric contract field must be included in the universal profile.",
        ),
        _check(
            "uk00_pbw_formula_audit_has_complete_static_dynamic_linkage",
            pbw_reference.duplicate_stay_count == 0
            and pbw_dynamic_rows_scanned == translated.dynamic.row_count
            and pbw_tidal_volume_audit["dynamic_join"][
                "rows_without_static_reference"
            ]
            == 0,
            {
                "duplicate_static_stay_count": (
                    pbw_reference.duplicate_stay_count
                ),
                "dynamic_rows_scanned": pbw_dynamic_rows_scanned,
                "dynamic_rows_without_static_reference": (
                    pbw_tidal_volume_audit["dynamic_join"][
                        "rows_without_static_reference"
                    ]
                ),
            },
            {
                "duplicate_static_stay_count": 0,
                "dynamic_rows_scanned": translated.dynamic.row_count,
                "dynamic_rows_without_static_reference": 0,
            },
            "The cross-table formula test requires unique UK00 static references and a complete dynamic scan; invalid height, sex, or weight values are accounted for separately.",
        ),
        _check(
            "scale_entry_discovery_covers_all_bounded_invalid_value_fields",
            observed_discovery_columns == expected_discovery_columns,
            {"audited_columns": observed_discovery_columns},
            {"audited_columns": expected_discovery_columns},
            "Every dynamic field with complete preserved hard bounds must be included in scale-entry discovery.",
        ),
        _check(
            "every_unique_scale_entry_candidate_scope_has_a_review_route",
            candidate_review_coverage["uncovered_scope_count"] == 0,
            {
                "uncovered_scope_count": candidate_review_coverage[
                    "uncovered_scope_count"
                ],
                "uncovered_candidate_count": candidate_review_coverage[
                    "uncovered_candidate_count"
                ],
                "uncovered_scopes": candidate_review_coverage[
                    "uncovered_scopes"
                ],
            },
            {
                "uncovered_scope_count": 0,
                "uncovered_candidate_count": 0,
                "uncovered_scopes": [],
            },
            "Every field/hospital scope with uniquely recoverable discovery candidates must be routed to neighboring-measurement context, a targeted scale audit, or an explicit row-level scale-entry audit.",
        ),
        _check(
            "scale_entry_candidate_context_collection_is_complete",
            bool(candidate_context["collection_complete"]),
            {
                "selected_unique_candidate_count": candidate_context[
                    "selected_unique_candidate_count"
                ],
                "collected_candidate_count": candidate_context[
                    "collected_candidate_count"
                ],
                "invalid_candidate_key_count": candidate_context[
                    "invalid_candidate_key_count"
                ],
                "omitted_due_to_cap_count": candidate_context[
                    "omitted_due_to_cap_count"
                ],
                "second_pass_rows_scanned": candidate_context[
                    "second_pass_rows_scanned"
                ],
            },
            {
                "invalid_candidate_key_count": 0,
                "omitted_due_to_cap_count": 0,
                "second_pass_rows_scanned": candidate_context[
                    "expected_second_pass_rows"
                ],
            },
            "Candidate context must be complete; reaching the safety cap or failing to scan the complete second pass blocks interpretation.",
        ),
        _check(
            "legacy_invalid_rule_fields_have_numeric_arrow_types",
            not non_numeric_rule_columns,
            {"non_numeric_rule_columns": non_numeric_rule_columns},
            {"non_numeric_rule_columns": []},
            "A non-numeric physical type would make legacy numeric auditing impossible.",
        ),
        _check(
            "all_translated_rows_were_profiled",
            static_scanned == translated.static.row_count
            and dynamic_scanned == translated.dynamic.row_count,
            {"static": static_scanned, "dynamic": dynamic_scanned},
            {"static": translated.static.row_count, "dynamic": translated.dynamic.row_count},
            "The read-only audit must scan every translated row.",
        ),
        _check(
            "numeric_fields_contain_no_infinities",
            infinite_count == 0,
            {"infinite_value_count": infinite_count},
            {"infinite_value_count": 0},
            "Infinite values are non-finite numeric errors and require review.",
            severity="advisory",
        ),
        _check(
            "legacy_static_minus_one_sentinels_are_absent",
            sentinel_count == 0,
            {"sentinel_count": sentinel_count, "fields": sentinel_results},
            {"sentinel_count": 0},
            "The old pipeline treated -1 as missing in these six static fields.",
            severity="advisory",
        ),
        _check(
            "legacy_invalid_values_are_absent",
            invalid_count == 0,
            {"invalid_value_count": invalid_count},
            {"invalid_value_count": 0},
            "These values would have been masked by the legacy pipeline; this audit only reports them.",
            severity="advisory",
        ),
        _check(
            "known_legacy_site_drop_findings_appear_applied_upstream",
            not site_drop_unresolved,
            {
                "unresolved_count": len(site_drop_unresolved),
                "finding_ids": [item["id"] for item in site_drop_unresolved],
            },
            {"unresolved_count": 0},
            "Previously invalid site-variable pairs should be missing if pooling already applied the legacy correction.",
            severity="advisory",
        ),
        _check(
            "known_legacy_unit_and_definition_findings_are_resolved",
            not manual_unit_or_definition,
            {
                "manual_review_count": len(manual_unit_or_definition),
                "finding_ids": [item["id"] for item in manual_unit_or_definition],
            },
            {"manual_review_count": 0},
            "ETCO2 scale and I:E direction require human comparison with peer hospitals.",
            severity="advisory",
        ),
        _check(
            "cross_hospital_distribution_screen_has_no_flags",
            not distribution_issues,
            {"flagged_site_variable_count": len(distribution_issues)},
            {"flagged_site_variable_count": 0},
            "IQR-fence flags are screening signals, not automatic proof of a unit error.",
            severity="advisory",
        ),
        _check(
            "targeted_scale_hypotheses_are_approved_for_cleaning",
            not unapproved_scale_hypotheses,
            {
                "unapproved_count": len(unapproved_scale_hypotheses),
                "hypothesis_ids": [
                    item["id"] for item in unapproved_scale_hypotheses
                ],
            },
            {"unapproved_count": 0},
            "Candidate rescalings remain evidence-only until production results and clinical meaning are reviewed and explicitly approved for the cleaned layer.",
            severity="advisory",
        ),
        _check(
            "row_level_scale_entry_recovery_candidates_are_absent",
            row_level_recovery_candidate_count == 0,
            {
                "recoverable_candidate_count": (
                    row_level_recovery_candidate_count
                ),
                "audit_ids_with_candidates": [
                    item["id"]
                    for item in row_level_scale_entries
                    if item["total_recoverable_candidate_count"] > 0
                ],
            },
            {"recoverable_candidate_count": 0},
            "A candidate is an isolated out-of-window value whose explicit scale correction lands inside a reviewed physiologic window. This audit reports candidates but does not change them.",
            severity="advisory",
        ),
        _check(
            "general_scale_entry_discovery_has_no_candidates",
            discovery_unique_count == 0 and discovery_ambiguous_count == 0,
            {
                "uniquely_recoverable_count": discovery_unique_count,
                "ambiguous_transform_count": discovery_ambiguous_count,
                "audited_field_count": scale_entry_discovery[
                    "audited_field_count"
                ],
            },
            {
                "uniquely_recoverable_count": 0,
                "ambiguous_transform_count": 0,
            },
            "The broad screen tests bounded variables for decimal-scale entry errors. Unique and ambiguous candidates require human review and are never corrected by this audit.",
            severity="advisory",
        ),
        _check(
            "scale_entry_candidates_have_temporal_context_within_primary_window",
            context_without_temporal_evidence_count == 0,
            {
                "candidate_count_without_temporal_context": (
                    context_without_temporal_evidence_count
                ),
                "primary_context_window_hours": candidate_context[
                    "primary_context_window_hours"
                ],
            },
            {"candidate_count_without_temporal_context": 0},
            "Missing neighboring measurements do not invalidate a candidate, but they leave temporal support unavailable for human review.",
            severity="advisory",
        ),
        _pbw_hypothesis_check(
            "uk00_target_absolute_vt_matches_six_ml_per_kg_pbw",
            pbw_target_comparison["both_present_count"] > 0
            and pbw_target_comparison["outside_tolerance_count"] == 0,
            {
                "compared_count": pbw_target_comparison[
                    "both_present_count"
                ],
                "within_tolerance_count": pbw_target_comparison[
                    "within_tolerance_count"
                ],
                "outside_tolerance_count": pbw_target_comparison[
                    "outside_tolerance_count"
                ],
                "absolute_tolerance_ml": pbw_target_comparison[
                    "absolute_tolerance_ml"
                ],
            },
            {
                "minimum_compared_count": 1,
                "outside_tolerance_count": 0,
            },
            "Exact agreement would support, but not by itself prove, that the source-labelled UK00 field stores the absolute 6 mL/kg PBW target.",
        ),
        _pbw_hypothesis_check(
            "uk00_reported_vt_per_kg_matches_vt_div_pbw",
            pbw_normalized_comparison["both_present_count"] > 0
            and pbw_normalized_comparison["outside_tolerance_count"] == 0,
            {
                "compared_count": pbw_normalized_comparison[
                    "both_present_count"
                ],
                "within_tolerance_count": pbw_normalized_comparison[
                    "within_tolerance_count"
                ],
                "outside_tolerance_count": pbw_normalized_comparison[
                    "outside_tolerance_count"
                ],
                "absolute_tolerance_ml_per_kg": (
                    pbw_normalized_comparison[
                        "absolute_tolerance_ml_per_kg"
                    ]
                ),
            },
            {
                "minimum_compared_count": 1,
                "outside_tolerance_count": 0,
            },
            "Agreement would identify vt_per_kg as delivered vt normalized by the tested PBW formula; it does not validate the upstream formula provenance.",
        ),
        _pbw_hypothesis_check(
            "uk00_target_difference_from_vt_is_explained_only_by_actual_vs_pbw",
            pbw_actual_weight_comparison["both_present_count"] > 0
            and pbw_actual_weight_comparison[
                "outside_tolerance_count"
            ]
            == 0,
            {
                "compared_count": pbw_actual_weight_comparison[
                    "both_present_count"
                ],
                "within_tolerance_count": pbw_actual_weight_comparison[
                    "within_tolerance_count"
                ],
                "outside_tolerance_count": pbw_actual_weight_comparison[
                    "outside_tolerance_count"
                ],
                "absolute_tolerance_ml": pbw_actual_weight_comparison[
                    "absolute_tolerance_ml"
                ],
            },
            {
                "minimum_compared_count": 1,
                "outside_tolerance_count": 0,
            },
            "This directly tests the proposed explanation target = delivered vt * PBW / actual weight. Failure means delivered-target differences cannot be attributed solely to distance from predicted weight.",
        ),
        _check(
            "computed_driving_pressure_matches_inspiratory_pressure_minus_peep",
            formula_result["total_outside_tolerance_count"] == 0,
            {
                "compared_count": formula_result["total_both_present_count"],
                "outside_tolerance_count": formula_result[
                    "total_outside_tolerance_count"
                ],
                "absolute_tolerance": formula_result["absolute_tolerance"],
            },
            {"outside_tolerance_count": 0},
            "delta_p_computed is expected to equal insp_pressure minus peep.",
            severity="advisory",
        ),
        _check(
            "isofa_values_are_confined_to_uk00",
            not availability_mismatches,
            {"mismatches": availability_mismatches},
            {"unexpected_non_missing_counts": {}},
            "iSOFA is user-confirmed as available only for asic_UK00; its derivation remains unresolved.",
            severity="advisory",
        ),
        CheckResult(
            name="legacy_raw_non_numeric_strings_can_be_reaudited",
            status="skipped",
            severity="advisory",
            observed={
                "auditability": policy.legacy_raw_numeric_parsing.get(
                    "auditability_at_translated_boundary"
                ),
                "preserved_legacy_parser": policy.legacy_raw_numeric_parsing,
            },
            expected={"original_hospital_strings_available": True},
            details=(
                "Translated Parquet fields are already numeric, so discarded source strings "
                "cannot be reconstructed at this boundary. The exact legacy parsing rules are preserved in policy."
            ),
        ),
    ]

    return AuditReport(
        contract_version=contract.contract_version,
        dataset="asic_translated_cross_hospital_data_quality",
        dataset_context=config.dataset_context,
        generated_at_utc=utc_timestamp(),
        overall_status=overall_status(checks),
        inputs={
            "translated_directory": str(config.translated_dir),
            "config": str(config.source_path),
            "translated_contract": str(contract.source_path),
            "data_quality_policy": str(policy.source_path),
            "policy_version": policy.policy_version,
            "policy_status": policy.status,
            "static": _schema_summary(translated.static),
            "dynamic": _schema_summary(translated.dynamic),
        },
        metrics={
            "method": {
                "read_only": True,
                "all_rows_scanned": True,
                "quantile_method": "deterministic bottom-k sample by stay/time key",
                "quantile_sample_size_per_hospital_variable": (
                    config.quantile_sample_size_per_hospital_variable
                ),
                "distribution_minimum_non_missing_values": (
                    policy.distribution.minimum_non_missing_values
                ),
                "distribution_minimum_hospitals": policy.distribution.minimum_hospitals,
                "distribution_iqr_fence_factor": (
                    policy.distribution.hospital_metric_iqr_fence_factor
                ),
                "targeted_candidate_transformations_written_to_data": False,
                "row_level_scale_entry_candidates_written_to_data": False,
                "general_scale_entry_discovery_candidates_written_to_data": (
                    False
                ),
                "candidate_context_second_pass": bool(
                    candidate_context["second_pass_required"]
                ),
                "candidate_context_identifiers_written_to_report": False,
                "candidate_context_written_to_data": False,
                "predicted_body_weight_formula_additional_pass": True,
                "predicted_body_weight_formula_rows_scanned": (
                    pbw_dynamic_rows_scanned
                ),
                "predicted_body_weight_formula_values_written_to_data": False,
                "predicted_body_weight_formula_identifiers_written_to_report": (
                    False
                ),
            },
            "hospital_row_counts": {
                "static": static_hospital_rows,
                "dynamic": dynamic_hospital_rows,
            },
            "numeric_profile_column_counts": {
                "static": len(static_numeric),
                "dynamic": len(dynamic_numeric),
            },
            "numeric_profiles": profiles,
            "cross_hospital_distribution_issues": distribution_issues,
            "legacy_invalid_value_findings": invalid_findings,
            "legacy_static_sentinel_findings": sentinel_results,
            "known_legacy_semantic_findings": known_findings,
            "targeted_scale_hypotheses": targeted_scales,
            "row_level_scale_entry_audits": row_level_scale_entries,
            "scale_entry_candidate_discovery": scale_entry_discovery,
            "scale_entry_candidate_review_coverage": candidate_review_coverage,
            "scale_entry_candidate_context": candidate_context,
            "predicted_body_weight_tidal_volume_audit": (
                pbw_tidal_volume_audit
            ),
            "targeted_cross_hospital_comparisons": targeted_comparisons,
            "targeted_bucket_audits": targeted_buckets,
            "targeted_review_coverage": policy.targeted_review_coverage,
            "relationship_audits": relationships,
            "availability_audits": availability,
            "legacy_sources": list(policy.legacy_sources),
            "legacy_coverage": policy.legacy_coverage,
        },
        checks=tuple(checks),
        limitations=(
            "This is a read-only screening audit; it does not convert units, mask invalid values, drop fields, or select a cohort.",
            "Cross-hospital distribution flags identify review candidates and do not by themselves prove a unit or definition error.",
            "Quantiles are exact when finite_count does not exceed the configured bottom-k capacity and deterministic estimates otherwise.",
            "Original non-numeric strings cannot be re-audited because pooled and translated Parquet fields are already numeric.",
            "Known legacy corrections are treated as hypotheses to verify against the pooled source, not silently re-applied.",
            "Targeted candidate scale profiles are computed in memory only; they do not modify or create data artifacts.",
            "Row-level scale-entry candidates are reported only when the raw value is outside an explicit recovery window and the configured transformation lands inside it; no candidate is corrected by this audit.",
            "General scale-entry discovery uses existing hard validity windows as a screening device. A uniquely recoverable arithmetic candidate is not proof that the transformed value is clinically correct.",
            "Neighboring-measurement and same-row related-field evidence is supporting or contradictory context only. It does not establish the source unit, prove that a recovery is correct, or approve a cleaning rule.",
            "Temporal context uses strictly earlier and later valid measurements from the same ICU stay. Same-time duplicate rows are excluded from temporal neighbors, and related fields can themselves contain unresolved measurement error.",
            "The UK00 predicted-body-weight equations are tested hypotheses from the NIH ARDS Network convention, not confirmed documentation of the source system's formula or field semantics.",
            "Agreement with 6 mL/kg PBW can identify an arithmetic construction but cannot establish whether the resulting absolute volume was a prescribed target, retrospective derivation, or another source-system concept.",
            "A candidate distribution aligning with peers is supporting evidence, not proof of a common unit or clinical definition.",
        ),
    )
