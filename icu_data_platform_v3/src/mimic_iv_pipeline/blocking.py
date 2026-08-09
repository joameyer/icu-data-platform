from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import math
import statistics
from typing import Any, Iterable

from mimic_iv_pipeline.contracts import VariableSpec
from mimic_iv_pipeline.errors import MIMICPipelineError


def _event_order(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row["event_time_offset_us"]),
        str(row.get("source_table", "")),
        int(row.get("itemid") or -1),
        int(row.get("source_row_number") or -1),
    )


def _assigned_index(offset_us: int, delta_us: int) -> int:
    if offset_us < 0:
        return math.floor(offset_us / delta_us)
    if offset_us == 0:
        return 0
    return math.ceil(offset_us / delta_us)


def _block_indices(extent_us: int, delta_us: int, pre_horizon_hours: int) -> list[int]:
    pre_count = math.ceil(pre_horizon_hours * 3_600_000_000 / delta_us)
    post_count = math.ceil(extent_us / delta_us) if extent_us else 0
    return [*range(-pre_count, 0), 0, *range(1, post_count + 1)]


def _coordinates(
    index: int, delta_us: int, extent_us: int
) -> dict[str, Any]:
    delta_h = delta_us / 3_600_000_000
    extent_h = extent_us / 3_600_000_000
    if index < 0:
        start_h, end_h = index * delta_h, (index + 1) * delta_h
        return {
            "time_domain": "pre_admission",
            "block_label": f"pre_{abs(start_h):g}h_to_{abs(end_h):g}h",
            "block_start_h": start_h,
            "block_end_h": end_h,
            "nominal_block_end_h": end_h,
            "information_cutoff_h": end_h,
            "is_terminal_partial": False,
        }
    if index == 0:
        return {
            "time_domain": "admission",
            "block_label": "admission_0h",
            "block_start_h": 0.0,
            "block_end_h": 0.0,
            "nominal_block_end_h": 0.0,
            "information_cutoff_h": 0.0,
            "is_terminal_partial": False,
        }
    start_h, nominal_end_h = (index - 1) * delta_h, index * delta_h
    end_h = min(nominal_end_h, extent_h)
    return {
        "time_domain": "post_admission",
        "block_label": f"post_{nominal_end_h:g}h",
        "block_start_h": start_h,
        "block_end_h": end_h,
        "nominal_block_end_h": nominal_end_h,
        "information_cutoff_h": end_h,
        "is_terminal_partial": end_h < nominal_end_h,
    }


def _aggregate(values: list[dict[str, Any]], operation: str) -> float | None:
    numbers = [float(row["value"]) for row in values]
    if not numbers:
        return None
    if operation == "median":
        return float(statistics.median(numbers))
    if operation == "minimum":
        return min(numbers)
    if operation == "maximum":
        return max(numbers)
    if operation == "last":
        return float(max(values, key=_event_order)["value"])
    raise MIMICPipelineError(f"Unsupported block aggregation: {operation}")


def block_canonical_events(
    canonical_events: Iterable[dict[str, Any]],
    stays: Iterable[dict[str, Any]],
    variable_specs: dict[str, VariableSpec],
    *,
    resolution_minutes: int,
    pre_horizon_hours: int = 72,
) -> list[dict[str, Any]]:
    """Create one resolution directly from canonical events and official outtime."""

    if resolution_minutes <= 0:
        raise MIMICPipelineError("resolution_minutes must be positive")
    delta_us = resolution_minutes * 60 * 1_000_000
    selected_specs = {
        name: spec
        for name, spec in variable_specs.items()
        if spec.role == "predictor"
        and spec.family in {"vital", "laboratory", "rolling_fluid_balance"}
    }
    events_by_stay: dict[object, list[dict[str, Any]]] = defaultdict(list)
    for row in canonical_events:
        if row.get("variable") not in selected_specs:
            continue
        if row.get("eligibility_status") != "eligible" or row.get("value") is None:
            continue
        events_by_stay[row.get("stay_id")].append(row)

    output: list[dict[str, Any]] = []
    for stay in stays:
        if not stay.get("supervised_eligible", False):
            continue
        intime = stay.get("intime")
        outtime = stay.get("official_outtime", stay.get("outtime"))
        if not isinstance(intime, datetime) or not isinstance(outtime, datetime):
            raise MIMICPipelineError("Eligible stay lacks official ICU times")
        extent_us = int((outtime - intime).total_seconds() * 1_000_000)
        if extent_us < 0:
            raise MIMICPipelineError("Eligible stay has negative official extent")

        assigned: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        min_offset = -pre_horizon_hours * 3_600_000_000
        for event in events_by_stay.get(stay.get("stay_id"), []):
            offset = int(event["event_time_offset_us"])
            if offset < min_offset or offset > extent_us:
                continue
            index = _assigned_index(offset, delta_us)
            assigned[index][str(event["variable"])].append(event)

        for index in _block_indices(extent_us, delta_us, pre_horizon_hours):
            variable_rows = assigned.get(index, {})
            row: dict[str, Any] = {
                "dataset_id": stay.get("dataset_id", "mimic_iv_3_1"),
                "site_id": stay.get("site_id", "mimic"),
                "stay_id": stay.get("stay_id"),
                "block_index": index,
                "resolution_minutes": resolution_minutes,
                "extent_basis": "official_icustays_outtime",
                **_coordinates(index, delta_us, extent_us),
            }
            observed = 0
            for name in sorted(selected_specs):
                values = sorted(variable_rows.get(name, []), key=_event_order)
                observed += len(values)
                for operation in selected_specs[name].aggregations:
                    row[f"{name}__{operation}"] = _aggregate(values, operation)
                row[f"{name}__observation_count"] = len(values)
                row[f"{name}__last_value"] = (
                    float(values[-1]["value"]) if values else None
                )
                row[f"{name}__last_observation_time_h"] = (
                    values[-1]["event_time_offset_us"] / 3_600_000_000
                    if values
                    else None
                )
            row["block_observation_count"] = observed
            row["block_has_observations"] = observed > 0
            output.append(row)
    return output
