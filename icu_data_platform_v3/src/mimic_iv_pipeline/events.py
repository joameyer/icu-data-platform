from __future__ import annotations

from datetime import datetime
import math
import statistics
from typing import Any, Iterable

from mimic_iv_pipeline.contracts import PlausibilityRange, VariableSpec
from mimic_iv_pipeline.errors import MIMICPipelineError


CONVERSIONS = {
    "identity": lambda value: value,
    "fahrenheit_to_celsius": lambda value: (value - 32.0) / 1.8,
    "inch_to_cm": lambda value: value * 2.54,
    "g_per_dL_to_dg_per_L": lambda value: value * 100.0,
    "bilirubin_mg_per_dL_to_umol_per_L": lambda value: value * 17.104,
    "creatinine_mg_per_dL_to_umol_per_L": lambda value: value * 88.4,
    "hemoglobin_g_per_dL_to_mmol_per_L": lambda value: value / 1.611,
    "bun_mg_per_dL_to_urea_mmol_per_L": lambda value: value / 2.801,
}


def normalize_numeric_event(
    row: dict[str, Any],
    *,
    variable: VariableSpec,
    plausibility: PlausibilityRange,
    conversion_id: str,
    icu_intime: datetime,
) -> dict[str, Any]:
    """Normalize one selected source row without discarding rejected evidence."""

    event_time = row.get("event_time")
    if not isinstance(event_time, datetime):
        raise MIMICPipelineError("Selected event is missing a valid event_time")
    if event_time.tzinfo != icu_intime.tzinfo:
        raise MIMICPipelineError("event_time and icu_intime timezone forms differ")
    reasons: list[str] = []
    source_value = row.get("source_value")
    try:
        numeric = float(source_value)
    except (TypeError, ValueError):
        numeric = math.nan
        reasons.append("not_numeric")
    if not math.isfinite(numeric):
        if "not_numeric" not in reasons:
            reasons.append("nonfinite")
        value = None
    else:
        conversion = CONVERSIONS.get(conversion_id)
        if conversion is None:
            raise MIMICPipelineError(f"Unknown conversion: {conversion_id}")
        converted = float(conversion(numeric))
        if not math.isfinite(converted):
            reasons.append("nonfinite_after_conversion")
            value = None
        elif not plausibility.contains(converted):
            reasons.append("outside_plausible_range")
            value = None
        else:
            value = converted

    eligibility = "eligible"
    if variable.eligibility != "enabled":
        eligibility = variable.eligibility
        reasons.append(f"variable_{variable.eligibility}")
    if value is None:
        eligibility = "rejected"

    offset_us = int((event_time - icu_intime).total_seconds() * 1_000_000)
    return {
        **row,
        "event_time": event_time,
        "icu_intime": icu_intime,
        "event_time_offset_us": offset_us,
        "variable": variable.name,
        "role": variable.role,
        "family": variable.family,
        "value": value,
        "unit": variable.unit,
        "conversion_id": conversion_id,
        "plausibility_status": "accepted" if value is not None else "rejected",
        "eligibility_status": eligibility,
        "rejection_reasons": reasons,
    }


def derive_observed_bmi(
    measurement_events: Iterable[dict[str, Any]],
    *,
    intime: datetime,
    outtime: datetime,
) -> dict[str, Any]:
    """Use medians of valid observed ICU height and weight; never impute."""

    heights: list[float] = []
    weights: list[float] = []
    for row in measurement_events:
        event_time, value = row.get("event_time"), row.get("value")
        if not isinstance(event_time, datetime) or not intime <= event_time <= outtime:
            continue
        if row.get("eligibility_status") != "eligible" or value is None:
            continue
        if row.get("variable") == "height_cm":
            heights.append(float(value))
        elif row.get("variable") == "weight_kg":
            weights.append(float(value))
    median_height = statistics.median(heights) if heights else None
    median_weight = statistics.median(weights) if weights else None
    bmi = None
    if median_height is not None and median_weight is not None:
        bmi = median_weight / ((median_height / 100.0) ** 2)
    return {
        "height_cm": median_height,
        "weight_kg": median_weight,
        "bmi": bmi,
        "height_observation_count": len(heights),
        "weight_observation_count": len(weights),
        "bmi_observed_only": True,
    }
