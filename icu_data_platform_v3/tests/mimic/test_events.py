from datetime import datetime, timedelta

import pytest

from mimic_iv_pipeline.contracts import PlausibilityRange, VariableSpec
from mimic_iv_pipeline.events import derive_observed_bmi, normalize_numeric_event


def _spec(name: str, unit: str = "degC", eligibility: str = "enabled") -> VariableSpec:
    return VariableSpec(name, "predictor", "vital", unit, ("median",), eligibility)


def test_unit_conversion_precedes_plausibility_and_preserves_source() -> None:
    t0 = datetime(2200, 1, 1)
    row = normalize_numeric_event(
        {"stay_id": 1, "event_time": t0 + timedelta(hours=1), "source_value": 98.6, "source_unit": "degF"},
        variable=_spec("core_temp"),
        plausibility=PlausibilityRange(25, 45),
        conversion_id="fahrenheit_to_celsius",
        icu_intime=t0,
    )
    assert row["source_value"] == 98.6
    assert row["value"] == pytest.approx(37.0)
    assert row["event_time_offset_us"] == 3_600_000_000


def test_human_gated_conversion_cannot_enter_blocks() -> None:
    t0 = datetime(2200, 1, 1)
    row = normalize_numeric_event(
        {"stay_id": 1, "event_time": t0, "source_value": 28.01},
        variable=_spec("urea", "mmol/L", "human_gate"),
        plausibility=PlausibilityRange(),
        conversion_id="bun_mg_per_dL_to_urea_mmol_per_L",
        icu_intime=t0,
    )
    assert row["value"] == pytest.approx(10.0)
    assert row["eligibility_status"] == "human_gate"


def test_bmi_uses_medians_and_never_imputes() -> None:
    t0 = datetime(2200, 1, 1)
    events = [
        {"variable": "height_cm", "value": 160.0, "event_time": t0, "eligibility_status": "eligible"},
        {"variable": "height_cm", "value": 180.0, "event_time": t0, "eligibility_status": "eligible"},
        {"variable": "weight_kg", "value": 72.25, "event_time": t0, "eligibility_status": "eligible"},
    ]
    result = derive_observed_bmi(events, intime=t0, outtime=t0 + timedelta(hours=1))
    assert result["height_cm"] == 170.0
    assert result["bmi"] == pytest.approx(25.0)
    missing = derive_observed_bmi(events[:2], intime=t0, outtime=t0)
    assert missing["bmi"] is None
