from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mimic_iv_pipeline.blocking import block_canonical_events
from mimic_iv_pipeline.contracts import load_profile


def _event(stay_id: int, t0: datetime, hours: float, value: float, row_number: int):
    return {
        "stay_id": stay_id,
        "event_time": t0 + timedelta(hours=hours),
        "event_time_offset_us": round(hours * 3_600_000_000),
        "variable": "heart_rate",
        "value": value,
        "eligibility_status": "eligible",
        "source_table": "chartevents",
        "itemid": 220045,
        "source_row_number": row_number,
    }


def test_right_boundaries_empty_blocks_terminal_partial_and_deterministic_last(
    project_root: Path,
) -> None:
    profile = load_profile(
        project_root / "mimic/config/profiles/phase_aware_mortality_0_1.yaml"
    )
    t0 = datetime(2200, 1, 1)
    stay = {
        "stay_id": 1,
        "intime": t0,
        "official_outtime": t0 + timedelta(hours=17),
        "supervised_eligible": True,
    }
    events = [
        _event(1, t0, -8.0, 60, 1),
        _event(1, t0, -8.000001, 50, 2),
        _event(1, t0, 0, 70, 3),
        _event(1, t0, 8.0, 80, 4),
        _event(1, t0, 8.000001, 90, 5),
        _event(1, t0, 8.000001, 91, 6),
    ]
    rows = block_canonical_events(
        events, [stay], profile.variables, resolution_minutes=480
    )
    by_index = {row["block_index"]: row for row in rows}
    assert len(rows) == 13  # nine pre, admission, and three post blocks
    assert by_index[-1]["heart_rate__median"] == 60
    assert by_index[-1]["block_label"] == "pre_8h_to_0h"
    assert by_index[-2]["heart_rate__median"] == 50
    assert by_index[0]["heart_rate__median"] == 70
    assert by_index[1]["heart_rate__median"] == 80
    assert by_index[2]["heart_rate__median"] == pytest.approx(90.5)
    assert by_index[2]["heart_rate__last_value"] == 91
    assert by_index[3]["block_end_h"] == 17
    assert by_index[3]["nominal_block_end_h"] == 24
    assert by_index[3]["is_terminal_partial"] is True
    assert by_index[3]["block_has_observations"] is False
    assert "fio2__median" not in by_index[1]


def test_15m_and_8h_are_both_direct_canonical_aggregations(project_root: Path) -> None:
    profile = load_profile(
        project_root / "mimic/config/profiles/phase_aware_mortality_0_1.yaml"
    )
    t0 = datetime(2200, 1, 1)
    stay = {
        "stay_id": 2,
        "intime": t0,
        "official_outtime": t0 + timedelta(hours=8),
        "supervised_eligible": True,
    }
    events = [
        _event(2, t0, 0.1, 1, 1),
        _event(2, t0, 0.2, 100, 2),
        _event(2, t0, 7.9, 2, 3),
    ]
    rows_15m = block_canonical_events(
        events, [stay], profile.variables, resolution_minutes=15
    )
    rows_8h = block_canonical_events(
        events, [stay], profile.variables, resolution_minutes=480
    )
    post_8h = next(row for row in rows_8h if row["block_index"] == 1)
    assert post_8h["heart_rate__median"] == 2
    assert post_8h["heart_rate__observation_count"] == 3
    assert sum(row["heart_rate__observation_count"] for row in rows_15m) == 3


def test_event_at_official_outtime_is_included_and_later_event_is_not(
    project_root: Path,
) -> None:
    profile = load_profile(
        project_root / "mimic/config/profiles/phase_aware_mortality_0_1.yaml"
    )
    t0 = datetime(2200, 1, 1)
    stay = {
        "stay_id": 3,
        "intime": t0,
        "official_outtime": t0 + timedelta(hours=1),
        "supervised_eligible": True,
    }
    rows = block_canonical_events(
        [_event(3, t0, 1, 80, 1), _event(3, t0, 1.1, 90, 2)],
        [stay],
        profile.variables,
        resolution_minutes=15,
    )
    assert sum(row["heart_rate__observation_count"] for row in rows) == 1
    assert next(row for row in rows if row["block_index"] == 4)[
        "heart_rate__last_value"
    ] == 80
