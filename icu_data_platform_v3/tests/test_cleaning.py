from __future__ import annotations

from collections import Counter
import math

import pyarrow as pa

from asic_pipeline.cleaning.pipeline import (
    CleaningRule,
    ListCleaningRule,
    _clean_height_list,
    _clean_numeric_array,
)


def test_power_of_ten_repair_requires_exactly_one_valid_factor() -> None:
    rule = CleaningRule(
        rule_id="PH",
        table="dynamic",
        variable="ph_art",
        hard_min=6.8,
        hard_max=7.8,
        invalid_zero=False,
        allow_power_of_ten_repair=True,
        source="test",
    )
    counts: Counter[str] = Counter()
    source = pa.array(
        [7.4, 740.0, 74.0, 7400.0, 8.0, 0.0, None, math.nan],
        type=pa.float64(),
    )

    output = _clean_numeric_array(
        source,
        rule,
        (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        counts,
    )

    assert output.to_pylist() == [7.4, 7.4, 7.4, 7.4, None, None, None, None]
    assert counts["kept_in_range"] == 1
    assert counts["corrected_factor_0.001"] == 1
    assert counts["corrected_factor_0.01"] == 1
    assert counts["corrected_factor_0.1"] == 1
    assert counts["masked_out_of_range"] == 2
    assert counts["masked_nonfinite"] == 1


def test_ambiguous_power_of_ten_repair_is_masked() -> None:
    rule = CleaningRule(
        rule_id="BP",
        table="dynamic",
        variable="sbp",
        hard_min=0,
        hard_max=300,
        invalid_zero=True,
        allow_power_of_ten_repair=True,
        source="test",
    )
    counts: Counter[str] = Counter()

    output = _clean_numeric_array(
        pa.array([3000.0], type=pa.float64()),
        rule,
        (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        counts,
    )

    assert output.to_pylist() == [None]
    assert counts["masked_ambiguous_power_of_ten"] == 1


def test_height_cleaning_preserves_list_order_duplicates_and_empty_lists() -> None:
    rule = ListCleaningRule(
        rule_id="HEIGHT",
        table="static",
        variable="height_measurements_cm",
        element_hard_min=120,
        element_hard_max=220,
    )
    counts: Counter[str] = Counter()
    source = pa.array(
        [[170.0, 8.0, None, math.nan, 180.0, 170.0], [8.0], [], None],
        type=pa.list_(pa.float64()),
    )

    output = _clean_height_list(source, rule, counts)

    assert output.to_pylist() == [[170.0, 180.0, 170.0], [], [], None]
    assert counts["removed_out_of_range_element_count"] == 2
    assert counts["removed_null_element_count"] == 1
    assert counts["removed_nonfinite_element_count"] == 1
    assert counts["cells_empty_after_cleaning"] == 1
