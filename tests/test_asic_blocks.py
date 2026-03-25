from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.blocks import (  # noqa: E402
    build_asic_chapter1_8h_blocks,
)


class TestASICChapter1Blocks(unittest.TestCase):
    def test_build_asic_chapter1_8h_blocks_constructs_only_completed_blocks(self) -> None:
        chapter1_cohort_df = pd.DataFrame(
            {
                "stay_id_global": ["asic_A_1", "asic_A_2", "asic_B_1", "asic_B_2"],
                "hospital_id": ["asic_A", "asic_A", "asic_B", "asic_B"],
                "icu_admission_time": pd.Series([0, 0, 0, 0], dtype="Int64"),
                "icu_end_time_proxy": pd.Series(
                    [
                        "0 days 07:59:59",
                        "0 days 08:00:00",
                        "0 days 23:59:59",
                        "1 days 00:00:00",
                    ],
                    dtype="string",
                ),
            }
        )
        dynamic_df = pd.DataFrame(
            {
                "stay_id_global": [
                    "asic_A_1",
                    "asic_A_2",
                    "asic_B_1",
                    "asic_B_2",
                    "asic_B_2",
                ],
                "hospital_id": ["asic_A", "asic_A", "asic_B", "asic_B", "asic_B"],
                "time": [
                    "0 days 00:00:00",
                    "0 days 08:00:00",
                    "0 days 23:59:59",
                    "-1h",
                    "1 days 00:00:00",
                ],
            }
        )

        result = build_asic_chapter1_8h_blocks(chapter1_cohort_df, dynamic_df)

        expected_blocks = [
            ("asic_A_2", 0, 0, 8, 8),
            ("asic_B_1", 0, 0, 8, 8),
            ("asic_B_1", 1, 8, 16, 16),
            ("asic_B_2", 0, 0, 8, 8),
            ("asic_B_2", 1, 8, 16, 16),
            ("asic_B_2", 2, 16, 24, 24),
        ]
        actual_blocks = list(
            result.block_index[
                ["stay_id_global", "block_index", "block_start_h", "block_end_h", "prediction_time_h"]
            ].itertuples(index=False, name=None)
        )
        self.assertEqual(actual_blocks, expected_blocks)

        stay_counts = result.stay_block_counts.set_index("stay_id_global")
        self.assertEqual(int(stay_counts.loc["asic_A_1", "completed_block_count"]), 0)
        self.assertEqual(int(stay_counts.loc["asic_A_2", "completed_block_count"]), 1)
        self.assertEqual(int(stay_counts.loc["asic_B_1", "completed_block_count"]), 2)
        self.assertEqual(int(stay_counts.loc["asic_B_2", "completed_block_count"]), 3)
        self.assertTrue(bool(stay_counts.loc["asic_A_2", "ends_exactly_on_8h_boundary"]))
        self.assertTrue(bool(stay_counts.loc["asic_B_2", "ends_exactly_on_8h_boundary"]))
        self.assertEqual(int(stay_counts.loc["asic_B_2", "terminal_block_end_h"]), 24)

        negative_times = result.negative_dynamic_time_qc.set_index("stay_id_global")
        self.assertEqual(
            int(negative_times.loc["asic_B_2", "negative_dynamic_time_row_count"]),
            1,
        )
        self.assertEqual(float(negative_times.loc["asic_B_2", "min_negative_time_h"]), -1.0)
        self.assertEqual(negative_times.loc["asic_B_2", "example_negative_times"], ["-1h"])

        distribution = result.block_count_distribution_by_hospital.set_index(
            ["hospital_id", "completed_block_count"]
        )
        self.assertEqual(int(distribution.loc[("asic_A", 0), "stay_count"]), 1)
        self.assertEqual(int(distribution.loc[("asic_A", 1), "stay_count"]), 1)
        self.assertEqual(int(distribution.loc[("asic_B", 2), "stay_count"]), 1)
        self.assertEqual(int(distribution.loc[("asic_B", 3), "stay_count"]), 1)

        metrics = dict(result.qc_summary[["metric", "value"]].itertuples(index=False))
        self.assertEqual(int(metrics["retained_stays_with_zero_blocks"]), 1)
        self.assertEqual(int(metrics["retained_stays_with_one_or_more_blocks"]), 3)
        self.assertEqual(int(metrics["block_rows_total"]), 6)
        self.assertEqual(int(metrics["negative_dynamic_time_rows_in_retained_input"]), 1)
        self.assertEqual(
            int(metrics["all_constructed_blocks_end_on_or_before_icu_end_time_proxy"]),
            1,
        )
        self.assertEqual(int(metrics["retained_stays_ending_exactly_on_8h_boundary"]), 2)
        self.assertEqual(
            int(metrics["exact_boundary_stays_have_terminal_block_end_equal_proxy"]),
            1,
        )

        examples = result.example_stays.set_index("example_type")
        self.assertEqual(examples.loc["zero_blocks", "block_boundaries"], [])
        self.assertEqual(examples.loc["exact_8h_boundary", "block_boundaries"], ["[0, 8)"])

    def test_build_asic_chapter1_8h_blocks_requires_zero_admission_anchor(self) -> None:
        chapter1_cohort_df = pd.DataFrame(
            {
                "stay_id_global": ["asic_A_1"],
                "hospital_id": ["asic_A"],
                "icu_admission_time": pd.Series([1], dtype="Int64"),
                "icu_end_time_proxy": pd.Series(["0 days 08:00:00"], dtype="string"),
            }
        )
        dynamic_df = pd.DataFrame(
            {
                "stay_id_global": ["asic_A_1"],
                "hospital_id": ["asic_A"],
                "time": ["0 days 08:00:00"],
            }
        )

        with self.assertRaisesRegex(ValueError, "icu_admission_time = 0"):
            build_asic_chapter1_8h_blocks(chapter1_cohort_df, dynamic_df)
