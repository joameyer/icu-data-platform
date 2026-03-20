from __future__ import annotations

import sys
import unittest
import warnings
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.qc.dynamic_checks import (  # noqa: E402
    DEFAULT_DYNAMIC_DATA_DIR,
    build_harmonized_dynamic_table,
    coerce_numeric_series,
    find_non_numeric_value_issues,
    flag_cross_hospital_distribution_issues,
    load_dynamic_tables,
    load_dynamic_translation,
    numeric_distribution_summary,
    parse_ie_ratio_value,
)


class TestASICDynamicChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DEFAULT_DYNAMIC_DATA_DIR.exists():
            raise unittest.SkipTest(
                f"ASIC sample data not found at {DEFAULT_DYNAMIC_DATA_DIR}"
            )

        cls.translation = load_dynamic_translation()
        cls.dynamic_tables = load_dynamic_tables(DEFAULT_DYNAMIC_DATA_DIR)

    def test_parse_ie_ratio_value(self) -> None:
        self.assertAlmostEqual(parse_ie_ratio_value("1/1.5"), 1 / 1.5)
        self.assertAlmostEqual(parse_ie_ratio_value("1.5/1"), 1.5)
        self.assertAlmostEqual(parse_ie_ratio_value("0.769"), 0.769)
        self.assertTrue(pd.isna(parse_ie_ratio_value("not_a_ratio")))

    def test_non_numeric_issue_report_flags_known_cases(self) -> None:
        issues = find_non_numeric_value_issues(
            dynamic_tables=self.dynamic_tables,
            translation=self.translation,
        )

        uk02_ie = issues[
            (issues["hospital"] == "asic_UK02") & (issues["raw_column"] == "I:E")
        ]
        self.assertEqual(len(uk02_ie), 1)
        self.assertGreater(int(uk02_ie.iloc[0]["raw_non_numeric_count"]), 0)
        self.assertEqual(int(uk02_ie.iloc[0]["unresolved_after_custom_parser_count"]), 0)

        uk04_bilirubin = issues[
            (issues["hospital"] == "asic_UK04")
            & (issues["raw_column"] == "Bilirubin_ges.")
        ]
        self.assertEqual(len(uk04_bilirubin), 1)
        self.assertGreater(
            int(uk04_bilirubin.iloc[0]["resolved_by_custom_parser_count"]), 0
        )
        self.assertEqual(
            int(uk04_bilirubin.iloc[0]["unresolved_after_custom_parser_count"]), 0
        )

    def test_coerce_numeric_series_handles_uk04_special_strings(self) -> None:
        series = pd.Series(["<0.15", "storniert", "2.4"])
        parsed = coerce_numeric_series(series, "crp", hospital="asic_UK04")

        self.assertEqual(float(parsed.iloc[0]), 0.0)
        self.assertTrue(pd.isna(parsed.iloc[1]))
        self.assertEqual(float(parsed.iloc[2]), 2.4)

    def test_harmonized_ie_ratio_keeps_uk02_values(self) -> None:
        uk02_df = self.dynamic_tables["asic_UK02"]
        harmonized_df, _ = build_harmonized_dynamic_table(
            uk02_df,
            "asic_UK02",
            self.translation,
        )

        self.assertGreater(int(harmonized_df["ie_ratio"].notna().sum()), 0)

        parsed = coerce_numeric_series(uk02_df["I:E"], "ie_ratio").dropna()
        self.assertGreater(int(parsed.shape[0]), 0)
        self.assertAlmostEqual(float(parsed.iloc[0]), 1 / 1.5)

    def test_harmonized_table_warns_on_unexpected_strings(self) -> None:
        df = pd.DataFrame({"CRP": ["1.2", "unexpected", None]})
        translation = {"CRP": "crp"}

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            harmonized_df, _ = build_harmonized_dynamic_table(
                df,
                "asic_TEST",
                translation,
            )

        self.assertAlmostEqual(float(harmonized_df.loc[0, "crp"]), 1.2)
        self.assertTrue(pd.isna(harmonized_df.loc[1, "crp"]))
        self.assertEqual(len(caught), 1)
        self.assertIn("asic_TEST", str(caught[0].message))
        self.assertIn("unexpected", str(caught[0].message))

    def test_distribution_issue_report_runs(self) -> None:
        harmonized_tables = {
            hospital: build_harmonized_dynamic_table(df, hospital, self.translation)[0]
            for hospital, df in self.dynamic_tables.items()
        }

        summary_df = numeric_distribution_summary(harmonized_tables, min_non_null=20)
        issue_df = flag_cross_hospital_distribution_issues(
            summary_df,
            min_hospitals=4,
            fence_factor=1.5,
        )

        self.assertFalse(summary_df.empty)
        self.assertTrue(
            {
                "canonical_name",
                "hospital",
                "n",
                "flagged_metrics",
                "min",
                "median",
                "iqr",
                "max",
                "range_width",
            }.issubset(issue_df.columns)
        )


if __name__ == "__main__":
    unittest.main()
