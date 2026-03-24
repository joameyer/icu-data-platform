from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.cohort import (  # noqa: E402
    build_asic_chapter1_cohort,
)


class TestASICChapter1Cohort(unittest.TestCase):
    def _cohort_df(
        self,
        hospital_id: str,
        stay_ids: list[str],
        icu_mortality: list[object],
        readmission: list[object] | None = None,
    ) -> pd.DataFrame:
        if readmission is None:
            readmission = [0] * len(stay_ids)
        return pd.DataFrame(
            {
                "stay_id_global": stay_ids,
                "hospital_id": [hospital_id] * len(stay_ids),
                "readmission": pd.Series(readmission, dtype="Int64"),
                "icu_mortality": pd.Series(icu_mortality, dtype="Int64"),
                "icd10_codes": pd.Series(["A00"] * len(stay_ids), dtype="string"),
                "icu_admission_time": pd.Series([0] * len(stay_ids), dtype="Int64"),
                "icu_end_time_proxy": pd.Series(
                    ["0 days 01:00:00"] * len(stay_ids),
                    dtype="string",
                ),
            }
        )

    def test_build_asic_chapter1_cohort_treats_partially_non_missing_icu_mortality_as_available(
        self,
    ) -> None:
        cohort_df = self._cohort_df(
            "asic_TEST",
            ["asic_TEST_1", "asic_TEST_2"],
            [0, pd.NA],
        )
        dynamic_df = pd.DataFrame(
            {
                "hospital_id": ["asic_TEST", "asic_TEST"],
                "stay_id_global": ["asic_TEST_1", "asic_TEST_2"],
                "time": ["0 days 00:00:00", "0 days 00:00:00"],
                "heart_rate": [80, 81],
                "map": [70, 71],
                "resp_rate": [14, 15],
                "spo2": [96, 97],
            }
        )

        result = build_asic_chapter1_cohort(cohort_df, dynamic_df)
        site_eligibility = result.site_eligibility.iloc[0]

        self.assertTrue(bool(site_eligibility["icu_mortality_available"]))
        self.assertTrue(bool(site_eligibility["site_included_ch1"]))
        self.assertEqual(
            site_eligibility["icu_mortality_verification_status"],
            "verified_non_missing_harmonized_icu_mortality",
        )

    def test_build_asic_chapter1_cohort_raises_when_raw_icu_mortality_exists_but_harmonized_is_all_missing(
        self,
    ) -> None:
        cohort_df = self._cohort_df(
            "asic_TEST",
            ["asic_TEST_1", "asic_TEST_2"],
            [pd.NA, pd.NA],
        )
        dynamic_df = pd.DataFrame(
            {
                "hospital_id": ["asic_TEST", "asic_TEST"],
                "stay_id_global": ["asic_TEST_1", "asic_TEST_2"],
                "time": ["0 days 00:00:00", "0 days 00:00:00"],
                "heart_rate": [80, 81],
                "map": [70, 71],
                "resp_rate": [14, 15],
                "spo2": [96, 97],
            }
        )
        static_source_map = pd.DataFrame(
            {
                "hospital": ["asic_TEST"],
                "canonical_name": ["icu_mortality"],
                "raw_source_columns_used": [["Sterblichkeit"]],
            }
        )
        raw_static_tables = {
            "asic_TEST": pd.DataFrame(
                {
                    "Sterblichkeit": ["ICU", "0"],
                }
            )
        }

        with self.assertRaisesRegex(
            ValueError,
            "harmonized icu_mortality is entirely missing, but raw source column",
        ):
            build_asic_chapter1_cohort(
                cohort_df,
                dynamic_df,
                static_source_map=static_source_map,
                raw_static_tables=raw_static_tables,
            )

    def test_build_asic_chapter1_cohort_uses_group_based_core_vital_eligibility(self) -> None:
        stay_ids = [f"asic_TEST_{index}" for index in range(3)]
        cohort_df = self._cohort_df(
            "asic_TEST",
            stay_ids,
            [0] * 3,
        )
        dynamic_df = pd.DataFrame(
            {
                "hospital_id": ["asic_TEST"] * 3,
                "stay_id_global": stay_ids,
                "time": ["0 days 00:00:00"] * 3,
                "heart_rate": [80, 81, 82],
                "map": [pd.NA, pd.NA, pd.NA],
                "sbp": [110, 112, 108],
                "dbp": [70, 71, 69],
                "resp_rate": [14, 15, 16],
                "spo2": [pd.NA, pd.NA, pd.NA],
                "sao2": [97, 98, 99],
                "core_temp": [pd.NA, pd.NA, pd.NA],
            }
        )

        result = build_asic_chapter1_cohort(cohort_df, dynamic_df)
        site_eligibility = result.site_eligibility.iloc[0]

        self.assertEqual(int(site_eligibility["usable_core_vital_group_count"]), 4)
        self.assertTrue(bool(site_eligibility["core_vital_coverage_sufficient"]))
        self.assertTrue(bool(site_eligibility["include_in_chapter1"]))
        self.assertEqual(site_eligibility["blood_pressure_satisfied_by"], ["sbp", "dbp"])
        self.assertEqual(site_eligibility["oxygenation_satisfied_by"], ["sao2"])

        coverage = result.core_vital_group_coverage.set_index("physiologic_group")
        self.assertEqual(coverage.at["blood_pressure", "satisfying_variables"], ["sbp", "dbp"])
        self.assertFalse(bool(coverage.at["core_temp_optional", "required_for_inclusion"]))
