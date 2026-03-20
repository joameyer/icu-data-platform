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

from icu_data_platform.sources.asic.extract.raw_tables import (  # noqa: E402
    DEFAULT_ASIC_RAW_DATA_DIR,
    load_static_tables,
    load_static_translation,
)
from icu_data_platform.sources.asic.harmonize.static import (  # noqa: E402
    FINAL_STATIC_COLUMNS,
    build_harmonized_static_table,
    harmonize_static_tables,
)


class TestASICStaticHarmonization(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DEFAULT_ASIC_RAW_DATA_DIR.exists():
            raise unittest.SkipTest(
                f"ASIC sample data not found at {DEFAULT_ASIC_RAW_DATA_DIR}"
            )

        cls.translation = load_static_translation()
        cls.raw_tables = load_static_tables(DEFAULT_ASIC_RAW_DATA_DIR)

    def test_build_harmonized_static_table_maps_core_fields(self) -> None:
        raw_df = self.raw_tables["asic_UK02"]
        harmonized_df, source_map = build_harmonized_static_table(
            raw_df,
            "asic_UK02",
            self.translation,
        )

        self.assertEqual(list(harmonized_df.columns), FINAL_STATIC_COLUMNS)
        self.assertEqual(str(harmonized_df["stay_id"].dtype), "Int64")
        self.assertTrue(
            set(harmonized_df["sex"].dropna().unique()).issubset({"F", "M"})
        )
        self.assertFalse(
            any(
                value in {"L", "M", "P", "1", "2", "3", "X"}
                for value in harmonized_df["bmi_group"].dropna().astype(str)
            )
        )
        self.assertEqual(set(source_map["stay_id"]), {"PseudoID", "Pseudo-ID"})
        self.assertTrue(
            set(harmonized_df["hosp_mortality"].dropna().astype(int).unique()).issubset(
                {0, 1}
            )
        )
        self.assertTrue(
            set(harmonized_df["icu_mortality"].dropna().astype(int).unique()).issubset(
                {0, 1}
            )
        )

    def test_harmonize_static_tables_keeps_missing_columns_explicit(self) -> None:
        result = harmonize_static_tables(DEFAULT_ASIC_RAW_DATA_DIR)

        uk03 = result.tables_by_hospital["asic_UK03"]
        for column in [
            "hosp_los",
            "icu_readmit",
            "icu_mortality",
            "dialysis_free_days",
            "vent_free_days",
        ]:
            self.assertTrue(uk03[column].isna().all(), column)

        expected_rows = sum(df.shape[0] for df in self.raw_tables.values())
        self.assertEqual(int(result.combined.shape[0]), expected_rows)
        self.assertEqual(int(result.schema_summary.shape[0]), len(self.raw_tables))

    def test_static_harmonization_warns_on_unexpected_values(self) -> None:
        df = pd.DataFrame(
            {
                "PseudoID": [1],
                "clusterGeschlecht": ["X"],
                "BMI": ["Z"],
                "Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)": ["unknown"],
                "Sterblichkeit": ["weird"],
            }
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            harmonized_df, _ = build_harmonized_static_table(
                df,
                "asic_TEST",
                self.translation,
            )

        self.assertEqual(int(harmonized_df.loc[0, "stay_id"]), 1)
        self.assertEqual(len(caught), 4)
        messages = " | ".join(str(w.message) for w in caught)
        self.assertIn("sex", messages)
        self.assertIn("bmi_group", messages)
        self.assertIn("hosp_mortality", messages)
        self.assertIn("icu_mortality", messages)
