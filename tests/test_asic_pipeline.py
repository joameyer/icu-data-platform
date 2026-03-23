from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.extract.raw_tables import (  # noqa: E402
    DEFAULT_ASIC_RAW_DATA_DIR,
    load_dynamic_tables,
    load_static_tables,
)
from icu_data_platform.sources.asic.pipeline import (  # noqa: E402
    build_asic_harmonized_dataset,
    write_asic_harmonized_dataset,
)
from icu_data_platform.sources.asic.qc.stay_id import (  # noqa: E402
    assert_valid_asic_stay_ids,
    build_asic_stay_id_qc,
)


class TestASICPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DEFAULT_ASIC_RAW_DATA_DIR.exists():
            raise unittest.SkipTest(
                f"ASIC sample data not found at {DEFAULT_ASIC_RAW_DATA_DIR}"
            )

        cls.raw_static_tables = load_static_tables(DEFAULT_ASIC_RAW_DATA_DIR)
        cls.raw_dynamic_tables = load_dynamic_tables(DEFAULT_ASIC_RAW_DATA_DIR)

    def test_build_asic_harmonized_dataset(self) -> None:
        dataset = build_asic_harmonized_dataset(DEFAULT_ASIC_RAW_DATA_DIR)

        self.assertEqual(
            int(dataset.static.combined.shape[0]),
            sum(df.shape[0] for df in self.raw_static_tables.values()),
        )
        self.assertEqual(
            int(dataset.dynamic.combined.shape[0]),
            sum(df.shape[0] for df in self.raw_dynamic_tables.values()),
        )
        self.assertIn("stay_id_local", dataset.static.combined.columns)
        self.assertIn("stay_id_global", dataset.static.combined.columns)
        self.assertIn("stay_id_local", dataset.dynamic.combined.columns)
        self.assertIn("stay_id_global", dataset.dynamic.combined.columns)
        self.assertTrue(dataset.stay_id_qc.mapping_failures.empty)
        self.assertTrue(dataset.stay_id_qc.static_duplicate_global_ids.empty)
        self.assertTrue(dataset.stay_id_qc.duplicate_dynamic_time_index.empty)
        summary = dict(dataset.stay_id_qc.summary[["metric", "value"]].itertuples(index=False))
        self.assertEqual(summary["static_rows_total"], int(dataset.static.combined.shape[0]))
        self.assertEqual(
            summary["static_unique_stay_id_global_total"],
            int(dataset.static.combined["stay_id_global"].nunique(dropna=True)),
        )
        self.assertFalse(dataset.static.source_map.empty)
        self.assertFalse(dataset.dynamic.source_map.empty)
        self.assertEqual(
            int(dataset.dynamic.non_numeric_issues["unresolved_after_custom_parser_count"].sum()),
            0,
        )

    def test_write_asic_harmonized_dataset_outputs_expected_files(self) -> None:
        dataset = build_asic_harmonized_dataset(DEFAULT_ASIC_RAW_DATA_DIR)
        with TemporaryDirectory() as tmpdir:
            output_paths = write_asic_harmonized_dataset(
                dataset,
                Path(tmpdir),
                output_format="csv",
            )

            expected_keys = {
                "static_harmonized",
                "static_source_map",
                "static_schema_summary",
                "static_categorical_value_summary",
                "dynamic_harmonized",
                "dynamic_source_map",
                "dynamic_schema_summary",
                "dynamic_non_numeric_issues",
                "dynamic_distribution_summary",
                "dynamic_distribution_issues",
                "qc_stay_id_summary",
                "qc_stay_id_unique_stays_per_hospital",
                "qc_stay_id_local_id_collisions",
                "qc_stay_id_missing_id_values",
                "qc_stay_id_duplicate_static_global_ids",
                "qc_stay_id_mapping_failures",
                "qc_stay_id_duplicate_dynamic_time_index",
            }
            self.assertEqual(set(output_paths), expected_keys)
            self.assertTrue(all(path.exists() for path in output_paths.values()))

            static_df = pd.read_csv(output_paths["static_harmonized"])
            dynamic_df = pd.read_csv(output_paths["dynamic_harmonized"])
            self.assertGreater(int(static_df.shape[0]), 0)
            self.assertGreater(int(dynamic_df.shape[0]), 0)

    def test_stay_id_qc_reports_failures_for_bad_inputs(self) -> None:
        static_df = pd.DataFrame(
            {
                "hospital_id": ["asic_A", "asic_A"],
                "stay_id_local": ["1", "1"],
                "stay_id_global": ["asic_A_1", "asic_A_1"],
            }
        )
        dynamic_df = pd.DataFrame(
            {
                "hospital_id": ["asic_A", "asic_A", "asic_B", "asic_A"],
                "stay_id_local": ["1", "1", "9", None],
                "stay_id_global": ["asic_A_1", "asic_A_1", "asic_B_9", None],
                "time": ["0 days", "0 days", "0 days", "1 day"],
            }
        )

        qc_result = build_asic_stay_id_qc(static_df=static_df, dynamic_df=dynamic_df)

        self.assertFalse(qc_result.static_duplicate_global_ids.empty)
        self.assertIn(
            "duplicate_count_per_stay_id_global",
            qc_result.static_duplicate_global_ids.columns,
        )
        self.assertEqual(
            int(qc_result.static_duplicate_global_ids.iloc[0]["duplicate_count_per_stay_id_global"]),
            2,
        )
        self.assertFalse(qc_result.mapping_failures.empty)
        self.assertFalse(qc_result.missing_id_values.empty)
        self.assertFalse(qc_result.duplicate_dynamic_time_index.empty)
        summary = dict(qc_result.summary[["metric", "value"]].itertuples(index=False))
        self.assertEqual(summary["static_rows_total"], 2)
        self.assertEqual(summary["static_unique_stay_id_global_total"], 1)
        with self.assertRaisesRegex(ValueError, "ASIC pooled stay-ID QC failed"):
            assert_valid_asic_stay_ids(qc_result)
