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
            }
            self.assertEqual(set(output_paths), expected_keys)
            self.assertTrue(all(path.exists() for path in output_paths.values()))

            static_df = pd.read_csv(output_paths["static_harmonized"])
            dynamic_df = pd.read_csv(output_paths["dynamic_harmonized"])
            self.assertGreater(int(static_df.shape[0]), 0)
            self.assertGreater(int(dynamic_df.shape[0]), 0)
