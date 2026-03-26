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

from icu_data_platform.analysis_seed.chapter1.io import (  # noqa: E402
    Chapter1SeedInputTables,
)
from icu_data_platform.analysis_seed.chapter1.pipeline import (  # noqa: E402
    build_and_write_chapter1_seed_dataset,
    build_chapter1_seed_dataset,
)
from icu_data_platform.sources.asic.extract.raw_tables import (  # noqa: E402
    DEFAULT_ASIC_RAW_DATA_DIR,
)
from icu_data_platform.sources.asic.pipeline import (  # noqa: E402
    build_asic_harmonized_dataset,
    build_asic_standardized_dataset,
    write_asic_harmonized_dataset,
    write_asic_standardized_dataset,
)


class TestChapter1Seed(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DEFAULT_ASIC_RAW_DATA_DIR.exists():
            raise unittest.SkipTest(
                f"ASIC sample data not found at {DEFAULT_ASIC_RAW_DATA_DIR}"
            )

        cls.harmonized_dataset = build_asic_harmonized_dataset(DEFAULT_ASIC_RAW_DATA_DIR)
        cls.standardized_dataset = build_asic_standardized_dataset(cls.harmonized_dataset)
        cls.seed_inputs = Chapter1SeedInputTables(
            static_harmonized=cls.harmonized_dataset.static.combined,
            dynamic_harmonized=cls.harmonized_dataset.dynamic.combined,
            block_index=cls.standardized_dataset.blocked_8h.block_index,
            blocked_dynamic_features=cls.standardized_dataset.blocked_8h.blocked_dynamic_features,
            stay_block_counts=cls.standardized_dataset.blocked_8h.stay_block_counts,
            stay_level=cls.standardized_dataset.stay_level.table,
        )
        cls.seed_dataset = build_chapter1_seed_dataset(cls.seed_inputs)

    def test_standardized_dataset_builds_generic_block_artifacts(self) -> None:
        standardized = self.standardized_dataset
        stay_level = standardized.stay_level.table
        blocks = standardized.blocked_8h

        self.assertEqual(
            int(stay_level.shape[0]),
            int(self.harmonized_dataset.static.combined["stay_id_global"].nunique(dropna=True)),
        )
        self.assertEqual(
            int(blocks.blocked_dynamic_features.shape[0]),
            int(blocks.block_index.shape[0]),
        )
        metrics = dict(blocks.qc_summary[["metric", "value"]].itertuples(index=False))
        self.assertEqual(int(metrics["block_index_is_generic_pre_analytic_structure"]), 1)
        self.assertEqual(
            int(metrics["stays_total"]),
            int(stay_level.shape[0]),
        )

    def test_chapter1_seed_builds_retained_stays_instances_and_model_ready_dataset(self) -> None:
        dataset = self.seed_dataset

        self.assertGreater(int(dataset.cohort.table.shape[0]), 0)
        self.assertTrue(dataset.cohort.table["readmission"].eq(0).all())
        self.assertTrue(dataset.cohort.table["first_stay_proxy_eligible"].all())

        self.assertGreater(int(dataset.valid_instances.valid_instances.shape[0]), 0)
        self.assertTrue(dataset.valid_instances.valid_instances["valid_instance"].all())
        self.assertTrue(
            set(dataset.valid_instances.valid_instances["stay_id_global"]).issubset(
                set(dataset.cohort.table["stay_id_global"])
            )
        )

        feature_set = dataset.feature_set_definition.set_index("feature_name")
        self.assertIn("heart_rate_last", feature_set.index)
        self.assertTrue(bool(feature_set.loc["heart_rate_last", "selected_for_model"]))
        self.assertIn("heart_rate_median", feature_set.index)
        self.assertTrue(bool(feature_set.loc["heart_rate_median", "selected_for_model"]))

        self.assertGreaterEqual(
            int(dataset.labels.labels.shape[0]),
            int(dataset.labels.usable_labels.shape[0]),
        )
        self.assertTrue(
            dataset.labels.notes["note"]
            .str.contains("within-horizon event labels", case=False, na=False)
            .any()
        )

        self.assertGreater(int(dataset.model_ready.table.shape[0]), 0)
        self.assertIn("label_value", dataset.model_ready.table.columns)
        self.assertIn("heart_rate_last", dataset.model_ready.table.columns)
        self.assertIn("heart_rate_median", dataset.model_ready.table.columns)
        self.assertFalse(dataset.model_ready.feature_availability_by_horizon.empty)

    def test_build_and_write_chapter1_seed_dataset_reads_standardized_artifacts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            upstream_dir = Path(tmpdir) / "asic_harmonized"
            chapter1_dir = Path(tmpdir) / "chapter1_seed"

            write_asic_harmonized_dataset(
                self.harmonized_dataset,
                output_dir=upstream_dir,
                output_format="csv",
            )
            write_asic_standardized_dataset(
                self.standardized_dataset,
                output_dir=upstream_dir,
                output_format="csv",
            )

            dataset, output_paths = build_and_write_chapter1_seed_dataset(
                input_dir=upstream_dir,
                output_dir=chapter1_dir,
                input_format="csv",
                output_format="csv",
            )

            expected_keys = {
                "cohort_chapter1_retained_stay_table",
                "instances_chapter1_valid_instances",
                "labels_chapter1_terminal_icu_mortality_labels",
                "model_ready_chapter1_model_ready_dataset",
            }
            self.assertTrue(expected_keys.issubset(output_paths))
            for key in expected_keys:
                self.assertTrue(output_paths[key].exists(), key)

            model_ready_df = pd.read_csv(output_paths["model_ready_chapter1_model_ready_dataset"])
            self.assertEqual(int(model_ready_df.shape[0]), int(dataset.model_ready.table.shape[0]))
