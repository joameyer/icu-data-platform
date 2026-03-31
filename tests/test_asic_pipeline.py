from __future__ import annotations

import os
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
    DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    build_asic_chapter1_dataset,
    build_asic_harmonized_dataset,
    build_and_write_asic_chapter1_dataset,
    build_and_write_asic_harmonized_dataset,
    build_and_write_asic_standardized_dataset,
    build_and_write_asic_standardized_dataset_from_harmonized_outputs,
    write_asic_chapter1_dataset,
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
        cls.harmonized_dataset = build_asic_harmonized_dataset(DEFAULT_ASIC_RAW_DATA_DIR)
        cls.chapter1_dataset = build_asic_chapter1_dataset(
            cls.harmonized_dataset,
            raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
        )

    def test_build_asic_harmonized_dataset(self) -> None:
        dataset = self.harmonized_dataset

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
        self.assertFalse(dataset.dynamic.semantic_decisions.empty)
        self.assertFalse(dataset.dynamic.invalid_value_rules.empty)
        self.assertFalse(dataset.dynamic.invalid_value_qc.empty)
        self.assertFalse(dataset.mech_vent_ge_24h_qc.stay_level.empty)
        self.assertFalse(dataset.mech_vent_ge_24h_qc.hospital_summary.empty)
        self.assertFalse(dataset.mech_vent_ge_24h_qc.documentation.empty)
        self.assertFalse(hasattr(dataset, "cohort"))
        self.assertFalse(hasattr(dataset, "chapter1_8h_blocks"))

    def test_build_asic_harmonized_dataset_builds_mech_vent_qc_artifacts(self) -> None:
        dataset = self.harmonized_dataset
        qc = dataset.mech_vent_ge_24h_qc

        stay_level = qc.stay_level
        self.assertEqual(
            int(stay_level.shape[0]),
            int(dataset.dynamic.combined["stay_id_global"].nunique(dropna=True)),
        )
        self.assertTrue(
            {
                "stay_id_global",
                "hospital_id",
                "number_of_ventilation_supported_timestamps",
                "number_of_derived_ventilation_episodes",
                "maximum_derived_episode_duration_hours",
                "mech_vent_ge_24h_qc",
            }.issubset(stay_level.columns)
        )
        self.assertTrue(
            stay_level["maximum_derived_episode_duration_hours"].ge(0).all()
        )

        episode_level = qc.episode_level
        self.assertTrue(
            {
                "stay_id_global",
                "hospital_id",
                "episode_index",
                "episode_start_time",
                "episode_end_time",
                "episode_duration_hours",
            }.issubset(episode_level.columns)
        )
        self.assertTrue(episode_level["episode_duration_hours"].ge(0).all())

        failed_stays = qc.failed_stays
        expected_failed_stays = stay_level.loc[~stay_level["mech_vent_ge_24h_qc"]]
        self.assertEqual(
            set(failed_stays["stay_id_global"]),
            set(expected_failed_stays["stay_id_global"]),
        )
        self.assertTrue(failed_stays["mech_vent_ge_24h_qc"].eq(False).all())

        hospital_summary = qc.hospital_summary
        self.assertEqual(
            set(hospital_summary["hospital_id"]),
            set(stay_level["hospital_id"].dropna().unique()),
        )
        self.assertTrue(
            hospital_summary["proportion_satisfying_mech_vent_ge_24h_qc"].between(0, 1).all()
        )

        documentation = qc.documentation
        self.assertTrue(
            documentation["note"].str.contains("8 hours", case=False, na=False).any()
        )
        self.assertTrue(
            documentation["note"].str.contains(
                "cohort-contract verification for Chapter 1",
                na=False,
            ).any()
        )
        self.assertTrue(
            documentation["note"].str.contains("not a claim of perfect ground-truth", na=False).any()
        )

    def test_build_asic_chapter1_dataset_builds_authoritative_stay_level_cohort(self) -> None:
        harmonized_dataset = self.harmonized_dataset
        chapter1_dataset = self.chapter1_dataset
        cohort = chapter1_dataset.cohort.table
        cohort_summary = dict(
            chapter1_dataset.cohort.summary[["metric", "value"]].itertuples(index=False)
        )

        self.assertEqual(
            int(cohort.shape[0]),
            int(harmonized_dataset.static.combined["stay_id_global"].nunique(dropna=True)),
        )
        self.assertFalse(cohort["stay_id_global"].duplicated().any())
        self.assertTrue((cohort["icu_admission_time"] == 0).all())
        self.assertIn("readmission", cohort.columns)
        self.assertNotIn("icu_readmit", cohort.columns)
        self.assertIn("icu_end_time_proxy", cohort.columns)
        self.assertEqual(
            cohort_summary["stays_missing_icu_end_time_proxy"],
            int(cohort["icu_end_time_proxy"].isna().sum()),
        )
        self.assertEqual(
            cohort_summary["cohort_rows_total"],
            int(cohort.shape[0]),
        )
        self.assertEqual(
            cohort_summary["cohort_unique_stay_id_global_total"],
            int(cohort["stay_id_global"].nunique(dropna=True)),
        )

        dynamic_end_time = (
            harmonized_dataset.dynamic.combined.assign(
                parsed_time=pd.to_timedelta(
                    harmonized_dataset.dynamic.combined["time"],
                    errors="coerce",
                )
            )
            .dropna(subset=["parsed_time"])
            .groupby("stay_id_global")["parsed_time"]
            .max()
            .astype("string")
        )
        cohort_with_dynamic = cohort.dropna(subset=["icu_end_time_proxy"]).set_index("stay_id_global")
        self.assertTrue(
            cohort_with_dynamic["icu_end_time_proxy"].equals(
                dynamic_end_time.loc[cohort_with_dynamic.index]
            )
        )

        notes = chapter1_dataset.cohort.preprocessing_notes
        self.assertTrue(
            notes["note"].str.contains("proxy-based", case=False, na=False).any()
        )
        self.assertTrue(
            notes["note"].str.contains("inherited from the provided ASIC source cohort", na=False)
            .any()
        )
        self.assertTrue(
            notes["note"].str.contains("AMA and hospice flags are not derived", na=False).any()
        )

        proxy_summary = chapter1_dataset.cohort.icu_end_time_proxy_summary_by_hospital
        self.assertEqual(
            set(proxy_summary["hospital_id"]),
            set(cohort["hospital_id"].dropna().unique()),
        )

        coding_distribution = chapter1_dataset.cohort.coding_distribution_by_hospital
        self.assertEqual(
            set(coding_distribution["variable"]),
            {"readmission", "icu_mortality"},
        )
        hospital_totals = cohort.groupby("hospital_id").size()
        distribution_totals = (
            coding_distribution.groupby(["hospital_id", "variable"])["count"].sum()
        )
        for hospital_id, total in hospital_totals.items():
            self.assertEqual(int(distribution_totals[(hospital_id, "readmission")]), int(total))
            self.assertEqual(int(distribution_totals[(hospital_id, "icu_mortality")]), int(total))

    def test_build_asic_chapter1_dataset_builds_chapter1_site_restricted_cohort(self) -> None:
        chapter1 = self.chapter1_dataset.cohort.chapter1

        retained_hospitals = set(chapter1.retained_hospitals["hospital_id"])
        self.assertEqual(
            retained_hospitals,
            {"asic_UK02", "asic_UK04", "asic_UK07", "asic_UK08"},
        )

        coverage = chapter1.core_vital_group_coverage
        uk01_groups = coverage[coverage["hospital_id"] == "asic_UK01"].set_index("physiologic_group")
        uk06_groups = coverage[coverage["hospital_id"] == "asic_UK06"].set_index("physiologic_group")
        uk03_groups = coverage[coverage["hospital_id"] == "asic_UK03"].set_index("physiologic_group")
        self.assertEqual(uk01_groups.at["cardiac_rate", "satisfying_variables"], [])
        self.assertEqual(uk06_groups.at["blood_pressure", "satisfying_variables"], [])
        self.assertEqual(uk03_groups.at["blood_pressure", "satisfying_variables"], [])
        self.assertEqual(
            uk03_groups.at["oxygenation", "satisfying_variables"],
            ["spo2", "sao2"],
        )

        site_eligibility = chapter1.site_eligibility.set_index("hospital_id")
        self.assertFalse(bool(site_eligibility.loc["asic_UK00", "icu_mortality_available"]))
        self.assertTrue(bool(site_eligibility.loc["asic_UK06", "icu_mortality_available"]))
        self.assertEqual(int(site_eligibility.loc["asic_UK03", "usable_core_vital_group_count"]), 3)
        self.assertTrue(bool(site_eligibility.loc["asic_UK03", "core_vitals_eligible"]))
        self.assertEqual(int(site_eligibility.loc["asic_UK01", "usable_core_vital_group_count"]), 0)
        self.assertFalse(bool(site_eligibility.loc["asic_UK01", "site_included_ch1"]))
        self.assertIn(
            "insufficient core-vitals coverage",
            site_eligibility.loc["asic_UK01", "exclusion_reasons"],
        )
        self.assertEqual(
            site_eligibility.loc["asic_UK01", "icu_mortality_verification_status"],
            "verified_raw_icu_mortality_source_present_but_all_missing",
        )
        self.assertEqual(int(site_eligibility.loc["asic_UK06", "usable_core_vital_group_count"]), 0)
        self.assertFalse(bool(site_eligibility.loc["asic_UK06", "core_vitals_eligible"]))
        self.assertFalse(bool(site_eligibility.loc["asic_UK06", "site_included_ch1"]))
        self.assertIn(
            "insufficient core-vitals coverage",
            site_eligibility.loc["asic_UK06", "exclusion_reasons"],
        )
        self.assertEqual(
            site_eligibility.loc["asic_UK06", "icu_mortality_verification_status"],
            "verified_non_missing_harmonized_icu_mortality",
        )
        self.assertFalse(bool(site_eligibility.loc["asic_UK03", "site_included_ch1"]))
        self.assertIn(
            "missing/unusable icu_mortality",
            site_eligibility.loc["asic_UK03", "exclusion_reasons"],
        )
        self.assertEqual(
            site_eligibility.loc["asic_UK03", "icu_mortality_verification_status"],
            "verified_no_raw_icu_mortality_source_column",
        )
        self.assertEqual(site_eligibility.loc["asic_UK03", "blood_pressure_satisfied_by"], [])
        self.assertEqual(
            site_eligibility.loc["asic_UK00", "icu_mortality_verification_status"],
            "verified_raw_icu_mortality_source_present_but_all_missing",
        )

        site_counts_summary = dict(
            chapter1.site_counts_summary[["metric", "value"]].itertuples(index=False)
        )
        self.assertEqual(site_counts_summary["hospitals_before_site_level_exclusion"], 8)
        self.assertEqual(site_counts_summary["hospitals_after_site_level_exclusion"], 4)

        stay_exclusions = chapter1.stay_exclusions
        self.assertTrue(
            stay_exclusions.loc[stay_exclusions["exclude_site_ineligible"], "exclude_missing_readmission"]
            .eq(False)
            .all()
        )
        self.assertEqual(int(stay_exclusions["exclude_no_dynamic_data"].sum()), 0)
        self.assertEqual(int(stay_exclusions["exclude_missing_readmission"].sum()), 0)
        self.assertEqual(int(stay_exclusions["exclude_readmission_flagged"].sum()), 1)
        self.assertEqual(int(stay_exclusions["first_stay_proxy_eligible"].sum()), 39)
        self.assertEqual(int(chapter1.table.shape[0]), 39)
        self.assertEqual(int(chapter1.retained_stays.shape[0]), 39)
        self.assertTrue(chapter1.table["has_dynamic_data"].all())
        self.assertTrue(chapter1.table["first_stay_proxy_eligible"].all())
        self.assertTrue(chapter1.table["final_retained_ch1"].all())
        self.assertTrue(chapter1.table["final_ch1_status"].eq("retained").all())
        self.assertTrue(chapter1.table["exclusion_reason"].isna().all())
        self.assertTrue(chapter1.table["readmission"].eq(0).all())
        self.assertEqual(
            sorted(chapter1.table["hospital_id"].dropna().unique().tolist()),
            sorted(chapter1.retained_hospitals["hospital_id"].tolist()),
        )

        counts = chapter1.counts_by_hospital.set_index("hospital_id")
        self.assertEqual(int(counts.loc["asic_UK02", "after_site_level_exclusion"]), 10)
        self.assertEqual(int(counts.loc["asic_UK07", "after_missing_readmission_exclusion"]), 10)
        self.assertEqual(int(counts.loc["asic_UK07", "excluded_readmission_flagged_stays"]), 1)
        self.assertEqual(int(counts.loc["asic_UK07", "final_retained_stays"]), 9)
        self.assertEqual(int(counts.loc["asic_UK00", "after_site_level_exclusion"]), 0)

        stay_summary = chapter1.stay_exclusion_summary_by_hospital.set_index("hospital_id")
        self.assertEqual(int(stay_summary.loc["asic_UK02", "before_site_level_exclusion"]), 10)
        self.assertEqual(int(stay_summary.loc["asic_UK07", "after_readmission_flagged_exclusion"]), 9)
        self.assertEqual(int(stay_summary.loc["asic_UK07", "excluded_readmission_flagged_stays"]), 1)
        self.assertEqual(int(stay_summary.loc["asic_UK01", "after_site_level_exclusion"]), 0)

        notes = chapter1.notes
        self.assertTrue(
            notes["note"].str.contains("site-restricted ASIC Chapter 1 cohort", na=False).any()
        )
        self.assertTrue(
            notes["note"].str.contains(
                "blood_pressure=map or sbp or dbp",
                na=False,
            ).any()
        )

    def test_build_asic_chapter1_dataset_builds_chapter1_8h_blocks(self) -> None:
        chapter1_dataset = self.chapter1_dataset
        blocks = chapter1_dataset.chapter1_8h_blocks
        retained_cohort = chapter1_dataset.cohort.chapter1.table

        stay_counts = blocks.stay_block_counts
        self.assertEqual(int(stay_counts.shape[0]), int(retained_cohort.shape[0]))
        self.assertEqual(
            set(stay_counts["stay_id_global"]),
            set(retained_cohort["stay_id_global"]),
        )

        proxy_hours = pd.to_timedelta(
            retained_cohort["icu_end_time_proxy"],
            errors="coerce",
        ).dt.total_seconds() / 3600.0
        expected_block_rows = int((proxy_hours // 8).fillna(0).clip(lower=0).sum())
        self.assertEqual(int(blocks.block_index.shape[0]), expected_block_rows)
        self.assertEqual(
            int(blocks.blocked_dynamic_features.shape[0]),
            int(blocks.block_index.shape[0]),
        )

        metrics = dict(blocks.qc_summary[["metric", "value"]].itertuples(index=False))
        self.assertEqual(
            int(metrics["retained_chapter1_stays_total"]),
            int(retained_cohort.shape[0]),
        )
        self.assertEqual(int(metrics["block_rows_total"]), int(blocks.block_index.shape[0]))
        self.assertEqual(
            int(metrics["retained_stays_with_zero_blocks"])
            + int(metrics["retained_stays_with_one_or_more_blocks"]),
            int(retained_cohort.shape[0]),
        )
        self.assertEqual(
            int(metrics["all_constructed_blocks_end_on_or_before_icu_end_time_proxy"]),
            1,
        )
        self.assertEqual(
            int(metrics["exact_boundary_stays_have_terminal_block_end_equal_proxy"]),
            1,
        )
        self.assertEqual(
            int(metrics["negative_dynamic_time_rows_in_retained_input"]),
            int(blocks.negative_dynamic_time_qc["negative_dynamic_time_row_count"].sum()),
        )

        terminal_block_end = (
            blocks.block_index.groupby("stay_id_global")["block_end_h"]
            .max()
            .rename("max_block_end_h")
        )
        validation = stay_counts.set_index("stay_id_global").join(terminal_block_end, how="left")
        validation["max_block_end_h"] = validation["max_block_end_h"].fillna(0)
        self.assertTrue(
            validation.loc[validation["completed_block_count"].ge(1), "max_block_end_h"]
            .le(validation.loc[validation["completed_block_count"].ge(1), "icu_end_time_proxy_hours"])
            .all()
        )
        exact_boundary = validation[validation["ends_exactly_on_8h_boundary"]]
        self.assertTrue(
            exact_boundary["terminal_block_end_h"]
            .eq(exact_boundary["icu_end_time_proxy_hours"])
            .all()
        )

        distribution = blocks.block_count_distribution_by_hospital
        self.assertEqual(
            set(distribution["hospital_id"]),
            set(retained_cohort["hospital_id"].dropna().unique()),
        )

        blocked_features = blocks.blocked_dynamic_features
        self.assertEqual(
            list(blocked_features[blocks.block_index.columns].itertuples(index=False, name=None)),
            list(blocks.block_index.itertuples(index=False, name=None)),
        )
        self.assertTrue(blocked_features["dynamic_row_count"].ge(0).all())
        self.assertTrue(blocked_features["observed_variables_in_block"].ge(0).all())
        self.assertIn("heart_rate_obs_count", blocked_features.columns)
        self.assertIn("heart_rate_median", blocked_features.columns)
        self.assertIn("heart_rate_last", blocked_features.columns)
        self.assertTrue(blocked_features["heart_rate_obs_count"].ge(0).all())
        self.assertTrue(blocked_features["heart_rate_obs_count"].gt(0).any())

    def test_build_asic_harmonized_dataset_applies_semantic_site_rules(self) -> None:
        dataset = self.harmonized_dataset

        uk04_etco2 = pd.to_numeric(
            dataset.dynamic.tables_by_hospital["asic_UK04"]["etco2"],
            errors="coerce",
        ).dropna()
        self.assertGreater(int(uk04_etco2.shape[0]), 0)
        self.assertGreater(float(uk04_etco2.median()), 39.0)
        self.assertLess(float(uk04_etco2.median()), 45.0)

        self.assertTrue(
            dataset.dynamic.tables_by_hospital["asic_UK06"]["vt_per_kg_ibw"].isna().all()
        )
        self.assertTrue(
            dataset.dynamic.tables_by_hospital["asic_UK03"]["norepinephrine_iv_cont"].isna().all()
        )
        self.assertTrue(
            dataset.dynamic.tables_by_hospital["asic_UK08"]["clonidine_iv_cont"].isna().all()
        )

        post_issues = dataset.dynamic.distribution_issues
        self.assertTrue(
            post_issues[
                (post_issues["canonical_name"] == "etco2")
                & (post_issues["hospital"] == "asic_UK04")
            ].empty
        )
        self.assertTrue(
            post_issues[
                (post_issues["canonical_name"] == "vt_per_kg_ibw")
                & (post_issues["hospital"] == "asic_UK06")
            ].empty
        )
        self.assertTrue(
            post_issues[
                (post_issues["canonical_name"] == "norepinephrine_iv_cont")
                & (post_issues["hospital"] == "asic_UK03")
            ].empty
        )

    def test_build_asic_harmonized_dataset_records_semantic_decisions(self) -> None:
        dataset = self.harmonized_dataset
        decisions = dataset.dynamic.semantic_decisions

        uk04_etco2 = decisions[
            (decisions["hospital"] == "asic_UK04")
            & (decisions["canonical_name"] == "etco2")
        ].iloc[0]
        self.assertEqual(
            uk04_etco2["decision_classification"],
            "deterministic unit conversion",
        )
        self.assertEqual(uk04_etco2["applied_action"], "convert_pa_to_mmhg")
        self.assertEqual(uk04_etco2["chapter1_recommendation"], "retain")

        uk03_norepi = decisions[
            (decisions["hospital"] == "asic_UK03")
            & (decisions["canonical_name"] == "norepinephrine_iv_cont")
        ].iloc[0]
        self.assertEqual(
            uk03_norepi["decision_classification"],
            "site-specific invalid variable to set missing or exclude",
        )
        self.assertTrue(bool(uk03_norepi["metadata_review_required"]))
        self.assertEqual(uk03_norepi["chapter1_recommendation"], "site_drop")

        uk08_clonidine = decisions[
            (decisions["hospital"] == "asic_UK08")
            & (decisions["canonical_name"] == "clonidine_iv_cont")
        ].iloc[0]
        self.assertEqual(
            uk08_clonidine["decision_classification"],
            "site-specific invalid variable to set missing or exclude",
        )
        self.assertTrue(bool(uk08_clonidine["metadata_review_required"]))
        self.assertEqual(uk08_clonidine["chapter1_recommendation"], "site_drop")

        uk02_ie_ratio = decisions[
            (decisions["hospital"] == "asic_UK02")
            & (decisions["canonical_name"] == "ie_ratio")
        ].iloc[0]
        self.assertEqual(
            uk02_ie_ratio["decision_classification"],
            "unresolved mismatch requiring metadata confirmation",
        )
        self.assertTrue(bool(uk02_ie_ratio["metadata_review_required"]))
        self.assertEqual(uk02_ie_ratio["chapter1_recommendation"], "exclude")
        self.assertGreater(float(uk02_ie_ratio["candidate_reciprocal_median_before"]), 1.0)

    def test_build_asic_harmonized_dataset_applies_invalid_value_cleaning(self) -> None:
        dataset = self.harmonized_dataset
        dynamic_df = dataset.dynamic.combined

        sbp = pd.to_numeric(dynamic_df["sbp"], errors="coerce")
        self.assertFalse(((sbp <= 0) | (sbp > 300)).fillna(False).any())

        map_values = pd.to_numeric(dynamic_df["map"], errors="coerce")
        self.assertFalse(((map_values <= 0) | (map_values > 250)).fillna(False).any())

        pf_ratio = pd.to_numeric(dynamic_df["pf_ratio"], errors="coerce")
        self.assertFalse(((pf_ratio <= 0) | (pf_ratio > 2500)).fillna(False).any())

        vt_per_kg_ibw = pd.to_numeric(dynamic_df["vt_per_kg_ibw"], errors="coerce")
        self.assertFalse(((vt_per_kg_ibw <= 0) | (vt_per_kg_ibw > 30)).fillna(False).any())

        core_temp = pd.to_numeric(dynamic_df["core_temp"], errors="coerce")
        self.assertFalse(((core_temp < 25) | (core_temp > 45)).fillna(False).any())

    def test_build_asic_harmonized_dataset_records_invalid_value_qc(self) -> None:
        dataset = self.harmonized_dataset
        rules = dataset.dynamic.invalid_value_rules
        qc = dataset.dynamic.invalid_value_qc

        sbp_rule = rules[rules["canonical_name"] == "sbp"].iloc[0]
        self.assertEqual(float(sbp_rule["hard_min"]), 0.0)
        self.assertEqual(float(sbp_rule["hard_max"]), 300.0)
        self.assertTrue(bool(sbp_rule["invalid_zero"]))

        sbp_uk08 = qc[
            (qc["hospital"] == "asic_UK08")
            & (qc["canonical_name"] == "sbp")
        ].iloc[0]
        self.assertGreater(int(sbp_uk08["invalid_count"]), 0)
        self.assertGreater(float(sbp_uk08["invalid_proportion"]), 0.0)
        self.assertEqual(sbp_uk08["dominant_invalid_hospital"], "asic_UK08")
        self.assertTrue(bool(sbp_uk08["invalidity_concentrated_in_specific_hospitals"]))

        vt_qc = qc[
            (qc["hospital"] == "asic_UK07")
            & (qc["canonical_name"] == "vt_per_kg_ibw")
        ].iloc[0]
        self.assertGreater(int(vt_qc["invalid_count"]), 0)
        self.assertTrue(
            any(abs(float(value) - 38.71) < 1e-9 for value in vt_qc["invalid_examples"])
        )

    def test_write_asic_harmonized_dataset_outputs_expected_files(self) -> None:
        dataset = self.harmonized_dataset
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
                "dynamic_semantic_decisions",
                "dynamic_invalid_value_rules",
                "dynamic_invalid_value_qc",
                "dynamic_distribution_summary",
                "dynamic_distribution_issues",
                "qc_stay_id_summary",
                "qc_stay_id_unique_stays_per_hospital",
                "qc_stay_id_local_id_collisions",
                "qc_stay_id_missing_id_values",
                "qc_stay_id_duplicate_static_global_ids",
                "qc_stay_id_mapping_failures",
                "qc_stay_id_duplicate_dynamic_time_index",
                "qc_mech_vent_ge_24h_stay_level",
                "qc_mech_vent_ge_24h_episode_level",
                "qc_mech_vent_ge_24h_hospital_summary",
                "qc_mech_vent_ge_24h_failed_stays",
                "qc_mech_vent_ge_24h_documentation",
            }
            self.assertEqual(set(output_paths), expected_keys)
            self.assertTrue(all(path.exists() for path in output_paths.values()))

            static_df = pd.read_csv(output_paths["static_harmonized"])
            dynamic_df = pd.read_csv(output_paths["dynamic_harmonized"])
            mech_vent_stay_qc = pd.read_csv(output_paths["qc_mech_vent_ge_24h_stay_level"])
            self.assertGreater(int(static_df.shape[0]), 0)
            self.assertGreater(int(dynamic_df.shape[0]), 0)
            self.assertGreater(int(mech_vent_stay_qc.shape[0]), 0)

    def test_build_standardized_from_harmonized_outputs_matches_direct_builder(self) -> None:
        with TemporaryDirectory() as tmpdir:
            harmonized_dir = Path(tmpdir) / "harmonized"
            direct_dir = Path(tmpdir) / "direct_standardized"
            streamed_dir = Path(tmpdir) / "streamed_standardized"

            harmonized_dataset, _ = build_and_write_asic_harmonized_dataset(
                raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
                output_dir=harmonized_dir,
                output_format="csv",
            )
            _, direct_paths = build_and_write_asic_standardized_dataset(
                harmonized_dataset,
                output_dir=direct_dir,
                output_format="csv",
            )
            streamed_paths = build_and_write_asic_standardized_dataset_from_harmonized_outputs(
                input_dir=harmonized_dir,
                output_dir=streamed_dir,
                input_format="csv",
                output_format="csv",
                dynamic_chunksize=10_000,
            )

            self.assertEqual(set(streamed_paths), set(direct_paths))

            def assert_equal_csv(
                key: str,
                sort_columns: list[str],
            ) -> None:
                direct_df = pd.read_csv(direct_paths[key]).sort_values(sort_columns).reset_index(
                    drop=True
                )
                streamed_df = (
                    pd.read_csv(streamed_paths[key]).sort_values(sort_columns).reset_index(drop=True)
                )
                pd.testing.assert_frame_equal(direct_df, streamed_df, check_dtype=False)

            assert_equal_csv("cohort_stay_level", ["hospital_id", "stay_id_global"])
            assert_equal_csv("cohort_summary", ["metric"])
            assert_equal_csv("cohort_preprocessing_notes", ["note_id"])
            assert_equal_csv(
                "cohort_icu_end_time_proxy_summary_by_hospital",
                ["hospital_id"],
            )
            assert_equal_csv(
                "cohort_coding_distribution_by_hospital",
                ["hospital_id", "variable", "code"],
            )
            assert_equal_csv(
                "blocked_asic_8h_block_index",
                ["hospital_id", "stay_id_global", "block_index"],
            )
            assert_equal_csv(
                "blocked_asic_8h_blocked_dynamic_features",
                ["hospital_id", "stay_id_global", "block_index"],
            )
            assert_equal_csv(
                "blocked_asic_8h_stay_block_counts",
                ["hospital_id", "stay_id_global"],
            )
            assert_equal_csv(
                "blocked_asic_8h_block_count_distribution_by_hospital",
                ["hospital_id", "completed_block_count"],
            )
            assert_equal_csv(
                "blocked_asic_8h_negative_dynamic_time_qc",
                ["hospital_id", "stay_id_global"],
            )
            assert_equal_csv("blocked_asic_8h_qc_summary", ["metric"])
            assert_equal_csv("blocked_asic_8h_example_stays", ["example_type"])

    def test_write_asic_chapter1_dataset_outputs_expected_files(self) -> None:
        dataset = self.chapter1_dataset
        with TemporaryDirectory() as tmpdir:
            output_paths = write_asic_chapter1_dataset(
                dataset,
                Path(tmpdir),
                output_format="csv",
            )

            expected_keys = {
                "cohort_stay_level",
                "cohort_summary",
                "cohort_preprocessing_notes",
                "cohort_icu_end_time_proxy_summary_by_hospital",
                "cohort_coding_distribution_by_hospital",
                "cohort_chapter1_stay_level",
                "cohort_chapter1_notes",
                "cohort_chapter1_core_vital_group_coverage",
                "cohort_chapter1_site_eligibility",
                "cohort_chapter1_site_counts_summary",
                "cohort_chapter1_stay_exclusions",
                "cohort_chapter1_stay_exclusion_summary_by_hospital",
                "cohort_chapter1_counts_by_hospital",
                "cohort_chapter1_retained_hospitals",
                "cohort_chapter1_retained_stays",
                "blocks_chapter1_8h_block_index",
                "blocks_chapter1_8h_blocked_dynamic_features",
                "blocks_chapter1_8h_stay_block_counts",
                "blocks_chapter1_8h_block_count_distribution_by_hospital",
                "blocks_chapter1_8h_negative_dynamic_time_qc",
                "blocks_chapter1_8h_qc_summary",
                "blocks_chapter1_8h_example_stays",
            }
            self.assertEqual(set(output_paths), expected_keys)
            self.assertTrue(all(path.exists() for path in output_paths.values()))

            cohort_df = pd.read_csv(output_paths["cohort_stay_level"])
            chapter1_df = pd.read_csv(output_paths["cohort_chapter1_stay_level"])
            block_index_df = pd.read_csv(output_paths["blocks_chapter1_8h_block_index"])
            blocked_dynamic_df = pd.read_csv(
                output_paths["blocks_chapter1_8h_blocked_dynamic_features"]
            )
            self.assertGreater(int(cohort_df.shape[0]), 0)
            self.assertGreater(int(chapter1_df.shape[0]), 0)
            self.assertGreater(int(block_index_df.shape[0]), 0)
            self.assertEqual(int(blocked_dynamic_df.shape[0]), int(block_index_df.shape[0]))

    def test_build_and_write_asic_harmonized_dataset_uses_default_artifacts_dir(self) -> None:
        with TemporaryDirectory() as tmpdir:
            previous_cwd = Path.cwd()
            os.chdir(tmpdir)
            try:
                dataset, output_paths = build_and_write_asic_harmonized_dataset(
                    raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
                )
            finally:
                os.chdir(previous_cwd)

            expected_root = Path(tmpdir) / DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR
            self.assertGreater(int(dataset.static.combined.shape[0]), 0)
            self.assertEqual(
                Path(tmpdir) / output_paths["static_harmonized"],
                expected_root / "static" / "harmonized.csv",
            )
            self.assertTrue(all((Path(tmpdir) / path).exists() for path in output_paths.values()))

    def test_build_and_write_asic_chapter1_dataset_uses_default_artifacts_dir(self) -> None:
        with TemporaryDirectory() as tmpdir:
            previous_cwd = Path.cwd()
            os.chdir(tmpdir)
            try:
                harmonized_dataset = build_asic_harmonized_dataset(DEFAULT_ASIC_RAW_DATA_DIR)
                dataset, output_paths = build_and_write_asic_chapter1_dataset(
                    harmonized_dataset,
                    raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
                )
            finally:
                os.chdir(previous_cwd)

            expected_root = Path(tmpdir) / DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR
            self.assertGreater(int(dataset.cohort.chapter1.table.shape[0]), 0)
            self.assertEqual(
                Path(tmpdir) / output_paths["blocks_chapter1_8h_block_index"],
                expected_root / "blocks" / "chapter1_8h_block_index.csv",
            )
            self.assertTrue(all((Path(tmpdir) / path).exists() for path in output_paths.values()))

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
