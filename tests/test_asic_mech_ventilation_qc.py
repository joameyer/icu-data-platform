from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.qc.mech_ventilation import (  # noqa: E402
    build_asic_mech_vent_ge_24h_qc,
)


class TestASICMechanicalVentilationQC(unittest.TestCase):
    def test_build_asic_mech_vent_ge_24h_qc_derives_supported_episodes(self) -> None:
        dynamic_df = pd.DataFrame(
            [
                {
                    "stay_id_global": "asic_A_1",
                    "hospital_id": "asic_A",
                    "time": "0 days 00:00:00",
                    "fio2": 0.40,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_1",
                    "hospital_id": "asic_A",
                    "time": "0 days 08:00:00",
                    "fio2": pd.NA,
                    "peep": 8.0,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_1",
                    "hospital_id": "asic_A",
                    "time": "0 days 16:00:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": 450.0,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_1",
                    "hospital_id": "asic_A",
                    "time": "1 days 00:00:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": 6.5,
                },
                {
                    "stay_id_global": "asic_A_1",
                    "hospital_id": "asic_A",
                    "time": "1 days 12:00:00",
                    "fio2": 0.50,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_2",
                    "hospital_id": "asic_A",
                    "time": "0 days 00:00:00",
                    "fio2": 0.35,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_2",
                    "hospital_id": "asic_A",
                    "time": "0 days 07:00:00",
                    "fio2": pd.NA,
                    "peep": 7.0,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_2",
                    "hospital_id": "asic_A",
                    "time": "0 days 16:30:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": 420.0,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_A_2",
                    "hospital_id": "asic_A",
                    "time": "0 days 23:00:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": 6.8,
                },
                {
                    "stay_id_global": "asic_B_3",
                    "hospital_id": "asic_B",
                    "time": "0 days 02:00:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
                {
                    "stay_id_global": "asic_B_3",
                    "hospital_id": "asic_B",
                    "time": "0 days 10:00:00",
                    "fio2": pd.NA,
                    "peep": pd.NA,
                    "vt": pd.NA,
                    "vt_per_kg_ibw": pd.NA,
                },
            ]
        )

        result = build_asic_mech_vent_ge_24h_qc(dynamic_df)

        stay_level = result.stay_level.set_index("stay_id_global")
        self.assertEqual(
            int(stay_level.loc["asic_A_1", "number_of_ventilation_supported_timestamps"]),
            5,
        )
        self.assertEqual(
            int(stay_level.loc["asic_A_1", "number_of_derived_ventilation_episodes"]),
            2,
        )
        self.assertAlmostEqual(
            float(stay_level.loc["asic_A_1", "maximum_derived_episode_duration_hours"]),
            24.0,
        )
        self.assertTrue(bool(stay_level.loc["asic_A_1", "mech_vent_ge_24h_qc"]))

        self.assertEqual(
            int(stay_level.loc["asic_A_2", "number_of_ventilation_supported_timestamps"]),
            4,
        )
        self.assertEqual(
            int(stay_level.loc["asic_A_2", "number_of_derived_ventilation_episodes"]),
            2,
        )
        self.assertAlmostEqual(
            float(stay_level.loc["asic_A_2", "maximum_derived_episode_duration_hours"]),
            7.0,
        )
        self.assertFalse(bool(stay_level.loc["asic_A_2", "mech_vent_ge_24h_qc"]))

        self.assertEqual(
            int(stay_level.loc["asic_B_3", "number_of_ventilation_supported_timestamps"]),
            0,
        )
        self.assertEqual(
            int(stay_level.loc["asic_B_3", "number_of_derived_ventilation_episodes"]),
            0,
        )
        self.assertAlmostEqual(
            float(stay_level.loc["asic_B_3", "maximum_derived_episode_duration_hours"]),
            0.0,
        )
        self.assertFalse(bool(stay_level.loc["asic_B_3", "mech_vent_ge_24h_qc"]))

        episode_level = result.episode_level
        self.assertEqual(int(episode_level.shape[0]), 4)
        first_episode = episode_level[
            (episode_level["stay_id_global"] == "asic_A_1")
            & (episode_level["episode_index"] == 1)
        ].iloc[0]
        self.assertEqual(first_episode["episode_start_time"], "0 days 00:00:00")
        self.assertEqual(first_episode["episode_end_time"], "1 days 00:00:00")
        self.assertAlmostEqual(float(first_episode["episode_duration_hours"]), 24.0)
        self.assertEqual(
            int(first_episode["ventilation_supported_timestamp_count_in_episode"]),
            4,
        )

        failed_stays = set(result.failed_stays["stay_id_global"])
        self.assertEqual(failed_stays, {"asic_A_2", "asic_B_3"})
        self.assertTrue(result.failed_stays["mech_vent_ge_24h_qc"].eq(False).all())

        hospital_summary = result.hospital_summary.set_index("hospital_id")
        self.assertEqual(int(hospital_summary.loc["asic_A", "stays_checked"]), 2)
        self.assertEqual(
            int(hospital_summary.loc["asic_A", "stays_satisfying_mech_vent_ge_24h_qc"]),
            1,
        )
        self.assertAlmostEqual(
            float(hospital_summary.loc["asic_A", "proportion_satisfying_mech_vent_ge_24h_qc"]),
            0.5,
        )
        self.assertAlmostEqual(
            float(hospital_summary.loc["asic_B", "max_max_derived_episode_duration_hours"]),
            0.0,
        )

        documentation = result.documentation
        self.assertTrue(documentation["note"].str.contains("8 hours", na=False).any())
        self.assertTrue(documentation["note"].str.contains("Chapter 1", na=False).any())
        self.assertTrue(documentation["note"].str.contains("not a claim", na=False).any())

    def test_build_asic_mech_vent_ge_24h_qc_requires_columns(self) -> None:
        dynamic_df = pd.DataFrame(
            {
                "stay_id_global": ["asic_A_1"],
                "hospital_id": ["asic_A"],
                "time": ["0 days 00:00:00"],
                "fio2": [0.40],
                "peep": [pd.NA],
                "vt": [450.0],
            }
        )

        with self.assertRaisesRegex(
            KeyError,
            "missing required mechanical-ventilation QC columns",
        ):
            build_asic_mech_vent_ge_24h_qc(dynamic_df)

    def test_build_asic_mech_vent_ge_24h_qc_fails_for_invalid_supported_times(self) -> None:
        dynamic_df = pd.DataFrame(
            {
                "stay_id_global": ["asic_A_1"],
                "hospital_id": ["asic_A"],
                "time": ["not_a_timedelta"],
                "fio2": [0.40],
                "peep": [pd.NA],
                "vt": [pd.NA],
                "vt_per_kg_ibw": [pd.NA],
            }
        )

        with self.assertRaisesRegex(
            ValueError,
            "could not parse time values for ventilation-supported rows",
        ):
            build_asic_mech_vent_ge_24h_qc(dynamic_df)


if __name__ == "__main__":
    unittest.main()
