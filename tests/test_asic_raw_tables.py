from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from icu_data_platform.sources.asic.extract.raw_tables import (  # noqa: E402
    DYNAMIC_FILENAME_PREFIX,
    extract_local_stay_id,
    get_dynamic_dir,
    load_dynamic_for_hospital,
    load_dynamic_tables,
    load_static_for_hospital,
    load_static_tables,
)


class TestASICRawTables(unittest.TestCase):
    def test_get_dynamic_dir_prefers_dynamic_filtered(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_00"
            (hospital_dir / "dynamic").mkdir(parents=True)
            (hospital_dir / "dynamic_filtered").mkdir()

            self.assertEqual(
                get_dynamic_dir(hospital_dir),
                hospital_dir / "dynamic_filtered",
            )

    def test_extract_local_stay_id_handles_both_dynamic_patterns(self) -> None:
        filtered_path = Path("/tmp/UK_00/dynamic_filtered/9990.csv")
        dynamic_path = Path(
            f"/tmp/UK_01/dynamic/{DYNAMIC_FILENAME_PREFIX}9991.csv"
        )

        self.assertEqual(extract_local_stay_id(filtered_path), "9990")
        self.assertEqual(extract_local_stay_id(dynamic_path), "9991")

    def test_load_dynamic_for_hospital_injects_stay_id_columns(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_01"
            dynamic_dir = hospital_dir / "dynamic"
            dynamic_dir.mkdir(parents=True)
            (dynamic_dir / f"{DYNAMIC_FILENAME_PREFIX}9990.csv").write_text(
                "Zeit_ab_Aufnahme;HF\n15;80\n30;82\n",
                encoding="utf-8",
            )
            (dynamic_dir / f"{DYNAMIC_FILENAME_PREFIX}9991.csv").write_text(
                "Zeit_ab_Aufnahme;HF\n15;70\n",
                encoding="utf-8",
            )

            df = load_dynamic_for_hospital(hospital_dir)

            self.assertEqual(
                df["Pseudo-ID"].astype("string").tolist(),
                ["9990", "9990", "9991"],
            )
            self.assertEqual(
                df["PseudoID"].astype("string").tolist(),
                ["9990", "9990", "9991"],
            )
            self.assertEqual(df["HF"].tolist(), [80, 82, 70])
            self.assertIn("Time", df.columns)
            self.assertEqual(str(df.loc[0, "Time"]), "0 days 00:15:00")

    def test_load_dynamic_for_hospital_overwrites_stay_id_columns_from_filename(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_04"
            dynamic_dir = hospital_dir / "dynamic"
            dynamic_dir.mkdir(parents=True)
            (dynamic_dir / f"{DYNAMIC_FILENAME_PREFIX}9990.csv").write_text(
                "Pseudo-ID;Zeit_ab_Aufnahme;HF\n1234;15;80\n1234;30;82\n",
                encoding="utf-8",
            )

            df = load_dynamic_for_hospital(hospital_dir)

            self.assertEqual(df["Pseudo-ID"].astype("string").tolist(), ["9990", "9990"])
            self.assertEqual(df["PseudoID"].astype("string").tolist(), ["9990", "9990"])

    def test_load_dynamic_for_hospital_parses_decimal_commas_and_storno(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_01"
            dynamic_dir = hospital_dir / "dynamic"
            dynamic_dir.mkdir(parents=True)
            (dynamic_dir / f"{DYNAMIC_FILENAME_PREFIX}9990.csv").write_text(
                "Zeit_ab_Aufnahme;PEEP;CRP\n15;4,75;storno\n30;5,5;storniert\n",
                encoding="utf-8",
            )

            df = load_dynamic_for_hospital(hospital_dir)

            self.assertEqual(df["PEEP"].tolist(), [4.75, 5.5])
            self.assertTrue(df["CRP"].isna().all())

    def test_load_dynamic_for_hospital_keeps_dot_decimals_for_other_dynamic_hospitals(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_02"
            dynamic_dir = hospital_dir / "dynamic"
            dynamic_dir.mkdir(parents=True)
            (dynamic_dir / f"{DYNAMIC_FILENAME_PREFIX}9990.csv").write_text(
                "Zeit_ab_Aufnahme;PEEP\n15;4.75\n30;5.5\n",
                encoding="utf-8",
            )

            df = load_dynamic_for_hospital(hospital_dir)

            self.assertEqual(df["PEEP"].tolist(), [4.75, 5.5])

    def test_load_static_for_hospital_fills_missing_stay_id_alias(self) -> None:
        with TemporaryDirectory() as tmpdir:
            hospital_dir = Path(tmpdir) / "UK_02"
            static_dir = hospital_dir / "static"
            static_dir.mkdir(parents=True)
            (static_dir / "andere_variablen_kds_patienten.csv").write_text(
                "Pseudo-ID;clusterGeschlecht\n9990;M\n9991;W\n",
                encoding="utf-8",
            )

            df = load_static_for_hospital(hospital_dir)

            self.assertIn("Pseudo-ID", df.columns)
            self.assertIn("PseudoID", df.columns)
            self.assertEqual(df["PseudoID"].astype("string").tolist(), ["9990", "9991"])

    def test_load_table_wrappers_use_compatible_hospital_keys(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            hospital_dir = root / "UK_00"
            dynamic_filtered_dir = hospital_dir / "dynamic_filtered"
            static_dir = hospital_dir / "static"
            dynamic_filtered_dir.mkdir(parents=True)
            static_dir.mkdir(parents=True)

            (dynamic_filtered_dir / "9990.csv").write_text(
                "Zeit_ab_Aufnahme,HF\n0,95\n",
                encoding="utf-8",
            )
            (static_dir / "andere_variablen_kds_patienten.csv").write_text(
                "Pseudo-ID;clusterGeschlecht\n9990;M\n",
                encoding="utf-8",
            )

            self.assertEqual(list(load_static_tables(root)), ["asic_UK00"])
            self.assertEqual(list(load_dynamic_tables(root)), ["asic_UK00"])


if __name__ == "__main__":
    unittest.main()
