from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mimic_iv_pipeline.contracts import load_profile
from mimic_iv_pipeline.production import (
    audit_production_candidate,
    build_production_candidate,
)


def _write_gzip_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_synthetic_production_candidate_and_independent_audit(
    tmp_path: Path, project_root: Path
) -> None:
    source = tmp_path / "mimic" / "data"
    candidate_root = tmp_path / "private" / "candidates"
    audit_root = tmp_path / "private" / "audits"
    profile_path = project_root / "mimic/config/profiles/phase_aware_mortality_0_1.yaml"
    profile = load_profile(profile_path)

    _write_gzip_csv(
        source / "hosp/patients.csv.gz",
        ["subject_id", "gender", "anchor_age", "anchor_year"],
        [
            {"subject_id": 1, "gender": "F", "anchor_age": 60, "anchor_year": 2200},
            {"subject_id": 2, "gender": "M", "anchor_age": 17, "anchor_year": 2200},
        ],
    )
    _write_gzip_csv(
        source / "hosp/admissions.csv.gz",
        ["subject_id", "hadm_id", "discharge_location", "hospital_expire_flag"],
        [
            {"subject_id": 1, "hadm_id": 10, "discharge_location": "HOME", "hospital_expire_flag": 0},
            {"subject_id": 2, "hadm_id": 20, "discharge_location": "HOME", "hospital_expire_flag": 0},
        ],
    )
    _write_gzip_csv(
        source / "hosp/services.csv.gz",
        ["subject_id", "hadm_id", "transfertime", "prev_service", "curr_service"],
        [
            {"subject_id": 1, "hadm_id": 10, "transfertime": "2200-01-01 00:00:00", "prev_service": "", "curr_service": "MED"},
            {"subject_id": 2, "hadm_id": 20, "transfertime": "2200-01-01 00:00:00", "prev_service": "", "curr_service": "MED"},
        ],
    )
    _write_gzip_csv(
        source / "icu/icustays.csv.gz",
        ["subject_id", "hadm_id", "stay_id", "intime", "outtime"],
        [
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "intime": "2200-01-01 01:00:00", "outtime": "2200-01-01 10:00:00"},
            {"subject_id": 2, "hadm_id": 20, "stay_id": 200, "intime": "2200-01-01 01:00:00", "outtime": "2200-01-01 02:00:00"},
        ],
    )
    _write_gzip_csv(
        source / "hosp/diagnoses_icd.csv.gz",
        ["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"],
        [
            {"subject_id": 1, "hadm_id": 10, "seq_num": 1, "icd_code": "K74.60", "icd_version": 10},
            {"subject_id": 1, "hadm_id": 10, "seq_num": 2, "icd_code": "042", "icd_version": 9},
        ],
    )
    lab_itemids = sorted(
        {
            itemid
            for raw in profile.registries["item_sources"]["labevents"].values()
            for itemid in raw["itemids"]
        }
    )
    _write_gzip_csv(
        source / "hosp/d_labitems.csv.gz",
        ["itemid", "label", "fluid", "category"],
        [
            {"itemid": itemid, "label": "Lipase" if itemid == 50956 else f"item_{itemid}", "fluid": "Blood", "category": "Chemistry"}
            for itemid in lab_itemids
        ],
    )
    _write_gzip_csv(
        source / "icu/chartevents.csv.gz",
        ["subject_id", "hadm_id", "stay_id", "charttime", "itemid", "value", "valuenum", "valueuom"],
        [
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "charttime": "2200-01-01 01:00:00", "itemid": 220045, "value": "70", "valuenum": 70, "valueuom": "bpm"},
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "charttime": "2200-01-01 09:00:00", "itemid": 220045, "value": "80", "valuenum": 80, "valueuom": "bpm"},
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "charttime": "2200-01-01 02:00:00", "itemid": 226730, "value": "170", "valuenum": 170, "valueuom": "cm"},
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "charttime": "2200-01-01 02:00:00", "itemid": 226512, "value": "72.25", "valuenum": 72.25, "valueuom": "kg"},
            {"subject_id": 1, "hadm_id": 10, "stay_id": 100, "charttime": "2200-01-01 03:00:00", "itemid": 223835, "value": "40", "valuenum": 40, "valueuom": "%"},
        ],
    )
    _write_gzip_csv(
        source / "hosp/labevents.csv.gz",
        ["subject_id", "hadm_id", "specimen_id", "itemid", "charttime", "value", "valuenum", "valueuom"],
        [
            {"subject_id": 1, "hadm_id": 10, "specimen_id": 1, "itemid": 50862, "charttime": "2200-01-01 02:00:00", "value": "4", "valuenum": 4, "valueuom": "g/dL"},
            {"subject_id": 1, "hadm_id": 10, "specimen_id": 2, "itemid": 50813, "charttime": "2200-01-01 03:00:00", "value": "2", "valuenum": 2, "valueuom": "mmol/L"},
            {"subject_id": 1, "hadm_id": 10, "specimen_id": 3, "itemid": 51006, "charttime": "2200-01-01 04:00:00", "value": "28.01", "valuenum": 28.01, "valueuom": "mg/dL"},
        ],
    )
    config = tmp_path / "production.yaml"
    config.write_text(
        f"""artifact: test\nartifact_version: '0.1'\nstatus: human_approved_for_operator_executed_nonrelease_candidate\nprofile: {profile_path}\npaths:\n  source_root: {source}\n  candidate_root: {candidate_root}\n  audit_root: {audit_root}\nexecution:\n  csv_block_size_bytes: 1024\n  sqlite_insert_batch_rows: 2\n  parquet_batch_rows: 3\n  parquet_compression: zstd\nauthorized_actions:\n  read_mimic_iv_3_1_source: true\n  build_run_scoped_candidate: true\n  write_private_manifest_and_aggregate_audit: true\n  create_release: false\n  modify_pointer: false\n  export_row_level_data: false\n  train_model: false\n""",
        encoding="utf-8",
    )

    candidate = build_production_candidate(config, run_id="20260808T200000Z-test")
    report = audit_production_candidate(config, run_id="20260808T200000Z-test")
    assert json.loads(report.read_text(encoding="utf-8"))["status"] == "PASS"
    static = pq.read_table(candidate / "static_cohort.parquet").to_pylist()
    eligible = next(row for row in static if row["stay_id"] == 100)
    assert eligible["bmi"] == pytest.approx(25.0)
    assert eligible["cirrhosis"] is True
    assert eligible["aids"] is None
    blocked = pq.read_table(candidate / "blocked_8h.parquet")
    assert "fio2__median" not in blocked.column_names
    assert blocked.column("urea__median").null_count == blocked.num_rows
    manifest = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["lineage"]["blocked_8h_reads_blocked_15m"] is False
