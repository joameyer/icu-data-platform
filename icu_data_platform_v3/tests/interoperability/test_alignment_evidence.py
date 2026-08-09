from __future__ import annotations

import csv
import gzip
from pathlib import Path

from interoperability_catalog.evidence import audit_mapping_evidence


def _write_gzip(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_bounded_aggregate_mapping_evidence_has_no_rows_or_paths(
    tmp_path: Path, project_root: Path
) -> None:
    source = tmp_path / "mimic"
    _write_gzip(
        source / "icu/d_items.csv.gz",
        ["itemid", "label", "category", "linksto", "unitname"],
        [
            {"itemid": 220045, "label": "Heart Rate", "category": "Routine Vital Signs", "linksto": "chartevents", "unitname": "bpm"},
            {"itemid": 223761, "label": "Temperature Fahrenheit", "category": "Routine Vital Signs", "linksto": "chartevents", "unitname": "degF"},
        ],
    )
    _write_gzip(
        source / "hosp/d_labitems.csv.gz",
        ["itemid", "label", "fluid", "category"],
        [
            {"itemid": 50862, "label": "Albumin", "fluid": "Blood", "category": "Chemistry"},
            {"itemid": 50813, "label": "Lactate", "fluid": "Blood", "category": "Blood Gas"},
        ],
    )
    _write_gzip(
        source / "icu/chartevents.csv.gz",
        ["itemid", "valueuom"],
        [
            {"itemid": 220045, "valueuom": "bpm"},
            {"itemid": 220045, "valueuom": "beats/min"},
            {"itemid": 999999, "valueuom": "bpm"},
        ],
    )
    _write_gzip(
        source / "hosp/labevents.csv.gz",
        ["itemid", "valueuom"],
        [
            {"itemid": 50862, "valueuom": "g/dL"},
            {"itemid": 50813, "valueuom": "mmol/L"},
        ],
    )
    private, public = audit_mapping_evidence(
        mimic_root=source,
        mapping_csv=project_root / "interoperability/config/mimic_source_mappings_0_1.csv",
    )
    assert public["unit_summary"]["chartevents.selected_observation_count"] == 2
    assert public["unit_summary"]["chartevents.accepted_unit_observation_count"] == 1
    assert public["unit_summary"]["chartevents.unknown_or_unaccepted_unit_observation_count"] == 1
    assert "observed_item_unit_counts" not in public
    assert private["row_level_data_exported"] is False
    assert str(tmp_path) not in str(private)
    assert "source_value" not in str(private)
