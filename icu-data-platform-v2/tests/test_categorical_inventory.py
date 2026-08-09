from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.categorical_inventory import (
    inventory_categorical_values,
    load_categorical_inventory_config,
)
from asic_pipeline.cli import main
from asic_pipeline.contracts import load_input_contract
from asic_pipeline.errors import ConfigurationError


PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT_PATH = (
    PROJECT_ROOT / "asic" / "config" / "pooled" / "contract.yaml"
)
REGISTRY_PATH = (
    PROJECT_ROOT
    / "asic"
    / "config"
    / "pooled_to_translated"
    / "policy.yaml"
)
HOSPITALS = (0, 2, 3, 4, 6, 7, 8)


def _arrow_type(type_name: str) -> pa.DataType:
    return {
        "large_string": pa.large_string(),
        "string": pa.string(),
        "int8": pa.int8(),
        "int32": pa.int32(),
        "float": pa.float32(),
        "double": pa.float64(),
        "timestamp[us]": pa.timestamp("us"),
    }[type_name]


def _static_values(column: str, row_count: int) -> list[object]:
    categorical: dict[str, list[object]] = {
        "Phase": [0, 1, 2, 0, 1, 2, 0],
        "clusterGeschlecht": [
            "weiblich",
            "männlich",
            None,
            "weiblich",
            "männlich",
            "weiblich",
            "männlich",
        ],
        "clusterKoerpergewicht": ["leicht", "mittel", "schwer", None, None, None, None],
        "clusterKoerpergroesse": ["klein", "mittel", "groß", None, None, None, None],
        "BMI": ["niedrig", "normal", "hoch", None, None, None, None],
        "Wiederaufnahme_ICU": [0.0, 1.0, None, 0.0, 0.0, 1.0, 0.0],
        "clusterAlter": ["jung", "mittel", "alt", None, None, None, None],
        "Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)": [
            "verlegt intern",
            "verlegt extern",
            "verstorben",
            None,
            None,
            None,
            None,
        ],
        "Sterblichkeit": ["0", "ICU", "KH", "nan", None, "0", "KH"],
        "KH-Sterblichkeit": ["false", "true", None, "false", "true", None, "false"],
    }
    if column == "Pseudo-ID":
        return [f"{1000 + hospital}:{hospital}" for hospital in HOSPITALS]
    if column == "hid":
        return [float(hospital) for hospital in HOSPITALS]
    if column in categorical:
        return categorical[column]
    return [None] * row_count


def _dynamic_values(column: str, stay_ids: list[str]) -> list[object]:
    row_count = len(stay_ids)
    hospitals = [int(stay_id.rsplit(":", 1)[1]) for stay_id in stay_ids]
    if column == "Pseudo-ID":
        return stay_ids
    if column == "hid":
        return hospitals
    if column == "Zeit_ab_Aufnahme":
        return [float((index % 2) * 60) for index in range(row_count)]
    if column == "timeidx":
        anchor = datetime(2020, 1, 1)
        return [
            anchor + timedelta(minutes=(index % 2) * 60)
            for index in range(row_count)
        ]
    if column == "ARDS_Diagnose_App":
        return [index % 2 for index in range(row_count)]
    if column == "ECMO":
        return [0.0, 1.0, float("nan"), None, *([0.0] * (row_count - 4))]
    if column == "Lagerungstherapie":
        return [float(index % 3) for index in range(row_count)]
    if column == "LesebestaetigungSchweregrad":
        return [None if index % 3 else 1.0 for index in range(row_count)]
    if column == "LesebestaetigugnTherapie_utc":
        return [
            0.0 if hospital == 0 and index % 2 == 0 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "LesebestaetigungTherapie_utc":
        return [
            1.0 if hospital == 2 and index % 2 == 0 else None
            for index, hospital in enumerate(hospitals)
        ]
    return [None] * row_count


def _write_pooled_fixture(root: Path) -> Path:
    contract = load_input_contract(CONTRACT_PATH)
    pooled = root / "asic" / "data" / "demo" / "pooled"
    pooled.mkdir(parents=True)
    static_ids = [f"{1000 + hospital}:{hospital}" for hospital in HOSPITALS]

    static_fields = []
    static_arrays = []
    for column in contract.static.columns:
        arrow_type = _arrow_type(column.arrow_type_for("demo"))
        static_fields.append(pa.field(column.name, arrow_type))
        static_arrays.append(
            pa.array(
                _static_values(column.name, len(static_ids)),
                type=arrow_type,
            )
        )
    pq.write_table(
        pa.Table.from_arrays(static_arrays, schema=pa.schema(static_fields)),
        pooled / contract.static.filename,
    )

    dynamic_ids = [stay_id for stay_id in static_ids for _ in range(2)]
    dynamic_fields = []
    dynamic_arrays = []
    for column in contract.dynamic.columns:
        arrow_type = _arrow_type(column.arrow_type_for("demo"))
        dynamic_fields.append(pa.field(column.name, arrow_type))
        dynamic_arrays.append(
            pa.array(_dynamic_values(column.name, dynamic_ids), type=arrow_type)
        )
    pq.write_table(
        pa.Table.from_arrays(dynamic_arrays, schema=pa.schema(dynamic_fields)),
        pooled / contract.dynamic.filename,
        row_group_size=4,
    )
    return pooled


def _write_config(
    root: Path,
    pooled: Path,
    *,
    max_distinct_values: int = 1000,
) -> Path:
    config_path = root / "asic" / "config" / "datasets" / "demo.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "dataset_context": "demo",
                "contract": str(CONTRACT_PATH),
                "translation_policy": str(REGISTRY_PATH),
                "paths": {
                    "pooled": str(pooled),
                    "reports": str(root / "asic" / "reports" / "demo"),
                },
                "categorical_inventory": {
                    "batch_size": 4,
                    "max_distinct_values_per_column": max_distinct_values,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path


def _value_record(inventory: dict[str, object], value: object) -> dict[str, object]:
    return next(item for item in inventory["values"] if item["value"] == value)


def test_categorical_inventory_counts_values_missingness_and_hospitals(
    tmp_path: Path,
) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    config = load_categorical_inventory_config(_write_config(tmp_path, pooled))
    result = inventory_categorical_values(config, reporter=None)

    assert result.audit_status == "pass"
    assert result.static_row_count == 7
    assert result.dynamic_row_count == 14
    assert result.report["checks"]["all_rows_scanned"] is True
    assert (
        result.report["documentation"]
        == "asic/docs/pooled_to_translated.md"
    )
    assert result.report["tables"]["static"]["categorical_column_count"] == 10
    assert result.report["tables"]["dynamic"]["categorical_column_count"] == 5

    death_status = result.report["tables"]["static"]["columns"]["death_status"]
    assert death_status["missing"]["null"] == 1
    assert death_status["missing"]["nan"] == 0
    literal_nan = _value_record(death_status, "nan")
    assert literal_nan["value_type"] == "string"
    assert literal_nan["count"] == 1

    ecmo = result.report["tables"]["dynamic"]["columns"]["ecmo"]
    assert ecmo["missing"]["null"] == 1
    assert ecmo["missing"]["nan"] == 1
    assert _value_record(ecmo, 0.0)["by_hospital"]["asic_UK00"] == 1

    therapy = result.report["tables"]["dynamic"]["columns"][
        "therapy_read_confirmation_utc"
    ]
    assert therapy["approved_source_merge"] is True
    assert therapy["source_columns"] == [
        "LesebestaetigugnTherapie_utc",
        "LesebestaetigungTherapie_utc",
    ]
    assert therapy["source_merge_conflict_rows"] == 0
    assert _value_record(therapy, 0.0)["by_hospital"]["asic_UK00"] == 1
    assert _value_record(therapy, 1.0)["by_hospital"]["asic_UK02"] == 1

    json.dumps(result.report, allow_nan=False)


def test_categorical_inventory_warns_when_distinct_limit_is_reached(
    tmp_path: Path,
) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    config = load_categorical_inventory_config(
        _write_config(tmp_path, pooled, max_distinct_values=1)
    )
    result = inventory_categorical_values(config, reporter=None)

    assert result.audit_status == "warning"
    assert (
        result.report["checks"]["inventories_complete_within_limit"] is False
    )
    assert "static.sex" in result.report["inventory_limit"]["incomplete_columns"]
    sex = result.report["tables"]["static"]["columns"]["sex"]
    assert sex["inventory_complete"] is False
    assert sex["distinct_non_missing_values"] is None
    assert sex["untracked_non_missing_rows"] > 0


def test_categorical_inventory_reports_resolved_binary_alias_conflict(
    tmp_path: Path,
) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("LesebestaetigungTherapie_utc").to_pylist()
    values[0] = 1.0
    source_index = dynamic.schema.get_field_index("LesebestaetigungTherapie_utc")
    dynamic = dynamic.set_column(
        source_index,
        "LesebestaetigungTherapie_utc",
        pa.array(values, type=dynamic.schema.field(source_index).type),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_categorical_inventory_config(_write_config(tmp_path, pooled))

    result = inventory_categorical_values(config, reporter=None)

    assert result.audit_status == "pass"
    assert result.report["checks"]["all_rows_scanned"] is True
    assert (
        result.report["checks"][
            "approved_categorical_aliases_have_no_unresolved_conflicts"
        ]
        is True
    )
    therapy = result.report["tables"]["dynamic"]["columns"][
        "therapy_read_confirmation_utc"
    ]
    assert therapy["source_merge_overlap_rows"] == 1
    assert therapy["source_merge_conflict_rows"] == 1
    assert therapy["source_merge_resolved_conflict_rows"] == 1
    assert therapy["source_merge_conflicts_by_hospital"]["asic_UK00"] == 1
    assert therapy["source_merge_resolved_conflicts_by_hospital"][
        "asic_UK00"
    ] == 1
    assert therapy["conflicting_rows_excluded_from_value_inventory"] == 0
    assert therapy["row_accounting"] == {
        "scanned_rows": 14,
        "inventoried_non_missing_rows": 2,
        "missing_rows": 12,
        "excluded_source_conflict_rows": 0,
        "sum_matches_scanned_rows": True,
    }
    assert therapy["values"] == [
        {
            "value": 1.0,
            "value_type": "float",
            "count": 2,
            "by_hospital": {
                "asic_UK00": 1,
                "asic_UK02": 1,
                "asic_UK03": 0,
                "asic_UK04": 0,
                "asic_UK06": 0,
                "asic_UK07": 0,
                "asic_UK08": 0,
            },
        }
    ]
    assert therapy["source_merge_conflicting_value_pairs"] == [
        {
            "left_source": "LesebestaetigugnTherapie_utc",
            "left_value": 0.0,
            "left_value_type": "float",
            "right_source": "LesebestaetigungTherapie_utc",
            "right_value": 1.0,
            "right_value_type": "float",
            "count": 1,
            "by_hospital": {
                "asic_UK00": 1,
                "asic_UK02": 0,
                "asic_UK03": 0,
                "asic_UK04": 0,
                "asic_UK06": 0,
                "asic_UK07": 0,
                "asic_UK08": 0,
            },
        }
    ]
    assert result.report["blocking_findings"][
        "categorical_alias_merge_conflicts"
    ] == []

    report_path = tmp_path / "asic" / "reports" / "demo" / "conflicts.json"
    exit_code = main(
        [
            "inventory-pooled-categories",
            "--config",
            str(config.source_path),
            "--report-path",
            str(report_path),
        ]
    )
    assert exit_code == 0
    written_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert written_report["audit_status"] == "pass"
    assert written_report["checks"]["all_rows_scanned"] is True


def test_categorical_inventory_blocks_nonbinary_therapy_confirmation(
    tmp_path: Path,
) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("LesebestaetigungTherapie_utc").to_pylist()
    values[0] = 2.0
    source_index = dynamic.schema.get_field_index(
        "LesebestaetigungTherapie_utc"
    )
    dynamic = dynamic.set_column(
        source_index,
        "LesebestaetigungTherapie_utc",
        pa.array(values, type=dynamic.schema.field(source_index).type),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_categorical_inventory_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="non-binary values"):
        inventory_categorical_values(config, reporter=None)


def test_categorical_inventory_cli_writes_report(tmp_path: Path) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    config_path = _write_config(tmp_path, pooled)
    report_path = tmp_path / "asic" / "reports" / "demo" / "inventory.json"

    exit_code = main(
        [
            "inventory-pooled-categories",
            "--config",
            str(config_path),
            "--report-path",
            str(report_path),
        ]
    )

    assert exit_code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["artifact"] == "asic_categorical_value_inventory"
    assert report["audit_status"] == "pass"


def test_categorical_inventory_config_rejects_cross_context_report_path(
    tmp_path: Path,
) -> None:
    pooled = _write_pooled_fixture(tmp_path)
    config_path = _write_config(tmp_path, pooled)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["paths"]["reports"] = str(tmp_path / "asic" / "reports" / "production")
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="production"):
        load_categorical_inventory_config(config_path)
