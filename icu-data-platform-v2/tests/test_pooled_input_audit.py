from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from asic_pipeline.audit import audit_pooled_input
from asic_pipeline.cli import main
from asic_pipeline.config import load_config
from asic_pipeline.contracts import load_input_contract


def _column_specs(schema: pa.Schema) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for field in schema:
        arrow_type: str | dict[str, str]
        if field.name == "Pseudo-ID":
            arrow_type = {"production": "large_string", "mock": "string"}
        else:
            arrow_type = str(field.type)
        specs.append({"name": field.name, "arrow_type": arrow_type})
    return specs


def _write_fixture(
    root: Path,
    *,
    mismatch_hid: bool = False,
    time_offset_seconds: int = 0,
    extra_static_column: bool = False,
    dataset_context: str = "mock",
    duplicate_dynamic_key: bool = False,
) -> Path:
    pooled = root / "asic" / "data" / dataset_context / "pooled"
    reports = root / "asic" / "reports" / dataset_context
    config_dir = root / "asic" / "config"
    contract_dir = config_dir / "pooled"
    pooled.mkdir(parents=True)
    contract_dir.mkdir(parents=True)
    id_type = (
        pa.large_string()
        if dataset_context in {"demo", "production"}
        else pa.string()
    )

    static_schema = pa.schema(
        [
            pa.field("Pseudo-ID", id_type),
            pa.field("hid", pa.float64()),
            pa.field("source_value", pa.string()),
        ]
    )
    static_values: dict[str, pa.Array] = {
        "Pseudo-ID": pa.array(["1:0", "1:2"], type=id_type),
        "hid": pa.array([2.0 if mismatch_hid else 0.0, 2.0], type=pa.float64()),
        "source_value": pa.array(["a", "b"], type=pa.string()),
    }
    static_contract_schema = static_schema
    if extra_static_column:
        static_schema = static_schema.append(pa.field("unexpected", pa.int8()))
        static_values["unexpected"] = pa.array([1, 1], type=pa.int8())
    static_table = pa.Table.from_arrays(
        [static_values[field.name] for field in static_schema],
        schema=static_schema,
    )

    dynamic_schema = pa.schema(
        [
            pa.field("Pseudo-ID", id_type),
            pa.field("hid", pa.int32()),
            pa.field("Zeit_ab_Aufnahme", pa.float32()),
            pa.field("timeidx", pa.timestamp("us")),
            pa.field("measurement", pa.float32()),
        ]
    )
    anchor = datetime(2020, 1, 1)
    minutes = [0.0, 0.0 if duplicate_dynamic_key else 60.0, 0.0, 60.0]
    anchored = [
        anchor + timedelta(minutes=value, seconds=time_offset_seconds)
        for value in minutes
    ]
    dynamic_table = pa.Table.from_arrays(
        [
            pa.array(["1:0", "1:0", "1:2", "1:2"], type=id_type),
            pa.array([0, 0, 2, 2], type=pa.int32()),
            pa.array(minutes, type=pa.float32()),
            pa.array(anchored, type=pa.timestamp("us")),
            pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()),
        ],
        schema=dynamic_schema,
    )
    pq.write_table(static_table, pooled / "static.parquet")
    pq.write_table(dynamic_table, pooled / "dynamic.parquet")

    contract = {
        "contract_version": "test-1",
        "dataset": "asic_pooled_test",
        "expected_hospital_codes": [0, 2],
        "identifiers": {
            "stay_id_column": "Pseudo-ID",
            "hospital_id_column": "hid",
        },
        "time": {
            "relative_time_column": "Zeit_ab_Aufnahme",
            "anchored_time_column": "timeidx",
        },
        "tables": {
            "static": {
                "filename": "static.parquet",
                "columns": _column_specs(static_contract_schema),
            },
            "dynamic": {
                "filename": "dynamic.parquet",
                "columns": _column_specs(dynamic_schema),
            },
        },
    }
    contract_path = contract_dir / "pooled_input_test.yaml"
    contract_path.write_text(
        yaml.safe_dump(contract, sort_keys=False),
        encoding="utf-8",
    )

    config = {
        "dataset_context": dataset_context,
        "contract": str(contract_path),
        "paths": {
            "pooled": str(pooled),
            "reports": str(reports),
        },
        "audit": {
            "batch_size": 2,
            "check_duplicate_dynamic_keys": True,
            "time_anchor": "2020-01-01 00:00:00",
            "time_tolerance_seconds": 0.0,
        },
    }
    config_path = config_dir / "datasets" / f"{dataset_context}.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _audit(config_path: Path):
    config = load_config(config_path)
    contract = load_input_contract(config.contract_path)
    return audit_pooled_input(config, contract)


def test_valid_pooled_fixture_passes(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path))

    assert report.overall_status == "pass"
    assert report.metrics["static"]["unique_stay_count"] == 2
    assert report.metrics["dynamic"]["unique_stay_count"] == 2
    assert all(check.status == "pass" for check in report.checks)


def test_production_context_uses_strict_production_arrow_type(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, dataset_context="production"))

    assert report.overall_status == "pass"
    assert report.inputs["static"]["columns"][0]["arrow_type"] == "large_string"
    assert report.inputs["dynamic"]["columns"][0]["arrow_type"] == "large_string"
    assert report.limitations[0].startswith("Production results")


def test_demo_context_uses_production_arrow_type_and_subset_limitation(
    tmp_path: Path,
) -> None:
    report = _audit(_write_fixture(tmp_path, dataset_context="demo"))

    assert report.overall_status == "pass"
    assert report.inputs["static"]["columns"][0]["arrow_type"] == "large_string"
    assert report.inputs["dynamic"]["columns"][0]["arrow_type"] == "large_string"
    assert report.limitations[0].startswith("Demo results")


def test_pseudo_id_suffix_hid_mismatch_is_blocking(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, mismatch_hid=True))

    assert report.overall_status == "fail"
    check = next(
        item for item in report.checks if item.name == "static_identifiers_are_valid"
    )
    assert check.status == "fail"
    assert check.observed["pseudo_id_suffix_hid_mismatches"] == 1


def test_time_disagreement_is_blocking_after_production_review(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, time_offset_seconds=30))

    assert report.overall_status == "fail"
    check = next(
        item
        for item in report.checks
        if item.name == "anchored_time_agrees_with_relative_minutes"
    )
    assert check.severity == "blocking"
    assert check.status == "fail"
    assert check.observed["disagreement_rows"] == 4


def test_duplicate_dynamic_stay_time_key_is_blocking(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, duplicate_dynamic_key=True))

    assert report.overall_status == "fail"
    check = next(
        item
        for item in report.checks
        if item.name == "dynamic_stay_time_keys_are_unique"
    )
    assert check.severity == "blocking"
    assert check.status == "fail"
    assert check.observed["duplicate_rows_beyond_first"] == 1


def test_schema_drift_is_blocking(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, extra_static_column=True))

    assert report.overall_status == "fail"
    check = next(
        item
        for item in report.checks
        if item.name == "static_schema_matches_contract"
    )
    assert check.status == "fail"
    assert check.observed["extra_columns"] == ["unexpected"]


def test_cli_writes_report_without_writing_data(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)

    exit_code = main(["audit-pooled", "--config", str(config_path)])

    assert exit_code == 0
    reports = list((tmp_path / "asic" / "reports" / "mock").glob("*.json"))
    assert len(reports) == 1
    assert not (tmp_path / "asic" / "data" / "mock" / "translated").exists()
    assert not (tmp_path / "asic" / "data" / "mock" / "cleaned").exists()
    assert not (tmp_path / "asic" / "data" / "mock" / "derived").exists()


def test_cli_rejects_cross_context_report_override(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    report_path = tmp_path / "asic" / "reports" / "production" / "audit.json"

    exit_code = main(
        [
            "audit-pooled",
            "--config",
            str(config_path),
            "--report-path",
            str(report_path),
        ]
    )

    assert exit_code == 2
    assert not report_path.exists()


def test_frozen_contract_contains_complete_mock_schema() -> None:
    contract_path = (
        Path(__file__).parents[1]
        / "asic"
        / "config"
        / "pooled"
        / "contract.yaml"
    )
    contract = load_input_contract(contract_path)

    assert len(contract.static.columns) == 21
    assert len(contract.dynamic.columns) == 139
    assert contract.expected_hospital_codes == (0, 2, 3, 4, 6, 7, 8)
    assert contract.contract_version == "1.2"

    static_columns = {column.name: column for column in contract.static.columns}
    dynamic_columns = {column.name: column for column in contract.dynamic.columns}
    assert static_columns["Pseudo-ID"].arrow_type_for("production") == "large_string"
    assert static_columns["Pseudo-ID"].arrow_type_for("demo") == "large_string"
    assert static_columns["Pseudo-ID"].arrow_type_for("mock") == "string"
    assert static_columns["Phase"].arrow_type_for("production") == "int8"
    assert static_columns["Phase"].arrow_type_for("mock") == "double"
    assert dynamic_columns["ARDS_Diagnose_App"].arrow_type_for("production") == "int8"
    assert dynamic_columns["ARDS_Diagnose_App"].arrow_type_for("mock") == "double"
