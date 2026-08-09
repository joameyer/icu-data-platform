from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.contracts import load_input_contract
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.mock_data import (
    generate_and_write_mock_data,
    load_mock_generation_config,
)


PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT_PATH = (
    PROJECT_ROOT / "asic" / "config" / "pooled" / "contract.yaml"
)


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


def _write_production_fixture(root: Path) -> tuple[Path, list[str]]:
    contract = load_input_contract(CONTRACT_PATH)
    pooled = root / "asic" / "data" / "production" / "pooled"
    pooled.mkdir(parents=True)
    hospital_codes = list(contract.expected_hospital_codes)
    stay_ids = [
        f"{1000 + position}:{hospital}"
        for hospital in hospital_codes
        for position in range(3)
    ]
    static_hids = [
        float(hospital)
        for hospital in hospital_codes
        for _ in range(3)
    ]

    static_arrays: list[pa.Array] = []
    static_schema_fields: list[pa.Field] = []
    for column in contract.static.columns:
        arrow_type = _arrow_type(column.arrow_type_for("production"))
        static_schema_fields.append(pa.field(column.name, arrow_type))
        if column.name == "Pseudo-ID":
            values = stay_ids
        elif column.name == "hid":
            values = static_hids
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            values = [f"{column.name}_{index % 3}" for index in range(len(stay_ids))]
        elif pa.types.is_integer(arrow_type):
            values = [index % 3 for index in range(len(stay_ids))]
        else:
            values = [float(index + 1) for index in range(len(stay_ids))]
        static_arrays.append(pa.array(values, type=arrow_type))
    static_table = pa.Table.from_arrays(
        static_arrays,
        schema=pa.schema(static_schema_fields),
    )
    pq.write_table(static_table, pooled / contract.static.filename)

    dynamic_stay_ids = [stay_id for stay_id in stay_ids for _ in range(2)]
    dynamic_hids = [
        int(stay_id.rsplit(":", 1)[1]) for stay_id in dynamic_stay_ids
    ]
    relative_minutes = [minute for _ in stay_ids for minute in (0.0, 60.0)]
    anchor = datetime(2020, 1, 1)
    anchored_times = [
        anchor + timedelta(minutes=minute) for minute in relative_minutes
    ]
    dynamic_arrays: list[pa.Array] = []
    dynamic_schema_fields: list[pa.Field] = []
    for column in contract.dynamic.columns:
        arrow_type = _arrow_type(column.arrow_type_for("production"))
        dynamic_schema_fields.append(pa.field(column.name, arrow_type))
        if column.name == "Pseudo-ID":
            values = dynamic_stay_ids
        elif column.name == "hid":
            values = dynamic_hids
        elif column.name == "Zeit_ab_Aufnahme":
            values = relative_minutes
        elif column.name == "timeidx":
            values = anchored_times
        elif pa.types.is_integer(arrow_type):
            values = [index % 4 for index in range(len(dynamic_stay_ids))]
        else:
            values = [float(index % 11) for index in range(len(dynamic_stay_ids))]
        dynamic_arrays.append(pa.array(values, type=arrow_type))
    dynamic_table = pa.Table.from_arrays(
        dynamic_arrays,
        schema=pa.schema(dynamic_schema_fields),
    )
    pq.write_table(
        dynamic_table,
        pooled / contract.dynamic.filename,
        row_group_size=10,
    )
    return pooled, stay_ids


def _write_config(root: Path, source_pooled: Path) -> Path:
    config_path = root / "asic" / "config" / "pooled" / "generation" / "mock.yaml"
    config_path.parent.mkdir(parents=True)
    output_root = root / "asic" / "data" / "mock"
    config = {
        "contract": str(CONTRACT_PATH),
        "paths": {
            "source_pooled": str(source_pooled),
            "output_pooled": str(output_root / "pooled"),
            "manifest": str(output_root / "mock_generation_manifest.json"),
        },
        "sampling": {"seed": 42, "stays_per_hospital": 2},
        "protected_columns": {
            "static": ["Pseudo-ID", "Liegedauer_ICU", "hid"],
            "dynamic": ["Pseudo-ID", "Zeit_ab_Aufnahme", "hid", "timeidx"],
        },
        "static_shuffle_blocks": [
            {
                "name": "height_weight_bmi",
                "columns": [
                    "clusterKoerpergewicht",
                    "clusterKoerpergroesse",
                    "BMI",
                    "weightKg",
                    "heightcm",
                ],
            },
            {
                "name": "outcomes",
                "columns": [
                    "Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)",
                    "Sterblichkeit",
                    "KH-Sterblichkeit",
                ],
            },
        ],
    }
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    return config_path


def test_generate_and_write_mock_data_end_to_end(tmp_path: Path) -> None:
    source_pooled, source_stay_ids = _write_production_fixture(tmp_path)
    config = load_mock_generation_config(_write_config(tmp_path, source_pooled))

    result = generate_and_write_mock_data(
        config,
        reporter=None,
    )

    assert result.audit_status == "pass"
    assert result.static_row_count == 14
    assert result.dynamic_row_count == 28
    assert result.stay_count == 14
    assert result.static_path.is_file()
    assert result.dynamic_path.is_file()
    assert result.manifest_path.is_file()

    mock_static = pq.read_table(result.static_path).to_pandas()
    mock_dynamic = pq.read_table(result.dynamic_path).to_pandas()
    assert not set(mock_static["Pseudo-ID"]).intersection(source_stay_ids)
    assert set(mock_static["Pseudo-ID"]) == set(mock_dynamic["Pseudo-ID"])
    for hospital in (0, 2, 3, 4, 6, 7, 8):
        expected = {f"1:{hospital}", f"2:{hospital}"}
        assert expected.issubset(set(mock_static["Pseudo-ID"]))

    contract = load_input_contract(CONTRACT_PATH)
    assert [str(field.type) for field in pq.read_schema(result.static_path)] == [
        column.arrow_type_for("mock") for column in contract.static.columns
    ]
    assert [str(field.type) for field in pq.read_schema(result.dynamic_path)] == [
        column.arrow_type_for("mock") for column in contract.dynamic.columns
    ]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["contract_audit_status"] == "pass"
    assert manifest["output"]["unique_stays"] == 14
    assert "stay_id_mapping" not in manifest

    with pytest.raises(FileExistsError, match="--overwrite"):
        generate_and_write_mock_data(config, reporter=None)


def test_mock_generation_config_rejects_production_output(tmp_path: Path) -> None:
    source_pooled, _ = _write_production_fixture(tmp_path)
    config_path = _write_config(tmp_path, source_pooled)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["paths"]["output_pooled"] = str(
        tmp_path / "asic" / "data" / "production" / "mock-output"
    )
    config_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigurationError, match="production"):
        load_mock_generation_config(config_path)
