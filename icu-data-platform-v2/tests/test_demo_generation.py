from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.demo_data import (
    generate_and_write_demo_data,
    load_demo_generation_config,
)
from asic_pipeline.errors import ConfigurationError


def _write_production_fixture(root: Path) -> tuple[Path, Path]:
    pooled = root / "asic" / "data" / "production" / "pooled"
    config_dir = root / "asic" / "config"
    contract_dir = config_dir / "pooled"
    pooled.mkdir(parents=True)
    contract_dir.mkdir(parents=True)

    stay_ids = [
        f"{1000 + position}:{hospital}"
        for hospital in (0, 2)
        for position in range(3)
    ]
    static_schema = pa.schema(
        [
            pa.field("Pseudo-ID", pa.large_string()),
            pa.field("hid", pa.float64()),
            pa.field("source_text", pa.large_string()),
            pa.field("source_integer", pa.int8()),
        ]
    )
    static_table = pa.Table.from_arrays(
        [
            pa.array(stay_ids, type=pa.large_string()),
            pa.array([0.0] * 3 + [2.0] * 3, type=pa.float64()),
            pa.array(["a", None, "c", "d", "e", "f"], type=pa.large_string()),
            pa.array([1, 2, 3, 4, 5, 6], type=pa.int8()),
        ],
        schema=static_schema,
    )
    pq.write_table(static_table, pooled / "static.parquet")

    dynamic_ids = [stay_id for stay_id in stay_ids for _ in range(2)]
    minutes = [minute for _ in stay_ids for minute in (0.0, 60.0)]
    anchor = datetime(2020, 1, 1)
    dynamic_schema = pa.schema(
        [
            pa.field("DeltaP", pa.float32()),
            pa.field("deltaP", pa.float32()),
            pa.field("Pseudo-ID", pa.large_string()),
            pa.field("Zeit_ab_Aufnahme", pa.float32()),
            pa.field("hid", pa.int32()),
            pa.field("timeidx", pa.timestamp("us")),
        ]
    )
    dynamic_table = pa.Table.from_arrays(
        [
            pa.array([float(index) for index in range(12)], type=pa.float32()),
            pa.array(
                [None if index == 3 else float(index + 100) for index in range(12)],
                type=pa.float32(),
            ),
            pa.array(dynamic_ids, type=pa.large_string()),
            pa.array(minutes, type=pa.float32()),
            pa.array(
                [int(stay_id.rsplit(":", 1)[1]) for stay_id in dynamic_ids],
                type=pa.int32(),
            ),
            pa.array(
                [anchor + timedelta(minutes=minute) for minute in minutes],
                type=pa.timestamp("us"),
            ),
        ],
        schema=dynamic_schema,
    )
    pq.write_table(dynamic_table, pooled / "dynamic.parquet", row_group_size=3)

    def column_specs(schema: pa.Schema) -> list[dict[str, object]]:
        specs: list[dict[str, object]] = []
        for field in schema:
            arrow_type: str | dict[str, str]
            if field.name == "Pseudo-ID":
                arrow_type = {"production": "large_string", "mock": "string"}
            else:
                arrow_type = str(field.type)
            specs.append({"name": field.name, "arrow_type": arrow_type})
        return specs

    contract = {
        "contract_version": "demo-test-1",
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
                "columns": column_specs(static_schema),
            },
            "dynamic": {
                "filename": "dynamic.parquet",
                "columns": column_specs(dynamic_schema),
            },
        },
    }
    contract_path = contract_dir / "pooled_input_test.yaml"
    contract_path.write_text(
        yaml.safe_dump(contract, sort_keys=False),
        encoding="utf-8",
    )
    return pooled, contract_path


def _write_generation_config(
    root: Path,
    source_pooled: Path,
    contract_path: Path,
) -> Path:
    config_path = root / "asic" / "config" / "pooled" / "generation" / "demo.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    output_root = root / "asic" / "data" / "demo"
    config = {
        "contract": str(contract_path),
        "paths": {
            "source_pooled": str(source_pooled),
            "output_pooled": str(output_root / "pooled"),
            "manifest": str(output_root / "demo_generation_manifest.json"),
        },
        "sampling": {"seed": 42, "stays_per_hospital": 2},
    }
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    return config_path


def test_demo_generation_preserves_selected_source_rows(tmp_path: Path) -> None:
    source_pooled, contract_path = _write_production_fixture(tmp_path)
    config = load_demo_generation_config(
        _write_generation_config(tmp_path, source_pooled, contract_path)
    )

    result = generate_and_write_demo_data(config, reporter=None)

    assert result.audit_status == "pass"
    assert result.static_row_count == 4
    assert result.dynamic_row_count == 8
    assert result.stay_count == 4

    source_static = pq.read_table(source_pooled / "static.parquet")
    source_dynamic = pq.read_table(source_pooled / "dynamic.parquet")
    demo_static = pq.read_table(result.static_path)
    demo_dynamic = pq.read_table(result.dynamic_path)
    selected_ids = demo_static.column("Pseudo-ID").combine_chunks()
    expected_static = source_static.filter(
        pc.is_in(source_static.column("Pseudo-ID"), value_set=selected_ids)
    )
    expected_dynamic = source_dynamic.filter(
        pc.is_in(source_dynamic.column("Pseudo-ID"), value_set=selected_ids)
    )

    assert demo_static.equals(expected_static)
    assert demo_dynamic.equals(expected_dynamic)
    assert demo_static.schema == source_static.schema
    assert demo_dynamic.schema == source_dynamic.schema
    assert set(selected_ids.to_pylist()).issubset(
        set(source_static.column("Pseudo-ID").to_pylist())
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["contract_audit_status"] == "pass"
    assert manifest["selection_only"] is True
    assert manifest["identifiers_remapped"] is False
    assert manifest["values_transformed"] is False
    assert manifest["output"]["unique_stays"] == 4
    assert "selected_ids" not in manifest
    assert "stay_id_mapping" not in manifest

    with pytest.raises(FileExistsError, match="--overwrite"):
        generate_and_write_demo_data(config, reporter=None)


def test_demo_generation_is_reproducible_with_explicit_overwrite(
    tmp_path: Path,
) -> None:
    source_pooled, contract_path = _write_production_fixture(tmp_path)
    config = load_demo_generation_config(
        _write_generation_config(tmp_path, source_pooled, contract_path)
    )
    first = generate_and_write_demo_data(config, reporter=None)
    first_static = pq.read_table(first.static_path)
    first_dynamic = pq.read_table(first.dynamic_path)

    second = generate_and_write_demo_data(
        config,
        overwrite=True,
        reporter=None,
    )

    assert pq.read_table(second.static_path).equals(first_static)
    assert pq.read_table(second.dynamic_path).equals(first_dynamic)


def test_demo_generation_config_rejects_non_demo_output(tmp_path: Path) -> None:
    source_pooled, contract_path = _write_production_fixture(tmp_path)
    config_path = _write_generation_config(
        tmp_path,
        source_pooled,
        contract_path,
    )
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["paths"]["output_pooled"] = str(
        tmp_path / "asic" / "data" / "production" / "demo-output"
    )
    config_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="production"):
        load_demo_generation_config(config_path)
