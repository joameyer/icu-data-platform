from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from asic_pipeline.audit import audit_translated_input
from asic_pipeline.cli import main
from asic_pipeline.contracts import load_translated_input_contract
from asic_pipeline.contracts import load_input_contract
from asic_pipeline.translations import load_translation_registry
from asic_pipeline.translations.pipeline import _translated_schema
from asic_pipeline.translated_input import load_translated_input_config


REQUIRED_CHECKS = {
    "input_schemas_match_contract": True,
    "row_counts_preserved": True,
}


def _column_specs(schema: pa.Schema) -> list[dict[str, Any]]:
    return [
        {"name": field.name, "arrow_type": str(field.type)}
        for field in schema
    ]


def _write_fixture(
    root: Path,
    *,
    canonical_mismatch: bool = False,
    manifest_policy_version: str = "1.3",
    manifest_source_static_rows: int = 2,
    extra_static_column: bool = False,
) -> Path:
    translated = root / "asic" / "data" / "mock" / "translated"
    reports = root / "asic" / "reports" / "mock"
    translated.mkdir(parents=True)

    static_contract_schema = pa.schema(
        [
            pa.field("stay_id_global", pa.string()),
            pa.field("hospital_id", pa.string()),
            pa.field("hospital_code_source", pa.float64()),
            pa.field("value", pa.float32()),
        ]
    )
    static_schema = static_contract_schema
    if extra_static_column:
        static_schema = static_schema.append(pa.field("unexpected", pa.int8()))
    static_values: dict[str, pa.Array] = {
        "stay_id_global": pa.array(["1:0", "1:2"], type=pa.string()),
        "hospital_id": pa.array(
            ["asic_UK02" if canonical_mismatch else "asic_UK00", "asic_UK02"],
            type=pa.string(),
        ),
        "hospital_code_source": pa.array([0.0, 2.0], type=pa.float64()),
        "value": pa.array([1.0, 2.0], type=pa.float32()),
        "unexpected": pa.array([1, 1], type=pa.int8()),
    }
    static = pa.Table.from_arrays(
        [static_values[field.name] for field in static_schema],
        schema=static_schema,
    )

    dynamic_schema = pa.schema(
        [
            pa.field("stay_id_global", pa.string()),
            pa.field("hospital_id", pa.string()),
            pa.field("hospital_code_source", pa.int32()),
            pa.field("minutes_since_icu_admission", pa.float32()),
            pa.field("anchored_time_since_icu_admission", pa.timestamp("us")),
            pa.field("measurement", pa.float32()),
        ]
    )
    anchor = datetime(2020, 1, 1)
    minutes = [0.0, 60.0, 0.0, 60.0]
    dynamic = pa.Table.from_arrays(
        [
            pa.array(["1:0", "1:0", "1:2", "1:2"], type=pa.string()),
            pa.array(
                ["asic_UK00", "asic_UK00", "asic_UK02", "asic_UK02"],
                type=pa.string(),
            ),
            pa.array([0, 0, 2, 2], type=pa.int32()),
            pa.array(minutes, type=pa.float32()),
            pa.array(
                [anchor + timedelta(minutes=value) for value in minutes],
                type=pa.timestamp("us"),
            ),
            pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float32()),
        ],
        schema=dynamic_schema,
    )
    pq.write_table(static, translated / "static.parquet")
    pq.write_table(dynamic, translated / "dynamic.parquet", row_group_size=2)

    contract = {
        "contract_version": "test-1",
        "dataset": "asic_translated_test",
        "expected_hospital_codes": [0, 2],
        "producer": {
            "pooled_contract_version": "1.2",
            "translation_policy_version": "1.3",
            "compatible_generator_versions": ["0.6.3"],
            "documentation": "asic/docs/pooled_to_translated.md",
        },
        "manifest": {
            "filename": "translation_manifest.json",
            "artifact": "asic_translated",
            "audit_status": "pass",
            "required_checks": REQUIRED_CHECKS,
        },
        "identifiers": {
            "stay_id_column": "stay_id_global",
            "canonical_hospital_id_column": "hospital_id",
            "source_hospital_code_column": "hospital_code_source",
        },
        "time": {
            "relative_time_column": "minutes_since_icu_admission",
            "anchored_time_column": "anchored_time_since_icu_admission",
        },
        "downstream": {
            "preserve_all_input_columns": True,
            "preserve_row_count_and_order": True,
            "preserve_identifiers_and_time_keys": True,
            "value_changes_require_approved_cleaning_rules": True,
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
    config_dir = root / "asic" / "config"
    contract_path = config_dir / "translated" / "contract.yaml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        yaml.safe_dump(contract, sort_keys=False),
        encoding="utf-8",
    )

    manifest = {
        "artifact": "asic_translated",
        "generator_version": "0.6.3",
        "dataset_context": "mock",
        "source_contract_version": "1.2",
        "translation_policy_version": manifest_policy_version,
        "documentation": "asic/docs/pooled_to_translated.md",
        "audit_status": "pass",
        "checks": REQUIRED_CHECKS,
        "source": {
            "static_rows": manifest_source_static_rows,
            "dynamic_rows": 4,
        },
        "output": {
            "translated_directory": str(translated.resolve()),
            "static_rows": 2,
            "dynamic_rows": 4,
            "static_columns": len(static_contract_schema),
            "dynamic_columns": len(dynamic_schema),
        },
    }
    (translated / "translation_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    config = {
        "dataset_context": "mock",
        "translated_contract": str(contract_path),
        "paths": {
            "translated": str(translated),
            "cleaned": str(root / "asic" / "data" / "mock" / "cleaned"),
            "reports": str(reports),
        },
        "translated_audit": {
            "batch_size": 2,
            "check_duplicate_dynamic_keys": True,
            "time_anchor": "2020-01-01 00:00:00",
            "time_tolerance_seconds": 0.0,
        },
    }
    config_path = config_dir / "datasets" / "mock.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _audit(config_path: Path):
    config = load_translated_input_config(config_path)
    contract = load_translated_input_contract(config.contract_path)
    return audit_translated_input(config, contract)


def test_valid_translated_fixture_passes(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path))

    assert report.overall_status == "pass"
    assert report.contract_version == "test-1"
    assert report.metrics["static"]["unique_stay_count"] == 2
    assert report.metrics["dynamic"]["unique_stay_count"] == 2
    assert all(check.status == "pass" for check in report.checks)


def test_translated_schema_drift_is_blocking(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, extra_static_column=True))

    assert report.overall_status == "fail"
    check = next(
        item for item in report.checks if item.name == "static_schema_matches_contract"
    )
    assert check.observed["extra_columns"] == ["unexpected"]


def test_manifest_policy_version_drift_is_blocking(tmp_path: Path) -> None:
    report = _audit(
        _write_fixture(tmp_path, manifest_policy_version="unexpected")
    )

    assert report.overall_status == "fail"
    check = next(
        item
        for item in report.checks
        if item.name == "translation_manifest_producer_matches_contract"
    )
    assert check.status == "fail"


def test_manifest_row_preservation_mismatch_is_blocking(tmp_path: Path) -> None:
    report = _audit(
        _write_fixture(tmp_path, manifest_source_static_rows=3)
    )

    assert report.overall_status == "fail"
    check = next(
        item
        for item in report.checks
        if item.name == "translation_manifest_rows_are_preserved"
    )
    assert check.status == "fail"


def test_canonical_hospital_id_mismatch_is_blocking(tmp_path: Path) -> None:
    report = _audit(_write_fixture(tmp_path, canonical_mismatch=True))

    assert report.overall_status == "fail"
    check = next(
        item for item in report.checks if item.name == "static_identifiers_are_valid"
    )
    assert check.observed["canonical_hospital_id_mismatches"] == 1


def test_translated_audit_cli_writes_only_report(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)

    exit_code = main(["audit-translated", "--config", str(config_path)])

    assert exit_code == 0
    reports = list((tmp_path / "asic" / "reports" / "mock").glob("*.json"))
    assert len(reports) == 1
    assert not (tmp_path / "asic" / "data" / "mock" / "cleaned").exists()
    assert not (tmp_path / "asic" / "data" / "mock" / "derived").exists()


def test_frozen_translated_contract_has_exact_accepted_schema() -> None:
    contract_path = (
        Path(__file__).parents[1]
        / "asic"
        / "config"
        / "translated"
        / "contract.yaml"
    )
    contract = load_translated_input_contract(contract_path)

    assert contract.contract_version == "1.3"
    assert contract.dataset == "asic_translated"
    assert len(contract.static.columns) == 22
    assert len(contract.dynamic.columns) == 132
    assert contract.expected_hospital_codes == (0, 2, 3, 4, 6, 7, 8)
    assert contract.producer.pooled_contract_version == "1.2"
    assert contract.producer.translation_policy_version == "1.3"
    assert contract.producer.compatible_generator_versions == ("0.6.3",)
    assert contract.preserve_all_input_columns is True
    assert contract.preserve_row_count_and_order is True
    assert contract.preserve_identifiers_and_time_keys is True
    assert contract.value_changes_require_approved_cleaning_rules is True

    static = {column.name: column for column in contract.static.columns}
    dynamic = {column.name: column for column in contract.dynamic.columns}
    assert static["stay_id_global"].arrow_type_for("production") == "large_string"
    assert static["stay_id_global"].arrow_type_for("demo") == "large_string"
    assert static["stay_id_global"].arrow_type_for("mock") == "string"
    assert static["study_implementation_phase"].arrow_type_for("mock") == "double"
    assert dynamic["ards_diagnosis_app"].arrow_type_for("production") == "int8"
    assert dynamic["ards_diagnosis_app"].arrow_type_for("mock") == "double"


def test_frozen_translated_schema_matches_reviewed_translation_plan() -> None:
    project_root = Path(__file__).parents[1]
    pooled_contract = load_input_contract(
        project_root / "asic" / "config" / "pooled" / "contract.yaml"
    )
    registry = load_translation_registry(
        project_root
        / "asic"
        / "config"
        / "pooled_to_translated"
        / "policy.yaml",
        pooled_contract,
    )
    translated_contract = load_translated_input_contract(
        project_root / "asic" / "config" / "translated" / "contract.yaml"
    )

    for context in ("production", "mock"):
        for table_name, source_table, expected_table in (
            ("static", pooled_contract.static, translated_contract.static),
            ("dynamic", pooled_contract.dynamic, translated_contract.dynamic),
        ):
            source_schema = pa.schema(
                [
                    pa.field(
                        column.name,
                        pa.type_for_alias(column.arrow_type_for(context)),
                    )
                    for column in source_table.columns
                ]
            )
            generated = _translated_schema(
                source_schema,
                registry.entries_for(table_name),
                registry.merge_rules_for(table_name),
                registry.value_mappings_for(table_name),
                registry.hospital_specific_semantic_split_rules_for(table_name),
                registry.output_order_for(table_name),
            )
            frozen = pa.schema(
                [
                    pa.field(
                        column.name,
                        pa.type_for_alias(column.arrow_type_for(context)),
                    )
                    for column in expected_table.columns
                ]
            )
            assert frozen == generated
