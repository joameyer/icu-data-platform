from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import zipfile

import pyarrow.parquet as pq
import pytest

from asic_pipeline.errors import IngestionError
from asic_pipeline.ingestion import IngestionConfig, ingest_hospital
from asic_pipeline.inventory import build_raw_inventory, write_inventory_report_bundle
from asic_pipeline.inventory.hashing import sha256_file


def _ingestion_config(
    inventory_config: object,
    raw_root: Path,
    project_root: Path,
    *,
    rows_per_batch: int = 1,
) -> IngestionConfig:
    return IngestionConfig(
        inventory=inventory_config,  # type: ignore[arg-type]
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=raw_root.parent / "data",
        rows_per_batch=rows_per_batch,
    )


def _write_inventory(config: object, run_id: str) -> None:
    inventory = build_raw_inventory(config)  # type: ignore[arg-type]
    assert inventory.overall_status == "pass"
    write_inventory_report_bundle(
        inventory,
        config.paths.reports,  # type: ignore[attr-defined]
        run_id=run_id,
    )


def test_lossless_ingestion_preserves_duplicate_headers_empty_strings_and_order(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    first_stay = "synthetic_stay_00"
    first_path = raw_root / "00" / f"{first_stay}.csv"
    headers = [f"column_{index}" for index in range(125)]
    headers[0] = "Pseudo-ID"
    headers[1] = "Zeit_ab_Aufnahme"
    headers[2] = "literal_empty"
    headers[121] = "ARDS_Diagnose_App"
    headers[122] = "ARDS_Diagnose_App"
    first_row = [""] * 125
    first_row[0] = first_stay
    first_row[1] = "0"
    first_row[121] = "yes"
    first_path.write_text(
        ";".join(headers) + "\n" + ";".join(first_row) + "\n",
        encoding="utf-8",
        newline="",
    )

    second_stay = "synthetic_stay_00_b"
    second_path = raw_root / "00" / f"{second_stay}.csv"
    second_path.write_text(
        "Pseudo-ID;Zeit_ab_Aufnahme;extra_raw\n"
        f"{second_stay};60;nan\n",
        encoding="utf-8",
        newline="",
    )
    _write_inventory(inventory_config, "lossless_inventory")
    config = _ingestion_config(inventory_config, raw_root, project_root)

    result = ingest_hospital(config, "lossless_inventory", "asic_UK00")

    assert result.static_row_count == 1
    assert result.dynamic_row_count == 2
    assert result.publication_ready is False
    assert result.output_directory.stat().st_mode & 0o777 == 0o700
    assert result.dynamic_path.stat().st_mode & 0o777 == 0o600

    table = pq.read_table(result.dynamic_path)
    names = table.schema.names
    assert "ARDS_Diagnose_App" not in names
    assert names.count("ARDS_Diagnose_App_col1") == 1
    assert names.count("ARDS_Diagnose_App_col2") == 1
    assert len(names) == len(set(names))
    literal_index = names.index("literal_empty")
    extra_index = names.index("extra_raw")
    assert table.column(literal_index).to_pylist() == ["", None]
    assert table.column(extra_index).to_pylist() == [None, "nan"]
    assert table.column("ARDS_Diagnose_App_col1").to_pylist() == ["yes", None]
    assert table.column("ARDS_Diagnose_App_col2").to_pylist() == ["", None]
    first_field = table.schema.field("ARDS_Diagnose_App_col1")
    second_field = table.schema.field("ARDS_Diagnose_App_col2")
    assert first_field.metadata[b"raw_name"] == b"ARDS_Diagnose_App"
    assert first_field.metadata[b"raw_occurrence"] == b"1"
    assert second_field.metadata[b"raw_name"] == b"ARDS_Diagnose_App"
    assert second_field.metadata[b"raw_occurrence"] == b"2"
    assert table.column(names.index("__v3_source_row_number")).to_pylist() == [2, 2]
    assert table.column(names.index("__v3_source_order")).to_pylist() == [1, 2]
    assert table.column(names.index("__v3_filename_stay_id")).to_pylist() == [
        first_stay,
        second_stay,
    ]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["value_representation"] == {
        "raw_cells": "string",
        "literal_empty_cell": "empty_string",
        "column_absent_from_source_schema": "parquet_null",
        "clinical_numeric_inference_applied": False,
        "textual_missing_policy_applied": False,
        "clinical_parsing_applied": False,
        "clinical_semantic_translation_applied": False,
        "duplicate_physical_names_disambiguated": True,
    }
    assert manifest["tables"]["dynamic"]["source_row_count"] == 2
    assert manifest["artifact_version"] == "0.2"
    assert manifest["tables"]["dynamic"]["output_row_count"] == 2
    assert manifest["tables"]["dynamic"]["schema_variant_count"] == 2
    assert manifest["tables"]["dynamic"]["literal_empty_cell_count"] > 0
    assert manifest["tables"]["dynamic"]["absent_source_column_null_count"] > 0
    assert [
        item
        for item in manifest["raw_columns"]["dynamic"]
        if item["raw_name"] == "ARDS_Diagnose_App"
    ] == [
        {
            "raw_name": "ARDS_Diagnose_App",
            "occurrence": 1,
            "physical_name": "ARDS_Diagnose_App_col1",
            "physical_name_rule_id": (
                "uk00_ards_diagnose_app_positional_storage_names"
            ),
        },
        {
            "raw_name": "ARDS_Diagnose_App",
            "occurrence": 2,
            "physical_name": "ARDS_Diagnose_App_col2",
            "physical_name_rule_id": (
                "uk00_ards_diagnose_app_positional_storage_names"
            ),
        },
    ]
    assert manifest["checks"]["parquet_physical_column_names_unique"] is True
    assert manifest["checks"]["duplicate_raw_occurrences_preserved"] is True
    assert {
        item["classification"]
        for item in manifest["source_selection"]["excluded_files"]
    } == {"additional_static_candidate"}
    assert manifest["source_selection"]["hospital_file_classification_counts"] == {
        "additional_static_candidate": 1,
        "dynamic_candidate": 2,
        "selected_static_candidate": 1,
    }
    assert manifest["source_selection"][
        "untrusted_pooled_directories_pruned"
    ] == ["pooled"]
    assert manifest["source_selection"]["unknown_or_unclassified_file_count"] == 0

    with pytest.raises(IngestionError, match="will not be overwritten"):
        ingest_hospital(config, "lossless_inventory", "asic_UK00")


def test_ingestion_rejects_source_changed_after_inventory(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    _write_inventory(inventory_config, "hash_inventory")
    dynamic_path = (
        raw_root
        / "02"
        / "dynamische_variablen_kds_patient_synthetic_stay_02.csv"
    )
    dynamic_path.write_text(
        dynamic_path.read_text(encoding="utf-8") + "synthetic_stay_02;120;1/2\n",
        encoding="utf-8",
        newline="",
    )
    config = _ingestion_config(inventory_config, raw_root, project_root)

    with pytest.raises(IngestionError, match="Source hash changed after inventory"):
        ingest_hospital(config, "hash_inventory", "asic_UK02")
    assert not (config.output_root / "asic_UK02").exists()


def test_ingestion_rejects_any_unapproved_inventory_blocker(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    inventory = build_raw_inventory(inventory_config)  # type: ignore[arg-type]
    inventory.private_manifest["blocking_findings"] = [
        {"check": "unknown_or_unclassified_files"}
    ]
    write_inventory_report_bundle(
        inventory,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id="blocked_inventory",
    )
    config = _ingestion_config(inventory_config, raw_root, project_root)

    with pytest.raises(IngestionError, match="blockers not approved"):
        ingest_hospital(config, "blocked_inventory", "asic_UK00")
    assert not (config.output_root / "asic_UK00").exists()


def test_exact_reviewed_zip_is_rehashed_and_excluded_from_ingestion(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    base_config, raw_root = synthetic_inventory
    archive_path = raw_root / "00" / "synthetic_old_snapshot.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("snapshot.csv", "raw;value\n1;old\n")
    archive_size = archive_path.stat().st_size
    archive_hash = sha256_file(archive_path)

    shipped_policy = project_root / "asic/config/inventory/policy.yaml"
    policy_text = shipped_policy.read_text(encoding="utf-8")
    policy_text = policy_text.replace(
        "expected_size_bytes: 290741496",
        f"expected_size_bytes: {archive_size}",
    )
    policy_text = policy_text.replace(
        "expected_sha256: 8d277b9c16bde6ca404203da6148d01cbd717619f299cd60e95b860fc6687084",
        f"expected_sha256: {archive_hash}",
    )
    test_policy = raw_root.parent / "inventory_policy.yaml"
    test_policy.write_text(policy_text, encoding="utf-8", newline="")
    inventory_config = replace(  # type: ignore[arg-type]
        base_config,
        policy_path=test_policy,
    )
    inventory = build_raw_inventory(inventory_config)
    assert [item["check"] for item in inventory.blocking_findings] == [
        "provisional_archive_owner_confirmation"
    ]
    write_inventory_report_bundle(
        inventory,
        inventory_config.paths.reports,
        run_id="archive_inventory",
    )
    config = _ingestion_config(inventory_config, raw_root, project_root)

    result = ingest_hospital(config, "archive_inventory", "asic_UK00")

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["input"]["inventory_blockers_allowed_for_ingestion"] == [
        "provisional_archive_owner_confirmation"
    ]
    excluded = manifest["source_selection"]["excluded_files"]
    archive_record = next(
        item
        for item in excluded
        if item["classification"] == "provisionally_excluded_archive"
    )
    assert archive_record["sha256"] == archive_hash
    assert archive_record["source_hash_revalidated"] is True
    assert archive_record["archive_resolution"]["ingestion_action"] == "exclude"
    assert manifest["source_selection"][
        "global_provisional_archive_exclusions"
    ][0]["sha256"] == archive_hash
    assert manifest["source_selection"]["selected_dynamic_file_count"] == 1
    assert manifest["tables"]["dynamic"]["output_row_count"] == 2
