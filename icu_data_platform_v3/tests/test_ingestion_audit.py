from __future__ import annotations

import json
from pathlib import Path

import pytest

from asic_pipeline.errors import IngestionError
from asic_pipeline.ingestion import (
    IngestionConfig,
    build_ingestion_audit,
    ingest_hospital,
    write_ingestion_audit_bundle,
)
from asic_pipeline.inventory import build_raw_inventory, write_inventory_report_bundle
from asic_pipeline.inventory.policy import load_inventory_policy


def _complete_ingestion(
    inventory_config: object,
    raw_root: Path,
    project_root: Path,
    run_id: str,
) -> IngestionConfig:
    inventory = build_raw_inventory(inventory_config)  # type: ignore[arg-type]
    assert inventory.overall_status == "pass"
    write_inventory_report_bundle(
        inventory,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id=run_id,
    )
    config = IngestionConfig(
        inventory=inventory_config,  # type: ignore[arg-type]
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=raw_root.parent / "data",
        rows_per_batch=1,
    )
    policy = load_inventory_policy(
        project_root / "asic/config/inventory/policy.yaml"
    )
    for mapping in policy.hospital_mappings:
        ingest_hospital(
            config,
            run_id,
            mapping.canonical_hospital_id,
        )
    return config


def test_post_ingestion_audit_streams_provenance_and_writes_safe_reports(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    stay_id = "synthetic_stay_00"
    headers = [f"column_{index}" for index in range(125)]
    headers[0] = "Pseudo-ID"
    headers[1] = "Zeit_ab_Aufnahme"
    headers[121] = "ARDS_Diagnose_App"
    headers[122] = "ARDS_Diagnose_App"
    first_row = [""] * 125
    first_row[0] = stay_id
    first_row[1] = "0"
    second_row = list(first_row)
    second_row[1] = "60"
    (raw_root / "00" / f"{stay_id}.csv").write_text(
        ";".join(headers)
        + "\n"
        + ";".join(first_row)
        + "\n"
        + ";".join(second_row)
        + "\n",
        encoding="utf-8",
        newline="",
    )
    config = _complete_ingestion(
        inventory_config,
        raw_root,
        project_root,
        "audit_inventory",
    )

    result = build_ingestion_audit(config, "audit_inventory")

    assert result.overall_status == "pass", result.private_failures
    assert result.blocking_findings == ()
    assert result.private_failures == ()
    assert result.review_payload["metrics"]["audited_hospital_count"] == 8
    assert result.review_payload["metrics"]["audited_table_count"] == 16
    assert result.review_payload["metrics"]["expected_static_row_count"] == 8
    assert result.review_payload["metrics"]["expected_dynamic_row_count"] == 16
    assert all(
        check["status"] == "pass" for check in result.review_payload["checks"]
    )

    written = write_ingestion_audit_bundle(
        result,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id="ingestion_audit",
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    assert written.review_json_path is not None
    review_text = written.review_json_path.read_text(encoding="utf-8")
    assert "synthetic_stay" not in review_text
    assert "ARDS_Diagnose_App" not in review_text
    assert "dynamische_variablen_kds_patient_synthetic" not in review_text
    assert written.review_markdown_path is not None
    assert written.review_markdown_path.is_file()

    with pytest.raises(IngestionError, match="will not be overwritten"):
        write_ingestion_audit_bundle(
            result,
            inventory_config.paths.reports,  # type: ignore[attr-defined]
            run_id="ingestion_audit",
        )


def test_post_ingestion_audit_detects_manifest_hash_tampering(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    config = _complete_ingestion(
        inventory_config,
        raw_root,
        project_root,
        "tamper_inventory",
    )
    manifest_path = (
        config.output_root / "asic_UK02" / "ingestion_manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["outputs"]["dynamic"]["sha256"] = "0" * 64
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="",
    )
    manifest_path.chmod(0o600)

    result = build_ingestion_audit(config, "tamper_inventory")

    assert result.overall_status == "fail"
    assert "parquet_integrity_and_manifest_hash" in {
        finding["check"] for finding in result.blocking_findings
    }
    assert any(
        failure["hospital"] == "asic_UK02"
        and failure["table"] == "dynamic"
        for failure in result.private_failures
        if failure["check"] == "parquet_integrity_and_manifest_hash"
    )


def test_ingestion_audit_has_no_numpy_runtime_dependency(
    project_root: Path,
) -> None:
    source = (
        project_root / "src/asic_pipeline/ingestion/audit.py"
    ).read_text(encoding="utf-8")

    assert "import numpy" not in source
    assert ".to_numpy(" not in source
    assert "pc.run_end_encode(" in source
