from __future__ import annotations

import json
from pathlib import Path
import zipfile

from asic_pipeline.inventory import (
    build_raw_anomaly_audit,
    build_raw_inventory,
    write_inventory_report_bundle,
    write_raw_anomaly_audit_bundle,
)


def test_raw_anomaly_audit_preserves_both_occurrences_and_compares_zip(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    stay_id = "synthetic_stay_00"
    dynamic_name = f"{stay_id}.csv"
    dynamic_path = raw_root / "00" / dynamic_name
    dynamic_text = (
        "Pseudo-ID;Zeit_ab_Aufnahme;ARDS_Diagnose_App;ARDS_Diagnose_App\n"
        f"{stay_id};0;yes;\n"
        f"{stay_id};60;;no\n"
        f"{stay_id};120;yes;yes\n"
        f"{stay_id};180;yes;no\n"
        f"{stay_id};240;;\n"
    )
    dynamic_path.write_text(dynamic_text, encoding="utf-8", newline="")

    archive_name = "synthetic_control_export.zip"
    archive_path = raw_root / "00" / archive_name
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(dynamic_name, dynamic_text.replace("\n", "\r\n"))
        archive.writestr(
            "static.csv", (raw_root / "00" / "static.csv").read_bytes()
        )
        archive.writestr("nested_export.zip", b"synthetic nested archive")

    inventory = build_raw_inventory(config)  # type: ignore[arg-type]
    written_inventory = write_inventory_report_bundle(
        inventory,
        config.paths.reports,  # type: ignore[attr-defined]
        run_id="inventory_for_anomaly_test",
    )
    assert written_inventory.overall_status == "fail"

    audit = build_raw_anomaly_audit(  # type: ignore[arg-type]
        config,
        "inventory_for_anomaly_test",
    )
    duplicate = audit.review_payload["duplicate_column_audit"]
    assert duplicate["file_count"] == 1
    assert duplicate["data_record_count"] == 5
    assert duplicate["accounted_record_count"] == 5
    assert duplicate["category_counts"] == {
        "first_only": 1,
        "second_only": 1,
        "equal_nonempty": 1,
        "conflicting_nonempty": 1,
        "both_empty": 1,
    }
    assert duplicate["second_occurrence_nonempty_count"] == 3

    archive = audit.review_payload["archive_comparison"]
    assert archive["classification_counts"] == {
        "archive_only_nested_zip": 1,
        "equivalent_after_csv_parsing": 1,
        "exact_byte_duplicate": 1,
    }
    assert archive["flat_only_count"] == 1
    assert archive["archive_only_count"] == 1
    checks = {check["name"]: check for check in audit.review_payload["checks"]}
    assert checks["hospital_mapping_contract_approved"]["status"] == "pass"
    assert checks["provisional_archive_owner_confirmation"]["status"] == "fail"
    assert checks["zip_overlap_content_equivalent"]["severity"] == "advisory"
    assert checks["zip_and_flat_file_sets_equal"]["severity"] == "advisory"
    assert {finding["check"] for finding in audit.blocking_findings} == {
        "v2_second_occurrence_all_missing_revalidated",
        "provisional_archive_owner_confirmation",
    }

    review_text = json.dumps(audit.review_payload)
    private_text = json.dumps(audit.private_manifest)
    assert stay_id not in review_text
    assert archive_name not in review_text
    assert "ARDS_Diagnose_App" not in review_text
    assert archive_name in private_text
    assert "ARDS_Diagnose_App" in private_text

    written_audit = write_raw_anomaly_audit_bundle(
        audit,
        config.paths.reports,  # type: ignore[attr-defined]
        run_id="synthetic_anomaly_audit",
    )
    assert written_audit.private_report_directory is not None
    assert written_audit.review_json_path is not None
    assert written_audit.private_report_directory.stat().st_mode & 0o777 == 0o700
    assert "synthetic_stay" not in written_audit.review_json_path.read_text(
        encoding="utf-8"
    )
