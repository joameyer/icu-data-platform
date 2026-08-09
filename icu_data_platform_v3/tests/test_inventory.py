from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from asic_pipeline.inventory import build_raw_inventory, write_inventory_report_bundle


def test_inventory_accepts_approved_hospital_mapping_contract(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, _ = synthetic_inventory
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    assert result.overall_status == "pass"
    assert result.blocking_findings == ()
    mapping_check = next(
        check
        for check in result.review_payload["checks"]
        if check["name"] == "hospital_mapping_contract_approved"
    )
    assert mapping_check["status"] == "pass"
    assert result.review_payload["metrics"]["checkpoint_directory_count_excluded"] == 1
    assert result.review_payload["metrics"][
        "untrusted_pooled_directory_count_excluded"
    ] == 1
    assert result.review_payload["static_candidate_comparisons"][0][
        "classification"
    ] == "exact_byte_duplicate"
    assert result.review_payload["hospital_mapping_evidence"]["01"][
        "mapping_status"
    ] == "approved_by_data_owner"
    assert result.review_payload["hospital_mapping_evidence"]["01"][
        "canonical_hospital_id"
    ] == "asic_UK01"
    assert result.review_payload["hospital_mapping_evidence"]["01"][
        "cohort_action"
    ] == "include"


def test_checkpoint_and_pooled_contents_are_never_enumerated(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, _ = synthetic_inventory
    result = build_raw_inventory(config)  # type: ignore[arg-type]
    private_text = json.dumps(result.private_manifest)
    file_text = json.dumps(result.private_tables["files"])

    assert "protected_stay_name.csv" not in private_text
    assert "protected_stay_name.csv" not in file_text
    assert "lossy.parquet" not in private_text
    assert "lossy.parquet" not in file_text
    assert ".ipynb_checkpoints" in private_text
    assert "pooled" in private_text


def test_review_report_contains_no_source_filenames_or_stay_values(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, _ = synthetic_inventory
    result = build_raw_inventory(config)  # type: ignore[arg-type]
    review = json.dumps(result.review_payload, ensure_ascii=False)
    private = json.dumps(result.private_tables["files"], ensure_ascii=False)

    assert "synthetic_stay_" not in review
    assert "dynamische_variablen_kds_patient" not in review
    assert "synthetic_stay_" in private
    assert "static.csv" in private


def test_malformed_header_value_is_not_copied_to_review(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    protected_header_value = "synthetic_stay_value_in_malformed_header"
    (raw_root / "03" / "static.csv").write_text(
        f"{protected_header_value};age\nvalue;42\n",
        encoding="utf-8",
        newline="",
    )
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    assert protected_header_value not in json.dumps(result.review_payload)
    assert protected_header_value in json.dumps(result.private_tables)


def test_reviewed_uk00_duplicate_header_is_accepted_positionally(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    stay_id = "synthetic_stay_00"
    headers = [f"column_{index}" for index in range(125)]
    headers[0] = "Pseudo-ID"
    headers[1] = "Zeit_ab_Aufnahme"
    headers[121] = "ARDS_Diagnose_App"
    headers[122] = "ARDS_Diagnose_App"
    row = [""] * 125
    row[0] = stay_id
    row[1] = "0"
    row[121] = "1"
    path = raw_root / "00" / f"{stay_id}.csv"
    path.write_text(
        ";".join(headers) + "\n" + ";".join(row) + "\n",
        encoding="utf-8",
        newline="",
    )

    result = build_raw_inventory(config)  # type: ignore[arg-type]
    record = next(
        item
        for item in result.private_tables["files"]
        if item["hospital_folder"] == "00"
        and item["classification"] == "dynamic_candidate"
    )
    assert record["inspection_status"] == "complete"
    assert record["reviewed_duplicate_header"]["rule_id"] == (
        "uk00_ards_diagnose_app_duplicate"
    )
    assert result.review_payload["metrics"][
        "reviewed_duplicate_header_file_count"
    ] == 1
    blocking = {finding["check"] for finding in result.blocking_findings}
    assert "csv_encoding_dialect_and_structure" not in blocking


def test_report_bundle_is_immutable_and_private_evidence_is_owner_only(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, _ = synthetic_inventory
    result = build_raw_inventory(config)  # type: ignore[arg-type]
    written = write_inventory_report_bundle(
        result,
        config.paths.reports,  # type: ignore[attr-defined]
        run_id="synthetic_run",
    )

    assert written.private_report_directory is not None
    assert written.review_json_path is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    review_text = written.review_json_path.read_text(encoding="utf-8")
    assert "synthetic_stay_" not in review_text
    files_path = written.private_report_directory / "files.parquet"
    assert files_path.is_file()
    files_table = pq.read_table(files_path)
    assert {
        "csv",
        "delimiter",
        "encoding",
        "identifiers",
        "relative_path",
        "sha256",
    }.issubset(files_table.schema.names)
    complete_rows = [
        row
        for row in files_table.to_pylist()
        if row["inspection_status"] == "complete"
    ]
    assert complete_rows
    assert all(row["csv"] is not None for row in complete_rows)
    assert all(row["identifiers"] is not None for row in complete_rows)

    try:
        write_inventory_report_bundle(
            result,
            config.paths.reports,  # type: ignore[attr-defined]
            run_id="synthetic_run",
        )
    except Exception as exc:
        assert "will not be overwritten" in str(exc)
    else:
        raise AssertionError("An immutable report run was overwritten")
