from __future__ import annotations

from pathlib import Path

from asic_pipeline.inventory import build_raw_inventory


def test_filename_identifier_disagreement_is_blocking(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    dynamic = raw_root / "02" / "dynamische_variablen_kds_patient_synthetic_stay_02.csv"
    dynamic.write_text(
        "Pseudo-ID;Zeit_ab_Aufnahme;I:E\n"
        "different_identifier;0;1:2\n",
        encoding="utf-8",
        newline="",
    )
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    blocking = {item["check"] for item in result.blocking_findings}
    assert "filename_and_in_file_identifiers_agree" in blocking


def test_unknown_file_is_blocking_but_not_exposed_in_review(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    protected_name = "unexpected_protected_stay_name.txt"
    (raw_root / "02" / protected_name).write_text("unknown\n", encoding="utf-8")
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    blocking = {item["check"] for item in result.blocking_findings}
    assert "unknown_or_unclassified_files" in blocking
    assert protected_name not in str(result.review_payload)


def test_ambiguous_delimiter_is_blocking(
    synthetic_inventory: tuple[object, Path],
) -> None:
    config, raw_root = synthetic_inventory
    static = raw_root / "03" / "static.csv"
    static.write_text("Pseudo-ID;age,value\na;b,c\n", encoding="utf-8", newline="")
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    blocking = {item["check"] for item in result.blocking_findings}
    assert "csv_encoding_dialect_and_structure" in blocking


def test_source_file_symbolic_link_is_not_followed(
    synthetic_inventory: tuple[object, Path], tmp_path: Path
) -> None:
    config, raw_root = synthetic_inventory
    target = tmp_path / "outside_source.csv"
    target.write_text("protected content that must not be read\n", encoding="utf-8")
    link = raw_root / "02" / "unexpected_link.txt"
    link.symlink_to(target)
    result = build_raw_inventory(config)  # type: ignore[arg-type]

    blocking = {item["check"] for item in result.blocking_findings}
    assert "symbolic_link_sources" in blocking
    record = next(
        item for item in result.private_tables["files"] if item["is_symbolic_link"]
    )
    assert record["sha256"] is None
    assert record["size_bytes"] is None
