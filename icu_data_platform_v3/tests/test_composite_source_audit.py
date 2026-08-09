from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.harmonization import (
    CompositeSourceAuditConfig,
    HarmonizationReviewConfig,
    build_composite_source_audit,
    write_composite_source_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


REVIEW_RUN = "20260805T104417Z"


def _config(tmp_path: Path, project_root: Path) -> CompositeSourceAuditConfig:
    reports = tmp_path / "reports" / "demo"
    inventory = InventoryConfig(
        dataset_context="demo",
        policy_path=project_root / "asic/config/inventory/policy.yaml",
        comparison_reference_path=None,
        paths=InventoryPaths(
            raw_root=tmp_path / "raw",
            reports=reports,
            runs=tmp_path / "runs" / "demo",
        ),
        source_path=project_root / "asic/config/datasets/demo.yaml",
    )
    ingestion = IngestionConfig(
        inventory=inventory,
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=tmp_path / "data" / "demo",
        rows_per_batch=3,
    )
    schema_tokens = SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )
    harmonization = HarmonizationReviewConfig(
        schema_tokens=schema_tokens,
        policy_path=project_root / "asic/config/harmonization/review_policy.yaml",
    )
    return CompositeSourceAuditConfig(
        harmonization_review=harmonization,
        policy_path=(
            project_root / "asic/config/harmonization/composite_source_audit.yaml"
        ),
    )


def _occurrence(
    review_id: str,
    hospital: str,
    target: str,
    physical_name: str,
    raw_name: str,
    nonempty: int,
) -> dict[str, object]:
    return {
        "review_item_id": review_id,
        "hospital": hospital,
        "table": "dynamic",
        "candidate_target": target,
        "physical_name": physical_name,
        "raw_name": raw_name,
        "raw_occurrence": 1,
        "raw_nonempty_count": nonempty,
        "all_missing_or_empty": nonempty == 0,
    }


def _write_evidence(config: CompositeSourceAuditConfig, *, unresolved: bool = False) -> None:
    private = (
        config.reports_root / "private" / "harmonization_registry_review" / REVIEW_RUN
    )
    review = config.reports_root / "review" / "harmonization_registry_review"
    private.mkdir(parents=True)
    review.mkdir(parents=True)
    (private / "harmonization_registry_review_manifest.json").write_text(
        json.dumps({"artifact_version": "0.3"}), encoding="utf-8"
    )
    (review / f"{REVIEW_RUN}.json").write_text(
        json.dumps(
            {
                "artifact_version": "0.3",
                "metrics": {"technical_blocking_finding_count": 0},
            }
        ),
        encoding="utf-8",
    )
    rows = (
        _occurrence("R0080", "asic_UK00", "ph_art", "ph_kept", "ph_kept", 4),
        _occurrence("R0129", "asic_UK00", "ph_art", "ph_empty", "ph_empty", 0),
        _occurrence(
            "R0144",
            "asic_UK00",
            "therapy_read_confirmation_utc",
            "therapy_a",
            "therapy_a_raw",
            5,
        ),
        _occurrence(
            "R0145",
            "asic_UK00",
            "therapy_read_confirmation_utc",
            "therapy_b",
            "therapy_b_raw",
            5,
        ),
        _occurrence("R0326", "asic_UK02", "ntprobnp", "nt_empty", "nt_empty", 0),
        _occurrence("R0344", "asic_UK02", "ntprobnp", "nt_kept", "nt_kept", 3),
    )
    _write_private_parquet(private / "occurrences.parquet", rows)

    ingested = config.ingested_root / "asic_UK00"
    ingested.mkdir(parents=True)
    left = [None, "0", "1", None, "", "0", "0", "1", "1"]
    right = [None, None, None, "0", "1", "0", "1", "0", "1"]
    if unresolved:
        right[-1] = "2"
    fields = [
        pa.field(
            "therapy_a",
            pa.string(),
            metadata={b"raw_name": b"therapy_a_raw", b"raw_occurrence": b"1"},
        ),
        pa.field(
            "therapy_b",
            pa.string(),
            metadata={b"raw_name": b"therapy_b_raw", b"raw_occurrence": b"1"},
        ),
    ]
    dynamic = ingested / "dynamic.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array(left, type=pa.string()), pa.array(right, type=pa.string())],
            schema=pa.schema(fields),
        ),
        dynamic,
    )
    (ingested / "ingestion_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_lossless_hospital_ingestion",
                "artifact_version": "0.2",
                "hospital": {"canonical_hospital_id": "asic_UK00"},
                "outputs": {"dynamic": {"sha256": sha256_file(dynamic)}},
            }
        ),
        encoding="utf-8",
    )


def test_composite_audit_validates_retirements_and_all_nine_pair_patterns(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_evidence(config)

    result = build_composite_source_audit(config, REVIEW_RUN)

    assert result.overall_status == "pass"
    assert result.blocking_findings == ()
    metrics = result.review_payload["metrics"]
    assert metrics["validated_retirement_count"] == 2
    assert metrics["unresolved_row_count"] == 0
    assert metrics["pair_pattern_count_total"] == 9
    assert set(metrics["pair_pattern_counts"].values()) == {1}
    assert metrics["output_value_type"] == "fixed_size_list_bool_2"
    assert result.private_manifest["composite"]["reduction_applied"] is False

    written = write_composite_source_audit_bundle(
        result, config.reports_root, "composite_output"
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    assert written.review_markdown_path is not None
    assert written.review_markdown_path.is_file()


def test_composite_audit_blocks_nonbinary_source_token(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_evidence(config, unresolved=True)

    result = build_composite_source_audit(config, REVIEW_RUN)

    assert result.overall_status == "fail"
    assert {item["check"] for item in result.blocking_findings} == {
        "composite_binary_token_domain",
        "fixed_size_list_contract_valid",
    }
    assert result.review_payload["metrics"]["unresolved_row_count"] == 1
    assert result.private_unresolved_tokens == (
        {"source_position": 1, "raw_token": "2", "count": 1},
    )
