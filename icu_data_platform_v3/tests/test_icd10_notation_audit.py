from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    ICD10NotationAuditConfig,
    build_icd10_notation_audit,
    load_icd10_notation_audit_config,
    load_icd10_notation_audit_policy,
    write_icd10_notation_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


STATIC_RUN = "20260805T112746Z"
HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)


def _config(tmp_path: Path, project_root: Path) -> ICD10NotationAuditConfig:
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
        rows_per_batch=10,
    )
    schema = SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )
    review = HarmonizationReviewConfig(
        schema_tokens=schema,
        policy_path=project_root / "asic/config/harmonization/review_policy.yaml",
    )
    return ICD10NotationAuditConfig(
        harmonization_review=review,
        policy_path=project_root
        / "asic/config/harmonization/icd10_notation_audit.yaml",
    )


def _write_static(path: Path) -> None:
    field = pa.field(
        "icd_source",
        pa.large_string(),
        metadata={b"raw_name": b"ICD-10_Codes", b"raw_occurrence": b"1"},
    )
    table = pa.Table.from_arrays(
        [pa.array(["A01", "A01;B02", "nan", ""], type=pa.large_string())],
        schema=pa.schema([field]),
    )
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)


def _write_input_evidence(config: ICD10NotationAuditConfig) -> None:
    private = (
        config.reports_root / "private" / "static_contract_audit" / STATIC_RUN
    )
    private.mkdir(parents=True)
    occurrences = []
    for index, hospital in enumerate(HOSPITALS, start=1):
        occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": hospital,
                "table": "static",
                "physical_name": "icd_source",
                "raw_name": "ICD-10_Codes",
                "raw_occurrence": 1,
                "candidate_target": "icd10_codes",
                "scan_row_count": 4,
                "scan_source_null_count": 0,
                "scan_literal_empty_count": 1,
                "scan_nonempty_count": 3,
            }
        )
        hospital_dir = config.ingested_root / hospital
        static_path = hospital_dir / "static.parquet"
        _write_static(static_path)
        (hospital_dir / "ingestion_manifest.json").write_text(
            json.dumps(
                {
                    "artifact": "asic_v3_lossless_hospital_ingestion",
                    "artifact_version": "0.2",
                    "hospital": {"canonical_hospital_id": hospital},
                    "outputs": {"static": {"sha256": sha256_file(static_path)}},
                }
            ),
            encoding="utf-8",
        )
    _write_private_parquet(private / "occurrences.parquet", occurrences)
    (private / "static_contract_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_static_contract_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "selected_occurrence_count": len(occurrences),
            }
        ),
        encoding="utf-8",
    )
    review_dir = config.reports_root / "review" / "static_contract_audit"
    review_dir.mkdir(parents=True)
    (review_dir / f"{STATIC_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_static_contract_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "metrics": {"technical_blocking_finding_count": 0},
            }
        ),
        encoding="utf-8",
    )


def test_icd10_notation_audit_is_lossless_private_and_review_gated(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)

    result = build_icd10_notation_audit(config, STATIC_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    assert result.review_payload["metrics"]["audited_hospital_count"] == 8
    assert result.review_payload["metrics"]["scanned_row_count"] == 32
    assert result.review_payload["metrics"]["nonempty_cell_count"] == 24
    assert result.review_payload["metrics"]["textual_missing_candidate_count"] == 8
    assert result.review_payload["metrics"]["single_code_candidate_count"] == 8
    assert result.review_payload["metrics"]["nonstring_source_cell_count"] == 0
    assert result.review_payload["proposal"]["source_preservation_target"] == (
        "icd10_codes_source_text"
    )
    serialized = json.dumps(result.review_payload)
    assert "A01" not in serialized
    assert '"nan"' not in serialized
    assert "ICD-10_Codes" not in serialized
    assert any(row["raw_token"] == "A01" for row in result.private_examples)

    written = write_icd10_notation_audit_bundle(
        result,
        config.reports_root,
        "icd10_output",
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_icd10_notation_audit_bundle(
            result,
            config.reports_root,
            "icd10_output",
        )


def test_icd10_notation_audit_surfaces_changed_ingested_hash(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)
    path = config.ingested_root / "asic_UK00" / "static.parquet"
    table = pq.read_table(path)
    pq.write_table(table.replace_schema_metadata({b"changed": b"true"}), path)

    result = build_icd10_notation_audit(config, STATIC_RUN)

    assert result.has_technical_failure is True
    assert "ingested_static_hashes_match_manifests" in {
        row["check"] for row in result.technical_blocking_findings
    }


def test_icd10_policy_and_shipped_configs_are_fail_closed(project_root: Path) -> None:
    for context in ("demo", "production"):
        config = load_icd10_notation_audit_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_icd10_notation_audit_policy(config.policy_path)
        assert policy.version == "0.1"
        assert policy.static_contract_audit_basis_run_id == STATIC_RUN
        assert policy.rows_per_batch == 10000
        assert policy.reviewed_static_decisions_path.name == (
            "reviewed_static_decisions_0_3.yaml"
        )
