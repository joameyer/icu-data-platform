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
    ICD10ComponentAuditConfig,
    build_icd10_component_audit,
    load_icd10_component_audit_config,
    load_icd10_component_audit_policy,
    load_reviewed_icd10_contract,
    write_icd10_component_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


NOTATION_RUN = "20260805T131735Z"
STATIC_RUN = "20260805T112746Z"
HOSPITAL_COUNTS = {
    "asic_UK00": 3676,
    "asic_UK01": 1571,
    "asic_UK02": 902,
    "asic_UK03": 1360,
    "asic_UK04": 486,
    "asic_UK06": 678,
    "asic_UK07": 2217,
    "asic_UK08": 5164,
}


def _config(tmp_path: Path, project_root: Path) -> ICD10ComponentAuditConfig:
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
    return ICD10ComponentAuditConfig(
        review,
        project_root / "asic/config/harmonization/icd10_component_audit.yaml",
    )


def _values_for(hospital: str, count: int) -> list[str]:
    comma_value = "A01, B02" if hospital in {"asic_UK06", "asic_UK08"} else "A01,B02"
    values = [comma_value] * count
    if hospital == "asic_UK01":
        values[0] = ""
        values[1:6] = ["A01"] * 5
    elif hospital == "asic_UK02":
        values[:2] = ["A01"] * 2
    elif hospital == "asic_UK03":
        values[:24] = ["nan"] * 24
    return values


def _write_input_evidence(config: ICD10ComponentAuditConfig) -> None:
    static_private = (
        config.reports_root / "private" / "static_contract_audit" / STATIC_RUN
    )
    static_private.mkdir(parents=True)
    occurrences = []
    hospital_summaries = []
    syntax_patterns = []
    for index, (hospital, count) in enumerate(HOSPITAL_COUNTS.items(), start=1):
        values = _values_for(hospital, count)
        nonempty = sum(value != "" for value in values)
        missing = 24 if hospital == "asic_UK03" else 0
        single = 5 if hospital == "asic_UK01" else 2 if hospital == "asic_UK02" else 0
        comma = nonempty - missing - single
        occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": hospital,
                "table": "static",
                "physical_name": "icd_source",
                "raw_name": "ICD-10_Codes",
                "raw_occurrence": 1,
                "candidate_target": "icd10_codes",
                "scan_row_count": count,
                "scan_source_null_count": 0,
                "scan_literal_empty_count": count - nonempty,
                "scan_nonempty_count": nonempty,
            }
        )
        field = pa.field(
            "icd_source",
            pa.large_string(),
            metadata={b"raw_name": b"ICD-10_Codes", b"raw_occurrence": b"1"},
        )
        table = pa.Table.from_arrays(
            [pa.array(values, type=pa.large_string())], schema=pa.schema([field])
        )
        hospital_dir = config.ingested_root / hospital
        hospital_dir.mkdir(parents=True)
        static_path = hospital_dir / "static.parquet"
        pq.write_table(table, static_path)
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
        hospital_summaries.append(
            {
                "hospital": hospital,
                "row_count": count,
                "nonempty_count": nonempty,
                "textual_missing_candidate_count": missing,
                "single_code_candidate_count": single,
            }
        )
        if comma:
            syntax_patterns.append(
                {
                    "hospital": hospital,
                    "syntax_signature": (
                        "comma+whitespace"
                        if hospital in {"asic_UK06", "asic_UK08"}
                        else "comma"
                    ),
                    "cell_count": comma,
                }
            )
        if single:
            syntax_patterns.append(
                {
                    "hospital": hospital,
                    "syntax_signature": "single_code_candidate",
                    "cell_count": single,
                }
            )
        if missing:
            syntax_patterns.append(
                {
                    "hospital": hospital,
                    "syntax_signature": "textual_missing_candidate",
                    "cell_count": missing,
                }
            )
    _write_private_parquet(static_private / "occurrences.parquet", tuple(occurrences))
    (static_private / "static_contract_audit_manifest.json").write_text(
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

    notation_private = (
        config.reports_root / "private" / "icd10_notation_audit" / NOTATION_RUN
    )
    notation_private.mkdir(parents=True)
    (notation_private / "icd10_notation_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_notation_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "inputs": {"static_contract_audit_run_id": STATIC_RUN},
            }
        ),
        encoding="utf-8",
    )
    notation_review = config.reports_root / "review" / "icd10_notation_audit"
    notation_review.mkdir(parents=True)
    (notation_review / f"{NOTATION_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_notation_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "metrics": {
                    "audited_hospital_count": 8,
                    "scanned_row_count": 16054,
                    "nonempty_cell_count": 16053,
                    "textual_missing_candidate_count": 24,
                    "single_code_candidate_count": 7,
                    "nonstring_source_cell_count": 0,
                    "technical_blocking_finding_count": 0,
                },
                "hospital_summaries": hospital_summaries,
                "syntax_patterns": syntax_patterns,
            }
        ),
        encoding="utf-8",
    )


def test_reviewed_icd10_contract_is_evidence_bound_and_parser_disabled(
    project_root: Path,
) -> None:
    contract = load_reviewed_icd10_contract(
        project_root
        / "asic/config/harmonization/reviewed_icd10_contract_0_1.yaml"
    )

    assert contract.version == "0.1"
    assert contract.source_text_target == "icd10_codes_source_text"
    assert contract.list_target == "icd10_codes"
    assert contract.list_value_type == "list_large_string"
    assert contract.approved_textual_missing_cell_count == 24
    missing = contract.missing_decision_for("asic_UK03")
    assert missing is not None
    assert missing.matches("nan") is True
    assert missing.matches(" NaN ") is True
    assert contract.missing_decision_for("asic_UK02") is None
    with pytest.raises(HarmonizationError, match="remains blocked"):
        contract.require_parser_active()


def test_icd10_component_audit_reproduces_complete_clean_grammar(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)

    result = build_icd10_component_audit(config, NOTATION_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    metrics = result.review_payload["metrics"]
    assert metrics["scanned_row_count"] == 16054
    assert metrics["approved_textual_missing_cell_count"] == 24
    assert metrics["parsed_cell_count"] == 16029
    assert metrics["comma_delimited_cell_count"] == 16022
    assert metrics["single_component_cell_count"] == 7
    assert metrics["multi_component_cell_count"] == 16022
    assert metrics["component_count"] == 32051
    assert metrics["code_candidate_component_count"] == 32051
    assert metrics["unexpected_component_count"] == 0
    assert metrics["empty_component_count"] == 0
    assert metrics["components_with_trimmed_whitespace"] == 5842
    assert metrics["technical_blocking_finding_count"] == 0
    assert {item["check"] for item in result.blocking_findings} == {
        "icd10_component_evidence_approved",
        "icd10_canonical_list_contract_activated",
    }
    serialized = json.dumps(result.review_payload)
    assert "A01" not in serialized
    assert '"nan"' not in serialized
    assert "ICD-10_Codes" not in serialized

    written = write_icd10_component_audit_bundle(
        result, config.reports_root, "component_output"
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_icd10_component_audit_bundle(
            result, config.reports_root, "component_output"
        )


def test_icd10_component_audit_surfaces_empty_and_unexpected_components(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)
    path = config.ingested_root / "asic_UK00" / "static.parquet"
    table = pq.read_table(path)
    values = table.column(0).to_pylist()
    values[0] = "A01,,protected-unexpected"
    field = table.schema.field(0)
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array(values, type=pa.large_string())], schema=pa.schema([field])
        ),
        path,
    )
    manifest_path = path.parent / "ingestion_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["outputs"]["static"]["sha256"] = sha256_file(path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = build_icd10_component_audit(config, NOTATION_RUN)

    assert result.review_payload["metrics"]["empty_component_count"] == 1
    assert result.review_payload["metrics"]["unexpected_component_count"] == 1
    assert {item["check"] for item in result.blocking_findings}.issuperset(
        {
            "icd10_empty_components_resolved",
            "icd10_unexpected_component_syntax_reviewed",
        }
    )
    assert any(
        row.get("raw_component") == "protected-unexpected"
        for row in result.private_examples
    )
    assert "protected-unexpected" not in json.dumps(result.review_payload)


def test_icd10_component_policy_and_shipped_configs_are_fail_closed(
    project_root: Path,
) -> None:
    for context in ("demo", "production"):
        config = load_icd10_component_audit_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_icd10_component_audit_policy(config.policy_path)
        assert policy.version == "0.1"
        assert policy.notation_audit_basis_run_id == NOTATION_RUN
        assert policy.rows_per_batch == 10000
        assert policy.reviewed_contract_path.name == (
            "reviewed_icd10_contract_0_1.yaml"
        )
