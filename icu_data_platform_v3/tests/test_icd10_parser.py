from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    ICD10HarmonizationMetrics,
    harmonize_reviewed_static_icd10_batch,
    load_reviewed_icd10_contract,
    load_reviewed_icd10_private_evidence,
    require_full_reviewed_icd10_evidence_counts,
)
from asic_pipeline.inventory.report import _write_private_parquet


DETAIL_RUN = "20260805T135859Z"
COMPONENT_RUN = "20260805T134245Z"


def _contract(project_root: Path):
    return load_reviewed_icd10_contract(
        project_root
        / "asic/config/harmonization/reviewed_icd10_contract_0_2.yaml"
    )


def _write_private_evidence(tmp_path: Path) -> Path:
    context_root = tmp_path / "reports" / "demo"
    private = (
        context_root
        / "private"
        / "icd10_component_detail_audit"
        / DETAIL_RUN
    )
    private.mkdir(parents=True)
    examples = (
        {
            "hospital": "asic_UK03",
            "detail_class": "empty_trailing",
            "raw_source_text": "A01,",
            "cell_count": 1,
        },
        {
            "hospital": "asic_UK08",
            "detail_class": "incomplete_component_candidate",
            "raw_component": "protected-incomplete",
            "cell_count": 1,
        },
    )
    path = private / "examples.parquet"
    _write_private_parquet(path, examples)
    (private / "icd10_component_detail_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_component_detail_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "inputs": {"icd10_component_audit_run_id": COMPONENT_RUN},
            }
        ),
        encoding="utf-8",
    )
    review = context_root / "review" / "icd10_component_detail_audit"
    review.mkdir(parents=True)
    (review / f"{DETAIL_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_component_detail_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "inputs": {"icd10_component_audit_run_id": COMPONENT_RUN},
                "metrics": {
                    "incomplete_component_candidate_count": 1,
                    "technical_blocking_finding_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_reviewed_icd10_contract_v02_activates_only_bounded_parser(
    project_root: Path,
) -> None:
    contract = _contract(project_root)

    assert contract.version == "0.2"
    assert contract.preserve_source_order is True
    assert contract.preserve_duplicates is False
    assert contract.allow_parser_activation is True
    assert contract.allow_synthetic_batch_transform is True
    assert contract.allow_private_review_evidence_reads is True
    assert contract.allow_harmonized_artifact_writes is False
    assert contract.allow_hospital_concatenation is False
    assert contract.allow_union_schema_freeze is False
    assert contract.approved_trailing_empty_count("asic_UK03") == 20
    assert contract.approved_trailing_empty_count("asic_UK08") == 6
    assert contract.approved_trailing_empty_count("asic_UK02") == 0
    contract.require_parser_active()

    old = load_reviewed_icd10_contract(
        project_root
        / "asic/config/harmonization/reviewed_icd10_contract_0_1.yaml"
    )
    with pytest.raises(HarmonizationError, match="remains blocked"):
        old.require_parser_active()


def test_private_incomplete_component_evidence_stays_owner_only(
    tmp_path: Path,
    project_root: Path,
) -> None:
    contract = _contract(project_root)
    path = _write_private_evidence(tmp_path)

    evidence = load_reviewed_icd10_private_evidence(path, contract)

    assert evidence.audit_run_id == DETAIL_RUN
    assert evidence.approves_incomplete_component(
        "asic_UK08", "protected-incomplete"
    )
    assert not evidence.approves_incomplete_component(
        "asic_UK07", "protected-incomplete"
    )

    review_path = (
        tmp_path
        / "reports/demo/review/icd10_component_detail_audit"
        / f"{DETAIL_RUN}.json"
    )
    review = json.loads(review_path.read_text(encoding="utf-8"))
    review["metrics"]["incomplete_component_candidate_count"] = 2
    review_path.write_text(json.dumps(review), encoding="utf-8")
    with pytest.raises(HarmonizationError) as captured:
        load_reviewed_icd10_private_evidence(path, contract)
    assert "protected-incomplete" not in str(captured.value)


def test_icd10_parser_preserves_source_and_applies_approved_list_rules(
    tmp_path: Path,
    project_root: Path,
) -> None:
    contract = _contract(project_root)
    evidence = load_reviewed_icd10_private_evidence(
        _write_private_evidence(tmp_path), contract
    )
    raw = "A41, E66,E66,"

    result = harmonize_reviewed_static_icd10_batch(
        pa.array([raw, "nan", "", None, "A41,"], type=pa.large_string()),
        "asic_UK03",
        contract,
        evidence,
    )

    assert result.source_text.type == pa.large_string()
    assert result.source_text.to_pylist() == [raw, None, None, None, "A41,"]
    assert result.codes.type == pa.list_(pa.large_string())
    assert result.codes.to_pylist() == [
        ["A41", "E66"],
        None,
        None,
        None,
        ["A41"],
    ]
    assert result.parse_status.to_pylist() == [
        "comma_split+trailing_empty_removed+ordered_duplicates_removed",
        "approved_textual_missing",
        "literal_empty_missing",
        "source_schema_absent",
        "comma_split+trailing_empty_removed",
    ]
    metrics = result.metrics
    assert metrics.input_cell_count == 5
    assert metrics.parsed_cell_count == 2
    assert metrics.approved_textual_missing_count == 1
    assert metrics.source_component_count == 6
    assert metrics.valid_component_count_before_deduplication == 4
    assert metrics.retained_unique_valid_component_count == 3
    assert metrics.trailing_empty_component_removed_count == 2
    assert metrics.duplicate_component_removed_count == 1
    assert metrics.component_accounting_is_conserved is True
    result.require_resolved()


def test_icd10_parser_uses_private_incomplete_rule_and_blocks_every_other_one(
    tmp_path: Path,
    project_root: Path,
) -> None:
    contract = _contract(project_root)
    evidence = load_reviewed_icd10_private_evidence(
        _write_private_evidence(tmp_path), contract
    )
    reviewed_raw = "A41, protected-incomplete,A41"
    unknown_raw = "A41,protected-other"

    result = harmonize_reviewed_static_icd10_batch(
        pa.array([reviewed_raw, unknown_raw], type=pa.large_string()),
        "asic_UK08",
        contract,
        evidence,
    )

    assert result.source_text.to_pylist() == [reviewed_raw, unknown_raw]
    assert result.codes.to_pylist() == [["A41"], None]
    assert result.parse_status.to_pylist() == [
        "comma_split+reviewed_incomplete_removed+ordered_duplicates_removed",
        "unresolved",
    ]
    assert result.metrics.reviewed_incomplete_component_removed_count == 1
    assert result.metrics.duplicate_component_removed_count == 1
    assert result.metrics.unapproved_noncode_component_count == 1
    assert result.metrics.unresolved_cell_count == 1
    assert result.metrics.component_accounting_is_conserved is True
    with pytest.raises(HarmonizationError) as captured:
        result.require_resolved()
    assert "protected-incomplete" not in str(captured.value)
    assert "protected-other" not in str(captured.value)

    wrong_hospital = harmonize_reviewed_static_icd10_batch(
        pa.array([reviewed_raw], type=pa.large_string()),
        "asic_UK07",
        contract,
        evidence,
    )
    assert wrong_hospital.codes.to_pylist() == [None]
    assert wrong_hospital.metrics.unresolved_cell_count == 1


def test_icd10_parser_blocks_unreviewed_empty_positions_and_empty_lists(
    tmp_path: Path,
    project_root: Path,
) -> None:
    contract = _contract(project_root)
    evidence = load_reviewed_icd10_private_evidence(
        _write_private_evidence(tmp_path), contract
    )

    result = harmonize_reviewed_static_icd10_batch(
        pa.array([",A41", "A41,,E66", ",", "A41,"], type=pa.large_string()),
        "asic_UK03",
        contract,
        evidence,
    )

    assert result.codes.to_pylist() == [None, None, None, ["A41"]]
    assert result.metrics.unapproved_empty_component_count == 3
    assert result.metrics.unresolved_cell_count == 3
    assert result.metrics.component_accounting_is_conserved is True

    unapproved_hospital = harmonize_reviewed_static_icd10_batch(
        pa.array(["A41,"], type=pa.large_string()),
        "asic_UK02",
        contract,
        evidence,
    )
    assert unapproved_hospital.codes.to_pylist() == [None]
    assert unapproved_hospital.metrics.unresolved_cell_count == 1


def test_full_icd10_evidence_validator_is_exact_and_fail_closed(
    project_root: Path,
) -> None:
    contract = _contract(project_root)
    uk03 = ICD10HarmonizationMetrics(
        hospital="asic_UK03",
        input_cell_count=16048,
        approved_textual_missing_count=24,
        parsed_cell_count=16024,
        source_component_count=465989,
        valid_component_count_before_deduplication=465969,
        retained_unique_valid_component_count=369464,
        trailing_empty_component_removed_count=20,
        cells_with_trailing_empty_removed_count=20,
        duplicate_component_removed_count=96505,
    )
    uk08 = ICD10HarmonizationMetrics(
        hospital="asic_UK08",
        input_cell_count=6,
        parsed_cell_count=6,
        source_component_count=13,
        valid_component_count_before_deduplication=6,
        retained_unique_valid_component_count=6,
        trailing_empty_component_removed_count=6,
        cells_with_trailing_empty_removed_count=6,
        reviewed_incomplete_component_removed_count=1,
        cells_with_reviewed_incomplete_removed_count=1,
    )

    require_full_reviewed_icd10_evidence_counts((uk03, uk08), contract)

    uk08.reviewed_incomplete_component_removed_count = 0
    with pytest.raises(HarmonizationError, match="do not match reviewed evidence"):
        require_full_reviewed_icd10_evidence_counts((uk03, uk08), contract)
