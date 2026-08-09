from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from asic_pipeline.derivation import driving_pressure_review as review_module
from asic_pipeline.derivation.driving_pressure_review import (
    DrivingPressureSemanticsConfig,
    DrivingPressureSemanticsPolicy,
    run_driving_pressure_semantics_review,
    scan_driving_pressure_semantics,
)
from asic_pipeline.derivation.review import CleanedReleaseInput


def _write_dynamic(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "hospital_id": [
                    "asic_UK00",
                    "asic_UK00",
                    "asic_UK00",
                    "asic_UK00",
                    "asic_UK01",
                    "asic_UK01",
                    "asic_UK01",
                ],
                "insp_pressure": [20.0, 20.0, 4.0, 70.0, 10.0, 5.0, None],
                "peep": [5.0, 5.0, 5.0, 5.0, 10.0, None, 5.0],
                "delta_p_reported": [15.0, 20.0, 4.0, 65.0, 0.0, 5.0, None],
            }
        ),
        path,
    )


def _policy(path: Path) -> DrivingPressureSemanticsPolicy:
    return DrivingPressureSemanticsPolicy(
        version="0.1",
        cleaned_release_id="20260806T114234Z",
        expected_dynamic_rows=7,
        rows_per_batch=2,
        agreement_tolerances=(0.01, 0.1, 1.0),
        candidate_minimum=0.0,
        candidate_maximum=60.0,
        private_directory_name="driving_pressure_semantics",
        review_directory_name="driving_pressure_semantics",
        private_artifact_version="0.1",
        review_artifact_version="0.1",
        source_path=path,
    )


def test_driving_pressure_scan_compares_both_interpretations_by_hospital(
    tmp_path: Path,
) -> None:
    dynamic_path = tmp_path / "dynamic.parquet"
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text("test: true\n", encoding="utf-8")
    _write_dynamic(dynamic_path)

    evidence = scan_driving_pressure_semantics(dynamic_path, _policy(policy_path))

    assert evidence["row_count"] == 7
    overall = evidence["overall"]["counts"]
    assert overall["both_input_count"] == 5
    assert overall["insp_minus_peep_below_candidate_range_count"] == 1
    assert overall["insp_minus_peep_above_candidate_range_count"] == 1
    assert overall["reported_overlap_count"] == 5
    assert overall["insp_minus_peep_within_0_01_count"] == 3
    assert overall["insp_unchanged_within_0_01_count"] == 2
    assert overall["primary_only_insp_minus_peep_matches_count"] == 3
    assert overall["primary_only_insp_unchanged_matches_count"] == 2
    assert overall["negative_subtraction_reported_matches_insp_count"] == 1
    assert overall["high_subtraction_reported_matches_subtraction_count"] == 1
    assert evidence["hospitals"]["asic_UK00"]["counts"]["row_count"] == 4
    assert evidence["hospitals"]["asic_UK01"]["counts"]["row_count"] == 3


def test_driving_pressure_review_writes_aggregate_evidence_only(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    dynamic_path = tmp_path / "input/dynamic.parquet"
    _write_dynamic(dynamic_path)
    manifest_path = tmp_path / "input/release_manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    source = CleanedReleaseInput(
        release_directory=dynamic_path.parent,
        release_manifest_path=manifest_path,
        release_manifest={},
        source_files={"dynamic": dynamic_path},
        schemas={},
    )
    monkeypatch.setattr(review_module, "load_cleaned_release_input", lambda *_: source)
    expected = {
        "both_input_count": 5,
        "absolute_interpretation_below_zero_count": 1,
        "absolute_interpretation_above_sixty_count": 1,
        "reported_overlap_count": 5,
        "absolute_interpretation_within_0_01_count": 3,
    }
    monkeypatch.setattr(review_module, "EXPECTED_PRIOR_EVIDENCE", expected)

    raw = yaml.safe_load(
        (
            project_root
            / "asic/config/derivation/driving_pressure_semantics_review.yaml"
        ).read_text(encoding="utf-8")
    )
    raw["input"]["expected_dynamic_rows"] = 7
    raw["prior_evidence_cross_check"] = expected
    policy_path = tmp_path / "driving_pressure_semantics_review.yaml"
    policy_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    base_policy = project_root / "asic/config/derivation/contract_review.yaml"
    config = DrivingPressureSemanticsConfig(
        derivation_review=SimpleNamespace(
            dataset_context="production",
            reports_root=tmp_path / "reports",
            policy_path=base_policy,
        ),
        policy_path=policy_path,
    )

    result = run_driving_pressure_semantics_review(config, "dp001")

    assert result.overall_status == "pending_human_review"
    assert result.technical_blocking_findings == ()
    payload = yaml.safe_load(result.review_json_path.read_text(encoding="utf-8"))
    assert payload["evidence"]["row_count"] == 7
    assert payload["clinical_data_written"] is False
    assert payload["derived_formula_activated"] is False
    assert "stay_id_global" not in result.review_json_path.read_text(encoding="utf-8")
    assert "Hospital comparison" in result.review_markdown_path.read_text(
        encoding="utf-8"
    )
