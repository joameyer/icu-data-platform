from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.cleaning import range_evidence_0_2 as module
from asic_pipeline.cleaning.range_evidence_0_2 import (
    HOSPITALS,
    VARIABLES,
    CleaningRangeEvidence02Config,
    RangeState,
    load_cleaning_range_evidence_0_2_config,
    load_cleaning_range_evidence_0_2_policy,
    run_cleaning_range_evidence_0_2,
)
from asic_pipeline.inventory.hashing import sha256_file


def test_shipped_range_evidence_policy_is_complete_and_read_only(
    project_root: Path,
) -> None:
    config = load_cleaning_range_evidence_0_2_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_cleaning_range_evidence_0_2_policy(config.policy_path)

    assert policy.release_id == "20260807T112402Z"
    assert policy.contract_version == "0.2"
    assert policy.expected_rows == 24069379
    assert policy.rows_per_batch == 50000
    assert policy.factors == (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0)
    assert policy.sofa_without_gcs_semantic_max == 20.0


def test_evlwi_direction_and_unique_recovery_are_separate(
    project_root: Path,
) -> None:
    config = load_cleaning_range_evidence_0_2_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_cleaning_range_evidence_0_2_policy(config.policy_path)
    state = RangeState(
        hospital="asic_UK00",
        variable="evlwi",
        unit="mL_per_kg",
        rule="test",
        upper=80.0,
        factors=policy.factors,
        integer_tolerance=policy.integer_tolerance,
        sofa_without_gcs_semantic_max=20.0,
    )

    state.add(pa.array([None, -1.0, 0.0, 50.0, 500.0, 50000.0]), 100000)

    assert state.counts["negative_count"] == 1
    assert state.counts["zero_count"] == 1
    assert state.counts["positive_within_count"] == 1
    assert state.counts["above_upper_count"] == 2
    assert state.counts["above_multiple_factor_count"] == 1
    assert state.counts["above_unique_factor_count"] == 1
    assert state.counts["legacy_rule_violation_count"] == 4


def test_sofa_without_gcs_tracks_fractional_and_twenty_point_domain() -> None:
    state = RangeState(
        hospital="asic_UK00",
        variable="sofa_score_without_gcs",
        unit="score_point",
        rule="test",
        upper=24.0,
        factors=(0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        integer_tolerance=1e-9,
        sofa_without_gcs_semantic_max=20.0,
    )

    state.add(pa.array([0.0, 19.0, 20.5, 21.0, 24.0, 25.0]), 100000)

    assert state.counts["fractional_score_count"] == 1
    assert state.counts["above_semantic_max_20_count"] == 4
    assert state.counts["above_upper_count"] == 1


def _write_synthetic_release(data: Path) -> Path:
    release = data / "harmonized/releases/20260807T112402Z"
    release.mkdir(parents=True)
    arrays: dict[str, pa.Array] = {
        "hospital_id": pa.array(HOSPITALS, type=pa.large_string())
    }
    for variable in VARIABLES:
        values = [1.0] * len(HOSPITALS)
        if variable == "albumin":
            values[0] = 0.0
        elif variable == "evlwi":
            values[0] = 50000.0
        elif variable == "ptt":
            values[1] = 301.0
        elif variable == "sofa_score_unspecified":
            values[2] = 2.5
        elif variable == "sofa_score_without_gcs":
            values = [None] * len(HOSPITALS)  # type: ignore[assignment]
        arrays[variable] = pa.array(values, type=pa.float64())
    dynamic = release / "dynamic.parquet"
    pq.write_table(pa.table(arrays), dynamic)
    manifest = release / "release_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "release_id": "20260807T112402Z",
                "frozen_contract_version": "0.2",
                "cleaning_input_approved": True,
                "files": {
                    "dynamic": {
                        "path": str(dynamic),
                        "sha256": sha256_file(dynamic),
                        "row_count": len(HOSPITALS),
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pointer = data / "harmonized/current_release.json"
    pointer.write_text(
        json.dumps(
            {
                "release_id": "20260807T112402Z",
                "frozen_contract_version": "0.2",
                "release_manifest": str(manifest),
                "release_manifest_sha256": sha256_file(manifest),
                "cleaning_input_approved": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    contract = data / "contracts/harmonized_schema_dictionary/0.2"
    contract.mkdir(parents=True)
    (contract / "freeze_manifest.json").write_text(
        json.dumps(
            {
                "contract_version": "0.2",
                "schema_frozen": True,
                "dictionary_frozen": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return dynamic


def test_complete_audit_writes_aggregate_evidence_only(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    shipped = load_cleaning_range_evidence_0_2_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_cleaning_range_evidence_0_2_policy(shipped.policy_path)
    data = tmp_path / "data"
    reports = tmp_path / "reports"
    _write_synthetic_release(data)
    synthetic_policy = replace(policy, expected_rows=len(HOSPITALS))
    monkeypatch.setattr(
        module,
        "load_cleaning_range_evidence_0_2_policy",
        lambda _: synthetic_policy,
    )
    config = CleaningRangeEvidence02Config(
        dataset_context="production",
        data_root=data,
        reports_root=reports,
        policy_path=shipped.policy_path,
    )

    result = run_cleaning_range_evidence_0_2(config, "synthetic-range")
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))

    assert result.overall_status == "pending_human_review"
    assert result.technical_blocking_findings == ()
    assert len(result.blocking_findings) == 1
    assert review["metrics"]["dynamic_rows_scanned"] == len(HOSPITALS)
    assert review["metrics"]["hospital_variable_profile_count"] == 96
    assert review["clinical_data_written_or_modified"] is False
    assert (
        result.private_report_directory / "hospital_variable_profiles.parquet"
    ).is_file()
    assert (
        result.private_report_directory
        / "decision_relevant_value_frequencies.parquet"
    ).is_file()


def test_range_evidence_job_is_low_resource_and_review_gated(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_cleaning_range_evidence_0_2_production.sh"
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "audit-cleaning-0-2-ranges" in text
    assert "expected a technical pass pending one human range decision" in text
    assert "clinical_data_written=false" in text
    assert "cleaning_rules_activated=false" in text
