from __future__ import annotations

import json
from pathlib import Path

from asic_pipeline.cleaning.policy_0_2_review import (
    Cleaning02PolicyReviewConfig,
    _range_rows,
    load_cleaning_0_2_policy_review_config,
    load_cleaning_0_2_policy_review_policy,
    run_cleaning_0_2_policy_review,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.unit_resolution.decisions import load_candidate_unit_decisions


def test_shipped_cleaning_0_2_review_is_complete_and_report_only(
    project_root: Path,
) -> None:
    config = load_cleaning_0_2_policy_review_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_cleaning_0_2_policy_review_policy(config.policy_path)
    candidate = load_candidate_unit_decisions(policy.candidate_decisions_path)
    rows = _range_rows(candidate)

    assert policy.harmonized_release_id == "20260807T112402Z"
    assert policy.harmonized_contract_version == "0.2"
    assert policy.proposed_policy["replay_every_cleaning_0_1_rule"] is True
    assert policy.proposed_policy["apply_general_negative_medication_rule"] is True
    assert len(rows) == 12
    assert sum(row["historical_outside_legacy_range_count"] for row in rows) == 10021
    assert policy.expected["clinical_rows_read_by_review"] == 0


def test_cleaning_0_2_review_writes_one_human_gate_without_clinical_data(
    tmp_path: Path,
    project_root: Path,
) -> None:
    shipped = load_cleaning_0_2_policy_review_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    data = tmp_path / "data"
    reports = tmp_path / "reports"
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
    release = data / "harmonized/releases/20260807T112402Z"
    release.mkdir(parents=True)
    manifest = release / "release_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonized_release",
                "release_id": "20260807T112402Z",
                "frozen_contract_version": "0.2",
                "cleaning_input_approved": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pointer = data / "harmonized/current_release.json"
    pointer.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_current_harmonized_release",
                "release_id": "20260807T112402Z",
                "frozen_contract_version": "0.2",
                "release_manifest": str(manifest),
                "release_manifest_sha256": sha256_file(manifest),
                "harmonized_layer_ready": True,
                "cleaning_input_approved": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    config = Cleaning02PolicyReviewConfig(
        dataset_context="production",
        data_root=data,
        reports_root=reports,
        policy_path=shipped.policy_path,
    )

    result = run_cleaning_0_2_policy_review(config, "synthetic-review")
    payload = json.loads(result.review_json_path.read_text(encoding="utf-8"))

    assert result.overall_status == "pending_human_review"
    assert len(result.blocking_findings) == 1
    assert result.technical_blocking_findings == ()
    assert payload["clinical_rows_read"] == 0
    assert payload["clinical_data_written_or_modified"] is False
    assert len(payload["range_decisions"]) == 12
    assert "Historical old-range findings" in result.review_markdown_path.read_text(
        encoding="utf-8"
    )


def test_cleaning_0_2_policy_review_runs_on_frontend(project_root: Path) -> None:
    script = project_root / "asic/slurm/review_cleaning_0_2_policy_production.sh"
    text = script.read_text(encoding="utf-8")

    assert "review-cleaning-0-2-policy" in text
    assert "metadata-only review" in text
    assert "clinical_rows_read=0" in text
    assert "cleaned_candidate_generated=false" in text
    assert "sbatch" not in text
