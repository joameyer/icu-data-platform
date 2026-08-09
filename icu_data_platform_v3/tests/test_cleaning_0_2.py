from __future__ import annotations

from collections import Counter
from dataclasses import replace
import json
import math
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.cleaning.cleaning_0_2 import (
    _apply_priority_rule,
    _cleaned_schema,
    _mask_negative_medication,
    _priority_invalid_mask,
    _reviewed_medication_variables,
    _validate_range_evidence,
    load_cleaning_0_2_audit_config,
    load_cleaning_0_2_audit_policy,
    load_cleaning_0_2_build_config,
    load_cleaning_0_2_policy,
)
from asic_pipeline.cleaning import cleaning_0_2_promotion as promotion_module
from asic_pipeline.cleaning.cleaning_0_2_promotion import (
    Cleaned02PromotionConfig,
    load_cleaned_0_2_promotion_config,
    load_cleaned_0_2_promotion_policy,
    promote_cleaned_0_2_release,
)
from asic_pipeline.inventory.hashing import sha256_file


def _policies(project_root: Path):
    config_path = project_root / "asic/config/datasets/production.yaml"
    build_config = load_cleaning_0_2_build_config(config_path)
    audit_config = load_cleaning_0_2_audit_config(config_path)
    build = load_cleaning_0_2_policy(build_config.policy_path)
    audit = load_cleaning_0_2_audit_policy(audit_config.policy_path)
    return build, audit


def test_shipped_cleaning_0_2_contract_is_complete_and_review_gated(
    project_root: Path,
) -> None:
    policy, audit = _policies(project_root)

    assert policy.version == "0.2"
    assert policy.release_id == "20260807T112402Z"
    assert policy.contract_version == "0.2"
    assert policy.range_run_id == "20260807T133609Z"
    assert policy.range_expected["legacy_findings"] == 10565
    assert len(policy.rules) == 12
    assert [rule.variable for rule in policy.rules] == [
        "albumin",
        "creatinine",
        "evlwi",
        "gedvi",
        "hemoglobin",
        "inr",
        "lactate_art",
        "platelets",
        "ptt",
        "sofa_score_unspecified",
        "sofa_score_without_gcs",
        "svri",
    ]
    assert policy.expected["known_priority_zero_masks"] == 9597
    assert policy.expected["known_priority_negative_masks"] == 50
    assert policy.expected["expected_evlwi_factor_0_001_corrections"] == 32
    assert policy.expected["expected_evlwi_ambiguous_masks"] == 10
    assert policy.expected["expected_positive_upper_values_preserved"] == 332
    assert policy.expected["expected_negative_medication_masks"] == 201
    assert policy.candidate_status.endswith("requires_complete_cleaning_0_2_audit")
    assert audit.build_policy_path == policy.source_path
    assert audit.private_directory_name == "cleaning_0_2_audit"

    promotion_config = load_cleaned_0_2_promotion_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    promotion = load_cleaned_0_2_promotion_policy(
        promotion_config.policy_path
    )
    assert promotion.build_run_id == "20260807T154100Z"
    assert promotion.audit_run_id == promotion.build_run_id
    assert promotion.previous_release_id == "20260806T114234Z"
    assert promotion.release_id == promotion.build_run_id
    assert promotion.expected_metrics["priority_mask_count"] == 9740
    assert promotion.expected_metrics["albumin_above_1000_mask_count"] == 83
    assert promotion.approval_statement.endswith("cleaning policy 0.2.")


def test_evlwi_only_repairs_a_unique_power_of_ten_candidate(
    project_root: Path,
) -> None:
    policy, _ = _policies(project_root)
    rule = next(rule for rule in policy.rules if rule.variable == "evlwi")
    counts: Counter[str] = Counter()

    output = _apply_priority_rule(
        pa.array(
            [None, math.nan, -1.0, 0.0, 10.0, 62260.0, 345.0, 100000.0],
            type=pa.float64(),
        ),
        rule,
        counts,
    )

    assert output.to_pylist()[:5] == [None, None, None, None, 10.0]
    assert output[5].as_py() == pytest.approx(62.26)
    assert output.to_pylist()[6:] == [None, None]
    assert counts["corrected_factor_0.001"] == 1
    assert counts["masked_ambiguous_upper"] == 1
    assert counts["masked_unrecoverable_upper"] == 1
    assert counts["masked_nonpositive"] == 2
    assert not any(_priority_invalid_mask(output, rule).to_pylist())


@pytest.mark.parametrize(
    ("variable", "source", "expected"),
    [
        ("albumin", [0.0, 400.0, 1000.0, 1001.0], [None, 400.0, 1000.0, None]),
        ("creatinine", [-1.0, 0.0, 7832.0], [None, None, 7832.0]),
        ("gedvi", [0.0, 1500.0, 2500.0], [None, 1500.0, 2500.0]),
        ("hemoglobin", [0.0, 7.5, 25.0, 25.1], [None, 7.5, 25.0, None]),
        ("inr", [0.0, 2.0, 20.0], [None, 2.0, 20.0]),
        ("lactate_art", [-0.1, 0.0, 3.0, 31.0], [None, None, 3.0, 31.0]),
        ("platelets", [0.0, 250.0, 2001.0], [None, 250.0, None]),
        ("ptt", [0.0, 50.0, 301.0], [None, 50.0, 301.0]),
        ("svri", [-1.0, 0.0, 2000.0, 10001.0], [None, None, 2000.0, 10001.0]),
        ("sofa_score_unspecified", [0.0, 12.0, 24.0, 25.0], [0.0, 12.0, 24.0, None]),
        ("sofa_score_without_gcs", [0.0, 10.0, 20.0, 20.5], [0.0, 10.0, 20.0, None]),
    ],
)
def test_priority_rules_follow_the_approved_matrix(
    project_root: Path,
    variable: str,
    source: list[float],
    expected: list[float | None],
) -> None:
    policy, _ = _policies(project_root)
    rule = next(rule for rule in policy.rules if rule.variable == variable)

    observed = _apply_priority_rule(
        pa.array(source, type=pa.float64()), rule, Counter()
    )

    assert observed.to_pylist() == expected
    assert not any(_priority_invalid_mask(observed, rule).to_pylist())


def test_negative_medication_rule_preserves_zero_positive_and_null() -> None:
    counts = {"inhaled_no": Counter()}
    output = _mask_negative_medication(
        pa.array([-1.8, 0.0, 4.0, None], type=pa.float64()),
        pa.array(["asic_UK08"] * 4, type=pa.large_string()),
        "inhaled_no",
        counts,
    )

    assert output.to_pylist() == [None, 0.0, 4.0, None]
    assert counts["inhaled_no"]["masked_negative"] == 1
    assert counts["inhaled_no"]["input_zero"] == 1
    assert counts["inhaled_no"]["output_zero"] == 1
    assert counts["inhaled_no"]["input_positive"] == 1
    assert counts["inhaled_no"]["output_positive"] == 1
    assert counts["inhaled_no"]["masked_negative_hospital_asic_UK08"] == 1


def test_medication_scope_includes_all_nine_semantic_split_targets() -> None:
    decisions = SimpleNamespace(
        medication_variables=tuple(f"source_{index}" for index in range(25)),
        semantic_splits=tuple(
            {"target_variable": f"split_{index}"} for index in range(9)
        ),
    )

    variables = _reviewed_medication_variables(decisions)

    assert len(variables) == 34
    assert "source_24" in variables
    assert "split_8" in variables


def test_cleaned_schema_preserves_fields_and_marks_candidate_boundary() -> None:
    source = pa.schema(
        [pa.field("hospital_id", pa.large_string()), pa.field("albumin", pa.float64())],
        metadata={b"old": b"metadata"},
    )

    output = _cleaned_schema(source, "0.2")

    assert list(output) == list(source)
    assert output.metadata == {
        b"asic_v3_stage": b"cleaned_0_2_candidate",
        b"asic_v3_harmonized_contract_version": b"0.2",
        b"asic_v3_cleaning_policy_version": b"0.2",
        b"publication_ready": b"false",
        b"cleaning_applied": b"true",
        b"derivation_applied": b"false",
    }


def test_range_evidence_lineage_is_json_serializable(
    tmp_path: Path,
) -> None:
    run_id = "20260807T133609Z"
    generated = "2026-08-07T13:37:29+00:00"
    review_dir = tmp_path / "review/cleaning_range_evidence_0_2"
    private_dir = tmp_path / "private/cleaning_range_evidence_0_2" / run_id
    review_dir.mkdir(parents=True)
    private_dir.mkdir(parents=True)
    metrics = {
        "dynamic_rows_scanned": 24,
        "hospital_variable_profile_count": 2,
        "legacy_rule_violation_count_after_harmonization_0_2": 5,
        "negative_count": 1,
        "zero_count": 2,
        "above_upper_count": 2,
        "fractional_score_count": 0,
        "technical_blocking_finding_count": 0,
    }
    review = {
        "artifact": "asic_v3_cleaning_range_evidence_0_2_review",
        "artifact_version": "0.1",
        "dataset_context": "production",
        "generated_at_utc": generated,
        "run_id": run_id,
        "harmonized_release_id": "release",
        "harmonized_contract_version": "0.2",
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": [
            {"check": "cleaning_0_2_range_dispositions_approved"}
        ],
        "technical_blocking_findings": [],
        "metrics": metrics,
        "clinical_data_written_or_modified": False,
        "cleaning_rules_activated": False,
        "publication_ready": False,
    }
    (review_dir / f"{run_id}.json").write_text(
        json.dumps(review), encoding="utf-8"
    )
    private = {
        "artifact": "asic_v3_cleaning_range_evidence_0_2_private",
        "artifact_version": "0.1",
        "dataset_context": "production",
        "generated_at_utc": generated,
        "run_id": run_id,
        "harmonized_release_id": "release",
        "metrics": metrics,
        "complete_frequency_evidence": True,
        "patient_rows_or_identifiers_written": False,
        "clinical_data_modified": False,
    }
    (private_dir / "audit_manifest.json").write_text(
        json.dumps(private), encoding="utf-8"
    )
    pq.write_table(
        pa.table({"profile": pa.array([1, 2], type=pa.int64())}),
        private_dir / "hospital_variable_profiles.parquet",
    )
    pq.write_table(
        pa.table({"frequency": pa.array([1], type=pa.int64())}),
        private_dir / "decision_relevant_value_frequencies.parquet",
    )
    config = SimpleNamespace(reports_root=tmp_path, dataset_context="production")
    policy = SimpleNamespace(
        range_run_id=run_id,
        range_artifact_version="0.1",
        range_generated_at=generated,
        release_id="release",
        contract_version="0.2",
        range_expected={
            "dynamic_rows": 24,
            "hospital_profiles": 2,
            "legacy_findings": 5,
            "negatives": 1,
            "zeros": 2,
            "above_upper": 2,
            "fractional_scores": 0,
        },
    )

    lineage = _validate_range_evidence(config, policy)  # type: ignore[arg-type]

    assert isinstance(lineage["review_path"], str)
    assert isinstance(lineage["private_manifest_path"], str)
    json.dumps(lineage, allow_nan=False)


def test_cleaning_0_2_job_is_modest_complete_and_review_gated(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_cleaning_0_2_build_and_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "exec sbatch" in text
    assert "build-cleaned-0-2-candidate" in text
    assert "audit-cleaned-0-2-candidate" in text
    assert "expected technical pass pending human approval" in text
    assert "cleaned_dictionary_generated=true" in text
    assert "publication_ready=false" in text
    assert "promote" not in text


def test_cleaned_0_2_promotion_preserves_candidate_and_previous_release(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shipped_config = load_cleaned_0_2_promotion_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    shipped_policy = load_cleaned_0_2_promotion_policy(
        shipped_config.policy_path
    )
    build_run = "synthetic-cleaned-02"
    expected_metrics = {
        **shipped_policy.expected_metrics,
        "compared_static_rows": 2,
        "compared_dynamic_rows": 2,
        "compared_output_cells": 4,
    }
    policy = replace(
        shipped_policy,
        build_run_id=build_run,
        audit_run_id=build_run,
        release_id=build_run,
        expected_metrics=expected_metrics,
    )
    data_root = tmp_path / "data"
    reports_root = tmp_path / "reports"
    audit_config = SimpleNamespace(
        dataset_context="production",
        data_root=data_root,
        reports_root=reports_root,
        policy_path=policy.audit_policy_path,
        build=SimpleNamespace(policy_path=policy.cleaning_policy_path),
    )
    config = Cleaned02PromotionConfig(
        audit=audit_config,  # type: ignore[arg-type]
        policy_path=shipped_config.policy_path,
    )
    cleaning_policy = SimpleNamespace(
        version="0.2",
        release_id=policy.harmonized_release_id,
        contract_version="0.2",
        artifact_version="0.2",
        candidate_directory_name="cleaned_0_2_candidates",
        candidate_status="nonpublishable_requires_complete_cleaning_0_2_audit",
        dictionary_filename="cleaned_variable_dictionary.parquet",
        dictionary_markdown_filename="cleaned_variable_dictionary.md",
        expected={"static_clinical_variables": 1, "dynamic_clinical_variables": 1},
    )
    audit_policy = SimpleNamespace(
        build_policy_path=policy.cleaning_policy_path,
        private_artifact_version="0.2",
        review_artifact_version="0.2",
        private_directory_name="cleaning_0_2_audit",
        review_directory_name="cleaning_0_2_audit",
    )
    source_schemas = {
        table: pa.schema([pa.field(f"{table}_value", pa.float64())])
        for table in ("static", "dynamic")
    }
    harmonized_manifest = tmp_path / "harmonized_release_manifest.json"
    harmonized_manifest.write_text("{}\n", encoding="utf-8")
    source = SimpleNamespace(
        source_schemas=source_schemas,
        release_manifest_path=harmonized_manifest,
    )
    candidate = data_root / "cleaned_0_2_candidates" / build_run
    candidate.mkdir(parents=True)
    outputs: dict[str, dict[str, object]] = {}
    for table in ("static", "dynamic"):
        schema = _cleaned_schema(source_schemas[table], "0.2")
        values = pa.Table.from_arrays(
            [pa.array([1.0, 2.0], type=pa.float64())], schema=schema
        )
        path = candidate / f"{table}.parquet"
        pq.write_table(values, path)
        outputs[table] = {
            "path": str(path),
            "sha256": sha256_file(path),
            "row_count": 2,
            "schema_sha256": promotion_module._schema_digest(schema),
        }
    dictionary = pa.table(
        {
            "table": pa.array(["static", "dynamic"], type=pa.large_string()),
            "variable": pa.array(
                ["static_value", "dynamic_value"], type=pa.large_string()
            ),
        }
    )
    dictionary_path = candidate / "cleaned_variable_dictionary.parquet"
    pq.write_table(dictionary, dictionary_path)
    markdown_path = candidate / "cleaned_variable_dictionary.md"
    markdown_path.write_text("# Synthetic cleaned dictionary\n", encoding="utf-8")
    outputs["cleaned_variable_dictionary"] = {
        "path": str(dictionary_path),
        "sha256": sha256_file(dictionary_path),
        "row_count": 2,
    }
    outputs["cleaned_variable_dictionary_markdown"] = {
        "path": str(markdown_path),
        "sha256": sha256_file(markdown_path),
    }
    candidate_metrics = {
        "priority_mask_count": expected_metrics["priority_mask_count"],
        "negative_medication_mask_count": expected_metrics[
            "negative_medication_mask_count"
        ],
    }
    candidate_manifest = {
        "artifact": "asic_v3_cleaning_0_2_candidate",
        "artifact_version": "0.2",
        "dataset_context": "production",
        "run_id": build_run,
        "status": cleaning_policy.candidate_status,
        "lineage": {
            "harmonized_release_id": policy.harmonized_release_id,
            "harmonized_contract_version": "0.2",
            "cleaning_0_2_policy_sha256": policy.cleaning_policy_sha256,
        },
        "outputs": outputs,
        "metrics": candidate_metrics,
        "rule_accounting": {"synthetic": {"masked": 1}},
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "columns_dropped": False,
        "height_aggregation_applied": False,
        "cleaning_applied": True,
        "derivation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    candidate_manifest_path = candidate / "cleaning_0_2_manifest.json"
    candidate_manifest_path.write_text(
        json.dumps(candidate_manifest) + "\n", encoding="utf-8"
    )
    review_dir = reports_root / "review/cleaning_0_2_audit"
    private_dir = reports_root / "private/cleaning_0_2_audit" / build_run
    review_dir.mkdir(parents=True)
    private_dir.mkdir(parents=True)
    review = {
        "artifact": "asic_v3_cleaning_0_2_audit_review",
        "artifact_version": "0.2",
        "dataset_context": "production",
        "run_id": build_run,
        "build_run_id": build_run,
        "harmonized_release_id": policy.harmonized_release_id,
        "cleaning_policy_version": "0.2",
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": [{"check": policy.resolved_blocker}],
        "technical_blocking_findings": [],
        "metrics": expected_metrics,
        "every_output_cell_recomputed": True,
        "cleaned_dictionary_recomputed": True,
        "uncertain_positive_extremes_preserved": True,
        "rows_or_stays_filtered": False,
        "columns_dropped": False,
        "derivation_applied": False,
        "candidate_data_modified": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    review_path = review_dir / f"{build_run}.json"
    review_path.write_text(json.dumps(review) + "\n", encoding="utf-8")
    private = {
        "artifact": "asic_v3_cleaning_0_2_audit_private",
        "artifact_version": "0.2",
        "dataset_context": "production",
        "run_id": build_run,
        "build_run_id": build_run,
        "build_manifest_sha256": sha256_file(candidate_manifest_path),
        "metrics": expected_metrics,
        "rule_accounting": candidate_manifest["rule_accounting"],
        "source_data_modified": False,
        "candidate_data_modified": False,
        "publication_ready": False,
    }
    (private_dir / "cleaning_0_2_audit_manifest.json").write_text(
        json.dumps(private) + "\n", encoding="utf-8"
    )
    previous_release = (
        data_root / policy.releases_directory / policy.previous_release_id
    )
    previous_release.mkdir(parents=True)
    previous_manifest_path = previous_release / "release_manifest.json"
    previous_manifest_path.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_cleaned_release",
                "release_id": policy.previous_release_id,
                "status": "released_cleaned_layer",
                "cleaning_policy_version": "0.1",
                "cleaned_layer_ready": True,
                "derived_input_approved": True,
                "publication_ready": True,
                "external_data_export_authorized": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    pointer = data_root / policy.current_release_pointer
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_current_cleaned_release",
                "artifact_version": "0.1",
                "dataset_context": "production",
                "release_id": policy.previous_release_id,
                "cleaning_policy_version": "0.1",
                "release_manifest": str(previous_manifest_path),
                "release_manifest_sha256": sha256_file(previous_manifest_path),
                "cleaned_layer_ready": True,
                "derived_input_approved": True,
                "publication_ready": True,
                "external_data_export_authorized": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    previous_pointer_hash = sha256_file(pointer)
    monkeypatch.setattr(
        promotion_module,
        "load_cleaned_0_2_promotion_policy",
        lambda _path: policy,
    )
    monkeypatch.setattr(
        promotion_module, "load_cleaning_0_2_audit_policy", lambda _path: audit_policy
    )
    monkeypatch.setattr(
        promotion_module, "load_cleaning_0_2_policy", lambda _path: cleaning_policy
    )
    monkeypatch.setattr(promotion_module, "_load_input", lambda *_args: source)

    result = promote_cleaned_0_2_release(config)

    promoted_pointer = json.loads(
        result.current_release_pointer_path.read_text(encoding="utf-8")
    )
    promoted_manifest = json.loads(
        result.release_manifest_path.read_text(encoding="utf-8")
    )
    assert promoted_pointer["release_id"] == build_run
    assert promoted_pointer["previous_release_id"] == policy.previous_release_id
    assert sha256_file(result.previous_pointer_snapshot_path) == previous_pointer_hash
    assert promoted_manifest["dictionary_bytes_preserved"] is True
    assert promoted_manifest["previous_releases_preserved"] is True
    for filename in (
        "static.parquet",
        "dynamic.parquet",
        "cleaned_variable_dictionary.parquet",
        "cleaned_variable_dictionary.md",
    ):
        assert sha256_file(result.release_directory / filename) == sha256_file(
            candidate / filename
        )
    assert candidate.is_dir()
    assert previous_release.is_dir()


def test_cleaned_0_2_promotion_job_is_low_resource_and_nonoverwriting(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_promote_cleaned_0_2_release_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "ASIC_CLEANED_0_2_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "20260807T154100Z" in text
    assert "20260806T114234Z" in text
    assert "promote-cleaned-0-2-release" in text
    assert "nothing will be overwritten" in text
    assert "audited_parquet_and_dictionary_bytes_preserved=true" in text
    assert "previous_cleaned_release_preserved=true" in text
    assert "cleaning_rerun_during_promotion=false" in text
    assert "external_data_export_authorized=false" in text
