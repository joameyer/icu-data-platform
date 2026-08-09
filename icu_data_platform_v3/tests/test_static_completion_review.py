from __future__ import annotations

import json
from pathlib import Path

import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    StaticCompletionReviewConfig,
    build_static_completion_review,
    load_static_completion_review_config,
    load_static_completion_review_policy,
    write_static_completion_review_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


STATIC_RUN = "20260805T112746Z"
TARGETS = (
    ("icu_los", "numeric", 2, 0),
    ("dialysis_free_days", "numeric", 1, 1),
    ("vent_free_days", "numeric", 1, 1),
    ("icd10_codes", "free_text", 0, 0),
)


def _config(tmp_path: Path, project_root: Path) -> StaticCompletionReviewConfig:
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
    return StaticCompletionReviewConfig(
        harmonization_review=review,
        policy_path=(
            project_root
            / "asic/config/harmonization/static_completion_review.yaml"
        ),
    )


def _write_input_evidence(config: StaticCompletionReviewConfig) -> None:
    private = (
        config.reports_root / "private" / "static_contract_audit" / STATIC_RUN
    )
    private.mkdir(parents=True)
    occurrences = []
    variables = []
    sanitized_variables = []
    for index, (target, kind, direct, sentinel) in enumerate(TARGETS, start=1):
        occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": "asic_UK00",
                "table": "static",
                "raw_name": f"protected_header_{index}",
                "candidate_target": target,
                "scan_row_count": 3,
                "scan_source_null_count": 0,
                "scan_literal_empty_count": 1,
                "scan_nonempty_count": 2,
                "scan_direct_numeric_count": direct,
                "scan_approved_missing_sentinel_count": sentinel,
                "scan_unresolved_nonempty_count": 0,
            }
        )
        variables.append(
            {
                "candidate_target": target,
                "candidate_kind": kind,
            }
        )
        sanitized_variables.append(
            {
                "candidate_target": target,
                "scan_row_count": 3,
                "source_null_count": 0,
                "literal_empty_count": 1,
                "nonempty_count": 2,
                "direct_numeric_count": direct,
                "approved_missing_sentinel_count": sentinel,
                "unresolved_nonempty_count": 0,
            }
        )
    _write_private_parquet(private / "occurrences.parquet", occurrences)
    _write_private_parquet(private / "variables.parquet", variables)
    (private / "static_contract_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_static_contract_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "selected_occurrence_count": len(occurrences),
                "selected_variable_count": len(variables),
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
                "variables": sanitized_variables,
            }
        ),
        encoding="utf-8",
    )


def test_static_completion_review_is_sanitized_read_only_and_review_gated(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)

    result = build_static_completion_review(config, STATIC_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    assert result.review_payload["metrics"]["selected_variable_count"] == 4
    assert result.review_payload["metrics"]["selected_occurrence_count"] == 4
    assert (
        result.review_payload["metrics"]["numeric_unresolved_nonempty_token_count"]
        == 0
    )
    serialized = json.dumps(result.review_payload)
    assert "protected_header" not in serialized
    assert result.review_payload["publication"] == {
        "production_data_read": False,
        "harmonized_artifacts_generated": False,
        "cleaned_artifacts_generated": False,
        "registry_approved": False,
    }

    written = write_static_completion_review_bundle(
        result,
        config.reports_root,
        "static_completion_output",
    )
    assert written.review_markdown_path is not None
    assert written.review_json_path is not None
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_static_completion_review_bundle(
            result,
            config.reports_root,
            "static_completion_output",
        )


def test_static_completion_review_detects_changed_sanitized_counts(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)
    review_path = (
        config.reports_root
        / "review"
        / "static_contract_audit"
        / f"{STATIC_RUN}.json"
    )
    payload = json.loads(review_path.read_text(encoding="utf-8"))
    payload["variables"][0]["nonempty_count"] = 99
    review_path.write_text(json.dumps(payload), encoding="utf-8")

    result = build_static_completion_review(config, STATIC_RUN)

    assert result.has_technical_failure is True
    assert "sanitized_review_matches_private_evidence" in {
        item["check"] for item in result.technical_blocking_findings
    }


def test_static_completion_policy_and_shipped_configs_are_fail_closed(
    project_root: Path,
) -> None:
    for context in ("demo", "production"):
        config = load_static_completion_review_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_static_completion_review_policy(config.policy_path)
        assert policy.version == "0.1"
        assert policy.static_contract_audit_basis_run_id == STATIC_RUN
        assert tuple(item.target for item in policy.proposals) == tuple(
            item[0] for item in TARGETS
        )
