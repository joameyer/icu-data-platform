from __future__ import annotations

import json
from pathlib import Path

import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    StaticRegistryCompletionAuditConfig,
    build_static_registry_completion_audit,
    load_static_registry_completion_audit_config,
    load_static_registry_completion_audit_policy,
    write_static_registry_completion_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


REGISTRY_RUN = "20260805T104417Z"


def _config(tmp_path: Path, project_root: Path) -> StaticRegistryCompletionAuditConfig:
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
    return StaticRegistryCompletionAuditConfig(
        harmonization_review=review,
        policy_path=(
            project_root
            / "asic/config/harmonization/static_registry_completion_audit.yaml"
        ),
    )


def _occurrence(
    index: int,
    hospital: str,
    raw_name: str,
    target: str | None,
    kind: str,
) -> dict[str, object]:
    return {
        "review_item_id": f"R{index:04d}",
        "hospital": hospital,
        "table": "static",
        "physical_name": f"protected_physical_{index}",
        "raw_name": raw_name,
        "raw_occurrence": 1,
        "candidate_target": target,
        "candidate_kind": kind,
        "candidate_status": "prior_reference_candidate_requires_raw_revalidation",
        "mapping_review_status": "requires_human_review",
        "all_missing_or_empty": False,
        "raw_nonempty_count": 3,
    }


def _write_registry_review_evidence(
    config: StaticRegistryCompletionAuditConfig,
    *,
    include_unaccounted: bool = False,
) -> None:
    rows = [
        _occurrence(1, "asic_UK00", "Pseudo-ID", "stay_id_global", "identifier"),
        _occurrence(2, "asic_UK00", "Cluster-ID", "cluster_id", "categorical"),
        _occurrence(3, "asic_UK00", "weightKg", "weight_kg", "numeric"),
        _occurrence(4, "asic_UK03", "ICD-10_Codes", "icd10_codes", "free_text"),
    ]
    if include_unaccounted:
        rows.append(_occurrence(5, "asic_UK00", "protected_unknown", None, "numeric"))
    variables = [
        {"candidate_target": target}
        for target in sorted(
            {str(row["candidate_target"]) for row in rows if row["candidate_target"]}
        )
    ]
    private = (
        config.reports_root
        / "private"
        / "harmonization_registry_review"
        / REGISTRY_RUN
    )
    private.mkdir(parents=True)
    _write_private_parquet(private / "occurrences.parquet", rows)
    _write_private_parquet(private / "variables.parquet", variables)
    (private / "harmonization_registry_review_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonization_registry_review_private",
                "artifact_version": "0.3",
                "dataset_context": "demo",
                "occurrence_review_row_count": len(rows),
                "variable_review_row_count": len(variables),
            }
        ),
        encoding="utf-8",
    )
    review_dir = config.reports_root / "review" / "harmonization_registry_review"
    review_dir.mkdir(parents=True)
    (review_dir / f"{REGISTRY_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonization_registry_review",
                "artifact_version": "0.3",
                "dataset_context": "demo",
                "metrics": {
                    "raw_column_occurrence_count": len(rows),
                    "technical_blocking_finding_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )


def test_static_registry_completion_accounts_for_sources_and_stays_private(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_registry_review_evidence(config)

    result = build_static_registry_completion_audit(config, REGISTRY_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    metrics = result.review_payload["metrics"]
    assert metrics["static_raw_occurrence_count"] == 4
    assert metrics["approved_mapping_occurrence_count"] == 4
    assert metrics["pending_mapping_occurrence_count"] == 0
    assert metrics["approved_additional_output_count"] == 3
    assert metrics["pending_value_type_variable_count"] == 1
    names = [row["name"] for row in result.review_payload["proposed_ordered_schema"]]
    assert names == [
        "stay_id_global",
        "stay_id_local",
        "hospital_id",
        "cluster_id",
        "weight_kg",
        "icd10_codes_source_text",
        "icd10_codes",
    ]
    serialized = json.dumps(result.review_payload)
    assert "protected_physical" not in serialized
    assert "Pseudo-ID" not in serialized
    assert result.review_payload["publication"]["clinical_data_read"] is False
    assert result.review_payload["publication"]["harmonized_artifacts_generated"] is False

    written = write_static_registry_completion_audit_bundle(
        result,
        config.reports_root,
        "static_registry_output",
    )
    assert written.private_directory is not None
    assert (written.private_directory / "occurrence_coverage.parquet").is_file()
    assert (written.private_directory / "variable_dictionary.parquet").is_file()
    assert written.review_markdown_path is not None
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_static_registry_completion_audit_bundle(
            result,
            config.reports_root,
            "static_registry_output",
        )


def test_static_registry_completion_fails_technically_on_unaccounted_occurrence(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_registry_review_evidence(config, include_unaccounted=True)

    result = build_static_registry_completion_audit(config, REGISTRY_RUN)

    assert result.has_technical_failure is True
    assert "static_occurrence_coverage_complete" in {
        item["check"] for item in result.technical_blocking_findings
    }


def test_static_registry_completion_policy_and_configs_are_fail_closed(
    project_root: Path,
) -> None:
    for context in ("demo", "production"):
        config = load_static_registry_completion_audit_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_static_registry_completion_audit_policy(config.policy_path)
        assert policy.version == "0.1"
        assert policy.registry_review_basis_run_id == REGISTRY_RUN
        assert policy.expected_production_static_occurrence_count == 130
        assert len(policy.approved_hospitals) == 8
        assert policy.proposed_order[0:3] == (
            "stay_id_global",
            "stay_id_local",
            "hospital_id",
        )
