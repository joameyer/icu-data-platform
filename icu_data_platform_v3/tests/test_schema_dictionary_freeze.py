from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    SchemaDictionaryFreezeConfig,
    freeze_schema_dictionary,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file


def _source_evidence(
    tmp_path: Path,
    project_root: Path,
) -> SchemaDictionaryFreezeConfig:
    reports = tmp_path / "reports"
    data = tmp_path / "data"
    policy_path = (
        project_root
        / "asic/config/harmonization/reviewed_schema_dictionary_freeze_0_1.yaml"
    )
    policy = load_schema_dictionary_freeze_policy(policy_path)
    schemas = {
        table: [
            {
                "ordered_position": item.position,
                "variable": item.name,
                "physical_type": item.physical_type,
                "unit": item.unit,
                "analysis_eligibility": item.eligibility,
            }
            for item in fields
        ]
        for table, fields in policy.schemas
    }
    dictionary_rows = [
        {
            "table": table,
            **row,
            "definition": f"Reviewed definition of {row['variable']}",
            "definition_status": "reviewed_for_test",
            "analysis_caveat": (
                None
                if row["analysis_eligibility"] == "eligible"
                else "reviewed_restriction"
            ),
            "all_missing": False,
            "source_hospitals": ["asic_UK00"],
            "source_hospital_count": 1,
            "source_strategy": "reviewed_test_source",
            "publication_ready": False,
        }
        for table, values in schemas.items()
        for row in values
    ]
    private_dir = (
        reports
        / "private/schema_dictionary_review"
        / policy.source_review_run_id
    )
    private_dir.mkdir(parents=True)
    dictionary_path = private_dir / "proposed_variable_dictionary.parquet"
    pq.write_table(pa.Table.from_pylist(dictionary_rows), dictionary_path)
    manifest_path = private_dir / "schema_dictionary_review_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "artifact_version": policy.private_artifact_version,
                "run_id": policy.source_review_run_id,
                "candidate_run_id": policy.candidate_run_id,
                "output_sha256": {
                    dictionary_path.name: sha256_file(dictionary_path)
                },
            }
        ),
        encoding="utf-8",
    )
    review_dir = reports / "review/schema_dictionary_review"
    review_dir.mkdir(parents=True)
    (review_dir / f"{policy.source_review_run_id}.json").write_text(
        json.dumps(
            {
                "artifact_version": policy.review_artifact_version,
                "run_id": policy.source_review_run_id,
                "candidate_run_id": policy.candidate_run_id,
                "consolidated_audit_run_id": policy.consolidated_audit_run_id,
                "categorical_review_run_id": policy.categorical_review_run_id,
                "overall_status": "fail",
                "blocking_findings": [
                    {"check": name, "details": "human approval required"}
                    for name in sorted(
                        {
                            "ordered_harmonized_union_schema_approved",
                            "variable_dictionary_approved",
                        }
                    )
                ],
                "metrics": dict(policy.expected_metrics),
                "schemas": schemas,
                "operational_provenance_fields": [
                    {"name": name, "physical_type": physical_type}
                    for name, physical_type in policy.provenance_fields
                ],
            }
        ),
        encoding="utf-8",
    )
    return SchemaDictionaryFreezeConfig(
        review=SimpleNamespace(
            dataset_context="production",
            reports_root=reports,
            consolidated=SimpleNamespace(data_root=data),
        ),
        policy_path=policy_path,
    )


def test_freeze_copies_exact_approved_contract_without_clinical_scan(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _source_evidence(tmp_path, project_root)

    result = freeze_schema_dictionary(config)

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_frozen"] is True
    assert manifest["dictionary_frozen"] is True
    assert manifest["clinical_data_artifacts_generated"] is False
    assert manifest["publication_ready"] is False
    assert pq.read_table(
        result.contract_directory / "variable_dictionary.parquet"
    ).num_rows == 153
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    assert review["overall_status"] == "pass"
    assert review["blocking_findings"] == []
    assert review["schema_frozen"] is True
    assert review["dictionary_frozen"] is True


def test_freeze_rejects_changed_review_content(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _source_evidence(tmp_path, project_root)
    policy = load_schema_dictionary_freeze_policy(config.policy_path)
    review_path = (
        config.reports_root
        / "review/schema_dictionary_review"
        / f"{policy.source_review_run_id}.json"
    )
    review = json.loads(review_path.read_text(encoding="utf-8"))
    review["metrics"]["dynamic_variable_count"] = 129
    review_path.write_text(json.dumps(review), encoding="utf-8")

    with pytest.raises(
        HarmonizationError, match="Approved schema review content or lineage changed"
    ):
        freeze_schema_dictionary(config)
