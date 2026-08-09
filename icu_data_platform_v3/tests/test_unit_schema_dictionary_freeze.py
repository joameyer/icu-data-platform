from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    _schemas_payload,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.unit_resolution import (
    UnitSchemaDictionaryFreezeConfig,
    build_amended_schema_and_dictionary,
    freeze_unit_schema_dictionary,
    load_reviewed_medication_semantics,
    load_reviewed_unit_decisions,
    load_unit_schema_amendment_policy,
    load_unit_schema_dictionary_freeze_config,
    load_unit_schema_dictionary_freeze_policy,
)
from asic_pipeline.unit_resolution.schema_freeze import (
    APPROVAL_STATEMENT,
    EXPECTED_BOUNDARY,
    EXPECTED_REVIEW_METRICS,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def _build_source_evidence(
    tmp_path: Path,
    project_root: Path,
) -> tuple[UnitSchemaDictionaryFreezeConfig, dict[str, Path]]:
    shipped = load_unit_schema_dictionary_freeze_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    freeze_policy = load_unit_schema_dictionary_freeze_policy(shipped.policy_path)
    amendment_policy = load_unit_schema_amendment_policy(
        freeze_policy.amendment_policy_path
    )
    reviewed = load_reviewed_unit_decisions(
        freeze_policy.reviewed_unit_decisions_path
    )
    medication = load_reviewed_medication_semantics(
        freeze_policy.reviewed_medication_semantics_path
    )
    baseline_policy = load_schema_dictionary_freeze_policy(
        amendment_policy.baseline_freeze_policy_path
    )
    baseline_schemas = _schemas_payload(baseline_policy)
    baseline_dictionary: list[dict[str, object]] = []
    for table in ("static", "dynamic"):
        for field in baseline_schemas[table]:
            baseline_dictionary.append(
                {
                    "table": table,
                    **field,
                    "definition": f"Synthetic definition for {field['variable']}",
                    "definition_status": "reviewed",
                    "analysis_caveat": None,
                    "all_missing": False,
                    "source_hospitals": ["asic_UK00"],
                    "source_hospital_count": 1,
                    "source_strategy": "synthetic_test",
                    "categorical_contract_version": None,
                    "publication_ready": False,
                }
            )
    schemas, dictionary = build_amended_schema_and_dictionary(
        baseline_schemas,
        baseline_dictionary,
        reviewed,
        medication,
    )

    data_root = tmp_path / "data"
    reports_root = tmp_path / "reports"
    baseline_manifest = (
        data_root
        / "contracts/harmonized_schema_dictionary/0.1/freeze_manifest.json"
    )
    _write_json(
        baseline_manifest,
        {
            "contract_version": "0.1",
            "schema_frozen": True,
            "dictionary_frozen": True,
        },
    )
    unit_audit_manifest = (
        reports_root
        / "private/unit_decision_audit"
        / amendment_policy.audit_run_id
        / "unit_decision_audit_manifest.json"
    )
    _write_json(
        unit_audit_manifest,
        {"artifact": "synthetic_unit_audit", "run_id": amendment_policy.audit_run_id},
    )

    run_id = freeze_policy.source_review_run_id
    private = (
        reports_root / "private/unit_schema_dictionary_amendment" / run_id
    )
    private.mkdir(parents=True)
    dictionary_path = private / "proposed_variable_dictionary.parquet"
    static_path = private / "proposed_static_schema.json"
    dynamic_path = private / "proposed_dynamic_schema.json"
    pq.write_table(pa.Table.from_pylist(dictionary), dictionary_path)
    _write_json(static_path, schemas["static"])
    _write_json(dynamic_path, schemas["dynamic"])
    source_manifest = private / "schema_amendment_manifest.json"
    _write_json(
        source_manifest,
        {
            "artifact": "asic_v3_unit_schema_dictionary_amendment_private",
            "artifact_version": "0.1",
            "dataset_context": "production",
            "generated_at_utc": "2026-08-07T10:14:15+00:00",
            "run_id": run_id,
            "baseline_contract_version": "0.1",
            "proposed_contract_version": "0.2",
            "lineage_sha256": {
                "baseline_freeze_manifest": sha256_file(baseline_manifest),
                "unit_decision_audit_manifest": sha256_file(unit_audit_manifest),
                "reviewed_unit_decisions": freeze_policy.reviewed_unit_decisions_sha256,
                "reviewed_medication_value_semantics": (
                    freeze_policy.reviewed_medication_semantics_sha256
                ),
                "policy": freeze_policy.amendment_policy_sha256,
            },
            "output_sha256": {
                dictionary_path.name: sha256_file(dictionary_path),
                static_path.name: sha256_file(static_path),
                dynamic_path.name: sha256_file(dynamic_path),
            },
            "metrics": EXPECTED_REVIEW_METRICS,
            "schema_frozen": False,
            "dictionary_frozen": False,
            "clinical_data_written": False,
            "publication_ready": False,
        },
    )
    review_json = (
        reports_root / "review/unit_schema_dictionary_amendment" / f"{run_id}.json"
    )
    _write_json(
        review_json,
        {
            "artifact": "asic_v3_unit_schema_dictionary_amendment_review",
            "artifact_version": "0.1",
            "dataset_context": "production",
            "run_id": run_id,
            "generated_at_utc": "2026-08-07T10:14:15+00:00",
            "baseline_contract_version": "0.1",
            "proposed_contract_version": "0.2",
            "unit_decision_audit_run_id": amendment_policy.audit_run_id,
            "overall_status": "pending_human_review",
            "blocking_findings": [
                {"check": "ordered_schema_dictionary_0_2_approved"}
            ],
            "technical_blocking_findings": [],
            "metrics": EXPECTED_REVIEW_METRICS,
            "clinical_rows_read": 0,
            "clinical_data_written": False,
            "schema_frozen": False,
            "dictionary_frozen": False,
            "existing_releases_modified": False,
            "cleaning_ranges_activated": False,
            "publication_ready": False,
            "external_data_export_authorized": False,
        },
    )
    amendment = SimpleNamespace(
        dataset_context="production",
        data_root=data_root,
        reports_root=reports_root,
    )
    config = UnitSchemaDictionaryFreezeConfig(
        amendment=amendment,  # type: ignore[arg-type]
        policy_path=shipped.policy_path,
    )
    return config, {
        "review": review_json,
        "dictionary": dictionary_path,
        "static": static_path,
        "dynamic": dynamic_path,
        "baseline_manifest": baseline_manifest,
    }


def test_shipped_freeze_policy_binds_exact_approved_review(
    project_root: Path,
) -> None:
    config = load_unit_schema_dictionary_freeze_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_unit_schema_dictionary_freeze_policy(config.policy_path)

    assert policy.contract_version == "0.2"
    assert policy.source_review_run_id == "20260807T101321Z"
    assert policy.approval_statement == APPROVAL_STATEMENT
    assert policy.reference_correction["original_approval_reference"] == (
        "20260807T101415Z"
    )
    assert policy.expected_metrics == EXPECTED_REVIEW_METRICS
    assert EXPECTED_BOUNDARY["read_clinical_rows"] is False
    assert EXPECTED_BOUNDARY["activate_unit_conversions"] is False
    assert EXPECTED_BOUNDARY["activate_cleaning_rule"] is False
    assert EXPECTED_BOUNDARY["authorize_publication"] is False


def test_freeze_copies_exact_reviewed_bytes_and_records_safe_boundary(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config, source = _build_source_evidence(tmp_path, project_root)
    baseline_before = sha256_file(source["baseline_manifest"])

    result = freeze_unit_schema_dictionary(config)

    assert result.contract_version == "0.2"
    for source_name, frozen_name in (
        ("dictionary", "variable_dictionary.parquet"),
        ("static", "static_schema.json"),
        ("dynamic", "dynamic_schema.json"),
    ):
        assert sha256_file(source[source_name]) == sha256_file(
            result.contract_directory / frozen_name
        )
    assert sha256_file(source["baseline_manifest"]) == baseline_before
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    assert manifest["contract_version"] == "0.2"
    assert manifest["schema_frozen"] is True
    assert manifest["dictionary_frozen"] is True
    assert manifest["unit_conversions_activated"] is False
    assert manifest["semantic_splits_activated"] is False
    assert manifest["cleaning_rule_activated"] is False
    assert manifest["clinical_rows_read"] == 0
    assert manifest["clinical_data_artifacts_generated"] is False
    assert review["blocking_findings"] == []
    assert review["publication_ready"] is False
    rows = pq.read_table(
        result.contract_directory / "variable_dictionary.parquet"
    ).to_pylist()
    medication_rows = [
        row for row in rows if row["medication_or_therapy_variable"] is True
    ]
    assert len(rows) == 162
    assert len(medication_rows) == 34
    assert all(
        row["zero_semantics"] == "explicit_observed_numeric_zero_preserved"
        and row["null_semantics"] == "unavailable_or_unobserved_not_assumed_zero"
        and row["negative_value_cleaning_policy"]
        == "mask_negative_medication_or_therapy_value"
        for row in medication_rows
    )


def test_freeze_rejects_changed_review_and_never_overwrites(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config, source = _build_source_evidence(tmp_path, project_root)
    review = json.loads(source["review"].read_text(encoding="utf-8"))
    review["metrics"]["audited_negative_medication_value_count"] = 200
    _write_json(source["review"], review)
    with pytest.raises(HarmonizationError, match="review changed"):
        freeze_unit_schema_dictionary(config)

    config, _ = _build_source_evidence(tmp_path / "second", project_root)
    freeze_unit_schema_dictionary(config)
    with pytest.raises(HarmonizationError, match="already exists"):
        freeze_unit_schema_dictionary(config)
