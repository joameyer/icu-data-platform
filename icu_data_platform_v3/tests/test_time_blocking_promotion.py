from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.time_blocking.engine import (
    TimeBlockingBuildConfig,
    _schema_digest,
)
from asic_pipeline.time_blocking.evidence import CoreDerivedReleaseInput
from asic_pipeline.time_blocking import promotion as promotion_module
from asic_pipeline.time_blocking.promotion import (
    TimeBlockingPromotionConfig,
    TimeBlockingPromotionPolicy,
    load_time_blocking_promotion_config,
    load_time_blocking_promotion_policy,
    promote_time_blocking_release,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _promotion_fixture(tmp_path: Path, monkeypatch):
    data_root = tmp_path / "asic/data/production"
    reports_root = tmp_path / "asic/reports/production"
    config_path = tmp_path / "asic/config/datasets/production.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text("dataset_context: production\n", encoding="utf-8")

    contract_source = tmp_path / "asic/config/time_blocking/contract.yaml"
    implementation_source = tmp_path / "asic/config/time_blocking/implementation.yaml"
    execution_source = tmp_path / "asic/config/time_blocking/execution.yaml"
    promotion_source = tmp_path / "asic/config/time_blocking/promotion.yaml"
    contract_source.parent.mkdir(parents=True)
    contract_source.write_text("contract: fixture\n", encoding="utf-8")
    implementation_source.write_text("implementation: fixture\n", encoding="utf-8")
    execution_source.write_text("execution: fixture\n", encoding="utf-8")
    promotion_source.write_text("promotion: fixture\n", encoding="utf-8")

    run_id = "fixture-run"
    candidate_directory = Path("derived/time_blocking/8h/candidates")
    releases_directory = Path("derived/time_blocking/8h/releases")
    current_pointer = Path("derived/time_blocking/8h/current_release.json")
    candidate_dir = data_root / candidate_directory / run_id
    candidate_dir.mkdir(parents=True)

    blocks = pa.table({"block_index": pa.array([0, 1], type=pa.int32())})
    summary = pa.table({"grid_block_count": pa.array([1, 1], type=pa.int32())})
    dictionary = pa.table({"output_name": ["synthetic__last_observation_value"]})
    tables = {
        "blocks.parquet": blocks,
        "stay_block_summary.parquet": summary,
        "blocked_variable_dictionary.parquet": dictionary,
    }
    outputs = {}
    for name, table in tables.items():
        path = candidate_dir / name
        pq.write_table(table, path)
        outputs[name] = {
            "sha256": sha256_file(path),
            "row_count": table.num_rows,
            "schema_sha256": _schema_digest(table.schema),
        }
    dictionary_markdown = candidate_dir / "blocked_variable_dictionary.md"
    dictionary_markdown.write_text("# Synthetic dictionary\n", encoding="utf-8")
    outputs[dictionary_markdown.name] = {
        "sha256": sha256_file(dictionary_markdown),
        "row_count": None,
        "schema_sha256": None,
    }

    core_release = data_root / "derived/releases/core-release"
    core_release.mkdir(parents=True)
    static_path = core_release / "static.parquet"
    dynamic_path = core_release / "dynamic.parquet"
    pq.write_table(pa.table({"stay": [1, 2]}), static_path)
    pq.write_table(pa.table({"stay": [1, 2], "value": [3.0, 4.0]}), dynamic_path)
    core_manifest_path = core_release / "release_manifest.json"
    _write_json(core_manifest_path, {"artifact": "fixture-core-release"})
    core_pointer_path = data_root / "derived/current_release.json"
    _write_json(core_pointer_path, {"artifact": "fixture-current-core"})
    source = CoreDerivedReleaseInput(
        pointer_path=core_pointer_path,
        pointer_sha256=sha256_file(core_pointer_path),
        release_directory=core_release,
        release_manifest_path=core_manifest_path,
        release_manifest_sha256=sha256_file(core_manifest_path),
        table_paths={"static": static_path, "dynamic": dynamic_path},
        table_hashes={
            "static": sha256_file(static_path),
            "dynamic": sha256_file(dynamic_path),
        },
        schemas={"static": pq.read_schema(static_path), "dynamic": pq.read_schema(dynamic_path)},
    )

    candidate_metrics = {
        "source_dynamic_row_count": 3,
        "assigned_source_row_count": 3,
        "stay_summary_row_count": 2,
        "block_row_count": 2,
        "empty_block_count": 1,
        "terminal_partial_block_count": 1,
        "output_feature_count": 1,
    }
    execution_lineage = {
        "production_workflow_policy_sha256": sha256_file(implementation_source),
        "production_execution_authorization_sha256": sha256_file(execution_source),
        "authorized_candidate_run_id": run_id,
        "authorized_audit_run_id": run_id,
    }
    manifest = {
        "artifact": "asic_v3_time_blocking_candidate",
        "artifact_version": "0.1",
        "dataset_context": "production",
        "run_id": run_id,
        "status": "nonpublishable_requires_independent_audit_and_human_promotion",
        "resolution": "8h",
        "time_blocking_contract_version": "0.1",
        "lineage": {
            "contract_sha256": sha256_file(contract_source),
            "core_derived_release_id": "core-release",
            "core_derived_release_manifest_sha256": source.release_manifest_sha256,
            "core_derived_file_hashes": source.table_hashes,
            "source_static_path": str(static_path),
            **execution_lineage,
        },
        "outputs": outputs,
        "metrics": candidate_metrics,
        "static_table_copied": False,
        "static_table_repeated_on_blocks": False,
        "rows_or_stays_filtered": False,
        "analysis_cohort_created": False,
        "carry_forward_or_imputation_applied": False,
        "medication_dose_totals_emitted": False,
        "release_created": False,
        "current_release_pointer_modified": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    candidate_manifest_path = candidate_dir / "candidate_manifest.json"
    _write_json(candidate_manifest_path, manifest)
    candidate_manifest_hash = sha256_file(candidate_manifest_path)

    build_review_dir = Path("time_blocking/8h/build")
    audit_review_dir = Path("time_blocking/8h/audit")
    build_review_path = reports_root / "review" / build_review_dir / f"{run_id}.json"
    _write_json(
        build_review_path,
        {
            "artifact": "asic_v3_time_blocking_candidate_build_review",
            "artifact_version": "0.1",
            "dataset_context": "production",
            "run_id": run_id,
            "time_blocking_contract_version": "0.1",
            "candidate_manifest_sha256": candidate_manifest_hash,
            "metrics": candidate_metrics,
            "overall_status": "nonpublishable_requires_independent_audit",
            "source_release_modified": False,
            "release_created": False,
            "current_release_pointer_modified": False,
            "rows_or_stays_filtered": False,
            "carry_forward_or_imputation_applied": False,
            "publication_ready": False,
            "external_data_export_authorized": False,
        },
    )
    build_private_path = (
        reports_root / "private" / build_review_dir / run_id / "build_manifest.json"
    )
    _write_json(build_private_path, manifest)

    audit_metrics = {
        "assigned_source_rows": 3,
        "compared_block_rows": 2,
        "compared_feature_cells": 2,
        "compared_stays": 2,
    }
    audit_review = {
        "artifact": "asic_v3_time_blocking_candidate_audit_review",
        "artifact_version": "0.1",
        "dataset_context": "production",
        "run_id": run_id,
        "build_run_id": run_id,
        "overall_status": "pending_human_review",
        "time_blocking_contract_version": "0.1",
        "candidate_manifest_sha256": candidate_manifest_hash,
        "metrics": audit_metrics,
        "technical_blocking_findings": [],
        "blocking_findings": [
            {
                "check": "time_blocking_candidate_promotion_approved",
                "details": "A technically passing candidate would still require explicit human promotion approval.",
            }
        ],
        "candidate_modified": False,
        "source_release_modified": False,
        "release_created": False,
        "current_release_pointer_modified": False,
        "rows_or_stays_filtered": False,
        "carry_forward_or_imputation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    audit_review_path = reports_root / "review" / audit_review_dir / f"{run_id}.json"
    _write_json(audit_review_path, audit_review)
    audit_private_path = (
        reports_root / "private" / audit_review_dir / run_id / "audit_manifest.json"
    )
    _write_json(
        audit_private_path,
        {
            **audit_review,
            "candidate_directory": str(candidate_dir),
            "source_dynamic_sha256": source.table_hashes["dynamic"],
        },
    )

    contract = SimpleNamespace(
        source_path=contract_source,
        version="0.1",
        core_derived_release_id="core-release",
        resolution_label="8h",
        candidate_directory=candidate_directory,
        release_directory=releases_directory,
        current_release_pointer=current_pointer,
        block_file="blocks.parquet",
        stay_summary_file="stay_block_summary.parquet",
        dictionary_parquet="blocked_variable_dictionary.parquet",
        dictionary_markdown="blocked_variable_dictionary.md",
        candidate_manifest="candidate_manifest.json",
    )
    implementation = SimpleNamespace(
        source_path=implementation_source,
        source_sha256=sha256_file(implementation_source),
        contract_path=contract_source,
        execution_authorization_path=execution_source,
        candidate_artifact_version="0.1",
        audit_artifact_version="0.1",
        build_review_directory=build_review_dir,
        build_private_directory=build_review_dir,
        audit_review_directory=audit_review_dir,
        audit_private_directory=audit_review_dir,
    )
    policy = TimeBlockingPromotionPolicy(
        candidate_run_id=run_id,
        audit_run_id=run_id,
        core_derived_release_id="core-release",
        contract_version="0.1",
        candidate_artifact_version="0.1",
        audit_artifact_version="0.1",
        contract_path=contract_source,
        contract_sha256=sha256_file(contract_source),
        implementation_policy_path=implementation_source,
        implementation_policy_sha256=sha256_file(implementation_source),
        execution_authorization_path=execution_source,
        execution_authorization_sha256=sha256_file(execution_source),
        approval_role="data_owner",
        approval_date="2026-08-08",
        approval_statement="fixture approval",
        approval_context="fixture audit",
        candidate_manifest_sha256=candidate_manifest_hash,
        expected_candidate_metrics=candidate_metrics,
        expected_audit_metrics=audit_metrics,
        resolved_blocker="time_blocking_candidate_promotion_approved",
        release_id=run_id,
        releases_directory=releases_directory,
        current_release_pointer=current_pointer,
        release_manifest_name="release_manifest.json",
        review_directory=Path("time_blocking/8h/promotion"),
        release_artifact_version="0.1",
        source_path=promotion_source,
    )
    build_config = TimeBlockingBuildConfig(
        dataset_context="production",
        data_root=data_root,
        reports_root=reports_root,
        implementation_policy_path=implementation_source,
        source_path=config_path,
    )
    config = TimeBlockingPromotionConfig(build=build_config, policy_path=promotion_source)

    monkeypatch.setattr(
        promotion_module, "load_time_blocking_promotion_policy", lambda *_: policy
    )
    monkeypatch.setattr(
        promotion_module,
        "load_time_blocking_implementation_policy",
        lambda *_: implementation,
    )
    monkeypatch.setattr(
        promotion_module, "load_time_blocking_contract", lambda *_: contract
    )
    monkeypatch.setattr(
        promotion_module,
        "require_production_execution_authorization",
        lambda *_: execution_lineage,
    )
    monkeypatch.setattr(promotion_module, "_source_input", lambda *_: source)
    return SimpleNamespace(
        config=config,
        policy=policy,
        contract=contract,
        source=source,
        candidate_dir=candidate_dir,
        candidate_manifest_path=candidate_manifest_path,
        audit_review_path=audit_review_path,
        data_root=data_root,
        reports_root=reports_root,
    )


def test_shipped_time_blocking_promotion_records_exact_approval(
    project_root: Path,
) -> None:
    config = load_time_blocking_promotion_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_time_blocking_promotion_policy(config.policy_path)

    assert policy.candidate_run_id == "20260808T130534Z"
    assert policy.audit_run_id == policy.candidate_run_id
    assert policy.release_id == policy.candidate_run_id
    assert policy.core_derived_release_id == "20260808T074305Z"
    assert policy.candidate_manifest_sha256 == (
        "c7436c8b624e5245ceef49500c4d952798ac5d05ca110393cfdd2c8d8e790882"
    )
    assert policy.expected_audit_metrics["compared_feature_cells"] == 825_976_872
    assert policy.approval_statement == (
        "I approve promotion of ASIC v3 8-hour time-blocking candidate "
        "20260808T130534Z under time-blocking contract 0.1 as immutable "
        "release 20260808T130534Z, and authorize operator-executed promotion "
        "and atomic creation of only the 8-hour time-blocking current-release "
        "pointer. External export remains unauthorized."
    )


def test_promotion_is_byte_preserving_and_creates_only_nested_pointer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _promotion_fixture(tmp_path, monkeypatch)
    core_pointer_before = fixture.source.pointer_path.read_bytes()
    core_hashes_before = {
        name: sha256_file(path) for name, path in fixture.source.table_paths.items()
    }
    candidate_before = {
        path.name: path.read_bytes() for path in fixture.candidate_dir.iterdir()
    }

    result = promote_time_blocking_release(fixture.config)

    assert result.release_id == "fixture-run"
    assert fixture.source.pointer_path.read_bytes() == core_pointer_before
    assert {
        name: sha256_file(path) for name, path in fixture.source.table_paths.items()
    } == core_hashes_before
    for name, contents in candidate_before.items():
        assert (fixture.candidate_dir / name).read_bytes() == contents
        assert (result.release_directory / name).read_bytes() == contents
    assert not (result.release_directory / "static.parquet").exists()
    pointer = json.loads(
        result.current_release_pointer_path.read_text(encoding="utf-8")
    )
    assert pointer["artifact"] == "asic_v3_current_time_blocking_release"
    assert pointer["release_id"] == "fixture-run"
    assert pointer["core_derived_release_id"] == "core-release"
    assert pointer["analysis_input_approved"] is True
    manifest = json.loads(result.release_manifest_path.read_text(encoding="utf-8"))
    assert manifest["payload_byte_identity_preserved"] is True
    assert manifest["static_table_copied"] is False
    assert manifest["core_derived_current_pointer_modified"] is False
    assert manifest["time_blocking_rerun_during_promotion"] is False
    assert manifest["external_data_export_authorized"] is False
    assert fixture.candidate_dir.is_dir()
    with pytest.raises(HarmonizationError, match="target or initial pointer"):
        promote_time_blocking_release(fixture.config)


def test_promotion_does_not_reopen_wide_candidate_parquet_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _promotion_fixture(tmp_path, monkeypatch)

    def fail_if_parquet_metadata_is_reopened(*args, **kwargs):
        raise AssertionError("promotion must rely on the audited byte-bound manifest")

    monkeypatch.setattr(pq, "ParquetFile", fail_if_parquet_metadata_is_reopened)

    result = promote_time_blocking_release(fixture.config)

    assert result.release_directory.is_dir()
    assert result.current_release_pointer_path.is_file()


def test_promotion_rejects_audit_drift_before_creating_release(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _promotion_fixture(tmp_path, monkeypatch)
    review = json.loads(fixture.audit_review_path.read_text(encoding="utf-8"))
    review["metrics"]["compared_feature_cells"] += 1
    _write_json(fixture.audit_review_path, review)

    with pytest.raises(HarmonizationError, match="audit changed"):
        promote_time_blocking_release(fixture.config)

    assert not (
        fixture.data_root
        / fixture.contract.release_directory
        / fixture.policy.release_id
    ).exists()
    assert not (
        fixture.data_root / fixture.contract.current_release_pointer
    ).exists()
