from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.derivation import audit as audit_module
from asic_pipeline.derivation import pipeline as pipeline_module
from asic_pipeline.derivation.pipeline import (
    CoreDerivedConfig,
    CoreDerivedPolicy,
    ReviewedEvidence,
    derived_schema,
    load_core_derived_0_2_config,
    load_core_derived_policy,
    resolve_mortality,
    run_core_derived_candidate_build,
)
from asic_pipeline.derivation import promotion as promotion_module
from asic_pipeline.derivation import promotion_0_2 as promotion_0_2_module
from asic_pipeline.derivation.promotion import (
    CoreDerivedPromotionConfig,
    CoreDerivedPromotionPolicy,
    load_core_derived_promotion_config,
    load_core_derived_promotion_policy,
    promote_core_derived_release,
)
from asic_pipeline.derivation.promotion_0_2 import (
    CoreDerived02PromotionConfig,
    CoreDerived02PromotionPolicy,
    load_core_derived_0_2_promotion_config,
    load_core_derived_0_2_promotion_policy,
    promote_core_derived_0_2_release,
)
from asic_pipeline.derivation.audit import run_core_derived_candidate_audit
from asic_pipeline.derivation.review import CleanedReleaseInput
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file


def test_mortality_uses_three_sources_and_separate_conflicts() -> None:
    assert resolve_mortality("died_in_icu", True, "died") == (
        True,
        True,
        False,
        False,
    )
    assert resolve_mortality("died_in_hospital", None, "died") == (
        True,
        False,
        False,
        False,
    )
    assert resolve_mortality(None, False, "transferred") == (
        False,
        False,
        False,
        False,
    )
    assert resolve_mortality("discharged_alive", True, "transferred") == (
        None,
        False,
        True,
        False,
    )
    assert resolve_mortality(None, None, None) == (None, None, False, False)


def test_derived_schema_appends_without_replacing_inputs() -> None:
    source = pa.schema([pa.field("source_value", pa.float64())])
    dynamic = derived_schema(source, "dynamic")
    static = derived_schema(source, "static")

    assert dynamic.names[:1] == ["source_value"]
    assert dynamic.names[-3:] == [
        "hours_since_icu_admission",
        "delta_p_computed",
        "delta_p_computed_out_of_range",
    ]
    assert static.names[:1] == ["source_value"]
    assert static.names[-9:] == [
        "hospital_mortality",
        "icu_mortality",
        "hospital_mortality_source_conflict",
        "icu_mortality_source_conflict",
        "ventilation_supported_timestamp_count",
        "ventilation_episode_count",
        "maximum_observed_ventilation_episode_hours",
        "observed_mechanical_ventilation_ge_24h",
        "icu_recording_extent_hours",
    ]


def test_derived_0_2_schema_preserves_new_cleaned_columns_and_versions() -> None:
    source = pa.schema(
        [
            pa.field("source_value", pa.float64()),
            pa.field(
                "clonidine_iv_cont_weight_normalized", pa.float64()
            ),
        ],
        metadata={b"asic_v3_cleaning_policy_version": b"0.2"},
    )

    dynamic = derived_schema(source, "dynamic", "0.2")

    assert dynamic.names[:2] == source.names
    assert dynamic.metadata[b"asic_v3_core_derived_contract_version"] == b"0.2"
    assert (
        dynamic.field("delta_p_computed").metadata[
            b"asic_v3_contract_version"
        ]
        == b"0.2"
    )


def _fixture(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    dynamic_path = input_dir / "dynamic.parquet"
    static_path = input_dir / "static.parquet"
    dynamic_table = pa.table(
        {
            "stay_id_global": ["A", "A", "A", "A", "A", "B"],
            "hospital_id": ["asic_UK00"] * 6,
            "minutes_since_icu_admission": [-60.0, 0.0, 480.0, 960.0, 1440.0, 0.0],
            "insp_pressure": [20.0, 20.0, 4.0, 70.0, None, None],
            "peep": [5.0, 5.0, 5.0, 5.0, 5.0, None],
            "delta_p_reported": [15.0, 15.0, None, None, None, None],
            "fio2": [40.0, 40.0, 40.0, 40.0, 40.0, None],
            "vt": [None] * 6,
            "vt_per_kg_ideal_body_weight": [None] * 6,
        }
    )
    static_table = pa.table(
        {
            "stay_id_global": ["A", "B"],
            "hospital_id": ["asic_UK00", "asic_UK00"],
            "death_status": ["died_in_icu", None],
            "hospital_mortality_reported": [True, None],
            "discharge_status": ["died", None],
        }
    )
    pq.write_table(dynamic_table, dynamic_path)
    pq.write_table(static_table, static_path)
    release_manifest_path = input_dir / "release_manifest.json"
    release_manifest_path.write_text("{}\n", encoding="utf-8")
    release_manifest = {
        "files": {
            "static": {"sha256": sha256_file(static_path)},
            "dynamic": {"sha256": sha256_file(dynamic_path)},
        }
    }
    source = CleanedReleaseInput(
        release_directory=input_dir,
        release_manifest_path=release_manifest_path,
        release_manifest=release_manifest,
        source_files={"static": static_path, "dynamic": dynamic_path},
        schemas={"static": static_table.schema, "dynamic": dynamic_table.schema},
    )
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text("approved: true\n", encoding="utf-8")
    policy = CoreDerivedPolicy(
        version="0.1",
        cleaned_release_id="clean001",
        expected_static_rows=2,
        expected_dynamic_rows=6,
        core_review_generated_at_utc="2026-08-06T15:27:00+00:00",
        driving_pressure_review_run_id="dp001",
        expected_core_evidence={},
        expected_driving_evidence={},
        expected_output_evidence={
            "hours_non_missing_count": 6,
            "hours_negative_count": 1,
            "driving_pressure_in_range_count": 2,
            "driving_pressure_out_of_range_count": 2,
            "hospital_mortality": {
                "false": 0,
                "true": 1,
                "missing": 1,
                "conflicts": 0,
            },
            "icu_mortality": {
                "false": 0,
                "true": 1,
                "missing": 1,
                "conflicts": 0,
            },
            "ventilation_supported_timestamp_count_total": 5,
            "ventilation_episode_count_total": 1,
            "observed_mechanical_ventilation_ge_24h_true": 1,
        },
        rows_per_batch=2,
        compression="zstd",
        candidate_directory_name="derived_candidates",
        candidate_artifact_version="0.1",
        build_review_directory_name="derived_build",
        audit_private_directory_name="derived_audit",
        audit_review_directory_name="derived_audit",
        audit_private_artifact_version="0.1",
        audit_review_artifact_version="0.1",
        source_path=policy_path,
    )
    core_path = tmp_path / "core.json"
    driving_path = tmp_path / "driving.json"
    core_path.write_text("{}\n", encoding="utf-8")
    driving_path.write_text("{}\n", encoding="utf-8")
    evidence = ReviewedEvidence(
        core_review_path=core_path,
        core_review_payload={"run_id": "core001"},
        driving_review_path=driving_path,
        driving_review_payload={"run_id": "dp001"},
    )
    config = CoreDerivedConfig(
        derivation_review=SimpleNamespace(
            dataset_context="production",
            data_root=tmp_path / "data",
            reports_root=tmp_path / "reports",
            policy_path=tmp_path / "base.yaml",
        ),
        policy_path=policy_path,
    )
    return config, policy, source, evidence


def test_build_and_independent_audit_recompute_every_derived_value(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, policy, source, evidence = _fixture(tmp_path)
    base_policy = SimpleNamespace(cleaned_release_id="clean001")
    for module in (pipeline_module, audit_module):
        monkeypatch.setattr(module, "load_core_derived_policy", lambda *_: policy)
        monkeypatch.setattr(
            module, "load_derivation_contract_review_policy", lambda *_: base_policy
        )
        monkeypatch.setattr(module, "load_cleaned_release_input", lambda *_: source)
        monkeypatch.setattr(module, "load_reviewed_evidence", lambda *_: evidence)

    build = run_core_derived_candidate_build(config, "derived001")
    audit = run_core_derived_candidate_audit(
        config, "derived001", "audit001"
    )

    assert build.candidate_directory.is_dir()
    assert audit.overall_status == "pending_human_review"
    dynamic = pq.read_table(build.candidate_directory / "dynamic.parquet")
    assert dynamic["hours_since_icu_admission"].to_pylist() == [
        -1.0,
        0.0,
        8.0,
        16.0,
        24.0,
        0.0,
    ]
    assert dynamic["delta_p_computed"].to_pylist() == [15.0, 15.0, None, None, None, None]
    assert dynamic["delta_p_computed_out_of_range"].to_pylist() == [
        False,
        False,
        True,
        True,
        None,
        None,
    ]
    static = pq.read_table(build.candidate_directory / "static.parquet")
    assert static["hospital_mortality"].to_pylist() == [True, None]
    assert static["icu_mortality"].to_pylist() == [True, None]
    assert static["ventilation_supported_timestamp_count"].to_pylist() == [5, 0]
    assert static["ventilation_episode_count"].to_pylist() == [1, 0]
    assert static["maximum_observed_ventilation_episode_hours"].to_pylist() == [24.0, 0.0]
    assert static["observed_mechanical_ventilation_ge_24h"].to_pylist() == [True, False]
    report = audit.review_markdown_path.read_text(encoding="utf-8")
    assert "Every input column preserved exactly: `true`" in report
    assert "PENDING HUMAN REVIEW" in report


def test_core_derived_0_2_reuses_formulas_and_preserves_input_boundary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, prior_policy, source, evidence = _fixture(tmp_path)
    contract_hash = sha256_file(prior_policy.source_path)
    policy = replace(
        prior_policy,
        version="0.2",
        cleaned_release_id="cleaned02",
        candidate_directory_name="derived_0_2_candidates",
        candidate_artifact_version="0.2",
        build_review_directory_name="derived_0_2_build",
        audit_private_directory_name="derived_0_2_audit",
        audit_review_directory_name="derived_0_2_audit",
        audit_private_artifact_version="0.2",
        audit_review_artifact_version="0.2",
        previous_contract_path=prior_policy.source_path,
        previous_contract_sha256=contract_hash,
        cleaned_promotion_policy_path=prior_policy.source_path,
        cleaned_promotion_policy_sha256=contract_hash,
    )
    config = replace(
        config,
        cleaned_0_2_promotion=SimpleNamespace(policy_path=prior_policy.source_path),
    )
    for module in (pipeline_module, audit_module):
        monkeypatch.setattr(module, "load_core_derived_policy", lambda *_: policy)
        monkeypatch.setattr(module, "load_reviewed_evidence", lambda *_: evidence)
    monkeypatch.setattr(
        pipeline_module, "load_cleaned_0_2_release_input", lambda *_: source
    )

    build = run_core_derived_candidate_build(config, "derived02")
    audit = run_core_derived_candidate_audit(config, "derived02", "audit02")

    assert audit.overall_status == "pending_human_review"
    manifest = json.loads(build.manifest_path.read_text(encoding="utf-8"))
    assert manifest["core_derived_contract_version"] == "0.2"
    assert manifest["lineage"]["clinical_formula_changes_from_0_1"] == 0
    assert manifest["lineage"]["output_semantic_changes_from_0_1"] == 0
    output = pq.read_table(build.candidate_directory / "dynamic.parquet")
    assert output.schema.metadata[
        b"asic_v3_core_derived_contract_version"
    ] == b"0.2"
    assert output.column_names[: len(source.schemas["dynamic"])] == (
        source.schemas["dynamic"].names
    )


def test_explicit_core_derived_promotion_preserves_audited_bytes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, core_policy, source, evidence = _fixture(tmp_path)
    base_policy = SimpleNamespace(cleaned_release_id="clean001")
    for module in (pipeline_module, audit_module):
        monkeypatch.setattr(
            module, "load_core_derived_policy", lambda *_: core_policy
        )
        monkeypatch.setattr(
            module,
            "load_derivation_contract_review_policy",
            lambda *_: base_policy,
        )
        monkeypatch.setattr(module, "load_cleaned_release_input", lambda *_: source)
        monkeypatch.setattr(module, "load_reviewed_evidence", lambda *_: evidence)

    run_id = "20260806T170134Z"
    build = run_core_derived_candidate_build(config, run_id)
    audit = run_core_derived_candidate_audit(config, run_id, run_id)
    audit_payload = json.loads(audit.review_json_path.read_text(encoding="utf-8"))
    promotion_source = tmp_path / "promotion.yaml"
    promotion_source.write_text("approved: true\n", encoding="utf-8")
    promotion_policy = CoreDerivedPromotionPolicy(
        version="0.1",
        derived_run_id=run_id,
        audit_run_id=run_id,
        cleaned_release_id="clean001",
        core_derived_contract_version="0.1",
        candidate_artifact_version="0.1",
        audit_private_artifact_version="0.1",
        audit_review_artifact_version="0.1",
        approval_role="data_owner",
        approval_date="2026-08-06",
        approval_statement="approved fixture candidate",
        approval_context="complete fixture audit",
        expected_metrics=audit_payload["metrics"],
        expected_accounting=audit_payload["derivation_accounting"],
        resolved_blocker="core_derived_candidate_release_approved",
        release_id=run_id,
        releases_directory=Path("derived/releases"),
        current_release_pointer=Path("derived/current_release.json"),
        release_artifact_version="0.1",
        review_directory_name="derived_promotion",
        source_path=promotion_source,
    )
    monkeypatch.setattr(
        promotion_module,
        "load_core_derived_promotion_policy",
        lambda *_: promotion_policy,
    )
    monkeypatch.setattr(
        promotion_module, "load_core_derived_policy", lambda *_: core_policy
    )
    monkeypatch.setattr(
        promotion_module,
        "load_derivation_contract_review_policy",
        lambda *_: base_policy,
    )
    monkeypatch.setattr(
        promotion_module, "load_cleaned_release_input", lambda *_: source
    )
    monkeypatch.setattr(
        promotion_module, "load_reviewed_evidence", lambda *_: evidence
    )
    promotion_config = CoreDerivedPromotionConfig(
        derived=config,
        policy_path=promotion_source,
    )

    result = promote_core_derived_release(promotion_config)

    release_manifest = json.loads(
        result.release_manifest_path.read_text(encoding="utf-8")
    )
    current = json.loads(
        result.current_release_pointer_path.read_text(encoding="utf-8")
    )
    assert release_manifest["core_derived_layer_ready"] is True
    assert release_manifest["analysis_input_approved"] is True
    assert release_manifest["derivation_rerun_during_promotion"] is False
    assert release_manifest["cohort_generated"] is False
    assert release_manifest["time_blocking_applied"] is False
    assert release_manifest["external_data_export_authorized"] is False
    assert current["release_id"] == run_id
    assert current["release_manifest_sha256"] == sha256_file(
        result.release_manifest_path
    )
    for table in ("static", "dynamic"):
        assert sha256_file(
            result.release_directory / f"{table}.parquet"
        ) == sha256_file(build.candidate_directory / f"{table}.parquet")
    assert build.candidate_directory.is_dir()
    with pytest.raises(HarmonizationError, match="target already exists"):
        promote_core_derived_release(promotion_config)


def test_shipped_core_derived_promotion_records_exact_approval(
    project_root: Path,
) -> None:
    config = load_core_derived_promotion_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_core_derived_promotion_policy(config.policy_path)

    assert policy.derived_run_id == "20260806T170134Z"
    assert policy.audit_run_id == "20260806T170134Z"
    assert policy.release_id == "20260806T170134Z"
    assert policy.core_derived_contract_version == "0.1"
    assert policy.approval_statement == (
        "I approve promotion of core-derived candidate 20260806T170134Z "
        "under core-derived contract 0.1."
    )
    assert policy.expected_metrics["compared_output_cell_count"] == 3_322_168_300
    assert policy.expected_accounting["driving_pressure_out_of_range_count"] == 3_636


def test_shipped_core_derived_0_2_contract_is_lineage_only(
    project_root: Path,
) -> None:
    config = load_core_derived_0_2_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_core_derived_policy(config.policy_path)

    assert policy.version == "0.2"
    assert policy.cleaned_release_id == "20260807T154100Z"
    assert policy.expected_static_input_columns == 28
    assert policy.expected_dynamic_input_columns == 144
    assert policy.candidate_directory_name == "derived_0_2_candidates"
    assert policy.previous_contract_sha256 == sha256_file(
        project_root
        / "asic/config/derivation/reviewed_core_derived_contract_0_1.yaml"
    )
    assert policy.expected_output_evidence[
        "driving_pressure_out_of_range_count"
    ] == 3_636


def test_core_derived_0_2_promotion_preserves_prior_release_and_pointer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, prior_policy, source, evidence = _fixture(tmp_path)
    contract_hash = sha256_file(prior_policy.source_path)
    core_policy = replace(
        prior_policy,
        version="0.2",
        cleaned_release_id="cleaned02",
        candidate_directory_name="derived_0_2_candidates",
        candidate_artifact_version="0.2",
        build_review_directory_name="derived_0_2_build",
        audit_private_directory_name="derived_0_2_audit",
        audit_review_directory_name="derived_0_2_audit",
        audit_private_artifact_version="0.2",
        audit_review_artifact_version="0.2",
        previous_contract_path=prior_policy.source_path,
        previous_contract_sha256=contract_hash,
        cleaned_promotion_policy_path=prior_policy.source_path,
        cleaned_promotion_policy_sha256=contract_hash,
    )
    config = replace(
        config,
        cleaned_0_2_promotion=SimpleNamespace(policy_path=prior_policy.source_path),
    )
    for module in (pipeline_module, audit_module):
        monkeypatch.setattr(
            module, "load_core_derived_policy", lambda *_: core_policy
        )
        monkeypatch.setattr(module, "load_reviewed_evidence", lambda *_: evidence)
    monkeypatch.setattr(
        pipeline_module, "load_cleaned_0_2_release_input", lambda *_: source
    )
    run_id = "derived02promote"
    build = run_core_derived_candidate_build(config, run_id)
    audit = run_core_derived_candidate_audit(config, run_id, run_id)
    audit_payload = json.loads(audit.review_json_path.read_text(encoding="utf-8"))

    prior_release_id = "derived01"
    prior_release = config.data_root / "derived/releases" / prior_release_id
    prior_release.mkdir(parents=True)
    prior_files = {}
    for table in ("static", "dynamic"):
        path = prior_release / f"{table}.parquet"
        path.write_bytes(source.source_files[table].read_bytes())
        prior_files[table] = {"sha256": sha256_file(path)}
    prior_manifest_path = prior_release / "release_manifest.json"
    prior_manifest_path.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_core_derived_release",
                "release_id": prior_release_id,
                "status": "released_core_derived_layer",
                "core_derived_contract_version": "0.1",
                "cleaned_release_id": "cleaned01",
                "files": prior_files,
                "core_derived_layer_ready": True,
                "analysis_input_approved": True,
                "publication_ready": True,
                "external_data_export_authorized": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    current_pointer = config.data_root / "derived/current_release.json"
    current_pointer.parent.mkdir(parents=True, exist_ok=True)
    current_pointer.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_current_core_derived_release",
                "release_id": prior_release_id,
                "core_derived_contract_version": "0.1",
                "cleaned_release_id": "cleaned01",
                "release_manifest": str(prior_manifest_path),
                "release_manifest_sha256": sha256_file(prior_manifest_path),
                "core_derived_layer_ready": True,
                "analysis_input_approved": True,
                "publication_ready": True,
                "external_data_export_authorized": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    previous_pointer_bytes = current_pointer.read_bytes()
    previous_hashes = {
        table: sha256_file(prior_release / f"{table}.parquet")
        for table in ("static", "dynamic")
    }
    promotion_source = tmp_path / "promotion02.yaml"
    promotion_source.write_text("approved: true\n", encoding="utf-8")
    promotion_policy = CoreDerived02PromotionPolicy(
        version="0.1",
        derived_run_id=run_id,
        audit_run_id=run_id,
        cleaned_release_id="cleaned02",
        contract_version="0.2",
        candidate_artifact_version="0.2",
        audit_private_artifact_version="0.2",
        audit_review_artifact_version="0.2",
        contract_path=core_policy.source_path,
        contract_sha256=contract_hash,
        approval_role="data_owner",
        approval_date="2026-08-08",
        approval_statement="fixture approval",
        approval_context="fixture context",
        resolved_blocker="core_derived_candidate_release_approved",
        previous_release_id=prior_release_id,
        previous_contract_version="0.1",
        previous_cleaned_release_id="cleaned01",
        current_release_pointer=Path("derived/current_release.json"),
        release_id=run_id,
        releases_directory=Path("derived/releases"),
        previous_pointer_snapshot_name="previous_current_release.json",
        release_artifact_version="0.2",
        review_directory_name="derived_0_2_promotion",
        source_path=promotion_source,
    )
    monkeypatch.setattr(
        promotion_0_2_module,
        "load_core_derived_0_2_promotion_policy",
        lambda *_: promotion_policy,
    )
    monkeypatch.setattr(
        promotion_0_2_module,
        "load_core_derived_policy",
        lambda *_: core_policy,
    )
    monkeypatch.setattr(
        promotion_0_2_module,
        "_load_core_derived_source",
        lambda *_: source,
    )
    monkeypatch.setattr(
        promotion_0_2_module,
        "load_reviewed_evidence",
        lambda *_: evidence,
    )
    monkeypatch.setattr(
        promotion_0_2_module, "EXPECTED_METRICS", audit_payload["metrics"]
    )
    monkeypatch.setattr(
        promotion_0_2_module,
        "EXPECTED_ACCOUNTING",
        audit_payload["derivation_accounting"],
    )
    promotion_config = CoreDerived02PromotionConfig(
        derived=config,
        policy_path=promotion_source,
    )

    result = promote_core_derived_0_2_release(promotion_config)

    assert result.previous_pointer_snapshot_path.read_bytes() == previous_pointer_bytes
    assert json.loads(
        result.current_release_pointer_path.read_text(encoding="utf-8")
    )["release_id"] == run_id
    for table in ("static", "dynamic"):
        assert sha256_file(
            result.release_directory / f"{table}.parquet"
        ) == sha256_file(build.candidate_directory / f"{table}.parquet")
        assert sha256_file(prior_release / f"{table}.parquet") == previous_hashes[
            table
        ]
    assert build.candidate_directory.is_dir()


def test_shipped_core_derived_0_2_promotion_records_exact_approval(
    project_root: Path,
) -> None:
    config = load_core_derived_0_2_promotion_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_core_derived_0_2_promotion_policy(config.policy_path)

    assert policy.derived_run_id == "20260808T074305Z"
    assert policy.audit_run_id == policy.derived_run_id
    assert policy.release_id == policy.derived_run_id
    assert policy.cleaned_release_id == "20260807T154100Z"
    assert policy.contract_version == "0.2"
    assert policy.previous_release_id == "20260806T170134Z"
    assert policy.approval_statement == (
        "I approve promotion of core-derived 0.2 candidate "
        "20260808T074305Z under core-derived contract 0.2."
    )
