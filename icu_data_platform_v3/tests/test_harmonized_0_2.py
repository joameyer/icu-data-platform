from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.unit_resolution import load_reviewed_unit_decisions
from asic_pipeline.unit_resolution.harmonized_0_2 import (
    HOSPITALS,
    Harmonized02AuditConfig,
    Harmonized02BuildConfig,
    Harmonized02Inputs,
    _accounting,
    _audit_expected_batch,
    _validate_accounting,
    _transform_batch,
    load_harmonized_0_2_audit_config,
    load_harmonized_0_2_audit_policy,
    load_harmonized_0_2_build_config,
    load_harmonized_0_2_build_policy,
    run_harmonized_0_2_audit,
    run_harmonized_0_2_build,
)
from asic_pipeline.unit_resolution import harmonized_0_2 as harmonized_0_2_module
from asic_pipeline.unit_resolution import harmonized_0_2_promotion as promotion_module
from asic_pipeline.unit_resolution.harmonized_0_2_promotion import (
    Harmonized02PromotionConfig,
    load_harmonized_0_2_promotion_config,
    load_harmonized_0_2_promotion_policy,
    promote_harmonized_0_2_release,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.harmonization.harmonized_build import _arrow_type
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    _schemas_payload,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.unit_resolution import load_unit_schema_amendment_policy
from asic_pipeline.unit_resolution import load_unit_schema_dictionary_freeze_policy


def _decision_counters(decisions: object) -> tuple[dict[str, Counter[str]], dict[str, Counter[str]]]:
    conversion = {
        identifier: Counter()
        for identifier in decisions.approved_harmonization_action_ids  # type: ignore[attr-defined]
    }
    splits = {
        str(row["decision_id"]): Counter()
        for row in decisions.semantic_splits  # type: ignore[attr-defined]
    }
    return conversion, splits


def test_shipped_harmonized_0_2_policies_are_immutable_and_review_gated(
    project_root: Path,
) -> None:
    config_path = project_root / "asic/config/datasets/production.yaml"
    build_config = load_harmonized_0_2_build_config(config_path)
    audit_config = load_harmonized_0_2_audit_config(config_path)
    build = load_harmonized_0_2_build_policy(build_config.policy_path)
    audit = load_harmonized_0_2_audit_policy(audit_config.policy_path)

    assert build.source_release_id == "20260806T111156Z"
    assert build.weight_release_id == "20260806T114234Z"
    assert build.target_contract_version == "0.2"
    assert build.expected["unit_conversion_rules"] == 9
    assert build.expected["semantic_split_rules"] == 9
    assert build.expected["current_negative_medication_values_preserved"] == 201
    assert build.output_status.endswith("requires_complete_audit")
    assert audit.build_policy_path == build.source_path
    assert audit.private_directory_name == "harmonized_0_2_audit"

    promotion_config = load_harmonized_0_2_promotion_config(config_path)
    promotion = load_harmonized_0_2_promotion_policy(
        promotion_config.policy_path
    )
    assert promotion.build_run_id == "20260807T112402Z"
    assert promotion.audit_run_id == promotion.build_run_id
    assert promotion.contract_version == "0.2"
    assert promotion.previous_release_id == "20260806T111156Z"
    assert promotion.expected_metrics["compared_output_cells"] == 3466440088
    assert promotion.approval_statement.endswith("contract 0.2.")


def test_multi_hospital_rules_for_same_variable_are_all_applied(
    project_root: Path,
) -> None:
    decisions = load_reviewed_unit_decisions(
        project_root
        / "asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml"
    )
    schema = pa.schema(
        [
            pa.field("hospital_id", pa.large_string()),
            pa.field("stay_id_global", pa.large_string()),
            pa.field("d_dimer", pa.float64()),
            pa.field("vasopressin_iv_cont", pa.float64()),
        ]
    )
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(
                ["asic_UK00", "asic_UK03", "asic_UK08", "asic_UK01"],
                type=pa.large_string(),
            ),
            pa.array(["stay-00", "stay-03", "stay-08", "stay-01"], type=pa.large_string()),
            pa.array([1.0, 2.0, 3.0, 4.0]),
            pa.array([2.0, 60.0, 120.0, 30.0]),
        ],
        schema=schema,
    )
    conversion, splits = _decision_counters(decisions)

    output = _transform_batch(
        batch,
        "dynamic",
        schema,
        None,  # type: ignore[arg-type] -- policy is intentionally not used here
        decisions,
        {"stay-00": 70.0},
        conversion,
        splits,
        Counter(),
    )

    assert output.column(schema.get_field_index("d_dimer")).to_pylist() == pytest.approx(
        [1.0, 2000.0, 3000.0, 4.0]
    )
    assert output.column(
        schema.get_field_index("vasopressin_iv_cont")
    ).to_pylist() == pytest.approx([140.0, 60.0, 2.0, 30.0])
    for decision_id in (
        "UK03-D-DIMER-UG-ML-TO-NG-ML",
        "UK08-D-DIMER-UG-ML-TO-NG-ML",
        "UK08-VASOPRESSIN-IU-H-TO-IU-MIN",
        "UK00-VASOPRESSIN-IU-KG-MIN-TO-IU-MIN",
    ):
        assert conversion[decision_id]["scope_rows"] == 1
        assert conversion[decision_id]["input_non_null"] == 1
        assert conversion[decision_id]["output_non_null"] == 1


def test_semantic_split_routes_value_without_coalescence_or_loss(
    project_root: Path,
) -> None:
    decisions = load_reviewed_unit_decisions(
        project_root
        / "asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml"
    )
    schema = pa.schema(
        [
            pa.field("hospital_id", pa.large_string()),
            pa.field("stay_id_global", pa.large_string()),
            pa.field("clonidine_iv_cont", pa.float64()),
            pa.field("clonidine_iv_cont_weight_normalized", pa.float64()),
        ]
    )
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["asic_UK08", "asic_UK01"], type=pa.large_string()),
            pa.array(["stay-08", "stay-01"], type=pa.large_string()),
            pa.array([0.0, 2.0]),
        ],
        schema=pa.schema(list(schema)[:3]),
    )
    conversion, splits = _decision_counters(decisions)
    medication = Counter()

    output = _transform_batch(
        batch,
        "dynamic",
        schema,
        None,  # type: ignore[arg-type]
        decisions,
        {},
        conversion,
        splits,
        medication,
    )

    assert output.column(schema.get_field_index("clonidine_iv_cont")).to_pylist() == [
        None,
        2.0,
    ]
    assert output.column(
        schema.get_field_index("clonidine_iv_cont_weight_normalized")
    ).to_pylist() == [0.0, None]
    counts = splits["UK08-CLONIDINE-WEIGHT-NORMALIZED-SPLIT"]
    assert counts["source_non_null"] == 1
    assert counts["canonical_values_masked"] == 1
    assert counts["parallel_target_non_null"] == 1
    assert counts["parallel_target_finite_zero"] == 1
    assert medication["input_finite_zero"] == 1
    assert medication["output_finite_zero"] == 1


def _synthetic_values(field: pa.Field) -> pa.Array:
    if field.name == "hospital_id":
        return pa.array(HOSPITALS, type=field.type)
    if field.name == "stay_id_global":
        return pa.array([f"stay-{hospital}" for hospital in HOSPITALS], type=field.type)
    if field.name == "inhaled_no":
        return pa.array(
            [-1.0 if hospital == "asic_UK08" else 1.0 for hospital in HOSPITALS],
            type=field.type,
        )
    if field.name.startswith("__v3_"):
        values = (
            list(range(len(HOSPITALS)))
            if pa.types.is_integer(field.type)
            else [f"source-{index}" for index in range(len(HOSPITALS))]
        )
        return pa.array(values, type=field.type)
    if pa.types.is_floating(field.type):
        return pa.array([1.0] * len(HOSPITALS), type=field.type)
    if pa.types.is_integer(field.type):
        return pa.array([0] * len(HOSPITALS), type=field.type)
    if pa.types.is_boolean(field.type):
        return pa.array([False] * len(HOSPITALS), type=field.type)
    if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
        return pa.array(["synthetic"] * len(HOSPITALS), type=field.type)
    if pa.types.is_timestamp(field.type):
        return pa.array(
            [datetime(2020, 1, 1, tzinfo=timezone.utc)] * len(HOSPITALS),
            type=field.type,
        )
    if pa.types.is_fixed_size_list(field.type):
        return pa.array([[None, None] for _ in HOSPITALS], type=field.type)
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        item = "A01" if pa.types.is_string(field.type.value_type) or pa.types.is_large_string(field.type.value_type) else 170.0
        return pa.array([[item] for _ in HOSPITALS], type=field.type)
    raise AssertionError(f"Unhandled synthetic Arrow type: {field.type}")


def test_all_nine_conversions_and_splits_pass_conservation_together(
    project_root: Path,
) -> None:
    config_path = project_root / "asic/config/datasets/production.yaml"
    build_config = load_harmonized_0_2_build_config(config_path)
    production_policy = load_harmonized_0_2_build_policy(build_config.policy_path)
    policy = replace(
        production_policy,
        expected={
            **production_policy.expected,
            "current_negative_medication_values_preserved": 1,
        },
    )
    decisions = load_reviewed_unit_decisions(production_policy.reviewed_decisions_path)
    freeze = load_unit_schema_dictionary_freeze_policy(
        build_config.freeze.policy_path
    )
    amendment = load_unit_schema_amendment_policy(freeze.amendment_policy_path)
    baseline = load_schema_dictionary_freeze_policy(
        amendment.baseline_freeze_policy_path
    )
    schemas = _schemas_payload(baseline)
    dynamic_rows = list(schemas["dynamic"])
    for position, split in enumerate(decisions.semantic_splits, start=len(dynamic_rows) + 1):
        dynamic_rows.append(
            {
                "ordered_position": position,
                "variable": split["target_variable"],
                "physical_type": "double",
            }
        )
    clinical_fields = [
        pa.field(str(row["variable"]), _arrow_type(str(row["physical_type"])))
        for row in dynamic_rows
    ]
    provenance_fields = [
        pa.field(name, _arrow_type(physical_type), nullable=False)
        for name, physical_type in baseline.provenance_fields
    ]
    target_schema = pa.schema(clinical_fields + provenance_fields)
    split_targets = {str(row["target_variable"]) for row in decisions.semantic_splits}
    source_fields = [
        field for field in target_schema if field.name not in split_targets
    ]
    source_schema = pa.schema(source_fields)
    batch = pa.RecordBatch.from_arrays(
        [_synthetic_values(field) for field in source_schema],
        schema=source_schema,
    )
    conversion, splits = _decision_counters(decisions)
    medication = Counter()

    output = _transform_batch(
        batch,
        "dynamic",
        target_schema,
        policy,
        decisions,
        {"stay-asic_UK00": 70.0},
        conversion,
        splits,
        medication,
    )
    accounting = _accounting(
        conversion,
        splits,
        medication,
        {"static_rows_scanned": 8},
    )
    _validate_accounting(policy, accounting)

    audit_conversion, audit_splits = _decision_counters(decisions)
    audit_medication = Counter()
    independently_recomputed = _audit_expected_batch(
        batch,
        "dynamic",
        target_schema,
        decisions,
        {"stay-asic_UK00": 70.0},
        audit_conversion,
        audit_splits,
        audit_medication,
    )

    assert output.num_rows == 8
    assert output.num_columns == 144
    assert output.equals(independently_recomputed)
    assert accounting == _accounting(
        audit_conversion,
        audit_splits,
        audit_medication,
        {"static_rows_scanned": 8},
    )
    assert all(counts["scope_rows"] == 1 for counts in conversion.values())
    assert all(counts["scope_rows"] == 1 for counts in splits.values())
    assert medication["input_finite_negative"] == 1
    assert medication["output_finite_negative"] == 1


def test_run_scoped_build_and_independent_audit_complete_without_publication(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production_config = project_root / "asic/config/datasets/production.yaml"
    shipped_build_config = load_harmonized_0_2_build_config(production_config)
    shipped_audit_config = load_harmonized_0_2_audit_config(production_config)
    shipped_policy = load_harmonized_0_2_build_policy(
        shipped_build_config.policy_path
    )
    audit_policy = load_harmonized_0_2_audit_policy(
        shipped_audit_config.policy_path
    )
    policy = replace(
        shipped_policy,
        expected={
            **shipped_policy.expected,
            "static_rows": 8,
            "dynamic_rows": 8,
            "current_negative_medication_values_preserved": 1,
        },
    )
    decisions = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    freeze = load_unit_schema_dictionary_freeze_policy(
        shipped_build_config.freeze.policy_path
    )
    amendment = load_unit_schema_amendment_policy(freeze.amendment_policy_path)
    baseline = load_schema_dictionary_freeze_policy(
        amendment.baseline_freeze_policy_path
    )
    baseline_schemas = _schemas_payload(baseline)

    target_schemas: dict[str, pa.Schema] = {}
    source_schemas: dict[str, pa.Schema] = {}
    for table in ("static", "dynamic"):
        rows = list(baseline_schemas[table])
        if table == "dynamic":
            for position, split in enumerate(
                decisions.semantic_splits,
                start=len(rows) + 1,
            ):
                rows.append(
                    {
                        "ordered_position": position,
                        "variable": split["target_variable"],
                        "physical_type": "double",
                    }
                )
        clinical = [
            pa.field(str(row["variable"]), _arrow_type(str(row["physical_type"])))
            for row in rows
        ]
        provenance = [
            pa.field(name, _arrow_type(physical_type), nullable=False)
            for name, physical_type in baseline.provenance_fields
        ]
        target_schemas[table] = pa.schema(clinical + provenance)
        split_targets = {
            str(row["target_variable"]) for row in decisions.semantic_splits
        }
        source_schemas[table] = pa.schema(
            [
                field
                for field in target_schemas[table]
                if field.name not in split_targets
            ]
        )

    data_root = tmp_path / "data"
    reports_root = tmp_path / "reports"
    input_dir = tmp_path / "inputs"
    input_dir.mkdir(parents=True)
    source_files: dict[str, Path] = {}
    for table in ("static", "dynamic"):
        batch = pa.RecordBatch.from_arrays(
            [_synthetic_values(field) for field in source_schemas[table]],
            schema=source_schemas[table],
        )
        path = input_dir / f"{table}.parquet"
        pq.write_table(pa.Table.from_batches([batch]), path)
        source_files[table] = path
    source_manifest = input_dir / "source_release_manifest.json"
    weight_manifest = input_dir / "weight_release_manifest.json"
    target_manifest = input_dir / "target_contract_manifest.json"
    for path, payload in (
        (source_manifest, {"artifact": "synthetic_source"}),
        (weight_manifest, {"artifact": "synthetic_weight_reference"}),
        (target_manifest, {"artifact": "synthetic_contract"}),
    ):
        path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    inputs = Harmonized02Inputs(
        source_manifest_path=source_manifest,
        source_manifest={"artifact": "synthetic_source"},
        source_files=source_files,
        weight_manifest_path=weight_manifest,
        weight_static_path=source_files["static"],
        target_manifest_path=target_manifest,
        target_manifest={"artifact": "synthetic_contract"},
        target_schemas=target_schemas,
        weights={"stay-asic_UK00": 70.0},
        weight_accounting={
            "static_rows_scanned": 8,
            "uk00_static_rows": 1,
            "uk00_valid_weight_count": 1,
            "uk00_null_weight_count": 0,
            "uk00_invalid_weight_count": 0,
            "duplicate_static_stay_count": 0,
            "missing_static_stay_count": 0,
        },
    )
    freeze_context = SimpleNamespace(
        dataset_context="production",
        data_root=data_root,
        reports_root=reports_root,
    )
    build_config = Harmonized02BuildConfig(
        freeze=freeze_context,  # type: ignore[arg-type]
        policy_path=shipped_build_config.policy_path,
    )
    audit_config = Harmonized02AuditConfig(
        build=build_config,
        policy_path=shipped_audit_config.policy_path,
    )
    monkeypatch.setattr(
        harmonized_0_2_module,
        "load_harmonized_0_2_build_policy",
        lambda _path: policy,
    )
    monkeypatch.setattr(
        harmonized_0_2_module,
        "load_harmonized_0_2_audit_policy",
        lambda _path: audit_policy,
    )
    monkeypatch.setattr(
        harmonized_0_2_module,
        "load_reviewed_medication_semantics",
        lambda _path: SimpleNamespace(evidence={"negative_value_count": 1}),
    )
    monkeypatch.setattr(
        harmonized_0_2_module,
        "_load_inputs",
        lambda _config, _policy: inputs,
    )

    build = run_harmonized_0_2_build(build_config, "synthetic-build")
    audit = run_harmonized_0_2_audit(
        audit_config,
        build.run_id,
        "synthetic-audit",
    )

    assert audit.overall_status == "pending_human_review"
    assert len(audit.blocking_findings) == 1
    manifest = json.loads(build.manifest_path.read_text(encoding="utf-8"))
    review = json.loads(audit.review_json_path.read_text(encoding="utf-8"))
    assert manifest["row_counts"] == {"static": 8, "dynamic": 8}
    assert manifest["publication_ready"] is False
    assert len(manifest["transformation_accounting"]["unit_conversions"]) == 9
    assert len(manifest["transformation_accounting"]["semantic_splits"]) == 9
    assert review["technical_status"] == "pass"
    assert review["every_output_cell_recomputed"] is True
    assert review["metrics"]["compared_output_cells"] == 1376
    assert len(review["unit_conversion_summaries"]) == 9
    assert len(review["semantic_split_summaries"]) == 9

    shipped_promotion_config = load_harmonized_0_2_promotion_config(
        production_config
    )
    shipped_promotion_policy = load_harmonized_0_2_promotion_policy(
        shipped_promotion_config.policy_path
    )
    promotion_policy = replace(
        shipped_promotion_policy,
        build_run_id=build.run_id,
        audit_run_id=audit.run_id,
        release_id=build.run_id,
        expected_metrics={
            **shipped_promotion_policy.expected_metrics,
            "compared_static_rows": 8,
            "compared_dynamic_rows": 8,
            "compared_output_cells": 1376,
            "negative_medication_values_preserved": 1,
        },
    )
    contract_manifest = (
        data_root
        / policy.target_contract_directory
        / "freeze_manifest.json"
    )
    contract_manifest.parent.mkdir(parents=True)
    contract_manifest.write_bytes(target_manifest.read_bytes())

    previous_release = (
        data_root
        / promotion_policy.releases_directory
        / promotion_policy.previous_release_id
    )
    previous_release.mkdir(parents=True)
    previous_manifest_path = previous_release / "release_manifest.json"
    previous_manifest_path.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonized_release",
                "release_id": promotion_policy.previous_release_id,
                "status": "released_harmonized_layer",
                "frozen_contract_version": (
                    promotion_policy.previous_contract_version
                ),
                "harmonized_layer_ready": True,
                "cleaning_input_approved": True,
                "publication_ready": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    current_pointer = data_root / promotion_policy.current_release_pointer
    current_pointer.parent.mkdir(parents=True, exist_ok=True)
    current_pointer.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_current_harmonized_release",
                "artifact_version": "0.1",
                "dataset_context": "production",
                "release_id": promotion_policy.previous_release_id,
                "frozen_contract_version": (
                    promotion_policy.previous_contract_version
                ),
                "release_manifest": str(previous_manifest_path),
                "release_manifest_sha256": sha256_file(previous_manifest_path),
                "harmonized_layer_ready": True,
                "cleaning_input_approved": True,
                "publication_ready": True,
                "external_data_export_authorized": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    previous_pointer_hash = sha256_file(current_pointer)
    promotion_config = Harmonized02PromotionConfig(
        audit=audit_config,
        policy_path=shipped_promotion_config.policy_path,
    )
    monkeypatch.setattr(
        promotion_module,
        "load_harmonized_0_2_promotion_policy",
        lambda _path: promotion_policy,
    )
    monkeypatch.setattr(
        promotion_module,
        "load_harmonized_0_2_audit_policy",
        lambda _path: audit_policy,
    )
    monkeypatch.setattr(
        promotion_module,
        "load_harmonized_0_2_build_policy",
        lambda _path: policy,
    )
    monkeypatch.setattr(
        promotion_module,
        "_target_schemas",
        lambda _config, _policy: (
            target_schemas,
            {"contract_version": "0.2"},
        ),
    )

    promoted = promote_harmonized_0_2_release(promotion_config)

    promoted_pointer = json.loads(
        promoted.current_release_pointer_path.read_text(encoding="utf-8")
    )
    promoted_manifest = json.loads(
        promoted.release_manifest_path.read_text(encoding="utf-8")
    )
    assert promoted_pointer["release_id"] == build.run_id
    assert promoted_pointer["previous_release_id"] == (
        promotion_policy.previous_release_id
    )
    assert sha256_file(promoted.previous_pointer_snapshot_path) == (
        previous_pointer_hash
    )
    assert promoted_manifest["payload_byte_identity_preserved"] is True
    assert promoted_manifest["previous_releases_preserved"] is True
    for table in ("static", "dynamic"):
        assert sha256_file(promoted.release_directory / f"{table}.parquet") == (
            sha256_file(build.candidate_directory / f"{table}.parquet")
        )
    assert previous_release.is_dir()
    assert build.candidate_directory.is_dir()
