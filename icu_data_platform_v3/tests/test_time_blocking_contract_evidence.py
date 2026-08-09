from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.time_blocking.evidence import (
    _contract_schema_digest_from_parquet,
    _schema_digest,
    assign_time_block,
    load_time_blocking_evidence_config,
    load_time_blocking_evidence_policy,
    run_time_blocking_contract_evidence,
)
from asic_pipeline.time_blocking import (
    aggregate_stay,
    build_output_specs,
    load_time_blocking_build_config,
    load_time_blocking_contract,
    load_time_blocking_implementation_policy,
    require_production_execution_authorization,
    run_time_blocking_candidate_audit,
    run_time_blocking_candidate_build,
)
from asic_pipeline.errors import HarmonizationError


def test_reviewed_right_labelled_time_assignment() -> None:
    assert assign_time_block(-481.0) == ("pre_admission", -2)
    assert assign_time_block(-480.0) == ("pre_admission", -1)
    assert assign_time_block(-0.001) == ("pre_admission", -1)
    assert assign_time_block(0.0) == ("admission", 0)
    assert assign_time_block(0.001) == ("post_admission", 1)
    assert assign_time_block(479.999) == ("post_admission", 1)
    assert assign_time_block(480.0) == ("post_admission", 1)
    assert assign_time_block(480.001) == ("post_admission", 2)


def test_parquet_schema_digest_restores_contract_list_child_name(
    tmp_path: Path,
) -> None:
    contract_schema = pa.schema(
        [
            pa.field(
                "confirmation",
                pa.list_(pa.bool_(), 2),
                metadata={b"asic_v3_unit": b"not_applicable"},
            )
        ],
        metadata={b"asic_v3_stage": b"derived_candidate"},
    )
    table = pa.Table.from_arrays(
        [pa.array([[True, False]], type=pa.list_(pa.bool_(), 2))],
        schema=contract_schema,
    )
    path = tmp_path / "nested.parquet"
    pq.write_table(table, path)
    parquet_schema = pq.ParquetFile(path).schema_arrow

    assert parquet_schema == contract_schema
    assert _schema_digest(parquet_schema) != _schema_digest(contract_schema)
    assert (
        _contract_schema_digest_from_parquet(parquet_schema)
        == _schema_digest(contract_schema)
    )


def test_shipped_registry_covers_exactly_147_dynamic_fields(
    project_root: Path,
) -> None:
    policy = load_time_blocking_evidence_policy(
        project_root / "asic/config/time_blocking/contract_evidence_8h.yaml"
    )

    assert len(policy.registry_variables) == 147
    assert len(set(policy.registry_variables)) == 147
    assert policy.canonical_last_observation_outputs == (
        "last_observation_value",
        "last_observation_time_h",
        "last_observation_age_h",
    )
    assert len(policy.analysis_ineligible) == 9
    assert policy.conditionally_ineligible == ("vt_per_kg_ideal_body_weight",)


def _reviewed_source_schema(project_root: Path) -> pa.Schema:
    policy = load_time_blocking_evidence_policy(
        project_root / "asic/config/time_blocking/contract_evidence_8h.yaml"
    )
    return pa.schema(
        [
            pa.field(
                name,
                _dynamic_type(name, policy),
                metadata={b"asic_v3_unit": b"synthetic"},
            )
            for name in policy.registry_variables
        ]
    )


def test_frozen_contract_registry_defers_dose_totals(project_root: Path) -> None:
    contract = load_time_blocking_contract(
        project_root
        / "asic/config/time_blocking/reviewed_8h_contract_0_1.yaml"
    )
    implementation = load_time_blocking_implementation_policy(
        project_root / "asic/config/time_blocking/local_implementation_8h.yaml"
    )
    specs = build_output_specs(contract, _reviewed_source_schema(project_root))
    names = {spec.output_name for spec in specs}

    assert contract.evidence.run_id == "20260808T114902Z"
    assert implementation.allowed_dataset_contexts == ("demo",)
    assert "heart_rate__last_observation_value" in names
    assert "isofa_total_score__last_observation_time_h" in names
    assert "ecmo__last_observation_age_h" in names
    assert "fluid_balance_24h__mean" not in names
    assert "vt_per_kg_ideal_body_weight__mean" not in names
    assert "feo2__mean" not in names
    assert not any("dose_total" in name for name in names)
    assert not any("administration_count" in name for name in names)


def test_production_workflow_policy_requires_separate_exact_run_authorization(
    tmp_path: Path,
    project_root: Path,
) -> None:
    policy = load_time_blocking_implementation_policy(
        project_root
        / "asic/config/time_blocking/production_candidate_audit_8h.yaml"
    )
    contract = load_time_blocking_contract(
        project_root
        / "asic/config/time_blocking/reviewed_8h_contract_0_1.yaml"
    )
    assert policy.workflow_mode == "production_execution_pending"
    assert policy.allowed_dataset_contexts == ("production",)
    assert policy.execution_authorization_path is not None
    assert policy.execution_authorization_path.is_file()
    active_lineage = require_production_execution_authorization(
        policy,
        contract,
        "20260808T130534Z",
        "20260808T130534Z",
    )
    assert active_lineage["authorized_candidate_run_id"] == "20260808T130534Z"
    assert active_lineage["authorized_audit_run_id"] == "20260808T130534Z"

    authorization_path = tmp_path / "reviewed_execution.yaml"
    authorized = replace(
        policy, execution_authorization_path=authorization_path
    )
    authorization = {
        "artifact": "asic_v3_time_blocking_8h_production_execution_authorization",
        "artifact_version": "0.1",
        "status": "human_approved_for_exact_candidate_and_audit_run",
        "dataset_context": "production",
        "resolution": "8h",
        "core_derived_release_id": "20260808T074305Z",
        "approval": {
            "role": "data_owner",
            "recorded_date": "2026-08-08",
            "statement": (
                "I authorize exact candidate prod-build and independent "
                "audit prod-audit execution."
            ),
        },
        "lineage": {
            "time_blocking_contract_sha256": policy.contract_sha256,
            "production_workflow_policy_sha256": policy.source_sha256,
        },
        "run_scope": {
            "candidate_run_id": "prod-build",
            "audit_run_id": "prod-audit",
        },
        "authorized_actions": {
            "build_exact_run_scoped_candidate": True,
            "run_exact_independent_audit": True,
            "promote_release": False,
            "modify_current_release_pointer": False,
            "modify_core_derived_release": False,
            "filter_rows_or_stays": False,
            "create_analysis_cohort": False,
            "apply_carry_forward_or_imputation": False,
            "authorize_external_data_export": False,
        },
    }
    authorization_path.write_text(
        yaml.safe_dump(authorization, sort_keys=False), encoding="utf-8"
    )

    lineage = require_production_execution_authorization(
        authorized,
        contract,
        "prod-build",
        "prod-audit",
    )
    assert lineage["authorized_candidate_run_id"] == "prod-build"
    assert lineage["authorized_audit_run_id"] == "prod-audit"
    assert lineage["production_execution_authorization_sha256"] == sha256_file(
        authorization_path
    )

    authorization["authorized_actions"]["promote_release"] = True
    authorization_path.write_text(
        yaml.safe_dump(authorization, sort_keys=False), encoding="utf-8"
    )
    with pytest.raises(
        HarmonizationError,
        match="execution authorization is invalid",
    ):
        require_production_execution_authorization(
            authorized,
            contract,
            "prod-build",
            "prod-audit",
        )


def test_aggregate_stay_uses_reviewed_right_labels_and_missingness(
    project_root: Path,
) -> None:
    contract = load_time_blocking_contract(
        project_root
        / "asic/config/time_blocking/reviewed_8h_contract_0_1.yaml"
    )
    specs = build_output_specs(contract, _reviewed_source_schema(project_root))
    times = [
        -480.0,
        -0.1,
        0.0,
        60.0,
        479.999,
        480.0,
        480.0,
        480.001,
        600.0,
    ]
    heart_rates = [50.0, None, 60.0, 70.0, 79.0, 80.0, 81.0, 82.0, None]
    dexamethasone = [None, None, None, 0.0, None, None, None, None, 4.0]
    ecmo = [False, None, False, False, False, True, False, True, None]
    pairs = [
        [None, None],
        [None, None],
        [True, None],
        [False, True],
        [None, None],
        [None, None],
        [True, False],
        [False, False],
        [None, None],
    ]
    rows: list[dict[str, object]] = []
    for source_order, (minutes, heart_rate, dose, flag, pair) in enumerate(
        zip(times, heart_rates, dexamethasone, ecmo, pairs, strict=True),
        start=1,
    ):
        rows.append(
            {
                "stay_id_global": "s1",
                "hospital_id": "asic_UK00",
                "minutes_since_icu_admission": minutes,
                "__v3_source_order": source_order,
                "heart_rate": heart_rate,
                "dexamethasone_iv_bolus": dose,
                "ecmo": flag,
                "therapy_read_confirmation_utc": pair,
                "isofa_total_score": (
                    5.0 if minutes == 60.0 else 7.0 if minutes == 480.0 else None
                ),
                "fluid_balance_24h": (
                    100.0 if minutes == 60.0 else 150.0 if minutes == 480.0 else None
                ),
            }
        )
    blocks, summary = aggregate_stay(
        "s1", "asic_UK00", 10.0, rows, contract, specs
    )
    by_index = {row["block_index"]: row for row in blocks}

    assert list(by_index) == [-1, 0, 1, 2]
    assert by_index[-1]["block_label"] == "pre_8h"
    assert by_index[0]["interval_semantics"] == "singleton_zero"
    assert by_index[1]["block_label"] == "8h"
    assert by_index[1]["source_row_count"] == 4
    assert by_index[1]["heart_rate__last_observation_value"] == 81.0
    assert by_index[1]["heart_rate__last_observation_time_h"] == 8.0
    assert by_index[1]["heart_rate__last_observation_age_h"] == 0.0
    assert by_index[1]["dexamethasone_iv_bolus__observation_count"] == 1
    assert by_index[1]["dexamethasone_iv_bolus__observed_zero_count"] == 1
    assert by_index[1]["ecmo__observed_true_count"] == 1
    assert by_index[1]["ecmo__observed_false_count"] == 3
    assert by_index[1]["ecmo__any_true"] is True
    assert by_index[1]["ecmo__all_true"] is False
    assert by_index[1]["ecmo__last_observation_time_h"] == 8.0
    assert by_index[1]["ecmo__last_observation_age_h"] == 0.0
    assert by_index[1]["isofa_total_score__last_observation_value"] == 7.0
    assert by_index[1]["isofa_total_score__last_observation_time_h"] == 8.0
    assert by_index[1]["fluid_balance_24h__last_observation_value"] == 150.0
    assert by_index[1]["fluid_balance_24h__last_observation_time_h"] == 8.0
    assert (
        by_index[1][
            "therapy_read_confirmation_utc__pair_state_true_false_count"
        ]
        == 1
    )
    assert by_index[2]["is_terminal_by_recording_extent_proxy"] is True
    assert by_index[2]["is_full_by_recording_extent_proxy"] is False
    assert by_index[2]["available_through_h"] == 10.0
    assert by_index[2]["dexamethasone_iv_bolus__last_observation_age_h"] == 0.0
    assert by_index[-1]["dexamethasone_iv_bolus__observation_count"] == 0
    assert by_index[0]["dexamethasone_iv_bolus__observation_count"] == 0
    assert by_index[2]["dexamethasone_iv_bolus__observation_count"] == 1
    assert summary["source_row_count"] == len(rows)
    assert summary["terminal_partial_block_count"] == 1


def test_aggregate_stay_retains_empty_and_exact_terminal_blocks(
    project_root: Path,
) -> None:
    contract = load_time_blocking_contract(
        project_root
        / "asic/config/time_blocking/reviewed_8h_contract_0_1.yaml"
    )
    specs = build_output_specs(contract, _reviewed_source_schema(project_root))
    rows = [
        {
            "stay_id_global": "s1",
            "hospital_id": "asic_UK00",
            "minutes_since_icu_admission": 0.0,
            "__v3_source_order": 1,
        },
        {
            "stay_id_global": "s1",
            "hospital_id": "asic_UK00",
            "minutes_since_icu_admission": 960.0,
            "__v3_source_order": 2,
            "heart_rate": 80.0,
        },
    ]
    blocks, summary = aggregate_stay(
        "s1", "asic_UK00", 16.0, rows, contract, specs
    )

    assert [row["block_index"] for row in blocks] == [0, 1, 2]
    assert blocks[1]["source_row_count"] == 0
    assert blocks[1]["heart_rate__observation_count"] == 0
    assert blocks[1]["heart_rate__mean"] is None
    assert blocks[2]["block_label"] == "16h"
    assert blocks[2]["is_terminal_by_recording_extent_proxy"] is True
    assert blocks[2]["is_full_by_recording_extent_proxy"] is True
    assert summary["empty_block_count"] == 1
    assert summary["terminal_partial_block_count"] == 0

    positive_only = [
        {
            "stay_id_global": "s2",
            "hospital_id": "asic_UK01",
            "minutes_since_icu_admission": 60.0,
            "__v3_source_order": 1,
            "heart_rate": 75.0,
        }
    ]
    admission_empty, _ = aggregate_stay(
        "s2", "asic_UK01", 1.0, positive_only, contract, specs
    )
    assert [row["block_index"] for row in admission_empty] == [0, 1]
    assert admission_empty[0]["source_row_count"] == 0
    assert admission_empty[0]["heart_rate__last_observation_value"] is None


def _dynamic_type(name: str, policy: object) -> pa.DataType:
    if name in {"stay_id_global", "stay_id_local", "hospital_id"}:
        return pa.large_string()
    if name in {"__v3_source_file_id", "__v3_source_schema_variant_id"}:
        return pa.string()
    if name == "__v3_source_file_order":
        return pa.int32()
    if name in {"__v3_source_row_number", "__v3_source_order"}:
        return pa.int64()
    if name == "anchored_time_since_icu_admission":
        return pa.timestamp("ns")
    if name in {"ecmo", "delta_p_computed_out_of_range"}:
        return pa.bool_()
    if name == "therapy_read_confirmation_utc":
        return pa.list_(pa.bool_(), 2)
    if name == "severity_read_confirmation":
        return pa.large_string()
    if name == "ards_diagnosis_app":
        return pa.int32()
    return pa.float64()


def _dynamic_values(name: str, times: list[float]) -> list[object]:
    rows = len(times)
    if name == "stay_id_global":
        return ["s1"] * 5 + ["s2"]
    if name == "stay_id_local":
        return ["l1"] * 5 + ["l2"]
    if name == "hospital_id":
        return ["asic_UK00"] * 5 + ["asic_UK01"]
    if name == "minutes_since_icu_admission":
        return times
    if name == "hours_since_icu_admission":
        return [value / 60.0 for value in times]
    if name == "anchored_time_since_icu_admission":
        return [None] * rows
    if name == "__v3_source_file_id":
        return ["private-token"] * rows
    if name == "__v3_source_file_order":
        return [1] * rows
    if name == "__v3_source_row_number":
        return [2, 3, 4, 5, 6, 2]
    if name == "__v3_source_order":
        return [1, 2, 3, 4, 5, 1]
    if name == "__v3_source_schema_variant_id":
        return ["variant"] * rows
    if name == "heart_rate":
        return [70.0, 71.0, 72.0, 73.0, 74.0, 80.0]
    if name == "dexamethasone_iv_bolus":
        return [None, 4.0, None, 0.0, 4.0, None]
    if name == "ecmo":
        return [False, False, True, None, True, False]
    if name == "delta_p_computed_out_of_range":
        return [False] * rows
    if name == "therapy_read_confirmation_utc":
        return [[False, None], [True, False], [None, None], [True, True], [None, None], [False, True]]
    if name == "severity_read_confirmation":
        return [None] * rows
    if name == "ards_diagnosis_app":
        return [None] * rows
    return [None] * rows


def _write_synthetic_release(
    temporary_root: Path,
    shipped_policy_path: Path,
) -> Path:
    asic_root = temporary_root / "asic"
    config_dir = asic_root / "config/datasets"
    policy_dir = asic_root / "config/time_blocking"
    config_dir.mkdir(parents=True)
    policy_dir.mkdir(parents=True)
    policy = yaml.safe_load(shipped_policy_path.read_text(encoding="utf-8"))
    policy["input"].update(
        {
            "core_derived_release_id": "synthetic",
            "cleaned_release_id": "cleaned-synthetic",
            "harmonized_release_id": "harmonized-synthetic",
            "expected_static_rows": 2,
            "expected_dynamic_rows": 6,
            "expected_static_columns": 4,
            "expected_negative_time_rows": 1,
        }
    )
    policy["streaming"]["primary_rows_per_batch"] = 2
    policy["streaming"]["verification_rows_per_batch"] = 3
    lineage_dir = asic_root / "config/lineage"
    lineage_dir.mkdir()
    for name, item in policy["lineage_contracts"].items():
        contract = lineage_dir / f"{name}.yaml"
        contract.write_text(f"name: {name}\n", encoding="utf-8")
        item["path"] = f"../lineage/{contract.name}"
        item["sha256"] = hashlib.sha256(contract.read_bytes()).hexdigest()
    policy_path = policy_dir / "contract_evidence_8h.yaml"
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    dataset_config = {
        "dataset_context": "demo",
        "time_blocking_contract_evidence_policy": "../time_blocking/contract_evidence_8h.yaml",
        "paths": {
            "data": "../../data/demo",
            "reports": "../../reports/demo",
        },
    }
    config_path = config_dir / "demo.yaml"
    config_path.write_text(yaml.safe_dump(dataset_config), encoding="utf-8")

    loaded_policy = load_time_blocking_evidence_policy(policy_path)
    static_schema = pa.schema(
        [
            pa.field("stay_id_global", pa.large_string()),
            pa.field("hospital_id", pa.large_string()),
            pa.field("icu_recording_extent_hours", pa.float64()),
            pa.field("diagnosis_components", pa.list_(pa.large_string())),
        ]
    )
    static = pa.Table.from_arrays(
        [
            pa.array(["s1", "s2"], type=pa.large_string()),
            pa.array(["asic_UK00", "asic_UK01"], type=pa.large_string()),
            pa.array([10.0, 0.0], type=pa.float64()),
            pa.array([["A", "B"], None], type=pa.list_(pa.large_string())),
        ],
        schema=static_schema,
    )
    times = [-60.0, 0.0, 60.0, 480.0, 600.0, 0.0]
    fields: list[pa.Field] = []
    arrays: list[pa.Array] = []
    derived_fields = {
        "hours_since_icu_admission",
        "delta_p_computed",
        "delta_p_computed_out_of_range",
    }
    operational_provenance_fields = {
        "__v3_source_file_id",
        "__v3_source_file_order",
        "__v3_source_row_number",
        "__v3_source_order",
        "__v3_source_schema_variant_id",
    }
    for name in loaded_policy.registry_variables:
        data_type = _dynamic_type(name, loaded_policy)
        metadata = {b"asic_v3_unit": b"synthetic"}
        if name in derived_fields:
            metadata[b"asic_v3_definition"] = (
                f"Synthetic derived definition for {name}.".encode()
            )
        fields.append(
            pa.field(
                name,
                data_type,
                metadata=metadata,
            )
        )
        arrays.append(pa.array(_dynamic_values(name, times), type=data_type))
    dynamic_schema = pa.schema(fields)
    dynamic = pa.Table.from_arrays(arrays, schema=dynamic_schema)

    dictionary_dir = (
        asic_root / "data/demo/contracts/harmonized_schema_dictionary/0.2"
    )
    dictionary_dir.mkdir(parents=True)
    dictionary_path = dictionary_dir / "variable_dictionary.parquet"
    dictionary_rows = [
        {
            "table": "dynamic",
            "variable": name,
            "definition": f"Synthetic frozen definition for {name}.",
        }
        for name in loaded_policy.registry_variables
        if name not in derived_fields | operational_provenance_fields
    ]
    pq.write_table(pa.Table.from_pylist(dictionary_rows), dictionary_path)
    dictionary_manifest = {
        "artifact": "asic_v3_frozen_harmonized_schema_dictionary",
        "contract_version": "0.2",
        "schema_frozen": True,
        "dictionary_frozen": True,
        "files": {
            "variable_dictionary.parquet": sha256_file(dictionary_path),
        },
    }
    (dictionary_dir / "freeze_manifest.json").write_text(
        json.dumps(dictionary_manifest, indent=2) + "\n", encoding="utf-8"
    )

    release_dir = asic_root / "data/demo/derived/releases/synthetic"
    release_dir.mkdir(parents=True)
    static_path = release_dir / "static.parquet"
    dynamic_path = release_dir / "dynamic.parquet"
    pq.write_table(static, static_path, compression="zstd")
    pq.write_table(dynamic, dynamic_path, compression="zstd")
    manifest = {
        "artifact": "asic_v3_core_derived_release",
        "artifact_version": "0.2",
        "dataset_context": "demo",
        "release_id": "synthetic",
        "status": "released_core_derived_layer",
        "cleaned_release_id": "cleaned-synthetic",
        "core_derived_contract_version": "0.2",
        "files": {
            "static": {
                "sha256": sha256_file(static_path),
                "row_count": 2,
                "schema_sha256": _schema_digest(static.schema),
            },
            "dynamic": {
                "sha256": sha256_file(dynamic_path),
                "row_count": 6,
                "schema_sha256": _schema_digest(dynamic.schema),
            },
        },
        "payload_byte_identity_preserved": True,
        "rows_or_stays_filtered": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    manifest_path = release_dir / "release_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    pointer = {
        "artifact": "asic_v3_current_core_derived_release",
        "artifact_version": "0.2",
        "dataset_context": "demo",
        "release_id": "synthetic",
        "core_derived_contract_version": "0.2",
        "cleaned_release_id": "cleaned-synthetic",
        "release_manifest": str(manifest_path),
        "release_manifest_sha256": sha256_file(manifest_path),
        "core_derived_layer_ready": True,
        "analysis_input_approved": True,
        "publication_ready": True,
        "external_data_export_authorized": False,
    }
    pointer_path = asic_root / "data/demo/derived/current_release.json"
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_path.write_text(json.dumps(pointer, indent=2) + "\n", encoding="utf-8")
    return config_path


def test_consolidated_evidence_is_read_only_and_batch_independent(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config_path = _write_synthetic_release(
        tmp_path,
        project_root / "asic/config/time_blocking/contract_evidence_8h.yaml",
    )
    config = load_time_blocking_evidence_config(config_path)
    result = run_time_blocking_contract_evidence(
        config,
        "synthetic-audit",
    )

    assert result.overall_status == "pending_human_review"
    assert not result.technical_blocking_findings
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    accounting = review["assignment_evidence"]["row_and_stay_accounting"]
    assert accounting["pre_admission_source_row_count"] == 1
    assert accounting["admission_source_row_count"] == 2
    assert accounting["post_admission_source_row_count"] == 3
    assert accounting["positive_exact_boundary_source_row_count"] == 1
    assert accounting["terminal_partial_block_count"] == 1
    assert accounting["terminal_partial_source_row_count"] == 1
    assert accounting["source_order_hospital_count"] == 2
    assert accounting["source_order_hospital_start_failure_count"] == 0
    assert (
        accounting["nonincreasing_source_order_within_hospital_row_count"]
        == 0
    )
    assert (
        accounting["noncontiguous_source_order_within_hospital_row_count"]
        == 0
    )
    assert accounting["source_order_terminal_value_sum"] == 6
    assert review["cross_batch_assignment_reproducible"] is True
    assert review["variable_profiles"]["heart_rate"]["unit"] == "synthetic"
    assert review["blocked_rows_written"] is False
    assert review["current_release_pointer_modified"] is False
    assert not (config.data_root / "derived/time_blocking").exists()
    assert result.private_report_directory.stat().st_mode & 0o777 == 0o700
    assert result.review_json_path.stat().st_mode & 0o777 == 0o640


def _authorize_synthetic_local_implementation(
    config_path: Path,
    project_root: Path,
) -> Path:
    evidence_config = load_time_blocking_evidence_config(config_path)
    evidence_result = run_time_blocking_contract_evidence(
        evidence_config,
        "20260808T114902Z",
    )
    review = json.loads(
        evidence_result.review_json_path.read_text(encoding="utf-8")
    )
    accounting = review["assignment_evidence"]["row_and_stay_accounting"]
    policy_dir = config_path.parents[1] / "time_blocking"
    evidence_policy_path = policy_dir / "contract_evidence_8h.yaml"

    contract = yaml.safe_load(
        (
            project_root
            / "asic/config/time_blocking/reviewed_8h_contract_0_1.yaml"
        ).read_text(encoding="utf-8")
    )
    contract["evidence"].update(
        {
            "review_relative_path": str(
                evidence_result.review_json_path.relative_to(
                    evidence_config.reports_root
                )
            ),
            "review_sha256": sha256_file(evidence_result.review_json_path),
            "expected_dynamic_rows": accounting["dynamic_row_count"],
            "expected_static_rows": review["static_accounting"]["row_count"],
            "expected_negative_time_rows": accounting[
                "pre_admission_source_row_count"
            ],
            "expected_total_grid_blocks": accounting["total_grid_block_count"],
            "expected_empty_grid_blocks": accounting[
                "total_empty_grid_block_count"
            ],
            "expected_terminal_partial_blocks": accounting[
                "terminal_partial_block_count"
            ],
            "expected_terminal_partial_source_rows": accounting[
                "terminal_partial_source_row_count"
            ],
        }
    )
    contract["immutable_inputs"].update(
        {
            "evidence_policy": evidence_policy_path.name,
            "evidence_policy_sha256": sha256_file(evidence_policy_path),
            "core_derived_release_id": "synthetic",
            "cleaned_release_id": "cleaned-synthetic",
            "harmonized_release_id": "harmonized-synthetic",
        }
    )
    contract_path = policy_dir / "reviewed_8h_contract_0_1.yaml"
    contract_path.write_text(
        yaml.safe_dump(contract, sort_keys=False), encoding="utf-8"
    )

    implementation = yaml.safe_load(
        (
            project_root
            / "asic/config/time_blocking/local_implementation_8h.yaml"
        ).read_text(encoding="utf-8")
    )
    implementation["contract"] = {
        "path": contract_path.name,
        "sha256": sha256_file(contract_path),
    }
    implementation_path = policy_dir / "local_implementation_8h.yaml"
    implementation_path.write_text(
        yaml.safe_dump(implementation, sort_keys=False), encoding="utf-8"
    )
    dataset = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    dataset["time_blocking_implementation_policy"] = (
        "../time_blocking/local_implementation_8h.yaml"
    )
    config_path.write_text(yaml.safe_dump(dataset), encoding="utf-8")
    return config_path


def test_local_candidate_build_and_independent_audit(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config_path = _write_synthetic_release(
        tmp_path,
        project_root / "asic/config/time_blocking/contract_evidence_8h.yaml",
    )
    _authorize_synthetic_local_implementation(config_path, project_root)
    config = load_time_blocking_build_config(config_path)
    build = run_time_blocking_candidate_build(config, "local-build")

    assert build.block_row_count == 5
    assert (build.candidate_directory / "blocks.parquet").is_file()
    assert not (config.data_root / "derived/time_blocking/8h/current_release.json").exists()
    manifest = json.loads(build.manifest_path.read_text(encoding="utf-8"))
    assert manifest["static_table_copied"] is False
    assert manifest["medication_dose_totals_emitted"] is False
    assert manifest["metrics"]["assigned_source_row_count"] == 6
    assert manifest["lineage"]["harmonized_dictionary_contract_version"] == "0.2"
    dictionary = pq.read_table(
        build.candidate_directory / "blocked_variable_dictionary.parquet"
    ).to_pylist()
    heart_rate_definition = next(
        row["source_definition"]
        for row in dictionary
        if row["source_variable"] == "heart_rate"
    )
    derived_definition = next(
        row["source_definition"]
        for row in dictionary
        if row["source_variable"] == "delta_p_computed"
    )
    assert heart_rate_definition == "Synthetic frozen definition for heart_rate."
    assert derived_definition == (
        "Synthetic derived definition for delta_p_computed."
    )
    provenance_definition = next(
        row["source_definition"]
        for row in dictionary
        if row["source_variable"] == "__v3_source_file_id"
    )
    assert provenance_definition.startswith(
        "Opaque deterministic source-file identifier"
    )

    implementation = yaml.safe_load(
        config.implementation_policy_path.read_text(encoding="utf-8")
    )
    implementation["execution"]["input_rows_per_batch"] = 3
    implementation["execution"]["output_rows_per_batch"] = 2
    config.implementation_policy_path.write_text(
        yaml.safe_dump(implementation, sort_keys=False), encoding="utf-8"
    )
    second = run_time_blocking_candidate_build(config, "local-build-batch-3")
    assert pq.read_table(
        build.candidate_directory / "blocks.parquet"
    ).equals(
        pq.read_table(second.candidate_directory / "blocks.parquet"),
        check_metadata=True,
    )
    assert pq.read_table(
        build.candidate_directory / "stay_block_summary.parquet"
    ).equals(
        pq.read_table(
            second.candidate_directory / "stay_block_summary.parquet"
        ),
        check_metadata=True,
    )

    audit = run_time_blocking_candidate_audit(
        config,
        build.run_id,
        "local-audit",
    )
    assert audit.overall_status == "pending_human_review"
    assert not audit.technical_blocking_findings
    assert audit.blocking_findings == (
        {
            "check": "time_blocking_candidate_promotion_approved",
            "details": "A technically passing candidate would still require explicit human promotion approval.",
        },
    )


def test_production_candidate_rejects_unapproved_run_ids(
    project_root: Path,
) -> None:
    config = load_time_blocking_build_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    with pytest.raises(
        HarmonizationError,
        match="Production time-blocking execution authorization is invalid",
    ):
        run_time_blocking_candidate_build(config, "must-not-run")
    with pytest.raises(
        HarmonizationError,
        match="Production time-blocking execution authorization is invalid",
    ):
        run_time_blocking_candidate_audit(config, "must-not-run", "must-not-run")
