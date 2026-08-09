from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.derivation.review import CleanedReleaseInput
from asic_pipeline.unit_resolution import (
    load_candidate_unit_decisions,
    load_unit_decision_audit_config,
    load_unit_decision_audit_policy,
)
from asic_pipeline.unit_resolution.decision_audit import (
    EXPECTED_BOUNDARY,
    HOSPITALS,
    _add_values,
    _build_action_evidence,
    _build_mask_evidence,
    _scan_candidate_evidence,
    _target_variables,
)


def test_shipped_unit_decision_audit_policy_is_complete_and_read_only(
    project_root: Path,
) -> None:
    config = load_unit_decision_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_unit_decision_audit_policy(config.policy_path)

    assert config.dataset_context == "production"
    assert policy.version == "0.2"
    assert policy.cleaned_release_id == "20260806T114234Z"
    assert policy.expected_variable_count == 72
    assert policy.expected_action_count == 9
    assert policy.expected_mask_count == 9
    assert policy.expected_target_variable_count == 16
    assert policy.expected_weight_action_count == 1
    assert EXPECTED_BOUNDARY["write_reports_only"] is True
    assert EXPECTED_BOUNDARY["write_clinical_data"] is False
    assert EXPECTED_BOUNDARY["activate_candidate_decisions"] is False
    assert EXPECTED_BOUNDARY["convert_persisted_values"] is False
    assert EXPECTED_BOUNDARY["mask_persisted_values"] is False


def test_candidate_scan_profiles_all_scopes_and_audits_weight_linkage(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = load_unit_decision_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    shipped = load_unit_decision_audit_policy(config.policy_path)
    decisions = load_candidate_unit_decisions(shipped.decisions_path)
    variables = _target_variables(decisions)
    policy = replace(
        shipped,
        expected_static_rows=len(HOSPITALS),
        expected_dynamic_rows=len(HOSPITALS),
        rows_per_batch=3,
        sample_capacity=50,
        sample_rows_per_batch=3,
    )

    stays = [f"synthetic-{hospital}" for hospital in HOSPITALS]
    static_schema = pa.schema(
        [
            pa.field("stay_id_global", pa.large_string()),
            pa.field("hospital_id", pa.large_string()),
            pa.field("weight_kg", pa.float64()),
        ]
    )
    static_path = tmp_path / "static.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(stays, type=pa.large_string()),
                pa.array(HOSPITALS, type=pa.large_string()),
                pa.array([80.0, None, None, None, None, None, None, None]),
            ],
            schema=static_schema,
        ),
        static_path,
    )

    dynamic_schema = pa.schema(
        [
            pa.field("stay_id_global", pa.large_string()),
            pa.field("hospital_id", pa.large_string()),
            *(pa.field(variable, pa.float64()) for variable in variables),
        ]
    )
    values_by_variable = {
        variable: [1.0] * len(HOSPITALS) for variable in variables
    }
    values_by_variable["albumin"][-1] = 100.0
    values_by_variable["vasopressin_iv_cont"][0] = 0.001
    dynamic_path = tmp_path / "dynamic.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(stays, type=pa.large_string()),
                pa.array(HOSPITALS, type=pa.large_string()),
                *(
                    pa.array(values_by_variable[variable], type=pa.float64())
                    for variable in variables
                ),
            ],
            schema=dynamic_schema,
        ),
        dynamic_path,
    )
    source = CleanedReleaseInput(
        release_directory=tmp_path,
        release_manifest_path=tmp_path / "release_manifest.json",
        release_manifest={},
        source_files={"static": static_path, "dynamic": dynamic_path},
        schemas={"static": static_schema, "dynamic": dynamic_schema},
    )

    pre, post, peers, accounting, weight_accounting = _scan_candidate_evidence(
        source, policy, decisions, None
    )
    actions = _build_action_evidence(
        decisions, pre, post, peers, weight_accounting
    )
    masks = _build_mask_evidence(decisions, pre, peers)

    assert len(pre) == len(HOSPITALS) * 16
    assert len(post) == 9
    assert len(peers) == 16
    assert len(actions) == 9
    assert len(masks) == 9
    assert accounting["dynamic_rows_scanned"] == len(HOSPITALS)
    assert accounting["uk00_valid_weight_count"] == 1
    albumin = next(
        row for row in actions if row["decision_id"] == "UK08-ALBUMIN-MG-L-TO-DG-L"
    )
    assert albumin["median_before"] == 100.0
    assert albumin["median_after"] == 1.0
    vasopressin = next(
        row
        for row in actions
        if row["decision_id"] == "UK00-VASOPRESSIN-IU-KG-MIN-TO-IU-MIN"
    )
    assert vasopressin["median_after"] == 0.08
    assert vasopressin["converted_with_weight_count"] == 1
    assert vasopressin["missing_or_invalid_weight_count"] == 0
    assert all(row["values_modified_by_this_audit"] is False for row in actions)
    assert all(row["values_modified_by_this_audit"] is False for row in masks)


def test_finite_aware_sampling_keeps_every_sparse_value(
    project_root: Path,
) -> None:
    from asic_pipeline.harmonization.consolidated_audit import NumericAccumulator

    config = load_unit_decision_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = replace(
        load_unit_decision_audit_policy(config.policy_path),
        sample_capacity=100,
        sample_rows_per_batch=4,
    )
    values = pa.array([None] * 49_999 + [160.0], type=pa.float64())
    accumulator = NumericAccumulator(100, 17)

    _add_values(accumulator, values, policy)
    profile = accumulator.profile("dynamic", "asic_UK03", "furosemide", "test")

    assert profile["finite_count"] == 1
    assert profile["median"] == 160.0
    assert profile["quantile_sample_count"] == 1
    assert profile["quantiles_are_exact"] is True
