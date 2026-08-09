from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.unit_resolution.medication_semantics import (
    EXPECTED_BOUNDARY,
    MedicationSemanticsAuditPolicy,
    _scan_values,
    load_medication_semantics_audit_config,
    load_medication_semantics_audit_policy,
)
from asic_pipeline.unit_resolution.reviewed_decisions import (
    load_reviewed_unit_decisions,
)


HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)


def test_shipped_medication_semantics_policy_is_read_only(
    project_root: Path,
) -> None:
    config = load_medication_semantics_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_medication_semantics_audit_policy(config.policy_path)

    assert policy.cleaned_release_id == "20260806T114234Z"
    assert policy.expected_source_variable_count == 25
    assert policy.expected_split_count == 9
    assert policy.expected_proposed_variable_count == 34
    assert policy.expected_conversion_scope_count == 4
    assert EXPECTED_BOUNDARY["write_reports_only"] is True
    assert EXPECTED_BOUNDARY["impute_null_as_zero"] is False
    assert EXPECTED_BOUNDARY["convert_zero_to_null"] is False
    assert EXPECTED_BOUNDARY["apply_carry_forward"] is False


def test_complete_scan_counts_zero_null_and_preserves_split_classes(
    tmp_path: Path,
    project_root: Path,
) -> None:
    reviewed = load_reviewed_unit_decisions(
        project_root
        / "asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml"
    )
    static_rows = []
    dynamic = {
        "stay_id_global": [],
        "hospital_id": [],
        **{variable: [] for variable in reviewed.medication_variables},
    }
    for hospital in HOSPITALS:
        stay = f"synthetic-{hospital}"
        static_rows.append(
            {"stay_id_global": stay, "hospital_id": hospital, "weight_kg": 70.0}
        )
        for value in (0.0, 1.0):
            dynamic["stay_id_global"].append(stay)
            dynamic["hospital_id"].append(hospital)
            for variable in reviewed.medication_variables:
                selected: float | None = value
                if hospital == "asic_UK01" and variable == "clonidine_iv_cont":
                    selected = None
                if (
                    hospital == "asic_UK02"
                    and variable == "prednisolone_iv_bolus"
                    and value == 1.0
                ):
                    selected = -1.0
                dynamic[variable].append(selected)
    static_path = tmp_path / "static.parquet"
    dynamic_path = tmp_path / "dynamic.parquet"
    pq.write_table(pa.Table.from_pylist(static_rows), static_path)
    dynamic_table = pa.Table.from_pydict(dynamic)
    pq.write_table(dynamic_table, dynamic_path)
    shipped = load_medication_semantics_audit_policy(
        project_root
        / "asic/config/unit_resolution/medication_value_semantics_audit.yaml"
    )
    policy = MedicationSemanticsAuditPolicy(
        version=shipped.version,
        cleaned_release_id=shipped.cleaned_release_id,
        expected_static_rows=len(static_rows),
        expected_dynamic_rows=dynamic_table.num_rows,
        reviewed_decisions_path=shipped.reviewed_decisions_path,
        reviewed_decisions_sha256=shipped.reviewed_decisions_sha256,
        derivation_policy_path=shipped.derivation_policy_path,
        derivation_policy_sha256=shipped.derivation_policy_sha256,
        expected_hospital_count=8,
        expected_source_variable_count=25,
        expected_split_count=9,
        expected_proposed_variable_count=34,
        expected_conversion_scope_count=4,
        rows_per_batch=3,
        private_directory_name=shipped.private_directory_name,
        review_directory_name=shipped.review_directory_name,
        private_artifact_version=shipped.private_artifact_version,
        review_artifact_version=shipped.review_artifact_version,
        source_path=shipped.source_path,
    )

    profiles, variables, conversions, splits, summary = _scan_values(
        dynamic_path,
        static_path,
        dynamic_table.schema,
        policy,
        reviewed,
        progress=None,
    )

    assert len(profiles) == 8 * 25
    assert len(variables) == 25
    assert len(conversions) == 4
    assert len(splits) == 9
    clonidine = next(row for row in variables if row["variable"] == "clonidine_iv_cont")
    assert clonidine["zero_count"] == 7
    assert clonidine["positive_count"] == 7
    assert clonidine["null_count"] == 2
    prednisolone = next(
        row for row in variables if row["variable"] == "prednisolone_iv_bolus"
    )
    assert prednisolone["negative_count"] == 1
    assert all(row["zero_to_null_count"] == 0 for row in conversions)
    assert all(row["null_to_zero_count"] == 0 for row in conversions)
    assert all(
        row["classification_conserved_in_parallel_target"] is True
        for row in splits
    )
    assert summary["nonzero_without_valid_weight_count"] == 0
