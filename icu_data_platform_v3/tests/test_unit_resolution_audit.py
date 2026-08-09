from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.derivation.review import CleanedReleaseInput
from asic_pipeline.unit_resolution import (
    load_unit_resolution_audit_config,
    load_unit_resolution_audit_policy,
)
from asic_pipeline.unit_resolution.audit import (
    EXPECTED_BOUNDARY,
    EXPECTED_DECISION_FRAMEWORK,
    HOSPITALS,
    _aggregate_priority_ranges,
    _scan_cleaned_dynamic,
)


def test_shipped_unit_resolution_policy_is_complete_and_read_only(
    project_root: Path,
) -> None:
    config = load_unit_resolution_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_unit_resolution_audit_policy(config.policy_path)

    assert config.dataset_context == "production"
    assert policy.cleaned_release_id == "20260806T114234Z"
    assert policy.harmonized_contract_version == "0.1"
    assert policy.expected_unresolved_count == 72
    assert policy.expected_priority_count == 12
    assert sum(dict(policy.expected_priority_outside_counts).values()) == 10_021
    assert EXPECTED_BOUNDARY["write_reports_only"] is True
    assert EXPECTED_BOUNDARY["write_clinical_data"] is False
    assert EXPECTED_BOUNDARY["apply_unit_conversions"] is False
    assert EXPECTED_BOUNDARY["apply_legacy_ranges"] is False
    assert EXPECTED_BOUNDARY["mask_values"] is False
    assert EXPECTED_DECISION_FRAMEWORK["distribution_evidence_can_establish_unit"] is False
    assert EXPECTED_DECISION_FRAMEWORK["preserve_values_until_unit_is_resolved"] is True


def test_cleaned_scan_profiles_every_hospital_and_preserves_range_findings(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = load_unit_resolution_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    shipped = load_unit_resolution_audit_policy(config.policy_path)
    policy = replace(
        shipped,
        expected_dynamic_rows=len(HOSPITALS),
        rows_per_batch=3,
        sample_capacity=20,
        sample_rows_per_batch=3,
    )
    dynamic = tmp_path / "dynamic.parquet"
    schema = pa.schema(
        [
            pa.field("hospital_id", pa.large_string()),
            pa.field("albumin", pa.float64()),
        ]
    )
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array(HOSPITALS, type=pa.large_string()),
                pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 999.0]),
            ],
            schema=schema,
        ),
        dynamic,
    )
    source = CleanedReleaseInput(
        release_directory=tmp_path,
        release_manifest_path=tmp_path / "release_manifest.json",
        release_manifest={},
        source_files={"dynamic": dynamic},
        schemas={"dynamic": schema},
    )
    rule = {
        "legacy_name": "Albumin",
        "hard_min": 0.0,
        "hard_max": 10.0,
        "invalid_zero": False,
        "description": "synthetic rule",
    }

    profiles, range_rows, hospital_rows = _scan_cleaned_dynamic(
        source,
        policy,
        ({"variable": "albumin"},),
        {"albumin": rule},
        None,
    )
    summaries = _aggregate_priority_ranges(range_rows, {"albumin": rule})

    assert len(profiles) == len(HOSPITALS)
    assert all(row["finite_count"] == 1 for row in profiles)
    assert hospital_rows == {hospital: 1 for hospital in HOSPITALS}
    assert summaries[0]["finite_count"] == len(HOSPITALS)
    assert summaries[0]["outside_legacy_range_count"] == 1
    assert summaries[0]["values_modified_by_this_audit"] is False
