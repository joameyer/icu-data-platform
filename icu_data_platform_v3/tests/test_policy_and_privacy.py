from __future__ import annotations

from pathlib import Path

import pytest

from asic_pipeline.config import PRODUCTION_RAW_ROOT, load_inventory_config
from asic_pipeline.ingestion import load_ingestion_config, load_ingestion_policy
from asic_pipeline.errors import InventoryError
from asic_pipeline.inventory.classifier import discover_raw_files
from asic_pipeline.inventory.policy import load_inventory_policy
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.schema_tokens import load_schema_token_config


def test_policy_matches_both_flat_dynamic_filename_conventions(
    synthetic_inventory: tuple[object, Path], project_root: Path
) -> None:
    _, raw_root = synthetic_inventory
    policy = load_inventory_policy(project_root / "asic/config/inventory/policy.yaml")
    discovery = discover_raw_files(raw_root, policy)
    dynamic = [
        item for item in discovery.files if item.classification == "dynamic_candidate"
    ]

    assert len(dynamic) == 8
    assert {item.filename_pattern_scope for item in dynamic} == {"00", "default"}


def test_review_payload_guard_fails_closed() -> None:
    with pytest.raises(InventoryError, match="Protected field"):
        assert_review_payload_is_safe(
            {"safe_count": 2, "nested": {"filename_stay_id": "protected"}}
        )


def test_shipped_configs_keep_outputs_inside_v3(project_root: Path) -> None:
    for context in ("demo", "production"):
        config = load_inventory_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        assert config.dataset_context == context
        assert config.paths.reports.is_relative_to(project_root / "asic")
        assert config.paths.runs.is_relative_to(project_root / "asic")
        ingestion = load_ingestion_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        assert ingestion.data_root.is_relative_to(project_root / "asic")
        assert ingestion.dataset_context == context
        schema_tokens = load_schema_token_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        assert schema_tokens.policy_path.is_relative_to(project_root / "asic")
        assert schema_tokens.dataset_context == context
        if context == "production":
            assert config.paths.raw_root == PRODUCTION_RAW_ROOT.resolve()


def test_reviewed_raw_anomalies_are_exactly_scoped(project_root: Path) -> None:
    policy = load_inventory_policy(project_root / "asic/config/inventory/policy.yaml")

    header = [f"column_{index}" for index in range(125)]
    header[121] = "ARDS_Diagnose_App"
    header[122] = "ARDS_Diagnose_App"
    rule = policy.reviewed_duplicate_header_for(
        "00",
        tuple(header),
        ("ARDS_Diagnose_App",),
    )
    assert rule is not None
    assert rule.ingestion_action == "preserve_as_separate_positional_source_columns"
    assert rule.translation_action == "drop_second_only_if_all_empty_revalidated"

    header[122] = "different_column"
    assert (
        policy.reviewed_duplicate_header_for(
            "00", tuple(header), ("ARDS_Diagnose_App",)
        )
        is None
    )

    archive = policy.provisional_archive_exclusion_for(
        "00",
        290741496,
        "8d277b9c16bde6ca404203da6148d01cbd717619f299cd60e95b860fc6687084",
    )
    assert archive is not None
    assert archive.ingestion_action == "exclude"
    assert archive.approval_status == "provisional_pending_owner_confirmation"
    assert policy.provisional_archive_exclusion_for("00", 290741495, archive.expected_sha256) is None


def test_hospital_mapping_contract_includes_approved_uk01(project_root: Path) -> None:
    policy = load_inventory_policy(project_root / "asic/config/inventory/policy.yaml")

    assert {
        mapping.source_folder: mapping.canonical_hospital_id
        for mapping in policy.hospital_mappings
    } == {
        "00": "asic_UK00",
        "01": "asic_UK01",
        "02": "asic_UK02",
        "03": "asic_UK03",
        "04": "asic_UK04",
        "06": "asic_UK06",
        "07": "asic_UK07",
        "08": "asic_UK08",
    }
    uk01 = policy.hospital_mapping_for("01")
    assert uk01 is not None
    assert uk01.approval_status == "approved_by_data_owner"
    assert uk01.cohort_action == "include"
    assert uk01.evidence["identity_confirmed"] is True
    assert uk01.evidence["cohort_inclusion_confirmed"] is True


def test_lossless_ingestion_policy_is_fail_closed(project_root: Path) -> None:
    policy = load_ingestion_policy(
        project_root / "asic/config/ingestion/policy.yaml"
    )

    assert policy.required_inventory_artifact_version == "0.4"
    assert policy.policy_version == "0.2"
    assert policy.ingestion_manifest_version == "0.2"
    assert policy.allowed_inventory_blockers == (
        "provisional_archive_owner_confirmation",
    )
    assert "provisionally_excluded_archive" in (
        policy.source_selection.excluded_classifications
    )
    assert policy.source_selection.revalidate_source_sha256 is True
    assert policy.source_selection.revalidate_header_and_row_count is True
    assert len(policy.provenance_columns) == 10
    duplicate_rule = policy.duplicate_physical_name_rule_for(
        "00",
        "dynamic",
        "ARDS_Diagnose_App",
        2,
    )
    assert duplicate_rule is not None
    assert duplicate_rule.physical_names == (
        "ARDS_Diagnose_App_col1",
        "ARDS_Diagnose_App_col2",
    )
