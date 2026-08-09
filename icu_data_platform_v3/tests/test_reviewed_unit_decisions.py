from __future__ import annotations

from pathlib import Path

from asic_pipeline.unit_resolution.reviewed_decisions import (
    EXPECTED_ACTION_IDS,
    EXPECTED_BOUNDARY,
    EXPECTED_EXECUTION_CONTRACT,
    EXPECTED_MEDICATION_VALUE_SEMANTICS,
    EXPECTED_MEDICATION_VARIABLES,
    EXPECTED_SPLITS,
    load_reviewed_unit_decisions,
)


def _load(project_root: Path):
    return load_reviewed_unit_decisions(
        project_root
        / "asic/config/unit_resolution/reviewed_unit_decisions_0_2.yaml"
    )


def test_reviewed_decisions_bind_complete_audit_and_all_conversions(
    project_root: Path,
) -> None:
    reviewed = _load(project_root)

    assert reviewed.version == "0.2"
    assert reviewed.audit_run_id == "20260807T083117Z"
    assert set(reviewed.approved_harmonization_action_ids) == EXPECTED_ACTION_IDS
    assert len(reviewed.approved_harmonization_action_ids) == 9
    assert reviewed.execution_contract == EXPECTED_EXECUTION_CONTRACT
    assert reviewed.medication_variables == EXPECTED_MEDICATION_VARIABLES
    assert reviewed.medication_value_semantics == EXPECTED_MEDICATION_VALUE_SEMANTICS
    assert EXPECTED_BOUNDARY["authorize_clinical_data_build"] is False
    assert EXPECTED_BOUNDARY["drop_source_values"] is False


def test_all_audited_mask_scopes_are_replaced_by_semantic_splits(
    project_root: Path,
) -> None:
    reviewed = _load(project_root)
    splits = {
        (row["hospital"], row["source_variable"]): row
        for row in reviewed.semantic_splits
    }

    assert set(splits) == set(EXPECTED_SPLITS)
    assert len(splits) == 9
    assert reviewed.execution_contract["cleaning_mask_split_source_values"] is False
    assert reviewed.execution_contract["retain_parallel_variables_in_cleaned_layer"] is True
    assert all(row["canonical_variable"] == row["source_variable"] for row in splits.values())
    assert all(row["target_value_type"] == "float64" for row in splits.values())


def test_resolved_and_unresolved_parallel_variables_are_explicit(
    project_root: Path,
) -> None:
    reviewed = _load(project_root)
    by_target = {row["target_variable"]: row for row in reviewed.semantic_splits}

    resolved = {
        "clonidine_iv_cont_weight_normalized": "ug_per_kg_per_h",
        "epinephrine_iv_cont_absolute": "ug_per_min",
        "ketanest_iv_cont_weight_normalized": "mg_per_kg_per_h",
        "morphine_iv_cont_weight_normalized": "ug_per_kg_per_h",
        "norepinephrine_iv_cont_absolute": "ug_per_min",
        "propofol_iv_cont_weight_normalized": "mg_per_kg_per_h",
    }
    for target, unit in resolved.items():
        assert by_target[target]["target_unit"] == unit
        assert by_target[target]["analysis_eligibility"] == "eligible"

    unresolved = {
        "hydrocortisone_iv_bolus_source_uk08",
        "prednisolone_iv_bolus_source_uk02",
        "sufentanil_iv_cont_source_uk00",
    }
    for target in unresolved:
        assert by_target[target]["target_unit"] == "unresolved_source_scale"
        assert (
            by_target[target]["analysis_eligibility"]
            == "ineligible_pending_definition"
        )


def test_parallel_targets_do_not_collide_with_existing_variables(
    project_root: Path,
) -> None:
    reviewed = _load(project_root)
    targets = [row["target_variable"] for row in reviewed.semantic_splits]

    assert len(targets) == len(set(targets))
    assert not set(targets).intersection(reviewed.base_candidate.variables)


def test_medication_zero_and_null_semantics_are_fail_closed(
    project_root: Path,
) -> None:
    reviewed = _load(project_root)

    assert len(reviewed.medication_variables) == 25
    assert reviewed.medication_value_semantics["null_to_zero_imputation"] == "prohibited"
    assert reviewed.medication_value_semantics["zero_to_null_conversion"] == "prohibited"
    assert reviewed.medication_value_semantics["null_implies_medication_inactive"] is False
    assert (
        reviewed.medication_value_semantics["carry_forward_or_interval_imputation"]
        == "requires_separate_reviewed_contract"
    )
