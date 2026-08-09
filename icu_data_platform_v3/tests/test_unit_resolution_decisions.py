from __future__ import annotations

from pathlib import Path

from asic_pipeline.unit_resolution.decisions import (
    EXPECTED_PRIORITY_RANGE_VARIABLES,
    EXPECTED_VARIABLES,
    SCORE_VARIABLES,
    load_candidate_unit_decisions,
)


def _load(project_root: Path):
    return load_candidate_unit_decisions(
        project_root
        / "asic/config/unit_resolution/candidate_unit_decisions_0_2.yaml"
    )


def test_candidate_unit_decisions_cover_every_formerly_unresolved_variable(
    project_root: Path,
) -> None:
    decisions = _load(project_root)

    assert set(decisions.variables) == EXPECTED_VARIABLES
    assert len(decisions.variables) == 72
    assert all(row["unit"] != "unresolved" for row in decisions.variables.values())
    assert all(row["eligibility"] == "eligible" for row in decisions.variables.values())


def test_all_sofa_and_isofa_fields_are_eligible_score_points(
    project_root: Path,
) -> None:
    decisions = _load(project_root)

    assert len(SCORE_VARIABLES) == 16
    for variable in SCORE_VARIABLES:
        assert decisions.variables[variable]["unit"] == "score_point"
        assert decisions.variables[variable]["eligibility"] == "eligible"
    assert (
        decisions.variables["sofa_score_without_gcs"]["caveat"]
        == "The current production column is all missing; availability does not alter its unit or eligibility."
    )


def test_reference_units_and_hospital_actions_are_explicit(
    project_root: Path,
) -> None:
    decisions = _load(project_root)
    units = {name: row["unit"] for name, row in decisions.variables.items()}

    assert units["albumin"] == "dg_per_L"
    assert units["bilirubin_total"] == "umol_per_L"
    assert units["creatinine"] == "umol_per_L"
    assert units["hemoglobin"] == "mmol_per_L"
    assert units["platelets"] == "10e9_per_L"
    assert units["urea"] == "mmol_per_L"
    assert units["wbc"] == "10e9_per_L"

    actions = {
        row["decision_id"]: row
        for row in decisions.hospital_harmonization_actions
    }
    assert actions["UK08-ALBUMIN-MG-L-TO-DG-L"]["factor"] == 0.01
    assert actions["UK03-D-DIMER-UG-ML-TO-NG-ML"]["factor"] == 1000.0
    assert actions["UK08-D-DIMER-UG-ML-TO-NG-ML"]["factor"] == 1000.0
    assert actions["UK03-TROPONIN-NG-L-TO-NG-ML"]["factor"] == 0.001
    assert actions["UK08-UREA-BUN-MG-DL-TO-MMOL-L"]["factor"] == 2.8


def test_unrecoverable_site_definitions_are_cleaning_masks(
    project_root: Path,
) -> None:
    decisions = _load(project_root)
    masks = {
        (row["hospital"], row["variable"])
        for row in decisions.hospital_cleaning_masks
    }

    assert ("asic_UK03", "norepinephrine_iv_cont") in masks
    assert ("asic_UK03", "epinephrine_iv_cont") in masks
    assert ("asic_UK08", "clonidine_iv_cont") in masks
    assert ("asic_UK08", "propofol_iv_cont") in masks
    assert ("asic_UK00", "sufentanil_iv_cont") in masks


def test_priority_cleaning_decisions_conserve_reviewed_accounting(
    project_root: Path,
) -> None:
    decisions = _load(project_root)

    assert set(decisions.priority_cleaning_range_decisions) == (
        EXPECTED_PRIORITY_RANGE_VARIABLES
    )
    assert (
        sum(
            row["legacy_outside_count"]
            for row in decisions.priority_cleaning_range_decisions.values()
        )
        == 10_021
    )
    assert (
        decisions.priority_cleaning_range_decisions["sofa_score_without_gcs"][
            "rule"
        ]
        == "preserve_schema_and_unit_regardless_of_current_missingness"
    )
