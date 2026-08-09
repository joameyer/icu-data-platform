from __future__ import annotations

from pathlib import Path

from asic_pipeline.unit_resolution.reviewed_medication_semantics import (
    EXPECTED_BOUNDARY,
    EXPECTED_EVIDENCE,
    EXPECTED_NEGATIVE_EVIDENCE,
    EXPECTED_NEGATIVE_RULE,
    load_reviewed_medication_semantics,
)


def test_reviewed_medication_semantics_bind_complete_evidence(
    project_root: Path,
) -> None:
    reviewed = load_reviewed_medication_semantics(
        project_root
        / "asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml"
    )

    assert reviewed.version == "0.2"
    assert reviewed.evidence == EXPECTED_EVIDENCE
    assert reviewed.negative_evidence == EXPECTED_NEGATIVE_EVIDENCE
    assert reviewed.negative_cleaning_rule == EXPECTED_NEGATIVE_RULE
    assert reviewed.evidence["explicit_zero_count"] == 5_013_108
    assert reviewed.evidence["negative_value_count"] == 201


def test_general_negative_rule_preserves_provenance_and_zero(
    project_root: Path,
) -> None:
    reviewed = load_reviewed_medication_semantics(
        project_root
        / "asic/config/unit_resolution/reviewed_medication_value_semantics_0_2.yaml"
    )
    rule = reviewed.negative_cleaning_rule

    assert rule["selection"] == "dictionary_medication_or_therapy_variable_true"
    assert rule["finite_negative_condition"] == "value_less_than_zero"
    assert rule["action"] == "set_null_and_count_by_hospital_and_variable"
    assert rule["layer"] == "cleaned"
    assert rule["preserve_negative_in_harmonized_layer"] is True
    assert rule["preserve_zero"] is True
    assert rule["take_absolute_value"] is False
    assert rule["clip_negative_to_zero"] is False
    assert EXPECTED_BOUNDARY["activate_cleaning_policy_0_2"] is False
    assert EXPECTED_BOUNDARY["write_clinical_data"] is False
