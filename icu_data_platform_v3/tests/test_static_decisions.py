from __future__ import annotations

import pyarrow as pa
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    harmonize_reviewed_static_boolean_batch,
    harmonize_reviewed_static_identifier_batch,
    harmonize_reviewed_static_numeric_batch,
    load_reviewed_static_decision_registry,
)


def test_reviewed_uk00_mortality_decision_is_evidence_bound_and_fail_closed(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_1.yaml"
    )

    assert registry.static_contract_audit_run_id == "20260805T112746Z"
    assert registry.registry_review_run_id == "20260805T104417Z"
    assert registry.composite_audit_run_id == "20260805T111026Z"
    assert registry.allow_synthetic_batch_transform is True
    assert registry.allow_production_reads is False
    assert registry.allow_artifact_writes is False
    assert registry.allow_hospital_concatenation is False
    assert registry.allow_union_schema_freeze is False
    with pytest.raises(HarmonizationError, match="partial static decision registry"):
        registry.require_production_ready()

    decision = registry.rule_for(
        "asic_UK00", "static", "KH-Sterblichkeit"
    )
    assert decision is not None
    assert decision.target == "hospital_mortality_reported"
    assert decision.output_value_type == "bool"
    assert decision.output_unit == "not_applicable"
    assert decision.evidence_nonempty_count == 3676


def test_reviewed_uk00_mortality_transform_is_nullable_and_blocks_unknowns(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_1.yaml"
    )
    decision = registry.rule_for(
        "asic_UK00", "static", "KH-Sterblichkeit"
    )
    assert decision is not None

    result = harmonize_reviewed_static_boolean_batch(
        pa.array(["true", " FALSE ", "", None, "protected-unknown"]),
        decision,
    )

    assert result.values.type == pa.bool_()
    assert result.values.to_pylist() == [True, False, None, None, None]
    assert result.parse_status.to_pylist() == [
        "approved_boolean_mapping",
        "approved_boolean_mapping",
        "literal_empty_missing",
        "source_schema_absent",
        "unresolved",
    ]
    assert result.metrics.input_cell_count == 5
    assert result.metrics.approved_categorical_count == 2
    assert result.metrics.literal_empty_missing_count == 1
    assert result.metrics.source_schema_absent_count == 1
    assert result.metrics.unresolved_count == 1
    assert result.metrics.cell_accounting_is_conserved is True
    with pytest.raises(HarmonizationError) as captured:
        result.require_resolved()
    assert "protected-unknown" not in str(captured.value)

    resolved = harmonize_reviewed_static_boolean_batch(
        pa.array(["TRUE", "false", "", None]),
        decision,
    )
    assert resolved.values.to_pylist() == [True, False, None, None]
    resolved.require_resolved()


def test_reviewed_static_registry_v02_preserves_v01_and_adds_approved_rules(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_2.yaml"
    )

    assert registry.version == "0.2"
    assert registry.static_contract_audit_run_id == "20260805T112746Z"
    assert len(registry.decisions) == 3
    assert registry.rule_for(
        "asic_UK00", "static", "KH-Sterblichkeit"
    ) is not None
    weight = registry.rule_for("asic_UK00", "static", "weightKg")
    assert weight is not None
    assert weight.target == "weight_kg"
    assert weight.output_value_type == "float64"
    assert weight.output_unit == "kg"
    hosp_los = registry.rule_for("asic_UK00", "static", "Liegedauer_KH")
    assert hosp_los is not None
    assert hosp_los.target == "hosp_los"
    assert hosp_los.output_unit == "day"
    assert registry.identifier_decision is not None
    assert registry.identifier_decision.local_target == "stay_id_local"
    assert registry.identifier_decision.global_target == "stay_id_global"
    assert registry.identifier_decision.static_row_count == 16054
    assert registry.identifier_decision.within_hospital_duplicate_count == 0
    assert registry.identifier_decision.cross_hospital_local_overlap_count == 3676
    with pytest.raises(HarmonizationError, match="partial static decision registry"):
        registry.require_production_ready()


def test_weight_decimal_comma_and_uk00_hosp_los_missing_rules(project_root) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_2.yaml"
    )
    weight = registry.rule_for("asic_UK00", "static", "weightKg")
    hosp_los = registry.rule_for("asic_UK00", "static", "Liegedauer_KH")
    assert weight is not None
    assert hosp_los is not None

    weight_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["82.5", "82,5", "", None, "protected-unknown"]),
        weight,
    )
    assert weight_result.values.to_pylist() == [82.5, 82.5, None, None, None]
    assert weight_result.parse_status.to_pylist() == [
        "direct_numeric",
        "decimal_comma_parsed",
        "literal_empty_missing",
        "source_schema_absent",
        "unresolved",
    ]
    assert weight_result.metrics.cell_accounting_is_conserved is True
    with pytest.raises(HarmonizationError) as captured:
        weight_result.require_resolved()
    assert "protected-unknown" not in str(captured.value)

    los_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["nan", "12", "", None, "NaN"]),
        hosp_los,
    )
    assert los_result.values.to_pylist() == [None, None, None, None, None]
    assert los_result.parse_status.to_pylist() == [
        "approved_missing_sentinel",
        "unresolved",
        "literal_empty_missing",
        "source_schema_absent",
        "unresolved",
    ]
    assert los_result.metrics.approved_missing_sentinel_count == 1
    assert los_result.metrics.unresolved_count == 2


def test_static_identifier_preserves_local_and_derives_compatible_global_id(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_2.yaml"
    )
    decision = registry.identifier_decision
    assert decision is not None

    uk00 = harmonize_reviewed_static_identifier_batch(
        pa.array(["1234", "stay-A"]), "asic_UK00", decision
    )
    assert uk00.stay_id_local.type == pa.large_string()
    assert uk00.stay_id_local.to_pylist() == ["1234", "stay-A"]
    assert uk00.stay_id_global.to_pylist() == ["1234:0", "stay-A:0"]
    uk00.require_resolved()

    uk01 = harmonize_reviewed_static_identifier_batch(
        pa.array(["1234"]), "asic_UK01", decision
    )
    assert uk01.stay_id_global.to_pylist() == ["1234:1"]
    assert set(uk00.stay_id_global.to_pylist()).isdisjoint(
        uk01.stay_id_global.to_pylist()
    )


def test_static_identifier_blocks_missing_ambiguous_and_duplicate_values(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_2.yaml"
    )
    decision = registry.identifier_decision
    assert decision is not None

    result = harmonize_reviewed_static_identifier_batch(
        pa.array(
            [
                "protected-repeat-id",
                "protected-repeat-id",
                "12:3",
                " padded ",
                "",
                None,
            ]
        ),
        "asic_UK02",
        decision,
    )
    assert result.stay_id_local.to_pylist() == [None] * 6
    assert result.stay_id_global.to_pylist() == [None] * 6
    assert result.metrics.duplicate_local_identifier_row_count == 2
    assert result.metrics.separator_collision_count == 1
    assert result.metrics.surrounding_whitespace_count == 1
    assert result.metrics.literal_empty_count == 1
    assert result.metrics.source_missing_count == 1
    assert result.metrics.accounting_is_conserved is True
    with pytest.raises(HarmonizationError) as captured:
        result.require_resolved()
    assert "protected-repeat-id" not in str(captured.value)
    assert "12:3" not in str(captured.value)

    with pytest.raises(HarmonizationError, match="no approved hospital suffix"):
        harmonize_reviewed_static_identifier_batch(
            pa.array(["1234"]), "asic_UK05", decision
        )


def test_reviewed_static_registry_v03_adds_evidence_scoped_day_variables(
    project_root,
) -> None:
    registry = load_reviewed_static_decision_registry(
        project_root
        / "asic/config/harmonization/reviewed_static_decisions_0_3.yaml"
    )

    assert registry.version == "0.3"
    assert len(registry.decisions) == 21
    assert registry.identifier_decision is not None
    assert registry.allow_production_reads is False
    assert registry.allow_artifact_writes is False

    icu_uk00 = registry.rule_for(
        "asic_UK00", "static", "Liegedauer_ICU"
    )
    assert icu_uk00 is not None
    icu_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["12.5", "", None, "protected-unknown"]),
        icu_uk00,
    )
    assert icu_result.values.to_pylist() == [12.5, None, None, None]
    assert icu_result.metrics.unresolved_count == 1

    icu_uk01 = registry.rule_for(
        "asic_UK01", "static", "Liegedauer_ICU"
    )
    assert icu_uk01 is not None
    unavailable_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["", None, "1"]),
        icu_uk01,
    )
    assert unavailable_result.values.to_pylist() == [None, None, None]
    assert unavailable_result.metrics.unresolved_count == 1

    dfd_uk08 = registry.rule_for(
        "asic_UK08", "static", "Dialyse_(dialysefreie_Tage)"
    )
    assert dfd_uk08 is not None
    sentinel_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["-1", "-1.0", "20"]),
        dfd_uk08,
    )
    assert sentinel_result.values.to_pylist() == [None, None, None]
    assert sentinel_result.metrics.approved_missing_sentinel_count == 2
    assert sentinel_result.metrics.unresolved_count == 1

    dfd_uk02 = registry.rule_for(
        "asic_UK02", "static", "Dialyse_(dialysefreie_Tage)"
    )
    assert dfd_uk02 is not None
    direct_result = harmonize_reviewed_static_numeric_batch(
        pa.array(["28", "0"]),
        dfd_uk02,
    )
    assert direct_result.values.to_pylist() == [28.0, 0.0]
    direct_result.require_resolved()
