from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from asic_pipeline.cli import main
from asic_pipeline.contracts import load_input_contract
from asic_pipeline.errors import ContractError
from asic_pipeline.translations import (
    MERGE_POLICY_BINARY_OR,
    load_translation_registry,
)


PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT_PATH = (
    PROJECT_ROOT / "asic" / "config" / "pooled" / "contract.yaml"
)
REGISTRY_PATH = (
    PROJECT_ROOT
    / "asic"
    / "config"
    / "pooled_to_translated"
    / "policy.yaml"
)
REFERENCE_PATH = PROJECT_ROOT / "asic" / "docs" / "pooled_to_translated.md"


def test_registry_covers_every_source_column_once() -> None:
    contract = load_input_contract(CONTRACT_PATH)
    registry = load_translation_registry(REGISTRY_PATH, contract)

    assert registry.policy_version == "1.3"
    assert registry.status == "approved_for_pooled_to_translated"
    assert (
        registry.documentation
        == "asic/docs/pooled_to_translated.md"
    )
    assert len(registry.static) == 21
    assert len(registry.dynamic) == 139
    assert registry.collision_group_count == 10
    assert registry.approved_merge_count == 8
    assert registry.unresolved_collision_group_count == 2
    assert len(registry.drop_rules) == 1
    assert len(registry.value_mappings_for("static")) == 6
    assert registry.value_mappings_for("dynamic") == ()
    assert len(registry.numeric_missing_sentinel_rules_for("static")) == 3
    assert registry.numeric_missing_sentinel_rules_for("dynamic") == ()
    assert len(registry.hospital_specific_semantic_split_rules) == 1
    assert [group.name for group in registry.static_output_groups] == [
        "identifiers_and_study_timing",
        "demographics",
        "stay_and_outcomes",
    ]
    assert [group.name for group in registry.dynamic_output_groups] == [
        "identifiers_and_time",
        "physiological_values",
        "mechanical_ventilation",
        "laboratory_values",
        "intravenous_medications",
        "other_therapy_and_review_fields",
        "sofa",
        "isofa",
    ]
    assert len(registry.output_order_for("static")) == 22
    assert len(registry.output_order_for("dynamic")) == 132
    assert registry.categorical_columns_for("static") == (
        "study_implementation_phase",
        "sex",
        "weight_group",
        "height_group",
        "bmi_group",
        "icu_readmit",
        "age_group",
        "discharge_status",
        "death_status",
        "hospital_mortality_reported",
    )
    assert registry.categorical_columns_for("dynamic") == (
        "ards_diagnosis_app",
        "ecmo",
        "position_therapy",
        "severity_read_confirmation",
        "therapy_read_confirmation_utc",
    )
    assert registry.output_order_for("dynamic")[:5] == (
        "stay_id_global",
        "hospital_id",
        "hospital_code_source",
        "minutes_since_icu_admission",
        "anchored_time_since_icu_admission",
    )
    assert registry.output_order_for("dynamic")[-7:] == (
        "isofa_cardiovascular",
        "isofa_liver",
        "isofa_respiratory",
        "isofa_renal",
        "isofa_thrombocyte",
        "isofa_cns",
        "isofa_total_score",
    )
    hospital_mortality = registry.value_mapping_for(
        "static",
        "hospital_mortality_reported",
    )
    assert hospital_mortality is not None
    assert hospital_mortality.output_arrow_type == "boolean"
    assert hospital_mortality.mapping() == {"false": False, "true": True}

    bmi = registry.value_mapping_for("static", "bmi_group")
    assert bmi is not None
    assert bmi.mapping() == {
        "L": "underweight",
        "M": "normal_weight",
        "P": "overweight",
        "1": "obesity_class_1",
        "2": "obesity_class_2",
        "3": "obesity_class_3",
        "X": None,
        "NAN": None,
    }
    dialysis_free_days = registry.numeric_missing_sentinel_rule_for(
        "static",
        "dialysis_free_days",
    )
    assert dialysis_free_days is not None
    assert dialysis_free_days.values == (-1,)
    hosp_los = registry.numeric_missing_sentinel_rule_for("static", "hosp_los")
    assert hosp_los is not None
    assert hosp_los.values == (-1,)
    semantic_split = registry.hospital_specific_semantic_split_rules[0]
    assert semantic_split.name == "uk00_vt_per_ideal_bw_total"
    assert semantic_split.table == "dynamic"
    assert semantic_split.source_column == (
        "individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht"
    )
    assert semantic_split.base_target_name == "vt_per_kg_ideal_body_weight"
    assert semantic_split.new_target_name == "vt_per_ideal_bw_total"
    assert semantic_split.selected_hospital_codes == (0,)
    dynamic_order = registry.output_order_for("dynamic")
    assert dynamic_order.index("vt_per_ideal_bw_total") == (
        dynamic_order.index("vt_per_kg_ideal_body_weight") + 1
    )


def test_numeric_missing_sentinel_rejects_nonnumeric_target(
    tmp_path: Path,
) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["numeric_missing_sentinels"]["static"]["sex"] = {
        "status": "approved_missing_sentinel",
        "values": [-1],
        "reason": "Invalid test rule.",
    }
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="requires numeric source columns"):
        load_translation_registry(invalid_path, contract)


def test_semantic_split_rejects_source_target_mismatch(tmp_path: Path) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["hospital_specific_semantic_splits"][
        "uk00_vt_per_ideal_bw_total"
    ]["base_target_name"] = "vt_per_kg"
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="must translate to base_target_name"):
        load_translation_registry(invalid_path, contract)


def test_semantic_split_rejects_hospital_outside_contract(
    tmp_path: Path,
) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["hospital_specific_semantic_splits"][
        "uk00_vt_per_ideal_bw_total"
    ]["selected_hospital_codes"] = [1]
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="hospitals outside the contract"):
        load_translation_registry(invalid_path, contract)


def test_readable_reference_contains_every_executable_column_mapping() -> None:
    contract = load_input_contract(CONTRACT_PATH)
    registry = load_translation_registry(REGISTRY_PATH, contract)
    reference = REFERENCE_PATH.read_text(encoding="utf-8")

    assert registry.documentation == "asic/docs/pooled_to_translated.md"
    for table in ("static", "dynamic"):
        for entry in registry.entries_for(table):
            output = (
                "*drop after all-missing check*"
                if entry.english_name is None
                else f"`{entry.english_name}`"
            )
            assert f"| `{entry.source_name}` | {output} |" in reference


def test_only_reviewed_aliases_share_targets() -> None:
    contract = load_input_contract(CONTRACT_PATH)
    registry = load_translation_registry(REGISTRY_PATH, contract)
    dynamic = {
        entry.source_name: entry.english_name for entry in registry.dynamic
    }

    approved_aliases = [
        ("CK", "Creatinkinase"),
        ("IL-6", "Interleukin_6"),
        ("NT-proBNP", "NT-pro_BNP"),
        ("LesebestaetigugnTherapie_utc", "LesebestaetigungTherapie_utc"),
        ("Koerperkerntemperatur", "Körpertemperatur"),
        ("ECMO_FiO2", "Gaszusammensetzung_(%O2)"),
        ("Lymphocyten", "Lymphozyten_prozentual"),
        ("pH_Wert_(ohne_Temp-Korrektur)_arteriell", "pH_arteriell"),
    ]
    for left, right in approved_aliases:
        assert dynamic[left] == dynamic[right]

    unresolved_pairs = [
        ("DeltaP", "deltaP"),
        ("Organversagen:_SOFA_Score_ohne_GCS", "SOFA"),
    ]
    for left, right in unresolved_pairs:
        assert dynamic[left] != dynamic[right]
    assert dynamic["ARDS_Diagnose_App_duplicated_0"] is None
    assert dynamic["DeltaP"] == "delta_p_computed"
    static = {entry.source_name: entry.english_name for entry in registry.static}
    assert static["Phase"] == "study_implementation_phase"

    nt_probnp = next(
        rule for rule in registry.merge_rules if rule.name == "nt_probnp"
    )
    assert nt_probnp.allowed_nonmissing_hospitals == ()
    therapy_confirmation = next(
        rule
        for rule in registry.merge_rules
        if rule.name == "therapy_confirmation_typo_variants"
    )
    assert therapy_confirmation.conflict_policy == MERGE_POLICY_BINARY_OR


def test_unapproved_duplicate_english_name_is_rejected(tmp_path: Path) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["translations"]["dynamic"]["BNP"] = raw["translations"]["dynamic"][
        "CK"
    ]
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="unapproved duplicate English"):
        load_translation_registry(invalid_path, contract)


def test_output_groups_require_each_translated_column_exactly_once(
    tmp_path: Path,
) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    groups = raw["translated_output_groups"]["dynamic"]
    groups["physiological_values"].remove("heart_rate")
    groups["laboratory_values"].append("hospital_id")
    groups["laboratory_values"].append("creatinkinase")
    invalid_path = tmp_path / "invalid_output_groups.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(
        ContractError,
        match="must contain every translated output exactly once",
    ) as error:
        load_translation_registry(invalid_path, contract)

    message = str(error.value)
    assert "hospital_id" in message
    assert "heart_rate" in message
    assert "creatinkinase" in message


def test_categorical_mapping_rejects_duplicate_normalized_sources(
    tmp_path: Path,
) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["categorical_value_mappings"]["static"]["sex"]["values"]["m"] = (
        "male"
    )
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="duplicate normalized source"):
        load_translation_registry(invalid_path, contract)


def test_categorical_mapping_rejects_uninventoried_target(
    tmp_path: Path,
) -> None:
    contract = load_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    raw["categorical_value_mappings"]["static"]["cluster_id"] = {
        "status": "approved_mapping",
        "normalization": "strip",
        "output_arrow_type": "large_string",
        "values": {"x": "x"},
    }
    invalid_path = tmp_path / "invalid_registry.yaml"
    invalid_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="not included in the categorical"):
        load_translation_registry(invalid_path, contract)


def test_registry_validation_cli_is_read_only() -> None:
    exit_code = main(
        [
            "validate-translation-policy",
            "--config",
            str(PROJECT_ROOT / "asic" / "config" / "datasets" / "mock.yaml"),
        ]
    )

    assert exit_code == 0


def test_reviewed_legacy_duplicate_targets_are_accounted_for() -> None:
    contract = load_input_contract(CONTRACT_PATH)
    registry = load_translation_registry(REGISTRY_PATH, contract)
    current_sources = {column.name for column in contract.dynamic.columns}
    reviewed_legacy_duplicate_targets = {
        "ck": ("CK", "Creatinkinase"),
        "ecmo_o2": ("ECMO_FiO2", "Gaszusammensetzung_(%O2)"),
        "il6": ("IL-6", "Interleukin_6"),
        "lymph_pct": ("Lymphocyten", "Lymphozyten_prozentual"),
        "ntprobnp": ("NT-proBNP", "NT-pro_BNP"),
        "sofa": ("Organversagen:_SOFA_Score_ohne_GCS", "SOFA"),
    }
    assert all(
        set(sources) <= current_sources
        for sources in reviewed_legacy_duplicate_targets.values()
    )
    approved_source_sets = {
        frozenset(rule.source_columns): rule.target_name
        for rule in registry.merge_rules
    }
    for target, sources in reviewed_legacy_duplicate_targets.items():
        if target == "sofa":
            assert frozenset(sources) not in approved_source_sets
        else:
            assert approved_source_sets[frozenset(sources)] == target
