from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pyarrow as pa
import pytest
import yaml

from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization import (
    load_fully_reviewed_registry,
    load_registry_contract_policy,
    plan_harmonization_table,
    validate_registry_against_review_occurrences,
)


def _policy(project_root: Path):
    return load_registry_contract_policy(
        project_root / "asic/config/harmonization/registry_contract_policy.yaml"
    )


def _rules() -> list[dict[str, object]]:
    return [
        {
            "rule_id": "HARM-DYNAMIC-TEMP-A-001",
            "review_item_id": "R0001",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "temperature_a",
            "occurrence": 1,
            "action": "retain",
            "review_status": "human_approved_raw_v3",
            "evidence_raw_nonempty_count": 3,
            "target": "core_temp",
            "kind": "numeric",
            "value_type": "float64",
            "unit": "deg_C",
            "parser_rule_ids": ["PARSE-DIRECT-NUMERIC-001"],
            "alias_group_id": "ALIAS-CORE-TEMP-001",
            "merge_policy": "fail_if_both_nonmissing_and_unequal",
        },
        {
            "rule_id": "HARM-DYNAMIC-TEMP-B-001",
            "review_item_id": "R0002",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "temperature_b",
            "occurrence": 1,
            "action": "retain",
            "review_status": "human_approved_raw_v3",
            "evidence_raw_nonempty_count": 0,
            "target": "core_temp",
            "kind": "numeric",
            "value_type": "float64",
            "unit": "deg_C",
            "parser_rule_ids": ["PARSE-DIRECT-NUMERIC-001"],
            "alias_group_id": "ALIAS-CORE-TEMP-001",
            "merge_policy": "fail_if_both_nonmissing_and_unequal",
        },
        {
            "rule_id": "HARM-DYNAMIC-EMPTY-DROP-001",
            "review_item_id": "R0003",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "empty_duplicate",
            "occurrence": 2,
            "action": "drop_after_all_missing",
            "review_status": "human_approved_raw_v3",
            "evidence_raw_nonempty_count": 0,
            "target": None,
            "kind": None,
            "value_type": None,
            "unit": None,
        },
    ]


def _write_registry(
    path: Path,
    rules: list[dict[str, object]] | None = None,
) -> None:
    selected = rules or _rules()
    value = {
        "registry_version": "0.1",
        "status": "human_approved_complete_raw_v3_registry",
        "review": {
            "approved_by_role": "project_data_owner",
            "approved_at": "2026-08-05",
            "schema_token_run_id": "20260805T090942Z",
            "registry_review_run_id": "registry_review_input",
            "expected_occurrence_count": len(selected),
        },
        "rules": selected,
    }
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


def _occurrences() -> list[dict[str, object]]:
    return [
        {
            "review_item_id": "R0001",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "temperature_a",
            "raw_occurrence": 1,
            "raw_nonempty_count": 3,
            "all_missing_or_empty": False,
        },
        {
            "review_item_id": "R0002",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "temperature_b",
            "raw_occurrence": 1,
            "raw_nonempty_count": 0,
            "all_missing_or_empty": True,
        },
        {
            "review_item_id": "R0003",
            "hospital": "asic_UK00",
            "table": "dynamic",
            "raw_name": "empty_duplicate",
            "raw_occurrence": 2,
            "raw_nonempty_count": 0,
            "all_missing_or_empty": True,
        },
    ]


def _raw_field(physical: str, raw_name: str, occurrence: int) -> pa.Field:
    return pa.field(
        physical,
        pa.large_string(),
        metadata={
            b"asic_v3_role": b"raw_clinical_string",
            b"raw_name": raw_name.encode("utf-8"),
            b"raw_occurrence": str(occurrence).encode("ascii"),
        },
    )


def test_complete_registry_contract_plans_schema_but_cannot_execute(
    tmp_path: Path,
    project_root: Path,
) -> None:
    policy = _policy(project_root)
    assert policy.required_registry_review_artifact_version == "0.3"
    registry_path = tmp_path / "reviewed_registry.yaml"
    _write_registry(registry_path)
    registry = load_fully_reviewed_registry(registry_path, policy)

    evidence = validate_registry_against_review_occurrences(
        registry, _occurrences()
    )
    assert evidence.expected_occurrence_count == 3
    assert evidence.retained_occurrence_count == 2
    assert evidence.reviewed_drop_occurrence_count == 1

    schema = pa.schema(
        [
            pa.field("__v3_source_filename", pa.large_string()),
            _raw_field("temperature_a", "temperature_a", 1),
            _raw_field("temperature_b", "temperature_b", 1),
            _raw_field("empty_duplicate_col2", "empty_duplicate", 2),
        ]
    )
    plan = plan_harmonization_table(
        policy,
        registry,
        "asic_UK00",
        "dynamic",
        schema,
        {"__v3_source_filename"},
    )

    assert len(plan.source_fields) == 3
    assert plan.candidate_output_schema.names == ["core_temp"]
    assert plan.candidate_output_schema.field("core_temp").type == pa.float64()
    assert plan.dropped_source_count == 1
    assert plan.alias_group_count == 1
    assert plan.production_execution_allowed is False
    with pytest.raises(HarmonizationError, match="pending production"):
        plan.require_execution_ready()
    with pytest.raises(HarmonizationError, match="Production harmonization remains blocked"):
        policy.require_production_ready()


def test_registry_rejects_same_target_collision_without_alias_policy(
    tmp_path: Path,
    project_root: Path,
) -> None:
    rules = deepcopy(_rules())
    rules[0].pop("alias_group_id")
    rules[0].pop("merge_policy")
    path = tmp_path / "missing_alias.yaml"
    _write_registry(path, rules)

    with pytest.raises(ConfigurationError, match="explicit alias"):
        load_fully_reviewed_registry(path, _policy(project_root))


def test_registry_and_planner_support_reviewed_fixed_position_boolean_list(
    tmp_path: Path,
    project_root: Path,
) -> None:
    rules = _rules()
    for position, rule in enumerate(rules[:2]):
        rule["target"] = "therapy_read_confirmation_utc"
        rule["kind"] = "categorical"
        rule["value_type"] = "fixed_size_list_bool_2"
        rule["unit"] = "not_applicable"
        rule["alias_group_id"] = "COMPOSITE-THERAPY-CONFIRM-001"
        rule["merge_policy"] = "assemble_fixed_position_list"
        rule["source_position"] = position
    registry_path = tmp_path / "fixed_list_registry.yaml"
    _write_registry(registry_path, rules)
    policy = _policy(project_root)

    registry = load_fully_reviewed_registry(registry_path, policy)
    schema = pa.schema(
        [
            _raw_field(
                f"source_{index}",
                str(rule["raw_name"]),
                int(rule["occurrence"]),
            )
            for index, rule in enumerate(rules)
        ]
    )
    plan = plan_harmonization_table(
        policy,
        registry,
        "asic_UK00",
        "dynamic",
        schema,
        provenance_columns=(),
    )

    field = plan.candidate_output_schema.field("therapy_read_confirmation_utc")
    assert field.type == pa.list_(pa.bool_(), 2)
    assert [rule.source_position for rule in registry.rules[:2]] == [0, 1]


def test_registry_rejects_invalid_fixed_source_positions(
    tmp_path: Path,
    project_root: Path,
) -> None:
    rules = _rules()
    for rule in rules[:2]:
        rule["target"] = "therapy_read_confirmation_utc"
        rule["kind"] = "categorical"
        rule["value_type"] = "fixed_size_list_bool_2"
        rule["unit"] = "not_applicable"
        rule["alias_group_id"] = "COMPOSITE-THERAPY-CONFIRM-001"
        rule["merge_policy"] = "assemble_fixed_position_list"
        rule["source_position"] = 0
    registry_path = tmp_path / "bad_fixed_list_registry.yaml"
    _write_registry(registry_path, rules)

    with pytest.raises(ConfigurationError, match="contiguous positions"):
        load_fully_reviewed_registry(registry_path, _policy(project_root))


def test_registry_rejects_type_unit_conflicts_and_nonempty_drop(
    tmp_path: Path,
    project_root: Path,
) -> None:
    rules = deepcopy(_rules())
    rules[1]["unit"] = "deg_F"
    conflict_path = tmp_path / "unit_conflict.yaml"
    _write_registry(conflict_path, rules)
    with pytest.raises(ConfigurationError, match="inconsistent kind/type/unit"):
        load_fully_reviewed_registry(conflict_path, _policy(project_root))

    rules = deepcopy(_rules())
    rules[2]["evidence_raw_nonempty_count"] = 1
    drop_path = tmp_path / "bad_drop.yaml"
    _write_registry(drop_path, rules)
    with pytest.raises(ConfigurationError, match="requires zero non-empty"):
        load_fully_reviewed_registry(drop_path, _policy(project_root))


def test_registry_and_source_schema_require_exact_occurrence_coverage(
    tmp_path: Path,
    project_root: Path,
) -> None:
    policy = _policy(project_root)
    path = tmp_path / "registry.yaml"
    _write_registry(path)
    registry = load_fully_reviewed_registry(path, policy)

    with pytest.raises(HarmonizationError, match="different row counts"):
        validate_registry_against_review_occurrences(registry, _occurrences()[:-1])

    incomplete_schema = pa.schema(
        [
            _raw_field("temperature_a", "temperature_a", 1),
            _raw_field("temperature_b", "temperature_b", 1),
        ]
    )
    with pytest.raises(HarmonizationError, match="exact coverage"):
        plan_harmonization_table(
            policy,
            registry,
            "asic_UK00",
            "dynamic",
            incomplete_schema,
            (),
        )

    unknown_schema = pa.schema(
        [
            _raw_field("temperature_a", "temperature_a", 1),
            _raw_field("temperature_b", "temperature_b", 1),
            _raw_field("empty_duplicate_col2", "empty_duplicate", 2),
            _raw_field("unknown", "unknown", 1),
        ]
    )
    with pytest.raises(HarmonizationError, match="unreviewed raw field"):
        plan_harmonization_table(
            policy,
            registry,
            "asic_UK00",
            "dynamic",
            unknown_schema,
            (),
        )
