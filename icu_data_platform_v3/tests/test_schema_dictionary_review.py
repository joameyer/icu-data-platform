from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.harmonization import (
    SchemaDictionaryReviewConfig,
    load_reviewed_categorical_decisions,
    load_schema_dictionary_review_policy,
    run_schema_dictionary_review,
)
from asic_pipeline.harmonization.schema_dictionary_review import (
    resolve_reviewed_categorical_token,
)
from asic_pipeline.inventory.hashing import sha256_file


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_reviewed_categorical_special_rules_are_fail_closed(
    project_root: Path,
) -> None:
    decisions = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )
    ards = decisions.rule_for("dynamic", "ards_diagnosis_app")
    ecmo = decisions.rule_for("dynamic", "ecmo")
    position = decisions.rule_for("dynamic", "position_therapy")
    height = decisions.rule_for("static", "height_group")
    assert ards is not None and ecmo is not None and position is not None
    assert height is not None

    assert resolve_reviewed_categorical_token(ards, "asic_UK04", "3.000") == (
        True,
        3,
    )
    assert resolve_reviewed_categorical_token(ards, "asic_UK00", "3000.0") == (
        True,
        3000,
    )
    assert resolve_reviewed_categorical_token(ards, "asic_UK00", "30") == (
        False,
        30,
    )
    assert resolve_reviewed_categorical_token(ards, "asic_UK01", "10.0") == (
        False,
        10,
    )
    assert resolve_reviewed_categorical_token(ecmo, "asic_UK08", "0.00") == (
        True,
        False,
    )
    assert resolve_reviewed_categorical_token(ecmo, "asic_UK03", "nan") == (
        True,
        None,
    )
    assert resolve_reviewed_categorical_token(ecmo, "asic_UK00", "nan") == (
        False,
        None,
    )
    assert resolve_reviewed_categorical_token(
        position, "asic_UK00", "0.3333333333333333"
    ) == (True, 0.3333333333333333)
    assert resolve_reviewed_categorical_token(position, "asic_UK00", "0.7") == (
        False,
        None,
    )
    assert resolve_reviewed_categorical_token(height, "asic_UK01", "-1") == (
        True,
        None,
    )
    assert resolve_reviewed_categorical_token(height, "asic_UK02", "-1") == (
        False,
        "-1",
    )


def _representative_token(rule: object) -> tuple[str, str]:
    parser = rule.parser
    if parser == "integral_numeric_then_hospital_code_allowlist":
        return "asic_UK00", "0.0"
    if parser == "integral_binary_numeric":
        return "asic_UK00", "0.00"
    if parser == "direct_numeric_exact_reviewed_domain":
        return "asic_UK00", "0.0"
    if parser == "exact_hospital_scoped_domain":
        hospital, values = rule.allowed_values_by_hospital[0]
        return hospital, str(values[0])
    if parser == "previously_reviewed_nullable_boolean":
        return "asic_UK00", "False"
    return "asic_UK00", str(rule.allowed_values[0])


def test_schema_dictionary_review_reuses_reports_without_candidate_scan(
    tmp_path: Path,
    project_root: Path,
) -> None:
    reports = tmp_path / "reports"
    consolidated_dir = (
        reports
        / "private/consolidated_harmonization_audit/20260806T092818Z"
    )
    categorical_dir = (
        reports / "private/categorical_contract_review/20260806T100728Z"
    )
    decisions = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )

    domain_rows: list[dict[str, object]] = []
    scalar_rules = [
        rule
        for rule in decisions.rules
        if rule.parser != "previously_reviewed_fixed_position_composite"
    ]
    for rule in scalar_rules:
        non_null = (
            rule.expected_non_null_count
            if rule.expected_non_null_count is not None
            else 1
        )
        null = rule.expected_null_count if rule.expected_null_count is not None else 1
        if non_null:
            hospital, token = _representative_token(rule)
            domain_rows.append(
                {
                    "table": rule.table,
                    "hospital": hospital,
                    "variable": rule.variable,
                    "value_repr": token,
                    "value_is_null": False,
                    "count": non_null,
                    "domain_truncated": False,
                    "review_status": "approved_in_contract",
                }
            )
        if null:
            domain_rows.append(
                {
                    "table": rule.table,
                    "hospital": "asic_UK00",
                    "variable": rule.variable,
                    "value_repr": None,
                    "value_is_null": True,
                    "count": null,
                    "domain_truncated": False,
                    "review_status": "approved_in_contract",
                }
            )

    dictionary_rows: list[dict[str, object]] = []
    for table in ("static", "dynamic"):
        table_rules = [rule for rule in decisions.rules if rule.table == table]
        for position, rule in enumerate(table_rules, start=1):
            dictionary_rows.append(
                {
                    "table": table,
                    "ordered_position": position,
                    "variable": rule.variable,
                    "candidate_arrow_type": "large_string",
                    "candidate_value_type": "large_string",
                    "candidate_unit": "not_applicable",
                    "definition": f"Candidate definition for {rule.variable}",
                    "definition_status": "pending_consolidated_review",
                    "source_hospitals": ["asic_UK00"],
                    "source_hospital_count": 1,
                    "source_strategy": "synthetic_test",
                }
            )
    dictionary_rows.append(
        {
            "table": "dynamic",
            "ordered_position": 1
            + max(
                int(row["ordered_position"])
                for row in dictionary_rows
                if row["table"] == "dynamic"
            ),
            "variable": "minutes_since_icu_admission",
            "candidate_arrow_type": "double",
            "candidate_value_type": "float64",
            "candidate_unit": "min",
            "definition": "Elapsed minutes since ICU admission.",
            "definition_status": "pending_consolidated_review",
            "source_hospitals": ["asic_UK00"],
            "source_hospital_count": 1,
            "source_strategy": "synthetic_test",
        }
    )
    dictionary_path = consolidated_dir / "candidate_variable_dictionary.parquet"
    list_path = consolidated_dir / "list_profiles.parquet"
    domain_path = categorical_dir / "categorical_domain_review.parquet"
    _write_rows(dictionary_path, dictionary_rows)
    _write_rows(
        list_path,
        [
            {
                "table": "dynamic",
                "hospital": "asic_UK00",
                "variable": "therapy_read_confirmation_utc",
                "arrow_type": "fixed_size_list<element: bool>[2]",
                "minimum_list_length": 2,
                "maximum_list_length": 2,
            }
        ],
    )
    _write_rows(domain_path, domain_rows)
    consolidated_manifest_path = consolidated_dir / "consolidated_audit_manifest.json"
    consolidated_manifest_path.write_text(
        json.dumps(
            {
                "artifact_version": "0.1",
                "run_id": "20260806T092818Z",
                "candidate_run_id": "20260806T074852Z",
                "table_row_counts": {
                    "candidate_variable_dictionary.parquet": len(dictionary_rows)
                },
                "table_sha256": {
                    dictionary_path.name: sha256_file(dictionary_path),
                    list_path.name: sha256_file(list_path),
                },
            }
        ),
        encoding="utf-8",
    )
    categorical_manifest_path = categorical_dir / "categorical_review_manifest.json"
    categorical_manifest_path.write_text(
        json.dumps(
            {
                "artifact_version": "0.1",
                "run_id": "20260806T100728Z",
                "consolidated_audit_run_id": "20260806T092818Z",
                "candidate_run_id": "20260806T074852Z",
                "categorical_domain_review_sha256": sha256_file(domain_path),
            }
        ),
        encoding="utf-8",
    )
    config = SchemaDictionaryReviewConfig(
        consolidated=SimpleNamespace(
            dataset_context="production",
            reports_root=reports,
        ),
        policy_path=(
            project_root
            / "asic/config/harmonization/schema_dictionary_review.yaml"
        ),
    )

    result = run_schema_dictionary_review(config, "schema001")

    assert result.overall_status == "fail"
    assert result.has_technical_failure is False
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    assert review["metrics"]["candidate_rows_rescanned"] == 0
    assert review["metrics"]["categorical_evidence_failure_count"] == 0
    assert len(review["blocking_findings"]) == 2
    assert {
        row["variable"]: row["physical_type"]
        for row in review["schemas"]["dynamic"]
    }["position_therapy"] == "double"
    dynamic_schema = {
        row["variable"]: row for row in review["schemas"]["dynamic"]
    }
    assert dynamic_schema["anchored_time_since_icu_admission"] == {
        "ordered_position": dynamic_schema["minutes_since_icu_admission"][
            "ordered_position"
        ]
        + 1,
        "variable": "anchored_time_since_icu_admission",
        "physical_type": "timestamp[ns]",
        "unit": "artificial_anchored_timestamp",
        "analysis_eligibility": "eligible",
    }
    assert (
        pq.read_table(
            result.private_report_directory / "proposed_variable_dictionary.parquet"
        ).num_rows
        == len(dictionary_rows) + 1
    )


def test_hospital_conversion_output_units_are_dictionary_units(
    project_root: Path,
) -> None:
    from asic_pipeline.harmonization.schema_dictionary_review import (
        _dictionary_overlay,
    )
    from asic_pipeline.harmonization import load_reviewed_consolidated_decisions

    consolidated = load_reviewed_consolidated_decisions(
        project_root
        / "asic/config/harmonization/reviewed_consolidated_decisions_0_1.yaml"
    )
    categorical = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )
    rows = [
        {
            "table": "dynamic",
            "ordered_position": position,
            "variable": variable,
            "candidate_arrow_type": "double",
            "candidate_unit": "unresolved",
            "definition": variable,
            "definition_status": "pending_consolidated_review",
            "source_hospitals": [hospital],
            "source_hospital_count": 1,
            "source_strategy": "synthetic_test",
        }
        for position, (variable, hospital) in enumerate(
            (("hematocrit", "asic_UK03"), ("lymph_pct", "asic_UK00")),
            start=1,
        )
    ]

    overlaid = _dictionary_overlay(rows, consolidated, categorical)

    assert {row["variable"]: row["unit"] for row in overlaid} == {
        "hematocrit": "percent",
        "lymph_pct": "percent",
    }
    assert all(row["analysis_eligibility"] == "eligible" for row in overlaid)
