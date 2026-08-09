from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.harmonization.schema_dictionary_freeze import (
    _schemas_payload,
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.unit_resolution import load_reviewed_unit_decisions
from asic_pipeline.unit_resolution import load_reviewed_medication_semantics
from asic_pipeline.unit_resolution.schema_amendment import (
    EXPECTED_BOUNDARY,
    UnitSchemaAmendmentConfig,
    build_amended_schema_and_dictionary,
    load_unit_schema_amendment_config,
    load_unit_schema_amendment_policy,
    run_unit_schema_amendment_review,
)
from asic_pipeline.inventory.hashing import sha256_file


def _inputs(project_root: Path):
    config = load_unit_schema_amendment_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_unit_schema_amendment_policy(config.policy_path)
    reviewed = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    medication = load_reviewed_medication_semantics(
        policy.reviewed_medication_semantics_path
    )
    freeze = load_schema_dictionary_freeze_policy(
        policy.baseline_freeze_policy_path
    )
    schemas = _schemas_payload(freeze)
    dictionary = []
    for table in ("static", "dynamic"):
        for field in schemas[table]:
            dictionary.append(
                {
                    "table": table,
                    **field,
                    "definition": f"Synthetic definition for {field['variable']}",
                    "definition_status": "reviewed",
                    "analysis_caveat": None,
                    "all_missing": False,
                    "source_hospitals": ["asic_UK00"],
                    "source_hospital_count": 1,
                    "source_strategy": "synthetic_test",
                    "categorical_contract_version": None,
                    "publication_ready": False,
                }
            )
    return policy, reviewed, medication, schemas, dictionary


def test_shipped_schema_amendment_policy_is_report_only(project_root: Path) -> None:
    policy, _, _, _, _ = _inputs(project_root)

    assert policy.baseline_contract_version == "0.1"
    assert policy.proposed_contract_version == "0.2"
    assert policy.audit_run_id == "20260807T083117Z"
    assert policy.expected["reviewed_unit_update_count"] == 72
    assert policy.expected["semantic_split_count"] == 9
    assert EXPECTED_BOUNDARY["read_clinical_rows"] is False
    assert EXPECTED_BOUNDARY["freeze_contract_0_2"] is False
    assert EXPECTED_BOUNDARY["write_reports_only"] is True
    assert EXPECTED_BOUNDARY["drop_source_values"] is False


def test_amendment_preserves_positions_and_appends_nine_split_fields(
    project_root: Path,
) -> None:
    _, reviewed, medication, baseline_schemas, baseline_dictionary = _inputs(project_root)

    schemas, dictionary = build_amended_schema_and_dictionary(
        baseline_schemas, baseline_dictionary, reviewed, medication
    )

    assert len(schemas["static"]) == 23
    assert len(schemas["dynamic"]) == 139
    assert len(dictionary) == 162
    assert schemas["dynamic"][:130] == [
        field
        if field["variable"] not in reviewed.base_candidate.variables
        else field
        | {
            "unit": reviewed.base_candidate.variables[field["variable"]]["unit"],
            "analysis_eligibility": "eligible",
        }
        for field in baseline_schemas["dynamic"]
    ]
    assert [field["ordered_position"] for field in schemas["dynamic"]] == list(
        range(1, 140)
    )
    assert [field["variable"] for field in schemas["dynamic"][130:]] == [
        row["target_variable"] for row in reviewed.semantic_splits
    ]


def test_amendment_resolves_72_units_and_retains_three_source_scales(
    project_root: Path,
) -> None:
    _, reviewed, medication, baseline_schemas, baseline_dictionary = _inputs(project_root)
    schemas, dictionary = build_amended_schema_and_dictionary(
        baseline_schemas, baseline_dictionary, reviewed, medication
    )
    by_variable = {row["variable"]: row for row in dictionary}

    assert not any(
        field["unit"] == "unresolved" for field in schemas["dynamic"]
    )
    assert sum(
        field["unit"] == "unresolved_source_scale"
        for field in schemas["dynamic"]
    ) == 3
    for variable, decision in reviewed.base_candidate.variables.items():
        assert by_variable[variable]["unit"] == decision["unit"]
        assert by_variable[variable]["analysis_eligibility"] == "eligible"
        assert (
            by_variable[variable]["unit_provenance"]
            == "team_derived_not_hospital_supplied"
        )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"]["unit"]
        == "mg_per_kg_per_h"
    )
    assert (
        by_variable["prednisolone_iv_bolus_source_uk02"][
            "analysis_eligibility_detail"
        ]
        == "ineligible_pending_definition"
    )
    assert all(row["publication_ready"] is False for row in dictionary)


def test_canonical_split_sources_document_hospital_specific_parallel_field(
    project_root: Path,
) -> None:
    _, reviewed, medication, baseline_schemas, baseline_dictionary = _inputs(project_root)
    _, dictionary = build_amended_schema_and_dictionary(
        baseline_schemas, baseline_dictionary, reviewed, medication
    )
    by_variable = {row["variable"]: row for row in dictionary}

    assert (
        "asic_UK08 uses parallel variable propofol_iv_cont_weight_normalized."
        in by_variable["propofol_iv_cont"]["analysis_caveat"]
    )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"]["source_hospitals"]
        == ["asic_UK08"]
    )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"]["source_strategy"]
        == "hospital_semantic_split_preserve_unchanged"
    )
    assert sum(
        row["medication_or_therapy_variable"] is True for row in dictionary
    ) == 34
    assert (
        by_variable["propofol_iv_cont"]["zero_semantics"]
        == "explicit_observed_numeric_zero_preserved"
    )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"]["null_semantics"]
        == "unavailable_or_unobserved_not_assumed_zero"
    )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"][
            "null_to_zero_imputation_allowed"
        ]
        is False
    )
    assert (
        by_variable["propofol_iv_cont_weight_normalized"][
            "zero_to_null_conversion_allowed"
        ]
        is False
    )
    assert all(
        row["valid_min_inclusive"] == 0.0
        and row["negative_value_cleaning_policy"]
        == "mask_negative_medication_or_therapy_value"
        and row["negative_values_preserved_in_harmonized"] is True
        for row in dictionary
        if row["medication_or_therapy_variable"] is True
    )


def test_complete_review_writes_reports_without_reading_clinical_rows(
    tmp_path: Path,
    project_root: Path,
) -> None:
    policy, reviewed, _, schemas, dictionary = _inputs(project_root)
    freeze_policy = load_schema_dictionary_freeze_policy(
        policy.baseline_freeze_policy_path
    )
    data_root = tmp_path / "data"
    reports_root = tmp_path / "reports"
    contract = data_root / freeze_policy.output_directory
    contract.mkdir(parents=True)
    static_path = contract / "static_schema.json"
    dynamic_path = contract / "dynamic_schema.json"
    dictionary_path = contract / "variable_dictionary.parquet"
    static_path.write_text(json.dumps(schemas["static"]) + "\n", encoding="utf-8")
    dynamic_path.write_text(json.dumps(schemas["dynamic"]) + "\n", encoding="utf-8")
    pq.write_table(pa.Table.from_pylist(dictionary), dictionary_path)
    manifest_path = contract / "freeze_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_frozen_harmonized_schema_dictionary",
                "contract_version": "0.1",
                "schema_frozen": True,
                "dictionary_frozen": True,
                "files": {
                    static_path.name: sha256_file(static_path),
                    dynamic_path.name: sha256_file(dynamic_path),
                    dictionary_path.name: sha256_file(dictionary_path),
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    audit_dir = (
        reports_root
        / "private/unit_decision_audit"
        / policy.audit_run_id
    )
    audit_dir.mkdir(parents=True)
    (audit_dir / "unit_decision_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_unit_decision_audit_private",
                "artifact_version": "0.2",
                "run_id": policy.audit_run_id,
                "cleaned_release_id": "20260806T114234Z",
                "metrics": {
                    "conversion_count": 9,
                    "mask_count": 9,
                    "target_variable_count": 16,
                },
                "lineage": {
                    "candidate_unit_decisions_sha256": (
                        reviewed.base_candidate_sha256
                    )
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    fake_freeze = SimpleNamespace(
        dataset_context="production",
        data_root=data_root,
        reports_root=reports_root,
    )
    config = UnitSchemaAmendmentConfig(
        freeze=fake_freeze,  # type: ignore[arg-type]
        policy_path=policy.source_path,
    )

    result = run_unit_schema_amendment_review(config, "synthetic-review")

    assert result.overall_status == "pending_human_review"
    assert len(result.blocking_findings) == 1
    assert result.technical_blocking_findings == ()
    assert result.review_markdown_path.is_file()
    payload = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["clinical_rows_read"] == 0
    assert payload["metrics"]["dynamic_variable_count"] == 139
    assert payload["metrics"]["unresolved_source_scale_variable_count"] == 3
    assert payload["clinical_data_written"] is False
    assert payload["schema_frozen"] is False
