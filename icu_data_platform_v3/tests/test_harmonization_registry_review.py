from __future__ import annotations

import json
from pathlib import Path

import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    build_harmonization_registry_review,
    load_harmonization_review_config,
    load_harmonization_review_policy,
    write_harmonization_registry_review_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


def _column(
    *,
    hospital: str,
    table: str,
    physical_name: str,
    raw_name: str,
    target: str | None,
    kind: str,
    status: str = "prior_reference_candidate_requires_raw_revalidation",
    review_rule_id: str | None = None,
    all_missing: bool = False,
    allowed_tokens: list[str] | None = None,
    approved_parser: str | None = None,
) -> dict[str, object]:
    nonempty = 0 if all_missing else 3
    if kind == "categorical" and allowed_tokens:
        classes = {"approved_categorical_token": nonempty}
    elif kind == "categorical":
        classes = {"categorical_token": nonempty}
    elif kind == "numeric":
        classes = {"direct_numeric": nonempty}
    else:
        classes = {"identifier_token_redacted": nonempty}
    return {
        "hospital": hospital,
        "table": table,
        "physical_name": physical_name,
        "raw_name": raw_name,
        "raw_occurrence": 1,
        "candidate_target": target,
        "candidate_kind": kind,
        "candidate_status": status,
        "review_rule_id": review_rule_id,
        "approved_parser": approved_parser,
        "approved_allowed_tokens": allowed_tokens or [],
        "approved_missing_sentinel_tokens": [],
        "review_evidence_run_id": "input_evidence",
        "source_file_count_available": 1,
        "source_schema_variant_count_available": 1,
        "source_schema_present_row_count": 3,
        "expected_row_count": 3,
        "literal_empty_count": 0 if not all_missing else 3,
        "raw_nonempty_count": nonempty,
        "classification_counts": classes,
        "all_missing_or_empty": all_missing,
    }


def _config(tmp_path: Path, project_root: Path) -> HarmonizationReviewConfig:
    reports = tmp_path / "reports" / "demo"
    inventory = InventoryConfig(
        dataset_context="demo",
        policy_path=project_root / "asic/config/inventory/policy.yaml",
        comparison_reference_path=None,
        paths=InventoryPaths(
            raw_root=tmp_path / "raw",
            reports=reports,
            runs=tmp_path / "runs" / "demo",
        ),
        source_path=project_root / "asic/config/datasets/demo.yaml",
    )
    ingestion = IngestionConfig(
        inventory=inventory,
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=tmp_path / "data" / "demo",
        rows_per_batch=10,
    )
    schema_tokens = SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )
    return HarmonizationReviewConfig(
        schema_tokens=schema_tokens,
        policy_path=project_root / "asic/config/harmonization/review_policy.yaml",
    )


def _write_input_bundle(
    config: HarmonizationReviewConfig,
    run_id: str,
    *,
    unexpected_blocker: bool = False,
    same_hospital_alias: bool = False,
    approved_static_decisions: bool = False,
    carried_archive_blocker: bool = False,
) -> tuple[dict[str, object], ...]:
    rows = [
        _column(
            hospital="asic_UK00",
            table="static",
            physical_name="Cluster-ID",
            raw_name="Cluster-ID",
            target="cluster_id",
            kind="categorical",
            review_rule_id="HARM-STATIC-CLUSTER-UK00-001",
            allowed_tokens=["C1", "C2"],
        ),
        _column(
            hospital="asic_UK00",
            table="dynamic",
            physical_name="ARDS_Diagnose_App_col2",
            raw_name="ARDS_Diagnose_App",
            target=None,
            kind="categorical",
            status="reviewed_all_empty_drop_candidate_requires_inventory_revalidation",
            all_missing=True,
        ),
        _column(
            hospital="asic_UK00",
            table="dynamic",
            physical_name="Koerperkerntemperatur",
            raw_name="Koerperkerntemperatur",
            target="core_temp",
            kind="numeric",
        ),
        _column(
            hospital="asic_UK02",
            table="dynamic",
            physical_name="Körpertemperatur",
            raw_name="Körpertemperatur",
            target="core_temp",
            kind="numeric",
        ),
        _column(
            hospital="asic_UK00",
            table="static",
            physical_name="heightcm",
            raw_name="heightcm",
            target="height_measurements_cm",
            kind="numeric",
            review_rule_id="HARM-STATIC-HEIGHT-UK00-002",
            approved_parser="numeric_list",
        ),
        _column(
            hospital="asic_UK02",
            table="static",
            physical_name="heightcm",
            raw_name="heightcm",
            target="height_cm",
            kind="numeric",
        ),
        _column(
            hospital="asic_UK00",
            table="dynamic",
            physical_name="vt_pbw_source",
            raw_name="vt_pbw_source",
            target="vt_per_kg_ideal_body_weight",
            kind="numeric",
        ),
    ]
    if same_hospital_alias:
        rows.append(
            _column(
                hospital="asic_UK00",
                table="dynamic",
                physical_name="Koerperkerntemperatur_alternative",
                raw_name="Koerperkerntemperatur_alternative",
                target="core_temp",
                kind="numeric",
            )
        )
    if approved_static_decisions:
        rows.extend(
            [
                _column(
                    hospital="asic_UK02",
                    table="static",
                    physical_name="Wiederaufnahme_ICU",
                    raw_name="Wiederaufnahme_ICU",
                    target="icu_readmit",
                    kind="categorical",
                ),
                _column(
                    hospital="asic_UK02",
                    table="static",
                    physical_name="Zeit_seit_Studienbeginn",
                    raw_name="Zeit_seit_Studienbeginn",
                    target="time_since_study_start",
                    kind="numeric",
                ),
                _column(
                    hospital="asic_UK02",
                    table="static",
                    physical_name="Phase",
                    raw_name="Phase",
                    target="study_implementation_phase",
                    kind="categorical",
                ),
            ]
        )
    row_tuple = tuple(rows)
    private = (
        config.reports_root / "private" / "schema_token_inventory" / run_id
    )
    review_dir = config.reports_root / "review" / "schema_token_inventory"
    private.mkdir(parents=True)
    review_dir.mkdir(parents=True)
    generated = "2026-08-05T09:09:42+00:00"
    blockers = [
        {
            "check": "candidate_harmonization_registry_approved",
            "details": "review required",
        }
    ]
    if unexpected_blocker:
        blockers.append({"check": "numeric_candidate_unresolved_tokens", "details": "bad"})
    if carried_archive_blocker:
        blockers.append(
            {
                "check": "provisional_archive_owner_confirmation",
                "details": "historical input blocker",
            }
        )
    manifest = {
        "artifact": "asic_v3_schema_token_inventory_private",
        "artifact_version": "0.3",
        "dataset_context": "demo",
        "generated_at_utc": generated,
        "inputs": {
            "inventory_run_id": "inventory_input",
            "ingestion_audit_run_id": "audit_input",
        },
        "policies": {"schema_token_policy_version": "0.3"},
        "blocking_findings": blockers,
        "column_evidence_row_count": len(row_tuple),
        "token_evidence_row_count": 0,
    }
    review = {
        "artifact": "asic_v3_schema_token_inventory_review",
        "artifact_version": "0.3",
        "dataset_context": "demo",
        "generated_at_utc": generated,
        "inputs": {
            "inventory_run_id": "inventory_input",
            "ingestion_audit_run_id": "audit_input",
        },
        "blocking_findings": blockers,
        "metrics": {
            "raw_column_occurrence_count": len(row_tuple),
            "unmapped_raw_column_occurrence_count": 0,
            "numeric_unresolved_nonempty_token_count": 0,
            "columns_with_truncated_review_examples": 0,
            "reviewed_drop_candidate_nonempty_count": 0,
            "reviewed_rule_token_domain_violation_count": 0,
            "numeric_list_unresolved_element_count": 0,
        },
    }
    (private / "schema_token_inventory_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (review_dir / f"{run_id}.json").write_text(json.dumps(review), encoding="utf-8")
    _write_private_parquet(private / "columns.parquet", row_tuple)
    _write_private_parquet(private / "tokens.parquet", ())
    return row_tuple


def test_review_policy_and_shipped_configs_remain_fail_closed(
    project_root: Path,
) -> None:
    config = load_harmonization_review_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_harmonization_review_policy(config.policy_path)

    assert policy.version == "0.3"
    assert policy.private_artifact_version == "0.3"
    assert policy.review_artifact_version == "0.3"
    assert policy.resolved_schema_token_blockers == (
        "provisional_archive_owner_confirmation",
    )
    assert policy.allow_private_schema_token_evidence_reads is True
    assert policy.candidate_contract.status.endswith("proposals_only")
    height = policy.candidate_contract.proposal_for(
        "static", "height_measurements_cm", "numeric"
    )
    assert height.value_type == "list_float64"
    assert height.unit == "cm"
    assert height.unit_status == "human_approved_raw_v3"
    sex = policy.candidate_contract.categorical_for("static", "sex")
    assert sex is not None
    assert sex.values == {"M": "male", "W": "female"}
    split = policy.candidate_contract.semantic_split_for(
        "asic_UK00", "dynamic", "vt_per_kg_ideal_body_weight"
    )
    assert split is not None
    assert split.proposed_target == "vt_per_ideal_bw_total"
    assert split.proposed_unit == "mL"
    readmit = policy.candidate_contract.proposal_for(
        "static", "icu_readmit", "categorical"
    )
    assert readmit.value_type == "bool"
    assert readmit.value_type_status == "human_approved_raw_v3"
    assert readmit.unit == "not_applicable"
    readmit_values = policy.candidate_contract.categorical_for(
        "static", "icu_readmit"
    )
    assert readmit_values is not None
    assert readmit_values.review_status == (
        "human_approved_raw_v3_cross_hospital_domain"
    )
    assert readmit_values.values == {
        "0": False,
        "0.0": False,
        "1": True,
        "1.0": True,
    }
    study_time = policy.candidate_contract.proposal_for(
        "static", "time_since_study_start", "numeric"
    )
    assert study_time.value_type == "float64"
    assert study_time.value_type_status == "human_approved_raw_v3"
    assert study_time.unit == "day"
    assert study_time.unit_status == "human_approved_raw_v3"
    study_phase = policy.candidate_contract.proposal_for(
        "static", "study_implementation_phase", "categorical"
    )
    assert study_phase.value_type == "large_string"
    assert study_phase.value_type_status == "human_approved_raw_v3"
    assert study_phase.unit == "not_applicable"
    phase_values = policy.candidate_contract.categorical_for(
        "static", "study_implementation_phase"
    )
    assert phase_values is not None
    assert phase_values.review_status == (
        "human_approved_raw_v3_cross_hospital_domain"
    )
    assert phase_values.values == {
        "0": "calibration",
        "0.0": "calibration",
        "1": "roll_in",
        "1.0": "roll_in",
        "2": "app_implementation",
        "2.0": "app_implementation",
    }


def test_registry_review_accounts_for_every_occurrence_and_stays_private(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    input_rows = _write_input_bundle(config, "schema_token_input")

    result = build_harmonization_registry_review(config, "schema_token_input")

    assert result.overall_status == "fail"
    assert result.review_payload["artifact_version"] == "0.3"
    assert result.private_manifest["artifact_version"] == "0.3"
    assert result.technical_blocking_findings == ()
    assert len(result.private_occurrences) == len(input_rows)
    assert [row["review_item_id"] for row in result.private_occurrences] == [
        "R0001",
        "R0002",
        "R0003",
        "R0004",
        "R0005",
        "R0006",
        "R0007",
    ]
    drop = result.private_occurrences[1]
    assert drop["proposed_action"] == "drop_after_all_missing_precondition"
    assert drop["mapping_review_status"].startswith("human_approved")
    cluster = result.private_occurrences[0]
    assert cluster["approved_allowed_tokens"] == ["C1", "C2"]
    assert cluster["categorical_review_status"] == (
        "human_approved_raw_v3_hospital_domain"
    )
    height = result.private_occurrences[4]
    assert height["candidate_value_type"] == "list_float64"
    assert height["candidate_unit"] == "cm"
    core = [
        row for row in result.private_variables if row["candidate_target"] == "core_temp"
    ][0]
    assert core["cross_hospital_name_variant"] is True
    assert core["coexisting_alias_group"] is False
    assert core["source_raw_name_count"] == 2
    core_occurrences = [
        row
        for row in result.private_occurrences
        if row["candidate_target"] == "core_temp"
    ]
    assert all(
        row["same_hospital_target_occurrence_count"] == 1
        for row in core_occurrences
    )
    assert all(
        row["coexisting_alias_review_status"] == "not_applicable"
        for row in core_occurrences
    )
    metrics = result.review_payload["metrics"]
    assert metrics["raw_column_occurrence_count"] == 7
    assert metrics["accounted_occurrence_count"] == 7
    assert metrics["explicit_drop_after_precondition_count"] == 1
    assert metrics["cross_hospital_name_variant_group_count"] == 1
    assert metrics["coexisting_alias_group_count"] == 0
    assert metrics["candidate_semantic_split_group_count"] == 2
    assert metrics["observed_multi_target_source_group_count"] == 1
    assert metrics["known_semantic_split_candidate_count"] == 1
    vt = result.private_occurrences[6]
    assert vt["semantic_split_proposed_target"] == "vt_per_ideal_bw_total"
    assert vt["semantic_split_proposed_unit"] == "mL"
    review_text = json.dumps(result.review_payload, ensure_ascii=False)
    assert "Koerperkerntemperatur" not in review_text
    assert "Körpertemperatur" not in review_text
    assert "ARDS_Diagnose_App" not in review_text

    written = write_harmonization_registry_review_bundle(
        result, config.reports_root, "registry_review_output"
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    assert written.review_markdown_path is not None
    assert written.review_markdown_path.is_file()
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_harmonization_registry_review_bundle(
            result, config.reports_root, "registry_review_output"
        )


def test_registry_review_refuses_unresolved_schema_token_prerequisite(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_bundle(config, "bad_schema_token_input", unexpected_blocker=True)

    with pytest.raises(HarmonizationError, match="unresolved prerequisite"):
        build_harmonization_registry_review(config, "bad_schema_token_input")


def test_registry_review_requires_alias_only_for_same_hospital_target_collision(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_bundle(
        config,
        "same_hospital_alias_input",
        same_hospital_alias=True,
    )

    result = build_harmonization_registry_review(
        config, "same_hospital_alias_input"
    )

    core = next(
        row
        for row in result.private_variables
        if row["candidate_target"] == "core_temp"
    )
    assert core["cross_hospital_name_variant"] is True
    assert core["coexisting_alias_group"] is True
    uk00_core = [
        row
        for row in result.private_occurrences
        if row["hospital"] == "asic_UK00"
        and row["candidate_target"] == "core_temp"
    ]
    assert len(uk00_core) == 2
    assert all(
        row["same_hospital_target_occurrence_count"] == 2
        for row in uk00_core
    )
    assert all(
        row["coexisting_alias_review_status"] == "requires_human_review"
        for row in uk00_core
    )
    metrics = result.review_payload["metrics"]
    assert metrics["cross_hospital_name_variant_group_count"] == 1
    assert metrics["coexisting_alias_group_count"] == 1


def test_registry_review_surfaces_approved_static_type_and_boolean_domain(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_bundle(
        config,
        "approved_static_decisions_input",
        approved_static_decisions=True,
    )

    result = build_harmonization_registry_review(
        config, "approved_static_decisions_input"
    )

    readmit = next(
        row
        for row in result.private_occurrences
        if row["candidate_target"] == "icu_readmit"
    )
    assert readmit["candidate_value_type"] == "bool"
    assert readmit["value_type_review_status"] == "human_approved_raw_v3"
    assert readmit["candidate_unit"] == "not_applicable"
    assert readmit["categorical_review_status"] == (
        "human_approved_raw_v3_cross_hospital_domain"
    )
    study_time = next(
        row
        for row in result.private_occurrences
        if row["candidate_target"] == "time_since_study_start"
    )
    assert study_time["candidate_value_type"] == "float64"
    assert study_time["value_type_review_status"] == "human_approved_raw_v3"
    assert study_time["candidate_unit"] == "day"
    assert study_time["unit_review_status"] == "human_approved_raw_v3"
    study_phase = next(
        row
        for row in result.private_occurrences
        if row["candidate_target"] == "study_implementation_phase"
    )
    assert study_phase["candidate_value_type"] == "large_string"
    assert study_phase["value_type_review_status"] == "human_approved_raw_v3"
    assert study_phase["candidate_unit"] == "not_applicable"
    assert study_phase["categorical_review_status"] == (
        "human_approved_raw_v3_cross_hospital_domain"
    )
    assert result.review_payload["metrics"][
        "pending_categorical_variable_count"
    ] == 0


def test_registry_review_resolves_historical_archive_blocker_from_owner_decision(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_bundle(
        config,
        "archive_resolution_input",
        carried_archive_blocker=True,
    )

    result = build_harmonization_registry_review(
        config, "archive_resolution_input"
    )

    assert "provisional_archive_owner_confirmation" not in {
        item["check"] for item in result.blocking_findings
    }
    assert result.review_payload["metrics"][
        "resolved_carried_input_blocker_count"
    ] == 1
    assert result.review_payload["resolved_input_findings"] == [
        {
            "check": "provisional_archive_owner_confirmation",
            "resolution": (
                "confirmed_legacy_snapshot_excluded_from_authoritative_inputs"
            ),
        }
    ]
