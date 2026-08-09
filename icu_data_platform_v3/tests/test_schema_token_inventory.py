from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from asic_pipeline.errors import SchemaTokenError
from asic_pipeline.ingestion import (
    IngestionConfig,
    build_ingestion_audit,
    ingest_hospital,
    write_ingestion_audit_bundle,
)
from asic_pipeline.inventory import build_raw_inventory, write_inventory_report_bundle
from asic_pipeline.inventory.policy import load_inventory_policy
from asic_pipeline.schema_tokens import (
    SchemaTokenConfig,
    build_schema_token_inventory,
    load_schema_token_policy,
    write_schema_token_inventory_bundle,
)
from asic_pipeline.schema_tokens.inventory import (
    _ColumnStats,
    _classify_token,
    _shape_classification,
    _shape_classification_detail,
)


def _prepare_validated_ingestion(
    inventory_config: object,
    raw_root: Path,
    project_root: Path,
    *,
    inventory_run_id: str,
    audit_run_id: str,
) -> SchemaTokenConfig:
    inventory = build_raw_inventory(inventory_config)  # type: ignore[arg-type]
    assert inventory.overall_status == "pass"
    write_inventory_report_bundle(
        inventory,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id=inventory_run_id,
    )
    ingestion = IngestionConfig(
        inventory=inventory_config,  # type: ignore[arg-type]
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=raw_root.parent / "data",
        rows_per_batch=1,
    )
    inventory_policy = load_inventory_policy(
        project_root / "asic/config/inventory/policy.yaml"
    )
    for mapping in inventory_policy.hospital_mappings:
        ingest_hospital(
            ingestion,
            inventory_run_id,
            mapping.canonical_hospital_id,
        )
    audit = build_ingestion_audit(ingestion, inventory_run_id)
    assert audit.overall_status == "pass"
    write_ingestion_audit_bundle(
        audit,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id=audit_run_id,
    )
    return SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )


def _write_uk00_positional_fixture(raw_root: Path) -> None:
    stay_id = "synthetic_stay_00"
    headers = [f"column_{index}" for index in range(125)]
    headers[0] = "Pseudo-ID"
    headers[1] = "Zeit_ab_Aufnahme"
    headers[2] = "I:E"
    headers[121] = "ARDS_Diagnose_App"
    headers[122] = "ARDS_Diagnose_App"
    first = [""] * 125
    first[0] = stay_id
    first[1] = "0"
    first[2] = "1:2"
    first[121] = "1"
    second = list(first)
    second[1] = "60"
    second[2] = "0,5"
    (raw_root / "00" / f"{stay_id}.csv").write_text(
        ";".join(headers)
        + "\n"
        + ";".join(first)
        + "\n"
        + ";".join(second)
        + "\n",
        encoding="utf-8",
        newline="",
    )


def test_schema_token_inventory_is_lossless_bounded_and_review_gated(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    _write_uk00_positional_fixture(raw_root)
    config = _prepare_validated_ingestion(
        inventory_config,
        raw_root,
        project_root,
        inventory_run_id="schema_inventory_input",
        audit_run_id="schema_audit_input",
    )

    result = build_schema_token_inventory(
        config,
        "schema_inventory_input",
        "schema_audit_input",
    )

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    blocker_names = {item["check"] for item in result.blocking_findings}
    assert blocker_names == {
        "all_raw_columns_in_candidate_registry",
        "candidate_harmonization_registry_approved",
    }
    metrics = result.review_payload["metrics"]
    assert metrics["static_row_count"] == 8
    assert metrics["dynamic_row_count"] == 16
    assert metrics["numeric_unresolved_nonempty_token_count"] == 0
    assert metrics["reviewed_drop_candidate_nonempty_count"] == 0
    assert metrics["token_classification_counts"]["ratio_syntax_candidate"] == 8
    assert metrics["token_classification_counts"][
        "decimal_comma_syntax_candidate"
    ] == 8

    ie_rows = [
        row
        for row in result.private_columns
        if row["raw_name"] == "I:E"
    ]
    assert len(ie_rows) == 8
    assert all(row["candidate_target"] == "ie_ratio" for row in ie_rows)
    second_ards = next(
        row
        for row in result.private_columns
        if row["hospital"] == "asic_UK00"
        and row["raw_name"] == "ARDS_Diagnose_App"
        and row["raw_occurrence"] == 2
    )
    assert second_ards["raw_nonempty_count"] == 0
    assert second_ards["candidate_target"] is None
    assert second_ards["candidate_status"] == (
        "reviewed_all_empty_drop_candidate_requires_inventory_revalidation"
    )

    private_tokens = json.dumps(result.private_tokens, ensure_ascii=False)
    assert "1:2" in private_tokens
    assert "0,5" in private_tokens
    assert "synthetic_stay" not in private_tokens
    review = json.dumps(result.review_payload, ensure_ascii=False)
    assert "synthetic_stay" not in review
    assert "ARDS_Diagnose_App" not in review
    assert '"I:E"' not in review

    written = write_schema_token_inventory_bundle(
        result,
        inventory_config.paths.reports,  # type: ignore[attr-defined]
        run_id="schema_token_output",
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    assert written.review_markdown_path is not None
    assert written.review_markdown_path.is_file()
    with pytest.raises(SchemaTokenError, match="will not be overwritten"):
        write_schema_token_inventory_bundle(
            result,
            inventory_config.paths.reports,  # type: ignore[attr-defined]
            run_id="schema_token_output",
        )


def test_schema_token_inventory_detects_input_changed_after_audit(
    synthetic_inventory: tuple[object, Path],
    project_root: Path,
) -> None:
    inventory_config, raw_root = synthetic_inventory
    config = _prepare_validated_ingestion(
        inventory_config,
        raw_root,
        project_root,
        inventory_run_id="hash_schema_input",
        audit_run_id="hash_audit_input",
    )
    manifest_path = (
        config.ingestion.output_root / "asic_UK03" / "ingestion_manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["outputs"]["dynamic"]["sha256"] = "0" * 64
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="",
    )
    manifest_path.chmod(0o600)

    result = build_schema_token_inventory(
        config,
        "hash_schema_input",
        "hash_audit_input",
    )

    assert result.has_technical_failure is True
    assert {item["check"] for item in result.technical_blocking_findings} == {
        "input_parquet_hashes_match"
    }


def test_candidate_numeric_token_shapes_are_partitioned() -> None:
    missing = frozenset({"storniert"})

    assert _shape_classification("1.25", missing) == ("direct_numeric", 1.25)
    assert _shape_classification("0,5", missing) == (
        "decimal_comma_syntax_candidate",
        0.5,
    )
    assert _shape_classification("1/2", missing) == (
        "ratio_syntax_candidate",
        0.5,
    )
    assert _shape_classification("1:0", missing) == (
        "unresolved_ratio_zero_denominator",
        None,
    )
    assert _shape_classification("< 0,1", missing) == (
        "threshold_syntax_candidate",
        0.1,
    )
    assert _shape_classification("25%", missing) == (
        "percentage_syntax_candidate",
        25.0,
    )
    assert _shape_classification("storniert", missing) == (
        "textual_missing_candidate",
        None,
    )
    assert _shape_classification("unknown", missing) == (
        "unresolved_token",
        None,
    )
    assert _shape_classification(
        "[170.0]",
        missing,
        approved_parser="numeric_list",
        approved_list_missing_tokens=("nan",),
    ) == ("custom_parsed_numeric_list", 170.0)
    assert _shape_classification(
        "[170.0, 180.0]",
        missing,
        approved_parser="numeric_list",
        approved_list_missing_tokens=("nan",),
    ) == ("custom_parsed_numeric_list", None)
    with_missing = _shape_classification_detail(
        "[8.0, nan, 175.0]",
        missing,
        approved_parser="numeric_list",
        approved_list_missing_tokens=("nan",),
    )
    assert with_missing.name == "custom_parsed_numeric_list_with_missing_elements"
    assert with_missing.numeric_values == (8.0, 175.0)
    assert with_missing.list_element_count == 3
    assert with_missing.list_approved_missing_element_count == 1
    malformed = _shape_classification_detail(
        "[170.0, unknown]",
        missing,
        approved_parser="numeric_list",
        approved_list_missing_tokens=("nan",),
    )
    assert malformed.name == "unresolved_token"
    assert malformed.list_unresolved_element_count == 1


def test_schema_token_policy_combines_seed_and_partial_reviewed_registry(
    project_root: Path,
) -> None:
    policy = load_schema_token_policy(
        project_root / "asic/config/schema_tokens/policy.yaml"
    )

    assert policy.policy_version == "0.3"
    assert policy.rows_per_batch == 10000
    assert policy.store_identifier_examples is False
    assert policy.store_free_text_examples is False
    assert len(policy.candidate_rules) == 186
    assert policy.reviewed_rule_count == 18
    assert policy.reviewed_evidence_run_id == "20260805T064135Z"
    assert sum(
        rule.hospital is None
        and rule.table == "static"
        and rule.occurrence is None
        for rule in policy.candidate_rules
    ) == 26
    assert sum(
        rule.hospital is None
        and rule.table == "dynamic"
        and rule.occurrence is None
        for rule in policy.candidate_rules
    ) == 141
    assert policy.candidate_for("dynamic", "I:E", 1).target == "ie_ratio"  # type: ignore[union-attr]
    assert policy.candidate_for("dynamic", "I:E_eingestellt", 1).target == "ie_ratio_set"  # type: ignore[union-attr]
    assert policy.candidate_for("dynamic", "iSOFA.iSOFA_Gesamt", 1).target == "isofa_total_score"  # type: ignore[union-attr]
    duplicate = policy.candidate_for("dynamic", "ARDS_Diagnose_App", 2)
    assert duplicate is not None
    assert duplicate.target is None
    assert "requires_inventory_revalidation" in duplicate.status

    uk00_cluster = policy.candidate_for(
        "static", "Cluster-ID", 1, hospital="asic_UK00"
    )
    assert uk00_cluster is not None
    assert uk00_cluster.kind == "categorical"
    assert uk00_cluster.allowed_tokens == ("C1", "C2", "C3", "C4")
    assert _classify_token("C3", uk00_cluster, frozenset()) == (
        "approved_categorical_token",
        None,
    )
    assert _classify_token("C5", uk00_cluster, frozenset()) == (
        "unapproved_categorical_token",
        None,
    )

    height = policy.candidate_for(
        "static", "heightcm", 1, hospital="asic_UK00"
    )
    assert height is not None
    assert height.target == "height_measurements_cm"
    assert height.approved_parser == "numeric_list"
    assert height.approved_list_missing_tokens == ("nan",)
    assert height.canonical_value_type == "list_float64"
    assert height.expected_unit == "cm"
    assert height.preserve_list_order is True
    assert height.preserve_duplicates is True
    assert height.review_rule_id == "HARM-STATIC-HEIGHT-UK00-002"
    assert height.supersedes_rule_id == "HARM-STATIC-HEIGHT-UK00-001"
    assert height.evidence_run_id == "20260805T081315Z"

    nbsp_alias = policy.candidate_for(
        "static",
        "Dialyse_(dialysefreie_Tage)\u00a0",
        1,
        hospital="asic_UK01",
    )
    assert nbsp_alias is not None
    assert nbsp_alias.target == "dialysis_free_days"
    assert policy.candidate_for(
        "static",
        "Dialyse_(dialysefreie_Tage)\u00a0",
        1,
        hospital="asic_UK02",
    ) is None

    sentinel = policy.candidate_for(
        "static", "Liegedauer_KH", 1, hospital="asic_UK08"
    )
    assert sentinel is not None
    assert sentinel.approved_missing_sentinel_tokens == ("-1", "-1.0")
    uk07_hosp_los = policy.candidate_for(
        "static", "Liegedauer_KH", 1, hospital="asic_UK07"
    )
    assert uk07_hosp_los is not None
    assert uk07_hosp_los.approved_missing_sentinel_tokens == ()


def test_approved_numeric_grammar_truncation_is_scoped_and_nonblocking(
    project_root: Path,
) -> None:
    policy = load_schema_token_policy(
        project_root / "asic/config/schema_tokens/policy.yaml"
    )
    height = policy.candidate_for(
        "static", "heightcm", 1, hospital="asic_UK00"
    )
    assert height is not None
    height_stats = _ColumnStats(
        hospital="asic_UK00",
        table="static",
        physical_name="heightcm",
        raw_name="heightcm",
        occurrence=1,
        candidate=height,
        source_file_count=1,
        source_schema_variant_count=1,
        source_schema_present_row_count=3,
        expected_rows=3,
        maximum_examples=1,
        store_direct_numeric_examples=False,
        store_identifier_examples=False,
        store_free_text_examples=False,
    )
    height_stats.add_batch(
        pa.array(["[170.0]", "[8.0, nan, 175.0]", "[180.0, 180.0]"]),
        policy,
    )
    assert height_stats.numeric_list_cell_count == 3
    assert height_stats.numeric_list_element_count == 6
    assert height_stats.numeric_list_numeric_element_count == 5
    assert height_stats.numeric_list_approved_missing_element_count == 1
    assert height_stats.numeric_list_unresolved_element_count == 0
    assert height_stats.has_review_blocking_truncation() is False

    approved = policy.candidate_for(
        "dynamic", "PEEP", 1, hospital="asic_UK01"
    )
    assert approved is not None
    approved_stats = _ColumnStats(
        hospital="asic_UK01",
        table="dynamic",
        physical_name="PEEP",
        raw_name="PEEP",
        occurrence=1,
        candidate=approved,
        source_file_count=1,
        source_schema_variant_count=1,
        source_schema_present_row_count=2,
        expected_rows=2,
        maximum_examples=1,
        store_direct_numeric_examples=False,
        store_identifier_examples=False,
        store_free_text_examples=False,
    )
    approved_stats.add_batch(pa.array(["1,1", "2,2"]), policy)
    assert approved_stats.examples.truncated_classes == {
        "decimal_comma_syntax_candidate"
    }
    assert approved_stats.has_review_blocking_truncation() is False

    unreviewed = policy.candidate_for(
        "dynamic", "I:E", 1, hospital="asic_UK01"
    )
    assert unreviewed is not None
    unreviewed_stats = _ColumnStats(
        hospital="asic_UK01",
        table="dynamic",
        physical_name="I:E",
        raw_name="I:E",
        occurrence=1,
        candidate=unreviewed,
        source_file_count=1,
        source_schema_variant_count=1,
        source_schema_present_row_count=2,
        expected_rows=2,
        maximum_examples=1,
        store_direct_numeric_examples=False,
        store_identifier_examples=False,
        store_free_text_examples=False,
    )
    unreviewed_stats.add_batch(pa.array(["1:2", "1:3"]), policy)
    assert unreviewed_stats.has_review_blocking_truncation() is True
