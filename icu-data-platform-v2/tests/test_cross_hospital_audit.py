from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.audit import audit_cross_hospital_data_quality
from asic_pipeline.audit import write_audit_report
from asic_pipeline.contracts import load_translated_input_contract
from asic_pipeline.data_quality import (
    load_cross_hospital_audit_config,
    load_data_quality_policy,
)
from asic_pipeline.errors import ContractError


PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT_PATH = PROJECT_ROOT / "asic" / "config" / "translated" / "contract.yaml"
POLICY_PATH = (
    PROJECT_ROOT / "asic" / "config" / "translated" / "data_quality_audit.yaml"
)
HOSPITAL_CODES = (0, 2, 3, 4, 6, 7, 8)


def _arrow_type(name: str) -> pa.DataType:
    if name == "timestamp[us]":
        return pa.timestamp("us")
    return pa.type_for_alias(name)


def _base_values(contract, table_name: str, rows: int) -> dict[str, list]:
    table = getattr(contract, table_name)
    result = {}
    for column in table.columns:
        arrow_type = _arrow_type(column.arrow_type_for("mock"))
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            result[column.name] = [None] * rows
        elif pa.types.is_boolean(arrow_type):
            result[column.name] = [None] * rows
        elif pa.types.is_timestamp(arrow_type):
            result[column.name] = [None] * rows
        else:
            result[column.name] = [None] * rows
    return result


def _write_table(path: Path, contract_table, values: dict[str, list]) -> None:
    schema = pa.schema(
        [
            pa.field(column.name, _arrow_type(column.arrow_type_for("mock")))
            for column in contract_table.columns
        ]
    )
    arrays = [pa.array(values[field.name], type=field.type) for field in schema]
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path)


def _write_fixture(root: Path) -> Path:
    contract = load_translated_input_contract(CONTRACT_PATH)
    translated = root / "asic" / "data" / "mock" / "translated"
    reports = root / "asic" / "reports" / "mock"
    translated.mkdir(parents=True)

    static = _base_values(contract, "static", len(HOSPITAL_CODES))
    for index, code in enumerate(HOSPITAL_CODES):
        static["stay_id_global"][index] = f"stay-{index}:{code}"
        static["hospital_id"][index] = f"asic_UK{code:02d}"
        static["hospital_code_source"][index] = float(code)
        static["cluster_id"][index] = index
        static["hosp_los"][index] = 5.0
        static["icu_los"][index] = 3.0
        static["icu_readmit"][index] = 0.0
        static["dialysis_free_days"][index] = 20.0
        static["vent_free_days"][index] = 20.0
    static["sex"][0] = "male"
    static["height_cm"][0] = 173.0
    static["weight_kg"][0] = 80.0
    static["hosp_los"][0] = -1.0
    _write_table(translated / "static.parquet", contract.static, static)

    rows = len(HOSPITAL_CODES) * 2
    dynamic = _base_values(contract, "dynamic", rows)
    for hospital_index, code in enumerate(HOSPITAL_CODES):
        for offset in range(2):
            index = hospital_index * 2 + offset
            dynamic["stay_id_global"][index] = f"stay-{hospital_index}:{code}"
            dynamic["hospital_id"][index] = f"asic_UK{code:02d}"
            dynamic["hospital_code_source"][index] = code
            dynamic["minutes_since_icu_admission"][index] = float(offset * 60)
            dynamic["anchored_time_since_icu_admission"][index] = (
                datetime(2020, 1, 1) + timedelta(minutes=offset * 60)
            )
            dynamic["heart_rate"][index] = 70.0 + hospital_index
            dynamic["albumin"][index] = 3.0
            dynamic["insp_pressure"][index] = 20.0
            dynamic["peep"][index] = 8.0
            dynamic["delta_p_computed"][index] = 12.0
            dynamic["delta_p_reported"][index] = 12.0
            dynamic["ie_ratio"][index] = 0.5
            dynamic["core_temp"][index] = 37.0
            dynamic["evlwi"][index] = 10.0
            dynamic["ph_art"][index] = 7.4
            dynamic["map"][index] = 80.0
            dynamic["sbp"][index] = 120.0
            dynamic["dbp"][index] = 60.0
            dynamic["scvo2"][index] = 70.0
    dynamic["albumin"][0] = 0.0
    uk00_pbw = 50.0 + 0.91 * (173.0 - 152.4)
    dynamic["vt"][0] = uk00_pbw * 7.0
    dynamic["vt"][1] = uk00_pbw * 7.0
    dynamic["vt_per_kg"][0] = 7.0
    dynamic["vt_per_kg"][1] = 7.0
    dynamic["vt_per_ideal_bw_total"][0] = uk00_pbw * 6.0
    dynamic["vt_per_ideal_bw_total"][1] = uk00_pbw * 6.0
    dynamic["heart_rate"][1] = 1000.0
    dynamic["delta_p_computed"][1] = 13.0
    dynamic["ph_art"][0] = 745.0
    dynamic["ph_art"][2] = 8.0
    uk03_first = HOSPITAL_CODES.index(3) * 2
    dynamic["fio2"][uk03_first] = 0.5
    dynamic["fio2"][uk03_first + 1] = 50.0
    uk04_first = HOSPITAL_CODES.index(4) * 2
    dynamic["map"][uk04_first] = 65000.0
    dynamic["map"][uk04_first + 1] = 65.0
    dynamic["sbp"][uk04_first] = 138.0
    dynamic["dbp"][uk04_first] = 75.0
    uk07_first = HOSPITAL_CODES.index(7) * 2
    dynamic["core_temp"][uk07_first] = 3.7
    uk08_first = HOSPITAL_CODES.index(8) * 2
    dynamic["evlwi"][uk08_first] = 62260.0
    dynamic["scvo2"][0] = 5.0
    dynamic["etco2"][HOSPITAL_CODES.index(4) * 2] = 260.0
    dynamic["norepinephrine_iv_cont"][HOSPITAL_CODES.index(3) * 2] = 20.0
    dynamic["vt_per_kg_ideal_body_weight"][HOSPITAL_CODES.index(6) * 2] = 80.0
    dynamic["clonidine_iv_cont"][HOSPITAL_CODES.index(8) * 2] = 10.0
    for column in (
        "isofa_cardiovascular",
        "isofa_liver",
        "isofa_respiratory",
        "isofa_renal",
        "isofa_thrombocyte",
        "isofa_cns",
    ):
        dynamic[column][0] = 1.0
    dynamic["isofa_total_score"][0] = 6.0
    _write_table(translated / "dynamic.parquet", contract.dynamic, dynamic)

    config = {
        "dataset_context": "mock",
        "translated_contract": str(CONTRACT_PATH),
        "data_quality_policy": str(POLICY_PATH),
        "paths": {
            "translated": str(translated),
            "reports": str(reports),
        },
        "cross_hospital_audit": {
            "batch_size": 5,
            "quantile_sample_size_per_hospital_variable": 100,
            "max_examples": 5,
        },
    }
    config_path = root / "asic" / "config" / "datasets" / "mock.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _audit(config_path: Path):
    config = load_cross_hospital_audit_config(config_path)
    contract = load_translated_input_contract(config.translated_contract_path)
    policy = load_data_quality_policy(config.policy_path, contract)
    return audit_cross_hospital_data_quality(config, contract, policy)


def test_policy_preserves_complete_legacy_coverage() -> None:
    contract = load_translated_input_contract(CONTRACT_PATH)
    policy = load_data_quality_policy(POLICY_PATH, contract)

    assert contract.contract_version == "1.3"
    assert policy.policy_version == "1.10"
    assert policy.translated_contract_version == "1.3"
    assert policy.scale_entry_discovery.source == (
        "bounded_legacy_invalid_value_rules"
    )
    assert policy.scale_entry_discovery.factors == (10.0, 100.0, 1000.0)
    assert policy.scale_entry_discovery.minimum_finite_values_for_pattern == 20
    assert policy.candidate_context_audit.windows_hours == (1.0, 4.0, 8.0)
    assert policy.candidate_context_audit.maximum_candidate_rows == 100_000
    context_fields = {
        field.column: field for field in policy.candidate_context_audit.fields
    }
    assert set(context_fields) == {
        "core_temp",
        "evlwi",
        "ph_art",
        "fio2",
        "map",
        "spo2",
        "sao2",
        "scvo2",
    }
    assert context_fields["fio2"].excluded_hospitals == ("asic_UK03",)
    assert context_fields["map"].related_fields == ("sbp", "dbp")
    pbw = policy.predicted_body_weight_tidal_volume_audit
    assert pbw.audit_id == "uk00_ardsnet_pbw_tidal_volume_formula_review"
    assert pbw.hospital == "asic_UK00"
    assert pbw.male_intercept_kg == 50.0
    assert pbw.female_intercept_kg == 45.5
    assert pbw.height_coefficient_kg_per_cm == 0.91
    assert pbw.target_ml_per_kg_pbw == 6.0
    assert pbw.target_absolute_vt_column == "vt_per_ideal_bw_total"
    assert policy.legacy_coverage == {
        "invalid_value_rule_count": 44,
        "expanded_v2_invalid_value_check_count": 47,
        "targeted_semantic_finding_count": 5,
        "static_minus_one_sentinel_count": 6,
    }
    assert len(policy.invalid_value_rules) == 44
    assert sum(len(rule.columns) for rule in policy.invalid_value_rules) == 47
    assert policy.targeted_review_coverage == {
        "scale_hypothesis_count": 11,
        "row_level_scale_entry_audit_count": 1,
        "comparison_audit_count": 13,
        "bucket_audit_count": 4,
        "cross_table_formula_audit_count": 1,
    }
    assert len(policy.targeted_scale_audits) == 11
    assert len(policy.row_level_scale_entry_audits) == 1
    assert len(policy.targeted_comparison_audits) == 13
    assert len(policy.targeted_bucket_audits) == 4
    assert len(policy.relationship_audits) == 10

    scales = {audit.audit_id: audit for audit in policy.targeted_scale_audits}
    assert scales["uk06_vt_per_kg_mixed_scale_candidate"].threshold == 1000
    assert (
        scales["uk06_vt_per_kg_ideal_body_weight_mixed_scale_candidate"]
        .peer_hospitals
        == ("asic_UK02", "asic_UK03", "asic_UK04", "asic_UK07", "asic_UK08")
    )
    assert scales["uk03_d_dimer_peer_scale_candidate"].peer_hospitals == (
        "asic_UK00",
        "asic_UK02",
        "asic_UK07",
    )


def test_targeted_scale_peer_scope_rejects_target_hospital(
    tmp_path: Path,
) -> None:
    contract = load_translated_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    target = next(
        item
        for item in raw["targeted_scale_audits"]
        if item["id"] == "uk03_d_dimer_peer_scale_candidate"
    )
    target["peer_hospitals"] = ["asic_UK03"]
    policy_path = tmp_path / "invalid_peer_scope.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="must not include the target"):
        load_data_quality_policy(policy_path, contract)


def test_scale_entry_discovery_rejects_duplicate_factors(
    tmp_path: Path,
) -> None:
    contract = load_translated_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    raw["scale_entry_discovery"]["factors"] = [10, 10, 100]
    policy_path = tmp_path / "invalid_discovery_factors.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="unique and strictly increasing"):
        load_data_quality_policy(policy_path, contract)


def test_candidate_context_rejects_invalid_map_relationship(
    tmp_path: Path,
) -> None:
    contract = load_translated_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    map_field = next(
        item
        for item in raw["candidate_context_audit"]["fields"]
        if item["column"] == "map"
    )
    map_field["related_fields"] = ["sbp"]
    policy_path = tmp_path / "invalid_context_relationship.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="must contain sbp and dbp"):
        load_data_quality_policy(policy_path, contract)


def test_pbw_formula_audit_rejects_unknown_static_field(
    tmp_path: Path,
) -> None:
    contract = load_translated_input_contract(CONTRACT_PATH)
    raw = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    raw["predicted_body_weight_tidal_volume_audit"][
        "height_column"
    ] = "missing_height"
    policy_path = tmp_path / "invalid_pbw_static_field.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ContractError, match="unknown static columns"):
        load_data_quality_policy(policy_path, contract)


def test_read_only_audit_profiles_and_retains_legacy_findings(tmp_path: Path) -> None:
    config_path = _write_fixture(tmp_path)
    translated = tmp_path / "asic" / "data" / "mock" / "translated"
    before = {
        path.name: path.read_bytes()
        for path in translated.glob("*.parquet")
    }

    report = _audit(config_path)

    assert report.overall_status == "warning"
    assert report.metrics["numeric_profile_column_counts"] == {
        "static": 9,
        "dynamic": 127,
    }
    invalid = report.metrics["legacy_invalid_value_findings"]
    assert next(
        row
        for row in invalid
        if row["column"] == "albumin" and row["hospital"] == "asic_UK00"
    )["invalid_count"] == 1
    sentinels = report.metrics["legacy_static_sentinel_findings"]
    assert next(row for row in sentinels if row["column"] == "hosp_los")["count"] == 1
    known = {row["id"]: row for row in report.metrics["known_legacy_semantic_findings"]}
    assert known["uk03_norepinephrine_off_scale"]["verification_status"] == (
        "legacy_site_values_present_manual_review_required"
    )
    relationship = next(
        row
        for row in report.metrics["relationship_audits"]
        if row["id"] == "computed_driving_pressure_formula"
    )
    assert relationship["total_outside_tolerance_count"] == 1
    scoped_relationship = next(
        row
        for row in report.metrics["relationship_audits"]
        if row["id"] == "uk00_vt_vs_ideal_body_weight_field"
    )
    assert scoped_relationship["hospital_scope"] == ["asic_UK00"]
    assert [row["hospital"] for row in scoped_relationship["by_hospital"]] == [
        "asic_UK00"
    ]

    scale_hypotheses = {
        row["id"]: row for row in report.metrics["targeted_scale_hypotheses"]
    }
    etco2 = scale_hypotheses["uk04_etco2_legacy_divisor_candidate"]
    assert etco2["approval_status"] == "hypothesis_only_not_approved"
    assert etco2["candidate_is_hypothetical_and_not_written"] is True
    assert etco2["numerically_changed_count"] == 1
    assert etco2["before_outside_expected_range_count"] == 1
    assert etco2["after_outside_expected_range_count"] == 0
    assert etco2["hypothetical_candidate_profile"]["median"] == (
        260.0 / 7.50062
    )
    d_dimer = scale_hypotheses["uk03_d_dimer_peer_scale_candidate"]
    assert d_dimer["peer_hospital_scope"] == [
        "asic_UK00",
        "asic_UK02",
        "asic_UK07",
    ]
    assert [row["hospital"] for row in d_dimer["peer_raw_profiles"]] == [
        "asic_UK00",
        "asic_UK02",
        "asic_UK07",
    ]

    relationship_ids = {
        row["id"] for row in report.metrics["relationship_audits"]
    }
    assert {"scvo2_vs_sao2", "scvo2_vs_spo2"} <= relationship_ids
    comparison_ids = {
        row["id"]
        for row in report.metrics["targeted_cross_hospital_comparisons"]
    }
    assert {
        "urea_unit_review",
        "scvo2_definition_review",
        "vt_per_ideal_bw_total_definition_review",
    } <= comparison_ids

    pbw_audit = report.metrics[
        "predicted_body_weight_tidal_volume_audit"
    ]
    assert pbw_audit["status"] == (
        "hypothesis_evidence_only_not_approved_for_cleaning"
    )
    assert pbw_audit["interpretation_status"] == (
        "not_interpretable_mock_columns_were_independently_shuffled"
    )
    assert pbw_audit["static_reference"][
        "target_hospital_static_rows"
    ] == 1
    assert pbw_audit["static_reference"][
        "valid_predicted_body_weight_count"
    ] == 1
    assert pbw_audit["dynamic_join"]["rows_scanned"] == 14
    assert pbw_audit["dynamic_join"][
        "rows_without_static_reference"
    ] == 0
    pbw_comparisons = pbw_audit["comparisons"]
    target_formula = pbw_comparisons[
        "target_absolute_vt_vs_six_ml_per_kg_pbw"
    ]
    assert target_formula["both_present_count"] == 2
    assert target_formula["within_tolerance_count"] == 2
    assert target_formula["outside_tolerance_count"] == 0
    normalized_formula = pbw_comparisons[
        "reported_vt_per_kg_vs_vt_div_pbw"
    ]
    assert normalized_formula["both_present_count"] == 2
    assert normalized_formula["within_tolerance_count"] == 2
    actual_weight_formula = pbw_comparisons[
        "target_absolute_vt_vs_vt_scaled_by_pbw_over_actual_weight"
    ]
    assert actual_weight_formula["both_present_count"] == 2
    assert actual_weight_formula["outside_tolerance_count"] == 2
    assert pbw_audit["normalized_profiles"][
        "target_absolute_vt_div_pbw"
    ]["median"] == pytest.approx(6.0)
    assert pbw_audit["normalized_profiles"]["vt_div_pbw"][
        "median"
    ] == pytest.approx(7.0)
    assert pbw_audit["identifiers_written_to_report"] is False

    formula_linkage_check = next(
        check
        for check in report.checks
        if check.name
        == "uk00_pbw_formula_audit_has_complete_static_dynamic_linkage"
    )
    assert formula_linkage_check.status == "pass"
    target_formula_check = next(
        check
        for check in report.checks
        if check.name
        == "uk00_target_absolute_vt_matches_six_ml_per_kg_pbw"
    )
    assert target_formula_check.status == "skipped"
    normalized_formula_check = next(
        check
        for check in report.checks
        if check.name == "uk00_reported_vt_per_kg_matches_vt_div_pbw"
    )
    assert normalized_formula_check.status == "skipped"
    actual_weight_formula_check = next(
        check
        for check in report.checks
        if check.name
        == "uk00_target_difference_from_vt_is_explained_only_by_actual_vs_pbw"
    )
    assert actual_weight_formula_check.status == "skipped"
    assert actual_weight_formula_check.severity == "advisory"

    row_level = report.metrics["row_level_scale_entry_audits"]
    assert len(row_level) == 1
    ph_recovery = row_level[0]
    assert ph_recovery["id"] == "ph_art_divide_100_entry_error_candidate"
    assert ph_recovery["total_recoverable_candidate_count"] == 1
    assert ph_recovery["total_unrecoverable_outside_count"] == 1
    uk00_ph = next(
        row
        for row in ph_recovery["by_hospital"]
        if row["hospital"] == "asic_UK00"
    )
    assert uk00_ph["candidate_examples"] == [
        {"raw": 745.0, "hypothetical_recovered": 7.45}
    ]
    row_level_check = next(
        check
        for check in report.checks
        if check.name
        == "row_level_scale_entry_recovery_candidates_are_absent"
    )
    assert row_level_check.status == "fail"
    assert row_level_check.severity == "advisory"

    discovery = report.metrics["scale_entry_candidate_discovery"]
    assert discovery["status"] == "discovery_only_not_approved_for_cleaning"
    assert discovery["candidate_values_are_hypothetical_and_not_written"] is True
    assert discovery["audited_field_count"] == 44
    assert discovery["total_uniquely_recoverable_count"] >= 1
    assert discovery["total_ambiguous_transform_count"] >= 1
    assert "albumin" in discovery["excluded_unbounded_rule_columns"]
    assert "d_dimer" in discovery[
        "excluded_numeric_fields_without_complete_bounds"
    ]

    discovered_fields = {
        row["column"]: row for row in discovery["audited_fields"]
    }
    discovered_ph = discovered_fields["ph_art"]
    assert discovered_ph["has_explicit_row_level_recovery_audit"] is True
    discovered_ph_uk00 = next(
        row
        for row in discovered_ph["by_hospital"]
        if row["hospital"] == "asic_UK00"
    )
    assert discovered_ph_uk00["uniquely_recoverable_count"] == 1
    assert discovered_ph_uk00["unique_transformations"] == [
        {
            "id": "divide_by_100",
            "operation": "divide",
            "factor": 100.0,
            "uniquely_recoverable_count": 1,
            "hypothetical_recovered_profile": {
                "finite_count": 1,
                "finite_rate": 1.0,
                "min": 7.45,
                "q01": 7.45,
                "q1": 7.45,
                "median": 7.45,
                "q3": 7.45,
                "q99": 7.45,
                "max": 7.45,
                "zero_count": 0,
                "negative_count": 0,
            },
            "hypothetical_recovered_median_within_valid_q01_q99": False,
            "candidate_examples": [
                {"raw": 745.0, "hypothetical_recovered": 7.45}
            ],
        }
    ]

    discovered_heart_rate = discovered_fields["heart_rate"]
    discovered_hr_uk00 = next(
        row
        for row in discovered_heart_rate["by_hospital"]
        if row["hospital"] == "asic_UK00"
    )
    assert discovered_hr_uk00["ambiguous_transform_count"] == 1
    assert discovered_hr_uk00["ambiguous_examples"] == [
        {
            "raw": 1000.0,
            "possible_transformations": [
                "divide_by_10",
                "divide_by_100",
                "divide_by_1000",
            ],
        }
    ]
    discovery_check = next(
        check
        for check in report.checks
        if check.name == "general_scale_entry_discovery_has_no_candidates"
    )
    assert discovery_check.status == "fail"
    assert discovery_check.severity == "advisory"
    discovery_coverage_check = next(
        check
        for check in report.checks
        if check.name
        == "scale_entry_discovery_covers_all_bounded_invalid_value_fields"
    )
    assert discovery_coverage_check.status == "pass"

    review_coverage = report.metrics[
        "scale_entry_candidate_review_coverage"
    ]
    assert review_coverage["status"] == "complete"
    assert review_coverage["uncovered_scope_count"] == 0
    assert review_coverage["uncovered_candidate_count"] == 0
    review_coverage_check = next(
        check
        for check in report.checks
        if check.name
        == "every_unique_scale_entry_candidate_scope_has_a_review_route"
    )
    assert review_coverage_check.status == "pass"

    context = report.metrics["scale_entry_candidate_context"]
    assert context["status"] == (
        "context_evidence_only_not_approved_for_cleaning"
    )
    assert context["selected_unique_candidate_count"] == 5
    assert context["collected_candidate_count"] == 5
    assert context["collection_complete"] is True
    assert context["second_pass_rows_scanned"] == 14
    assert context["identifiers_written_to_report"] is False
    context_groups = {
        (item["column"], item["hospital"]): item
        for item in context["groups"]
    }
    assert ("fio2", "asic_UK03") not in context_groups
    ph_context = context_groups[("ph_art", "asic_UK00")]
    ph_one_hour = ph_context["temporal_evidence_by_window"][0]
    assert ph_one_hour["window_hours"] == 1.0
    assert ph_one_hour["classification_counts"] == {
        "recovered_is_closer": 1
    }
    assert ph_one_hour["any_neighbor_count"] == 1
    assert ph_one_hour["both_neighbors_count"] == 0
    ph_next = ph_context["examples"][0]["temporal_evidence"][0][
        "next_neighbor"
    ]
    assert ph_next["value"] == pytest.approx(7.4)
    assert ph_next["gap_hours"] == 1.0
    map_context = context_groups[("map", "asic_UK04")]
    map_related = map_context["examples"][0][
        "same_row_related_evidence"
    ]
    assert map_related["relationship"] == "map_from_sbp_dbp"
    assert map_related["classification"] == "contradicts_recovery"
    assert map_related["related_values"] == {"dbp": 75.0, "sbp": 138.0}
    assert map_related["expected_map"] == pytest.approx(96.0)
    assert map_related["recovered_between_dbp_and_sbp"] is False
    assert map_related["contradiction_reason"] == (
        "recovered_map_outside_dbp_sbp_interval"
    )
    assert context_groups[("core_temp", "asic_UK07")][
        "temporal_evidence_by_window"
    ][0]["classification_counts"] == {"recovered_is_closer": 1}
    assert context_groups[("evlwi", "asic_UK08")][
        "temporal_evidence_by_window"
    ][0]["classification_counts"] == {"recovered_is_closer": 1}
    assert context_groups[("scvo2", "asic_UK00")][
        "temporal_evidence_by_window"
    ][0]["classification_counts"] == {"recovered_is_closer": 1}
    assert "stay-" not in json.dumps(context)
    assert "stay_id" not in json.dumps(context)
    context_check = next(
        check
        for check in report.checks
        if check.name == "scale_entry_candidate_context_collection_is_complete"
    )
    assert context_check.status == "pass"

    buckets = {
        row["id"]: row for row in report.metrics["targeted_bucket_audits"]
    }
    uk06_vt_ideal = buckets[
        "uk06_vt_per_kg_ideal_body_weight_scale_buckets"
    ]
    assert uk06_vt_ideal["finite_count"] == 1
    assert next(
        bucket
        for bucket in uk06_vt_ideal["buckets"]
        if bucket["interval"] == "(30.0, 100.0]"
    )["count"] == 1

    check = next(
        check
        for check in report.checks
        if check.name
        == "targeted_scale_hypotheses_are_approved_for_cleaning"
    )
    assert check.status == "fail"
    assert check.severity == "advisory"
    after = {
        path.name: path.read_bytes()
        for path in translated.glob("*.parquet")
    }
    assert after == before
    report_path = tmp_path / "asic" / "reports" / "mock" / "report.json"
    write_audit_report(report, report_path)
    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert loaded["overall_status"] == "warning"


def test_candidate_context_cap_is_a_blocking_completeness_failure(
    tmp_path: Path,
) -> None:
    config_path = _write_fixture(tmp_path)
    raw_policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    raw_policy["candidate_context_audit"]["maximum_candidate_rows"] = 1
    policy_path = tmp_path / "candidate_context_cap.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw_policy, sort_keys=False),
        encoding="utf-8",
    )
    raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw_config["data_quality_policy"] = str(policy_path)
    config_path.write_text(
        yaml.safe_dump(raw_config, sort_keys=False),
        encoding="utf-8",
    )

    report = _audit(config_path)

    context = report.metrics["scale_entry_candidate_context"]
    assert context["selected_unique_candidate_count"] == 5
    assert context["collected_candidate_count"] == 1
    assert context["omitted_due_to_cap_count"] == 4
    assert context["collection_complete"] is False
    check = next(
        item
        for item in report.checks
        if item.name == "scale_entry_candidate_context_collection_is_complete"
    )
    assert check.status == "fail"
    assert check.severity == "blocking"
    assert report.overall_status == "fail"


def test_unique_scale_candidate_without_review_route_is_blocking(
    tmp_path: Path,
) -> None:
    config_path = _write_fixture(tmp_path)
    raw_policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    raw_policy["candidate_context_audit"]["fields"] = [
        field
        for field in raw_policy["candidate_context_audit"]["fields"]
        if field["column"] != "core_temp"
    ]
    policy_path = tmp_path / "missing_candidate_review_route.yaml"
    policy_path.write_text(
        yaml.safe_dump(raw_policy, sort_keys=False),
        encoding="utf-8",
    )
    raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw_config["data_quality_policy"] = str(policy_path)
    config_path.write_text(
        yaml.safe_dump(raw_config, sort_keys=False),
        encoding="utf-8",
    )

    report = _audit(config_path)

    coverage = report.metrics["scale_entry_candidate_review_coverage"]
    assert coverage["status"] == "incomplete_blocking"
    assert coverage["uncovered_scope_count"] == 1
    assert coverage["uncovered_candidate_count"] == 1
    assert coverage["uncovered_scopes"] == [
        {
            "column": "core_temp",
            "hospital": "asic_UK07",
            "uniquely_recoverable_count": 1,
            "review_route": "uncovered",
            "review_route_id": None,
        }
    ]
    check = next(
        item
        for item in report.checks
        if item.name
        == "every_unique_scale_entry_candidate_scope_has_a_review_route"
    )
    assert check.status == "fail"
    assert check.severity == "blocking"
    assert report.overall_status == "fail"
