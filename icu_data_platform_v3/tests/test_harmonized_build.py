from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from asic_pipeline.cleaning import (
    CleanedPromotionConfig,
    CleaningConfig,
    promote_cleaned_release,
    run_cleaning_candidate_audit,
    run_cleaning_candidate_build,
)

from asic_pipeline.harmonization import (
    HarmonizedBuildAuditConfig,
    HarmonizedBuildConfig,
    HarmonizedPromotionConfig,
    load_reviewed_categorical_decisions,
    load_schema_dictionary_freeze_policy,
    run_harmonized_build,
    run_harmonized_build_audit,
    promote_harmonized_release,
)
from asic_pipeline.harmonization import harmonized_build as build_module
from asic_pipeline.harmonization import harmonized_build_audit as audit_module
from asic_pipeline.inventory.hashing import sha256_file


HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)


def _value(variable: str, physical_type: str, hospital: str) -> object:
    if variable in {"stay_id_global", "stay_id_local"}:
        return "stay:00" if variable == "stay_id_global" else "stay"
    if variable == "hospital_id":
        return hospital
    values = {
        "age_group": "<70",
        "bmi_group": "underweight",
        "cluster_id": {
            "asic_UK00": "C1",
            "asic_UK01": "C5",
            "asic_UK02": "C7",
            "asic_UK03": "C6",
            "asic_UK04": "C10",
            "asic_UK06": "C8",
            "asic_UK07": "C11",
            "asic_UK08": "C12",
        }[hospital],
        "death_status": "discharged_alive",
        "discharge_status": "transferred",
        "height_group": "<180",
        "hospital_mortality_reported": False,
        "icu_readmit": False,
        "sex": "female",
        "study_implementation_phase": "calibration",
        "weight_group": "<65",
        "icd10_codes_source_text": "A01",
        "ecmo": "0",
        "position_therapy": "0",
    }
    if variable == "ards_diagnosis_app":
        if hospital in {"asic_UK02", "asic_UK03", "asic_UK07"}:
            return None
        return "1" if hospital == "asic_UK06" else "0"
    if variable == "severity_read_confirmation":
        return None
    if variable in {"feo2", "sofa_score_without_gcs", "stroke_volume_bolus"}:
        return None
    if variable in values:
        return values[variable]
    if physical_type == "list<item: double>":
        return [170.0]
    if physical_type == "list<item: large_string>":
        return ["A01"]
    if physical_type == "fixed_size_list<item: bool>[2]":
        return [None, None]
    if physical_type == "bool":
        return False
    if physical_type == "int32":
        return 0
    if physical_type == "large_string":
        return "source"
    return 1.0


def _write_build_fixture(
    tmp_path: Path,
    project_root: Path,
) -> tuple[HarmonizedBuildConfig, object]:
    data = tmp_path / "data"
    reports = tmp_path / "reports"
    freeze_policy_path = (
        project_root
        / "asic/config/harmonization/reviewed_schema_dictionary_freeze_0_1.yaml"
    )
    freeze_policy = load_schema_dictionary_freeze_policy(freeze_policy_path)
    frozen_dir = data / "contracts/harmonized_schema_dictionary/0.1"
    frozen_dir.mkdir(parents=True)
    schema_payloads: dict[str, list[dict[str, object]]] = {}
    for table, fields in freeze_policy.schemas:
        schema_payloads[table] = [
            {
                "ordered_position": item.position,
                "variable": item.name,
                "physical_type": item.physical_type,
                "unit": item.unit,
                "analysis_eligibility": item.eligibility,
            }
            for item in fields
        ]
        (frozen_dir / f"{table}_schema.json").write_text(
            json.dumps(schema_payloads[table]), encoding="utf-8"
        )
    (frozen_dir / "variable_dictionary.parquet").write_bytes(b"test dictionary")
    frozen_files = {
        path.name: sha256_file(path)
        for path in frozen_dir.iterdir()
        if path.is_file()
    }
    (frozen_dir / "freeze_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_frozen_harmonized_schema_dictionary",
                "contract_version": "0.1",
                "schema_frozen": True,
                "dictionary_frozen": True,
                "publication_ready": False,
                "harmonized_build_implementation_authorized": True,
                "lineage": {"candidate_run_id": "20260806T074852Z"},
                "files": frozen_files,
            }
        ),
        encoding="utf-8",
    )

    candidate_root = data / "harmonization_candidates/20260806T074852Z"
    hospital_manifests: list[dict[str, object]] = []
    provenance_types = dict(freeze_policy.provenance_fields)
    for hospital_index, hospital in enumerate(HOSPITALS):
        hospital_root = candidate_root / hospital
        hospital_root.mkdir(parents=True)
        outputs: dict[str, object] = {}
        for table in ("static", "dynamic"):
            rows = schema_payloads[table]
            fields = [
                pa.field(
                    str(row["variable"]),
                    (
                        pa.large_string()
                        if row["variable"]
                        in {"ards_diagnosis_app", "ecmo", "position_therapy"}
                        else build_module._arrow_type(str(row["physical_type"]))
                    ),
                )
                for row in rows
                if row["variable"] != "anchored_time_since_icu_admission"
            ]
            fields.extend(
                pa.field(name, build_module._arrow_type(physical_type))
                for name, physical_type in freeze_policy.provenance_fields
            )
            arrays = [
                pa.array(
                    [_value(field.name, str(field.type), hospital)],
                    type=field.type,
                )
                if not field.name.startswith("__v3_")
                else pa.array(
                    [
                        hospital_index
                        if pa.types.is_integer(field.type)
                        else f"source-{hospital_index}"
                    ],
                    type=field.type,
                )
                for field in fields
            ]
            path = hospital_root / f"{table}.parquet"
            pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), path)
            outputs[table] = {
                "sha256": sha256_file(path),
                "row_count": 1,
            }
        hospital_manifests.append({"hospital": hospital, "outputs": outputs})
    candidate_root.mkdir(parents=True, exist_ok=True)
    (candidate_root / "candidate_run_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonization_candidate_run",
                "artifact_version": "0.3",
                "run_id": "20260806T074852Z",
                "status": "candidate_nonpublishable_requires_consolidated_review",
                "publication_ready": False,
                "hospitals": hospital_manifests,
            }
        ),
        encoding="utf-8",
    )

    build_policy_path = tmp_path / "harmonized_build.yaml"
    build_policy_path.write_text(
        "\n".join(
            (
                "harmonized_build_policy_version: '0.1'",
                "status: approved_for_frozen_contract_bound_nonpublishable_build",
                "inputs:",
                "  candidate_run_id: 20260806T074852Z",
                "  candidate_artifact_version: '0.3'",
                "  candidate_status: candidate_nonpublishable_requires_consolidated_review",
                "  frozen_contract_version: '0.1'",
                "  frozen_contract_directory: contracts/harmonized_schema_dictionary/0.1",
                "decision_sources:",
                f"  consolidated_decisions: {project_root / 'asic/config/harmonization/reviewed_consolidated_decisions_0_1.yaml'}",
                "  consolidated_decisions_sha256: 3b9d49d24461e97354851f53937e5335248301099d4a7068f88c162df6e5ceb3",
                f"  categorical_decisions: {project_root / 'asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml'}",
                "  categorical_decisions_sha256: 013b6a01e07b947cb7ed56a1f0b0a24a7ab870b0cfcd0f5a56d34c28f8f9c53c",
                f"  schema_dictionary_freeze: {freeze_policy_path}",
                f"  schema_dictionary_freeze_sha256: {sha256_file(freeze_policy_path)}",
                "approved_hospitals:",
                *(f"  - {hospital}" for hospital in HOSPITALS),
                "expected_rows: {static: 8, dynamic: 8}",
                "approved_transformations:",
                "  apply_complete_categorical_contract: true",
                "  apply_four_hospital_unit_conversions: true",
                "  generate_artificial_anchored_time: true",
                "  artificial_time_source: minutes_since_icu_admission",
                "  artificial_time_target: anchored_time_since_icu_admission",
                "  artificial_time_anchor: '2020-01-01 00:00:00'",
                "  preserve_five_operational_provenance_fields: true",
                "  preserve_row_and_hospital_order: true",
                "streaming: {rows_per_batch: 2, parquet_compression: zstd}",
                "outputs:",
                "  directory_name: harmonized_candidates",
                "  artifact_version: '0.1'",
                "  review_directory_name: harmonized_build",
                "  status: frozen_contract_bound_nonpublishable_requires_build_audit",
                "boundary:",
                "  read_verified_candidate_artifacts: true",
                "  modify_candidate_artifacts: false",
                "  overwrite_outputs: false",
                "  filter_rows: false",
                "  filter_stays: false",
                "  apply_cleaning: false",
                "  apply_derivation: false",
                "  publish: false",
                "  update_release_pointer: false",
                "",
            )
        ),
        encoding="utf-8",
    )
    config = HarmonizedBuildConfig(
        freeze=SimpleNamespace(
            dataset_context="production", data_root=data, reports_root=reports
        ),
        policy_path=build_policy_path,
    )
    return config, freeze_policy


def _write_audit_policy(tmp_path: Path, build_config: HarmonizedBuildConfig) -> Path:
    audit_policy_path = tmp_path / "harmonized_build_audit.yaml"
    audit_policy_path.write_text(
        "\n".join(
            (
                "harmonized_build_audit_policy_version: '0.1'",
                "status: approved_for_complete_read_only_build_audit",
                f"build_policy: {build_config.policy_path}",
                f"build_policy_sha256: {sha256_file(build_config.policy_path)}",
                "audit:",
                "  rows_per_batch: 50000",
                "  verify_input_and_output_hashes: true",
                "  verify_frozen_schema_exactly: true",
                "  verify_all_rows_and_hospital_order: true",
                "  recompute_every_categorical_transformation: true",
                "  recompute_every_hospital_unit_conversion: true",
                "  recompute_every_artificial_timestamp: true",
                "  compare_every_unchanged_value: true",
                "  compare_all_five_provenance_fields: true",
                "  verify_rule_level_counts: true",
                "outputs:",
                "  private_directory_name: harmonized_build_audit",
                "  review_directory_name: harmonized_build_audit",
                "  private_artifact_version: '0.1'",
                "  review_artifact_version: '0.1'",
                "boundary:",
                "  read_candidate_clinical_rows: true",
                "  read_harmonized_clinical_rows: true",
                "  modify_clinical_data: false",
                "  write_reports_only: true",
                "  apply_cleaning: false",
                "  apply_derivation: false",
                "  approve_release_automatically: false",
                "  publish: false",
                "",
            )
        ),
        encoding="utf-8",
    )
    return audit_policy_path


def test_build_streams_frozen_schema_and_applies_only_approved_rules(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    config, freeze_policy = _write_build_fixture(tmp_path, project_root)
    categorical = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )
    test_categorical = replace(
        categorical,
        rules=tuple(
            replace(rule, expected_non_null_count=None, expected_null_count=None)
            for rule in categorical.rules
        ),
    )
    monkeypatch.setattr(
        build_module,
        "load_reviewed_categorical_decisions",
        lambda _: test_categorical,
    )

    result = run_harmonized_build(config, "build001")

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["publication_ready"] is False
    assert manifest["cleaning_applied"] is False
    assert manifest["derivation_applied"] is False
    assert manifest["outputs"]["static"]["row_count"] == 8
    assert manifest["outputs"]["dynamic"]["row_count"] == 8
    dynamic = pq.read_table(result.output_directory / "dynamic.parquet")
    assert dynamic.num_columns == 135
    assert dynamic.column("anchored_time_since_icu_admission").null_count == 0
    assert dynamic.column("hematocrit").to_pylist()[3] == 100.0
    assert dynamic.column("lymph_pct").to_pylist()[0] == 100.0
    assert dynamic.column("etco2").to_pylist()[4] == 1.0 / 7.50062
    assert dynamic.column("ards_diagnosis_app").type == pa.int32()
    assert dynamic.column("ecmo").type == pa.bool_()
    assert pq.read_table(result.output_directory / "static.parquet").num_columns == 28
    assert len(freeze_policy.provenance_fields) == 5


def test_complete_audit_recomputes_every_output_cell(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    config, _ = _write_build_fixture(tmp_path, project_root)
    categorical = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )
    test_categorical = replace(
        categorical,
        rules=tuple(
            replace(rule, expected_non_null_count=None, expected_null_count=None)
            for rule in categorical.rules
        ),
    )
    monkeypatch.setattr(
        build_module,
        "load_reviewed_categorical_decisions",
        lambda _: test_categorical,
    )
    monkeypatch.setattr(
        audit_module,
        "load_reviewed_categorical_decisions",
        lambda _: test_categorical,
    )
    build = run_harmonized_build(config, "build-audit-input")
    audit_policy_path = _write_audit_policy(tmp_path, config)
    audit_config = HarmonizedBuildAuditConfig(
        build=config,
        policy_path=audit_policy_path,
    )

    result = run_harmonized_build_audit(
        audit_config,
        build.run_id,
        "audit001",
    )

    assert result.overall_status == "pending_human_review"
    assert len(result.blocking_findings) == 1
    review = json.loads(result.review_json_path.read_text(encoding="utf-8"))
    assert review["technical_status"] == "pass"
    assert review["metrics"]["compared_static_rows"] == 8
    assert review["metrics"]["compared_dynamic_rows"] == 8
    assert review["metrics"]["compared_output_cell_count"] == (28 + 135) * 8


def test_explicit_promotion_preserves_audited_bytes_and_creates_release(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    config, _ = _write_build_fixture(tmp_path, project_root)
    categorical = load_reviewed_categorical_decisions(
        project_root
        / "asic/config/harmonization/reviewed_categorical_decisions_0_1.yaml"
    )
    test_categorical = replace(
        categorical,
        rules=tuple(
            replace(rule, expected_non_null_count=None, expected_null_count=None)
            for rule in categorical.rules
        ),
    )
    monkeypatch.setattr(
        build_module,
        "load_reviewed_categorical_decisions",
        lambda _: test_categorical,
    )
    monkeypatch.setattr(
        audit_module,
        "load_reviewed_categorical_decisions",
        lambda _: test_categorical,
    )
    run_id = "20260806T111156Z"
    build = run_harmonized_build(config, run_id)
    audit_policy_path = _write_audit_policy(tmp_path, config)
    audit_config = HarmonizedBuildAuditConfig(
        build=config,
        policy_path=audit_policy_path,
    )
    run_harmonized_build_audit(audit_config, run_id, run_id)

    promotion_policy = tmp_path / "promotion.yaml"
    promotion_policy.write_text(
        "\n".join(
            (
                "harmonized_promotion_policy_version: '0.1'",
                "status: explicitly_approved_for_immutable_harmonized_release",
                "approved_input:",
                f"  harmonized_build_run_id: {run_id}",
                f"  harmonized_build_audit_run_id: {run_id}",
                "  frozen_contract_version: '0.1'",
                "  build_artifact_version: '0.1'",
                "  audit_private_artifact_version: '0.1'",
                "  audit_review_artifact_version: '0.1'",
                "immutable_policy:",
                f"  harmonized_build_audit: {audit_policy_path}",
                f"  harmonized_build_audit_sha256: {sha256_file(audit_policy_path)}",
                "human_approval:",
                "  role: data_owner",
                "  recorded_date: '2026-08-06'",
                "  statement: I approve promotion of harmonized candidate 20260806T111156Z under frozen contract 0.1",
                "expected_audit:",
                "  technical_status: pass",
                "  overall_status_before_approval: pending_human_review",
                "  technical_blocking_finding_count: 0",
                "  human_blocking_finding_count_before_approval: 1",
                "  human_blocking_check_resolved_by_this_approval: harmonized_candidate_release_approved",
                "  compared_static_rows: 8",
                "  compared_dynamic_rows: 8",
                f"  compared_output_cells: {(28 + 135) * 8}",
                "release:",
                f"  release_id: {run_id}",
                "  releases_directory: harmonized/releases",
                "  current_release_pointer: harmonized/current_release.json",
                "  artifact_version: '0.1'",
                "  review_directory_name: harmonized_promotion",
                "boundary:",
                "  copy_audited_parquet_bytes_unchanged: true",
                "  preserve_candidate_artifacts: true",
                "  overwrite_existing_release: false",
                "  overwrite_existing_current_pointer: false",
                "  apply_harmonization: false",
                "  apply_cleaning: false",
                "  apply_derivation: false",
                "  filter_rows: false",
                "  filter_stays: false",
                "  authorize_harmonized_layer: true",
                "  authorize_cleaning_input: true",
                "  authorize_external_data_export: false",
                "",
            )
        ),
        encoding="utf-8",
    )
    promotion_config = HarmonizedPromotionConfig(
        audit=audit_config,
        policy_path=promotion_policy,
    )

    result = promote_harmonized_release(promotion_config)

    release_manifest = json.loads(
        result.release_manifest_path.read_text(encoding="utf-8")
    )
    current = json.loads(
        result.current_release_pointer_path.read_text(encoding="utf-8")
    )
    assert release_manifest["publication_ready"] is True
    assert release_manifest["cleaning_input_approved"] is True
    assert release_manifest["cleaning_applied"] is False
    assert release_manifest["derivation_applied"] is False
    assert current["release_id"] == run_id
    assert current["release_manifest_sha256"] == sha256_file(
        result.release_manifest_path
    )
    for table in ("static", "dynamic"):
        assert sha256_file(result.release_directory / f"{table}.parquet") == sha256_file(
            build.output_directory / f"{table}.parquet"
        )
    assert build.output_directory.is_dir()
    assert not (tmp_path / "data/cleaned").exists()
    assert not (tmp_path / "data/derived").exists()

    cleaning_raw = yaml.safe_load(
        (
            project_root / "asic/config/cleaning/cleaning_policy.yaml"
        ).read_text(encoding="utf-8")
    )
    cleaning_raw["input"]["harmonized_promotion_policy"] = str(promotion_policy)
    cleaning_raw["input"]["harmonized_promotion_policy_sha256"] = sha256_file(
        promotion_policy
    )
    cleaning_raw["native_v3_quality_registry"]["path"] = str(
        project_root
        / "asic/config/harmonization/cross_hospital_quality_audit.yaml"
    )
    cleaning_raw["expected_rows"] = {"static": 8, "dynamic": 8}
    cleaning_policy_path = tmp_path / "cleaning.yaml"
    cleaning_policy_path.write_text(
        yaml.safe_dump(cleaning_raw, sort_keys=False), encoding="utf-8"
    )
    cleaning_config = CleaningConfig(
        promotion=promotion_config,
        policy_path=cleaning_policy_path,
    )

    cleaned = run_cleaning_candidate_build(cleaning_config, "20260806T114234Z")
    cleaning_audit = run_cleaning_candidate_audit(
        cleaning_config,
        cleaned.run_id,
        "20260806T114234Z",
    )

    assert cleaning_audit.overall_status == "pending_human_review"
    cleaning_review = json.loads(
        cleaning_audit.review_json_path.read_text(encoding="utf-8")
    )
    assert len(cleaning_review["applied_rule_summaries"]) == 49
    assert len(cleaning_review["deferred_rule_summaries"]) == 12
    assert "Applied scalar-rule accounting" in (
        cleaning_audit.review_markdown_path.read_text(encoding="utf-8")
    )
    cleaned_manifest = json.loads(cleaned.manifest_path.read_text(encoding="utf-8"))
    assert cleaned_manifest["cleaning_applied"] is True
    assert cleaned_manifest["publication_ready"] is False
    cleaned_dynamic = pq.read_table(cleaned.candidate_directory / "dynamic.parquet")
    assert cleaned_dynamic.num_rows == 8
    assert cleaned_dynamic["vt_per_kg_ideal_body_weight"].to_pylist()[5] is None
    assert cleaned_dynamic["feo2"].null_count == 8
    assert result.release_directory.is_dir()

    cleaned_promotion_policy = tmp_path / "cleaned_promotion.yaml"
    expected_metrics = cleaning_review["metrics"]
    cleaned_promotion_policy.write_text(
        yaml.safe_dump(
            {
                "cleaned_promotion_policy_version": "0.1",
                "status": "explicitly_approved_for_immutable_cleaned_release",
                "approved_input": {
                    "cleaning_candidate_run_id": cleaned.run_id,
                    "cleaning_audit_run_id": cleaning_audit.run_id,
                    "harmonized_release_id": run_id,
                    "harmonized_contract_version": "0.1",
                    "candidate_artifact_version": "0.1",
                    "audit_private_artifact_version": "0.1",
                    "audit_review_artifact_version": "0.1",
                },
                "immutable_policy": {
                    "cleaning_policy": str(cleaning_policy_path),
                    "cleaning_policy_sha256": sha256_file(cleaning_policy_path),
                },
                "human_approval": {
                    "role": "data_owner",
                    "recorded_date": "2026-08-06",
                    "statement": "i approve",
                    "context": (
                        "Promotion of cleaned candidate 20260806T114234Z under "
                        "cleaning policy 0.1, as proposed in the immediately "
                        "preceding cleaned-candidate audit review."
                    ),
                },
                "expected_audit": {
                    "technical_status": "pass",
                    "overall_status_before_approval": "pending_human_review",
                    "technical_blocking_finding_count": 0,
                    "human_blocking_finding_count_before_approval": 1,
                    "human_blocking_check_resolved_by_this_approval": (
                        "cleaned_candidate_release_approved"
                    ),
                    **expected_metrics,
                },
                "release": {
                    "release_id": cleaned.run_id,
                    "releases_directory": "cleaned/releases",
                    "current_release_pointer": "cleaned/current_release.json",
                    "artifact_version": "0.1",
                    "review_directory_name": "cleaned_promotion",
                },
                "boundary": {
                    "copy_audited_parquet_bytes_unchanged": True,
                    "preserve_candidate_artifacts": True,
                    "overwrite_existing_release": False,
                    "overwrite_existing_current_pointer": False,
                    "rerun_cleaning": False,
                    "apply_harmonization": False,
                    "apply_derivation": False,
                    "filter_rows": False,
                    "filter_stays": False,
                    "drop_columns": False,
                    "authorize_cleaned_layer": True,
                    "authorize_derived_input": True,
                    "authorize_external_data_export": False,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    cleaned_promotion = promote_cleaned_release(
        CleanedPromotionConfig(
            cleaning=cleaning_config,
            policy_path=cleaned_promotion_policy,
        )
    )
    cleaned_release_manifest = json.loads(
        cleaned_promotion.release_manifest_path.read_text(encoding="utf-8")
    )
    cleaned_current = json.loads(
        cleaned_promotion.current_release_pointer_path.read_text(encoding="utf-8")
    )
    assert cleaned_release_manifest["cleaned_layer_ready"] is True
    assert cleaned_release_manifest["derived_input_approved"] is True
    assert cleaned_release_manifest["cleaning_rerun_during_promotion"] is False
    assert cleaned_release_manifest["derivation_applied"] is False
    assert cleaned_release_manifest["external_data_export_authorized"] is False
    assert cleaned_current["release_id"] == cleaned.run_id
    assert cleaned_current["release_manifest_sha256"] == sha256_file(
        cleaned_promotion.release_manifest_path
    )
    for table in ("static", "dynamic"):
        assert sha256_file(
            cleaned_promotion.release_directory / f"{table}.parquet"
        ) == sha256_file(cleaned.candidate_directory / f"{table}.parquet")
    assert cleaned.candidate_directory.is_dir()
    assert not (tmp_path / "data/derived").exists()
