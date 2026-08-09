from __future__ import annotations

from pathlib import Path
import subprocess


def test_time_blocking_contract_evidence_job_is_read_only_and_review_gated(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_time_blocking_8h_contract_evidence_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=8G" in text
    assert "ASIC_TIME_BLOCKING_EVIDENCE_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-time-blocking-8h-contract-evidence" in text
    assert "expected technically passing evidence pending human review" in text
    assert "blocked_rows_written=false" in text
    assert "time_blocking_candidate_created=false" in text
    assert "time_blocking_release_created=false" in text
    assert "current_release_pointer_modified=false" in text
    assert "carry_forward_or_imputation_applied=false" in text
    assert "external_data_export_authorized=false" in text


def test_time_blocking_candidate_and_audit_workflow_is_exact_run_gated(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_time_blocking_8h_candidate_and_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=16G" in text
    assert "#SBATCH --time=24:00:00" in text
    assert "ASIC_TIME_BLOCKING_8H_WORKER=1" in text
    assert "exec sbatch" in text
    assert "reviewed_8h_production_execution.yaml" in text
    assert "set the exact human-approved TIME_BLOCKING_8H_CANDIDATE_RUN_ID" in text
    assert "set the exact human-approved TIME_BLOCKING_8H_AUDIT_RUN_ID" in text
    assert "build-time-blocking-candidate" in text
    assert "audit-time-blocking-candidate" in text
    assert "expected technically passing independent audit pending human promotion approval" in text
    assert "core_pointer_sha256_before" in text
    assert "blocked_pointer_state_before" in text
    assert "independent_every_cell_audit_completed=true" in text
    assert "static_table_copied=false" in text
    assert "medication_dose_totals_emitted=false" in text
    assert "release_created=false" in text
    assert "current_release_pointer_modified=false" in text
    assert "external_data_export_authorized=false" in text
    assert "promote-time-blocking" not in text


def test_time_blocking_promotion_is_exact_byte_preserving_and_pointer_scoped(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_promote_time_blocking_8h_release_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --time=00:30:00" in text
    assert "ASIC_TIME_BLOCKING_8H_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "20260808T130534Z" in text
    assert "20260808T074305Z" in text
    assert "reviewed_8h_promotion_20260808T130534Z.yaml" in text
    assert "promote-time-blocking-release" in text
    assert "core_pointer_before" in text
    assert "only_time_blocking_8h_current_pointer_created=true" in text
    assert "static_table_copied=false" in text
    assert "time_blocking_rerun_during_promotion=false" in text
    assert "external_data_export_authorized=false" in text


def test_unit_schema_dictionary_0_2_freeze_runs_on_frontend_and_is_metadata_only(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/freeze_unit_schema_dictionary_0_2_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "freeze-unit-schema-dictionary" in text
    assert "20260807T101321Z" in text
    assert "harmonized_schema_dictionary/0.1" in text
    assert "harmonized_schema_dictionary/0.2" in text
    assert "run this metadata-only freeze with bash from a login frontend" in text
    assert "sbatch" not in text
    assert "build-harmonized-candidate" not in text
    assert "build-cleaned-candidate" not in text
    assert "build-core-derived-candidate" not in text


def test_harmonized_0_2_job_is_modest_complete_and_review_gated(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_harmonized_0_2_build_and_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "ASIC_HARMONIZED_0_2_WORKER=1" in text
    assert "exec sbatch" in text
    assert "build-harmonized-0-2-candidate" in text
    assert "audit-harmonized-0-2-candidate" in text
    assert "expected technically passing audit pending human approval" in text
    assert "harmonized_0_2_build_and_audit_completed=true" in text
    assert "cleaned/releases/${STATIC_WEIGHT_CLEANED_RELEASE_ID}/dynamic.parquet" not in text
    assert "publication_ready=false" in text


def test_harmonized_0_2_promotion_is_low_resource_and_preserves_prior_release(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_promote_harmonized_0_2_release_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "20260807T112402Z" in text
    assert "20260806T111156Z" in text
    assert "ASIC_HARMONIZED_0_2_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "promote-harmonized-0-2-release" in text
    assert "previous_harmonized_release_preserved=true" in text
    assert "cleaning_applied=false" in text
    assert "derivation_applied=false" in text


def test_all_hospital_ingestion_job_is_complete_and_fail_closed(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_lossless_ingestion_all_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    expected_hospitals = (
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    )
    for hospital in expected_hospitals:
        assert text.count(f"    {hospital}\n") == 1
    assert "ASIC_INGESTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "existing hospital output blocks all-hospital ingestion" in text
    assert "incomplete ingestion staging directories require review" in text
    assert "ingest-raw-hospital" in text
    assert "batch_ingestion_status=pass" in text
    assert "publication_ready=false" in text


def test_ingestion_audit_job_is_read_only_and_review_gated(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_ingestion_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_INGESTION_AUDIT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-ingested" in text
    assert "INVENTORY_RUN_ID" in text
    assert "ingestion_audit_completed_with_human_review_blocker=true" in text
    assert "ingest-raw-hospital" not in text


def test_schema_token_job_is_read_only_and_requires_reviewed_inputs(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_schema_token_inventory_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_SCHEMA_TOKEN_WORKER=1" in text
    assert "exec sbatch" in text
    assert "inventory-schema-tokens" in text
    assert "INVENTORY_RUN_ID" in text
    assert "INGESTION_AUDIT_RUN_ID" in text
    assert "schema_token_inventory_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text


def test_harmonization_registry_review_job_reads_only_review_evidence(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_harmonization_registry_review_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_HARMONIZATION_REVIEW_WORKER=1" in text
    assert "exec sbatch" in text
    assert "review-harmonization-registry" in text
    assert "SCHEMA_TOKEN_RUN_ID" in text
    assert "harmonization_registry_review_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text
    assert "/asic/data/production" not in text


def test_static_contract_job_is_read_only_and_review_gated(project_root: Path) -> None:
    script = project_root / "asic/slurm/run_static_contract_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_STATIC_CONTRACT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-static-contract" in text
    assert "REGISTRY_REVIEW_RUN_ID" in text
    assert "COMPOSITE_AUDIT_RUN_ID" in text
    assert "static_contract_audit_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text
    assert "harmonize" not in text


def test_static_completion_review_job_reads_only_prior_evidence(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_static_completion_review_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_STATIC_COMPLETION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "review-remaining-static-contract" in text
    assert "STATIC_CONTRACT_AUDIT_RUN_ID" in text
    assert "static_completion_review_completed_with_human_review_findings=true" in text
    assert "asic/data/production" not in text
    assert "ingest-raw-hospital" not in text


def test_icd10_notation_job_reads_only_static_icd10_evidence(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_icd10_notation_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_ICD10_NOTATION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-icd10-notation" in text
    assert "STATIC_CONTRACT_AUDIT_RUN_ID" in text
    assert "icd10_notation_audit_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text
    assert "harmonized" not in text


def test_icd10_component_job_is_read_only_and_review_gated(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_icd10_component_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_ICD10_COMPONENT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-icd10-components" in text
    assert "ICD10_NOTATION_AUDIT_RUN_ID" in text
    assert "icd10_component_audit_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text
    assert "asic/data/production/harmonized" not in text


def test_icd10_component_detail_job_is_read_only_and_review_gated(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_icd10_component_detail_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_ICD10_COMPONENT_DETAIL_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-icd10-component-details" in text
    assert "ICD10_COMPONENT_AUDIT_RUN_ID" in text
    assert "icd10_component_detail_audit_completed_with_human_review_findings=true" in text
    assert "ingest-raw-hospital" not in text
    assert "asic/data/production/harmonized" not in text


def test_static_registry_completion_job_reads_only_review_evidence(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_static_registry_completion_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_STATIC_REGISTRY_COMPLETION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-static-registry-completion" in text
    assert "REGISTRY_REVIEW_RUN_ID" in text
    assert "static_registry_completion_audit_completed_with_human_review_findings=true" in text
    assert "asic/data/production" not in text
    assert "ingest-raw-hospital" not in text


def test_harmonized_promotion_job_is_exactly_scoped_and_nonoverwriting(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_promote_harmonized_release_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_HARMONIZED_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "promote-harmonized-release" in text
    assert "20260806T111156Z" in text
    assert "promotion will not overwrite it" in text
    assert "harmonized_layer_ready=true" in text
    assert "cleaning_input_approved=true" in text
    assert "external_data_export_authorized=false" in text
    assert "clean" not in text.replace("cleaning_input_approved", "").replace(
        "cleaning_applied", ""
    )


def test_cleaning_job_builds_and_audits_without_publishing_or_deriving(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_cleaning_build_and_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_CLEANING_WORKER=1" in text
    assert "exec sbatch" in text
    assert "build-cleaned-candidate" in text
    assert "audit-cleaned-candidate" in text
    assert "20260806T111156Z" in text
    assert "expected technically passing cleaning audit pending human approval" in text
    assert "harmonized_data_modified=false" in text
    assert "rows_or_stays_filtered=false" in text
    assert "columns_dropped=false" in text
    assert "derivation_applied=false" in text
    assert "publication_ready=false" in text


def test_cleaned_promotion_job_is_exactly_scoped_and_nonoverwriting(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_promote_cleaned_release_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_CLEANED_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "promote-cleaned-release" in text
    assert "20260806T114234Z" in text
    assert "promotion will not overwrite it" in text
    assert "cleaned_layer_ready=true" in text
    assert "derived_input_approved=true" in text
    assert "cleaning_rerun_during_promotion=false" in text
    assert "derivation_applied=false" in text
    assert "external_data_export_authorized=false" in text


def test_derivation_contract_review_job_reads_cleaned_release_and_writes_reports_only(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_derivation_contract_review_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "ASIC_DERIVATION_REVIEW_WORKER=1" in text
    assert "exec sbatch" in text
    assert "review-derived-contract" in text
    assert "20260806T114234Z" in text
    assert "expected technically passing derivation review pending human approval" in text
    assert "cleaned_release_modified=false" in text
    assert "clinical_data_written=false" in text
    assert "cohort_filtering_applied=false" in text
    assert "time_blocking_applied=false" in text
    assert "publication_ready=false" in text


def test_driving_pressure_semantics_job_is_low_resource_and_read_only(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_driving_pressure_semantics_review_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --time=00:15:00" in text
    assert "ASIC_DRIVING_PRESSURE_REVIEW_WORKER=1" in text
    assert "exec sbatch" in text
    assert "review-driving-pressure-semantics" in text
    assert "20260806T114234Z" in text
    assert "expected technically passing driving-pressure review pending human approval" in text
    assert "cleaned_release_modified=false" in text
    assert "clinical_data_written=false" in text
    assert "derived_formula_activated=false" in text
    assert "values_masked=false" in text
    assert "publication_ready=false" in text


def test_core_derived_build_and_audit_job_is_bounded_and_nonpublishing(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_core_derived_build_and_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --cpus-per-task=2" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --time=00:30:00" in text
    assert "ASIC_CORE_DERIVED_WORKER=1" in text
    assert "exec sbatch" in text
    assert "build-core-derived-candidate" in text
    assert "audit-core-derived-candidate" in text
    assert "20260806T114234Z" in text
    assert "20260806T161232Z" in text
    assert "expected technically passing core-derived audit pending human approval" in text
    assert "cleaned_release_modified=false" in text
    assert "every_input_column_preserved_exactly=true" in text
    assert "every_derived_value_recomputed_exactly=true" in text
    assert "rows_or_stays_filtered=false" in text
    assert "cohort_generated=false" in text
    assert "time_blocking_applied=false" in text
    assert "publication_ready=false" in text


def test_core_derived_0_2_job_preserves_formulas_and_uses_cleaned_0_2(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_core_derived_0_2_build_and_audit_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --time=00:30:00" in text
    assert "ASIC_CORE_DERIVED_0_2_WORKER=1" in text
    assert "exec sbatch" in text
    assert "build-core-derived-0-2-candidate" in text
    assert "audit-core-derived-0-2-candidate" in text
    assert "20260807T154100Z" in text
    assert "clinical_formula_changes_from_0_1=0" in text
    assert "every_cleaned_0_2_input_column_preserved_exactly=true" in text
    assert "cleaned_release_modified=false" in text
    assert "cohort_generated=false" in text
    assert "time_blocking_applied=false" in text
    assert "publication_ready=false" in text


def test_core_derived_promotion_job_is_exactly_scoped_and_nonoverwriting(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_promote_core_derived_release_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --time=00:20:00" in text
    assert "ASIC_CORE_DERIVED_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "promote-core-derived-release" in text
    assert "20260806T170134Z" in text
    assert "promotion will not overwrite it" in text
    assert "core_derived_layer_ready=true" in text
    assert "analysis_input_approved=true" in text
    assert "derivation_rerun_during_promotion=false" in text
    assert "cleaning_rerun_during_promotion=false" in text
    assert "cohort_generated=false" in text
    assert "time_blocking_applied=false" in text
    assert "external_data_export_authorized=false" in text


def test_core_derived_0_2_promotion_preserves_previous_release_and_pointer(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_promote_core_derived_0_2_release_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --time=00:20:00" in text
    assert "ASIC_CORE_DERIVED_0_2_PROMOTION_WORKER=1" in text
    assert "exec sbatch" in text
    assert "promote-core-derived-0-2-release" in text
    assert "20260808T074305Z" in text
    assert "20260806T170134Z" in text
    assert "nothing will be overwritten" in text
    assert "previous_derived_release_preserved=true" in text
    assert "previous_current_pointer_bytes_preserved_exactly=true" in text
    assert "core_derived_layer_ready=true" in text
    assert "analysis_input_approved=true" in text
    assert "derivation_rerun_during_promotion=false" in text
    assert "cohort_generated=false" in text
    assert "time_blocking_applied=false" in text
    assert "external_data_export_authorized=false" in text


def test_unit_resolution_audit_job_is_low_resource_and_reports_only(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_unit_resolution_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=4G" in text
    assert "#SBATCH --time=00:30:00" in text
    assert "ASIC_UNIT_RESOLUTION_AUDIT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-unresolved-units" in text
    assert "20260806T114234Z" in text
    assert "expected technically passing unit-resolution audit pending human approval" in text
    assert "clinical_data_written=false" in text
    assert "harmonized_release_modified=false" in text
    assert "cleaned_release_modified=false" in text
    assert "derived_release_modified=false" in text
    assert "values_converted=false" in text
    assert "values_masked=false" in text
    assert "publication_ready=false" in text


def test_unit_decision_audit_job_is_low_resource_and_reports_only(
    project_root: Path,
) -> None:
    script = project_root / "asic/slurm/run_unit_decision_audit_production.sh"
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --time=00:20:00" in text
    assert "ASIC_UNIT_DECISION_AUDIT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-candidate-unit-decisions" in text
    assert "20260806T114234Z" in text
    assert "expected technically passing unit-decision audit pending human approval" in text
    assert "clinical_data_written=false" in text
    assert "harmonized_release_modified=false" in text
    assert "cleaned_release_modified=false" in text
    assert "derived_release_modified=false" in text
    assert "values_converted=false" in text
    assert "values_masked=false" in text
    assert "contract_0_2_activated=false" in text
    assert "publication_ready=false" in text


def test_unit_schema_amendment_job_is_tiny_and_reports_only(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_unit_schema_amendment_review_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=1G" in text
    assert "#SBATCH --time=00:05:00" in text
    assert "ASIC_UNIT_SCHEMA_AMENDMENT_WORKER=1" in text
    assert "exec sbatch" in text
    assert "review-unit-schema-amendment" in text
    assert "20260807T083117Z" in text
    assert "expected technically passing schema amendment pending human approval" in text
    assert "clinical_rows_read=0" in text
    assert "clinical_data_written=false" in text
    assert "schema_frozen=false" in text
    assert "dictionary_frozen=false" in text
    assert "existing_releases_modified=false" in text
    assert "cleaning_ranges_activated=false" in text
    assert "publication_ready=false" in text


def test_medication_semantics_and_schema_review_job_is_bounded_and_read_only(
    project_root: Path,
) -> None:
    script = (
        project_root
        / "asic/slurm/run_medication_semantics_and_schema_review_production.sh"
    )
    subprocess.run(["bash", "-n", str(script)], check=True)
    text = script.read_text(encoding="utf-8")

    assert "#SBATCH --partition=c23ms" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --mem=2G" in text
    assert "#SBATCH --time=00:15:00" in text
    assert "ASIC_MEDICATION_SEMANTICS_WORKER=1" in text
    assert "exec sbatch" in text
    assert "audit-medication-value-semantics" in text
    assert "review-unit-schema-amendment" in text
    assert "null_to_zero_imputation_applied=false" in text
    assert "zero_to_null_conversion_applied=false" in text
    assert "carry_forward_applied=false" in text
    assert "clinical_data_written=false" in text
    assert "existing_releases_modified=false" in text
    assert "schema_frozen=false" in text
    assert "dictionary_frozen=false" in text
    assert "publication_ready=false" in text
