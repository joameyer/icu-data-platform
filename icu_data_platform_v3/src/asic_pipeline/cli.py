from __future__ import annotations

import argparse
from pathlib import Path
import sys

from asic_pipeline.config import load_inventory_config
from asic_pipeline.cleaning import (
    load_cleaning_0_2_audit_config,
    load_cleaning_0_2_build_config,
    load_cleaned_0_2_promotion_config,
    load_cleaning_0_2_policy_review_config,
    load_cleaning_range_evidence_0_2_config,
    load_cleaning_config,
    load_cleaned_promotion_config,
    promote_cleaned_release,
    promote_cleaned_0_2_release,
    run_cleaning_candidate_audit,
    run_cleaning_candidate_build,
    run_cleaning_0_2_policy_review,
    run_cleaning_range_evidence_0_2,
    run_cleaning_0_2_audit,
    run_cleaning_0_2_build,
)
from asic_pipeline.errors import ASICPipelineError
from asic_pipeline.derivation import (
    load_core_derived_0_2_config,
    load_core_derived_0_2_promotion_config,
    load_core_derived_config,
    load_core_derived_promotion_config,
    load_driving_pressure_semantics_config,
    load_derivation_contract_review_config,
    run_core_derived_candidate_audit,
    run_core_derived_candidate_build,
    promote_core_derived_release,
    promote_core_derived_0_2_release,
    run_driving_pressure_semantics_review,
    run_derivation_contract_review,
)
from asic_pipeline.harmonization import (
    build_composite_source_audit,
    build_harmonization_registry_review,
    build_icd10_component_audit,
    build_icd10_component_detail_audit,
    build_icd10_notation_audit,
    build_static_contract_audit,
    build_static_completion_review,
    build_static_registry_completion_audit,
    load_composite_source_audit_config,
    load_categorical_review_config,
    load_schema_dictionary_review_config,
    load_schema_dictionary_freeze_config,
    load_harmonized_build_config,
    load_harmonized_build_audit_config,
    load_harmonized_promotion_config,
    load_consolidated_audit_config,
    load_harmonization_review_config,
    load_harmonization_dry_run_config,
    load_icd10_component_audit_config,
    load_icd10_component_detail_audit_config,
    load_icd10_notation_audit_config,
    load_static_contract_audit_config,
    load_static_completion_review_config,
    load_static_registry_completion_audit_config,
    run_harmonization_dry_run,
    run_consolidated_harmonization_audit,
    run_categorical_contract_review,
    run_schema_dictionary_review,
    freeze_schema_dictionary,
    run_harmonized_build,
    run_harmonized_build_audit,
    promote_harmonized_release,
    write_composite_source_audit_bundle,
    write_harmonization_registry_review_bundle,
    write_icd10_component_audit_bundle,
    write_icd10_component_detail_audit_bundle,
    write_icd10_notation_audit_bundle,
    write_static_contract_audit_bundle,
    write_static_completion_review_bundle,
    write_static_registry_completion_audit_bundle,
)
from asic_pipeline.ingestion import (
    build_ingestion_audit,
    ingest_hospital,
    load_ingestion_config,
    write_ingestion_audit_bundle,
)
from asic_pipeline.inventory import (
    build_raw_anomaly_audit,
    build_raw_inventory,
    write_inventory_report_bundle,
    write_raw_anomaly_audit_bundle,
)
from asic_pipeline.schema_tokens import (
    build_schema_token_inventory,
    load_schema_token_config,
    write_schema_token_inventory_bundle,
)
from asic_pipeline.time_blocking import (
    load_time_blocking_build_config,
    load_time_blocking_evidence_config,
    load_time_blocking_promotion_config,
    promote_time_blocking_release,
    run_time_blocking_candidate_audit,
    run_time_blocking_candidate_build,
    run_time_blocking_contract_evidence,
)
from asic_pipeline.unit_resolution import (
    freeze_unit_schema_dictionary,
    load_harmonized_0_2_audit_config,
    load_harmonized_0_2_build_config,
    load_harmonized_0_2_promotion_config,
    load_medication_semantics_audit_config,
    load_unit_schema_dictionary_freeze_config,
    load_unit_decision_audit_config,
    load_unit_resolution_audit_config,
    run_unit_decision_audit,
    load_unit_schema_amendment_config,
    run_unit_schema_amendment_review,
    run_unit_resolution_audit,
    run_medication_semantics_audit,
    run_harmonized_0_2_audit,
    run_harmonized_0_2_build,
    promote_harmonized_0_2_release,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="asic-pipeline",
        description="ASIC v3 raw-input pipeline",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser(
        "inventory-raw",
        help="Build read-only raw-file inventory evidence",
    )
    inventory.add_argument("--config", type=Path, required=True)
    inventory.add_argument(
        "--run-id",
        help="Optional immutable report run ID; defaults to a UTC timestamp",
    )
    anomalies = commands.add_parser(
        "audit-raw-anomalies",
        help="Audit reviewed duplicate-column and ZIP anomalies read-only",
    )
    anomalies.add_argument("--config", type=Path, required=True)
    anomalies.add_argument("--inventory-run-id", required=True)
    anomalies.add_argument(
        "--run-id",
        help="Optional immutable anomaly-audit run ID; defaults to a UTC timestamp",
    )
    ingestion = commands.add_parser(
        "ingest-raw-hospital",
        help="Losslessly ingest one approved hospital from immutable inventory evidence",
    )
    ingestion.add_argument("--config", type=Path, required=True)
    ingestion.add_argument("--inventory-run-id", required=True)
    ingestion.add_argument("--hospital", required=True, help="Canonical asic_UKNN ID")
    ingestion_audit = commands.add_parser(
        "audit-ingested",
        help="Audit all per-hospital lossless-ingestion artifacts read-only",
    )
    ingestion_audit.add_argument("--config", type=Path, required=True)
    ingestion_audit.add_argument("--inventory-run-id", required=True)
    ingestion_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    schema_tokens = commands.add_parser(
        "inventory-schema-tokens",
        help="Inventory raw schemas and parsing-token candidates read-only",
    )
    schema_tokens.add_argument("--config", type=Path, required=True)
    schema_tokens.add_argument("--inventory-run-id", required=True)
    schema_tokens.add_argument("--ingestion-audit-run-id", required=True)
    schema_tokens.add_argument(
        "--run-id",
        help="Optional immutable inventory run ID; defaults to a UTC timestamp",
    )
    harmonization_review = commands.add_parser(
        "review-harmonization-registry",
        help="Build a read-only candidate harmonization-registry review bundle",
    )
    harmonization_review.add_argument("--config", type=Path, required=True)
    harmonization_review.add_argument("--schema-token-run-id", required=True)
    harmonization_review.add_argument(
        "--run-id",
        help="Optional immutable review run ID; defaults to a UTC timestamp",
    )
    composite_audit = commands.add_parser(
        "audit-reviewed-composite-source",
        help="Audit the reviewed fixed-position source list without harmonizing data",
    )
    composite_audit.add_argument("--config", type=Path, required=True)
    composite_audit.add_argument("--registry-review-run-id", required=True)
    composite_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    static_contract = commands.add_parser(
        "audit-static-contract",
        help="Build read-only evidence for the first static harmonization batch",
    )
    static_contract.add_argument("--config", type=Path, required=True)
    static_contract.add_argument("--registry-review-run-id", required=True)
    static_contract.add_argument("--composite-audit-run-id", required=True)
    static_contract.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    static_completion = commands.add_parser(
        "review-remaining-static-contract",
        help="Re-aggregate remaining static evidence for human contract review",
    )
    static_completion.add_argument("--config", type=Path, required=True)
    static_completion.add_argument("--static-contract-audit-run-id", required=True)
    static_completion.add_argument(
        "--run-id",
        help="Optional immutable review run ID; defaults to a UTC timestamp",
    )
    icd10_audit = commands.add_parser(
        "audit-icd10-notation",
        help="Audit static ICD-10 source notation without parsing or transforming it",
    )
    icd10_audit.add_argument("--config", type=Path, required=True)
    icd10_audit.add_argument("--static-contract-audit-run-id", required=True)
    icd10_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    icd10_components = commands.add_parser(
        "audit-icd10-components",
        help="Audit the approved comma-list candidate without creating parsed data",
    )
    icd10_components.add_argument("--config", type=Path, required=True)
    icd10_components.add_argument("--icd10-notation-audit-run-id", required=True)
    icd10_components.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    icd10_component_details = commands.add_parser(
        "audit-icd10-component-details",
        help="Audit ICD-10 empty positions and duplicate structure without transforming data",
    )
    icd10_component_details.add_argument("--config", type=Path, required=True)
    icd10_component_details.add_argument("--icd10-component-audit-run-id", required=True)
    icd10_component_details.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    static_registry_completion = commands.add_parser(
        "audit-static-registry-completion",
        help="Account for every static source occurrence and draft the union contract",
    )
    static_registry_completion.add_argument("--config", type=Path, required=True)
    static_registry_completion.add_argument("--registry-review-run-id", required=True)
    static_registry_completion.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    harmonization_dry_run = commands.add_parser(
        "harmonize-candidate-dry-run",
        help="Stream all hospitals into an isolated non-publishable harmonization candidate",
    )
    harmonization_dry_run.add_argument("--config", type=Path, required=True)
    harmonization_dry_run.add_argument("--registry-review-run-id", required=True)
    harmonization_dry_run.add_argument("--ingestion-audit-run-id", required=True)
    harmonization_dry_run.add_argument(
        "--run-id",
        help="Optional immutable candidate run ID; defaults to a UTC timestamp",
    )
    consolidated_audit = commands.add_parser(
        "audit-harmonization-candidate",
        help="Run one complete read-only unit, v3-QC, integrity, and optional old-version comparison audit",
    )
    consolidated_audit.add_argument("--config", type=Path, required=True)
    consolidated_audit.add_argument("--harmonization-run-id", required=True)
    consolidated_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    categorical_review = commands.add_parser(
        "review-categorical-contract",
        help="Review immutable categorical domains without rescanning candidate data",
    )
    categorical_review.add_argument("--config", type=Path, required=True)
    categorical_review.add_argument("--consolidated-audit-run-id", required=True)
    categorical_review.add_argument(
        "--run-id",
        help="Optional immutable review run ID; defaults to a UTC timestamp",
    )
    schema_dictionary_review = commands.add_parser(
        "review-schema-dictionary",
        help="Build the complete ordered-schema and variable-dictionary freeze review",
    )
    schema_dictionary_review.add_argument("--config", type=Path, required=True)
    schema_dictionary_review.add_argument(
        "--run-id",
        help="Optional immutable review run ID; defaults to a UTC timestamp",
    )
    schema_dictionary_freeze = commands.add_parser(
        "freeze-schema-dictionary",
        help="Freeze the explicitly approved ordered schema and variable dictionary",
    )
    schema_dictionary_freeze.add_argument("--config", type=Path, required=True)
    harmonized_build = commands.add_parser(
        "build-harmonized-candidate",
        help="Stream the verified candidate into the frozen harmonized contract",
    )
    harmonized_build.add_argument("--config", type=Path, required=True)
    harmonized_build.add_argument(
        "--run-id",
        help="Optional immutable build run ID; defaults to a UTC timestamp",
    )
    harmonized_build_audit = commands.add_parser(
        "audit-harmonized-build",
        help="Recompute and compare every frozen harmonized output value read-only",
    )
    harmonized_build_audit.add_argument("--config", type=Path, required=True)
    harmonized_build_audit.add_argument(
        "--harmonized-build-run-id", required=True
    )
    harmonized_build_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to a UTC timestamp",
    )
    harmonized_promotion = commands.add_parser(
        "promote-harmonized-release",
        help="Promote the explicitly approved audited candidate immutably",
    )
    harmonized_promotion.add_argument("--config", type=Path, required=True)
    cleaning_build = commands.add_parser(
        "build-cleaned-candidate",
        help="Stream the released harmonized layer through the reviewed cleaning policy",
    )
    cleaning_build.add_argument("--config", type=Path, required=True)
    cleaning_build.add_argument(
        "--run-id",
        help="Optional immutable cleaning run ID; defaults to a UTC timestamp",
    )
    cleaning_audit = commands.add_parser(
        "audit-cleaned-candidate",
        help="Recompute and compare every cleaned candidate output value",
    )
    cleaning_audit.add_argument("--config", type=Path, required=True)
    cleaning_audit.add_argument("--cleaning-run-id", required=True)
    cleaning_audit.add_argument(
        "--run-id",
        help="Optional immutable cleaning audit run ID; defaults to a UTC timestamp",
    )
    cleaned_promotion = commands.add_parser(
        "promote-cleaned-release",
        help="Promote the explicitly approved audited cleaned candidate immutably",
    )
    cleaned_promotion.add_argument("--config", type=Path, required=True)
    derivation_review = commands.add_parser(
        "review-derived-contract",
        help="Audit and review all proposed core derived recipes in one pass",
    )
    derivation_review.add_argument("--config", type=Path, required=True)
    derivation_review.add_argument(
        "--run-id",
        help="Optional immutable derivation-review run ID; defaults to UTC",
    )
    driving_pressure_review = commands.add_parser(
        "review-driving-pressure-semantics",
        help="Compare hospital-specific driving-pressure interpretations read-only",
    )
    driving_pressure_review.add_argument("--config", type=Path, required=True)
    driving_pressure_review.add_argument(
        "--run-id",
        help="Optional immutable semantics-review run ID; defaults to UTC",
    )
    derived_build = commands.add_parser(
        "build-core-derived-candidate",
        help="Build the approved run-scoped non-publishable core-derived candidate",
    )
    derived_build.add_argument("--config", type=Path, required=True)
    derived_build.add_argument(
        "--run-id",
        help="Optional immutable derived candidate run ID; defaults to UTC",
    )
    derived_audit = commands.add_parser(
        "audit-core-derived-candidate",
        help="Independently recompute and audit every core-derived output cell",
    )
    derived_audit.add_argument("--config", type=Path, required=True)
    derived_audit.add_argument("--derived-run-id", required=True)
    derived_audit.add_argument(
        "--run-id",
        help="Optional immutable derived audit run ID; defaults to UTC",
    )
    derived_0_2_build = commands.add_parser(
        "build-core-derived-0-2-candidate",
        help="Build the cleaned-0.2-bound non-publishable derived candidate",
    )
    derived_0_2_build.add_argument("--config", type=Path, required=True)
    derived_0_2_build.add_argument(
        "--run-id",
        help="Optional immutable derived 0.2 candidate run ID; defaults to UTC",
    )
    derived_0_2_audit = commands.add_parser(
        "audit-core-derived-0-2-candidate",
        help="Independently audit every core-derived 0.2 output cell",
    )
    derived_0_2_audit.add_argument("--config", type=Path, required=True)
    derived_0_2_audit.add_argument("--derived-run-id", required=True)
    derived_0_2_audit.add_argument(
        "--run-id",
        help="Optional immutable derived 0.2 audit run ID; defaults to UTC",
    )
    derived_promotion = commands.add_parser(
        "promote-core-derived-release",
        help="Promote the explicitly approved audited core-derived candidate immutably",
    )
    derived_promotion.add_argument("--config", type=Path, required=True)
    derived_0_2_promotion = commands.add_parser(
        "promote-core-derived-0-2-release",
        help="Promote the explicitly approved core-derived 0.2 candidate exactly",
    )
    derived_0_2_promotion.add_argument("--config", type=Path, required=True)
    time_blocking_evidence = commands.add_parser(
        "audit-time-blocking-8h-contract-evidence",
        help=(
            "Run the approved read-only 8-hour time-blocking contract evidence audit"
        ),
    )
    time_blocking_evidence.add_argument("--config", type=Path, required=True)
    time_blocking_evidence.add_argument(
        "--run-id",
        help="Optional immutable evidence run ID; defaults to UTC",
    )
    time_blocking_build = commands.add_parser(
        "build-time-blocking-candidate",
        help="Build a run-scoped candidate under a reviewed resolution contract",
    )
    time_blocking_build.add_argument("--config", type=Path, required=True)
    time_blocking_build.add_argument("--resolution", choices=("8h",), required=True)
    time_blocking_build.add_argument(
        "--run-id",
        help="Optional immutable candidate run ID; defaults to UTC",
    )
    time_blocking_audit = commands.add_parser(
        "audit-time-blocking-candidate",
        help="Independently recompute every blocked candidate output cell",
    )
    time_blocking_audit.add_argument("--config", type=Path, required=True)
    time_blocking_audit.add_argument("--resolution", choices=("8h",), required=True)
    time_blocking_audit.add_argument("--build-run-id", required=True)
    time_blocking_audit.add_argument(
        "--run-id",
        help="Optional immutable audit run ID; defaults to UTC",
    )
    time_blocking_promotion = commands.add_parser(
        "promote-time-blocking-release",
        help="Promote the exactly approved audited time-blocking candidate",
    )
    time_blocking_promotion.add_argument(
        "--config", type=Path, required=True
    )
    time_blocking_promotion.add_argument(
        "--resolution", choices=("8h",), required=True
    )
    unit_resolution_audit = commands.add_parser(
        "audit-unresolved-units",
        help="Audit all unresolved units and related legacy ranges without changing data",
    )
    unit_resolution_audit.add_argument("--config", type=Path, required=True)
    unit_resolution_audit.add_argument(
        "--run-id",
        help="Optional immutable unit-resolution audit run ID; defaults to UTC",
    )
    unit_decision_audit = commands.add_parser(
        "audit-candidate-unit-decisions",
        help="Audit proposed unit conversions and site masks without changing data",
    )
    unit_decision_audit.add_argument("--config", type=Path, required=True)
    unit_decision_audit.add_argument(
        "--run-id",
        help="Optional immutable candidate-unit audit run ID; defaults to UTC",
    )
    unit_schema_amendment = commands.add_parser(
        "review-unit-schema-amendment",
        help="Plan the reviewed unit and semantic-split schema amendment read-only",
    )
    unit_schema_amendment.add_argument("--config", type=Path, required=True)
    unit_schema_amendment.add_argument(
        "--run-id",
        help="Optional immutable schema-amendment review run ID; defaults to UTC",
    )
    unit_schema_freeze = commands.add_parser(
        "freeze-unit-schema-dictionary",
        help="Freeze the exactly approved harmonized schema/dictionary contract 0.2",
    )
    unit_schema_freeze.add_argument("--config", type=Path, required=True)
    harmonized_0_2_build = commands.add_parser(
        "build-harmonized-0-2-candidate",
        help="Build the nonpublishable harmonized candidate under frozen contract 0.2",
    )
    harmonized_0_2_build.add_argument("--config", type=Path, required=True)
    harmonized_0_2_build.add_argument(
        "--run-id",
        help="Optional immutable harmonized 0.2 build run ID; defaults to UTC",
    )
    harmonized_0_2_audit = commands.add_parser(
        "audit-harmonized-0-2-candidate",
        help="Recompute and compare every harmonized 0.2 candidate output cell",
    )
    harmonized_0_2_audit.add_argument("--config", type=Path, required=True)
    harmonized_0_2_audit.add_argument("--build-run-id", required=True)
    harmonized_0_2_audit.add_argument(
        "--run-id",
        help="Optional immutable harmonized 0.2 audit run ID; defaults to UTC",
    )
    harmonized_0_2_promotion = commands.add_parser(
        "promote-harmonized-0-2-release",
        help="Promote the exactly approved harmonized 0.2 candidate byte-for-byte",
    )
    harmonized_0_2_promotion.add_argument("--config", type=Path, required=True)
    medication_semantics = commands.add_parser(
        "audit-medication-value-semantics",
        help="Audit medication zero and missing-value semantics without changing data",
    )
    medication_semantics.add_argument("--config", type=Path, required=True)
    medication_semantics.add_argument(
        "--run-id",
        help="Optional immutable medication-semantics audit run ID; defaults to UTC",
    )
    cleaning_0_2_review = commands.add_parser(
        "review-cleaning-0-2-policy",
        help="Review the complete cleaning 0.2 policy without reading clinical rows",
    )
    cleaning_0_2_review.add_argument("--config", type=Path, required=True)
    cleaning_0_2_review.add_argument(
        "--run-id",
        help="Optional immutable cleaning-policy review run ID; defaults to UTC",
    )
    cleaning_range_evidence = commands.add_parser(
        "audit-cleaning-0-2-ranges",
        help="Audit range directions and recoverability without changing clinical data",
    )
    cleaning_range_evidence.add_argument("--config", type=Path, required=True)
    cleaning_range_evidence.add_argument(
        "--run-id",
        help="Optional immutable range-evidence run ID; defaults to UTC",
    )
    cleaning_0_2_build = commands.add_parser(
        "build-cleaned-0-2-candidate",
        help="Build the approved run-scoped nonpublishable cleaned 0.2 candidate",
    )
    cleaning_0_2_build.add_argument("--config", type=Path, required=True)
    cleaning_0_2_build.add_argument(
        "--run-id",
        help="Optional immutable cleaned 0.2 build run ID; defaults to UTC",
    )
    cleaning_0_2_audit = commands.add_parser(
        "audit-cleaned-0-2-candidate",
        help="Recompute every cleaned 0.2 value and dictionary annotation",
    )
    cleaning_0_2_audit.add_argument("--config", type=Path, required=True)
    cleaning_0_2_audit.add_argument("--build-run-id", required=True)
    cleaning_0_2_audit.add_argument(
        "--run-id",
        help="Optional immutable cleaned 0.2 audit run ID; defaults to UTC",
    )
    commands.add_parser(
        "promote-cleaned-0-2-release",
        help="Promote the explicitly approved cleaned 0.2 candidate byte-for-byte",
    ).add_argument("--config", type=Path, required=True)
    return parser


def _inventory_command(config_path: Path, run_id: str | None) -> int:
    config = load_inventory_config(config_path)
    evidence = build_raw_inventory(config)
    written = write_inventory_report_bundle(evidence, config.paths.reports, run_id)
    print(f"inventory_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(f"review_report={written.review_markdown_path}")
    print("production_artifacts_generated=false")
    return 2 if written.publication_blocked else 0


def _anomaly_command(
    config_path: Path,
    inventory_run_id: str,
    run_id: str | None,
) -> int:
    config = load_inventory_config(config_path)
    evidence = build_raw_anomaly_audit(
        config,
        inventory_run_id,
        progress=print,
    )
    written = write_raw_anomaly_audit_bundle(
        evidence,
        config.paths.reports,
        run_id,
    )
    print(f"anomaly_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(f"review_report={written.review_markdown_path}")
    print("production_artifacts_generated=false")
    return 2 if written.publication_blocked else 0


def _ingestion_command(
    config_path: Path,
    inventory_run_id: str,
    hospital: str,
) -> int:
    config = load_ingestion_config(config_path)
    result = ingest_hospital(
        config,
        inventory_run_id,
        hospital,
        progress=print,
    )
    print("ingestion_status=pass")
    print(f"hospital={result.hospital_id}")
    print(f"static_rows={result.static_row_count}")
    print(f"dynamic_rows={result.dynamic_row_count}")
    print(f"manifest={result.manifest_path}")
    print(f"publication_ready={str(result.publication_ready).lower()}")
    return 0


def _ingestion_audit_command(
    config_path: Path,
    inventory_run_id: str,
    run_id: str | None,
) -> int:
    config = load_ingestion_config(config_path)
    result = build_ingestion_audit(
        config,
        inventory_run_id,
        progress=print,
    )
    written = write_ingestion_audit_bundle(
        result,
        config.inventory.paths.reports,
        run_id,
    )
    print(f"ingestion_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    blocker_names = {finding["check"] for finding in written.blocking_findings}
    if not blocker_names:
        return 0
    if blocker_names == {"provisional_archive_owner_confirmation"}:
        return 2
    return 1


def _schema_token_inventory_command(
    config_path: Path,
    inventory_run_id: str,
    ingestion_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_schema_token_config(config_path)
    result = build_schema_token_inventory(
        config,
        inventory_run_id,
        ingestion_audit_run_id,
        progress=print,
    )
    written = write_schema_token_inventory_bundle(
        result,
        config.ingestion.inventory.paths.reports,
        run_id,
    )
    print(f"schema_token_inventory_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _harmonization_registry_review_command(
    config_path: Path,
    schema_token_run_id: str,
    run_id: str | None,
) -> int:
    config = load_harmonization_review_config(config_path)
    result = build_harmonization_registry_review(config, schema_token_run_id)
    written = write_harmonization_registry_review_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"harmonization_registry_review_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _composite_source_audit_command(
    config_path: Path,
    registry_review_run_id: str,
    run_id: str | None,
) -> int:
    config = load_composite_source_audit_config(config_path)
    result = build_composite_source_audit(
        config,
        registry_review_run_id,
        progress=print,
    )
    written = write_composite_source_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"composite_source_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    return 1 if written.has_technical_failure else 0


def _static_contract_audit_command(
    config_path: Path,
    registry_review_run_id: str,
    composite_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_static_contract_audit_config(config_path)
    result = build_static_contract_audit(
        config,
        registry_review_run_id,
        composite_audit_run_id,
        progress=print,
    )
    written = write_static_contract_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"static_contract_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _static_completion_review_command(
    config_path: Path,
    static_contract_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_static_completion_review_config(config_path)
    result = build_static_completion_review(
        config,
        static_contract_audit_run_id,
    )
    written = write_static_completion_review_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"static_completion_review_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _icd10_notation_audit_command(
    config_path: Path,
    static_contract_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_icd10_notation_audit_config(config_path)
    result = build_icd10_notation_audit(
        config,
        static_contract_audit_run_id,
        progress=print,
    )
    written = write_icd10_notation_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"icd10_notation_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _icd10_component_audit_command(
    config_path: Path,
    notation_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_icd10_component_audit_config(config_path)
    result = build_icd10_component_audit(
        config,
        notation_audit_run_id,
        progress=print,
    )
    written = write_icd10_component_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"icd10_component_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _icd10_component_detail_audit_command(
    config_path: Path,
    component_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_icd10_component_detail_audit_config(config_path)
    result = build_icd10_component_detail_audit(
        config,
        component_audit_run_id,
        progress=print,
    )
    written = write_icd10_component_detail_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"icd10_component_detail_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _static_registry_completion_audit_command(
    config_path: Path,
    registry_review_run_id: str,
    run_id: str | None,
) -> int:
    config = load_static_registry_completion_audit_config(config_path)
    result = build_static_registry_completion_audit(
        config,
        registry_review_run_id,
    )
    written = write_static_registry_completion_audit_bundle(
        result,
        config.reports_root,
        run_id,
    )
    print(f"static_registry_completion_audit_status={written.overall_status}")
    print(f"blocking_finding_count={len(written.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(written.technical_blocking_findings)}"
    )
    print(f"review_report={written.review_markdown_path}")
    print("production_data_artifacts_generated=false")
    if written.has_technical_failure:
        return 1
    return 2 if written.blocking_findings else 0


def _harmonization_dry_run_command(
    config_path: Path,
    registry_review_run_id: str,
    ingestion_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_harmonization_dry_run_config(config_path)
    result = run_harmonization_dry_run(
        config,
        registry_review_run_id,
        ingestion_audit_run_id,
        run_id,
        progress=print,
    )
    print(f"harmonization_dry_run_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"candidate_directory={result.candidate_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_artifacts_generated=true")
    print("publication_ready=false")
    if result.has_technical_failure:
        return 1
    return 2 if result.blocking_findings else 0


def _consolidated_harmonization_audit_command(
    config_path: Path,
    harmonization_run_id: str,
    run_id: str | None,
) -> int:
    config = load_consolidated_audit_config(config_path)
    result = run_consolidated_harmonization_audit(
        config,
        harmonization_run_id,
        run_id,
        progress=print,
    )
    print(f"consolidated_harmonization_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_data_modified=false")
    print("production_data_artifacts_generated=false")
    print("publication_ready=false")
    if result.has_technical_failure:
        return 1
    return 2 if result.blocking_findings else 0


def _categorical_contract_review_command(
    config_path: Path,
    consolidated_audit_run_id: str,
    run_id: str | None,
) -> int:
    config = load_categorical_review_config(config_path)
    result = run_categorical_contract_review(
        config,
        consolidated_audit_run_id,
        run_id,
    )
    print(f"categorical_contract_review_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_rows_rescanned=0")
    print("candidate_data_modified=false")
    print("production_data_artifacts_generated=false")
    print("publication_ready=false")
    if result.has_technical_failure:
        return 1
    return 2 if result.blocking_findings else 0


def _schema_dictionary_review_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_schema_dictionary_review_config(config_path)
    result = run_schema_dictionary_review(config, run_id)
    print(f"schema_dictionary_review_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_rows_rescanned=0")
    print("candidate_data_modified=false")
    print("production_data_artifacts_generated=false")
    print("schema_frozen=false")
    print("dictionary_frozen=false")
    print("publication_ready=false")
    if result.has_technical_failure:
        return 1
    return 2 if result.blocking_findings else 0


def _schema_dictionary_freeze_command(config_path: Path) -> int:
    config = load_schema_dictionary_freeze_config(config_path)
    result = freeze_schema_dictionary(config)
    print("schema_dictionary_freeze_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"contract_version={result.contract_version}")
    print(f"contract_directory={result.contract_directory}")
    print(f"freeze_manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_rows_rescanned=0")
    print("candidate_data_modified=false")
    print("production_data_artifacts_generated=false")
    print("schema_frozen=true")
    print("dictionary_frozen=true")
    print("publication_ready=false")
    return 0


def _harmonized_build_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_harmonized_build_config(config_path)
    result = run_harmonized_build(config, run_id, progress=print)
    print("harmonized_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"harmonized_build_run_id={result.run_id}")
    print(f"candidate_directory={result.output_directory}")
    print(f"build_manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("production_data_artifacts_generated=true")
    print("cleaning_applied=false")
    print("derivation_applied=false")
    print("publication_ready=false")
    return 0


def _harmonized_build_audit_command(
    config_path: Path,
    harmonized_build_run_id: str,
    run_id: str | None,
) -> int:
    config = load_harmonized_build_audit_config(config_path)
    result = run_harmonized_build_audit(
        config,
        harmonized_build_run_id,
        run_id,
        progress=print,
    )
    print(f"harmonized_build_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"harmonized_build_run_id={result.harmonized_build_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_data_modified=false")
    print("harmonized_data_modified=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _harmonized_promotion_command(config_path: Path) -> int:
    config = load_harmonized_promotion_config(config_path)
    result = promote_harmonized_release(config)
    print("harmonized_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_parquet_bytes_preserved=true")
    print("harmonized_layer_ready=true")
    print("cleaning_input_approved=true")
    print("cleaning_applied=false")
    print("derivation_applied=false")
    print("publication_ready=true")
    print("external_data_export_authorized=false")
    return 0


def _cleaning_build_command(config_path: Path, run_id: str | None) -> int:
    config = load_cleaning_config(config_path)
    result = run_cleaning_candidate_build(config, run_id, progress=print)
    print("cleaning_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"cleaning_run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"cleaning_manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("harmonized_data_modified=false")
    print("rows_or_stays_filtered=false")
    print("columns_dropped=false")
    print("derivation_applied=false")
    print("publication_ready=false")
    return 0


def _cleaning_audit_command(
    config_path: Path,
    cleaning_run_id: str,
    run_id: str | None,
) -> int:
    config = load_cleaning_config(config_path)
    result = run_cleaning_candidate_audit(
        config,
        cleaning_run_id,
        run_id,
        progress=print,
    )
    print(f"cleaning_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"cleaning_run_id={result.cleaning_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("harmonized_data_modified=false")
    print("cleaning_candidate_modified=false")
    print("rows_or_stays_filtered=false")
    print("columns_dropped=false")
    print("derivation_applied=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _cleaned_promotion_command(config_path: Path) -> int:
    config = load_cleaned_promotion_config(config_path)
    result = promote_cleaned_release(config)
    print("cleaned_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_parquet_bytes_preserved=true")
    print("cleaned_layer_ready=true")
    print("derived_input_approved=true")
    print("cleaning_rerun_during_promotion=false")
    print("derivation_applied=false")
    print("publication_ready=true")
    print("external_data_export_authorized=false")
    return 0


def _derivation_contract_review_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_derivation_contract_review_config(config_path)
    result = run_derivation_contract_review(config, run_id, progress=print)
    print(f"derivation_contract_review_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("cleaned_release_modified=false")
    print("clinical_data_written=false")
    print("cohort_filtering_applied=false")
    print("time_blocking_applied=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _driving_pressure_semantics_review_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_driving_pressure_semantics_config(config_path)
    result = run_driving_pressure_semantics_review(config, run_id, progress=print)
    print(f"driving_pressure_semantics_review_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("cleaned_release_modified=false")
    print("clinical_data_written=false")
    print("derived_formula_activated=false")
    print("values_masked=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _core_derived_build_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_core_derived_config(config_path)
    result = run_core_derived_candidate_build(config, run_id, progress=print)
    print("core_derived_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"derived_run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("cleaned_release_modified=false")
    print("rows_or_stays_filtered=false")
    print("source_columns_changed_or_dropped=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("publication_ready=false")
    return 0


def _core_derived_audit_command(
    config_path: Path,
    derived_run_id: str,
    run_id: str | None,
) -> int:
    config = load_core_derived_config(config_path)
    result = run_core_derived_candidate_audit(
        config,
        derived_run_id,
        run_id,
        progress=print,
    )
    print(f"core_derived_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"derived_run_id={result.derived_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_data_modified=false")
    print("cleaned_release_modified=false")
    print("rows_or_stays_filtered=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _core_derived_0_2_build_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_core_derived_0_2_config(config_path)
    result = run_core_derived_candidate_build(config, run_id, progress=print)
    print("core_derived_0_2_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"derived_run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("input_cleaned_release=20260807T154100Z")
    print("clinical_formula_changes_from_0_1=0")
    print("cleaned_release_modified=false")
    print("rows_or_stays_filtered=false")
    print("source_columns_changed_or_dropped=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("publication_ready=false")
    return 0


def _core_derived_0_2_audit_command(
    config_path: Path,
    derived_run_id: str,
    run_id: str | None,
) -> int:
    config = load_core_derived_0_2_config(config_path)
    result = run_core_derived_candidate_audit(
        config,
        derived_run_id,
        run_id,
        progress=print,
    )
    print(f"core_derived_0_2_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"derived_run_id={result.derived_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("every_cleaned_0_2_input_column_preserved_exactly=true")
    print("every_derived_value_recomputed_exactly=true")
    print("candidate_data_modified=false")
    print("cleaned_release_modified=false")
    print("rows_or_stays_filtered=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _core_derived_promotion_command(config_path: Path) -> int:
    config = load_core_derived_promotion_config(config_path)
    result = promote_core_derived_release(config)
    print("core_derived_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_parquet_bytes_preserved_exactly=true")
    print("core_derived_layer_ready=true")
    print("analysis_input_approved=true")
    print("derivation_rerun_during_promotion=false")
    print("cleaning_rerun_during_promotion=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("external_data_export_authorized=false")
    return 0


def _core_derived_0_2_promotion_command(config_path: Path) -> int:
    config = load_core_derived_0_2_promotion_config(config_path)
    result = promote_core_derived_0_2_release(config)
    print("core_derived_0_2_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"previous_pointer_snapshot={result.previous_pointer_snapshot_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_parquet_bytes_preserved_exactly=true")
    print("previous_current_pointer_bytes_preserved_exactly=true")
    print("previous_releases_preserved=true")
    print("core_derived_layer_ready=true")
    print("analysis_input_approved=true")
    print("derivation_rerun_during_promotion=false")
    print("cohort_generated=false")
    print("time_blocking_applied=false")
    print("publication_ready=true")
    print("external_data_export_authorized=false")
    return 0


def _time_blocking_contract_evidence_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_time_blocking_evidence_config(config_path)
    result = run_time_blocking_contract_evidence(
        config,
        run_id,
        progress=print,
    )
    print(f"time_blocking_contract_evidence_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_data_written=false")
    print("blocked_rows_written=false")
    print("time_blocking_candidate_created=false")
    print("time_blocking_release_created=false")
    print("current_release_pointer_modified=false")
    print("core_derived_release_modified=false")
    print("rows_or_stays_filtered=false")
    print("carry_forward_or_imputation_applied=false")
    print("analysis_cohort_created=false")
    print("publication_ready=false")
    print("external_data_export_authorized=false")
    if result.technical_blocking_findings:
        return 1
    return 2


def _time_blocking_build_command(
    config_path: Path,
    resolution: str,
    run_id: str | None,
) -> int:
    config = load_time_blocking_build_config(config_path)
    result = run_time_blocking_candidate_build(config, run_id, progress=print)
    print("time_blocking_build_status=pass")
    print(f"resolution={resolution}")
    print(f"build_run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"candidate_manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print(f"block_row_count={result.block_row_count}")
    print("source_release_modified=false")
    print("release_created=false")
    print("current_release_pointer_modified=false")
    print("rows_or_stays_filtered=false")
    print("carry_forward_or_imputation_applied=false")
    print("medication_dose_totals_emitted=false")
    print("publication_ready=false")
    print("external_data_export_authorized=false")
    return 0


def _time_blocking_audit_command(
    config_path: Path,
    resolution: str,
    build_run_id: str,
    run_id: str | None,
) -> int:
    config = load_time_blocking_build_config(config_path)
    result = run_time_blocking_candidate_audit(
        config,
        build_run_id,
        run_id,
        progress=print,
    )
    print(f"time_blocking_audit_status={result.overall_status}")
    print(f"resolution={resolution}")
    print(f"build_run_id={result.build_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"technical_blocking_finding_count={len(result.technical_blocking_findings)}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_modified=false")
    print("source_release_modified=false")
    print("release_created=false")
    print("current_release_pointer_modified=false")
    print("publication_ready=false")
    print("external_data_export_authorized=false")
    if result.technical_blocking_findings:
        return 1
    return 2


def _time_blocking_promotion_command(
    config_path: Path,
    resolution: str,
) -> int:
    config = load_time_blocking_promotion_config(config_path)
    result = promote_time_blocking_release(config)
    print("time_blocking_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"resolution={resolution}")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_candidate_bytes_preserved=true")
    print("candidate_preserved=true")
    print("core_derived_release_modified=false")
    print("core_derived_current_pointer_modified=false")
    print("static_table_copied=false")
    print("static_table_repeated_on_blocks=false")
    print("time_blocking_rerun_during_promotion=false")
    print("rows_or_stays_filtered=false")
    print("analysis_cohort_created=false")
    print("carry_forward_or_imputation_applied=false")
    print("medication_dose_totals_emitted=false")
    print("time_blocking_layer_ready=true")
    print("analysis_input_approved=true")
    print("publication_ready=true")
    print("external_data_export_authorized=false")
    return 0


def _unit_resolution_audit_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_unit_resolution_audit_config(config_path)
    result = run_unit_resolution_audit(config, run_id, progress=print)
    print(f"unit_resolution_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_data_written=false")
    print("harmonized_release_modified=false")
    print("cleaned_release_modified=false")
    print("derived_release_modified=false")
    print("values_converted=false")
    print("values_masked=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _unit_decision_audit_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_unit_decision_audit_config(config_path)
    result = run_unit_decision_audit(config, run_id, progress=print)
    print(f"unit_decision_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_data_written=false")
    print("harmonized_release_modified=false")
    print("cleaned_release_modified=false")
    print("derived_release_modified=false")
    print("values_converted=false")
    print("values_masked=false")
    print("contract_0_2_activated=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _unit_schema_amendment_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_unit_schema_amendment_config(config_path)
    result = run_unit_schema_amendment_review(config, run_id)
    print(f"unit_schema_amendment_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_rows_read=0")
    print("clinical_data_written=false")
    print("schema_frozen=false")
    print("dictionary_frozen=false")
    print("existing_releases_modified=false")
    print("cleaning_ranges_activated=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _unit_schema_dictionary_freeze_command(config_path: Path) -> int:
    config = load_unit_schema_dictionary_freeze_config(config_path)
    result = freeze_unit_schema_dictionary(config)
    print("unit_schema_dictionary_freeze_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"contract_version={result.contract_version}")
    print(f"contract_directory={result.contract_directory}")
    print(f"freeze_manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_rows_read=0")
    print("clinical_data_written=false")
    print("existing_contract_0_1_modified=false")
    print("existing_releases_modified=false")
    print("unit_conversions_activated=false")
    print("semantic_splits_activated=false")
    print("cleaning_rule_activated=false")
    print("schema_frozen=true")
    print("dictionary_frozen=true")
    print("publication_ready=false")
    return 0


def _harmonized_0_2_build_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_harmonized_0_2_build_config(config_path)
    result = run_harmonized_0_2_build(config, run_id, progress=print)
    print("harmonized_0_2_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"build_run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("source_releases_modified=false")
    print("rows_or_stays_filtered=false")
    print("cleaning_applied=false")
    print("derivation_applied=false")
    print("publication_ready=false")
    return 0


def _harmonized_0_2_audit_command(
    config_path: Path,
    build_run_id: str,
    run_id: str | None,
) -> int:
    config = load_harmonized_0_2_audit_config(config_path)
    result = run_harmonized_0_2_audit(
        config,
        build_run_id,
        run_id,
        progress=print,
    )
    print(f"harmonized_0_2_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"build_run_id={result.build_run_id}")
    print(f"audit_run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_data_modified=false")
    print("source_releases_modified=false")
    print("cleaning_applied=false")
    print("derivation_applied=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _harmonized_0_2_promotion_command(config_path: Path) -> int:
    config = load_harmonized_0_2_promotion_config(config_path)
    result = promote_harmonized_0_2_release(config)
    print("harmonized_0_2_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"previous_pointer_snapshot={result.previous_pointer_snapshot_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_parquet_bytes_preserved=true")
    print("previous_harmonized_release_preserved=true")
    print("harmonized_layer_ready=true")
    print("cleaning_input_approved=true")
    print("cleaning_applied=false")
    print("derivation_applied=false")
    print("publication_ready=true")
    print("external_data_export_authorized=false")
    return 0


def _medication_semantics_audit_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_medication_semantics_audit_config(config_path)
    result = run_medication_semantics_audit(config, run_id, progress=print)
    print(f"medication_semantics_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_data_written=false")
    print("existing_releases_modified=false")
    print("unit_conversions_activated=false")
    print("semantic_splits_activated=false")
    print("null_to_zero_imputation_applied=false")
    print("zero_to_null_conversion_applied=false")
    print("carry_forward_applied=false")
    print("schema_frozen=false")
    print("dictionary_frozen=false")
    print("publication_ready=false")
    if result.technical_blocking_findings:
        return 1
    return 2 if result.blocking_findings else 0


def _cleaning_0_2_policy_review_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_cleaning_0_2_policy_review_config(config_path)
    result = run_cleaning_0_2_policy_review(config, run_id)
    print(f"cleaning_0_2_policy_review_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_rows_read=0")
    print("clinical_data_written=false")
    print("existing_releases_modified=false")
    print("cleaning_policy_0_2_activated=false")
    print("cleaned_candidate_generated=false")
    print("publication_ready=false")
    return 2


def _cleaning_range_evidence_0_2_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_cleaning_range_evidence_0_2_config(config_path)
    result = run_cleaning_range_evidence_0_2(
        config,
        run_id,
        progress=print,
    )
    print(f"cleaning_range_evidence_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print(
        "technical_blocking_finding_count="
        f"{len(result.technical_blocking_findings)}"
    )
    print(f"run_id={result.run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("clinical_data_written=false")
    print("existing_releases_modified=false")
    print("cleaning_rules_activated=false")
    print("publication_ready=false")
    return 2


def _cleaning_0_2_build_command(
    config_path: Path,
    run_id: str | None,
) -> int:
    config = load_cleaning_0_2_build_config(config_path)
    result = run_cleaning_0_2_build(config, run_id, progress=print)
    print("cleaning_0_2_build_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"run_id={result.run_id}")
    print(f"candidate_directory={result.candidate_directory}")
    print(f"manifest={result.manifest_path}")
    print(f"review_report={result.review_markdown_path}")
    print("complete_audit_run=false")
    print("existing_releases_modified=false")
    print("publication_ready=false")
    return 0


def _cleaning_0_2_audit_command(
    config_path: Path,
    build_run_id: str,
    run_id: str | None,
) -> int:
    config = load_cleaning_0_2_audit_config(config_path)
    result = run_cleaning_0_2_audit(
        config,
        build_run_id,
        run_id,
        progress=print,
    )
    print(f"cleaning_0_2_audit_status={result.overall_status}")
    print(f"blocking_finding_count={len(result.blocking_findings)}")
    print("technical_blocking_finding_count=0")
    print(f"run_id={result.run_id}")
    print(f"build_run_id={result.build_run_id}")
    print(f"private_report_directory={result.private_report_directory}")
    print(f"review_report={result.review_markdown_path}")
    print("candidate_data_modified=false")
    print("existing_releases_modified=false")
    print("publication_ready=false")
    return 2 if result.blocking_findings else 0


def _cleaned_0_2_promotion_command(config_path: Path) -> int:
    config = load_cleaned_0_2_promotion_config(config_path)
    result = promote_cleaned_0_2_release(config)
    print("cleaned_0_2_promotion_status=pass")
    print("blocking_finding_count=0")
    print("technical_blocking_finding_count=0")
    print(f"release_id={result.release_id}")
    print(f"release_directory={result.release_directory}")
    print(f"release_manifest={result.release_manifest_path}")
    print(f"current_release_pointer={result.current_release_pointer_path}")
    print(f"previous_pointer_snapshot={result.previous_pointer_snapshot_path}")
    print(f"review_report={result.review_markdown_path}")
    print("audited_candidate_bytes_preserved=true")
    print("previous_releases_preserved=true")
    print("cleaned_layer_ready=true")
    print("derived_input_approved=true")
    print("cleaning_rerun_during_promotion=false")
    print("derivation_applied=false")
    print("external_data_export_authorized=false")
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "inventory-raw":
            return _inventory_command(arguments.config, arguments.run_id)
        if arguments.command == "audit-raw-anomalies":
            return _anomaly_command(
                arguments.config,
                arguments.inventory_run_id,
                arguments.run_id,
            )
        if arguments.command == "ingest-raw-hospital":
            return _ingestion_command(
                arguments.config,
                arguments.inventory_run_id,
                arguments.hospital,
            )
        if arguments.command == "audit-ingested":
            return _ingestion_audit_command(
                arguments.config,
                arguments.inventory_run_id,
                arguments.run_id,
            )
        if arguments.command == "inventory-schema-tokens":
            return _schema_token_inventory_command(
                arguments.config,
                arguments.inventory_run_id,
                arguments.ingestion_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "review-harmonization-registry":
            return _harmonization_registry_review_command(
                arguments.config,
                arguments.schema_token_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-reviewed-composite-source":
            return _composite_source_audit_command(
                arguments.config,
                arguments.registry_review_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-static-contract":
            return _static_contract_audit_command(
                arguments.config,
                arguments.registry_review_run_id,
                arguments.composite_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "review-remaining-static-contract":
            return _static_completion_review_command(
                arguments.config,
                arguments.static_contract_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-icd10-notation":
            return _icd10_notation_audit_command(
                arguments.config,
                arguments.static_contract_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-icd10-components":
            return _icd10_component_audit_command(
                arguments.config,
                arguments.icd10_notation_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-icd10-component-details":
            return _icd10_component_detail_audit_command(
                arguments.config,
                arguments.icd10_component_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-static-registry-completion":
            return _static_registry_completion_audit_command(
                arguments.config,
                arguments.registry_review_run_id,
                arguments.run_id,
            )
        if arguments.command == "harmonize-candidate-dry-run":
            return _harmonization_dry_run_command(
                arguments.config,
                arguments.registry_review_run_id,
                arguments.ingestion_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "audit-harmonization-candidate":
            return _consolidated_harmonization_audit_command(
                arguments.config,
                arguments.harmonization_run_id,
                arguments.run_id,
            )
        if arguments.command == "review-categorical-contract":
            return _categorical_contract_review_command(
                arguments.config,
                arguments.consolidated_audit_run_id,
                arguments.run_id,
            )
        if arguments.command == "review-schema-dictionary":
            return _schema_dictionary_review_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "freeze-schema-dictionary":
            return _schema_dictionary_freeze_command(arguments.config)
        if arguments.command == "build-harmonized-candidate":
            return _harmonized_build_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-harmonized-build":
            return _harmonized_build_audit_command(
                arguments.config,
                arguments.harmonized_build_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-harmonized-release":
            return _harmonized_promotion_command(arguments.config)
        if arguments.command == "build-cleaned-candidate":
            return _cleaning_build_command(arguments.config, arguments.run_id)
        if arguments.command == "audit-cleaned-candidate":
            return _cleaning_audit_command(
                arguments.config,
                arguments.cleaning_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-cleaned-release":
            return _cleaned_promotion_command(arguments.config)
        if arguments.command == "review-derived-contract":
            return _derivation_contract_review_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "review-driving-pressure-semantics":
            return _driving_pressure_semantics_review_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "build-core-derived-candidate":
            return _core_derived_build_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-core-derived-candidate":
            return _core_derived_audit_command(
                arguments.config,
                arguments.derived_run_id,
                arguments.run_id,
            )
        if arguments.command == "build-core-derived-0-2-candidate":
            return _core_derived_0_2_build_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-core-derived-0-2-candidate":
            return _core_derived_0_2_audit_command(
                arguments.config,
                arguments.derived_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-core-derived-release":
            return _core_derived_promotion_command(arguments.config)
        if arguments.command == "promote-core-derived-0-2-release":
            return _core_derived_0_2_promotion_command(arguments.config)
        if arguments.command == "audit-time-blocking-8h-contract-evidence":
            return _time_blocking_contract_evidence_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "build-time-blocking-candidate":
            return _time_blocking_build_command(
                arguments.config,
                arguments.resolution,
                arguments.run_id,
            )
        if arguments.command == "audit-time-blocking-candidate":
            return _time_blocking_audit_command(
                arguments.config,
                arguments.resolution,
                arguments.build_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-time-blocking-release":
            return _time_blocking_promotion_command(
                arguments.config,
                arguments.resolution,
            )
        if arguments.command == "audit-unresolved-units":
            return _unit_resolution_audit_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-candidate-unit-decisions":
            return _unit_decision_audit_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "review-unit-schema-amendment":
            return _unit_schema_amendment_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "freeze-unit-schema-dictionary":
            return _unit_schema_dictionary_freeze_command(arguments.config)
        if arguments.command == "build-harmonized-0-2-candidate":
            return _harmonized_0_2_build_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-harmonized-0-2-candidate":
            return _harmonized_0_2_audit_command(
                arguments.config,
                arguments.build_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-harmonized-0-2-release":
            return _harmonized_0_2_promotion_command(arguments.config)
        if arguments.command == "audit-medication-value-semantics":
            return _medication_semantics_audit_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "review-cleaning-0-2-policy":
            return _cleaning_0_2_policy_review_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-cleaning-0-2-ranges":
            return _cleaning_range_evidence_0_2_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "build-cleaned-0-2-candidate":
            return _cleaning_0_2_build_command(
                arguments.config,
                arguments.run_id,
            )
        if arguments.command == "audit-cleaned-0-2-candidate":
            return _cleaning_0_2_audit_command(
                arguments.config,
                arguments.build_run_id,
                arguments.run_id,
            )
        if arguments.command == "promote-cleaned-0-2-release":
            return _cleaned_0_2_promotion_command(arguments.config)
    except ASICPipelineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"Unhandled command: {arguments.command}")
