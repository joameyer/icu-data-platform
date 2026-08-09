from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

from asic_pipeline.audit import audit_pooled_input, write_audit_report
from asic_pipeline.config import load_config, validate_context_output_path
from asic_pipeline.contracts import load_input_contract
from asic_pipeline.errors import ASICPipelineError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="asic-pipeline",
        description="Validated preprocessing for pooled ASIC ICU data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    audit_parser = subparsers.add_parser(
        "audit-pooled",
        help="Validate pooled Parquet files without transforming data.",
    )
    audit_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to an explicit demo, mock, or production YAML configuration.",
    )
    audit_parser.add_argument(
        "--report-path",
        type=Path,
        default=None,
        help="Optional JSON output path; defaults to the configured reports directory.",
    )
    translated_audit_parser = subparsers.add_parser(
        "audit-translated",
        help="Validate translated Parquet files and their manifest without changing data.",
    )
    translated_audit_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to an explicit demo, mock, or production YAML configuration.",
    )
    translated_audit_parser.add_argument(
        "--report-path",
        type=Path,
        default=None,
        help="Optional JSON output path; defaults to the configured reports directory.",
    )
    cross_hospital_parser = subparsers.add_parser(
        "audit-cross-hospital",
        help=(
            "Profile translated numeric values, legacy rules, and cross-hospital "
            "discrepancies without changing data."
        ),
    )
    cross_hospital_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to an explicit demo, mock, or production YAML configuration.",
    )
    cross_hospital_parser.add_argument(
        "--report-path",
        type=Path,
        default=None,
        help="Optional JSON output path; defaults to the configured reports directory.",
    )
    registry_parser = subparsers.add_parser(
        "validate-translation-policy",
        help="Validate the pooled-to-translated policy without reading data.",
    )
    registry_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a demo, mock, or production YAML configuration.",
    )
    mock_parser = subparsers.add_parser(
        "generate-mock",
        help="Generate protected development mock data from production pooled data.",
    )
    mock_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the mock-generation YAML configuration.",
    )
    mock_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly permit replacement of existing mock outputs.",
    )
    demo_parser = subparsers.add_parser(
        "generate-demo",
        help="Select an unchanged, protected subset of production pooled data.",
    )
    demo_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the demo-generation YAML configuration.",
    )
    demo_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly permit replacement of existing demo outputs.",
    )
    translate_parser = subparsers.add_parser(
        "pooled-to-translated",
        help=(
            "Apply reviewed column, alias, and categorical-value translations."
        ),
    )
    translate_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the demo, mock, or production dataset configuration.",
    )
    translate_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly permit replacement of existing translated outputs.",
    )
    categorical_parser = subparsers.add_parser(
        "inventory-pooled-categories",
        help=(
            "Inventory pooled categorical values without changing data tables."
        ),
    )
    categorical_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the demo, mock, or production dataset configuration.",
    )
    categorical_parser.add_argument(
        "--report-path",
        type=Path,
        default=None,
        help=(
            "Optional JSON output path; defaults to the configured reports directory."
        ),
    )
    return parser


def _default_report_path(reports_directory: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return reports_directory / f"pooled_input_audit_{timestamp}.json"


def _default_categorical_report_path(reports_directory: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return reports_directory / f"categorical_value_inventory_{timestamp}.json"


def _default_translated_report_path(reports_directory: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return reports_directory / f"translated_input_audit_{timestamp}.json"


def _default_cross_hospital_report_path(reports_directory: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return reports_directory / f"cross_hospital_data_quality_audit_{timestamp}.json"


def _run_audit_input(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    contract = load_input_contract(config.contract_path)
    report = audit_pooled_input(config, contract)
    report_path = (
        args.report_path.expanduser().resolve()
        if args.report_path is not None
        else _default_report_path(config.paths.reports)
    )
    validate_context_output_path(
        report_path.parent,
        config.dataset_context,
        "audit report output",
    )
    write_audit_report(report, report_path)

    print(f"Pooled input audit: {report.overall_status}")
    print(f"Dataset context: {report.dataset_context}")
    print(f"Static rows: {report.inputs['static']['row_count']}")
    print(f"Dynamic rows: {report.inputs['dynamic']['row_count']}")
    print(f"Report: {report_path}")
    for check in report.checks:
        if check.status == "fail":
            print(
                f"{check.severity.upper()} {check.name}: {check.observed}",
                file=sys.stderr,
            )
    return 1 if report.overall_status == "fail" else 0


def _run_audit_translated(args: argparse.Namespace) -> int:
    from asic_pipeline.audit import audit_translated_input
    from asic_pipeline.contracts import load_translated_input_contract
    from asic_pipeline.translated_input import load_translated_input_config

    config = load_translated_input_config(args.config)
    contract = load_translated_input_contract(config.contract_path)
    report = audit_translated_input(config, contract)
    report_path = (
        args.report_path.expanduser().resolve()
        if args.report_path is not None
        else _default_translated_report_path(config.reports_dir)
    )
    validate_context_output_path(
        report_path.parent,
        config.dataset_context,
        "translated audit report output",
    )
    write_audit_report(report, report_path)

    print(f"Translated input audit: {report.overall_status}")
    print(f"Dataset context: {report.dataset_context}")
    print(f"Static rows: {report.inputs['static']['row_count']}")
    print(f"Dynamic rows: {report.inputs['dynamic']['row_count']}")
    print(f"Report: {report_path}")
    for check in report.checks:
        if check.status == "fail":
            print(
                f"{check.severity.upper()} {check.name}: {check.observed}",
                file=sys.stderr,
            )
    return 1 if report.overall_status == "fail" else 0


def _run_audit_cross_hospital(args: argparse.Namespace) -> int:
    from asic_pipeline.audit import audit_cross_hospital_data_quality
    from asic_pipeline.contracts import load_translated_input_contract
    from asic_pipeline.data_quality import (
        load_cross_hospital_audit_config,
        load_data_quality_policy,
    )

    config = load_cross_hospital_audit_config(args.config)
    contract = load_translated_input_contract(config.translated_contract_path)
    policy = load_data_quality_policy(config.policy_path, contract)
    report = audit_cross_hospital_data_quality(config, contract, policy)
    report_path = (
        args.report_path.expanduser().resolve()
        if args.report_path is not None
        else _default_cross_hospital_report_path(config.reports_dir)
    )
    validate_context_output_path(
        report_path.parent,
        config.dataset_context,
        "cross-hospital audit report output",
    )
    write_audit_report(report, report_path)

    print(f"Cross-hospital data-quality audit: {report.overall_status}")
    print(f"Dataset context: {report.dataset_context}")
    print(f"Static rows: {report.inputs['static']['row_count']}")
    print(f"Dynamic rows: {report.inputs['dynamic']['row_count']}")
    print(
        "Distribution flags: "
        f"{len(report.metrics['cross_hospital_distribution_issues'])}"
    )
    print(f"Report: {report_path}")
    for check in report.checks:
        if check.status == "fail":
            print(
                f"{check.severity.upper()} {check.name}: {check.observed}",
                file=sys.stderr,
            )
    return 1 if report.overall_status == "fail" else 0


def _run_validate_translation_policy(args: argparse.Namespace) -> int:
    from asic_pipeline.translations import (
        load_translation_config,
        load_translation_registry,
    )

    config = load_translation_config(args.config)
    contract = load_input_contract(config.source_contract_path)
    registry = load_translation_registry(config.policy_path, contract)
    print(f"Translation policy: {registry.status}")
    print(f"Policy version: {registry.policy_version}")
    print(f"Source contract version: {registry.source_contract_version}")
    print(f"Static source columns: {len(registry.static)}")
    print(f"Dynamic source columns: {len(registry.dynamic)}")
    print(
        "Translated output columns: "
        f"static={len(registry.output_order_for('static'))} in "
        f"{len(registry.output_groups_for('static'))} groups, "
        f"dynamic={len(registry.output_order_for('dynamic'))} in "
        f"{len(registry.output_groups_for('dynamic'))} groups"
    )
    print(f"Approved merge groups: {registry.approved_merge_count}")
    print(
        "Unresolved collision groups: "
        f"{registry.unresolved_collision_group_count}"
    )
    print(f"Approved all-missing drops: {len(registry.drop_rules)}")
    print(
        "Approved categorical value mappings: "
        f"static={len(registry.value_mappings_for('static'))}, "
        f"dynamic={len(registry.value_mappings_for('dynamic'))}"
    )
    print(
        "Approved numeric missing sentinels: "
        f"static={len(registry.numeric_missing_sentinel_rules_for('static'))}, "
        f"dynamic={len(registry.numeric_missing_sentinel_rules_for('dynamic'))}"
    )
    print(
        "Approved hospital-specific semantic splits: "
        f"static={len(registry.hospital_specific_semantic_split_rules_for('static'))}, "
        "dynamic="
        f"{len(registry.hospital_specific_semantic_split_rules_for('dynamic'))}"
    )
    print(
        "Categorical inventories required: "
        f"static={registry.categorical_inventory_counts['static']}, "
        f"dynamic={registry.categorical_inventory_counts['dynamic']}"
    )
    print(
        "Semantic reviews required: "
        f"static={registry.semantic_review_counts['static']}, "
        f"dynamic={registry.semantic_review_counts['dynamic']}"
    )
    return 0


def _run_generate_mock(args: argparse.Namespace) -> int:
    from asic_pipeline.mock_data import (
        generate_and_write_mock_data,
        load_mock_generation_config,
    )

    config = load_mock_generation_config(args.config)
    result = generate_and_write_mock_data(config, overwrite=args.overwrite)
    print("Mock generation: pass")
    print(f"Static rows: {result.static_row_count}")
    print(f"Dynamic rows: {result.dynamic_row_count}")
    print(f"Unique stays: {result.stay_count}")
    print(f"Static: {result.static_path}")
    print(f"Dynamic: {result.dynamic_path}")
    print(f"Manifest: {result.manifest_path}")
    return 0


def _run_generate_demo(args: argparse.Namespace) -> int:
    from asic_pipeline.demo_data import (
        generate_and_write_demo_data,
        load_demo_generation_config,
    )

    config = load_demo_generation_config(args.config)
    result = generate_and_write_demo_data(config, overwrite=args.overwrite)
    print("Demo generation: pass")
    print(f"Static rows: {result.static_row_count}")
    print(f"Dynamic rows: {result.dynamic_row_count}")
    print(f"Unique stays: {result.stay_count}")
    print(f"Static: {result.static_path}")
    print(f"Dynamic: {result.dynamic_path}")
    print(f"Manifest: {result.manifest_path}")
    return 0


def _run_pooled_to_translated(args: argparse.Namespace) -> int:
    from asic_pipeline.translations import (
        load_translation_config,
        translate_and_write_pooled_data,
    )

    config = load_translation_config(args.config)
    result = translate_and_write_pooled_data(
        config,
        overwrite=args.overwrite,
    )
    print("Pooled-to-translated: pass")
    print(f"Static rows: {result.static_row_count}")
    print(f"Dynamic rows: {result.dynamic_row_count}")
    print(f"Static columns: {result.static_column_count}")
    print(f"Dynamic columns: {result.dynamic_column_count}")
    print(f"Static: {result.static_path}")
    print(f"Dynamic: {result.dynamic_path}")
    print(f"Manifest: {result.manifest_path}")
    return 0


def _run_inventory_categorical_values(args: argparse.Namespace) -> int:
    from asic_pipeline.categorical_inventory import (
        inventory_categorical_values,
        load_categorical_inventory_config,
        write_categorical_inventory_report,
    )

    config = load_categorical_inventory_config(args.config)
    result = inventory_categorical_values(config)
    report_path = (
        args.report_path.expanduser().resolve()
        if args.report_path is not None
        else _default_categorical_report_path(config.reports_dir)
    )
    validate_context_output_path(
        report_path.parent,
        config.dataset_context,
        "categorical inventory report output",
    )
    write_categorical_inventory_report(result.report, report_path)
    print(f"Categorical value inventory: {result.audit_status}")
    print(f"Dataset context: {config.dataset_context}")
    print(f"Static rows: {result.static_row_count}")
    print(f"Dynamic rows: {result.dynamic_row_count}")
    print(f"Report: {report_path}")
    return 1 if result.audit_status == "fail" else 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "audit-pooled":
            return _run_audit_input(args)
        if args.command == "audit-translated":
            return _run_audit_translated(args)
        if args.command == "audit-cross-hospital":
            return _run_audit_cross_hospital(args)
        if args.command == "validate-translation-policy":
            return _run_validate_translation_policy(args)
        if args.command == "generate-mock":
            return _run_generate_mock(args)
        if args.command == "generate-demo":
            return _run_generate_demo(args)
        if args.command == "pooled-to-translated":
            return _run_pooled_to_translated(args)
        if args.command == "inventory-pooled-categories":
            return _run_inventory_categorical_values(args)
    except ASICPipelineError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
