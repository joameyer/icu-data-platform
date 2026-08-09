from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import InventoryConfig
from asic_pipeline.errors import InventoryError
from asic_pipeline.inventory.classifier import FileCandidate, discover_raw_files
from asic_pipeline.inventory.comparison import (
    compare_folder_stays_to_pooled,
    load_pooled_reference_config,
)
from asic_pipeline.inventory.csv_dialect import detect_delimiter, inspect_csv
from asic_pipeline.inventory.duplicates import (
    classify_dynamic_duplicate_group,
    read_dynamic_file,
)
from asic_pipeline.inventory.encoding import detect_encoding
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.identifiers import inspect_identifiers
from asic_pipeline.inventory.policy import InventoryPolicy, load_inventory_policy
from asic_pipeline.inventory.static_candidates import compare_static_candidates
from asic_pipeline.privacy import assert_review_payload_is_safe


CSV_CLASSIFICATIONS = frozenset(
    {"selected_static_candidate", "additional_static_candidate", "dynamic_candidate"}
)
IDENTIFIER_BLOCKING_STATUSES = frozenset(
    {"multiple_identifier_columns", "multiple_in_file_identifiers", "mismatch"}
)
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$")


@dataclass(frozen=True)
class InventoryRunResult:
    dataset_context: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    review_payload: dict[str, Any]
    private_tables: dict[str, tuple[dict[str, Any], ...]]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def publication_blocked(self) -> bool:
        return self.overall_status == "fail"


def _check(
    name: str,
    passed: bool,
    observed: Any,
    expected: Any,
    details: str,
    *,
    severity: str = "blocking",
) -> CheckResult:
    return CheckResult(
        name=name,
        status="pass" if passed else "fail",
        severity=severity,  # type: ignore[arg-type]
        observed=observed,
        expected=expected,
        details=details,
    )


def _jsonable(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return _jsonable(value.to_dict())
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(child) for child in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _inspect_file(
    candidate: FileCandidate,
    file_id: str,
    policy: InventoryPolicy,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "file_id": file_id,
        "absolute_path": str(candidate.absolute_path),
        "relative_path": candidate.relative_path,
        "source_filename": candidate.source_filename,
        "hospital_folder": candidate.hospital_folder,
        "classification": candidate.classification,
        "filename_pattern_scope": candidate.filename_pattern_scope,
        "filename_stay_id": candidate.filename_stay_id,
        "size_bytes": None,
        "sha256": None,
        "inspection_status": "not_csv_data",
        "error": None,
        "is_symbolic_link": candidate.absolute_path.is_symlink(),
    }
    if record["is_symbolic_link"]:
        record["inspection_status"] = "symbolic_link_blocked"
        return record
    try:
        record["size_bytes"] = candidate.absolute_path.stat().st_size
        record["sha256"] = sha256_file(candidate.absolute_path)
        if candidate.classification == "archive_candidate":
            archive_rule = policy.provisional_archive_exclusion_for(
                candidate.hospital_folder,
                record["size_bytes"],
                record["sha256"],
            )
            if archive_rule is not None:
                record["classification"] = "provisionally_excluded_archive"
                record["archive_resolution"] = {
                    "rule_id": archive_rule.rule_id,
                    "approval_status": archive_rule.approval_status,
                    "ingestion_action": archive_rule.ingestion_action,
                    "retained_role": archive_rule.retained_role,
                    "match_basis": "hospital_folder+size_bytes+sha256",
                }
            record["inspection_status"] = "hashed"
            return record
        if candidate.classification not in CSV_CLASSIFICATIONS:
            record["inspection_status"] = "hashed"
            return record
        encoding = detect_encoding(candidate.absolute_path, policy.csv.encodings)
        record["encoding"] = _jsonable(encoding)
        if encoding.selected_encoding is None:
            record["inspection_status"] = "encoding_unresolved"
            return record
        dialect = detect_delimiter(
            candidate.absolute_path,
            encoding.selected_encoding,
            policy.csv.delimiters,
            policy.csv.sample_records,
            policy.csv.minimum_header_columns,
            policy.csv.require_consistent_record_width,
        )
        record["delimiter"] = _jsonable(dialect)
        if dialect.selected_delimiter is None:
            record["inspection_status"] = "delimiter_unresolved"
            return record
        inspection = inspect_csv(
            candidate.absolute_path,
            encoding.selected_encoding,
            dialect.selected_delimiter,
        )
        record["csv"] = _jsonable(inspection)
        reviewed_duplicate = policy.reviewed_duplicate_header_for(
            candidate.hospital_folder,
            inspection.header,
            inspection.duplicate_header_names,
        )
        if reviewed_duplicate is not None:
            record["reviewed_duplicate_header"] = {
                "rule_id": reviewed_duplicate.rule_id,
                "raw_column_name": reviewed_duplicate.raw_column_name,
                "zero_based_positions": [
                    index
                    for index, name in enumerate(inspection.header)
                    if name == reviewed_duplicate.raw_column_name
                ],
                "ingestion_action": reviewed_duplicate.ingestion_action,
                "translation_action": reviewed_duplicate.translation_action,
                "evidence": reviewed_duplicate.evidence,
            }
        if (
            inspection.parse_error is not None
            or (
                inspection.duplicate_header_names
                and reviewed_duplicate is None
            )
            or (
                policy.csv.require_consistent_record_width
                and inspection.inconsistent_record_count
            )
        ):
            record["inspection_status"] = "csv_structure_invalid"
            return record
        identifiers = inspect_identifiers(
            candidate.absolute_path,
            encoding.selected_encoding,
            dialect.selected_delimiter,
            policy.candidate_identifier_columns,
            policy.candidate_hospital_identity_columns,
            policy.candidate_time_columns,
            candidate.filename_stay_id,
        )
        record["identifiers"] = _jsonable(identifiers)
        record["inspection_status"] = "complete"
    except (OSError, UnicodeError, ValueError) as exc:
        record["inspection_status"] = "inspection_error"
        record["error"] = f"{type(exc).__name__}: {exc}"
    return record


def _dynamic_duplicate_evidence(
    records: list[dict[str, Any]],
    candidates_by_id: dict[str, FileCandidate],
    policy: InventoryPolicy,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if (
            record["classification"] == "dynamic_candidate"
            and record["filename_stay_id"] is not None
        ):
            groups[(record["hospital_folder"], record["filename_stay_id"])].append(
                record
            )
    evidence: list[dict[str, Any]] = []
    for (hospital, stay_id), group in sorted(groups.items()):
        if len(group) < 2:
            continue
        parsed = []
        for record in group:
            if record["inspection_status"] != "complete":
                parsed = []
                break
            candidate = candidates_by_id[record["file_id"]]
            parsed.append(
                read_dynamic_file(
                    candidate.absolute_path,
                    record["encoding"]["selected_encoding"],
                    record["delimiter"]["selected_delimiter"],
                    record["sha256"],
                    policy.candidate_time_columns,
                )
            )
        classification = (
            classify_dynamic_duplicate_group(tuple(parsed)) if parsed else "unresolved"
        )
        evidence.append(
            {
                "hospital_folder": hospital,
                "filename_stay_id": stay_id,
                "file_ids": [record["file_id"] for record in group],
                "file_count": len(group),
                "classification": classification,
                "resolution_status": "human_review_required",
            }
        )
    return evidence


def _static_comparison_evidence(
    records: list[dict[str, Any]],
    candidates_by_id: dict[str, FileCandidate],
    policy: InventoryPolicy,
) -> list[dict[str, Any]]:
    by_hospital: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record["classification"] in {
            "selected_static_candidate",
            "additional_static_candidate",
        }:
            by_hospital[record["hospital_folder"]].append(record)
    evidence: list[dict[str, Any]] = []
    for hospital, hospital_records in sorted(by_hospital.items()):
        selected = [
            record
            for record in hospital_records
            if record["classification"] == "selected_static_candidate"
        ]
        additional = [
            record
            for record in hospital_records
            if record["classification"] == "additional_static_candidate"
        ]
        for left in selected:
            for right in additional:
                result: dict[str, Any]
                if (
                    left["inspection_status"] == "complete"
                    and right["inspection_status"] == "complete"
                ):
                    comparison = compare_static_candidates(
                        candidates_by_id[left["file_id"]].absolute_path,
                        candidates_by_id[right["file_id"]].absolute_path,
                        left["encoding"]["selected_encoding"],
                        right["encoding"]["selected_encoding"],
                        left["delimiter"]["selected_delimiter"],
                        right["delimiter"]["selected_delimiter"],
                        left["sha256"],
                        right["sha256"],
                        policy.candidate_identifier_columns,
                    )
                    result = comparison.to_dict()
                else:
                    result = {"classification": "unresolved"}
                evidence.append(
                    {
                        "hospital_folder": hospital,
                        "selected_file_id": left["file_id"],
                        "additional_file_id": right["file_id"],
                        **result,
                    }
                )
    return evidence


def _folder_mapping_evidence(
    config: InventoryConfig,
    policy: InventoryPolicy,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    static_identifiers_by_folder: dict[str, set[str]] = defaultdict(set)
    for record in records:
        if (
            record["classification"] == "selected_static_candidate"
            and record["inspection_status"] == "complete"
        ):
            static_identifiers_by_folder[record["hospital_folder"]].update(
                record["identifiers"]["identifier_values"]
            )
    for folder in policy.identity_investigation_folders:
        mapping = policy.hospital_mapping_for(folder)
        if mapping is None:
            raise InventoryError(
                f"Identity investigation folder lacks a mapping: {folder}"
            )
        static_records = [
            record
            for record in records
            if record["hospital_folder"] == folder
            and record["classification"] == "selected_static_candidate"
            and record["inspection_status"] == "complete"
        ]
        identifiers = set()
        for record in static_records:
            identifiers.update(record["identifiers"]["identifier_values"])
        dynamic_records = [
            record
            for record in records
            if record["hospital_folder"] == folder
            and record["classification"] == "dynamic_candidate"
            and record["inspection_status"] == "complete"
        ]
        dynamic_status_counts = Counter(
            record["identifiers"]["filename_comparison_status"]
            for record in dynamic_records
        )
        decimal_comma_counts: Counter[str] = Counter()
        hospital_identity_values: dict[str, set[str]] = defaultdict(set)
        hospital_identity_nonempty_counts: Counter[str] = Counter()
        for record in (*static_records, *dynamic_records):
            for column, values in record["identifiers"][
                "hospital_identity_values"
            ].items():
                hospital_identity_values[column].update(values)
            hospital_identity_nonempty_counts.update(
                record["identifiers"]["hospital_identity_nonempty_counts"]
            )
        for record in dynamic_records:
            decimal_comma_counts.update(
                dict(record["csv"]["decimal_comma_candidate_counts"])
            )
        raw_overlap = {
            other_folder: len(identifiers & other_identifiers)
            for other_folder, other_identifiers in sorted(
                static_identifiers_by_folder.items()
            )
            if other_folder != folder
        }
        folder_evidence: dict[str, Any] = {
            "mapping_status": mapping.approval_status,
            "canonical_hospital_id": mapping.canonical_hospital_id,
            "cohort_action": mapping.cohort_action,
            "mapping_approval_evidence": mapping.evidence,
            "raw_static_unique_stays": len(identifiers),
            "raw_static_file_count": len(static_records),
            "raw_dynamic_file_count": len(dynamic_records),
            "raw_dynamic_row_count": sum(
                record["csv"]["data_record_count"] for record in dynamic_records
            ),
            "dynamic_identifier_status_counts": dict(
                sorted(dynamic_status_counts.items())
            ),
            "dynamic_schema_variant_count": len(
                {
                    record["csv"]["schema_variant_id"]
                    for record in dynamic_records
                }
            ),
            "decimal_comma_candidate_counts": dict(
                sorted(decimal_comma_counts.items())
            ),
            "raw_static_stay_overlap_by_other_folder": raw_overlap,
            "hospital_identity_values": {
                column: sorted(values)
                for column, values in sorted(hospital_identity_values.items())
            },
            "hospital_identity_summary": {
                column: {
                    "nonempty_count": hospital_identity_nonempty_counts[column],
                    "distinct_value_count": len(values),
                }
                for column, values in sorted(hospital_identity_values.items())
            },
            "pooled_comparison": {
                "status": "not_configured",
                "limitation": "No read-only comparison artifact was configured.",
            },
            "required_human_evidence": [],
            "remaining_non_mapping_reviews": [
                "raw-token revalidation of legacy UK01 decimal-comma parsing",
                "hospital-aware parsing and translation policies",
            ],
        }
        if config.comparison_reference_path is not None:
            reference = load_pooled_reference_config(config.comparison_reference_path)
            folder_evidence["pooled_comparison"] = compare_folder_stays_to_pooled(
                identifiers, reference
            ).to_dict()
        evidence[folder] = folder_evidence
    return evidence


def _schema_variants(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variants: dict[str, dict[str, Any]] = {}
    for record in records:
        if record["inspection_status"] != "complete":
            continue
        csv_evidence = record["csv"]
        variant_id = csv_evidence["schema_variant_id"]
        variant = variants.setdefault(
            variant_id,
            {
                "schema_variant_id": variant_id,
                "encoding": record["encoding"]["selected_encoding"],
                "delimiter": record["delimiter"]["selected_delimiter"],
                "header": csv_evidence["header"],
                "column_count": len(csv_evidence["header"]),
                "file_count": 0,
                "hospital_file_counts": Counter(),
                "classification_counts": Counter(),
                "file_ids": [],
            },
        )
        variant["file_count"] += 1
        variant["hospital_file_counts"][record["hospital_folder"]] += 1
        variant["classification_counts"][record["classification"]] += 1
        variant["file_ids"].append(record["file_id"])
    return [
        {
            **variant,
            "hospital_file_counts": dict(sorted(variant["hospital_file_counts"].items())),
            "classification_counts": dict(
                sorted(variant["classification_counts"].items())
            ),
        }
        for _, variant in sorted(variants.items())
    ]


def build_raw_inventory(config: InventoryConfig) -> InventoryRunResult:
    policy = load_inventory_policy(config.policy_path)
    root = config.paths.raw_root
    if not root.is_dir():
        raise InventoryError(f"Raw root is not an accessible directory: {root}")
    discovery = discover_raw_files(root, policy)
    records: list[dict[str, Any]] = []
    candidates_by_id: dict[str, FileCandidate] = {}
    for index, candidate in enumerate(discovery.files, start=1):
        file_id = f"file_{index:08d}"
        candidates_by_id[file_id] = candidate
        records.append(_inspect_file(candidate, file_id, policy))

    static_comparisons = _static_comparison_evidence(
        records, candidates_by_id, policy
    )
    dynamic_duplicates = _dynamic_duplicate_evidence(
        records, candidates_by_id, policy
    )
    mapping_evidence = _folder_mapping_evidence(config, policy, records)
    variants = _schema_variants(records)

    observed = set(discovery.observed_hospital_folders)
    expected = set(policy.expected_hospital_folders)
    unapproved_hospital_mappings = []
    for folder in sorted(observed):
        mapping = policy.hospital_mapping_for(folder)
        if (
            mapping is None
            or mapping.cohort_action != "include"
            or mapping.approval_status
            not in {"reviewed_existing_contract", "approved_by_data_owner"}
        ):
            unapproved_hospital_mappings.append(folder)
    classification_counts = Counter(record["classification"] for record in records)
    inspection_counts = Counter(record["inspection_status"] for record in records)
    selected_static_counts = Counter(
        record["hospital_folder"]
        for record in records
        if record["classification"] == "selected_static_candidate"
    )
    identifier_disagreements = [
        record
        for record in records
        if record.get("identifiers", {}).get("filename_comparison_status")
        in IDENTIFIER_BLOCKING_STATUSES
    ]
    invalid_static_identifiers = [
        record
        for record in records
        if record["classification"] == "selected_static_candidate"
        and (
            record["inspection_status"] != "complete"
            or record.get("identifiers", {}).get("filename_comparison_status")
            != "not_applicable_static"
            or record["identifiers"]["empty_count"] != 0
            or record["identifiers"]["duplicate_nonempty_count"] != 0
            or record["identifiers"]["nonempty_count"]
            != record["csv"]["data_record_count"]
        )
    ]
    dynamic_without_time_evidence = [
        record
        for record in records
        if record["classification"] == "dynamic_candidate"
        and record["inspection_status"] == "complete"
        and not record["identifiers"]["time_columns_present"]
    ]
    invalid_csv_records = [
        record
        for record in records
        if record["classification"] in CSV_CLASSIFICATIONS
        and record["inspection_status"] != "complete"
    ]
    reviewed_duplicate_header_records = [
        record for record in records if "reviewed_duplicate_header" in record
    ]
    provisional_archive_records = [
        record
        for record in records
        if record["classification"] == "provisionally_excluded_archive"
    ]
    symbolic_link_records = [
        record for record in records if record["is_symbolic_link"]
    ]
    additional_unresolved = [
        item
        for item in static_comparisons
        if item["classification"] != "exact_byte_duplicate"
    ]
    checks = [
        _check(
            "hospital_folder_set",
            observed == expected,
            {
                "observed": sorted(observed),
                "missing": sorted(expected - observed),
                "unexpected": sorted(observed - expected),
            },
            sorted(expected),
            "The observed top-level hospital set must exactly match the inventory policy.",
        ),
        _check(
            "hospital_mapping_contract_approved",
            not unapproved_hospital_mappings,
            unapproved_hospital_mappings,
            [],
            "Every observed raw folder must have an approved canonical hospital ID and cohort inclusion decision.",
        ),
        _check(
            "one_default_static_per_hospital",
            all(selected_static_counts[folder] == 1 for folder in observed),
            dict(sorted(selected_static_counts.items())),
            {folder: 1 for folder in sorted(observed)},
            "Each observed hospital requires exactly one default static.csv source.",
        ),
        _check(
            "unknown_or_unclassified_files",
            classification_counts["unknown_file"] == 0
            and classification_counts["unclassified_data_file"] == 0
            and classification_counts["archive_candidate"] == 0,
            {
                "unknown_file_count": classification_counts["unknown_file"],
                "unclassified_data_file_count": classification_counts[
                    "unclassified_data_file"
                ],
                "unreviewed_archive_candidate_count": classification_counts[
                    "archive_candidate"
                ],
            },
            {
                "unknown_file_count": 0,
                "unclassified_data_file_count": 0,
                "unreviewed_archive_candidate_count": 0,
            },
            "Unknown and unclassified raw files require human classification.",
        ),
        _check(
            "provisional_archive_owner_confirmation",
            not provisional_archive_records,
            len(provisional_archive_records),
            0,
            "The reviewed ZIP is excluded from ingestion provisionally but remains blocking until its owner confirms the legacy-snapshot role.",
        ),
        _check(
            "unknown_directories",
            not discovery.unknown_directories,
            len(discovery.unknown_directories),
            0,
            "Unexpected directories are not traversed and require human classification.",
        ),
        _check(
            "symbolic_link_sources",
            not symbolic_link_records,
            len(symbolic_link_records),
            0,
            "Symbolic-link source files are not followed and require explicit review.",
        ),
        _check(
            "checkpoint_directories_excluded",
            True,
            len(discovery.checkpoint_directories_pruned),
            "all occurrences pruned before enumeration",
            "Checkpoint directory contents were neither enumerated nor hashed.",
        ),
        _check(
            "untrusted_pooled_directory_excluded",
            True,
            len(discovery.untrusted_pooled_directories_pruned),
            "root pooled directory pruned before enumeration",
            "The lossy pooled directory is excluded from authoritative raw discovery.",
        ),
        _check(
            "csv_encoding_dialect_and_structure",
            not invalid_csv_records,
            len(invalid_csv_records),
            0,
            "Every classified raw CSV must have a resolved encoding, delimiter, and valid rectangular structure.",
        ),
        _check(
            "filename_and_in_file_identifiers_agree",
            not identifier_disagreements,
            len(identifier_disagreements),
            0,
            "Available in-file identifiers must agree with filename-derived identifiers.",
        ),
        _check(
            "static_identifier_integrity",
            not invalid_static_identifiers,
            len(invalid_static_identifiers),
            0,
            "Each default static source requires one non-empty, unique stay identifier per row.",
        ),
        _check(
            "dynamic_role_time_evidence",
            not dynamic_without_time_evidence,
            len(dynamic_without_time_evidence),
            0,
            "Each file classified by a dynamic filename pattern must expose a configured time column; otherwise its file role requires review.",
        ),
        _check(
            "dynamic_duplicate_stays_resolved",
            not dynamic_duplicates,
            len(dynamic_duplicates),
            0,
            "Every repeated hospital/stay filename identity requires explicit resolution, including exact duplicates.",
        ),
        _check(
            "additional_static_candidates_resolved",
            not additional_unresolved,
            Counter(item["classification"] for item in static_comparisons),
            {"non_exact_duplicate_count": 0},
            "Configured additional static candidates are accepted only when proven exact byte duplicates; all other relationships block.",
        ),
    ]
    status = overall_status(checks)
    blocking_findings = tuple(
        {
            "check": check.name,
            "observed": _jsonable(check.observed),
            "expected": _jsonable(check.expected),
            "details": check.details,
        }
        for check in checks
        if check.status != "pass" and check.severity == "blocking"
    )

    row_counts_by_hospital: Counter[str] = Counter()
    for record in records:
        if record["inspection_status"] == "complete":
            row_counts_by_hospital[str(record["hospital_folder"])] += record["csv"][
                "data_record_count"
            ]
    review_variants = [
        {
            **{
                key: value
                for key, value in variant.items()
                if key not in {"file_ids", "header", "schema_variant_id"}
            },
            "review_variant_key": f"variant_{index:04d}",
            "exact_header_available_in_private_bundle": True,
        }
        for index, variant in enumerate(variants, start=1)
    ]
    review_static = []
    for item in static_comparisons:
        sanitized = {
            key: value
            for key, value in item.items()
            if key
            not in {
                "selected_file_id",
                "additional_file_id",
                "left_stay_set_digest",
                "right_stay_set_digest",
                "left_only_columns",
                "right_only_columns",
            }
        }
        sanitized["left_only_column_count"] = len(item.get("left_only_columns", ()))
        sanitized["right_only_column_count"] = len(
            item.get("right_only_columns", ())
        )
        review_static.append(sanitized)
    review_mapping = {
        folder: {
            "mapping_status": value["mapping_status"],
            "canonical_hospital_id": value["canonical_hospital_id"],
            "cohort_action": value["cohort_action"],
            "mapping_approval_evidence": value["mapping_approval_evidence"],
            "raw_static_unique_stays": value["raw_static_unique_stays"],
            "raw_static_file_count": value["raw_static_file_count"],
            "raw_dynamic_file_count": value["raw_dynamic_file_count"],
            "raw_dynamic_row_count": value["raw_dynamic_row_count"],
            "dynamic_identifier_status_counts": value[
                "dynamic_identifier_status_counts"
            ],
            "dynamic_schema_variant_count": value[
                "dynamic_schema_variant_count"
            ],
            "decimal_comma_candidate_total": sum(
                value["decimal_comma_candidate_counts"].values()
            ),
            "columns_with_decimal_comma_candidates": len(
                value["decimal_comma_candidate_counts"]
            ),
            "raw_static_stay_overlap_by_other_folder": value[
                "raw_static_stay_overlap_by_other_folder"
            ],
            "hospital_identity_summary": value["hospital_identity_summary"],
            "pooled_comparison": value["pooled_comparison"],
            "required_human_evidence": value["required_human_evidence"],
            "remaining_non_mapping_reviews": value[
                "remaining_non_mapping_reviews"
            ],
        }
        for folder, value in mapping_evidence.items()
    }
    review_payload = {
        "artifact": "asic_v3_raw_inventory_review",
        "artifact_version": policy.inventory_manifest_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "overall_status": status,
        "authoritative_input_boundary": "raw_hospital_csv_files",
        "old_pooled_role": "untrusted_read_only_comparison_only",
        "metrics": {
            "observed_hospital_folders": sorted(observed),
            "approved_hospital_mapping_count": len(policy.hospital_mappings),
            "classified_file_count": len(records),
            "classification_counts": dict(sorted(classification_counts.items())),
            "inspection_status_counts": dict(sorted(inspection_counts.items())),
            "checkpoint_directory_count_excluded": len(
                discovery.checkpoint_directories_pruned
            ),
            "untrusted_pooled_directory_count_excluded": len(
                discovery.untrusted_pooled_directories_pruned
            ),
            "unknown_directory_count": len(discovery.unknown_directories),
            "source_row_counts_by_hospital": dict(sorted(row_counts_by_hospital.items())),
            "schema_variant_count": len(variants),
            "reviewed_duplicate_header_file_count": len(
                reviewed_duplicate_header_records
            ),
            "provisionally_excluded_archive_count": len(
                provisional_archive_records
            ),
            "dynamic_duplicate_group_counts": dict(
                sorted(
                    Counter(
                        item["classification"] for item in dynamic_duplicates
                    ).items()
                )
            ),
        },
        "schema_variants": review_variants,
        "static_candidate_comparisons": review_static,
        "hospital_mapping_evidence": review_mapping,
        "checks": [_jsonable(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_row_level_values": False,
            "protected_evidence_location": "owner-only private report bundle on the authorized cluster",
        },
        "limitations": [
            "The folder-to-hospital and cohort-inclusion contract is approved, including folder 01 as asic_UK01.",
            "Delimiter and encoding detection records evidence; it does not parse clinical values.",
            "Old pooled comparisons cannot reconstruct strings lost before v2.",
            "No production artifacts are generated by this command.",
            "The reviewed ZIP is provisionally excluded from ingestion but remains pending data-owner confirmation.",
        ],
    }
    assert_review_payload_is_safe(review_payload)

    manifest = {
        "artifact": "asic_v3_raw_inventory_private",
        "artifact_version": policy.inventory_manifest_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": review_payload["generated_at_utc"],
        "overall_status": status,
        "input_root": str(root),
        "policy_path": str(policy.source_path),
        "policy_version": policy.policy_version,
        "file_count": len(records),
        "reviewed_duplicate_header_file_count": len(
            reviewed_duplicate_header_records
        ),
        "provisional_archive_exclusions": [
            {
                "file_id": record["file_id"],
                "relative_path": record["relative_path"],
                "source_filename": record["source_filename"],
                "sha256": record["sha256"],
                "size_bytes": record["size_bytes"],
                "resolution": record["archive_resolution"],
            }
            for record in provisional_archive_records
        ],
        "observed_hospital_folders": sorted(observed),
        "hospital_mapping_contract": [
            {
                "source_folder": mapping.source_folder,
                "canonical_hospital_id": mapping.canonical_hospital_id,
                "approval_status": mapping.approval_status,
                "cohort_action": mapping.cohort_action,
                "evidence": mapping.evidence,
            }
            for mapping in policy.hospital_mappings
        ],
        "checkpoint_directories_pruned": list(
            discovery.checkpoint_directories_pruned
        ),
        "untrusted_pooled_directories_pruned": list(
            discovery.untrusted_pooled_directories_pruned
        ),
        "unknown_directories": list(discovery.unknown_directories),
        "file_and_row_conservation": {
            "discovered_file_count": len(discovery.files),
            "evidence_record_count": len(records),
            "all_discovered_files_have_one_evidence_record": len(discovery.files)
            == len(records),
            "classified_source_row_counts_by_hospital": dict(
                sorted(row_counts_by_hospital.items())
            ),
        },
        "blocking_findings": list(blocking_findings),
    }
    return InventoryRunResult(
        dataset_context=config.dataset_context,
        overall_status=status,
        blocking_findings=blocking_findings,
        private_manifest=manifest,
        review_payload=review_payload,
        private_tables={
            "files": tuple(records),
            "schema_variants": tuple(variants),
            "static_candidates": tuple(static_comparisons),
            "dynamic_stays": tuple(dynamic_duplicates),
            "hospital_mapping_evidence": tuple(
                {"hospital_folder": folder, **value}
                for folder, value in sorted(mapping_evidence.items())
            ),
        },
    )


def _write_private_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(_jsonable(value), stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _write_private_parquet(path: Path, rows: tuple[dict[str, Any], ...]) -> None:
    all_keys = sorted({key for row in rows for key in row})
    normalized = []
    for row in rows:
        normalized_row: dict[str, Any] = {}
        for key in all_keys:
            value = row.get(key)
            normalized_row[key] = (
                json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
                if isinstance(value, (dict, list, tuple, Counter))
                else value
            )
        normalized.append(normalized_row)
    table = pa.Table.from_pylist(normalized) if normalized else pa.table({"empty": []})
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    pq.write_table(table, temporary)
    temporary.chmod(0o600)
    temporary.replace(path)
    path.chmod(0o600)


def _review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 raw inventory review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Authoritative boundary: `{payload['authoritative_input_boundary']}`",
        "- Protected filenames and stay identifiers: excluded from this report",
        "",
        "## Blocking findings",
        "",
    ]
    findings = payload["blocking_findings"]
    if findings:
        for finding in findings:
            lines.append(f"- `{finding['check']}`: {finding['details']}")
    else:
        lines.append("- None.")
    lines.extend(["", "## Inventory totals", ""])
    for key, value in payload["metrics"].items():
        if isinstance(value, (str, int)):
            lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Lossless ingestion may carry only an exception explicitly approved in the "
        "ingestion policy. Ingested artifacts remain non-publishable until every "
        "blocking finding and downstream review gate is resolved."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def _write_review_file(path: Path, content: str, mode: int = 0o640) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def default_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def write_inventory_report_bundle(
    result: InventoryRunResult,
    reports_root: Path,
    run_id: str | None = None,
) -> InventoryRunResult:
    selected_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run_id):
        raise InventoryError(
            "run_id must contain only letters, digits, underscores, and hyphens"
        )
    private_directory = reports_root / "private" / "raw_inventory" / selected_run_id
    review_directory = reports_root / "review" / "raw_inventory"
    review_json = review_directory / f"{selected_run_id}.json"
    review_markdown = review_directory / f"{selected_run_id}.md"
    if private_directory.exists() or review_json.exists() or review_markdown.exists():
        raise InventoryError(
            f"Inventory report run already exists and will not be overwritten: {selected_run_id}"
        )
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.chmod(0o700)
    review_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    try:
        _write_private_json(
            private_directory / "inventory_manifest.json", result.private_manifest
        )
        _write_private_json(
            private_directory / "schema_variants.json",
            result.private_tables["schema_variants"],
        )
        _write_private_json(
            private_directory / "hospital_mapping_evidence.json",
            result.private_tables["hospital_mapping_evidence"],
        )
        _write_private_json(
            private_directory / "blocking_findings.json", result.blocking_findings
        )
        _write_private_parquet(
            private_directory / "files.parquet", result.private_tables["files"]
        )
        _write_private_parquet(
            private_directory / "static_candidates.parquet",
            result.private_tables["static_candidates"],
        )
        _write_private_parquet(
            private_directory / "dynamic_stays.parquet",
            result.private_tables["dynamic_stays"],
        )
        assert_review_payload_is_safe(result.review_payload)
        _write_review_file(
            review_json,
            json.dumps(result.review_payload, ensure_ascii=False, indent=2) + "\n",
        )
        _write_review_file(
            review_markdown, _review_markdown(result.review_payload)
        )
    except Exception:
        # Preserve any partial owner-only evidence for forensic inspection; the
        # next run must use a new run ID and cannot overwrite it.
        raise
    return InventoryRunResult(
        **{
            **asdict(result),
            "private_report_directory": private_directory,
            "review_json_path": review_json,
            "review_markdown_path": review_markdown,
        }
    )
