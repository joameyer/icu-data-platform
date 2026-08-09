from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import stat
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.errors import IngestionError
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.ingestion.pipeline import (
    _decode_mapping,
    _optional_mapping,
    _plan_raw_output_columns,
    _table_schema,
    _union_columns,
)
from asic_pipeline.ingestion.policy import IngestionPolicy, load_ingestion_policy
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import InventoryPolicy, load_inventory_policy
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


AUDIT_ARTIFACT_VERSION = "0.1"
EXPECTED_TABLE_FILES = frozenset(
    {"static.parquet", "dynamic.parquet", "ingestion_manifest.json"}
)
EXPECTED_DOWNSTREAM_GATES = (
    "numeric_token_parsing_policy_not_reviewed",
    "raw_to_translated_column_registry_not_reviewed",
    "translated_union_schema_not_approved",
)
CHECK_ORDER = (
    "complete_hospital_output_set",
    "v3_output_paths_and_permissions",
    "ingestion_manifest_contract",
    "inventory_manifest_linkage",
    "selected_and_excluded_file_conservation",
    "parquet_integrity_and_manifest_hash",
    "parquet_standard_reader_compatibility",
    "parquet_schema_and_raw_string_contract",
    "row_and_file_conservation",
    "provenance_complete_and_deterministic",
    "identifier_agreement",
    "reviewed_duplicate_header_storage",
    "publication_state_fail_closed",
    "provisional_archive_owner_confirmation",
)
CHECK_DETAILS = {
    "complete_hospital_output_set": (
        "Every approved hospital must have exactly one complete ingestion directory."
    ),
    "v3_output_paths_and_permissions": (
        "Ingested files must be non-symlink v3 artifacts with owner-only permissions."
    ),
    "ingestion_manifest_contract": (
        "Every ingestion manifest must satisfy the approved artifact 0.2 contract."
    ),
    "inventory_manifest_linkage": (
        "Every hospital artifact must link to the same immutable inventory evidence."
    ),
    "selected_and_excluded_file_conservation": (
        "Selected and excluded inventory records must be accounted for exactly once."
    ),
    "parquet_integrity_and_manifest_hash": (
        "Every Parquet file must be readable and match its recorded SHA-256 digest."
    ),
    "parquet_standard_reader_compatibility": (
        "Every Parquet file must be resolvable through the standard Arrow dataset reader."
    ),
    "parquet_schema_and_raw_string_contract": (
        "Physical schemas must have unique names and preserve the planned raw-string contract."
    ),
    "row_and_file_conservation": (
        "Inventory source files and rows must equal manifest and Parquet output counts."
    ),
    "provenance_complete_and_deterministic": (
        "Streamed provenance must preserve deterministic file, row, and source order."
    ),
    "identifier_agreement": (
        "Filename-derived and available in-file identifiers must agree without logging values."
    ),
    "reviewed_duplicate_header_storage": (
        "The reviewed UK00 duplicate occurrence must use the two approved physical fields with raw metadata."
    ),
    "publication_state_fail_closed": (
        "Every ingestion artifact must remain explicitly non-publishable with downstream gates."
    ),
    "provisional_archive_owner_confirmation": (
        "The provisionally excluded archive remains blocking until its owner confirms the legacy-snapshot role."
    ),
}


@dataclass(frozen=True)
class IngestionAuditResult:
    dataset_context: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_failures: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def publication_blocked(self) -> bool:
        return self.overall_status == "fail"


@dataclass(frozen=True)
class _InventoryEvidence:
    directory: Path
    manifest: dict[str, Any]
    records: tuple[dict[str, Any], ...]
    carried_blockers: tuple[str, ...]


class _Failures:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.hospitals: dict[str, set[str]] = defaultdict(set)
        self.details: list[dict[str, Any]] = []

    def add(
        self,
        check: str,
        detail: str,
        *,
        hospital: str | None = None,
        table: str | None = None,
    ) -> None:
        if check not in CHECK_ORDER:
            raise AssertionError(f"Unknown ingestion-audit check: {check}")
        self.counts[check] += 1
        if hospital is not None:
            self.hospitals[check].add(hospital)
        if len(self.details) < 1000:
            record: dict[str, Any] = {"check": check, "detail": detail}
            if hospital is not None:
                record["hospital"] = hospital
            if table is not None:
                record["table"] = table
            self.details.append(record)

    def checks(self, carried_blockers: tuple[str, ...]) -> list[CheckResult]:
        results: list[CheckResult] = []
        for name in CHECK_ORDER:
            failures = self.counts[name]
            if name == "provisional_archive_owner_confirmation":
                pending = name in carried_blockers
                results.append(
                    CheckResult(
                        name=name,
                        status="fail" if pending else "pass",
                        severity="blocking",
                        observed="pending" if pending else "resolved_or_not_applicable",
                        expected="resolved_or_not_applicable",
                        details=CHECK_DETAILS[name],
                    )
                )
                continue
            results.append(
                CheckResult(
                    name=name,
                    status="pass" if failures == 0 else "fail",
                    severity="blocking",
                    observed={
                        "failure_count": failures,
                        "affected_hospital_count": len(self.hospitals[name]),
                    },
                    expected={"failure_count": 0},
                    details=CHECK_DETAILS[name],
                )
            )
        return results


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise IngestionError(f"{label} cannot be read: {path}") from exc
    if not isinstance(value, dict):
        raise IngestionError(f"{label} must contain a JSON object: {path}")
    return value


def _load_inventory_evidence(
    config: IngestionConfig,
    policy: IngestionPolicy,
    inventory_run_id: str,
) -> _InventoryEvidence:
    if not RUN_ID_PATTERN.fullmatch(inventory_run_id):
        raise IngestionError("Invalid inventory run ID")
    directory = (
        config.inventory.paths.reports
        / "private"
        / "raw_inventory"
        / inventory_run_id
    )
    manifest_path = directory / "inventory_manifest.json"
    files_path = directory / "files.parquet"
    if not manifest_path.is_file() or not files_path.is_file():
        raise IngestionError(f"Private inventory evidence is incomplete: {directory}")
    manifest = _load_json(manifest_path, "Private inventory manifest")
    if manifest.get("artifact") != "asic_v3_raw_inventory_private":
        raise IngestionError("Inventory artifact identity is invalid")
    if manifest.get("artifact_version") != policy.required_inventory_artifact_version:
        raise IngestionError(
            "Ingestion audit requires inventory artifact version "
            f"{policy.required_inventory_artifact_version}"
        )
    if manifest.get("dataset_context") != config.dataset_context:
        raise IngestionError("Inventory and ingestion-audit contexts differ")
    if Path(str(manifest.get("input_root"))).resolve() != (
        config.inventory.paths.raw_root.resolve()
    ):
        raise IngestionError("Inventory raw root differs from the ingestion config")
    findings = manifest.get("blocking_findings")
    if not isinstance(findings, list):
        raise IngestionError("Inventory blocking findings are invalid")
    blockers = tuple(
        sorted(
            finding["check"]
            for finding in findings
            if isinstance(finding, dict) and isinstance(finding.get("check"), str)
        )
    )
    if len(blockers) != len(findings):
        raise IngestionError("Every inventory blocker must have a check name")
    unexpected = sorted(set(blockers) - set(policy.allowed_inventory_blockers))
    if unexpected:
        raise IngestionError(
            f"Inventory has blockers not approved for ingestion audit: {unexpected}"
        )
    try:
        table = pq.read_table(files_path)
    except (OSError, pa.ArrowException) as exc:
        raise IngestionError("Private inventory file evidence cannot be read") from exc
    required = {
        "file_id",
        "hospital_folder",
        "classification",
        "source_filename",
        "relative_path",
        "sha256",
        "csv",
        "encoding",
        "delimiter",
        "identifiers",
    }
    missing = required - set(table.schema.names)
    if missing:
        raise IngestionError(
            f"Private inventory evidence lacks required columns: {sorted(missing)}"
        )
    records = tuple(table.to_pylist())
    if manifest.get("file_count") != len(records):
        raise IngestionError("Private inventory file-count conservation failed")
    return _InventoryEvidence(directory, manifest, records, blockers)


def _safe_mode(path: Path) -> int | None:
    try:
        return stat.S_IMODE(path.lstat().st_mode)
    except OSError:
        return None


def _expected_file_metrics(record: dict[str, Any]) -> dict[str, Any]:
    csv_evidence = _decode_mapping(record, "csv")
    encoding = _decode_mapping(record, "encoding")
    delimiter = _decode_mapping(record, "delimiter")
    identifiers = _decode_mapping(record, "identifiers")
    return {
        "file_id": record["file_id"],
        "source_filename": record["source_filename"],
        "relative_path": record["relative_path"],
        "sha256": record["sha256"],
        "encoding": encoding.get("selected_encoding"),
        "delimiter": delimiter.get("selected_delimiter"),
        "schema_variant_id": csv_evidence.get("schema_variant_id"),
        "identifier_column": identifiers.get("identifier_column"),
        "identifier_comparison_status": identifiers.get(
            "filename_comparison_status"
        ),
        "source_row_count": csv_evidence.get("data_record_count"),
        "output_row_count": csv_evidence.get("data_record_count"),
        "header": csv_evidence.get("header"),
        "filename_stay_id": record.get("filename_stay_id"),
    }


def _expected_excluded_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "file_id": record["file_id"],
        "classification": record["classification"],
        "source_filename": record["source_filename"],
        "relative_path": record["relative_path"],
        "sha256": record["sha256"],
        "source_hash_revalidated": True,
        "archive_resolution": _optional_mapping(record, "archive_resolution"),
    }


def _expected_global_archive(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "file_id": record["file_id"],
        "hospital_folder": record["hospital_folder"],
        "source_filename": record["source_filename"],
        "relative_path": record["relative_path"],
        "size_bytes": record["size_bytes"],
        "sha256": record["sha256"],
        "source_hash_revalidated": True,
        "archive_resolution": _optional_mapping(record, "archive_resolution"),
    }


def _all_equal(array: pa.Array, value: Any) -> bool:
    if len(array) == 0:
        return True
    compared = pc.fill_null(pc.equal(array, pa.scalar(value, type=array.type)), False)
    return pc.all(compared).as_py() is True


def _all_null(array: pa.Array) -> bool:
    return array.null_count == len(array)


def _column(batch: pa.RecordBatch, name: str) -> pa.Array:
    index = batch.schema.get_field_index(name)
    if index < 0:
        raise IngestionError(f"Parquet provenance field is absent: {name}")
    return batch.column(index)


def _stream_provenance(
    parquet: pq.ParquetFile,
    policy: IngestionPolicy,
    expected_records: list[dict[str, Any]],
    hospital_id: str,
    source_folder: str,
    table_name: str,
    rows_per_batch: int,
    failures: _Failures,
) -> int:
    names = policy.provenance_columns
    physical_names = list(names.values())
    expected_by_order = {
        index: _expected_file_metrics(record)
        for index, record in enumerate(expected_records, start=1)
    }
    observed_rows_by_file: Counter[int] = Counter()
    next_row_by_file: dict[int, int] = defaultdict(lambda: 2)
    expected_source_order = 1
    last_file_order: int | None = None
    static_stay_ids: set[str] = set()
    streamed_rows = 0
    identifier_failed = False
    provenance_failed = False

    try:
        batches = parquet.iter_batches(
            batch_size=rows_per_batch,
            columns=physical_names,
            use_threads=False,
        )
        for batch in batches:
            batch_rows = batch.num_rows
            if batch_rows == 0:
                continue
            streamed_rows += batch_rows
            arrays = {key: _column(batch, name) for key, name in names.items()}

            required_non_null = set(names) - {"filename_stay_id", "in_file_stay_id"}
            if table_name == "dynamic":
                required_non_null.add("filename_stay_id")
            if table_name == "static":
                required_non_null.add("in_file_stay_id")
            if any(arrays[key].null_count for key in required_non_null):
                if not provenance_failed:
                    failures.add(
                        "provenance_complete_and_deterministic",
                        "Required provenance contains nulls.",
                        hospital=hospital_id,
                        table=table_name,
                    )
                    provenance_failed = True
            if not _all_equal(arrays["source_hospital_folder"], source_folder):
                failures.add(
                    "provenance_complete_and_deterministic",
                    "Source hospital folder is not constant and inventory-mapped.",
                    hospital=hospital_id,
                    table=table_name,
                )
            if not _all_equal(arrays["canonical_hospital_id"], hospital_id):
                failures.add(
                    "provenance_complete_and_deterministic",
                    "Canonical hospital ID is not constant.",
                    hospital=hospital_id,
                    table=table_name,
                )
            if table_name == "static" and not _all_null(
                arrays["filename_stay_id"]
            ):
                failures.add(
                    "identifier_agreement",
                    "Static filename-derived identifier must be null.",
                    hospital=hospital_id,
                    table=table_name,
                )

            source_order = arrays["source_order"]
            expected_orders = pa.array(
                range(expected_source_order, expected_source_order + batch_rows),
                type=pa.int64(),
            )
            if pc.all(pc.equal(source_order, expected_orders)).as_py() is not True:
                failures.add(
                    "provenance_complete_and_deterministic",
                    "Hospital-table source order is not contiguous from one.",
                    hospital=hospital_id,
                    table=table_name,
                )
            expected_source_order += batch_rows

            file_order_array = arrays["source_file_order"]
            try:
                encoded_file_orders = pc.run_end_encode(file_order_array)
                ends = [int(value) for value in encoded_file_orders.run_ends.to_pylist()]
                starts = [0, *ends[:-1]]
                order_values = encoded_file_orders.values.to_pylist()
            except (pa.ArrowException, TypeError, ValueError) as exc:
                raise IngestionError("File-order provenance cannot be inspected") from exc
            for start, end, order_value in zip(
                starts, ends, order_values, strict=True
            ):
                length = end - start
                if not isinstance(order_value, int):
                    failures.add(
                        "provenance_complete_and_deterministic",
                        "Source file order is not an integer.",
                        hospital=hospital_id,
                        table=table_name,
                    )
                    continue
                if last_file_order is None:
                    if order_value != 1:
                        failures.add(
                            "provenance_complete_and_deterministic",
                            "Source file order does not start at one.",
                            hospital=hospital_id,
                            table=table_name,
                        )
                elif order_value not in {last_file_order, last_file_order + 1}:
                    failures.add(
                        "provenance_complete_and_deterministic",
                        "Source file order is not deterministic and contiguous.",
                        hospital=hospital_id,
                        table=table_name,
                    )
                last_file_order = order_value
                expected = expected_by_order.get(order_value)
                if expected is None:
                    failures.add(
                        "row_and_file_conservation",
                        "Parquet provenance references an unexpected source-file order.",
                        hospital=hospital_id,
                        table=table_name,
                    )
                    continue
                observed_rows_by_file[order_value] += length
                for key, expected_key in (
                    ("source_filename", "source_filename"),
                    ("source_file_id", "file_id"),
                    ("source_schema_variant_id", "schema_variant_id"),
                    ("filename_stay_id", "filename_stay_id"),
                ):
                    part = arrays[key].slice(start, length)
                    value = expected[expected_key]
                    matches = _all_null(part) if value is None else _all_equal(part, value)
                    if not matches:
                        failures.add(
                            "provenance_complete_and_deterministic",
                            f"Provenance field {key} differs from inventory evidence.",
                            hospital=hospital_id,
                            table=table_name,
                        )
                row_numbers = arrays["source_row_number"].slice(start, length)
                first_expected = next_row_by_file[order_value]
                expected_rows = pa.array(
                    range(first_expected, first_expected + length),
                    type=pa.int64(),
                )
                if pc.all(pc.equal(row_numbers, expected_rows)).as_py() is not True:
                    failures.add(
                        "provenance_complete_and_deterministic",
                        "Source row numbers are not contiguous from CSV record two.",
                        hospital=hospital_id,
                        table=table_name,
                    )
                next_row_by_file[order_value] += length

            if table_name == "static":
                for value in arrays["in_file_stay_id"].to_pylist():
                    if not isinstance(value, str) or not value or value in static_stay_ids:
                        if not identifier_failed:
                            failures.add(
                                "identifier_agreement",
                                "Static identifiers are empty, non-string, or duplicated.",
                                hospital=hospital_id,
                                table=table_name,
                            )
                            identifier_failed = True
                    else:
                        static_stay_ids.add(value)
            else:
                in_file = arrays["in_file_stay_id"]
                filename = arrays["filename_stay_id"]
                equal = pc.fill_null(pc.equal(in_file, filename), True)
                empty = pc.fill_null(pc.equal(in_file, ""), False)
                if pc.all(pc.or_(equal, empty)).as_py() is not True:
                    if not identifier_failed:
                        failures.add(
                            "identifier_agreement",
                            "Available dynamic in-file identifiers disagree with filename-derived identifiers.",
                            hospital=hospital_id,
                            table=table_name,
                        )
                        identifier_failed = True
    except (OSError, pa.ArrowException, IngestionError) as exc:
        failures.add(
            "provenance_complete_and_deterministic",
            f"Provenance stream failed: {type(exc).__name__}: {exc}",
            hospital=hospital_id,
            table=table_name,
        )
        return streamed_rows

    expected_counts = {
        index: int(_decode_mapping(record, "csv")["data_record_count"])
        for index, record in enumerate(expected_records, start=1)
    }
    if dict(observed_rows_by_file) != expected_counts:
        failures.add(
            "row_and_file_conservation",
            "Per-file Parquet row counts differ from inventory evidence.",
            hospital=hospital_id,
            table=table_name,
        )
    return streamed_rows


def _manifest_contract_ok(
    manifest: dict[str, Any],
    config: IngestionConfig,
    inventory: _InventoryEvidence,
    ingestion_policy: IngestionPolicy,
    inventory_policy: InventoryPolicy,
    hospital_id: str,
    source_folder: str,
    expected_records: dict[str, list[dict[str, Any]]],
    hospital_records: list[dict[str, Any]],
    failures: _Failures,
) -> dict[str, tuple[Any, ...]]:
    planned: dict[str, tuple[Any, ...]] = {}
    mapping = inventory_policy.hospital_mapping_for(source_folder)
    core_expected = {
        "artifact": "asic_v3_lossless_hospital_ingestion",
        "artifact_version": ingestion_policy.ingestion_manifest_version,
        "dataset_context": config.dataset_context,
    }
    if any(manifest.get(key) != value for key, value in core_expected.items()):
        failures.add(
            "ingestion_manifest_contract",
            "Manifest identity, version, or context differs from the approved contract.",
            hospital=hospital_id,
        )
    expected_hospital = {
        "source_folder": source_folder,
        "canonical_hospital_id": hospital_id,
        "cohort_action": "include",
        "mapping_approval_status": mapping.approval_status if mapping else None,
    }
    if manifest.get("hospital") != expected_hospital:
        failures.add(
            "ingestion_manifest_contract",
            "Manifest hospital mapping differs from the approved contract.",
            hospital=hospital_id,
        )

    input_value = manifest.get("input")
    input_expected = {
        "authoritative_boundary": "raw_hospital_csv_files",
        "raw_root": str(config.inventory.paths.raw_root),
        "inventory_run_id": inventory.directory.name,
        "inventory_directory": str(inventory.directory),
        "inventory_artifact_version": inventory.manifest["artifact_version"],
        "inventory_blockers_allowed_for_ingestion": list(inventory.carried_blockers),
    }
    if input_value != input_expected:
        failures.add(
            "inventory_manifest_linkage",
            "Manifest input linkage differs from the immutable inventory run.",
            hospital=hospital_id,
        )
    expected_policies = {
        "inventory_policy_version": inventory_policy.policy_version,
        "ingestion_policy_version": ingestion_policy.policy_version,
        "ingestion_policy_path": str(ingestion_policy.source_path),
    }
    if manifest.get("policies") != expected_policies:
        failures.add(
            "ingestion_manifest_contract",
            "Manifest policy versions differ from deployed approved policies.",
            hospital=hospital_id,
        )

    expected_value_contract = {
        "raw_cells": "string",
        "literal_empty_cell": "empty_string",
        "column_absent_from_source_schema": "parquet_null",
        "clinical_numeric_inference_applied": False,
        "textual_missing_policy_applied": False,
        "clinical_parsing_applied": False,
        "clinical_semantic_translation_applied": False,
        "duplicate_physical_names_disambiguated": True,
    }
    if manifest.get("value_representation") != expected_value_contract:
        failures.add(
            "ingestion_manifest_contract",
            "Manifest raw-value representation differs from the lossless contract.",
            hospital=hospital_id,
        )

    selection = manifest.get("source_selection")
    selected_static = expected_records["static"]
    selected_dynamic = expected_records["dynamic"]
    excluded = sorted(
        (
            record
            for record in hospital_records
            if record.get("classification")
            in ingestion_policy.source_selection.excluded_classifications
        ),
        key=lambda item: str(item["file_id"]),
    )
    global_archives = sorted(
        (
            record
            for record in inventory.records
            if record.get("classification") == "provisionally_excluded_archive"
        ),
        key=lambda item: str(item["file_id"]),
    )
    checkpoint_paths = inventory.manifest.get("checkpoint_directories_pruned", [])
    pooled_paths = inventory.manifest.get("untrusted_pooled_directories_pruned", [])
    expected_selection = {
        "dynamic_filename_pattern": inventory_policy.dynamic_pattern_for(
            source_folder
        ).pattern,
        "hospital_file_classification_counts": dict(
            sorted(Counter(str(record["classification"]) for record in hospital_records).items())
        ),
        "selected_static_file_count": len(selected_static),
        "selected_dynamic_file_count": len(selected_dynamic),
        "duplicate_dynamic_stay_count": 0,
        "unknown_or_unclassified_file_count": 0,
        "checkpoint_directories_pruned": [
            path
            for path in checkpoint_paths
            if isinstance(path, str)
            and (path == source_folder or path.startswith(f"{source_folder}/"))
        ],
        "untrusted_pooled_directories_pruned": pooled_paths,
        "excluded_files": [_expected_excluded_record(record) for record in excluded],
        "global_provisional_archive_exclusions": [
            _expected_global_archive(record) for record in global_archives
        ],
    }
    if selection != expected_selection:
        failures.add(
            "selected_and_excluded_file_conservation",
            "Manifest source-selection evidence differs from inventory evidence.",
            hospital=hospital_id,
        )

    manifest_tables = manifest.get("tables")
    raw_columns = manifest.get("raw_columns")
    if not isinstance(manifest_tables, dict) or not isinstance(raw_columns, dict):
        failures.add(
            "ingestion_manifest_contract",
            "Manifest table or raw-column evidence is absent.",
            hospital=hospital_id,
        )
        return planned
    if raw_columns.get("review_status") != "pending_translation_registry_review":
        failures.add(
            "ingestion_manifest_contract",
            "Manifest raw-column review status is not pending translation review.",
            hospital=hospital_id,
        )
    for table_name in ("static", "dynamic"):
        records = expected_records[table_name]
        raw_identities = _union_columns(records)
        columns = _plan_raw_output_columns(
            source_folder, table_name, raw_identities, ingestion_policy
        )
        planned[table_name] = columns
        expected_raw_columns = [
            {
                "raw_name": item.identity.name,
                "occurrence": item.identity.occurrence,
                "physical_name": item.physical_name,
                "physical_name_rule_id": item.physical_name_rule_id,
            }
            for item in columns
        ]
        if raw_columns.get(table_name) != expected_raw_columns:
            failures.add(
                "parquet_schema_and_raw_string_contract",
                "Manifest raw-column union differs from inventory-derived planning.",
                hospital=hospital_id,
                table=table_name,
            )
        table_manifest = manifest_tables.get(table_name)
        expected_files = [_expected_file_metrics(record) for record in records]
        if not isinstance(table_manifest, dict):
            failures.add(
                "ingestion_manifest_contract",
                "Manifest table evidence is absent.",
                hospital=hospital_id,
                table=table_name,
            )
            continue
        expected_rows = sum(int(item["source_row_count"]) for item in expected_files)
        if table_manifest.get("files") != expected_files:
            failures.add(
                "selected_and_excluded_file_conservation",
                "Per-file manifest evidence differs from inventory evidence.",
                hospital=hospital_id,
                table=table_name,
            )
        expected_scalars = {
            "source_file_count": len(records),
            "source_row_count": expected_rows,
            "output_row_count": expected_rows,
            "output_column_count": len(columns)
            + len(ingestion_policy.provenance_columns),
            "raw_column_count": len(columns),
            "schema_variant_count": len(
                {
                    _decode_mapping(record, "csv")["schema_variant_id"]
                    for record in records
                }
            ),
            "identifier_mismatch_count": 0,
        }
        if any(table_manifest.get(key) != value for key, value in expected_scalars.items()):
            failures.add(
                "row_and_file_conservation",
                "Manifest table counts differ from inventory-derived counts.",
                hospital=hospital_id,
                table=table_name,
            )
        for count_name in (
            "literal_empty_cell_count",
            "absent_source_column_null_count",
        ):
            count_value = table_manifest.get(count_name)
            if (
                not isinstance(count_value, int)
                or isinstance(count_value, bool)
                or count_value < 0
            ):
                failures.add(
                    "ingestion_manifest_contract",
                    f"Manifest {count_name} is not a non-negative integer.",
                    hospital=hospital_id,
                    table=table_name,
                )

    checks = manifest.get("checks")
    if not isinstance(checks, dict) or not checks or any(value is not True for value in checks.values()):
        failures.add(
            "ingestion_manifest_contract",
            "Ingestion-time manifest checks are absent or not all true.",
            hospital=hospital_id,
        )
    publication = manifest.get("publication")
    expected_publication = {
        "ready": False,
        "carried_inventory_blockers": list(inventory.carried_blockers),
        "pending_downstream_gates": list(EXPECTED_DOWNSTREAM_GATES),
    }
    if publication != expected_publication or manifest.get("audit_status") != "pass":
        failures.add(
            "publication_state_fail_closed",
            "Manifest publication state or ingestion-time audit status is invalid.",
            hospital=hospital_id,
        )
    return planned


def build_ingestion_audit(
    config: IngestionConfig,
    inventory_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> IngestionAuditResult:
    reporter = progress or (lambda _: None)
    ingestion_policy = load_ingestion_policy(config.policy_path)
    inventory_policy = load_inventory_policy(config.inventory.policy_path)
    inventory = _load_inventory_evidence(config, ingestion_policy, inventory_run_id)
    failures = _Failures()
    mappings = tuple(
        mapping
        for mapping in inventory_policy.hospital_mappings
        if mapping.cohort_action == "include"
    )
    expected_hospitals = {mapping.canonical_hospital_id for mapping in mappings}
    manifest_mappings = inventory.manifest.get("hospital_mapping_contract")
    expected_mapping_contract = [
        {
            "source_folder": mapping.source_folder,
            "canonical_hospital_id": mapping.canonical_hospital_id,
            "approval_status": mapping.approval_status,
            "cohort_action": mapping.cohort_action,
            "evidence": mapping.evidence,
        }
        for mapping in inventory_policy.hospital_mappings
    ]
    if manifest_mappings != expected_mapping_contract:
        failures.add(
            "inventory_manifest_linkage",
            "Inventory hospital mapping contract differs from the deployed policy.",
        )
    if inventory.manifest.get("policy_version") != inventory_policy.policy_version:
        failures.add(
            "inventory_manifest_linkage",
            "Inventory artifact policy version differs from the deployed policy.",
        )

    if config.output_root.is_symlink():
        failures.add(
            "v3_output_paths_and_permissions",
            f"Ingested output root is a symbolic link: {config.output_root}",
        )
    if not config.output_root.is_dir():
        failures.add(
            "complete_hospital_output_set",
            f"Ingested output root is unavailable: {config.output_root}",
        )
        actual_entries: list[Path] = []
    else:
        actual_entries = list(config.output_root.iterdir())
        if _safe_mode(config.output_root) != 0o700:
            failures.add(
                "v3_output_paths_and_permissions",
                f"Ingested output root mode is {_safe_mode(config.output_root)!r}.",
            )
    actual_names = {entry.name for entry in actual_entries}
    if actual_names != expected_hospitals:
        failures.add(
            "complete_hospital_output_set",
            "Observed hospital output entries differ from the approved set: "
            f"{sorted(actual_names)}",
        )

    records_by_folder: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in inventory.records:
        folder = record.get("hospital_folder")
        if isinstance(folder, str):
            records_by_folder[folder].append(record)
    hospital_summaries: dict[str, dict[str, Any]] = {}
    private_hospital_evidence: dict[str, Any] = {}
    total_rows = {"static": 0, "dynamic": 0}

    for mapping in mappings:
        hospital_id = mapping.canonical_hospital_id
        source_folder = mapping.source_folder
        reporter(f"audit_start hospital={hospital_id}")
        directory = config.output_root / hospital_id
        hospital_records = records_by_folder[source_folder]
        expected_records = {
            "static": sorted(
                (
                    record
                    for record in hospital_records
                    if record.get("classification")
                    == ingestion_policy.source_selection.static_classification
                ),
                key=lambda item: str(item["file_id"]),
            ),
            "dynamic": sorted(
                (
                    record
                    for record in hospital_records
                    if record.get("classification")
                    == ingestion_policy.source_selection.dynamic_classification
                ),
                key=lambda item: str(item["file_id"]),
            ),
        }
        for table_name in ("static", "dynamic"):
            total_rows[table_name] += sum(
                int(_decode_mapping(record, "csv")["data_record_count"])
                for record in expected_records[table_name]
            )
        if not directory.is_dir() or directory.is_symlink():
            failures.add(
                "complete_hospital_output_set",
                f"Hospital output directory is missing or a symlink: {directory}",
                hospital=hospital_id,
            )
            continue
        if _safe_mode(directory) != 0o700:
            failures.add(
                "v3_output_paths_and_permissions",
                f"Hospital directory mode is {_safe_mode(directory)!r}.",
                hospital=hospital_id,
            )
        entries = list(directory.iterdir())
        if {entry.name for entry in entries} != EXPECTED_TABLE_FILES:
            failures.add(
                "complete_hospital_output_set",
                f"Hospital directory entries differ from contract: {[entry.name for entry in entries]}",
                hospital=hospital_id,
            )
        for entry in entries:
            if entry.is_symlink() or not entry.is_file() or _safe_mode(entry) != 0o600:
                failures.add(
                    "v3_output_paths_and_permissions",
                    f"Output file type or mode is invalid: {entry}",
                    hospital=hospital_id,
                )

        manifest_path = directory / "ingestion_manifest.json"
        try:
            manifest = _load_json(manifest_path, "Ingestion manifest")
        except IngestionError as exc:
            failures.add(
                "ingestion_manifest_contract",
                str(exc),
                hospital=hospital_id,
            )
            continue
        planned = _manifest_contract_ok(
            manifest,
            config,
            inventory,
            ingestion_policy,
            inventory_policy,
            hospital_id,
            source_folder,
            expected_records,
            hospital_records,
            failures,
        )
        hospital_summary: dict[str, Any] = {}
        private_table_evidence: dict[str, Any] = {}
        outputs = manifest.get("outputs")
        if not isinstance(outputs, dict):
            outputs = {}

        for table_name in ("static", "dynamic"):
            path = directory / f"{table_name}.parquet"
            records = expected_records[table_name]
            expected_rows = sum(
                int(_decode_mapping(record, "csv")["data_record_count"])
                for record in records
            )
            summary = {
                "source_file_count": len(records),
                "expected_row_count": expected_rows,
                "parquet_row_count": None,
                "physical_column_count": None,
                "raw_column_count": None,
                "schema_variant_count": len(
                    {
                        _decode_mapping(record, "csv")["schema_variant_id"]
                        for record in records
                    }
                ),
                "output_hash_matches_manifest": False,
                "provenance_rows_streamed": 0,
            }
            hospital_summary[table_name] = summary
            if not path.is_file() or path.is_symlink():
                failures.add(
                    "parquet_integrity_and_manifest_hash",
                    f"Parquet output is missing or a symlink: {path}",
                    hospital=hospital_id,
                    table=table_name,
                )
                continue
            reporter(f"audit_hash_start hospital={hospital_id} table={table_name}")
            try:
                observed_hash = sha256_file(path)
            except OSError as exc:
                failures.add(
                    "parquet_integrity_and_manifest_hash",
                    f"Parquet SHA-256 cannot be computed: {type(exc).__name__}: {exc}",
                    hospital=hospital_id,
                    table=table_name,
                )
                continue
            output_manifest = outputs.get(table_name)
            expected_hash = (
                output_manifest.get("sha256")
                if isinstance(output_manifest, dict)
                else None
            )
            expected_filename = (
                output_manifest.get("filename")
                if isinstance(output_manifest, dict)
                else None
            )
            hash_matches = observed_hash == expected_hash and expected_filename == path.name
            summary["output_hash_matches_manifest"] = hash_matches
            if not hash_matches:
                failures.add(
                    "parquet_integrity_and_manifest_hash",
                    "Parquet filename or SHA-256 differs from its manifest.",
                    hospital=hospital_id,
                    table=table_name,
                )
            try:
                parquet = pq.ParquetFile(path)
                schema = parquet.schema_arrow
            except (OSError, pa.ArrowException) as exc:
                failures.add(
                    "parquet_integrity_and_manifest_hash",
                    f"Parquet metadata cannot be read: {type(exc).__name__}: {exc}",
                    hospital=hospital_id,
                    table=table_name,
                )
                continue
            summary["parquet_row_count"] = parquet.metadata.num_rows
            summary["physical_column_count"] = len(schema)
            summary["raw_column_count"] = len(schema) - len(
                ingestion_policy.provenance_columns
            )
            if parquet.metadata.num_rows != expected_rows:
                failures.add(
                    "row_and_file_conservation",
                    "Parquet metadata row count differs from inventory evidence.",
                    hospital=hospital_id,
                    table=table_name,
                )
            try:
                dataset = ds.dataset(path, format="parquet")
                first_provenance = next(iter(ingestion_policy.provenance_columns.values()))
                dataset.scanner(columns=[first_provenance], batch_size=1).head(1)
            except (OSError, pa.ArrowException) as exc:
                failures.add(
                    "parquet_standard_reader_compatibility",
                    f"Arrow dataset reader cannot resolve the artifact: {type(exc).__name__}: {exc}",
                    hospital=hospital_id,
                    table=table_name,
                )

            columns = planned.get(table_name)
            if columns is None:
                failures.add(
                    "parquet_schema_and_raw_string_contract",
                    "Expected schema could not be planned from manifest evidence.",
                    hospital=hospital_id,
                    table=table_name,
                )
            else:
                expected_schema = _table_schema(table_name, columns, ingestion_policy)
                if schema != expected_schema or len(schema.names) != len(set(schema.names)):
                    failures.add(
                        "parquet_schema_and_raw_string_contract",
                        "Parquet schema differs from the inventory-derived raw-string schema.",
                        hospital=hospital_id,
                        table=table_name,
                    )
            if hospital_id == "asic_UK00" and table_name == "dynamic":
                duplicate_rule = ingestion_policy.duplicate_physical_name_rule_for(
                    "00", "dynamic", "ARDS_Diagnose_App", 2
                )
                planned_rule_applies = duplicate_rule is not None and any(
                    column.physical_name_rule_id == duplicate_rule.rule_id
                    for column in (columns or ())
                )
                fields_ok = True
                if planned_rule_applies and duplicate_rule is not None:
                    for occurrence, name in enumerate(
                        duplicate_rule.physical_names, start=1
                    ):
                        field_index = schema.get_field_index(name)
                        if field_index < 0:
                            fields_ok = False
                            continue
                        metadata = schema.field(field_index).metadata or {}
                        fields_ok = fields_ok and (
                            metadata.get(b"raw_name") == duplicate_rule.raw_column_name.encode()
                            and metadata.get(b"raw_occurrence") == str(occurrence).encode()
                            and metadata.get(b"physical_name_rule_id")
                            == duplicate_rule.rule_id.encode()
                        )
                if not fields_ok:
                    failures.add(
                        "reviewed_duplicate_header_storage",
                        "Reviewed UK00 positional fields or their raw-name metadata differ from policy.",
                        hospital=hospital_id,
                        table=table_name,
                    )

            reporter(f"audit_provenance_start hospital={hospital_id} table={table_name}")
            streamed = _stream_provenance(
                parquet,
                ingestion_policy,
                records,
                hospital_id,
                source_folder,
                table_name,
                config.rows_per_batch,
                failures,
            )
            summary["provenance_rows_streamed"] = streamed
            if streamed != expected_rows:
                failures.add(
                    "row_and_file_conservation",
                    "Streamed provenance row count differs from inventory evidence.",
                    hospital=hospital_id,
                    table=table_name,
                )
            private_table_evidence[table_name] = {
                **summary,
                "path": str(path),
                "sha256": observed_hash,
            }
            reporter(f"audit_table_complete hospital={hospital_id} table={table_name}")
        hospital_summaries[hospital_id] = hospital_summary
        private_hospital_evidence[hospital_id] = {
            "directory": str(directory),
            "manifest_path": str(manifest_path),
            "tables": private_table_evidence,
        }
        reporter(f"audit_complete hospital={hospital_id}")

    checks = failures.checks(inventory.carried_blockers)
    status = overall_status(checks)
    blocking_findings = tuple(
        {"check": check.name, "details": check.details}
        for check in checks
        if check.status != "pass" and check.severity == "blocking"
    )
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_ingestion_audit_review",
        "artifact_version": AUDIT_ARTIFACT_VERSION,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "input_inventory_run_id": inventory_run_id,
        "authoritative_input_boundary": "raw_hospital_csv_files",
        "audited_stage": "lossless_per_hospital_ingestion",
        "metrics": {
            "approved_hospital_count": len(expected_hospitals),
            "audited_hospital_count": len(hospital_summaries),
            "audited_table_count": sum(len(value) for value in hospital_summaries.values()),
            "expected_static_row_count": total_rows["static"],
            "expected_dynamic_row_count": total_rows["dynamic"],
            "ingestion_manifest_version": ingestion_policy.ingestion_manifest_version,
            "ingestion_policy_version": ingestion_policy.policy_version,
            "inventory_artifact_version": ingestion_policy.required_inventory_artifact_version,
        },
        "hospital_summaries": hospital_summaries,
        "checks": [asdict(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "publication": {
            "ingested_artifacts_ready": False,
            "clinical_parsing_started": False,
            "translated_concatenation_started": False,
        },
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only private report bundle on the authorized cluster",
        },
        "limitations": [
            "The audit reads Parquet metadata, hashes each output, and streams provenance columns only; it does not parse clinical values.",
            "A passing technical ingestion audit does not approve parsing, translation, union-schema concatenation, cleaning, derivation, or publication.",
            "The reviewed archive remains excluded from ingestion and pending owner confirmation when that blocker is carried by the inventory.",
            "The old pooled artifact is not an authoritative input and cannot reconstruct strings lost upstream.",
            "No translated, cleaned, derived, or pooled production data artifact is generated by this command.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_ingestion_audit_private",
        "artifact_version": AUDIT_ARTIFACT_VERSION,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "input": {
            "inventory_run_id": inventory_run_id,
            "inventory_directory": str(inventory.directory),
            "ingested_root": str(config.output_root),
            "ingestion_policy_path": str(ingestion_policy.source_path),
        },
        "hospital_evidence": private_hospital_evidence,
        "checks": [asdict(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "publication_ready": False,
    }
    return IngestionAuditResult(
        dataset_context=config.dataset_context,
        overall_status=status,
        blocking_findings=blocking_findings,
        private_manifest=private_manifest,
        private_failures=tuple(failures.details),
        review_payload=review_payload,
    )


def _write_json(path: Path, value: Any, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 post-ingestion audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input inventory run: `{payload['input_inventory_run_id']}`",
        "- Protected filenames, identifiers, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    findings = payload["blocking_findings"]
    if findings:
        lines.extend(
            f"- `{finding['check']}`: {finding['details']}" for finding in findings
        )
    else:
        lines.append("- None.")
    lines.extend(["", "## Audit totals", ""])
    for key, value in payload["metrics"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Hospital summaries", ""])
    for hospital, tables in payload["hospital_summaries"].items():
        static = tables["static"]
        dynamic = tables["dynamic"]
        lines.append(
            f"- `{hospital}`: static rows `{static['parquet_row_count']}`; "
            f"dynamic rows `{dynamic['parquet_row_count']}`; "
            f"hashes match `{str(static['output_hash_matches_manifest'] and dynamic['output_hash_matches_manifest']).lower()}`"
        )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review this sanitized report and the owner-only failure evidence before "
        "approving raw-schema and parsing-token inventory. Ingested artifacts remain "
        "non-publishable."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_ingestion_audit_bundle(
    result: IngestionAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> IngestionAuditResult:
    selected_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run_id):
        raise IngestionError(
            "run_id must contain only letters, digits, underscores, and hyphens"
        )
    private_directory = (
        reports_root / "private" / "ingestion_audit" / selected_run_id
    )
    review_directory = reports_root / "review" / "ingestion_audit"
    review_json = review_directory / f"{selected_run_id}.json"
    review_markdown = review_directory / f"{selected_run_id}.md"
    if private_directory.exists() or review_json.exists() or review_markdown.exists():
        raise IngestionError(
            "Ingestion audit run already exists and will not be overwritten: "
            f"{selected_run_id}"
        )
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.chmod(0o700)
    review_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_directory.chmod(0o750)
    _write_json(
        private_directory / "ingestion_audit_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_json(
        private_directory / "failures.json",
        result.private_failures,
        0o600,
    )
    assert_review_payload_is_safe(result.review_payload)
    _write_json(review_json, result.review_payload, 0o640)
    markdown_temporary = review_markdown.with_suffix(".md.tmp")
    descriptor = os.open(
        markdown_temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_review_markdown(result.review_payload))
        markdown_temporary.replace(review_markdown)
        review_markdown.chmod(0o640)
    except Exception:
        markdown_temporary.unlink(missing_ok=True)
        raise
    return IngestionAuditResult(
        **{
            **asdict(result),
            "private_report_directory": private_directory,
            "review_json_path": review_json,
            "review_markdown_path": review_markdown,
        }
    )
