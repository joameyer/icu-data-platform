from __future__ import annotations

from collections import Counter, defaultdict
import csv
from dataclasses import asdict, dataclass
from hashlib import sha256
import io
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Callable, Literal
import zipfile

import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import InventoryConfig
from asic_pipeline.errors import InventoryError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import load_inventory_policy
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_json,
    _write_private_parquet,
    _write_review_file,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


ANOMALY_AUDIT_VERSION = "0.1"
SUPPORTED_INVENTORY_VERSIONS = frozenset({"0.2", "0.3", "0.4"})
DUPLICATE_RAW_NAME = "ARDS_Diagnose_App"


@dataclass(frozen=True)
class CSVStructure:
    header: tuple[str, ...]
    data_record_count: int
    inconsistent_record_count: int
    cell_stream_sha256: str
    raw_sha256: str | None
    parse_error: str | None


@dataclass(frozen=True)
class AnomalyAuditResult:
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


class _HashingRawReader(io.RawIOBase):
    def __init__(self, source: BinaryIO) -> None:
        self._source = source
        self._digest = sha256()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:
        data = self._source.read(len(buffer))
        if not data:
            return 0
        buffer[: len(data)] = data
        self._digest.update(data)
        return len(data)

    @property
    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _update_cell_digest(digest: Any, row: list[str] | tuple[str, ...]) -> None:
    digest.update(b"R")
    digest.update(len(row).to_bytes(8, "big"))
    for value in row:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)


def _inspect_csv_reader(reader: Any, raw_digest: str | None = None) -> CSVStructure:
    digest = sha256()
    header: tuple[str, ...] = ()
    rows = 0
    inconsistent = 0
    error: str | None = None
    try:
        first = next(reader, None)
        if first is None:
            raise ValueError("CSV file has no header record")
        header = tuple(first)
        _update_cell_digest(digest, header)
        for row in reader:
            rows += 1
            if len(row) != len(header):
                inconsistent += 1
            _update_cell_digest(digest, row)
    except (csv.Error, UnicodeError, ValueError) as exc:
        error = f"{type(exc).__name__}: {exc}"
    return CSVStructure(
        header=header,
        data_record_count=rows,
        inconsistent_record_count=inconsistent,
        cell_stream_sha256=digest.hexdigest(),
        raw_sha256=raw_digest,
        parse_error=error,
    )


def _inspect_csv_path(path: Path, encoding: str, delimiter: str) -> CSVStructure:
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        return _inspect_csv_reader(reader)


def _inspect_zip_csv(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    encoding: str,
    delimiter: str,
) -> CSVStructure:
    with archive.open(info, "r") as source:
        hashing_source = _HashingRawReader(source)
        with io.BufferedReader(hashing_source) as buffered:
            with io.TextIOWrapper(
                buffered,
                encoding=encoding,
                errors="strict",
                newline="",
            ) as text_stream:
                reader = csv.reader(text_stream, delimiter=delimiter, strict=True)
                structure = _inspect_csv_reader(reader)
        raw_digest = hashing_source.hexdigest
    if structure.parse_error is not None:
        # A decoding/CSV failure can stop before EOF. Reopen the member so the
        # recorded byte hash always covers the complete uncompressed entry.
        raw_digest = _sha256_zip_entry(archive, info)
    return CSVStructure(
        **{**asdict(structure), "raw_sha256": raw_digest}
    )


def _sha256_zip_entry(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> str:
    digest = sha256()
    with archive.open(info, "r") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _decode_json_column(row: dict[str, Any], name: str) -> dict[str, Any]:
    value = row.get(name)
    if not isinstance(value, str):
        raise InventoryError(
            f"Private inventory row is missing serialized {name!r} evidence"
        )
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise InventoryError(f"Private inventory {name!r} evidence is not a mapping")
    return decoded


def _validated_source_path(
    record: dict[str, Any], raw_root: Path
) -> Path:
    absolute = Path(record["absolute_path"]).resolve()
    relative = Path(record["relative_path"])
    expected = (raw_root / relative).resolve()
    try:
        absolute.relative_to(raw_root)
    except ValueError as exc:
        raise InventoryError(
            "Private inventory source path escapes the authoritative raw root"
        ) from exc
    if absolute != expected or absolute.is_symlink():
        raise InventoryError(
            "Private inventory source path no longer resolves to its recorded location"
        )
    return absolute


def _audit_duplicate_file(
    record: dict[str, Any],
    raw_root: Path,
) -> tuple[dict[str, Any], CSVStructure, Counter[tuple[int, str]], Counter[tuple[str, str]]]:
    path = _validated_source_path(record, raw_root)
    observed_hash = sha256_file(path)
    expected_hash = record["sha256"]
    encoding = _decode_json_column(record, "encoding")["selected_encoding"]
    delimiter = _decode_json_column(record, "delimiter")["selected_delimiter"]
    csv_evidence = _decode_json_column(record, "csv")

    category_counts: Counter[str] = Counter()
    token_counts: Counter[tuple[int, str]] = Counter()
    pair_counts: Counter[tuple[str, str]] = Counter()
    cell_digest = sha256()
    header: tuple[str, ...] = ()
    row_count = 0
    inconsistent = 0
    parse_error: str | None = None
    positions: tuple[int, ...] = ()
    try:
        with path.open("r", encoding=encoding, newline="") as stream:
            reader = csv.reader(stream, delimiter=delimiter, strict=True)
            first = next(reader, None)
            if first is None:
                raise ValueError("CSV file has no header record")
            header = tuple(first)
            _update_cell_digest(cell_digest, header)
            positions = tuple(
                index for index, name in enumerate(header) if name == DUPLICATE_RAW_NAME
            )
            if len(positions) != 2:
                raise ValueError(
                    "Expected exactly two positional occurrences of the reviewed duplicate"
                )
            first_index, second_index = positions
            for row in reader:
                row_count += 1
                _update_cell_digest(cell_digest, row)
                if len(row) != len(header):
                    inconsistent += 1
                    continue
                first_value = row[first_index]
                second_value = row[second_index]
                if first_value:
                    token_counts[(1, first_value)] += 1
                if second_value:
                    token_counts[(2, second_value)] += 1
                pair_counts[(first_value, second_value)] += 1
                if not first_value and not second_value:
                    category_counts["both_empty"] += 1
                elif first_value and not second_value:
                    category_counts["first_only"] += 1
                elif not first_value and second_value:
                    category_counts["second_only"] += 1
                elif first_value == second_value:
                    category_counts["equal_nonempty"] += 1
                else:
                    category_counts["conflicting_nonempty"] += 1
    except (csv.Error, UnicodeError, OSError, ValueError) as exc:
        parse_error = f"{type(exc).__name__}: {exc}"

    accounted = sum(category_counts.values())
    structure = CSVStructure(
        header=header,
        data_record_count=row_count,
        inconsistent_record_count=inconsistent,
        cell_stream_sha256=cell_digest.hexdigest(),
        raw_sha256=observed_hash,
        parse_error=parse_error,
    )
    file_result = {
        "file_id": record["file_id"],
        "relative_path": record["relative_path"],
        "source_filename": record["source_filename"],
        "filename_stay_id": record["filename_stay_id"],
        "expected_sha256": expected_hash,
        "observed_sha256": observed_hash,
        "source_hash_matches_inventory": observed_hash == expected_hash,
        "encoding": encoding,
        "delimiter": delimiter,
        "duplicate_raw_name": DUPLICATE_RAW_NAME,
        "duplicate_positions_one_based": [index + 1 for index in positions],
        "column_count": len(header),
        "data_record_count": row_count,
        "both_empty": category_counts["both_empty"],
        "first_only": category_counts["first_only"],
        "second_only": category_counts["second_only"],
        "equal_nonempty": category_counts["equal_nonempty"],
        "conflicting_nonempty": category_counts["conflicting_nonempty"],
        "accounted_record_count": accounted,
        "accounting_invariant_holds": accounted == row_count,
        "inconsistent_record_count": inconsistent,
        "header_matches_inventory": list(header) == csv_evidence.get("header"),
        "parse_error": parse_error,
        "cell_stream_sha256": structure.cell_stream_sha256,
    }
    return file_result, structure, token_counts, pair_counts


def _check(
    name: str,
    passed: bool,
    observed: Any,
    expected: Any,
    details: str,
    *,
    severity: Literal["blocking", "advisory"] = "blocking",
) -> CheckResult:
    return CheckResult(
        name=name,
        status="pass" if passed else "fail",
        severity=severity,
        observed=observed,
        expected=expected,
        details=details,
    )


def _load_inventory_evidence(
    config: InventoryConfig, inventory_run_id: str
) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
    if not RUN_ID_PATTERN.fullmatch(inventory_run_id):
        raise InventoryError("Invalid inventory run ID")
    private_directory = (
        config.paths.reports / "private" / "raw_inventory" / inventory_run_id
    )
    manifest_path = private_directory / "inventory_manifest.json"
    files_path = private_directory / "files.parquet"
    if not manifest_path.is_file() or not files_path.is_file():
        raise InventoryError(
            f"Private inventory evidence is incomplete: {private_directory}"
        )
    with manifest_path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    if manifest.get("artifact_version") not in SUPPORTED_INVENTORY_VERSIONS:
        raise InventoryError(
            "Raw anomaly audit requires a supported private inventory artifact "
            f"version in {sorted(SUPPORTED_INVENTORY_VERSIONS)}"
        )
    if manifest.get("dataset_context") != config.dataset_context:
        raise InventoryError("Inventory evidence dataset context does not match config")
    table = pq.read_table(files_path)
    required_columns = {
        "absolute_path",
        "relative_path",
        "source_filename",
        "filename_stay_id",
        "classification",
        "inspection_status",
        "hospital_folder",
        "sha256",
        "size_bytes",
        "csv",
        "encoding",
        "delimiter",
    }
    missing = required_columns - set(table.schema.names)
    if missing:
        raise InventoryError(
            f"Private inventory files table lacks required columns: {sorted(missing)}"
        )
    records = table.to_pylist()
    if manifest.get("file_count") != len(records):
        raise InventoryError("Private inventory file-count conservation failed")
    return private_directory, manifest, records


def build_raw_anomaly_audit(
    config: InventoryConfig,
    inventory_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> AnomalyAuditResult:
    progress_callback = progress or (lambda _: None)
    policy = load_inventory_policy(config.policy_path)
    inventory_directory, inventory_manifest, records = _load_inventory_evidence(
        config, inventory_run_id
    )
    raw_root = config.paths.raw_root.resolve()
    manifest_root = Path(inventory_manifest["input_root"]).resolve()
    if raw_root != manifest_root:
        raise InventoryError("Inventory raw root no longer matches production config")

    duplicate_records = [
        record
        for record in records
        if record["hospital_folder"] == "00"
        and record["classification"] == "dynamic_candidate"
        and (
            record["inspection_status"] == "csv_structure_invalid"
            or record.get("reviewed_duplicate_header") is not None
        )
    ]
    if not duplicate_records:
        raise InventoryError("No UK00 duplicate-header inventory evidence was found")

    file_results: list[dict[str, Any]] = []
    structures_by_filename: dict[str, CSVStructure] = {}
    token_counts: Counter[tuple[str, int, str]] = Counter()
    pair_counts: Counter[tuple[str, str, str]] = Counter()
    for index, record in enumerate(duplicate_records, start=1):
        file_result, structure, file_tokens, file_pairs = _audit_duplicate_file(
            record, raw_root
        )
        file_results.append(file_result)
        structures_by_filename[record["source_filename"]] = structure
        layout = (
            f"columns_{file_result['column_count']}_positions_"
            + "_".join(
                str(value)
                for value in file_result["duplicate_positions_one_based"]
            )
        )
        for (occurrence, token), count in file_tokens.items():
            token_counts[(layout, occurrence, token)] += count
        for (first_value, second_value), count in file_pairs.items():
            pair_counts[(layout, first_value, second_value)] += count
        if index % 500 == 0 or index == len(duplicate_records):
            progress_callback(
                f"duplicate_column_files_processed={index}/{len(duplicate_records)}"
            )

    flat_records = [
        record
        for record in records
        if record["hospital_folder"] == "00"
        and record["classification"]
        in {
            "selected_static_candidate",
            "additional_static_candidate",
            "dynamic_candidate",
        }
    ]
    flat_by_name: dict[str, dict[str, Any]] = {}
    for record in flat_records:
        filename = record["source_filename"]
        if filename in flat_by_name:
            raise InventoryError("UK00 flat source filenames are not unique")
        flat_by_name[filename] = record
        if filename not in structures_by_filename:
            path = _validated_source_path(record, raw_root)
            observed_hash = sha256_file(path)
            if observed_hash != record["sha256"]:
                structures_by_filename[filename] = CSVStructure(
                    (), 0, 0, sha256(b"").hexdigest(), observed_hash,
                    "Source hash changed after inventory",
                )
            else:
                encoding = _decode_json_column(record, "encoding")[
                    "selected_encoding"
                ]
                delimiter = _decode_json_column(record, "delimiter")[
                    "selected_delimiter"
                ]
                structures_by_filename[filename] = _inspect_csv_path(
                    path, encoding, delimiter
                )

    archive_records = [
        record
        for record in records
        if record["classification"]
        in {
            "unknown_file",
            "archive_candidate",
            "provisionally_excluded_archive",
        }
    ]
    if len(archive_records) != 1:
        raise InventoryError(
            f"Expected exactly one unreviewed archive, observed {len(archive_records)}"
        )
    archive_record = archive_records[0]
    archive_path = _validated_source_path(archive_record, raw_root)
    if archive_path.suffix.casefold() != ".zip" or not zipfile.is_zipfile(archive_path):
        raise InventoryError("The sole unknown file is not the reviewed ZIP archive")
    archive_hash = sha256_file(archive_path)
    archive_hash_matches = archive_hash == archive_record["sha256"]
    if not archive_hash_matches:
        raise InventoryError("ZIP archive changed after the raw inventory run")

    archive_entry_results: list[dict[str, Any]] = []
    matched_flat_names: set[str] = set()
    zip_safety = Counter()
    with zipfile.ZipFile(archive_path) as archive:
        infos = archive.infolist()
        basename_counts = Counter(
            PurePosixPath(info.filename).name for info in infos if not info.is_dir()
        )
        for index, info in enumerate(infos, start=1):
            member_path = PurePosixPath(info.filename)
            basename = member_path.name
            entry_read_error: str | None = None
            try:
                if info.is_dir():
                    classification = "directory_entry"
                    archive_sha = None
                    structure = None
                else:
                    if member_path.is_absolute() or ".." in member_path.parts:
                        zip_safety["suspicious_path"] += 1
                    if info.flag_bits & 0x1:
                        zip_safety["encrypted_entry"] += 1
                    if ".ipynb_checkpoints" in member_path.parts:
                        zip_safety["checkpoint_entry"] += 1
                    flat_record = (
                        flat_by_name.get(basename)
                        if basename_counts[basename] == 1
                        else None
                    )
                    structure = None
                    if flat_record is None:
                        archive_sha = _sha256_zip_entry(archive, info)
                        classification = (
                            "archive_only_nested_zip"
                            if member_path.suffix.casefold() == ".zip"
                            else "archive_only_entry"
                        )
                    else:
                        matched_flat_names.add(basename)
                        flat_structure = structures_by_filename[basename]
                        if member_path.suffix.casefold() != ".csv":
                            archive_sha = _sha256_zip_entry(archive, info)
                            classification = (
                                "exact_byte_duplicate"
                                if archive_sha == flat_record["sha256"]
                                else "different_binary_content"
                            )
                        else:
                            encoding = _decode_json_column(flat_record, "encoding")[
                                "selected_encoding"
                            ]
                            delimiter = _decode_json_column(flat_record, "delimiter")[
                                "selected_delimiter"
                            ]
                            structure = _inspect_zip_csv(
                                archive, info, encoding, delimiter
                            )
                            archive_sha = structure.raw_sha256
                            if archive_sha == flat_record["sha256"]:
                                classification = "exact_byte_duplicate"
                            elif structure.parse_error or structure.inconsistent_record_count:
                                classification = "archive_csv_structurally_unresolved"
                            elif flat_structure.parse_error or flat_structure.inconsistent_record_count:
                                classification = "flat_csv_structurally_unresolved"
                            elif (
                                structure.cell_stream_sha256
                                == flat_structure.cell_stream_sha256
                            ):
                                classification = "equivalent_after_csv_parsing"
                            elif structure.header != flat_structure.header:
                                classification = "differing_schema"
                            elif (
                                structure.data_record_count
                                != flat_structure.data_record_count
                            ):
                                classification = "differing_row_count"
                            else:
                                classification = "differing_content_same_schema_and_rows"
            except (OSError, RuntimeError, UnicodeError, ValueError, zipfile.BadZipFile) as exc:
                archive_sha = None
                structure = None
                classification = "archive_entry_read_error"
                entry_read_error = f"{type(exc).__name__}: {exc}"
            archive_entry_results.append(
                {
                    "archive_entry_name": info.filename,
                    "archive_entry_basename": basename,
                    "archive_entry_size": info.file_size,
                    "archive_entry_compressed_size": info.compress_size,
                    "archive_entry_crc32": info.CRC,
                    "archive_entry_sha256": archive_sha,
                    "flat_source_filename": basename if basename in flat_by_name else None,
                    "flat_source_sha256": (
                        flat_by_name[basename]["sha256"]
                        if basename in flat_by_name
                        else None
                    ),
                    "classification": classification,
                    "archive_csv_row_count": (
                        structure.data_record_count if structure else None
                    ),
                    "archive_csv_column_count": (
                        len(structure.header) if structure else None
                    ),
                    "archive_csv_parse_error": (
                        structure.parse_error if structure else None
                    ),
                    "archive_entry_read_error": entry_read_error,
                }
            )
            if index % 500 == 0 or index == len(infos):
                progress_callback(
                    f"archive_entries_processed={index}/{len(infos)}"
                )

    flat_only_results = tuple(
        {
            "source_filename": filename,
            "relative_path": record["relative_path"],
            "classification": record["classification"],
            "sha256": record["sha256"],
            "size_bytes": record["size_bytes"],
        }
        for filename, record in sorted(flat_by_name.items())
        if filename not in matched_flat_names
    )

    layout_counts: dict[tuple[int, tuple[int, ...]], Counter[str]] = defaultdict(
        Counter
    )
    for result in file_results:
        key = (
            result["column_count"],
            tuple(result["duplicate_positions_one_based"]),
        )
        layout_counts[key]["file_count"] += 1
        layout_counts[key]["data_record_count"] += result["data_record_count"]
        for category in (
            "both_empty",
            "first_only",
            "second_only",
            "equal_nonempty",
            "conflicting_nonempty",
        ):
            layout_counts[key][category] += result[category]

    total_categories = Counter()
    for result in file_results:
        for category in (
            "both_empty",
            "first_only",
            "second_only",
            "equal_nonempty",
            "conflicting_nonempty",
        ):
            total_categories[category] += result[category]
    total_rows = sum(result["data_record_count"] for result in file_results)
    accounted_rows = sum(total_categories.values())
    second_nonempty = (
        total_categories["second_only"]
        + total_categories["equal_nonempty"]
        + total_categories["conflicting_nonempty"]
    )
    source_hash_mismatches = sum(
        not result["source_hash_matches_inventory"] for result in file_results
    ) + sum(
        structure.parse_error == "Source hash changed after inventory"
        for structure in structures_by_filename.values()
    )
    duplicate_parse_failures = sum(
        bool(result["parse_error"] or result["inconsistent_record_count"])
        for result in file_results
    )
    archive_classification_counts = Counter(
        result["classification"] for result in archive_entry_results
    )
    archive_content_differences = sum(
        count
        for classification, count in archive_classification_counts.items()
        if classification
        not in {"exact_byte_duplicate", "equivalent_after_csv_parsing"}
        and not classification.startswith("archive_only")
        and classification != "directory_entry"
    )
    archive_only_count = sum(
        classification.startswith("archive_only")
        for classification in (
            result["classification"] for result in archive_entry_results
        )
    )

    checks = [
        _check(
            "source_hashes_match_inventory",
            source_hash_mismatches == 0 and archive_hash_matches,
            source_hash_mismatches,
            0,
            "Every audited flat source and the ZIP must match its inventory SHA-256.",
        ),
        _check(
            "duplicate_column_files_structurally_readable",
            duplicate_parse_failures == 0,
            duplicate_parse_failures,
            0,
            "Every UK00 duplicate-header file must remain rectangular and strictly readable.",
        ),
        _check(
            "duplicate_column_accounting_invariant",
            accounted_rows == total_rows,
            {"rows": total_rows, "accounted": accounted_rows},
            "rows == sum of five literal-string relationship categories",
            "Every audited row must be classified exactly once.",
        ),
        _check(
            "v2_second_occurrence_all_missing_revalidated",
            second_nonempty == 0,
            second_nonempty,
            0,
            "The v2 all-missing drop precondition must be revalidated against raw strings.",
        ),
        _check(
            "zip_entry_paths_safe",
            sum(zip_safety.values()) == 0,
            dict(sorted(zip_safety.items())),
            {},
            "Archive entries must be unencrypted and exclude unsafe or checkpoint paths.",
        ),
        _check(
            "zip_overlap_content_equivalent",
            archive_content_differences == 0,
            archive_content_differences,
            0,
            "Archive/flat content differences are retained as evidence supporting the provisional old-snapshot classification.",
            severity="advisory",
        ),
        _check(
            "zip_and_flat_file_sets_equal",
            archive_only_count == 0 and not flat_only_results,
            {
                "archive_only": archive_only_count,
                "flat_only": len(flat_only_results),
            },
            {"archive_only": 0, "flat_only": 0},
            "Archive and flat file-set differences are retained as evidence supporting the provisional old-snapshot classification.",
            severity="advisory",
        ),
        _check(
            "provisional_archive_owner_confirmation",
            False,
            "provisional_pending_owner_confirmation",
            "confirmed_legacy_snapshot",
            "The ZIP is excluded from ingestion provisionally but remains blocking until its owner confirms the legacy-snapshot role.",
        ),
        _check(
            "hospital_mapping_contract_approved",
            all(
                policy.hospital_mapping_for(folder) is not None
                and policy.hospital_mapping_for(folder).cohort_action == "include"
                for folder in policy.expected_hospital_folders
            ),
            {
                mapping.source_folder: {
                    "canonical_hospital_id": mapping.canonical_hospital_id,
                    "cohort_action": mapping.cohort_action,
                    "approval_status": mapping.approval_status,
                }
                for mapping in policy.hospital_mappings
            },
            "all expected folders mapped and included",
            "The approved folder-to-hospital and cohort-inclusion contract applies to this audit.",
        ),
    ]
    status = overall_status(checks)
    blocking_findings = tuple(
        {
            "check": check.name,
            "observed": check.observed,
            "expected": check.expected,
            "details": check.details,
        }
        for check in checks
        if check.status != "pass" and check.severity == "blocking"
    )
    review_layouts = [
        {
            "review_layout_key": f"layout_{index:02d}",
            "column_count": key[0],
            "duplicate_positions_one_based": list(key[1]),
            **dict(counts),
        }
        for index, (key, counts) in enumerate(
            sorted(layout_counts.items()), start=1
        )
    ]
    review_payload = {
        "artifact": "asic_v3_raw_anomaly_audit_review",
        "artifact_version": ANOMALY_AUDIT_VERSION,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "overall_status": status,
        "input_inventory_run_id": inventory_run_id,
        "duplicate_column_audit": {
            "raw_variable_name_withheld": True,
            "file_count": len(file_results),
            "data_record_count": total_rows,
            "accounted_record_count": accounted_rows,
            "category_counts": dict(total_categories),
            "second_occurrence_nonempty_count": second_nonempty,
            "layouts": review_layouts,
            "v2_precondition_under_review": "second occurrence all missing",
        },
        "archive_comparison": {
            "archive_hash_matches_inventory": archive_hash_matches,
            "entry_count": len(archive_entry_results),
            "classification_counts": dict(
                sorted(archive_classification_counts.items())
            ),
            "flat_only_count": len(flat_only_results),
            "archive_only_count": archive_only_count,
            "nested_archive_entry_count": archive_classification_counts[
                "archive_only_nested_zip"
            ],
            "zip_safety_findings": dict(sorted(zip_safety.items())),
            "archive_role": "provisionally_excluded_pending_owner_confirmation",
        },
        "checks": [asdict(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "private_evidence_remains_on_authorized_cluster": True,
        },
        "limitations": [
            "Literal empty strings are distinguished from every non-empty token; no textual-missing policy is applied.",
            "CSV equivalence compares decoded literal cell streams and does not parse clinical values.",
            "Nested ZIP content is hashed as an entry but is not recursively inspected or extracted.",
            "Folder 01 is approved as asic_UK01 and included in the ingestion cohort; hospital-specific parsing remains a later review.",
            "No ingestion or production data artifacts are generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_raw_anomaly_audit_private",
        "artifact_version": ANOMALY_AUDIT_VERSION,
        "dataset_context": config.dataset_context,
        "generated_at_utc": review_payload["generated_at_utc"],
        "overall_status": status,
        "input_inventory_run_id": inventory_run_id,
        "input_inventory_directory": str(inventory_directory),
        "raw_root": str(raw_root),
        "duplicate_raw_name": DUPLICATE_RAW_NAME,
        "archive_relative_path": archive_record["relative_path"],
        "archive_sha256": archive_hash,
        "blocking_findings": list(blocking_findings),
    }
    return AnomalyAuditResult(
        overall_status=status,
        blocking_findings=blocking_findings,
        private_manifest=private_manifest,
        review_payload=review_payload,
        private_tables={
            "duplicate_column_files": tuple(file_results),
            "duplicate_token_counts": tuple(
                {
                    "layout": layout,
                    "occurrence": occurrence,
                    "raw_token": token,
                    "count": count,
                }
                for (layout, occurrence, token), count in sorted(
                    token_counts.items()
                )
            ),
            "duplicate_pair_counts": tuple(
                {
                    "layout": layout,
                    "first_raw_token": first_value,
                    "second_raw_token": second_value,
                    "count": count,
                }
                for (layout, first_value, second_value), count in sorted(
                    pair_counts.items()
                )
            ),
            "archive_entries": tuple(archive_entry_results),
            "flat_only_files": flat_only_results,
        },
    )


def _anomaly_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 raw anomaly audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input inventory run: `{payload['input_inventory_run_id']}`",
        "- Protected filenames, identifiers, and raw tokens: excluded",
        "",
        "## Duplicate-column audit",
        "",
        f"- Files: `{payload['duplicate_column_audit']['file_count']}`",
        f"- Rows: `{payload['duplicate_column_audit']['data_record_count']}`",
        f"- Second-occurrence non-empty: `{payload['duplicate_column_audit']['second_occurrence_nonempty_count']}`",
        "",
        "## Archive comparison",
        "",
        f"- Entries: `{payload['archive_comparison']['entry_count']}`",
        f"- Archive-only: `{payload['archive_comparison']['archive_only_count']}`",
        f"- Flat-only: `{payload['archive_comparison']['flat_only_count']}`",
        f"- Role: `{payload['archive_comparison']['archive_role']}`",
        "",
        "## Blocking findings",
        "",
    ]
    lines.extend(
        f"- `{finding['check']}`: {finding['details']}"
        for finding in payload["blocking_findings"]
    )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Do not ingest the ZIP, treat its provisional exclusion as final, or drop "
        "either duplicate occurrence outside the approved translation condition."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_raw_anomaly_audit_bundle(
    result: AnomalyAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> AnomalyAuditResult:
    selected_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run_id):
        raise InventoryError("Invalid anomaly-audit run ID")
    private_directory = (
        reports_root / "private" / "raw_anomaly_audit" / selected_run_id
    )
    review_directory = reports_root / "review" / "raw_anomaly_audit"
    review_json = review_directory / f"{selected_run_id}.json"
    review_markdown = review_directory / f"{selected_run_id}.md"
    if private_directory.exists() or review_json.exists() or review_markdown.exists():
        raise InventoryError(
            f"Anomaly-audit run exists and will not be overwritten: {selected_run_id}"
        )
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.chmod(0o700)
    review_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    _write_private_json(
        private_directory / "anomaly_audit_manifest.json", result.private_manifest
    )
    for table_name, rows in result.private_tables.items():
        _write_private_parquet(private_directory / f"{table_name}.parquet", rows)
    _write_private_json(
        private_directory / "blocking_findings.json", result.blocking_findings
    )
    assert_review_payload_is_safe(result.review_payload)
    _write_review_file(
        review_json,
        json.dumps(result.review_payload, ensure_ascii=False, indent=2) + "\n",
    )
    _write_review_file(review_markdown, _anomaly_markdown(result.review_payload))
    return AnomalyAuditResult(
        **{
            **asdict(result),
            "private_report_directory": private_directory,
            "review_json_path": review_json,
            "review_markdown_path": review_markdown,
        }
    )
