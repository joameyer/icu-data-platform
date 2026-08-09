from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.errors import IngestionError
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.ingestion.policy import IngestionPolicy, load_ingestion_policy
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import InventoryPolicy, load_inventory_policy
from asic_pipeline.inventory.report import RUN_ID_PATTERN


@dataclass(frozen=True)
class RawColumnIdentity:
    name: str
    occurrence: int


@dataclass(frozen=True)
class RawOutputColumn:
    identity: RawColumnIdentity
    physical_name: str
    physical_name_rule_id: str | None


@dataclass(frozen=True)
class IngestionResult:
    hospital_id: str
    output_directory: Path
    static_path: Path
    dynamic_path: Path
    manifest_path: Path
    static_row_count: int
    dynamic_row_count: int
    publication_ready: bool
    publication_blockers: tuple[str, ...]


@dataclass(frozen=True)
class InventoryEvidence:
    directory: Path
    manifest: dict[str, Any]
    records: tuple[dict[str, Any], ...]
    carried_blockers: tuple[str, ...]


@dataclass
class _TableMetrics:
    source_rows: int = 0
    output_rows: int = 0
    literal_empty_cells: int = 0
    absent_column_nulls: int = 0
    identifier_mismatches: int = 0


def _decode_mapping(record: dict[str, Any], name: str) -> dict[str, Any]:
    value = record.get(name)
    if not isinstance(value, str):
        raise IngestionError(f"Inventory record lacks serialized {name!r} evidence")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise IngestionError(f"Inventory {name!r} evidence is invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise IngestionError(f"Inventory {name!r} evidence must be a mapping")
    return decoded


def _optional_mapping(record: dict[str, Any], name: str) -> dict[str, Any] | None:
    value = record.get(name)
    if value is None:
        return None
    return _decode_mapping(record, name)


def _load_inventory_evidence(
    config: IngestionConfig,
    policy: IngestionPolicy,
    inventory_policy: InventoryPolicy,
    inventory_run_id: str,
) -> InventoryEvidence:
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
    try:
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise IngestionError("Private inventory manifest cannot be read") from exc
    if manifest.get("artifact") != "asic_v3_raw_inventory_private":
        raise IngestionError("Inventory artifact identity is invalid")
    if manifest.get("artifact_version") != policy.required_inventory_artifact_version:
        raise IngestionError(
            "Lossless ingestion requires inventory artifact version "
            f"{policy.required_inventory_artifact_version}"
        )
    if manifest.get("dataset_context") != config.dataset_context:
        raise IngestionError("Inventory and ingestion dataset contexts differ")
    if Path(str(manifest.get("input_root"))).resolve() != (
        config.inventory.paths.raw_root.resolve()
    ):
        raise IngestionError("Inventory raw root no longer matches ingestion config")
    findings = manifest.get("blocking_findings")
    if not isinstance(findings, list):
        raise IngestionError("Inventory manifest blocking findings are invalid")
    blocker_names = tuple(
        sorted(
            finding.get("check")
            for finding in findings
            if isinstance(finding, dict) and isinstance(finding.get("check"), str)
        )
    )
    if len(blocker_names) != len(findings):
        raise IngestionError("Every inventory blocker must have a check name")
    unexpected = sorted(set(blocker_names) - set(policy.allowed_inventory_blockers))
    if unexpected:
        raise IngestionError(
            f"Inventory has blockers not approved for ingestion: {unexpected}"
        )

    try:
        table = pq.read_table(files_path)
    except (OSError, pa.ArrowException) as exc:
        raise IngestionError("Private inventory file evidence cannot be read") from exc
    required_columns = {
        "absolute_path",
        "relative_path",
        "source_filename",
        "filename_stay_id",
        "file_id",
        "classification",
        "inspection_status",
        "hospital_folder",
        "sha256",
        "size_bytes",
        "is_symbolic_link",
        "csv",
        "encoding",
        "delimiter",
        "identifiers",
    }
    missing = required_columns - set(table.schema.names)
    if missing:
        raise IngestionError(
            f"Private inventory file evidence lacks columns: {sorted(missing)}"
        )
    records = tuple(table.to_pylist())
    if manifest.get("file_count") != len(records):
        raise IngestionError("Inventory file-count conservation failed")

    archives = [
        record
        for record in records
        if record.get("classification") == "provisionally_excluded_archive"
    ]
    for archive in archives:
        rule = inventory_policy.provisional_archive_exclusion_for(
            archive.get("hospital_folder"),
            archive.get("size_bytes"),
            archive.get("sha256"),
        )
        resolution = _optional_mapping(archive, "archive_resolution")
        if (
            rule is None
            or resolution is None
            or resolution.get("rule_id") != rule.rule_id
            or resolution.get("ingestion_action") != "exclude"
        ):
            raise IngestionError(
                "A provisionally excluded archive does not match the exact approved rule"
            )
        archive_path = _validated_source_path(
            archive, config.inventory.paths.raw_root
        )
        if (
            archive_path.stat().st_size != archive.get("size_bytes")
            or sha256_file(archive_path) != archive.get("sha256")
        ):
            raise IngestionError("The provisionally excluded archive changed after inventory")
    if "provisional_archive_owner_confirmation" in blocker_names:
        if len(archives) != 1:
            raise IngestionError(
                "The provisional archive blocker requires exactly one excluded archive"
            )
    return InventoryEvidence(directory, manifest, records, blocker_names)


def _column_identities(header: list[str] | tuple[str, ...]) -> tuple[RawColumnIdentity, ...]:
    occurrences: dict[str, int] = {}
    identities: list[RawColumnIdentity] = []
    for name in header:
        occurrence = occurrences.get(name, 0) + 1
        occurrences[name] = occurrence
        identities.append(RawColumnIdentity(name=name, occurrence=occurrence))
    return tuple(identities)


def _union_columns(records: list[dict[str, Any]]) -> tuple[RawColumnIdentity, ...]:
    seen: set[RawColumnIdentity] = set()
    ordered: list[RawColumnIdentity] = []
    for record in records:
        header = _decode_mapping(record, "csv").get("header")
        if not isinstance(header, list) or any(not isinstance(name, str) for name in header):
            raise IngestionError("Inventory CSV header evidence is invalid")
        for identity in _column_identities(header):
            if identity not in seen:
                seen.add(identity)
                ordered.append(identity)
    return tuple(ordered)


def _plan_raw_output_columns(
    hospital_folder: str,
    table_name: str,
    raw_columns: tuple[RawColumnIdentity, ...],
    policy: IngestionPolicy,
) -> tuple[RawOutputColumn, ...]:
    occurrence_counts = Counter(column.name for column in raw_columns)
    rules = {
        name: policy.duplicate_physical_name_rule_for(
            hospital_folder,
            table_name,
            name,
            count,
        )
        for name, count in occurrence_counts.items()
        if count > 1
    }
    unresolved = sorted(name for name, rule in rules.items() if rule is None)
    if unresolved:
        raise IngestionError(
            f"Duplicate raw headers lack approved physical names: {unresolved}"
        )
    planned: list[RawOutputColumn] = []
    for identity in raw_columns:
        rule = rules.get(identity.name)
        if rule is None:
            physical_name = identity.name
            rule_id = None
        else:
            physical_name = rule.physical_names[identity.occurrence - 1]
            rule_id = rule.rule_id
        planned.append(
            RawOutputColumn(
                identity=identity,
                physical_name=physical_name,
                physical_name_rule_id=rule_id,
            )
        )
    physical_names = [column.physical_name for column in planned]
    if len(physical_names) != len(set(physical_names)):
        raise IngestionError(
            "Approved duplicate physical names collide with another output column"
        )
    return tuple(planned)


def _table_schema(
    table_name: str,
    raw_columns: tuple[RawOutputColumn, ...],
    policy: IngestionPolicy,
) -> pa.Schema:
    provenance = policy.provenance_columns
    raw_names = {column.identity.name for column in raw_columns}
    collisions = sorted(raw_names & set(provenance.values()))
    if collisions:
        raise IngestionError(
            f"Raw headers collide with reserved provenance columns: {collisions}"
        )
    provenance_types: dict[str, pa.DataType] = {
        "source_hospital_folder": pa.string(),
        "canonical_hospital_id": pa.string(),
        "source_filename": pa.string(),
        "source_file_id": pa.string(),
        "source_file_order": pa.int32(),
        "source_row_number": pa.int64(),
        "source_order": pa.int64(),
        "filename_stay_id": pa.string(),
        "in_file_stay_id": pa.string(),
        "source_schema_variant_id": pa.string(),
    }
    fields = [
        pa.field(
            provenance[key],
            provenance_types[key],
            nullable=key in {"filename_stay_id", "in_file_stay_id"},
            metadata={b"asic_v3_role": b"source_provenance", b"provenance_key": key.encode()},
        )
        for key in provenance
    ]
    fields.extend(
        pa.field(
            column.physical_name,
            pa.string(),
            nullable=True,
            metadata={
                b"asic_v3_role": b"raw_clinical_string",
                b"raw_name": column.identity.name.encode("utf-8"),
                b"raw_occurrence": str(column.identity.occurrence).encode(),
                b"physical_name_rule_id": (
                    column.physical_name_rule_id or "identity"
                ).encode(),
            },
        )
        for column in raw_columns
    )
    return pa.schema(
        fields,
        metadata={
            b"asic_v3_stage": b"lossless_ingestion",
            b"asic_v3_table": table_name.encode(),
            b"raw_empty_cell": b"empty_string",
            b"raw_absent_column": b"parquet_null",
            b"physical_column_names_unique": b"true",
        },
    )


def _validated_source_path(record: dict[str, Any], raw_root: Path) -> Path:
    relative = Path(str(record["relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise IngestionError("Inventory source relative path is unsafe")
    expected = (raw_root / relative).resolve()
    observed = Path(str(record["absolute_path"])).resolve()
    try:
        expected.relative_to(raw_root)
    except ValueError as exc:
        raise IngestionError("Inventory source path escapes the raw root") from exc
    if observed != expected:
        raise IngestionError("Inventory absolute and relative source paths disagree")
    if expected.is_symlink() or bool(record.get("is_symbolic_link")):
        raise IngestionError("Symbolic-link sources cannot be ingested")
    if not expected.is_file():
        raise IngestionError("Inventory source file is no longer available")
    return expected


def _flush_columns(
    writer: pq.ParquetWriter,
    schema: pa.Schema,
    columns: list[list[Any]],
) -> int:
    if not columns or not columns[0]:
        return 0
    arrays = [
        pa.array(values, type=field.type)
        for values, field in zip(columns, schema, strict=True)
    ]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    writer.write_batch(batch)
    rows = batch.num_rows
    for values in columns:
        values.clear()
    return rows


def _write_table(
    table_name: str,
    records: list[dict[str, Any]],
    output_path: Path,
    hospital_folder: str,
    hospital_id: str,
    config: IngestionConfig,
    ingestion_policy: IngestionPolicy,
    progress: Callable[[str], None],
) -> tuple[dict[str, Any], tuple[RawOutputColumn, ...]]:
    raw_identities = _union_columns(records)
    raw_columns = _plan_raw_output_columns(
        hospital_folder,
        table_name,
        raw_identities,
        ingestion_policy,
    )
    schema = _table_schema(table_name, raw_columns, ingestion_policy)
    columns: list[list[Any]] = [[] for _ in schema]
    provenance_count = len(ingestion_policy.provenance_columns)
    metrics = _TableMetrics()
    file_metrics: list[dict[str, Any]] = []
    source_order = 0
    static_stay_ids: set[str] = set()

    with pq.ParquetWriter(
        output_path,
        schema,
        compression=ingestion_policy.parquet_compression,
    ) as writer:
        for file_order, record in enumerate(records, start=1):
            path = _validated_source_path(record, config.inventory.paths.raw_root)
            observed_hash = sha256_file(path)
            if observed_hash != record["sha256"]:
                raise IngestionError(
                    f"Source hash changed after inventory for file ID {record['file_id']}"
                )
            csv_evidence = _decode_mapping(record, "csv")
            encoding = _decode_mapping(record, "encoding").get("selected_encoding")
            delimiter = _decode_mapping(record, "delimiter").get("selected_delimiter")
            identifiers = _decode_mapping(record, "identifiers")
            expected_header = csv_evidence.get("header")
            expected_rows = csv_evidence.get("data_record_count")
            schema_variant = csv_evidence.get("schema_variant_id")
            if (
                not isinstance(encoding, str)
                or not isinstance(delimiter, str)
                or not isinstance(expected_header, list)
                or not isinstance(expected_rows, int)
                or not isinstance(schema_variant, str)
            ):
                raise IngestionError("Inventory CSV parsing evidence is incomplete")
            current_identities = _column_identities(expected_header)
            source_indices = {
                identity: index for index, identity in enumerate(current_identities)
            }
            identifier_column = identifiers.get("identifier_column")
            if identifier_column is not None and not isinstance(identifier_column, str):
                raise IngestionError("Inventory identifier-column evidence is invalid")
            identifier_index = (
                expected_header.index(identifier_column)
                if identifier_column is not None
                else None
            )
            filename_stay_id = record.get("filename_stay_id")
            if filename_stay_id is not None and not isinstance(filename_stay_id, str):
                raise IngestionError("Filename-derived stay ID evidence is invalid")

            file_rows = 0
            try:
                with path.open("r", encoding=encoding, newline="") as stream:
                    reader = csv.reader(stream, delimiter=delimiter, strict=True)
                    observed_header = next(reader, None)
                    if observed_header != expected_header:
                        raise IngestionError(
                            "Source header changed after inventory for file ID "
                            f"{record['file_id']}"
                        )
                    for row_number, row in enumerate(reader, start=2):
                        if len(row) != len(expected_header):
                            raise IngestionError(
                                f"Source record width changed for file ID {record['file_id']}"
                            )
                        file_rows += 1
                        source_order += 1
                        in_file_stay_id = (
                            row[identifier_index] if identifier_index is not None else None
                        )
                        if (
                            table_name == "dynamic"
                            and filename_stay_id is not None
                            and in_file_stay_id not in {None, "", filename_stay_id}
                        ):
                            metrics.identifier_mismatches += 1
                            raise IngestionError(
                                "Filename and in-file identifiers disagree for file ID "
                                f"{record['file_id']}"
                            )
                        if table_name == "static" and in_file_stay_id in {None, ""}:
                            raise IngestionError(
                                "Static row lacks an in-file stay ID for file ID "
                                f"{record['file_id']}"
                            )
                        if table_name == "static":
                            assert isinstance(in_file_stay_id, str)
                            if in_file_stay_id in static_stay_ids:
                                raise IngestionError(
                                    f"Static stay ID is duplicated in file ID {record['file_id']}"
                                )
                            static_stay_ids.add(in_file_stay_id)
                        provenance_values: dict[str, Any] = {
                            "source_hospital_folder": hospital_folder,
                            "canonical_hospital_id": hospital_id,
                            "source_filename": record["source_filename"],
                            "source_file_id": record["file_id"],
                            "source_file_order": file_order,
                            "source_row_number": row_number,
                            "source_order": source_order,
                            "filename_stay_id": filename_stay_id,
                            "in_file_stay_id": in_file_stay_id,
                            "source_schema_variant_id": schema_variant,
                        }
                        for index, key in enumerate(ingestion_policy.provenance_columns):
                            columns[index].append(provenance_values[key])
                        for raw_offset, output_column in enumerate(raw_columns):
                            source_index = source_indices.get(output_column.identity)
                            if source_index is None:
                                value: str | None = None
                                metrics.absent_column_nulls += 1
                            else:
                                value = row[source_index]
                                if value == "":
                                    metrics.literal_empty_cells += 1
                            columns[provenance_count + raw_offset].append(value)
                        if len(columns[0]) >= config.rows_per_batch:
                            metrics.output_rows += _flush_columns(writer, schema, columns)
            except (csv.Error, UnicodeError, OSError) as exc:
                raise IngestionError(
                    f"Source CSV cannot be read for file ID {record['file_id']}"
                ) from exc
            if file_rows != expected_rows:
                raise IngestionError(
                    f"Source row count changed after inventory for file ID {record['file_id']}"
                )
            metrics.source_rows += file_rows
            file_metrics.append(
                {
                    "file_id": record["file_id"],
                    "source_filename": record["source_filename"],
                    "relative_path": record["relative_path"],
                    "sha256": record["sha256"],
                    "encoding": encoding,
                    "delimiter": delimiter,
                    "schema_variant_id": schema_variant,
                    "identifier_column": identifier_column,
                    "identifier_comparison_status": identifiers.get(
                        "filename_comparison_status"
                    ),
                    "source_row_count": file_rows,
                    "output_row_count": file_rows,
                    "header": expected_header,
                    "filename_stay_id": filename_stay_id,
                }
            )
            progress(
                f"hospital={hospital_id} table={table_name} "
                f"files={file_order}/{len(records)} rows={metrics.source_rows}"
            )
        metrics.output_rows += _flush_columns(writer, schema, columns)

    output_path.chmod(0o600)
    parquet = pq.ParquetFile(output_path)
    if parquet.schema_arrow != schema:
        raise IngestionError(f"Written {table_name} schema differs from the planned schema")
    if parquet.metadata.num_rows != metrics.source_rows:
        raise IngestionError(f"Written {table_name} row count violates conservation")
    if metrics.output_rows != metrics.source_rows:
        raise IngestionError(f"Streamed {table_name} row count violates conservation")
    if metrics.identifier_mismatches:
        raise IngestionError(f"Written {table_name} contains identifier mismatches")
    return (
        {
            "source_file_count": len(records),
            "source_row_count": metrics.source_rows,
            "output_row_count": parquet.metadata.num_rows,
            "output_column_count": len(schema),
            "raw_column_count": len(raw_columns),
            "schema_variant_count": len(
                {item["schema_variant_id"] for item in file_metrics}
            ),
            "literal_empty_cell_count": metrics.literal_empty_cells,
            "absent_source_column_null_count": metrics.absent_column_nulls,
            "identifier_mismatch_count": metrics.identifier_mismatches,
            "files": file_metrics,
        },
        raw_columns,
    )


def _write_json_private(path: Path, value: dict[str, Any]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        path.chmod(0o600)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def ingest_hospital(
    config: IngestionConfig,
    inventory_run_id: str,
    hospital_id: str,
    progress: Callable[[str], None] | None = None,
) -> IngestionResult:
    reporter = progress or (lambda _: None)
    ingestion_policy = load_ingestion_policy(config.policy_path)
    inventory_policy = load_inventory_policy(config.inventory.policy_path)
    mapping = next(
        (
            item
            for item in inventory_policy.hospital_mappings
            if item.canonical_hospital_id == hospital_id
        ),
        None,
    )
    if mapping is None or mapping.cohort_action != "include":
        raise IngestionError(f"Hospital is not approved for ingestion: {hospital_id}")
    evidence = _load_inventory_evidence(
        config,
        ingestion_policy,
        inventory_policy,
        inventory_run_id,
    )
    manifest_mappings = evidence.manifest.get("hospital_mapping_contract")
    if not isinstance(manifest_mappings, list):
        raise IngestionError("Inventory lacks a frozen hospital mapping contract")
    manifest_mapping = next(
        (
            item
            for item in manifest_mappings
            if isinstance(item, dict)
            and item.get("source_folder") == mapping.source_folder
        ),
        None,
    )
    if (
        manifest_mapping is None
        or manifest_mapping.get("canonical_hospital_id") != hospital_id
        or manifest_mapping.get("cohort_action") != "include"
    ):
        raise IngestionError("Inventory hospital mapping differs from current policy")

    selected = [
        record
        for record in evidence.records
        if record.get("hospital_folder") == mapping.source_folder
    ]
    static_records = sorted(
        (
            record
            for record in selected
            if record.get("classification")
            == ingestion_policy.source_selection.static_classification
        ),
        key=lambda item: str(item["file_id"]),
    )
    dynamic_records = sorted(
        (
            record
            for record in selected
            if record.get("classification")
            == ingestion_policy.source_selection.dynamic_classification
        ),
        key=lambda item: str(item["file_id"]),
    )
    if len(static_records) != 1:
        raise IngestionError("Hospital requires exactly one selected static source")
    if not dynamic_records:
        raise IngestionError("Hospital requires at least one dynamic source file")
    dynamic_stay_ids = [record.get("filename_stay_id") for record in dynamic_records]
    if any(not isinstance(value, str) or not value for value in dynamic_stay_ids):
        raise IngestionError("Every dynamic source requires a filename-derived stay ID")
    if len(dynamic_stay_ids) != len(set(dynamic_stay_ids)):
        raise IngestionError("Dynamic filename-derived stay IDs must be unique")
    for record in (*static_records, *dynamic_records):
        if record.get("inspection_status") != "complete":
            raise IngestionError("Every selected source must have complete CSV evidence")
        if record.get("is_symbolic_link"):
            raise IngestionError("Selected source cannot be a symbolic link")

    excluded = [
        record
        for record in selected
        if record.get("classification")
        in ingestion_policy.source_selection.excluded_classifications
    ]
    handled_ids = {
        record["file_id"]
        for record in (*static_records, *dynamic_records, *excluded)
    }
    unhandled = [record for record in selected if record["file_id"] not in handled_ids]
    if unhandled:
        raise IngestionError(
            "Hospital inventory contains file classifications without an ingestion decision"
        )
    selected_ids = {record["file_id"] for record in (*static_records, *dynamic_records)}
    if selected_ids & {record["file_id"] for record in excluded}:
        raise IngestionError("An excluded source was selected for ingestion")
    for record in excluded:
        if record.get("classification") == "provisionally_excluded_archive":
            continue
        excluded_path = _validated_source_path(
            record, config.inventory.paths.raw_root
        )
        if sha256_file(excluded_path) != record["sha256"]:
            raise IngestionError(
                f"Excluded source hash changed after inventory for file ID {record['file_id']}"
            )

    final_directory = config.output_root / hospital_id
    if final_directory.exists():
        raise IngestionError(
            "Ingested hospital output already exists and will not be overwritten: "
            f"{final_directory}"
        )
    config.output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    config.output_root.chmod(0o700)
    with tempfile.TemporaryDirectory(
        prefix=f".{hospital_id}-ingestion-",
        dir=config.output_root,
    ) as temporary_directory:
        staged = Path(temporary_directory) / hospital_id
        staged.mkdir(mode=0o700)
        static_path = staged / "static.parquet"
        dynamic_path = staged / "dynamic.parquet"
        static_metrics, static_columns = _write_table(
            "static",
            static_records,
            static_path,
            mapping.source_folder,
            hospital_id,
            config,
            ingestion_policy,
            reporter,
        )
        dynamic_metrics, dynamic_columns = _write_table(
            "dynamic",
            dynamic_records,
            dynamic_path,
            mapping.source_folder,
            hospital_id,
            config,
            ingestion_policy,
            reporter,
        )
        carried_blockers = list(evidence.carried_blockers)
        global_archives = [
            record
            for record in evidence.records
            if record.get("classification") == "provisionally_excluded_archive"
        ]
        checkpoint_paths = evidence.manifest.get("checkpoint_directories_pruned", [])
        pooled_paths = evidence.manifest.get(
            "untrusted_pooled_directories_pruned", []
        )
        if not isinstance(checkpoint_paths, list) or not all(
            isinstance(path, str) for path in checkpoint_paths
        ):
            raise IngestionError("Inventory checkpoint-exclusion evidence is invalid")
        if not isinstance(pooled_paths, list) or not all(
            isinstance(path, str) for path in pooled_paths
        ):
            raise IngestionError("Inventory pooled-exclusion evidence is invalid")
        downstream_gates = [
            "numeric_token_parsing_policy_not_reviewed",
            "raw_to_translated_column_registry_not_reviewed",
            "translated_union_schema_not_approved",
        ]
        manifest = {
            "artifact": "asic_v3_lossless_hospital_ingestion",
            "artifact_version": ingestion_policy.ingestion_manifest_version,
            "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "dataset_context": config.dataset_context,
            "hospital": {
                "source_folder": mapping.source_folder,
                "canonical_hospital_id": hospital_id,
                "cohort_action": mapping.cohort_action,
                "mapping_approval_status": mapping.approval_status,
            },
            "input": {
                "authoritative_boundary": "raw_hospital_csv_files",
                "raw_root": str(config.inventory.paths.raw_root),
                "inventory_run_id": inventory_run_id,
                "inventory_directory": str(evidence.directory),
                "inventory_artifact_version": evidence.manifest["artifact_version"],
                "inventory_blockers_allowed_for_ingestion": carried_blockers,
            },
            "policies": {
                "inventory_policy_version": inventory_policy.policy_version,
                "ingestion_policy_version": ingestion_policy.policy_version,
                "ingestion_policy_path": str(ingestion_policy.source_path),
            },
            "value_representation": {
                "raw_cells": "string",
                "literal_empty_cell": "empty_string",
                "column_absent_from_source_schema": "parquet_null",
                "clinical_numeric_inference_applied": False,
                "textual_missing_policy_applied": False,
                "clinical_parsing_applied": False,
                "clinical_semantic_translation_applied": False,
                "duplicate_physical_names_disambiguated": True,
            },
            "source_selection": {
                "dynamic_filename_pattern": inventory_policy.dynamic_pattern_for(
                    mapping.source_folder
                ).pattern,
                "hospital_file_classification_counts": dict(
                    sorted(
                        Counter(
                            str(record["classification"]) for record in selected
                        ).items()
                    )
                ),
                "selected_static_file_count": len(static_records),
                "selected_dynamic_file_count": len(dynamic_records),
                "duplicate_dynamic_stay_count": 0,
                "unknown_or_unclassified_file_count": 0,
                "checkpoint_directories_pruned": [
                    path
                    for path in checkpoint_paths
                    if isinstance(path, str)
                    and (
                        path == mapping.source_folder
                        or path.startswith(f"{mapping.source_folder}/")
                    )
                ],
                "untrusted_pooled_directories_pruned": pooled_paths,
                "excluded_files": [
                    {
                        "file_id": record["file_id"],
                        "classification": record["classification"],
                        "source_filename": record["source_filename"],
                        "relative_path": record["relative_path"],
                        "sha256": record["sha256"],
                        "source_hash_revalidated": True,
                        "archive_resolution": _optional_mapping(
                            record, "archive_resolution"
                        ),
                    }
                    for record in excluded
                ],
                "global_provisional_archive_exclusions": [
                    {
                        "file_id": record["file_id"],
                        "hospital_folder": record["hospital_folder"],
                        "source_filename": record["source_filename"],
                        "relative_path": record["relative_path"],
                        "size_bytes": record["size_bytes"],
                        "sha256": record["sha256"],
                        "source_hash_revalidated": True,
                        "archive_resolution": _optional_mapping(
                            record, "archive_resolution"
                        ),
                    }
                    for record in global_archives
                ],
            },
            "raw_columns": {
                "static": [
                    {
                        "raw_name": item.identity.name,
                        "occurrence": item.identity.occurrence,
                        "physical_name": item.physical_name,
                        "physical_name_rule_id": item.physical_name_rule_id,
                    }
                    for item in static_columns
                ],
                "dynamic": [
                    {
                        "raw_name": item.identity.name,
                        "occurrence": item.identity.occurrence,
                        "physical_name": item.physical_name,
                        "physical_name_rule_id": item.physical_name_rule_id,
                    }
                    for item in dynamic_columns
                ],
                "review_status": "pending_translation_registry_review",
            },
            "tables": {"static": static_metrics, "dynamic": dynamic_metrics},
            "outputs": {
                "static": {
                    "filename": static_path.name,
                    "sha256": sha256_file(static_path),
                },
                "dynamic": {
                    "filename": dynamic_path.name,
                    "sha256": sha256_file(dynamic_path),
                },
            },
            "audit_status": "pass",
            "checks": {
                "only_inventory_selected_csvs_ingested": True,
                "reviewed_archive_excluded": not any(
                    record.get("classification")
                    == "provisionally_excluded_archive"
                    for record in (*static_records, *dynamic_records)
                ),
                "source_hashes_match_inventory": True,
                "source_headers_match_inventory": True,
                "source_rows_equal_output_rows": True,
                "filename_and_in_file_identifiers_agree": True,
                "raw_cells_written_as_strings": True,
                "literal_empty_and_absent_column_remain_distinguishable": True,
                "parquet_physical_column_names_unique": True,
                "duplicate_raw_occurrences_preserved": True,
                "source_order_preserved": True,
            },
            "publication": {
                "ready": False,
                "carried_inventory_blockers": carried_blockers,
                "pending_downstream_gates": downstream_gates,
            },
        }
        manifest_path = staged / "ingestion_manifest.json"
        _write_json_private(manifest_path, manifest)
        staged.replace(final_directory)
        final_directory.chmod(0o700)

    return IngestionResult(
        hospital_id=hospital_id,
        output_directory=final_directory,
        static_path=final_directory / "static.parquet",
        dynamic_path=final_directory / "dynamic.parquet",
        manifest_path=final_directory / "ingestion_manifest.json",
        static_row_count=static_metrics["output_row_count"],
        dynamic_row_count=dynamic_metrics["output_row_count"],
        publication_ready=False,
        publication_blockers=tuple(
            [*evidence.carried_blockers, *downstream_gates]
        ),
    )
