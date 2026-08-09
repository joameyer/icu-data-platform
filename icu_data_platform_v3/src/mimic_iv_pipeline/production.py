from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
from typing import Any, Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import yaml

from mimic_iv_pipeline.blocking import block_canonical_events
from mimic_iv_pipeline.cohort import select_cohort
from mimic_iv_pipeline.contracts import LoadedProfile, load_profile
from mimic_iv_pipeline.errors import MIMICPipelineError
from mimic_iv_pipeline.events import derive_observed_bmi, normalize_numeric_event
from mimic_iv_pipeline.phenotypes import phenotype_icd10


RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z(?:-[a-z0-9][a-z0-9_-]*)?$")
SOURCE_FILES = {
    "patients": "hosp/patients.csv.gz",
    "admissions": "hosp/admissions.csv.gz",
    "services": "hosp/services.csv.gz",
    "diagnoses_icd": "hosp/diagnoses_icd.csv.gz",
    "d_labitems": "hosp/d_labitems.csv.gz",
    "labevents": "hosp/labevents.csv.gz",
    "icustays": "icu/icustays.csv.gz",
    "chartevents": "icu/chartevents.csv.gz",
}


@dataclass(frozen=True)
class ProductionConfig:
    source_path: Path
    profile_path: Path
    source_root: Path
    candidate_root: Path
    audit_root: Path
    csv_block_size: int
    sqlite_insert_batch_rows: int
    parquet_batch_rows: int
    compression: str


@dataclass(frozen=True)
class SourceRule:
    variable: str
    conversion: str
    accepted_source_units: tuple[str | None, ...] | None
    value_kind: str


def _progress(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise MIMICPipelineError(f"Cannot read production config: {path}") from exc
    if not isinstance(value, dict):
        raise MIMICPipelineError("Production config must be a mapping")
    return value


def load_production_config(path: Path) -> ProductionConfig:
    source = path.expanduser().resolve()
    raw = _read_yaml(source)
    if raw.get("status") != "human_approved_for_operator_executed_nonrelease_candidate":
        raise MIMICPipelineError("Production execution is not human-approved")
    actions = raw.get("authorized_actions", {})
    required_true = (
        "read_mimic_iv_3_1_source",
        "build_run_scoped_candidate",
        "write_private_manifest_and_aggregate_audit",
    )
    if any(actions.get(key) is not True for key in required_true):
        raise MIMICPipelineError("Production config lacks required authorization")
    forbidden = ("create_release", "modify_pointer", "export_row_level_data", "train_model")
    if any(actions.get(key) is not False for key in forbidden):
        raise MIMICPipelineError("Production config does not fail closed")
    paths = raw.get("paths", {})
    execution = raw.get("execution", {})
    profile_value = raw.get("profile")
    if not isinstance(profile_value, str):
        raise MIMICPipelineError("Production profile path is missing")
    profile_path = (source.parent / profile_value).resolve()
    resolved_paths: dict[str, Path] = {}
    for name in ("source_root", "candidate_root", "audit_root"):
        value = paths.get(name) if isinstance(paths, dict) else None
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise MIMICPipelineError(f"Production {name} must be an absolute path")
        resolved_paths[name] = Path(value).resolve()
    source_root = resolved_paths["source_root"]
    candidate_root = resolved_paths["candidate_root"]
    audit_root = resolved_paths["audit_root"]
    if candidate_root == audit_root:
        raise MIMICPipelineError("Candidate and audit roots must differ")
    if candidate_root.is_relative_to(source_root) or audit_root.is_relative_to(source_root):
        raise MIMICPipelineError("Outputs cannot be written within the MIMIC source root")
    csv_block_size = int(execution.get("csv_block_size_bytes", 64 * 1024 * 1024))
    sqlite_rows = int(execution.get("sqlite_insert_batch_rows", 10_000))
    parquet_rows = int(execution.get("parquet_batch_rows", 10_000))
    if min(csv_block_size, sqlite_rows, parquet_rows) <= 0:
        raise MIMICPipelineError("Production batch sizes must be positive")
    compression = str(execution.get("parquet_compression", "zstd"))
    if compression != "zstd":
        raise MIMICPipelineError("Production Parquet compression must be zstd")
    return ProductionConfig(
        source_path=source,
        profile_path=profile_path,
        source_root=source_root,
        candidate_root=candidate_root,
        audit_root=audit_root,
        csv_block_size=csv_block_size,
        sqlite_insert_batch_rows=sqlite_rows,
        parquet_batch_rows=parquet_rows,
        compression=compression,
    )


def _source_paths(config: ProductionConfig) -> dict[str, Path]:
    paths = {name: config.source_root / relative for name, relative in SOURCE_FILES.items()}
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise MIMICPipelineError(f"Required MIMIC source files are missing: {missing}")
    return paths


def _iter_csv_batches(
    path: Path,
    *,
    columns: list[str],
    column_types: dict[str, pa.DataType],
    block_size: int,
) -> Iterator[pa.RecordBatch]:
    stream = pa.input_stream(str(path), compression="gzip")
    try:
        reader = pacsv.open_csv(
            stream,
            read_options=pacsv.ReadOptions(block_size=block_size, use_threads=True),
            parse_options=pacsv.ParseOptions(delimiter=","),
            convert_options=pacsv.ConvertOptions(
                include_columns=columns,
                column_types=column_types,
                strings_can_be_null=True,
                timestamp_parsers=["%Y-%m-%d %H:%M:%S", pacsv.ISO8601],
            ),
        )
        yield from reader
    finally:
        stream.close()


def _read_small_table(
    path: Path,
    *,
    columns: list[str],
    column_types: dict[str, pa.DataType],
    block_size: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    for batch in _iter_csv_batches(
        path, columns=columns, column_types=column_types, block_size=block_size
    ):
        for index, row in enumerate(batch.to_pylist()):
            row["__source_row_number"] = offset + index + 2
            rows.append(row)
        offset += batch.num_rows
    return rows


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


class _BufferedParquetWriter:
    def __init__(
        self, path: Path, schema: pa.Schema, *, compression: str, batch_rows: int
    ) -> None:
        self._writer = pq.ParquetWriter(path, schema, compression=compression)
        self._schema = schema
        self._batch_rows = batch_rows
        self._rows: list[dict[str, Any]] = []
        self.row_count = 0

    def append(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self._rows.append(row)
            if len(self._rows) >= self._batch_rows:
                self.flush()

    def flush(self) -> None:
        if not self._rows:
            return
        table = pa.Table.from_pylist(self._rows, schema=self._schema)
        self._writer.write_table(table)
        self.row_count += len(self._rows)
        self._rows.clear()

    def close(self) -> None:
        self.flush()
        self._writer.close()


def _static_schema(phenotypes: list[str]) -> pa.Schema:
    fields = [
        pa.field("dataset_id", pa.string(), False),
        pa.field("site_id", pa.string(), False),
        pa.field("subject_id", pa.int64(), False),
        pa.field("hadm_id", pa.int64(), False),
        pa.field("stay_id", pa.int64(), False),
        pa.field("intime", pa.timestamp("us"), True),
        pa.field("official_outtime", pa.timestamp("us"), True),
        pa.field("age_years", pa.int32(), True),
        pa.field("sex", pa.string(), True),
        pa.field("service_at_icu_intime", pa.string(), True),
        pa.field("service_attribution_time", pa.timestamp("us"), True),
        pa.field("service_source_order", pa.int64(), True),
        pa.field("patient_source_row_number", pa.int64(), True),
        pa.field("admission_source_row_number", pa.int64(), True),
        pa.field("icustay_source_row_number", pa.int64(), True),
        pa.field("discharge_location_normalized", pa.string(), True),
        pa.field("hospital_mortality", pa.int8(), True),
        pa.field("supervised_eligible", pa.bool_(), False),
        pa.field("exclusion_reasons", pa.list_(pa.string()), False),
        pa.field("height_cm", pa.float64(), True),
        pa.field("weight_kg", pa.float64(), True),
        pa.field("bmi", pa.float64(), True),
        pa.field("height_observation_count", pa.int32(), False),
        pa.field("weight_observation_count", pa.int32(), False),
        pa.field("has_icd9_diagnosis", pa.bool_(), False),
        pa.field("icd10_diagnosis_count", pa.int32(), False),
        pa.field("icd9_diagnosis_count", pa.int32(), False),
    ]
    fields.extend(pa.field(name, pa.bool_(), True) for name in phenotypes)
    return pa.schema(fields, metadata={b"privacy": b"cluster_private"})


CANONICAL_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string(), False),
        pa.field("site_id", pa.string(), False),
        pa.field("subject_id", pa.int64(), False),
        pa.field("hadm_id", pa.int64(), True),
        pa.field("stay_id", pa.int64(), False),
        pa.field("event_time", pa.timestamp("us"), False),
        pa.field("icu_intime", pa.timestamp("us"), False),
        pa.field("event_time_offset_us", pa.int64(), False),
        pa.field("variable", pa.string(), False),
        pa.field("role", pa.string(), False),
        pa.field("family", pa.string(), True),
        pa.field("source_table", pa.string(), False),
        pa.field("itemid", pa.int64(), True),
        pa.field("source_group_id", pa.string(), True),
        pa.field("source_value_text", pa.large_string(), True),
        pa.field("source_valuenum", pa.float64(), True),
        pa.field("source_unit", pa.string(), True),
        pa.field("source_row_number", pa.int64(), False),
        pa.field("source_file_id", pa.string(), False),
        pa.field("value", pa.float64(), True),
        pa.field("text_value", pa.large_string(), True),
        pa.field("unit", pa.string(), False),
        pa.field("conversion_id", pa.string(), True),
        pa.field("plausibility_status", pa.string(), False),
        pa.field("eligibility_status", pa.string(), False),
        pa.field("rejection_reasons", pa.list_(pa.string()), False),
    ],
    metadata={b"privacy": b"cluster_private", b"timestamp_semantics": b"MIMIC_shifted_timezone_naive"},
)


def _block_schema(profile: LoadedProfile) -> pa.Schema:
    fields = [
        pa.field("dataset_id", pa.string(), False),
        pa.field("site_id", pa.string(), False),
        pa.field("stay_id", pa.int64(), False),
        pa.field("block_index", pa.int32(), False),
        pa.field("resolution_minutes", pa.int32(), False),
        pa.field("extent_basis", pa.string(), False),
        pa.field("time_domain", pa.string(), False),
        pa.field("block_label", pa.string(), False),
        pa.field("block_start_h", pa.float64(), False),
        pa.field("block_end_h", pa.float64(), False),
        pa.field("nominal_block_end_h", pa.float64(), False),
        pa.field("information_cutoff_h", pa.float64(), False),
        pa.field("is_terminal_partial", pa.bool_(), False),
    ]
    selected = {
        name: spec
        for name, spec in profile.variables.items()
        if spec.role == "predictor" and spec.family is not None
    }
    for name in sorted(selected):
        for operation in selected[name].aggregations:
            fields.append(pa.field(f"{name}__{operation}", pa.float64(), True))
        fields.extend(
            [
                pa.field(f"{name}__observation_count", pa.int64(), False),
                pa.field(f"{name}__last_value", pa.float64(), True),
                pa.field(f"{name}__last_observation_time_h", pa.float64(), True),
            ]
        )
    fields.extend(
        [
            pa.field("block_observation_count", pa.int64(), False),
            pa.field("block_has_observations", pa.bool_(), False),
        ]
    )
    return pa.schema(fields, metadata={b"privacy": b"cluster_private"})


def _source_rules(profile: LoadedProfile, domain: str) -> dict[int, SourceRule]:
    raw_domain = profile.registries["item_sources"].get(domain, {})
    output: dict[int, SourceRule] = {}
    for variable, raw in raw_domain.items():
        entries = raw.get("sources") if isinstance(raw, dict) else None
        if entries is None:
            entries = [raw]
        for entry in entries:
            if not isinstance(entry, dict):
                raise MIMICPipelineError(f"Invalid source rule for {variable}")
            for itemid in entry.get("itemids", []):
                numeric = int(itemid)
                if numeric in output:
                    raise MIMICPipelineError(f"Duplicate selected itemid: {numeric}")
                output[numeric] = SourceRule(
                    variable=variable,
                    conversion=str(entry.get("conversion", "identity")),
                    accepted_source_units=(
                        tuple(entry.get("accepted_source_units", raw.get("accepted_source_units", [])))
                        if "accepted_source_units" in entry or "accepted_source_units" in raw
                        else None
                    ),
                    value_kind=str(entry.get("value_kind", raw.get("value_kind", "numeric"))),
                )
    return output


def _open_event_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(
        """
        CREATE TABLE events (
          dataset_id TEXT NOT NULL, site_id TEXT NOT NULL,
          subject_id INTEGER NOT NULL, hadm_id INTEGER, stay_id INTEGER NOT NULL,
          event_time TEXT NOT NULL, icu_intime TEXT NOT NULL,
          event_time_offset_us INTEGER NOT NULL, variable TEXT NOT NULL,
          role TEXT NOT NULL, family TEXT, source_table TEXT NOT NULL,
          itemid INTEGER, source_group_id TEXT, source_value_text TEXT,
          source_valuenum REAL, source_unit TEXT, source_row_number INTEGER NOT NULL,
          source_file_id TEXT NOT NULL, value REAL, text_value TEXT, unit TEXT NOT NULL,
          conversion_id TEXT, plausibility_status TEXT NOT NULL,
          eligibility_status TEXT NOT NULL, rejection_reasons TEXT NOT NULL
        )
        """
    )
    return connection


EVENT_COLUMNS = (
    "dataset_id", "site_id", "subject_id", "hadm_id", "stay_id",
    "event_time", "icu_intime", "event_time_offset_us", "variable", "role",
    "family", "source_table", "itemid", "source_group_id", "source_value_text",
    "source_valuenum", "source_unit", "source_row_number", "source_file_id",
    "value", "text_value", "unit", "conversion_id", "plausibility_status",
    "eligibility_status", "rejection_reasons",
)


def _database_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    values = dict(row)
    values["event_time"] = row["event_time"].isoformat(sep=" ")
    values["icu_intime"] = row["icu_intime"].isoformat(sep=" ")
    values["rejection_reasons"] = json.dumps(row.get("rejection_reasons", []))
    return tuple(values.get(name) for name in EVENT_COLUMNS)


def _insert_events(connection: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    placeholders = ",".join("?" for _ in EVENT_COLUMNS)
    connection.executemany(
        f"INSERT INTO events ({','.join(EVENT_COLUMNS)}) VALUES ({placeholders})",
        [_database_tuple(row) for row in rows],
    )


def _row_from_database(values: tuple[Any, ...]) -> dict[str, Any]:
    row = dict(zip(EVENT_COLUMNS, values, strict=True))
    row["event_time"] = datetime.fromisoformat(row["event_time"])
    row["icu_intime"] = datetime.fromisoformat(row["icu_intime"])
    row["rejection_reasons"] = json.loads(row["rejection_reasons"])
    return row


def _load_cohort_inputs(
    config: ProductionConfig, paths: dict[str, Path]
) -> tuple[list[dict[str, Any]], dict[int, dict[str, Any]]]:
    patients = _read_small_table(
        paths["patients"],
        columns=["subject_id", "gender", "anchor_age", "anchor_year"],
        column_types={
            "subject_id": pa.int64(), "gender": pa.string(),
            "anchor_age": pa.int32(), "anchor_year": pa.int32(),
        },
        block_size=config.csv_block_size,
    )
    admissions = _read_small_table(
        paths["admissions"],
        columns=["subject_id", "hadm_id", "discharge_location", "hospital_expire_flag"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(),
            "discharge_location": pa.string(), "hospital_expire_flag": pa.int8(),
        },
        block_size=config.csv_block_size,
    )
    icustays = _read_small_table(
        paths["icustays"],
        columns=["subject_id", "hadm_id", "stay_id", "intime", "outtime"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(), "stay_id": pa.int64(),
            "intime": pa.timestamp("us"), "outtime": pa.timestamp("us"),
        },
        block_size=config.csv_block_size,
    )
    hadm_ids = {row["hadm_id"] for row in icustays}
    services: list[dict[str, Any]] = []
    service_offset = 0
    for batch in _iter_csv_batches(
        paths["services"],
        columns=["subject_id", "hadm_id", "transfertime", "prev_service", "curr_service"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(),
            "transfertime": pa.timestamp("us"), "prev_service": pa.string(),
            "curr_service": pa.string(),
        },
        block_size=config.csv_block_size,
    ):
        for index, row in enumerate(batch.to_pylist()):
            if row["hadm_id"] in hadm_ids:
                row["__source_row_number"] = service_offset + index + 2
                services.append(row)
        service_offset += batch.num_rows
    cohort = select_cohort(icustays, patients, admissions, services)
    patient_by_subject = {int(row["subject_id"]): row for row in patients}
    for row in cohort:
        row["dataset_id"] = "mimic_iv_3_1"
        row["site_id"] = "mimic"
        patient = patient_by_subject.get(int(row["subject_id"]), {})
        row["sex"] = patient.get("gender")
    return cohort, patient_by_subject


def _validate_lab_dictionary(config: ProductionConfig, path: Path, selected: set[int]) -> None:
    labels: dict[int, str | None] = {}
    for batch in _iter_csv_batches(
        path,
        columns=["itemid", "label", "fluid", "category"],
        column_types={
            "itemid": pa.int64(), "label": pa.string(),
            "fluid": pa.string(), "category": pa.string(),
        },
        block_size=config.csv_block_size,
    ):
        for row in batch.to_pylist():
            if row["itemid"] in selected:
                labels[int(row["itemid"])] = row.get("label")
    missing = selected.difference(labels)
    if missing:
        raise MIMICPipelineError(f"Selected lab itemids missing from d_labitems: {sorted(missing)}")
    if "lipase" not in str(labels.get(50956, "")).lower():
        raise MIMICPipelineError("d_labitems 50956 no longer validates as Lipase")


def _unit_allowed(accepted: tuple[str | None, ...] | None, observed: object) -> bool:
    if accepted is None:
        return True
    if observed is None:
        return None in accepted
    normalize = lambda value: re.sub(r"[^a-z0-9]", "", str(value).lower())
    observed_token = normalize(observed)
    aliases = {
        "degc": {"degc", "c", "celsius"},
        "degf": {"degf", "f", "fahrenheit"},
        "inch": {"inch", "in", "inches"},
    }
    return any(
        expected is not None
        and observed_token in aliases.get(normalize(expected), {normalize(expected)})
        for expected in accepted
    )


def _canonical_numeric(
    raw: dict[str, Any], *, rule: SourceRule, profile: LoadedProfile,
    stay: dict[str, Any], source_table: str, source_file_id: str,
    row_number: int, group_id: object = None,
) -> dict[str, Any]:
    spec = profile.variables[rule.variable]
    normalized = normalize_numeric_event(
        {
            "dataset_id": "mimic_iv_3_1", "site_id": "mimic",
            "subject_id": int(stay["subject_id"]), "hadm_id": stay.get("hadm_id"),
            "stay_id": int(stay["stay_id"]), "event_time": raw["charttime"],
            "source_value": raw.get("valuenum"), "source_table": source_table,
            "itemid": int(raw["itemid"]), "source_unit": raw.get("valueuom"),
            "source_row_number": row_number, "source_file_id": source_file_id,
        },
        variable=spec,
        plausibility=profile.ranges.get(rule.variable) or profile.ranges["fluid_balance_24h"],
        conversion_id=rule.conversion,
        icu_intime=stay["intime"],
    )
    normalized.update(
        source_group_id=None if group_id is None else str(group_id),
        source_value_text=None if raw.get("value") is None else str(raw.get("value")),
        source_valuenum=raw.get("valuenum"),
        text_value=None,
    )
    normalized.pop("source_value", None)
    if not _unit_allowed(rule.accepted_source_units, raw.get("valueuom")):
        normalized["value"] = None
        normalized["plausibility_status"] = "rejected"
        normalized["eligibility_status"] = "rejected"
        normalized["rejection_reasons"].append("unexpected_or_missing_source_unit")
    return normalized


def _canonical_categorical(
    raw: dict[str, Any], *, rule: SourceRule, profile: LoadedProfile,
    stay: dict[str, Any], source_table: str, source_file_id: str, row_number: int,
    group_id: object = None,
) -> dict[str, Any]:
    text = None if raw.get("value") is None else str(raw.get("value")).strip()
    accepted = bool(text)
    event_time = raw["charttime"]
    spec = profile.variables[rule.variable]
    return {
        "dataset_id": "mimic_iv_3_1", "site_id": "mimic",
        "subject_id": int(stay["subject_id"]), "hadm_id": stay.get("hadm_id"),
        "stay_id": int(stay["stay_id"]), "event_time": event_time,
        "icu_intime": stay["intime"],
        "event_time_offset_us": int((event_time - stay["intime"]).total_seconds() * 1_000_000),
        "variable": rule.variable, "role": spec.role, "family": spec.family,
        "source_table": source_table, "itemid": int(raw["itemid"]),
        "source_group_id": None if group_id is None else str(group_id),
        "source_value_text": text, "source_valuenum": raw.get("valuenum"),
        "source_unit": raw.get("valueuom"), "source_row_number": row_number,
        "source_file_id": source_file_id, "value": None, "text_value": text,
        "unit": spec.unit, "conversion_id": None,
        "plausibility_status": "accepted" if accepted else "rejected",
        "eligibility_status": "audit_only" if accepted else "rejected",
        "rejection_reasons": [] if accepted else ["empty_categorical_value"],
    }


def _selected_rows(batch: pa.RecordBatch, selected: set[int], offset: int) -> Iterator[tuple[int, dict[str, Any]]]:
    itemids = batch.column(batch.schema.get_field_index("itemid")).to_pylist()
    indices = [index for index, value in enumerate(itemids) if value in selected]
    if not indices:
        return
    filtered = batch.take(pa.array(indices, type=pa.int64())).to_pylist()
    for index, row in zip(indices, filtered, strict=True):
        yield offset + index + 2, row


def _scan_chart_events(
    config: ProductionConfig, profile: LoadedProfile, path: Path,
    connection: sqlite3.Connection, eligible_by_stay: dict[int, dict[str, Any]],
    bmi_measurements: dict[int, list[dict[str, Any]]],
) -> Counter[str]:
    rules = _source_rules(profile, "chartevents")
    selected = set(rules)
    counts: Counter[str] = Counter()
    pending: list[dict[str, Any]] = []
    offset = 0
    next_progress = 25_000_000
    for batch in _iter_csv_batches(
        path,
        columns=["subject_id", "hadm_id", "stay_id", "charttime", "itemid", "value", "valuenum", "valueuom"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(), "stay_id": pa.int64(),
            "charttime": pa.timestamp("us"), "itemid": pa.int64(), "value": pa.string(),
            "valuenum": pa.float64(), "valueuom": pa.string(),
        },
        block_size=config.csv_block_size,
    ):
        for row_number, raw in _selected_rows(batch, selected, offset):
            counts["selected_item_rows"] += 1
            rule = rules[int(raw["itemid"])]
            counts[f"qc.selected_variable.{rule.variable}"] += 1
            counts[f"qc.source_unit.{rule.variable}.{raw.get('valueuom') or '<missing>'}"] += 1
            stay_id = raw.get("stay_id")
            stay = eligible_by_stay.get(stay_id)
            if stay is None:
                counts["outside_eligible_cohort"] += 1
                continue
            event_time = raw.get("charttime")
            if not isinstance(event_time, datetime):
                counts["missing_event_time"] += 1
                continue
            if event_time < stay["intime"] - timedelta(hours=72) or event_time > stay["official_outtime"]:
                counts["outside_contracted_time"] += 1
                continue
            if rule.value_kind == "categorical":
                event = _canonical_categorical(
                    raw, rule=rule, profile=profile, stay=stay,
                    source_table="chartevents", source_file_id="icu/chartevents.csv.gz",
                    row_number=row_number,
                )
            else:
                event = _canonical_numeric(
                    raw, rule=rule, profile=profile, stay=stay,
                    source_table="chartevents", source_file_id="icu/chartevents.csv.gz",
                    row_number=row_number,
                )
            pending.append(event)
            counts[f"canonical_{event['eligibility_status']}"] += 1
            counts[f"qc.canonical_variable.{rule.variable}"] += 1
            for reason in event["rejection_reasons"]:
                counts[f"qc.rejection_reason.{rule.variable}.{reason}"] += 1
            if rule.variable in {"height_cm", "weight_kg"}:
                bmi_measurements[int(stay_id)].append(event)
            if len(pending) >= config.sqlite_insert_batch_rows:
                _insert_events(connection, pending)
                connection.commit()
                pending.clear()
        offset += batch.num_rows
        if offset >= next_progress:
            _progress(f"chartevents scanned rows={offset:,} selected={counts['selected_item_rows']:,}")
            next_progress += 25_000_000
    _insert_events(connection, pending)
    connection.commit()
    return counts


def _scan_lab_events(
    config: ProductionConfig, profile: LoadedProfile, path: Path,
    connection: sqlite3.Connection, eligible_by_hadm: dict[int, dict[str, Any]],
) -> Counter[str]:
    rules = _source_rules(profile, "labevents")
    selected = set(rules)
    counts: Counter[str] = Counter()
    pending: list[dict[str, Any]] = []
    offset = 0
    next_progress = 10_000_000
    for batch in _iter_csv_batches(
        path,
        columns=["subject_id", "hadm_id", "specimen_id", "itemid", "charttime", "value", "valuenum", "valueuom"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(), "specimen_id": pa.int64(),
            "itemid": pa.int64(), "charttime": pa.timestamp("us"), "value": pa.string(),
            "valuenum": pa.float64(), "valueuom": pa.string(),
        },
        block_size=config.csv_block_size,
    ):
        for row_number, raw in _selected_rows(batch, selected, offset):
            counts["selected_item_rows"] += 1
            rule = rules[int(raw["itemid"])]
            counts[f"qc.selected_variable.{rule.variable}"] += 1
            counts[f"qc.source_unit.{rule.variable}.{raw.get('valueuom') or '<missing>'}"] += 1
            hadm_id = raw.get("hadm_id")
            stay = eligible_by_hadm.get(hadm_id)
            if stay is None:
                counts["outside_eligible_cohort"] += 1
                continue
            event_time = raw.get("charttime")
            if not isinstance(event_time, datetime):
                counts["missing_event_time"] += 1
                continue
            if event_time < stay["intime"] - timedelta(hours=72) or event_time > stay["official_outtime"]:
                counts["outside_contracted_time"] += 1
                continue
            if rule.value_kind == "categorical":
                event = _canonical_categorical(
                    raw, rule=rule, profile=profile, stay=stay,
                    source_table="labevents", source_file_id="hosp/labevents.csv.gz",
                    row_number=row_number, group_id=raw.get("specimen_id"),
                )
            else:
                event = _canonical_numeric(
                    raw, rule=rule, profile=profile, stay=stay,
                    source_table="labevents", source_file_id="hosp/labevents.csv.gz",
                    row_number=row_number, group_id=raw.get("specimen_id"),
                )
            pending.append(event)
            counts[f"canonical_{event['eligibility_status']}"] += 1
            counts[f"qc.canonical_variable.{rule.variable}"] += 1
            for reason in event["rejection_reasons"]:
                counts[f"qc.rejection_reason.{rule.variable}.{reason}"] += 1
            if len(pending) >= config.sqlite_insert_batch_rows:
                _insert_events(connection, pending)
                connection.commit()
                pending.clear()
        offset += batch.num_rows
        if offset >= next_progress:
            _progress(f"labevents scanned rows={offset:,} selected={counts['selected_item_rows']:,}")
            next_progress += 10_000_000
    _insert_events(connection, pending)
    connection.commit()
    return counts


def _scan_diagnoses(
    config: ProductionConfig, profile: LoadedProfile, path: Path,
    eligible_by_hadm: dict[int, dict[str, Any]],
) -> tuple[dict[int, dict[str, bool | None]], dict[int, Counter[str]]]:
    by_stay: dict[int, list[dict[str, Any]]] = defaultdict(list)
    coverage: dict[int, Counter[str]] = defaultdict(Counter)
    for batch in _iter_csv_batches(
        path,
        columns=["subject_id", "hadm_id", "seq_num", "icd_code", "icd_version"],
        column_types={
            "subject_id": pa.int64(), "hadm_id": pa.int64(), "seq_num": pa.int32(),
            "icd_code": pa.string(), "icd_version": pa.int8(),
        },
        block_size=config.csv_block_size,
    ):
        for row in batch.to_pylist():
            stay = eligible_by_hadm.get(row.get("hadm_id"))
            if stay is None:
                continue
            stay_id = int(stay["stay_id"])
            by_stay[stay_id].append(row)
            coverage[stay_id][f"icd{row.get('icd_version')}"] += 1
    prefixes = profile.registries["chronic_icd10"].get("phenotypes", {})
    phenotypes = {
        stay_id: phenotype_icd10(rows, prefixes) for stay_id, rows in by_stay.items()
    }
    return phenotypes, coverage


def _prepare_static_rows(
    cohort: list[dict[str, Any]], profile: LoadedProfile,
    bmi_measurements: dict[int, list[dict[str, Any]]],
    phenotypes: dict[int, dict[str, bool | None]],
    diagnosis_coverage: dict[int, Counter[str]],
) -> list[dict[str, Any]]:
    names = list(profile.registries["chronic_icd10"]["phenotypes"])
    output: list[dict[str, Any]] = []
    for stay in cohort:
        stay_id = int(stay["stay_id"])
        bmi = derive_observed_bmi(
            bmi_measurements.get(stay_id, []),
            intime=stay["intime"], outtime=stay["official_outtime"],
        ) if isinstance(stay.get("intime"), datetime) and isinstance(stay.get("official_outtime"), datetime) else {
            "height_cm": None, "weight_kg": None, "bmi": None,
            "height_observation_count": 0, "weight_observation_count": 0,
        }
        coverage = diagnosis_coverage.get(stay_id, Counter())
        flags = (
            phenotypes.get(stay_id, {name: False for name in names})
            if stay.get("supervised_eligible")
            else {name: None for name in names}
        )
        row = {
            "dataset_id": "mimic_iv_3_1", "site_id": "mimic",
            "subject_id": stay["subject_id"], "hadm_id": stay["hadm_id"],
            "stay_id": stay_id, "intime": stay.get("intime"),
            "official_outtime": stay.get("official_outtime"),
            "age_years": stay.get("age_years"), "sex": stay.get("sex"),
            "service_at_icu_intime": stay.get("service_at_icu_intime"),
            "service_attribution_time": stay.get("service_attribution_time"),
            "service_source_order": stay.get("service_source_order"),
            "patient_source_row_number": stay.get("patient_source_row_number"),
            "admission_source_row_number": stay.get("admission_source_row_number"),
            "icustay_source_row_number": stay.get("icustay_source_row_number"),
            "discharge_location_normalized": stay.get("discharge_location_normalized"),
            "hospital_mortality": stay.get("hospital_mortality"),
            "supervised_eligible": bool(stay.get("supervised_eligible")),
            "exclusion_reasons": stay.get("exclusion_reasons", []),
            **{key: bmi[key] for key in ("height_cm", "weight_kg", "bmi", "height_observation_count", "weight_observation_count")},
            "has_icd9_diagnosis": coverage["icd9"] > 0,
            "icd10_diagnosis_count": coverage["icd10"],
            "icd9_diagnosis_count": coverage["icd9"],
            **flags,
        }
        output.append(row)
    return output


def _event_groups(connection: sqlite3.Connection) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    cursor = connection.execute(
        f"SELECT {','.join(EVENT_COLUMNS)} FROM events "
        "ORDER BY stay_id, event_time_offset_us, variable, source_table, itemid, source_row_number"
    )
    current_stay: int | None = None
    rows: list[dict[str, Any]] = []
    while values := cursor.fetchone():
        row = _row_from_database(values)
        stay_id = int(row["stay_id"])
        if current_stay is None:
            current_stay = stay_id
        elif stay_id != current_stay:
            yield current_stay, rows
            current_stay, rows = stay_id, []
        rows.append(row)
    if current_stay is not None:
        yield current_stay, rows


def _contract_hashes(config: ProductionConfig) -> dict[str, str]:
    profile_raw = _read_yaml(config.profile_path)
    paths = {"profile": config.profile_path, "production_execution": config.source_path}
    for group in ("contracts", "registries"):
        for name, relative in profile_raw.get(group, {}).items():
            paths[f"{group}.{name}"] = (config.profile_path.parent / relative).resolve()
    return {name: _sha256(path) for name, path in sorted(paths.items())}


def _source_inventory(paths: dict[str, Path], root: Path) -> list[dict[str, Any]]:
    rows = []
    for name, path in sorted(paths.items()):
        stat = path.stat()
        rows.append(
            {
                "logical_source": name,
                "relative_path": path.relative_to(root).as_posix(),
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return rows


def _source_release_evidence(root: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "declared_release": "MIMIC-IV 3.1",
        "declaration_basis": "data_owner_confirmed_cluster_source_root",
    }
    checksum_file = root / "SHA256SUMS.txt"
    if checksum_file.is_file():
        evidence["checksum_manifest"] = {
            "relative_path": checksum_file.relative_to(root).as_posix(),
            "sha256": _sha256(checksum_file),
            "size_bytes": checksum_file.stat().st_size,
        }
    else:
        evidence["checksum_manifest"] = None
    return evidence


def _split_scan_counts(counts: Counter[str]) -> tuple[dict[str, int], dict[str, int]]:
    disposition = {
        key: value for key, value in sorted(counts.items()) if not key.startswith("qc.")
    }
    qc = {
        key.removeprefix("qc."): value
        for key, value in sorted(counts.items())
        if key.startswith("qc.")
    }
    return disposition, qc


def build_production_candidate(
    config_path: Path, *, run_id: str
) -> Path:
    if not RUN_ID.fullmatch(run_id):
        raise MIMICPipelineError("Run ID must be UTC-like YYYYMMDDTHHMMSSZ")
    config = load_production_config(config_path)
    allocated_cpus = int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))
    pa.set_cpu_count(max(1, allocated_cpus))
    profile = load_profile(config.profile_path)
    paths = _source_paths(config)
    config.candidate_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    final_dir = config.candidate_root / run_id
    staging_dir = config.candidate_root / f".{run_id}.staging"
    if final_dir.exists() or staging_dir.exists():
        raise MIMICPipelineError("Run-scoped candidate or staging directory already exists")
    staging_dir.mkdir(mode=0o700)
    started = datetime.now(timezone.utc).isoformat()

    _progress("loading cohort source tables")
    cohort, _ = _load_cohort_inputs(config, paths)
    eligible = [row for row in cohort if row["supervised_eligible"]]
    eligible_by_stay = {int(row["stay_id"]): row for row in eligible}
    eligible_by_hadm: dict[int, dict[str, Any]] = {}
    for row in eligible:
        hadm_id = int(row["hadm_id"])
        if hadm_id in eligible_by_hadm:
            raise MIMICPipelineError("More than one eligible ICU stay has the same hadm_id")
        eligible_by_hadm[hadm_id] = row

    lab_rules = _source_rules(profile, "labevents")
    _validate_lab_dictionary(config, paths["d_labitems"], set(lab_rules))
    database_path = staging_dir / "event_sort.sqlite"
    connection = _open_event_database(database_path)
    bmi_measurements: dict[int, list[dict[str, Any]]] = defaultdict(list)
    _progress(f"scanning chartevents for {len(eligible):,} eligible stays")
    chart_counts = _scan_chart_events(
        config, profile, paths["chartevents"], connection,
        eligible_by_stay, bmi_measurements,
    )
    _progress("scanning labevents")
    lab_counts = _scan_lab_events(
        config, profile, paths["labevents"], connection, eligible_by_hadm
    )
    connection.execute(
        "CREATE INDEX events_order ON events "
        "(stay_id, event_time_offset_us, variable, source_table, itemid, source_row_number)"
    )
    connection.commit()

    _progress("scanning diagnoses and building static cohort")
    phenotypes, diagnosis_coverage = _scan_diagnoses(
        config, profile, paths["diagnoses_icd"], eligible_by_hadm
    )
    static_rows = _prepare_static_rows(
        cohort, profile, bmi_measurements, phenotypes, diagnosis_coverage
    )
    static_path = staging_dir / "static_cohort.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            static_rows,
            schema=_static_schema(list(profile.registries["chronic_icd10"]["phenotypes"])),
        ),
        static_path,
        compression=config.compression,
    )

    canonical_path = staging_dir / "canonical_selected_events.parquet"
    block_15m_path = staging_dir / "blocked_15m.parquet"
    block_8h_path = staging_dir / "blocked_8h.parquet"
    canonical_writer = _BufferedParquetWriter(
        canonical_path, CANONICAL_SCHEMA,
        compression=config.compression, batch_rows=config.parquet_batch_rows,
    )
    writer_15m = _BufferedParquetWriter(
        block_15m_path, _block_schema(profile),
        compression=config.compression, batch_rows=config.parquet_batch_rows,
    )
    writer_8h = _BufferedParquetWriter(
        block_8h_path, _block_schema(profile),
        compression=config.compression, batch_rows=config.parquet_batch_rows,
    )
    groups = iter(_event_groups(connection))
    pending_group = next(groups, None)
    for stay in sorted(eligible, key=lambda row: int(row["stay_id"])):
        stay_id = int(stay["stay_id"])
        events: list[dict[str, Any]] = []
        if pending_group is not None and pending_group[0] < stay_id:
            raise MIMICPipelineError("Event database contains an unexpected stay")
        if pending_group is not None and pending_group[0] == stay_id:
            events = pending_group[1]
            pending_group = next(groups, None)
        canonical_writer.append(events)
        writer_15m.append(
            block_canonical_events(
                events, [stay], profile.variables, resolution_minutes=15
            )
        )
        writer_8h.append(
            block_canonical_events(
                events, [stay], profile.variables, resolution_minutes=480
            )
        )
    if pending_group is not None:
        raise MIMICPipelineError("Unconsumed event rows remain after stay merge")
    canonical_writer.close()
    writer_15m.close()
    writer_8h.close()
    _progress("finished canonical, 15-minute, and 8-hour parquet writers")
    connection.close()
    for suffix in ("", "-wal", "-shm"):
        temporary = Path(f"{database_path}{suffix}")
        if temporary.exists():
            temporary.unlink()

    artifacts = {}
    for name, path in (
        ("static_cohort", static_path),
        ("canonical_selected_events", canonical_path),
        ("blocked_15m", block_15m_path),
        ("blocked_8h", block_8h_path),
    ):
        artifacts[name] = {
            "file": path.name,
            "row_count": pq.ParquetFile(path).metadata.num_rows,
            "sha256": _sha256(path),
        }
    chart_disposition, chart_qc = _split_scan_counts(chart_counts)
    lab_disposition, lab_qc = _split_scan_counts(lab_counts)
    manifest = {
        "artifact": "mimic_iv_3_1_selected_layer_candidate",
        "artifact_version": "0.1",
        "status": "nonrelease_candidate_requires_independent_audit",
        "run_id": run_id,
        "started_at_utc": started,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "privacy": "cluster_private",
        "source_root": str(config.source_root),
        "source_release_evidence": _source_release_evidence(config.source_root),
        "source_inventory": _source_inventory(paths, config.source_root),
        "contract_sha256": _contract_hashes(config),
        "cohort_counts": {
            "candidate_stays": len(cohort),
            "supervised_eligible_stays": len(eligible),
            "excluded_stays": len(cohort) - len(eligible),
        },
        "source_disposition_counts": {
            "chartevents": chart_disposition,
            "labevents": lab_disposition,
        },
        "private_aggregate_qc_counts": {
            "chartevents": chart_qc,
            "labevents": lab_qc,
        },
        "clinical_gates": {
            "fluid_balance_24h": "unresolved_all_null_in_blocks",
            "arterial_specimen": "unresolved_analytes_all_null_in_blocks",
            "bun_to_urea": "unresolved_urea_all_null_in_blocks",
            "icd9_phenotypes": "unresolved_negative_flags_are_null_when_icd9_present",
        },
        "lineage": {
            "static_cohort": "MIMIC-IV 3.1 source tables",
            "canonical_selected_events": "MIMIC-IV 3.1 selected source rows",
            "blocked_15m": "canonical_selected_events",
            "blocked_8h": "canonical_selected_events",
            "blocked_8h_reads_blocked_15m": False,
        },
        "artifacts": artifacts,
        "prohibited_actions_performed": [],
    }
    _write_json(staging_dir / "manifest.json", manifest)
    staging_dir.rename(final_dir)
    _progress(f"candidate finalized at {final_dir}")
    return final_dir


def _parquet_sum(path: Path, column: str, batch_rows: int) -> int:
    total = 0
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=batch_rows, columns=[column]
    ):
        value = pc.sum(batch.column(0)).as_py()
        total += int(value or 0)
    return total


def _canonical_block_eligible_count(
    path: Path, profile: LoadedProfile, batch_rows: int
) -> int:
    enabled = {
        name
        for name, spec in profile.variables.items()
        if spec.role == "predictor" and spec.family is not None and spec.eligibility == "enabled"
    }
    count = 0
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=batch_rows,
        columns=["variable", "value", "eligibility_status"],
    ):
        values = batch.to_pydict()
        count += sum(
            1
            for variable, value, eligibility in zip(
                values["variable"], values["value"], values["eligibility_status"], strict=True
            )
            if variable in enabled and value is not None and eligibility == "eligible"
        )
    return count


def _column_nonnull_count(path: Path, column: str, batch_rows: int) -> int:
    parquet = pq.ParquetFile(path)
    column_index = parquet.schema_arrow.get_field_index(column)
    if column_index < 0:
        raise MIMICPipelineError(f"Parquet column does not exist: {column}")
    metadata_count = 0
    metadata_complete = True
    for row_group_index in range(parquet.metadata.num_row_groups):
        row_group = parquet.metadata.row_group(row_group_index)
        statistics = row_group.column(column_index).statistics
        if statistics is None or not statistics.has_null_count:
            metadata_complete = False
            break
        metadata_count += row_group.num_rows - statistics.null_count
    if metadata_complete:
        return metadata_count
    count = 0
    for batch in parquet.iter_batches(
        batch_size=batch_rows, columns=[column]
    ):
        count += batch.num_rows - batch.column(0).null_count
    return count


def audit_production_candidate(config_path: Path, *, run_id: str) -> Path:
    if not RUN_ID.fullmatch(run_id):
        raise MIMICPipelineError("Run ID must be UTC-like YYYYMMDDTHHMMSSZ")
    config = load_production_config(config_path)
    profile = load_profile(config.profile_path)
    candidate = config.candidate_root / run_id
    manifest_path = candidate / "manifest.json"
    if not manifest_path.is_file():
        raise MIMICPipelineError("Candidate manifest does not exist")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    findings: list[str] = []
    if manifest.get("run_id") != run_id:
        findings.append("manifest_run_id_mismatch")
    lineage = manifest.get("lineage", {})
    if lineage.get("blocked_8h") != "canonical_selected_events":
        findings.append("blocked_8h_direct_parent_invalid")
    if lineage.get("blocked_8h_reads_blocked_15m") is not False:
        findings.append("blocked_8h_depends_on_15m")

    artifact_paths: dict[str, Path] = {}
    for name, evidence in manifest.get("artifacts", {}).items():
        path = candidate / evidence["file"]
        artifact_paths[name] = path
        if not path.is_file():
            findings.append(f"missing_artifact:{name}")
            continue
        if _sha256(path) != evidence.get("sha256"):
            findings.append(f"sha256_mismatch:{name}")
        if pq.ParquetFile(path).metadata.num_rows != evidence.get("row_count"):
            findings.append(f"row_count_mismatch:{name}")
    required = {"static_cohort", "canonical_selected_events", "blocked_15m", "blocked_8h"}
    if set(artifact_paths) != required:
        findings.append("artifact_set_mismatch")

    canonical_count = None
    block_counts: dict[str, int] = {}
    if required.issubset(artifact_paths) and not any(not artifact_paths[name].is_file() for name in required):
        static_rows = pq.read_table(
            artifact_paths["static_cohort"],
            columns=["stay_id", "intime", "official_outtime", "supervised_eligible"],
        ).to_pylist()
        stay_ids = [int(row["stay_id"]) for row in static_rows]
        if len(stay_ids) != len(set(stay_ids)):
            findings.append("static_stay_id_not_unique")
        eligible_static = [row for row in static_rows if row["supervised_eligible"]]
        if len(eligible_static) != manifest.get("cohort_counts", {}).get("supervised_eligible_stays"):
            findings.append("eligible_static_count_mismatch")
        for name, resolution in (("blocked_15m", 15), ("blocked_8h", 480)):
            pre_count = math.ceil(72 * 60 / resolution)
            expected_rows = 0
            for row in eligible_static:
                extent_minutes = (
                    row["official_outtime"] - row["intime"]
                ).total_seconds() / 60
                expected_rows += pre_count + 1 + math.ceil(extent_minutes / resolution)
            observed_rows = pq.ParquetFile(artifact_paths[name]).metadata.num_rows
            if observed_rows != expected_rows:
                findings.append(f"complete_block_grid_row_count_failed:{name}")
        canonical_count = _canonical_block_eligible_count(
            artifact_paths["canonical_selected_events"], profile, config.parquet_batch_rows
        )
        for name in ("blocked_15m", "blocked_8h"):
            block_counts[name] = _parquet_sum(
                artifact_paths[name], "block_observation_count", config.parquet_batch_rows
            )
            if block_counts[name] != canonical_count:
                findings.append(f"event_to_block_conservation_failed:{name}")

        ventilation = {
            name for name, spec in profile.variables.items() if spec.role == "ventilation_audit"
        }
        block_names = set(pq.ParquetFile(artifact_paths["blocked_8h"]).schema_arrow.names)
        if any(any(field.startswith(f"{name}__") for field in block_names) for name in ventilation):
            findings.append("ventilation_marker_in_predictor_blocks")

        gated = {
            name
            for name, spec in profile.variables.items()
            if spec.role == "predictor" and spec.family is not None and spec.eligibility == "human_gate"
        }
        for block_name in ("blocked_15m", "blocked_8h"):
            path = artifact_paths[block_name]
            schema_names = set(pq.ParquetFile(path).schema_arrow.names)
            for variable in gated:
                value_columns = [
                    name for name in schema_names
                    if name.startswith(f"{variable}__")
                    and not name.endswith("__observation_count")
                    and not name.endswith("__last_observation_time_h")
                ]
                if not value_columns:
                    findings.append(f"gated_schema_missing:{block_name}:{variable}")
                for column in value_columns:
                    if _column_nonnull_count(path, column, config.parquet_batch_rows):
                        findings.append(f"gated_value_not_null:{block_name}:{column}")
                count_column = f"{variable}__observation_count"
                if count_column not in schema_names or _parquet_sum(path, count_column, config.parquet_batch_rows):
                    findings.append(f"gated_observation_count_nonzero:{block_name}:{variable}")

    for domain, counts in manifest.get("source_disposition_counts", {}).items():
        selected = int(counts.get("selected_item_rows", 0))
        dispositions = sum(
            int(value)
            for key, value in counts.items()
            if key != "selected_item_rows"
        )
        if selected != dispositions:
            findings.append(f"source_disposition_conservation_failed:{domain}")

    config.audit_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    audit_dir = config.audit_root / run_id
    if audit_dir.exists():
        raise MIMICPipelineError("Run-scoped audit directory already exists")
    audit_dir.mkdir(mode=0o700)
    report = {
        "artifact": "mimic_iv_3_1_candidate_independent_audit",
        "artifact_version": "0.1",
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS" if not findings else "FAIL",
        "privacy": "cluster_private_aggregate_evidence",
        "candidate_manifest_sha256": _sha256(manifest_path),
        "canonical_block_eligible_event_count": canonical_count,
        "blocked_observation_counts": block_counts,
        "findings": findings,
        "release_authorized": False,
        "pointer_change_authorized": False,
    }
    report_path = audit_dir / "audit.json"
    _write_json(report_path, report)
    if findings:
        raise MIMICPipelineError(
            f"Independent audit failed; private report: {report_path}"
        )
    return report_path
