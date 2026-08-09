"""Generate an unchanged stay-level subset of pooled ASIC production data.

The demo is protected real data. Selection is the only operation: no values,
identifiers, columns, types, or selected dynamic rows are transformed.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import tempfile
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline import __version__
from asic_pipeline.audit import audit_pooled_input
from asic_pipeline.config import AuditConfig, PathConfig, PipelineConfig
from asic_pipeline.contracts import InputContract, load_input_contract
from asic_pipeline.contracts.pooled import TableContract
from asic_pipeline.demo_data.config import DemoGenerationConfig
from asic_pipeline.errors import InputReadError


ProgressReporter = Callable[[str], None]
_PSEUDO_ID_PATTERN = re.compile(r"^.+:(\d+)$")


@dataclass(frozen=True)
class DemoGenerationResult:
    static_path: Path
    dynamic_path: Path
    manifest_path: Path
    static_row_count: int
    dynamic_row_count: int
    stay_count: int
    audit_status: str


def report_progress(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _emit_progress(reporter: ProgressReporter | None, message: str) -> None:
    if reporter is not None:
        reporter(message)


def _should_report_iteration(current: int, total: int) -> bool:
    interval = max(1, (total + 9) // 10)
    return current == total or current % interval == 0


def _assert_parquet_schema(
    path: Path,
    table_contract: TableContract,
    table_name: str,
) -> pq.ParquetFile:
    if not path.is_file():
        raise InputReadError(f"{table_name} Parquet file does not exist: {path}")
    parquet = pq.ParquetFile(path)
    observed = [(field.name, str(field.type)) for field in parquet.schema_arrow]
    expected = [
        (column.name, column.arrow_type_for("production"))
        for column in table_contract.columns
    ]
    if observed != expected:
        observed_names = [name for name, _ in observed]
        expected_names = [name for name, _ in expected]
        observed_types = dict(observed)
        expected_types = dict(expected)
        type_mismatches = [
            (name, observed_types[name], expected_types[name])
            for name in expected_names
            if name in observed_types and observed_types[name] != expected_types[name]
        ]
        raise ValueError(
            f"{table_name} schema does not match the production contract; "
            f"column_order_matches={observed_names == expected_names}, "
            f"type_mismatches={type_mismatches}"
        )
    return parquet


def _validate_and_extract_static_hospitals(
    static_table: pa.Table,
    contract: InputContract,
) -> tuple[pd.DataFrame, pd.Series]:
    identity = static_table.select(
        [contract.stay_id_column, contract.hospital_id_column]
    ).to_pandas()
    stay_ids = identity[contract.stay_id_column].astype("string")
    missing_ids = stay_ids.isna() | stay_ids.eq("")
    suffixes = stay_ids.str.extract(_PSEUDO_ID_PATTERN, expand=False)
    malformed_ids = ~missing_ids & suffixes.isna()
    if missing_ids.any() or malformed_ids.any():
        raise ValueError(
            "Production static Pseudo-ID values must be present and end in "
            "':<hospital>'; "
            f"missing={int(missing_ids.sum())}, "
            f"malformed={int(malformed_ids.sum())}"
        )
    if stay_ids.duplicated().any():
        raise ValueError(
            "Production static data must contain exactly one row per Pseudo-ID; "
            f"duplicate_rows={int(stay_ids.duplicated(keep=False).sum())}"
        )

    hospitals = pd.to_numeric(suffixes).astype(int)
    raw_hids = identity[contract.hospital_id_column]
    numeric_hids = pd.to_numeric(raw_hids, errors="coerce")
    missing_hids = raw_hids.isna()
    non_integral_hids = ~missing_hids & (
        numeric_hids.isna() | ~numeric_hids.mod(1).eq(0)
    )
    comparable = ~missing_hids & ~non_integral_hids
    mismatches = hospitals[comparable].ne(
        numeric_hids[comparable].astype(int)
    )
    if missing_hids.any() or non_integral_hids.any() or mismatches.any():
        raise ValueError(
            "Production static hid values must agree with Pseudo-ID suffixes; "
            f"missing={int(missing_hids.sum())}, "
            f"non_integral={int(non_integral_hids.sum())}, "
            f"mismatched={int(mismatches.sum())}"
        )

    observed = set(hospitals.unique())
    expected = set(contract.expected_hospital_codes)
    if observed != expected:
        raise ValueError(
            "Production static hospital set does not match the source contract; "
            f"observed={sorted(observed)}, expected={sorted(expected)}"
        )
    return identity, hospitals


def _sample_static_rows(
    static_table: pa.Table,
    contract: InputContract,
    stays_per_hospital: int,
    rng: np.random.Generator,
) -> tuple[pa.Table, list[str], Counter[int]]:
    identity, hospitals = _validate_and_extract_static_hospitals(
        static_table,
        contract,
    )
    selected_positions: list[int] = []
    for hospital in sorted(hospitals.unique()):
        hospital_positions = np.flatnonzero(hospitals.eq(hospital).to_numpy())
        source_ids = identity[contract.stay_id_column].iloc[hospital_positions]
        stable_positions = hospital_positions[
            np.argsort(source_ids.astype(str).to_numpy())
        ]
        if len(stable_positions) < stays_per_hospital:
            raise ValueError(
                f"Hospital {hospital} has {len(stable_positions)} stays; "
                f"{stays_per_hospital} were requested"
            )
        selected_positions.extend(
            rng.choice(
                stable_positions,
                size=stays_per_hospital,
                replace=False,
            ).tolist()
        )

    # Preserve the source static-row order after selecting the reproducible set.
    selected_positions.sort()
    selected_static = static_table.take(
        pa.array(selected_positions, type=pa.int64())
    )
    selected_ids = [
        str(value)
        for value in selected_static.column(contract.stay_id_column).to_pylist()
    ]
    hospital_counts = Counter(
        int(value.rsplit(":", 1)[1]) for value in selected_ids
    )
    return selected_static, selected_ids, hospital_counts


def _extract_dynamic_rows(
    dynamic_parquet: pq.ParquetFile,
    selected_ids: Sequence[str],
    stay_id_column: str,
    reporter: ProgressReporter | None,
) -> pa.Table:
    if dynamic_parquet.num_row_groups == 0:
        raise ValueError("The production dynamic Parquet file has no row groups")

    id_type = dynamic_parquet.schema_arrow.field(stay_id_column).type
    selected_array = pa.array(sorted(selected_ids), type=id_type)
    selected_parts: list[pa.Table] = []
    selected_row_count = 0
    _emit_progress(
        reporter,
        f"Scanning {dynamic_parquet.num_row_groups:,} dynamic row groups using "
        f"only {stay_id_column}.",
    )
    for row_group in range(dynamic_parquet.num_row_groups):
        id_column = dynamic_parquet.read_row_group(
            row_group,
            columns=[stay_id_column],
        ).column(stay_id_column).combine_chunks()
        mask = pc.is_in(id_column, value_set=selected_array)
        if pc.any(mask).as_py():
            table = dynamic_parquet.read_row_group(row_group)
            full_mask = pc.is_in(
                table.column(stay_id_column).combine_chunks(),
                value_set=selected_array,
            )
            selected = table.filter(full_mask)
            if selected.num_rows:
                selected_parts.append(selected)
                selected_row_count += selected.num_rows

        completed = row_group + 1
        if _should_report_iteration(completed, dynamic_parquet.num_row_groups):
            _emit_progress(
                reporter,
                f"Scanned {completed:,}/{dynamic_parquet.num_row_groups:,} row "
                f"groups; retained {selected_row_count:,} rows.",
            )

    if not selected_parts:
        raise ValueError("No dynamic rows contain the selected ICU stays")
    selected_dynamic = pa.concat_tables(selected_parts)
    present_ids = {
        str(value)
        for value in selected_dynamic.column(stay_id_column).to_pylist()
    }
    missing_stays = set(selected_ids) - present_ids
    if missing_stays:
        raise ValueError(
            f"Dynamic data have no rows for {len(missing_stays)} selected ICU stays"
        )
    return selected_dynamic


def _audit_staged_demo(
    staging_dir: Path,
    config: DemoGenerationConfig,
    contract: InputContract,
) -> str:
    audit_config = PipelineConfig(
        dataset_context="demo",
        contract_path=config.source_contract_path,
        paths=PathConfig(
            pooled=staging_dir,
            reports=staging_dir / "reports" / "demo",
        ),
        audit=AuditConfig(
            batch_size=100_000,
            check_duplicate_dynamic_keys=True,
            time_anchor="2020-01-01 00:00:00",
            time_tolerance_seconds=0.0,
        ),
        source_path=config.source_path,
    )
    report = audit_pooled_input(audit_config, contract)
    if report.overall_status != "pass":
        failures = [
            check.name for check in report.checks if check.status != "pass"
        ]
        raise ValueError(
            f"Generated demo data failed the pooled-input contract audit: {failures}"
        )
    return report.overall_status


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


def generate_and_write_demo_data(
    config: DemoGenerationConfig,
    *,
    overwrite: bool = False,
    reporter: ProgressReporter | None = report_progress,
) -> DemoGenerationResult:
    """Select, validate, and publish an unchanged pooled production subset."""

    contract = load_input_contract(config.source_contract_path)
    static_source = config.source_pooled_dir / contract.static.filename
    dynamic_source = config.source_pooled_dir / contract.dynamic.filename
    static_output = config.output_pooled_dir / contract.static.filename
    dynamic_output = config.output_pooled_dir / contract.dynamic.filename

    static_parquet = _assert_parquet_schema(
        static_source,
        contract.static,
        "Production static",
    )
    dynamic_parquet = _assert_parquet_schema(
        dynamic_source,
        contract.dynamic,
        "Production dynamic",
    )
    existing = [
        path
        for path in (static_output, dynamic_output, config.manifest_path)
        if path.exists()
    ]
    if existing and not overwrite:
        raise FileExistsError(
            "Demo outputs already exist. Rerun with --overwrite only after explicit "
            "replacement approval; "
            f"existing={[str(path) for path in existing]}"
        )

    _emit_progress(reporter, f"Reading production static data: {static_source}")
    static_table = static_parquet.read()
    rng = np.random.default_rng(config.seed)
    selected_static, selected_ids, hospital_counts = _sample_static_rows(
        static_table,
        contract,
        config.stays_per_hospital,
        rng,
    )
    _emit_progress(
        reporter,
        f"Selected {len(selected_ids):,} unchanged ICU stays from static data.",
    )
    selected_dynamic = _extract_dynamic_rows(
        dynamic_parquet,
        selected_ids,
        contract.stay_id_column,
        reporter,
    )
    _emit_progress(
        reporter,
        f"Selected {selected_dynamic.num_rows:,} unchanged dynamic rows.",
    )

    config.output_pooled_dir.parent.mkdir(parents=True, exist_ok=True)
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".asic-demo-build-",
        dir=config.output_pooled_dir.parent,
    ) as temporary_directory:
        staging_dir = Path(temporary_directory) / "pooled"
        staging_dir.mkdir()
        staged_static = staging_dir / contract.static.filename
        staged_dynamic = staging_dir / contract.dynamic.filename
        pq.write_table(selected_static, staged_static)
        pq.write_table(selected_dynamic, staged_dynamic)

        _emit_progress(
            reporter,
            "Auditing staged demo data against contract "
            f"v{contract.contract_version} using production Arrow types.",
        )
        audit_status = _audit_staged_demo(staging_dir, config, contract)
        manifest = {
            "artifact": "asic_pooled_demo",
            "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "generator_version": __version__,
            "source_contract_version": contract.contract_version,
            "configuration": str(config.source_path),
            "seed": config.seed,
            "stays_per_hospital": config.stays_per_hospital,
            "selection_only": True,
            "identifiers_remapped": False,
            "values_transformed": False,
            "source": {
                "pooled_directory": str(config.source_pooled_dir),
                "static_rows": static_parquet.metadata.num_rows,
                "dynamic_rows": dynamic_parquet.metadata.num_rows,
            },
            "output": {
                "pooled_directory": str(config.output_pooled_dir),
                "static_rows": selected_static.num_rows,
                "dynamic_rows": selected_dynamic.num_rows,
                "unique_stays": len(selected_ids),
                "stays_by_hospital": {
                    f"asic_UK{code:02d}": int(count)
                    for code, count in sorted(hospital_counts.items())
                },
            },
            "contract_audit_status": audit_status,
            "privacy_notice": (
                "Contains unchanged real project data and real Pseudo-ID values; "
                "keep only in authorized cluster storage and do not redistribute."
            ),
        }
        staged_manifest = Path(temporary_directory) / "manifest.json"
        _write_manifest(staged_manifest, manifest)

        config.output_pooled_dir.mkdir(parents=True, exist_ok=True)
        staged_static.replace(static_output)
        staged_dynamic.replace(dynamic_output)
        staged_manifest.replace(config.manifest_path)

    _emit_progress(
        reporter,
        f"Demo generation finished: static={selected_static.num_rows:,}, "
        f"dynamic={selected_dynamic.num_rows:,}, audit={audit_status}.",
    )
    return DemoGenerationResult(
        static_path=static_output,
        dynamic_path=dynamic_output,
        manifest_path=config.manifest_path,
        static_row_count=selected_static.num_rows,
        dynamic_row_count=selected_dynamic.num_rows,
        stay_count=len(selected_ids),
        audit_status=audit_status,
    )

