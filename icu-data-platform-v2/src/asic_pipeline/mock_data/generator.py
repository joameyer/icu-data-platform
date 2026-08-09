"""Generate privacy-protecting mock data from pooled production ASIC data.

The generator samples ICU stays within every hospital, shuffles source values
within hospital boundaries, remaps stay identifiers, and preserves protected
join/time columns. It is a development-data utility, not a clinical pipeline
stage. Generated data retain exact source values and must remain protected.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
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
from asic_pipeline.errors import InputReadError
from asic_pipeline.mock_data.config import MockGenerationConfig, ShuffleBlock


ProgressReporter = Callable[[str], None]


@dataclass(frozen=True)
class MockGenerationResult:
    static_path: Path
    dynamic_path: Path
    manifest_path: Path
    static_row_count: int
    dynamic_row_count: int
    stay_count: int
    audit_status: str


def report_progress(message: str) -> None:
    """Print a timestamped progress message immediately."""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def _emit_progress(reporter: ProgressReporter | None, message: str) -> None:
    if reporter is not None:
        reporter(message)


def _should_report_iteration(current: int, total: int) -> bool:
    interval = max(1, (total + 9) // 10)
    return current == total or current % interval == 0


def _extract_hospital_codes(pseudo_ids: pd.Series) -> pd.Series:
    """Extract integral hospital suffixes from `<stay>:<hospital>` IDs."""

    values = pseudo_ids.astype("string")
    missing = values.isna() | values.eq("")
    suffixes = values.str.extract(r"^.+:(\d+)\Z", expand=False)
    malformed = ~missing & suffixes.isna()
    if missing.any() or malformed.any():
        raise ValueError(
            "Pseudo-ID contains missing or malformed values; expected "
            "'<stay>:<hospital>' for every row. "
            f"missing={int(missing.sum())}, malformed={int(malformed.sum())}"
        )
    return pd.to_numeric(suffixes).astype(int)


def _validate_hid_agreement(
    frame: pd.DataFrame,
    hospital_codes: pd.Series,
    table_name: str,
) -> None:
    raw_hids = frame["hid"]
    numeric_hids = pd.to_numeric(raw_hids, errors="coerce")
    missing = raw_hids.isna()
    non_integral = ~missing & (
        numeric_hids.isna() | ~numeric_hids.mod(1).eq(0)
    )
    comparable = ~missing & ~non_integral
    mismatches = hospital_codes[comparable].ne(
        numeric_hids[comparable].astype(int)
    )
    if missing.any() or non_integral.any() or mismatches.any():
        raise ValueError(
            f"{table_name} hid values do not agree with Pseudo-ID suffixes; "
            f"missing={int(missing.sum())}, non_integral={int(non_integral.sum())}, "
            f"mismatched={int(mismatches.sum())}"
        )


def _require_columns(
    frame: pd.DataFrame,
    required: set[str],
    table_name: str,
) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise KeyError(f"{table_name} is missing configured columns: {missing}")


def _validate_static_data(
    static_df: pd.DataFrame,
    protected_columns: Sequence[str],
    shuffle_blocks: Sequence[ShuffleBlock],
    expected_hospital_codes: Sequence[int],
) -> pd.Series:
    block_columns = {
        column for block in shuffle_blocks for column in block.columns
    }
    _require_columns(
        static_df,
        set(protected_columns) | block_columns,
        "static table",
    )
    if static_df["Pseudo-ID"].duplicated().any():
        duplicate_count = int(
            static_df["Pseudo-ID"].duplicated(keep=False).sum()
        )
        raise ValueError(
            "Expected exactly one static row per Pseudo-ID; "
            f"duplicate_rows={duplicate_count}"
        )

    hospitals = _extract_hospital_codes(static_df["Pseudo-ID"])
    _validate_hid_agreement(static_df, hospitals, "Static")
    observed = set(hospitals.unique())
    expected = set(expected_hospital_codes)
    if observed != expected:
        raise ValueError(
            "Static hospital set does not match the source contract; "
            f"observed={sorted(observed)}, expected={sorted(expected)}"
        )
    return hospitals


def _sample_stays_per_hospital(
    static_df: pd.DataFrame,
    hospitals: pd.Series,
    stays_per_hospital: int,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.Series]:
    selected_positions: list[int] = []
    for hospital in sorted(hospitals.unique()):
        hospital_positions = np.flatnonzero(hospitals.eq(hospital).to_numpy())
        source_ids = static_df["Pseudo-ID"].iloc[hospital_positions]
        hospital_positions = hospital_positions[
            np.argsort(source_ids.astype(str).to_numpy())
        ]
        if len(hospital_positions) < stays_per_hospital:
            raise ValueError(
                f"Hospital {hospital} has {len(hospital_positions)} stays; "
                f"{stays_per_hospital} were requested"
            )
        selected_positions.extend(
            rng.choice(
                hospital_positions,
                size=stays_per_hospital,
                replace=False,
            ).tolist()
        )

    mock_df = static_df.iloc[selected_positions].copy().reset_index(drop=True)
    return mock_df, _extract_hospital_codes(mock_df["Pseudo-ID"])


def _shuffle_column_preserving_missingness(
    frame: pd.DataFrame,
    column: str,
    row_indices: np.ndarray,
    rng: np.random.Generator,
) -> None:
    values = frame.loc[row_indices, column].copy()
    non_missing = values.notna()
    shuffled = values.loc[non_missing].iloc[
        rng.permutation(int(non_missing.sum()))
    ]
    values.loc[non_missing] = shuffled.array
    frame.loc[row_indices, column] = values.array


def _shuffle_column_block(
    frame: pd.DataFrame,
    columns: Sequence[str],
    row_indices: np.ndarray,
    rng: np.random.Generator,
) -> None:
    permutation = rng.permutation(len(row_indices))
    shuffled = frame.loc[row_indices, list(columns)].iloc[permutation].copy()
    shuffled.index = row_indices
    frame.loc[row_indices, list(columns)] = shuffled


def _shuffle_static_within_hospitals(
    mock_df: pd.DataFrame,
    hospitals: pd.Series,
    protected_columns: Sequence[str],
    shuffle_blocks: Sequence[ShuffleBlock],
    rng: np.random.Generator,
) -> None:
    block_columns = {
        column for block in shuffle_blocks for column in block.columns
    }
    excluded = set(protected_columns) | block_columns
    independent_columns = [
        column for column in mock_df.columns if column not in excluded
    ]
    for hospital in sorted(hospitals.unique()):
        row_indices = mock_df.index[hospitals.eq(hospital)].to_numpy()
        for column in independent_columns:
            _shuffle_column_preserving_missingness(
                mock_df,
                column,
                row_indices,
                rng,
            )
        for block in shuffle_blocks:
            _shuffle_column_block(
                mock_df,
                block.columns,
                row_indices,
                rng,
            )


def _remap_stay_ids(
    mock_df: pd.DataFrame,
    hospitals: pd.Series,
    rng: np.random.Generator,
) -> dict[str, str]:
    source_ids = mock_df["Pseudo-ID"].astype("string")
    remapped_ids = pd.Series(index=mock_df.index, dtype="string")
    for hospital in sorted(hospitals.unique()):
        row_indices = mock_df.index[hospitals.eq(hospital)].to_numpy()
        new_stay_numbers = rng.permutation(len(row_indices)) + 1
        remapped_ids.loc[row_indices] = [
            f"{stay_number}:{hospital}" for stay_number in new_stay_numbers
        ]

    mock_df["Pseudo-ID"] = remapped_ids.astype(object)
    return dict(zip(source_ids.tolist(), remapped_ids.tolist(), strict=True))


def _generate_mock_static_data_and_mapping(
    static_df: pd.DataFrame,
    config: MockGenerationConfig,
    contract: InputContract,
    rng: np.random.Generator,
    reporter: ProgressReporter | None = None,
) -> tuple[pd.DataFrame, dict[str, str]]:
    _emit_progress(reporter, "Validating the production static table.")
    hospitals = _validate_static_data(
        static_df,
        config.protected_static_columns,
        config.static_shuffle_blocks,
        contract.expected_hospital_codes,
    )
    _emit_progress(
        reporter,
        f"Sampling {config.stays_per_hospital} ICU stays from each of "
        f"{hospitals.nunique()} hospitals.",
    )
    mock_df, mock_hospitals = _sample_stays_per_hospital(
        static_df,
        hospitals,
        config.stays_per_hospital,
        rng,
    )
    _emit_progress(
        reporter,
        f"Selected {len(mock_df):,} static stays; shuffling within hospitals.",
    )
    _shuffle_static_within_hospitals(
        mock_df,
        mock_hospitals,
        config.protected_static_columns,
        config.static_shuffle_blocks,
        rng,
    )
    _emit_progress(reporter, "Remapping selected ICU-stay identifiers.")
    stay_id_mapping = _remap_stay_ids(mock_df, mock_hospitals, rng)
    return mock_df, stay_id_mapping


def _validate_dynamic_data(
    dynamic_df: pd.DataFrame,
    protected_columns: Sequence[str],
    expected_hospital_codes: Sequence[int],
) -> pd.Series:
    _require_columns(dynamic_df, set(protected_columns), "dynamic table")
    hospitals = _extract_hospital_codes(dynamic_df["Pseudo-ID"])
    _validate_hid_agreement(dynamic_df, hospitals, "Dynamic")
    unexpected = sorted(set(hospitals.unique()) - set(expected_hospital_codes))
    if unexpected:
        raise ValueError(f"Dynamic table contains unexpected hospitals: {unexpected}")
    return hospitals


def _shuffle_and_remap_dynamic(
    selected_df: pd.DataFrame,
    stay_id_mapping: Mapping[str, str],
    config: MockGenerationConfig,
    contract: InputContract,
    rng: np.random.Generator,
    reporter: ProgressReporter | None = None,
) -> pd.DataFrame:
    mock_df = selected_df.copy().reset_index(drop=True)
    hospitals = _validate_dynamic_data(
        mock_df,
        config.protected_dynamic_columns,
        contract.expected_hospital_codes,
    )
    present_ids = set(mock_df["Pseudo-ID"].astype("string"))
    missing_count = len(set(stay_id_mapping) - present_ids)
    if missing_count:
        raise ValueError(
            f"Dynamic data have no rows for {missing_count} selected ICU stays"
        )

    shuffle_columns = [
        column
        for column in mock_df.columns
        if column not in config.protected_dynamic_columns
    ]
    _emit_progress(
        reporter,
        f"Shuffling {len(shuffle_columns)} dynamic columns independently "
        "within hospitals.",
    )
    for hospital in sorted(hospitals.unique()):
        row_indices = mock_df.index[hospitals.eq(hospital)].to_numpy()
        _emit_progress(
            reporter,
            f"Shuffling {len(row_indices):,} dynamic rows for hospital {hospital}.",
        )
        for column in shuffle_columns:
            _shuffle_column_preserving_missingness(
                mock_df,
                column,
                row_indices,
                rng,
            )

    _emit_progress(reporter, "Applying the in-memory ICU-stay ID mapping.")
    mock_df["Pseudo-ID"] = (
        mock_df["Pseudo-ID"]
        .astype("string")
        .map(stay_id_mapping)
        .astype(object)
    )
    if mock_df["Pseudo-ID"].isna().any():
        raise ValueError("Dynamic ID remapping produced missing Pseudo-ID values")

    actual_stays = mock_df["Pseudo-ID"].nunique()
    if actual_stays != len(stay_id_mapping):
        raise ValueError(
            f"Expected {len(stay_id_mapping)} dynamic stays, found {actual_stays}"
        )
    return mock_df


def generate_mock_data(
    static_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    config: MockGenerationConfig,
    contract: InputContract,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate matching in-memory mock tables for tests and small datasets."""

    rng = np.random.default_rng(config.seed)
    mock_static, stay_mapping = _generate_mock_static_data_and_mapping(
        static_df,
        config,
        contract,
        rng,
    )
    selected = dynamic_df[
        dynamic_df["Pseudo-ID"].astype("string").isin(stay_mapping)
    ].copy()
    mock_dynamic = _shuffle_and_remap_dynamic(
        selected,
        stay_mapping,
        config,
        contract,
        rng,
    )
    return mock_static, mock_dynamic


def generate_mock_dynamic_data_from_parquet(
    dynamic_file: Path,
    stay_id_mapping: Mapping[str, str],
    config: MockGenerationConfig,
    contract: InputContract,
    rng: np.random.Generator,
    reporter: ProgressReporter | None = None,
) -> pd.DataFrame:
    """Extract selected dynamic rows by row group, then shuffle and remap."""

    parquet = pq.ParquetFile(dynamic_file)
    if parquet.num_row_groups == 0:
        raise ValueError("The production dynamic Parquet file has no row groups")
    missing = sorted(
        set(config.protected_dynamic_columns) - set(parquet.schema_arrow.names)
    )
    if missing:
        raise KeyError(f"Dynamic Parquet file is missing protected columns: {missing}")

    _emit_progress(
        reporter,
        f"Dynamic source contains {parquet.metadata.num_rows:,} rows in "
        f"{parquet.num_row_groups:,} row groups.",
    )
    selected_ids = sorted(stay_id_mapping)
    relevant_row_groups: list[int] = []
    _emit_progress(
        reporter,
        "Scanning dynamic row groups using only the Pseudo-ID column.",
    )
    for row_group in range(parquet.num_row_groups):
        id_column = parquet.read_row_group(
            row_group,
            columns=["Pseudo-ID"],
        ).column("Pseudo-ID").combine_chunks()
        selected_array = pa.array(selected_ids, type=id_column.type)
        if pc.any(pc.is_in(id_column, value_set=selected_array)).as_py():
            relevant_row_groups.append(row_group)
        completed = row_group + 1
        if _should_report_iteration(completed, parquet.num_row_groups):
            _emit_progress(
                reporter,
                f"Scanned {completed:,}/{parquet.num_row_groups:,} row groups; "
                f"{len(relevant_row_groups):,} are relevant.",
            )
    if not relevant_row_groups:
        raise ValueError("No dynamic row groups contain the selected ICU stays")

    selected_parts: list[pd.DataFrame] = []
    selected_row_count = 0
    for position, row_group in enumerate(relevant_row_groups, start=1):
        table = parquet.read_row_group(row_group)
        id_column = table.column("Pseudo-ID").combine_chunks()
        selected_array = pa.array(selected_ids, type=id_column.type)
        selected_table = table.filter(pc.is_in(id_column, value_set=selected_array))
        if selected_table.num_rows:
            selected_parts.append(selected_table.to_pandas())
            selected_row_count += selected_table.num_rows
        if _should_report_iteration(position, len(relevant_row_groups)):
            _emit_progress(
                reporter,
                f"Processed {position:,}/{len(relevant_row_groups):,} relevant "
                f"row groups; retained {selected_row_count:,} rows.",
            )
    if not selected_parts:
        raise ValueError("No selected dynamic rows were extracted")

    selected_df = pd.concat(selected_parts, ignore_index=True)
    return _shuffle_and_remap_dynamic(
        selected_df,
        stay_id_mapping,
        config,
        contract,
        rng,
        reporter,
    )


def _assert_parquet_schema(
    path: Path,
    table_contract: TableContract,
    dataset_context: str,
    table_name: str,
) -> pq.ParquetFile:
    if not path.is_file():
        raise InputReadError(f"{table_name} Parquet file does not exist: {path}")
    parquet = pq.ParquetFile(path)
    observed = [(field.name, str(field.type)) for field in parquet.schema_arrow]
    expected = [
        (column.name, column.arrow_type_for(dataset_context))
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
            f"{table_name} schema does not match contract context "
            f"{dataset_context!r}; column_order_matches="
            f"{observed_names == expected_names}, type_mismatches={type_mismatches}"
        )
    return parquet


def _audit_staged_mock(
    staging_dir: Path,
    config: MockGenerationConfig,
    contract: InputContract,
) -> str:
    audit_config = PipelineConfig(
        dataset_context="mock",
        contract_path=config.source_contract_path,
        paths=PathConfig(
            pooled=staging_dir,
            reports=staging_dir / "reports" / "mock",
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
            f"Generated mock data failed contract audit: {failures}"
        )
    return report.overall_status


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


def _arrow_type_from_contract(type_name: str) -> pa.DataType:
    known_types: dict[str, pa.DataType] = {
        "string": pa.string(),
        "large_string": pa.large_string(),
        "int8": pa.int8(),
        "int32": pa.int32(),
        "float": pa.float32(),
        "double": pa.float64(),
        "timestamp[us]": pa.timestamp("us"),
    }
    try:
        return known_types[type_name]
    except KeyError as exc:
        raise ValueError(
            f"Mock writer does not support contract Arrow type {type_name!r}"
        ) from exc


def _write_mock_parquet(
    frame: pd.DataFrame,
    path: Path,
    table_contract: TableContract,
) -> None:
    """Write the exact frozen mock physical schema with safe Arrow casts."""

    schema = pa.schema(
        [
            pa.field(
                column.name,
                _arrow_type_from_contract(column.arrow_type_for("mock")),
            )
            for column in table_contract.columns
        ]
    )
    table = pa.Table.from_pandas(
        frame,
        schema=schema,
        preserve_index=False,
        safe=True,
    )
    pq.write_table(table, path)


def generate_and_write_mock_data(
    config: MockGenerationConfig,
    *,
    overwrite: bool = False,
    reporter: ProgressReporter | None = report_progress,
) -> MockGenerationResult:
    """Generate, validate, and publish pooled mock Parquet files."""

    contract = load_input_contract(config.source_contract_path)
    static_source = config.source_pooled_dir / contract.static.filename
    dynamic_source = config.source_pooled_dir / contract.dynamic.filename
    static_output = config.output_pooled_dir / contract.static.filename
    dynamic_output = config.output_pooled_dir / contract.dynamic.filename

    static_parquet = _assert_parquet_schema(
        static_source,
        contract.static,
        "production",
        "Production static",
    )
    dynamic_parquet = _assert_parquet_schema(
        dynamic_source,
        contract.dynamic,
        "production",
        "Production dynamic",
    )

    existing = [
        path
        for path in (static_output, dynamic_output, config.manifest_path)
        if path.exists()
    ]
    if existing and not overwrite:
        raise FileExistsError(
            "Mock outputs already exist. Rerun with --overwrite only after explicit "
            "replacement approval; "
            f"existing={[str(path) for path in existing]}"
        )

    _emit_progress(reporter, f"Reading production static data: {static_source}")
    static_df = pd.read_parquet(static_source)
    rng = np.random.default_rng(config.seed)
    mock_static, stay_mapping = _generate_mock_static_data_and_mapping(
        static_df,
        config,
        contract,
        rng,
        reporter,
    )
    _emit_progress(
        reporter,
        f"Static mock generation complete: {len(mock_static):,} stays.",
    )
    mock_dynamic = generate_mock_dynamic_data_from_parquet(
        dynamic_source,
        stay_mapping,
        config,
        contract,
        rng,
        reporter,
    )
    _emit_progress(
        reporter,
        f"Dynamic mock generation complete: {len(mock_dynamic):,} rows.",
    )

    config.output_pooled_dir.parent.mkdir(parents=True, exist_ok=True)
    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".asic-mock-build-",
        dir=config.output_pooled_dir.parent,
    ) as temporary_directory:
        staging_dir = Path(temporary_directory) / "pooled"
        staging_dir.mkdir()
        staged_static = staging_dir / contract.static.filename
        staged_dynamic = staging_dir / contract.dynamic.filename
        _write_mock_parquet(mock_static, staged_static, contract.static)
        _write_mock_parquet(mock_dynamic, staged_dynamic, contract.dynamic)

        _emit_progress(
            reporter,
            "Auditing staged mock data against contract "
            f"v{contract.contract_version}.",
        )
        audit_status = _audit_staged_mock(staging_dir, config, contract)
        hospital_counts = (
            _extract_hospital_codes(mock_static["Pseudo-ID"])
            .value_counts()
            .sort_index()
        )
        manifest = {
            "artifact": "asic_pooled_mock",
            "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "generator_version": __version__,
            "source_contract_version": contract.contract_version,
            "configuration": str(config.source_path),
            "seed": config.seed,
            "stays_per_hospital": config.stays_per_hospital,
            "source": {
                "pooled_directory": str(config.source_pooled_dir),
                "static_rows": static_parquet.metadata.num_rows,
                "dynamic_rows": dynamic_parquet.metadata.num_rows,
            },
            "output": {
                "pooled_directory": str(config.output_pooled_dir),
                "static_rows": len(mock_static),
                "dynamic_rows": len(mock_dynamic),
                "unique_stays": mock_static["Pseudo-ID"].nunique(),
                "stays_by_hospital": {
                    f"asic_UK{int(code):02d}": int(count)
                    for code, count in hospital_counts.items()
                },
            },
            "contract_audit_status": audit_status,
            "privacy_notice": (
                "Derived from protected project data; suitable only for software "
                "development and not for clinical/statistical inference or redistribution."
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
        f"Mock generation finished: static={len(mock_static):,}, "
        f"dynamic={len(mock_dynamic):,}, audit={audit_status}.",
    )
    return MockGenerationResult(
        static_path=static_output,
        dynamic_path=dynamic_output,
        manifest_path=config.manifest_path,
        static_row_count=len(mock_static),
        dynamic_row_count=len(mock_dynamic),
        stay_count=mock_static["Pseudo-ID"].nunique(),
        audit_status=audit_status,
    )
