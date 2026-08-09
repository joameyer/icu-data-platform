from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Literal

import pandas as pd

from asic_pipeline.audit.report import (
    AuditReport,
    CheckResult,
    overall_status,
    utc_timestamp,
)
from asic_pipeline.config import PipelineConfig
from asic_pipeline.contracts import InputContract
from asic_pipeline.contracts.pooled import TableContract
from asic_pipeline.io import PooledInput, PooledTable, open_pooled_input


_PSEUDO_ID_PATTERN = r"^.+:(\d+)\Z"


@dataclass
class _IdentifierStats:
    scanned_rows: int = 0
    missing_stay_ids: int = 0
    malformed_stay_ids: int = 0
    missing_hids: int = 0
    non_integral_hids: int = 0
    suffix_hid_mismatches: int = 0
    hospital_counts: Counter[int] = field(default_factory=Counter)
    hid_counts: Counter[int] = field(default_factory=Counter)
    stay_ids: set[str] = field(default_factory=set)
    duplicate_stay_id_rows: int = 0

    def add_batch(
        self,
        frame: pd.DataFrame,
        stay_id_column: str,
        hospital_id_column: str,
        track_duplicate_stays: bool,
    ) -> None:
        self.scanned_rows += int(frame.shape[0])

        stay_ids = frame[stay_id_column].astype("string")
        raw_hids = frame[hospital_id_column]
        numeric_hids = pd.to_numeric(raw_hids, errors="coerce")

        missing_stay_mask = stay_ids.isna() | stay_ids.eq("")
        suffixes = stay_ids.str.extract(_PSEUDO_ID_PATTERN, expand=False)
        malformed_mask = ~missing_stay_mask & suffixes.isna()
        missing_hid_mask = raw_hids.isna()
        integral_hid_mask = numeric_hids.notna() & numeric_hids.mod(1).eq(0)
        non_integral_hid_mask = ~missing_hid_mask & ~integral_hid_mask

        self.missing_stay_ids += int(missing_stay_mask.sum())
        self.malformed_stay_ids += int(malformed_mask.sum())
        self.missing_hids += int(missing_hid_mask.sum())
        self.non_integral_hids += int(non_integral_hid_mask.sum())

        valid_suffixes = pd.to_numeric(suffixes, errors="coerce").dropna().astype(int)
        valid_hids = numeric_hids[integral_hid_mask].astype(int)
        self.hospital_counts.update(valid_suffixes.tolist())
        self.hid_counts.update(valid_hids.tolist())

        comparable_mask = suffixes.notna() & integral_hid_mask
        comparable_suffixes = pd.to_numeric(suffixes[comparable_mask]).astype(int)
        comparable_hids = numeric_hids[comparable_mask].astype(int)
        self.suffix_hid_mismatches += int(
            comparable_suffixes.ne(comparable_hids).sum()
        )

        for stay_id in stay_ids[~missing_stay_mask].tolist():
            value = str(stay_id)
            if track_duplicate_stays and value in self.stay_ids:
                self.duplicate_stay_id_rows += 1
            self.stay_ids.add(value)


@dataclass
class _TimeStats:
    scanned_rows: int = 0
    missing_or_invalid_relative_time: int = 0
    missing_or_invalid_anchored_time: int = 0
    compared_rows: int = 0
    disagreement_rows: int = 0
    max_absolute_difference_seconds: float | None = None
    minimum_relative_minutes: float | None = None
    maximum_relative_minutes: float | None = None

    def add_batch(
        self,
        frame: pd.DataFrame,
        relative_time_column: str,
        anchored_time_column: str,
        anchor: pd.Timestamp,
        tolerance_seconds: float,
    ) -> None:
        self.scanned_rows += int(frame.shape[0])
        relative = pd.to_numeric(frame[relative_time_column], errors="coerce")
        anchored = pd.to_datetime(frame[anchored_time_column], errors="coerce")

        self.missing_or_invalid_relative_time += int(relative.isna().sum())
        self.missing_or_invalid_anchored_time += int(anchored.isna().sum())

        non_missing_relative = relative.dropna()
        if not non_missing_relative.empty:
            batch_minimum = float(non_missing_relative.min())
            batch_maximum = float(non_missing_relative.max())
            self.minimum_relative_minutes = (
                batch_minimum
                if self.minimum_relative_minutes is None
                else min(self.minimum_relative_minutes, batch_minimum)
            )
            self.maximum_relative_minutes = (
                batch_maximum
                if self.maximum_relative_minutes is None
                else max(self.maximum_relative_minutes, batch_maximum)
            )

        comparable = relative.notna() & anchored.notna()
        self.compared_rows += int(comparable.sum())
        if not comparable.any():
            return

        expected = anchor + pd.to_timedelta(relative[comparable], unit="m")
        differences = (anchored[comparable] - expected).abs().dt.total_seconds()
        self.disagreement_rows += int(differences.gt(tolerance_seconds).sum())
        batch_maximum_difference = float(differences.max())
        self.max_absolute_difference_seconds = (
            batch_maximum_difference
            if self.max_absolute_difference_seconds is None
            else max(self.max_absolute_difference_seconds, batch_maximum_difference)
        )


class _DynamicKeyTracker:
    """Exact, disk-backed duplicate detection with bounded Python memory."""

    def __init__(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="asic-dynamic-keys-"
        )
        database_path = Path(self._temporary_directory.name) / "keys.sqlite"
        self._connection = sqlite3.connect(database_path)
        self._connection.execute("PRAGMA journal_mode=OFF")
        self._connection.execute("PRAGMA synchronous=OFF")
        self._connection.execute("PRAGMA temp_store=FILE")
        self._connection.execute(
            """
            CREATE TABLE dynamic_keys (
                stay_id TEXT NOT NULL,
                relative_minutes REAL NOT NULL,
                PRIMARY KEY (stay_id, relative_minutes)
            ) WITHOUT ROWID
            """
        )
        self.duplicate_rows = 0
        self.evaluated_rows = 0

    def add_batch(
        self,
        frame: pd.DataFrame,
        stay_id_column: str,
        relative_time_column: str,
    ) -> None:
        stay_ids = frame[stay_id_column].astype("string")
        relative = pd.to_numeric(frame[relative_time_column], errors="coerce")
        valid = stay_ids.notna() & stay_ids.ne("") & relative.notna()
        rows = [
            (str(stay_id), float(minutes))
            for stay_id, minutes in zip(
                stay_ids[valid].tolist(),
                relative[valid].tolist(),
                strict=True,
            )
        ]
        before = self._connection.total_changes
        self._connection.executemany(
            "INSERT OR IGNORE INTO dynamic_keys VALUES (?, ?)",
            rows,
        )
        inserted = self._connection.total_changes - before
        self.evaluated_rows += len(rows)
        self.duplicate_rows += len(rows) - inserted
        self._connection.commit()

    def close(self) -> None:
        self._connection.close()
        self._temporary_directory.cleanup()


def _schema_summary(table: PooledTable) -> dict[str, Any]:
    return {
        "path": str(table.path),
        "row_count": table.row_count,
        "row_group_count": table.row_group_count,
        "column_count": len(table.schema),
        "columns": [
            {"name": field.name, "arrow_type": str(field.type)}
            for field in table.schema
        ],
    }


def _schema_check(
    table: PooledTable,
    expected: TableContract,
    dataset_context: str,
) -> CheckResult:
    observed_columns = [(field.name, str(field.type)) for field in table.schema]
    expected_columns = [
        (column.name, column.arrow_type_for(dataset_context))
        for column in expected.columns
    ]
    observed_names = [name for name, _ in observed_columns]
    expected_names = [name for name, _ in expected_columns]
    observed_types = dict(observed_columns)
    expected_types = dict(expected_columns)
    missing = [name for name in expected_names if name not in observed_types]
    extra = [name for name in observed_names if name not in expected_types]
    type_mismatches = [
        {
            "column": name,
            "observed": observed_types[name],
            "expected": expected_types[name],
        }
        for name in expected_names
        if name in observed_types and observed_types[name] != expected_types[name]
    ]
    matches = observed_columns == expected_columns
    return CheckResult(
        name=f"{table.name}_schema_matches_contract",
        status="pass" if matches else "fail",
        severity="blocking",
        observed={
            "column_count": len(observed_columns),
            "missing_columns": missing,
            "extra_columns": extra,
            "type_mismatches": type_mismatches,
            "column_order_matches": observed_names == expected_names,
        },
        expected={
            "column_count": len(expected_columns),
            "exact_names_order_and_arrow_types": True,
        },
        details=(
            "Observed names, order, and Arrow types match the frozen schema."
            if matches
            else "Schema drift requires explicit contract review before production use."
        ),
    )


def _check(
    name: str,
    condition: bool,
    observed: Any,
    expected: Any,
    details: str,
    severity: Literal["blocking", "advisory"] = "blocking",
) -> CheckResult:
    return CheckResult(
        name=name,
        status="pass" if condition else "fail",
        severity=severity,
        observed=observed,
        expected=expected,
        details=details,
    )


def _canonical_hospital_counts(counts: Counter[int]) -> dict[str, int]:
    return {
        f"asic_UK{hospital_code:02d}": int(count)
        for hospital_code, count in sorted(counts.items())
    }


def _required_columns_present(
    table: PooledTable,
    required: set[str],
) -> CheckResult:
    observed = set(table.schema.names)
    missing = sorted(required - observed)
    return _check(
        name=f"{table.name}_audit_columns_present",
        condition=not missing,
        observed={"missing": missing},
        expected={"required": sorted(required)},
        details="Columns required for pooled identifier/time validation.",
    )


def _scan_static(
    pooled: PooledInput,
    contract: InputContract,
    batch_size: int,
) -> _IdentifierStats:
    stats = _IdentifierStats()
    columns = [contract.stay_id_column, contract.hospital_id_column]
    for batch in pooled.static.parquet.iter_batches(
        batch_size=batch_size,
        columns=columns,
    ):
        stats.add_batch(
            batch.to_pandas(),
            stay_id_column=contract.stay_id_column,
            hospital_id_column=contract.hospital_id_column,
            track_duplicate_stays=True,
        )
    return stats


def _scan_dynamic(
    pooled: PooledInput,
    contract: InputContract,
    config: PipelineConfig,
) -> tuple[_IdentifierStats, _TimeStats, _DynamicKeyTracker | None]:
    identifier_stats = _IdentifierStats()
    time_stats = _TimeStats()
    key_tracker = (
        _DynamicKeyTracker() if config.audit.check_duplicate_dynamic_keys else None
    )
    columns = [
        contract.stay_id_column,
        contract.hospital_id_column,
        contract.relative_time_column,
        contract.anchored_time_column,
    ]
    anchor = pd.Timestamp(config.audit.time_anchor)
    try:
        for batch in pooled.dynamic.parquet.iter_batches(
            batch_size=config.audit.batch_size,
            columns=columns,
        ):
            frame = batch.to_pandas()
            identifier_stats.add_batch(
                frame,
                stay_id_column=contract.stay_id_column,
                hospital_id_column=contract.hospital_id_column,
                track_duplicate_stays=False,
            )
            time_stats.add_batch(
                frame,
                relative_time_column=contract.relative_time_column,
                anchored_time_column=contract.anchored_time_column,
                anchor=anchor,
                tolerance_seconds=config.audit.time_tolerance_seconds,
            )
            if key_tracker is not None:
                key_tracker.add_batch(
                    frame,
                    stay_id_column=contract.stay_id_column,
                    relative_time_column=contract.relative_time_column,
                )
    except Exception:
        if key_tracker is not None:
            key_tracker.close()
        raise
    return identifier_stats, time_stats, key_tracker


def audit_pooled_input(
    config: PipelineConfig,
    contract: InputContract,
) -> AuditReport:
    pooled = open_pooled_input(config.paths.pooled, contract)
    checks: list[CheckResult] = [
        _schema_check(pooled.static, contract.static, config.dataset_context),
        _schema_check(pooled.dynamic, contract.dynamic, config.dataset_context),
        _check(
            "static_has_rows",
            pooled.static.row_count > 0,
            pooled.static.row_count,
            "> 0",
            "The pooled static table must not be empty.",
        ),
        _check(
            "dynamic_has_rows",
            pooled.dynamic.row_count > 0,
            pooled.dynamic.row_count,
            "> 0",
            "The pooled dynamic table must not be empty.",
        ),
    ]

    static_required = {contract.stay_id_column, contract.hospital_id_column}
    dynamic_required = {
        contract.stay_id_column,
        contract.hospital_id_column,
        contract.relative_time_column,
        contract.anchored_time_column,
    }
    static_required_check = _required_columns_present(pooled.static, static_required)
    dynamic_required_check = _required_columns_present(pooled.dynamic, dynamic_required)
    checks.extend([static_required_check, dynamic_required_check])

    metrics: dict[str, Any] = {}
    if static_required_check.status == "pass":
        static_stats = _scan_static(
            pooled,
            contract,
            batch_size=config.audit.batch_size,
        )
        expected_hospitals = set(contract.expected_hospital_codes)
        observed_static_hospitals = set(static_stats.hospital_counts)
        static_identifier_issue_count = (
            static_stats.missing_stay_ids
            + static_stats.malformed_stay_ids
            + static_stats.missing_hids
            + static_stats.non_integral_hids
            + static_stats.suffix_hid_mismatches
        )
        checks.extend(
            [
                _check(
                    "static_metadata_row_count_matches_scan",
                    static_stats.scanned_rows == pooled.static.row_count,
                    static_stats.scanned_rows,
                    pooled.static.row_count,
                    "The identifier scan must cover every static row.",
                ),
                _check(
                    "static_identifiers_are_valid",
                    static_identifier_issue_count == 0,
                    {
                        "missing_stay_ids": static_stats.missing_stay_ids,
                        "malformed_stay_ids": static_stats.malformed_stay_ids,
                        "missing_hids": static_stats.missing_hids,
                        "non_integral_hids": static_stats.non_integral_hids,
                        "pseudo_id_suffix_hid_mismatches": (
                            static_stats.suffix_hid_mismatches
                        ),
                    },
                    {"all_issue_counts": 0},
                    "Pseudo-ID must be present and end in ':<hid>'; hid must be integral "
                    "and agree with that suffix.",
                ),
                _check(
                    "static_has_one_row_per_stay",
                    static_stats.duplicate_stay_id_rows == 0,
                    {"duplicate_rows_beyond_first": static_stats.duplicate_stay_id_rows},
                    {"duplicate_rows_beyond_first": 0},
                    "One Pseudo-ID represents exactly one ICU stay and must occur once in static.",
                ),
                _check(
                    "static_hospital_set_matches_contract",
                    observed_static_hospitals == expected_hospitals,
                    {
                        "hospital_codes": sorted(observed_static_hospitals),
                        "canonical_hospital_counts": _canonical_hospital_counts(
                            static_stats.hospital_counts
                        ),
                    },
                    {"hospital_codes": sorted(expected_hospitals)},
                    "The complete expected ASIC hospital set must be present.",
                ),
            ]
        )
        metrics["static"] = {
            "unique_stay_count": len(static_stats.stay_ids),
            "hospital_counts_from_pseudo_id": _canonical_hospital_counts(
                static_stats.hospital_counts
            ),
            "hospital_counts_from_hid": _canonical_hospital_counts(
                static_stats.hid_counts
            ),
        }
    else:
        static_stats = None

    if dynamic_required_check.status == "pass":
        dynamic_stats, time_stats, key_tracker = _scan_dynamic(
            pooled,
            contract,
            config,
        )
        expected_hospitals = set(contract.expected_hospital_codes)
        observed_dynamic_hospitals = set(dynamic_stats.hospital_counts)
        dynamic_identifier_issue_count = (
            dynamic_stats.missing_stay_ids
            + dynamic_stats.malformed_stay_ids
            + dynamic_stats.missing_hids
            + dynamic_stats.non_integral_hids
            + dynamic_stats.suffix_hid_mismatches
        )
        checks.extend(
            [
                _check(
                    "dynamic_metadata_row_count_matches_scan",
                    dynamic_stats.scanned_rows == pooled.dynamic.row_count,
                    dynamic_stats.scanned_rows,
                    pooled.dynamic.row_count,
                    "The identifier/time scan must cover every dynamic row.",
                ),
                _check(
                    "dynamic_identifiers_are_valid",
                    dynamic_identifier_issue_count == 0,
                    {
                        "missing_stay_ids": dynamic_stats.missing_stay_ids,
                        "malformed_stay_ids": dynamic_stats.malformed_stay_ids,
                        "missing_hids": dynamic_stats.missing_hids,
                        "non_integral_hids": dynamic_stats.non_integral_hids,
                        "pseudo_id_suffix_hid_mismatches": (
                            dynamic_stats.suffix_hid_mismatches
                        ),
                    },
                    {"all_issue_counts": 0},
                    "Pseudo-ID and hid must satisfy the same rules in every dynamic row.",
                ),
                _check(
                    "dynamic_hospital_set_matches_contract",
                    observed_dynamic_hospitals == expected_hospitals,
                    {
                        "hospital_codes": sorted(observed_dynamic_hospitals),
                        "canonical_hospital_counts": _canonical_hospital_counts(
                            dynamic_stats.hospital_counts
                        ),
                    },
                    {"hospital_codes": sorted(expected_hospitals)},
                    "The complete expected ASIC hospital set must be present.",
                ),
                _check(
                    "dynamic_time_values_are_present",
                    time_stats.missing_or_invalid_relative_time == 0
                    and time_stats.missing_or_invalid_anchored_time == 0,
                    {
                        "missing_or_invalid_relative_time": (
                            time_stats.missing_or_invalid_relative_time
                        ),
                        "missing_or_invalid_anchored_time": (
                            time_stats.missing_or_invalid_anchored_time
                        ),
                    },
                    {"both_issue_counts": 0},
                    "Both relative and anchored time representations are required per row.",
                ),
                _check(
                    "anchored_time_agrees_with_relative_minutes",
                    time_stats.disagreement_rows == 0,
                    {
                        "anchor": config.audit.time_anchor,
                        "compared_rows": time_stats.compared_rows,
                        "disagreement_rows": time_stats.disagreement_rows,
                        "max_absolute_difference_seconds": (
                            time_stats.max_absolute_difference_seconds
                        ),
                        "tolerance_seconds": config.audit.time_tolerance_seconds,
                    },
                    {"disagreement_rows": 0},
                    "The full production audit confirmed exact agreement. timeidx is an "
                    "artificial timestamp, never a true event timestamp.",
                ),
            ]
        )

        if key_tracker is None:
            checks.append(
                CheckResult(
                    name="dynamic_stay_time_keys_are_unique",
                    status="skipped",
                    severity="blocking",
                    observed=None,
                    expected={"duplicate_rows": 0},
                    details="Disabled by config.audit.check_duplicate_dynamic_keys.",
                )
            )
            duplicate_rows: int | None = None
        else:
            duplicate_rows = key_tracker.duplicate_rows
            evaluated_rows = key_tracker.evaluated_rows
            key_tracker.close()
            checks.append(
                _check(
                    "dynamic_stay_time_keys_are_unique",
                    duplicate_rows == 0,
                    {
                        "evaluated_rows": evaluated_rows,
                        "duplicate_rows_beyond_first": duplicate_rows,
                    },
                    {"duplicate_rows_beyond_first": 0},
                    "The full production audit confirmed one row per stay/time key.",
                )
            )

        metrics["dynamic"] = {
            "unique_stay_count": len(dynamic_stats.stay_ids),
            "hospital_counts_from_pseudo_id": _canonical_hospital_counts(
                dynamic_stats.hospital_counts
            ),
            "hospital_counts_from_hid": _canonical_hospital_counts(
                dynamic_stats.hid_counts
            ),
            "minimum_relative_minutes": time_stats.minimum_relative_minutes,
            "maximum_relative_minutes": time_stats.maximum_relative_minutes,
            "duplicate_stay_time_rows": duplicate_rows,
        }
    else:
        dynamic_stats = None

    if static_stats is not None and dynamic_stats is not None:
        dynamic_without_static = dynamic_stats.stay_ids - static_stats.stay_ids
        static_without_dynamic = static_stats.stay_ids - dynamic_stats.stay_ids
        checks.append(
            _check(
                "static_dynamic_stay_sets_match",
                not dynamic_without_static and not static_without_dynamic,
                {
                    "dynamic_stays_without_static_count": len(dynamic_without_static),
                    "static_stays_without_dynamic_count": len(static_without_dynamic),
                },
                {"both_counts": 0},
                "Every pooled ICU stay must occur in both tables; identifiers are not "
                "included in the report.",
            )
        )

    return AuditReport(
        contract_version=contract.contract_version,
        dataset=contract.dataset,
        dataset_context=config.dataset_context,
        generated_at_utc=utc_timestamp(),
        overall_status=overall_status(checks),
        inputs={
            "pooled_directory": str(config.paths.pooled),
            "config": str(config.source_path),
            "contract": str(contract.source_path),
            "static": _schema_summary(pooled.static),
            "dynamic": _schema_summary(pooled.dynamic),
        },
        metrics=metrics,
        checks=tuple(checks),
        limitations=(
            {
                "mock": (
                    "Mock-data results validate software and structure only; they are not "
                    "clinical or statistical evidence."
                ),
                "demo": (
                    "Demo results cover unchanged real data from a sampled subset only; "
                    "they do not establish full-dataset prevalence or representativeness."
                ),
                "production": (
                    "Production results validate the pooled input contract only; they do "
                    "not establish clinical validity or cross-hospital comparability."
                ),
            }[config.dataset_context],
            "The pooled audit does not translate, correct, mask, derive, filter, or write data tables.",
            "Unit, semantic, non-numeric-value, invalid-value, and cross-hospital "
            "distribution audits are deferred to later reviewed phases.",
        ),
    )
