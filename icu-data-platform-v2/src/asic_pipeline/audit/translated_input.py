from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from asic_pipeline.audit.pooled_input import (
    _DynamicKeyTracker,
    _TimeStats,
    _canonical_hospital_counts,
    _check,
    _schema_check,
    _schema_summary,
)
from asic_pipeline.audit.report import AuditReport, overall_status, utc_timestamp
from asic_pipeline.contracts import TranslatedInputContract
from asic_pipeline.errors import InputReadError
from asic_pipeline.io import PooledInput, PooledTable
from asic_pipeline.translated_input import TranslatedInputConfig


_STAY_ID_PATTERN = re.compile(r"^.+:(\d+)$")


@dataclass
class _TranslatedIdentifierStats:
    scanned_rows: int = 0
    missing_stay_ids: int = 0
    malformed_stay_ids: int = 0
    missing_source_hospital_codes: int = 0
    non_integral_source_hospital_codes: int = 0
    suffix_source_code_mismatches: int = 0
    missing_canonical_hospital_ids: int = 0
    canonical_hospital_id_mismatches: int = 0
    unexpected_hospital_codes: int = 0
    duplicate_stay_id_rows: int = 0
    hospital_counts: Counter[int] = field(default_factory=Counter)
    stay_ids: set[str] = field(default_factory=set)

    def add_batch(
        self,
        frame: pd.DataFrame,
        contract: TranslatedInputContract,
        track_duplicate_stays: bool,
    ) -> None:
        self.scanned_rows += int(frame.shape[0])
        stay_ids = frame[contract.stay_id_column].astype("string")
        canonical_ids = frame[contract.canonical_hospital_id_column].astype("string")
        source_codes_raw = frame[contract.source_hospital_code_column]
        source_codes = pd.to_numeric(source_codes_raw, errors="coerce")

        missing_stay = stay_ids.isna() | stay_ids.eq("")
        suffixes = stay_ids.str.extract(_STAY_ID_PATTERN, expand=False)
        malformed_stay = ~missing_stay & suffixes.isna()
        missing_source_code = source_codes_raw.isna()
        integral_source_code = source_codes.notna() & source_codes.mod(1).eq(0)
        non_integral_source_code = ~missing_source_code & ~integral_source_code
        missing_canonical = canonical_ids.isna() | canonical_ids.eq("")

        self.missing_stay_ids += int(missing_stay.sum())
        self.malformed_stay_ids += int(malformed_stay.sum())
        self.missing_source_hospital_codes += int(missing_source_code.sum())
        self.non_integral_source_hospital_codes += int(
            non_integral_source_code.sum()
        )
        self.missing_canonical_hospital_ids += int(missing_canonical.sum())

        valid_suffix = pd.to_numeric(suffixes, errors="coerce")
        comparable_source = valid_suffix.notna() & integral_source_code
        suffix_values = valid_suffix[comparable_source].astype(int)
        source_values = source_codes[comparable_source].astype(int)
        self.suffix_source_code_mismatches += int(
            suffix_values.ne(source_values).sum()
        )

        valid_suffix_mask = valid_suffix.notna()
        valid_suffix_values = valid_suffix[valid_suffix_mask].astype(int)
        self.hospital_counts.update(valid_suffix_values.tolist())
        expected_codes = set(contract.expected_hospital_codes)
        self.unexpected_hospital_codes += int(
            (~valid_suffix_values.isin(expected_codes)).sum()
        )

        comparable_canonical = valid_suffix.notna() & ~missing_canonical
        expected_canonical = valid_suffix[comparable_canonical].astype(int).map(
            lambda value: f"asic_UK{value:02d}"
        )
        observed_canonical = canonical_ids[comparable_canonical]
        self.canonical_hospital_id_mismatches += int(
            observed_canonical.ne(expected_canonical).sum()
        )

        for stay_id in stay_ids[~missing_stay].tolist():
            value = str(stay_id)
            if track_duplicate_stays and value in self.stay_ids:
                self.duplicate_stay_id_rows += 1
            self.stay_ids.add(value)


def _open_translated_input(
    directory: Path,
    contract: TranslatedInputContract,
) -> PooledInput:
    tables: dict[str, PooledTable] = {}
    for name, table_contract in (
        ("static", contract.static),
        ("dynamic", contract.dynamic),
    ):
        path = directory / table_contract.filename
        if not path.is_file():
            raise InputReadError(f"Translated {name} file does not exist: {path}")
        try:
            parquet = pq.ParquetFile(path)
        except Exception as exc:
            raise InputReadError(
                f"Could not open translated {name} Parquet file {path}: {exc}"
            ) from exc
        tables[name] = PooledTable(name=name, path=path, parquet=parquet)
    return PooledInput(static=tables["static"], dynamic=tables["dynamic"])


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
    except FileNotFoundError as exc:
        raise InputReadError(f"Translation manifest does not exist: {path}") from exc
    except (json.JSONDecodeError, OSError) as exc:
        raise InputReadError(f"Could not read translation manifest {path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise InputReadError("Translation manifest must contain a JSON object")
    return manifest


def _manifest_checks(
    manifest: dict[str, Any],
    config: TranslatedInputConfig,
    contract: TranslatedInputContract,
    translated: PooledInput,
) -> list:
    producer = contract.producer
    manifest_contract = contract.manifest
    checks = manifest.get("checks")
    checks = checks if isinstance(checks, dict) else {}
    required_checks = manifest_contract.checks()
    mismatches = {
        name: {"observed": checks.get(name), "expected": expected}
        for name, expected in required_checks.items()
        if checks.get(name) != expected
    }
    source = manifest.get("source")
    source = source if isinstance(source, dict) else {}
    output = manifest.get("output")
    output = output if isinstance(output, dict) else {}
    actual = {
        "static_rows": translated.static.row_count,
        "dynamic_rows": translated.dynamic.row_count,
        "static_columns": len(translated.static.schema),
        "dynamic_columns": len(translated.dynamic.schema),
    }
    manifest_output = {
        key: output.get(key)
        for key in (
            "static_rows",
            "dynamic_rows",
            "static_columns",
            "dynamic_columns",
        )
    }
    row_preservation = {
        "static": {
            "source": source.get("static_rows"),
            "output": output.get("static_rows"),
        },
        "dynamic": {
            "source": source.get("dynamic_rows"),
            "output": output.get("dynamic_rows"),
        },
    }
    output_directory = output.get("translated_directory")
    output_directory_matches = False
    if isinstance(output_directory, str):
        output_directory_matches = Path(output_directory).expanduser().resolve() == (
            config.translated_dir
        )

    return [
        _check(
            "translation_manifest_identity_matches_contract",
            manifest.get("artifact") == manifest_contract.artifact
            and manifest.get("audit_status") == manifest_contract.audit_status
            and manifest.get("dataset_context") == config.dataset_context,
            {
                "artifact": manifest.get("artifact"),
                "audit_status": manifest.get("audit_status"),
                "dataset_context": manifest.get("dataset_context"),
            },
            {
                "artifact": manifest_contract.artifact,
                "audit_status": manifest_contract.audit_status,
                "dataset_context": config.dataset_context,
            },
            "The manifest must identify a passing translated artifact for this context.",
        ),
        _check(
            "translation_manifest_producer_matches_contract",
            manifest.get("source_contract_version")
            == producer.pooled_contract_version
            and manifest.get("translation_policy_version")
            == producer.translation_policy_version
            and manifest.get("generator_version")
            in producer.compatible_generator_versions
            and manifest.get("documentation") == producer.documentation,
            {
                "source_contract_version": manifest.get("source_contract_version"),
                "translation_policy_version": manifest.get(
                    "translation_policy_version"
                ),
                "generator_version": manifest.get("generator_version"),
                "documentation": manifest.get("documentation"),
            },
            {
                "source_contract_version": producer.pooled_contract_version,
                "translation_policy_version": producer.translation_policy_version,
                "compatible_generator_versions": list(
                    producer.compatible_generator_versions
                ),
                "documentation": producer.documentation,
            },
            "Only artifacts from the reviewed pooled-to-translated producer are accepted.",
        ),
        _check(
            "translation_manifest_required_checks_pass",
            not mismatches,
            {"mismatches": mismatches},
            {"required_checks": required_checks},
            "Every frozen translation publication check must have its exact value.",
        ),
        _check(
            "translation_manifest_rows_are_preserved",
            source.get("static_rows") == output.get("static_rows")
            and source.get("dynamic_rows") == output.get("dynamic_rows"),
            row_preservation,
            {"source_equals_output": True},
            "The translation stage must preserve every static and dynamic row.",
        ),
        _check(
            "translation_manifest_output_matches_files",
            manifest_output == actual,
            {"manifest": manifest_output, "actual": actual},
            {"manifest_equals_actual": True},
            "Manifest row and column counts must match the physical Parquet files.",
        ),
        _check(
            "translation_manifest_output_path_matches_context",
            output_directory_matches,
            output_directory,
            str(config.translated_dir),
            "The manifest must refer to the configured translated directory.",
        ),
    ]


def _scan_identifiers(
    table: PooledTable,
    contract: TranslatedInputContract,
    batch_size: int,
    track_duplicate_stays: bool,
) -> _TranslatedIdentifierStats:
    stats = _TranslatedIdentifierStats()
    columns = [
        contract.stay_id_column,
        contract.canonical_hospital_id_column,
        contract.source_hospital_code_column,
    ]
    for batch in table.parquet.iter_batches(batch_size=batch_size, columns=columns):
        stats.add_batch(
            batch.to_pandas(),
            contract,
            track_duplicate_stays=track_duplicate_stays,
        )
    return stats


def _identifier_issue_counts(stats: _TranslatedIdentifierStats) -> dict[str, int]:
    return {
        "missing_stay_ids": stats.missing_stay_ids,
        "malformed_stay_ids": stats.malformed_stay_ids,
        "missing_source_hospital_codes": stats.missing_source_hospital_codes,
        "non_integral_source_hospital_codes": (
            stats.non_integral_source_hospital_codes
        ),
        "pseudo_id_suffix_source_code_mismatches": (
            stats.suffix_source_code_mismatches
        ),
        "missing_canonical_hospital_ids": stats.missing_canonical_hospital_ids,
        "canonical_hospital_id_mismatches": (
            stats.canonical_hospital_id_mismatches
        ),
        "unexpected_hospital_codes": stats.unexpected_hospital_codes,
    }


def audit_translated_input(
    config: TranslatedInputConfig,
    contract: TranslatedInputContract,
) -> AuditReport:
    translated = _open_translated_input(config.translated_dir, contract)
    manifest_path = config.translated_dir / contract.manifest.filename
    manifest = _load_manifest(manifest_path)
    checks = [
        _schema_check(translated.static, contract.static, config.dataset_context),
        _schema_check(translated.dynamic, contract.dynamic, config.dataset_context),
        _check(
            "static_has_rows",
            translated.static.row_count > 0,
            translated.static.row_count,
            "> 0",
            "The translated static table must not be empty.",
        ),
        _check(
            "dynamic_has_rows",
            translated.dynamic.row_count > 0,
            translated.dynamic.row_count,
            "> 0",
            "The translated dynamic table must not be empty.",
        ),
    ]
    checks.extend(_manifest_checks(manifest, config, contract, translated))

    static_stats = _scan_identifiers(
        translated.static,
        contract,
        config.audit.batch_size,
        track_duplicate_stays=True,
    )
    dynamic_stats = _TranslatedIdentifierStats()
    time_stats = _TimeStats()
    key_tracker = (
        _DynamicKeyTracker() if config.audit.check_duplicate_dynamic_keys else None
    )
    dynamic_columns = [
        contract.stay_id_column,
        contract.canonical_hospital_id_column,
        contract.source_hospital_code_column,
        contract.relative_time_column,
        contract.anchored_time_column,
    ]
    anchor = pd.Timestamp(config.audit.time_anchor)
    try:
        for batch in translated.dynamic.parquet.iter_batches(
            batch_size=config.audit.batch_size,
            columns=dynamic_columns,
        ):
            frame = batch.to_pandas()
            dynamic_stats.add_batch(
                frame,
                contract,
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

    static_issues = _identifier_issue_counts(static_stats)
    dynamic_issues = _identifier_issue_counts(dynamic_stats)
    expected_hospitals = set(contract.expected_hospital_codes)
    checks.extend(
        [
            _check(
                "static_identifier_scan_covers_all_rows",
                static_stats.scanned_rows == translated.static.row_count,
                static_stats.scanned_rows,
                translated.static.row_count,
                "The translated identifier scan must cover every static row.",
            ),
            _check(
                "static_identifiers_are_valid",
                not any(static_issues.values()),
                static_issues,
                {"all_issue_counts": 0},
                "Stay ID, source hospital code, and canonical hospital ID must agree.",
            ),
            _check(
                "static_has_one_row_per_stay",
                static_stats.duplicate_stay_id_rows == 0,
                {"duplicate_rows_beyond_first": static_stats.duplicate_stay_id_rows},
                {"duplicate_rows_beyond_first": 0},
                "One translated static row is required per ICU stay.",
            ),
            _check(
                "static_hospital_set_matches_contract",
                set(static_stats.hospital_counts) == expected_hospitals,
                {
                    "hospital_codes": sorted(static_stats.hospital_counts),
                    "canonical_hospital_counts": _canonical_hospital_counts(
                        static_stats.hospital_counts
                    ),
                },
                {"hospital_codes": sorted(expected_hospitals)},
                "The complete expected hospital set must be represented.",
            ),
            _check(
                "dynamic_identifier_time_scan_covers_all_rows",
                dynamic_stats.scanned_rows == translated.dynamic.row_count
                and time_stats.scanned_rows == translated.dynamic.row_count,
                {
                    "identifier_rows": dynamic_stats.scanned_rows,
                    "time_rows": time_stats.scanned_rows,
                },
                {"rows": translated.dynamic.row_count},
                "The translated identifier/time scan must cover every dynamic row.",
            ),
            _check(
                "dynamic_identifiers_are_valid",
                not any(dynamic_issues.values()),
                dynamic_issues,
                {"all_issue_counts": 0},
                "Stay ID, source hospital code, and canonical hospital ID must agree.",
            ),
            _check(
                "dynamic_hospital_set_matches_contract",
                set(dynamic_stats.hospital_counts) == expected_hospitals,
                {
                    "hospital_codes": sorted(dynamic_stats.hospital_counts),
                    "canonical_hospital_counts": _canonical_hospital_counts(
                        dynamic_stats.hospital_counts
                    ),
                },
                {"hospital_codes": sorted(expected_hospitals)},
                "The complete expected hospital set must be represented.",
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
                "Both translated relative-time representations are required.",
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
                "The anchored timestamp remains artificial time since ICU admission.",
            ),
        ]
    )

    if key_tracker is None:
        duplicate_rows = None
        checks.append(
            _check(
                "dynamic_stay_time_keys_are_unique",
                False,
                {"scan_disabled": True},
                {"duplicate_rows_beyond_first": 0},
                "Duplicate-key validation is mandatory at this boundary.",
            )
        )
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
                "One translated dynamic row is required per stay/time key.",
            )
        )

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
            "Every translated ICU stay must occur in both tables.",
        )
    )

    return AuditReport(
        contract_version=contract.contract_version,
        dataset=contract.dataset,
        dataset_context=config.dataset_context,
        generated_at_utc=utc_timestamp(),
        overall_status=overall_status(checks),
        inputs={
            "translated_directory": str(config.translated_dir),
            "config": str(config.source_path),
            "contract": str(contract.source_path),
            "manifest": str(manifest_path),
            "static": _schema_summary(translated.static),
            "dynamic": _schema_summary(translated.dynamic),
        },
        metrics={
            "static": {
                "unique_stay_count": len(static_stats.stay_ids),
                "hospital_counts": _canonical_hospital_counts(
                    static_stats.hospital_counts
                ),
            },
            "dynamic": {
                "unique_stay_count": len(dynamic_stats.stay_ids),
                "hospital_counts": _canonical_hospital_counts(
                    dynamic_stats.hospital_counts
                ),
                "minimum_relative_minutes": time_stats.minimum_relative_minutes,
                "maximum_relative_minutes": time_stats.maximum_relative_minutes,
                "duplicate_stay_time_rows": duplicate_rows,
            },
        },
        checks=tuple(checks),
        limitations=(
            "This audit validates translated structure, provenance, identifiers, time, and publication evidence only.",
            "It does not establish clinical validity, unit consistency, cross-hospital comparability, or analysis eligibility.",
            "It never modifies pooled, translated, cleaned, or derived data artifacts.",
        ),
    )
