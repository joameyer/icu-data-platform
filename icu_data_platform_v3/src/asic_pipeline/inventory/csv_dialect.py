from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
import re

from asic_pipeline.inventory.hashing import sha256_text


DECIMAL_COMMA_PATTERN = re.compile(
    r"^[+-]?(?:\d+,\d*|\d*,\d+)(?:[eE][+-]?\d+)?$"
)


@dataclass(frozen=True)
class DelimiterAttempt:
    delimiter: str
    header_columns: int
    sampled_records: int
    inconsistent_records: int
    status: str
    error: str | None


@dataclass(frozen=True)
class DelimiterDetection:
    status: str
    selected_delimiter: str | None
    attempts: tuple[DelimiterAttempt, ...]


@dataclass(frozen=True)
class CSVInspection:
    header: tuple[str, ...]
    data_record_count: int
    inconsistent_record_count: int
    duplicate_header_names: tuple[str, ...]
    decimal_comma_candidate_counts: tuple[tuple[str, int], ...]
    schema_variant_id: str
    parse_error: str | None


def _sample_delimiter(
    path: Path,
    encoding: str,
    delimiter: str,
    sample_records: int,
) -> DelimiterAttempt:
    header_columns = 0
    sampled = 0
    inconsistent = 0
    try:
        with path.open("r", encoding=encoding, newline="") as stream:
            reader = csv.reader(stream, delimiter=delimiter, strict=True)
            header = next(reader, None)
            if header is None:
                return DelimiterAttempt(
                    delimiter, 0, 0, 0, "empty", None
                )
            header_columns = len(header)
            for row in reader:
                sampled += 1
                if len(row) != header_columns:
                    inconsistent += 1
                if sampled >= sample_records:
                    break
    except (csv.Error, UnicodeError, OSError) as exc:
        return DelimiterAttempt(
            delimiter,
            header_columns,
            sampled,
            inconsistent,
            "parse_error",
            str(exc),
        )
    return DelimiterAttempt(
        delimiter,
        header_columns,
        sampled,
        inconsistent,
        "candidate",
        None,
    )


def detect_delimiter(
    path: Path,
    encoding: str,
    delimiters: tuple[str, ...],
    sample_records: int,
    minimum_header_columns: int,
    require_consistent_record_width: bool,
) -> DelimiterDetection:
    attempts = tuple(
        _sample_delimiter(path, encoding, delimiter, sample_records)
        for delimiter in delimiters
    )
    valid = [
        attempt
        for attempt in attempts
        if attempt.status == "candidate"
        and attempt.header_columns >= minimum_header_columns
        and (
            not require_consistent_record_width
            or attempt.inconsistent_records == 0
        )
    ]
    if not valid:
        return DelimiterDetection("unresolved", None, attempts)
    maximum_width = max(attempt.header_columns for attempt in valid)
    strongest = [
        attempt for attempt in valid if attempt.header_columns == maximum_width
    ]
    if len(strongest) != 1:
        return DelimiterDetection("ambiguous", None, attempts)
    return DelimiterDetection("selected", strongest[0].delimiter, attempts)


def inspect_csv(path: Path, encoding: str, delimiter: str) -> CSVInspection:
    header: tuple[str, ...] = ()
    row_count = 0
    inconsistent = 0
    decimal_counts: dict[str, int] = {}
    parse_error: str | None = None
    try:
        with path.open("r", encoding=encoding, newline="") as stream:
            reader = csv.reader(stream, delimiter=delimiter, strict=True)
            first = next(reader, None)
            if first is None:
                raise ValueError("CSV file has no header record")
            header = tuple(first)
            decimal_counts = {column: 0 for column in header}
            for row in reader:
                row_count += 1
                if len(row) != len(header):
                    inconsistent += 1
                    continue
                for column, value in zip(header, row, strict=True):
                    if DECIMAL_COMMA_PATTERN.fullmatch(value.strip()):
                        decimal_counts[column] += 1
    except (csv.Error, UnicodeError, OSError, ValueError) as exc:
        parse_error = str(exc)

    duplicate_names = tuple(
        sorted({name for name in header if header.count(name) > 1})
    )
    schema_material = json.dumps(
        {
            "encoding": encoding,
            "delimiter": delimiter,
            "header": list(header),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return CSVInspection(
        header=header,
        data_record_count=row_count,
        inconsistent_record_count=inconsistent,
        duplicate_header_names=duplicate_names,
        decimal_comma_candidate_counts=tuple(
            (column, count) for column, count in decimal_counts.items() if count
        ),
        schema_variant_id=f"schema_{sha256_text(schema_material)[:16]}",
        parse_error=parse_error,
    )

