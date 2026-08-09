from __future__ import annotations

import csv
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(frozen=True)
class ParsedDynamicFile:
    path: Path
    sha256: str
    header: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    time_column: str | None


def read_dynamic_file(
    path: Path,
    encoding: str,
    delimiter: str,
    file_sha256: str,
    candidate_time_columns: tuple[str, ...],
) -> ParsedDynamicFile:
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        header = tuple(next(reader))
        rows = tuple(tuple(row) for row in reader)
    time_columns = [column for column in candidate_time_columns if column in header]
    return ParsedDynamicFile(
        path=path,
        sha256=file_sha256,
        header=header,
        rows=rows,
        time_column=time_columns[0] if len(time_columns) == 1 else None,
    )


def _canonical_digest(parsed: ParsedDynamicFile) -> str:
    digest = sha256()
    for row in (parsed.header, *parsed.rows):
        for value in row:
            encoded = value.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def _pair_classification(
    left: ParsedDynamicFile,
    right: ParsedDynamicFile,
) -> str:
    if left.sha256 == right.sha256:
        return "exact_byte_duplicate"
    if _canonical_digest(left) == _canonical_digest(right):
        return "equivalent_after_parsing"
    if left.time_column is None or right.time_column is None:
        return "unresolved"
    if left.time_column != right.time_column:
        return "unresolved"
    left_time_index = left.header.index(left.time_column)
    right_time_index = right.header.index(right.time_column)
    if any(len(row) != len(left.header) for row in left.rows) or any(
        len(row) != len(right.header) for row in right.rows
    ):
        return "unresolved"
    left_keyed = {row[left_time_index]: row for row in left.rows}
    right_keyed = {row[right_time_index]: row for row in right.rows}
    if len(left_keyed) != len(left.rows) or len(right_keyed) != len(right.rows):
        return "unresolved"
    common_columns = [
        column
        for column in left.header
        if column in right.header and column != left.time_column
    ]
    for time_value in set(left_keyed) & set(right_keyed):
        left_row = left_keyed[time_value]
        right_row = right_keyed[time_value]
        for column in common_columns:
            left_value = left_row[left.header.index(column)]
            right_value = right_row[right.header.index(column)]
            if left_value and right_value and left_value != right_value:
                return "conflicting"
    return "complementary"


def classify_dynamic_duplicate_group(files: tuple[ParsedDynamicFile, ...]) -> str:
    if len(files) < 2:
        raise ValueError("A duplicate group must contain at least two files")
    pair_statuses = {
        _pair_classification(files[left_index], files[right_index])
        for left_index in range(len(files))
        for right_index in range(left_index + 1, len(files))
    }
    precedence = (
        "conflicting",
        "unresolved",
        "complementary",
        "equivalent_after_parsing",
        "exact_byte_duplicate",
    )
    return next(status for status in precedence if status in pair_statuses)
