from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path

from asic_pipeline.inventory.identifiers import digest_identifier_set


@dataclass(frozen=True)
class StaticCandidateComparison:
    classification: str
    exact_byte_duplicate: bool
    same_ordered_schema: bool
    left_row_count: int
    right_row_count: int
    identifier_column: str | None
    left_unique_stays: int | None
    right_unique_stays: int | None
    same_stay_set: bool | None
    left_only_stays: int | None
    right_only_stays: int | None
    overlapping_stays: int | None
    overlapping_equal_nonmissing_values: int | None
    complementary_nonmissing_values: int | None
    conflicting_nonmissing_values: int | None
    left_only_columns: tuple[str, ...]
    right_only_columns: tuple[str, ...]
    left_stay_set_digest: str | None
    right_stay_set_digest: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _read_keyed_rows(
    path: Path,
    encoding: str,
    delimiter: str,
    identifier_column: str,
) -> tuple[tuple[str, ...], list[list[str]], dict[str, list[str]]]:
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        header_list = next(reader)
        header = tuple(header_list)
        identifier_index = header.index(identifier_column)
        rows = list(reader)
    keyed: dict[str, list[str]] = {}
    for row in rows:
        if len(row) == len(header) and row[identifier_index] != "":
            keyed[row[identifier_index]] = row
    return header, rows, keyed


def compare_static_candidates(
    left_path: Path,
    right_path: Path,
    left_encoding: str,
    right_encoding: str,
    left_delimiter: str,
    right_delimiter: str,
    left_sha256: str,
    right_sha256: str,
    candidate_identifier_columns: tuple[str, ...],
) -> StaticCandidateComparison:
    exact = left_sha256 == right_sha256
    with left_path.open("r", encoding=left_encoding, newline="") as stream:
        left_reader = csv.reader(stream, delimiter=left_delimiter, strict=True)
        left_header = tuple(next(left_reader))
        left_count = sum(1 for _ in left_reader)
    with right_path.open("r", encoding=right_encoding, newline="") as stream:
        right_reader = csv.reader(stream, delimiter=right_delimiter, strict=True)
        right_header = tuple(next(right_reader))
        right_count = sum(1 for _ in right_reader)

    common_identifier_columns = [
        column
        for column in candidate_identifier_columns
        if column in left_header and column in right_header
    ]
    identifier_column = (
        common_identifier_columns[0]
        if len(common_identifier_columns) == 1
        else None
    )
    left_only_columns = tuple(column for column in left_header if column not in right_header)
    right_only_columns = tuple(column for column in right_header if column not in left_header)
    if identifier_column is None:
        return StaticCandidateComparison(
            classification="unresolved",
            exact_byte_duplicate=exact,
            same_ordered_schema=left_header == right_header,
            left_row_count=left_count,
            right_row_count=right_count,
            identifier_column=None,
            left_unique_stays=None,
            right_unique_stays=None,
            same_stay_set=None,
            left_only_stays=None,
            right_only_stays=None,
            overlapping_stays=None,
            overlapping_equal_nonmissing_values=None,
            complementary_nonmissing_values=None,
            conflicting_nonmissing_values=None,
            left_only_columns=left_only_columns,
            right_only_columns=right_only_columns,
            left_stay_set_digest=None,
            right_stay_set_digest=None,
        )

    left_header, left_rows, left_keyed = _read_keyed_rows(
        left_path, left_encoding, left_delimiter, identifier_column
    )
    right_header, right_rows, right_keyed = _read_keyed_rows(
        right_path, right_encoding, right_delimiter, identifier_column
    )
    left_ids = set(left_keyed)
    right_ids = set(right_keyed)
    overlap = left_ids & right_ids
    common_columns = [
        column
        for column in left_header
        if column in right_header and column != identifier_column
    ]
    left_indices = {column: left_header.index(column) for column in common_columns}
    right_indices = {column: right_header.index(column) for column in common_columns}
    equal = complementary = conflicts = 0
    for stay_id in overlap:
        left_row = left_keyed[stay_id]
        right_row = right_keyed[stay_id]
        for column in common_columns:
            left_value = left_row[left_indices[column]]
            right_value = right_row[right_indices[column]]
            if left_value and right_value:
                if left_value == right_value:
                    equal += 1
                else:
                    conflicts += 1
            elif left_value or right_value:
                complementary += 1

    if exact:
        classification = "exact_byte_duplicate"
    elif conflicts:
        classification = "conflicting"
    elif left_ids != right_ids or complementary or left_only_columns or right_only_columns:
        classification = "complementary"
    elif left_header == right_header and left_rows == right_rows:
        classification = "equivalent_after_parsing"
    else:
        classification = "unresolved"
    return StaticCandidateComparison(
        classification=classification,
        exact_byte_duplicate=exact,
        same_ordered_schema=left_header == right_header,
        left_row_count=left_count,
        right_row_count=right_count,
        identifier_column=identifier_column,
        left_unique_stays=len(left_ids),
        right_unique_stays=len(right_ids),
        same_stay_set=left_ids == right_ids,
        left_only_stays=len(left_ids - right_ids),
        right_only_stays=len(right_ids - left_ids),
        overlapping_stays=len(overlap),
        overlapping_equal_nonmissing_values=equal,
        complementary_nonmissing_values=complementary,
        conflicting_nonmissing_values=conflicts,
        left_only_columns=left_only_columns,
        right_only_columns=right_only_columns,
        left_stay_set_digest=digest_identifier_set(left_ids),
        right_stay_set_digest=digest_identifier_set(right_ids),
    )
