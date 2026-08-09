from __future__ import annotations

import csv
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(frozen=True)
class IdentifierEvidence:
    identifier_column: str | None
    candidate_columns_present: tuple[str, ...]
    identifier_values: tuple[str, ...]
    nonempty_count: int
    empty_count: int
    unique_nonempty_count: int
    duplicate_nonempty_count: int
    filename_comparison_status: str
    time_columns_present: tuple[str, ...]
    hospital_identity_values: dict[str, tuple[str, ...]]
    hospital_identity_nonempty_counts: dict[str, int]


def digest_identifier_set(values: set[str]) -> str:
    digest = sha256()
    for value in sorted(values):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def inspect_identifiers(
    path: Path,
    encoding: str,
    delimiter: str,
    candidate_identifier_columns: tuple[str, ...],
    candidate_hospital_identity_columns: tuple[str, ...],
    candidate_time_columns: tuple[str, ...],
    filename_stay_id: str | None,
) -> IdentifierEvidence:
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        header = next(reader, None)
        if header is None:
            return IdentifierEvidence(
                None, (), (), 0, 0, 0, 0, "empty_file", (), {}, {}
            )
        present = tuple(
            column for column in candidate_identifier_columns if column in header
        )
        time_present = tuple(
            column for column in candidate_time_columns if column in header
        )
        hospital_columns = tuple(
            column
            for column in candidate_hospital_identity_columns
            if column in header
        )
        hospital_indices = {
            column: header.index(column) for column in hospital_columns
        }
        hospital_values: dict[str, set[str]] = {
            column: set() for column in hospital_columns
        }
        hospital_counts = {column: 0 for column in hospital_columns}
        selected = present[0] if len(present) == 1 else None
        selected_index = header.index(selected) if selected is not None else None
        values: list[str] = []
        empty = 0
        for row in reader:
            if selected_index is not None:
                value = row[selected_index] if selected_index < len(row) else ""
                if value == "":
                    empty += 1
                else:
                    values.append(value)
            for column, column_index in hospital_indices.items():
                value = row[column_index] if column_index < len(row) else ""
                if value != "":
                    hospital_counts[column] += 1
                    hospital_values[column].add(value)

    unique = tuple(sorted(set(values)))
    if len(present) > 1:
        comparison = "multiple_identifier_columns"
    elif selected is None:
        comparison = "identifier_column_absent"
    elif not unique:
        comparison = "all_in_file_identifiers_empty"
    elif filename_stay_id is None:
        comparison = "not_applicable_static"
    elif len(unique) > 1:
        comparison = "multiple_in_file_identifiers"
    elif unique[0] == filename_stay_id:
        comparison = "exact_match"
    else:
        comparison = "mismatch"

    return IdentifierEvidence(
        identifier_column=selected,
        candidate_columns_present=present,
        identifier_values=unique,
        nonempty_count=len(values),
        empty_count=empty,
        unique_nonempty_count=len(unique),
        duplicate_nonempty_count=len(values) - len(unique),
        filename_comparison_status=comparison,
        time_columns_present=time_present,
        hospital_identity_values={
            column: tuple(sorted(values))
            for column, values in hospital_values.items()
        },
        hospital_identity_nonempty_counts=hospital_counts,
    )
