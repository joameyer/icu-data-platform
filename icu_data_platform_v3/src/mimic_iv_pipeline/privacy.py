from __future__ import annotations

from typing import Any, Iterable

from mimic_iv_pipeline.errors import MIMICPipelineError


PRIVATE_FIELDS = {
    "subject_id",
    "hadm_id",
    "stay_id",
    "event_time",
    "icu_intime",
    "official_outtime",
    "source_file_id",
    "source_path",
    "source_row_number",
}


def validate_public_aggregate_rows(
    rows: Iterable[dict[str, Any]], *, minimum_cell_count: int = 10
) -> None:
    """Fail closed if a proposed public row contains private or small-cell data."""

    for row in rows:
        overlap = PRIVATE_FIELDS.intersection(row)
        if overlap:
            raise MIMICPipelineError(
                f"Public aggregate contains private fields: {sorted(overlap)}"
            )
        for key, value in row.items():
            if key.endswith("_count") and isinstance(value, int):
                if 0 < value < minimum_cell_count:
                    raise MIMICPipelineError(
                        f"Public aggregate contains a small cell: {key}={value}"
                    )
