from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _serialize_cell(value: object) -> object:
    if isinstance(value, set):
        value = sorted(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value)
    return value


def prepare_dataframe_for_write(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        if result[column].map(lambda value: isinstance(value, (list, tuple, set, dict))).any():
            result[column] = result[column].map(_serialize_cell)
    return result


def write_dataframe(
    df: pd.DataFrame,
    path: Path,
    output_format: str = "csv",
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    to_write = prepare_dataframe_for_write(df)

    if output_format == "csv":
        to_write.to_csv(path, index=False)
        return path

    if output_format == "parquet":
        to_write.to_parquet(path, index=False)
        return path

    raise ValueError(f"Unsupported output format: {output_format}")
