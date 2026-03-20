from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import pandas as pd


DEFAULT_ASIC_RAW_DATA_DIR = Path("/Users/joanameyer/data/asic/raw_sample10")
DEFAULT_TRANSLATION_PATH = Path(__file__).resolve().parents[1] / "column_translation.json"


def hospital_name_from_path(path: Path, table_kind: Literal["static", "dynamic"]) -> str:
    return path.stem.replace(f"_{table_kind}", "")


def load_all_translation_maps(
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
) -> dict[str, dict[str, str]]:
    return json.loads(translation_path.read_text())


def load_static_translation(
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
) -> dict[str, str]:
    return load_all_translation_maps(translation_path)["static"]


def load_dynamic_translation(
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
) -> dict[str, str]:
    return load_all_translation_maps(translation_path)["dynamic"]


def load_raw_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
    table_kind: Literal["static", "dynamic"] = "static",
) -> dict[str, pd.DataFrame]:
    read_csv_kwargs: dict[str, object] = {}
    if table_kind == "dynamic":
        read_csv_kwargs["low_memory"] = False

    pattern = f"asic_*_{table_kind}.csv"
    return {
        hospital_name_from_path(path, table_kind): pd.read_csv(path, **read_csv_kwargs)
        for path in sorted(raw_dir.glob(pattern))
    }


def load_static_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> dict[str, pd.DataFrame]:
    return load_raw_tables(raw_dir=raw_dir, table_kind="static")


def load_dynamic_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> dict[str, pd.DataFrame]:
    return load_raw_tables(raw_dir=raw_dir, table_kind="dynamic")
