from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


DEFAULT_ASIC_RAW_DATA_DIR = Path("/Users/joanameyer/data/asic/synthetic_control_sample10")
DEFAULT_TRANSLATION_PATH = Path(__file__).resolve().parents[1] / "column_translation.json"
HOSPITAL_DIR_GLOB = "UK_*"
STATIC_FILENAME = "andere_variablen_kds_patienten.csv"
DYNAMIC_FILTERED_DIRNAME = "dynamic_filtered"
DYNAMIC_DIRNAME = "dynamic"
DYNAMIC_FILENAME_PREFIX = "dynamische_variablen_kds_patient_"
RAW_STAY_ID_COLUMNS = ("Pseudo-ID", "PseudoID")
DECIMAL_COMMA_DYNAMIC_HOSPITAL_DIRS = frozenset({"UK_01"})


def hospital_name_from_path(path: Path, table_kind: Literal["static", "dynamic"]) -> str:
    return path.stem.replace(f"_{table_kind}", "")


@dataclass(frozen=True)
class HospitalRawTables:
    static: pd.DataFrame
    dynamic: pd.DataFrame


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


def get_hospital_dirs(raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR) -> list[Path]:
    return sorted(path for path in raw_dir.glob(HOSPITAL_DIR_GLOB) if path.is_dir())


def hospital_name_from_dir(hospital_dir: Path) -> str:
    return f"asic_{hospital_dir.name.replace('_', '')}"


def get_dynamic_dir(hospital_dir: Path) -> Path:
    dynamic_filtered_dir = hospital_dir / DYNAMIC_FILTERED_DIRNAME
    if dynamic_filtered_dir.is_dir():
        return dynamic_filtered_dir

    dynamic_dir = hospital_dir / DYNAMIC_DIRNAME
    if dynamic_dir.is_dir():
        return dynamic_dir

    raise FileNotFoundError(
        f"Could not find '{DYNAMIC_FILTERED_DIRNAME}/' or '{DYNAMIC_DIRNAME}/' in {hospital_dir}"
    )


def extract_local_stay_id(path: Path) -> str:
    if path.parent.name == DYNAMIC_FILTERED_DIRNAME:
        return path.stem

    if path.parent.name == DYNAMIC_DIRNAME:
        if not path.stem.startswith(DYNAMIC_FILENAME_PREFIX):
            raise ValueError(
                f"Unexpected dynamic filename for {path.parent}: {path.name}"
            )
        return path.stem.removeprefix(DYNAMIC_FILENAME_PREFIX)

    raise ValueError(f"Could not extract local_stay_id from unexpected path: {path}")


def _string_series(values: object, index: pd.Index) -> pd.Series:
    return pd.Series(values, index=index, dtype="string").str.strip()


def _ensure_stay_id_alias_columns(
    df: pd.DataFrame,
    stay_id_values: object,
    prefer_supplied_values: bool = False,
    allow_conflicts: bool = False,
) -> pd.DataFrame:
    result = df.copy()
    resolved_stay_id = _string_series(stay_id_values, result.index)

    for column in RAW_STAY_ID_COLUMNS:
        if column not in result.columns:
            continue

        existing = _string_series(result[column], result.index)
        mismatch_mask = (
            existing.notna()
            & resolved_stay_id.notna()
            & existing.ne(resolved_stay_id)
        )
        if mismatch_mask.any() and not allow_conflicts:
            raise ValueError(
                f"Conflicting stay-id values found in raw column {column!r}"
            )

        if prefer_supplied_values:
            resolved_stay_id = resolved_stay_id.combine_first(existing)
        else:
            resolved_stay_id = existing.combine_first(resolved_stay_id)

    for column in RAW_STAY_ID_COLUMNS:
        result[column] = resolved_stay_id

    return result


def _ensure_dynamic_time_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Time" in df.columns or "Zeit_ab_Aufnahme" not in df.columns:
        return df

    result = df.copy()
    minutes_since_admit = pd.to_numeric(result["Zeit_ab_Aufnahme"], errors="coerce")
    result["Time"] = pd.to_timedelta(minutes_since_admit, unit="m")
    return result


def load_dynamic_for_hospital(hospital_dir: Path) -> pd.DataFrame:
    dynamic_dir = get_dynamic_dir(hospital_dir)
    separator = "," if dynamic_dir.name == DYNAMIC_FILTERED_DIRNAME else ";"
    read_csv_kwargs: dict[str, object] = {
        "low_memory": False,
        "na_values": ["storniert", "storno"],
    }
    if (
        dynamic_dir.name == DYNAMIC_DIRNAME
        and hospital_dir.name in DECIMAL_COMMA_DYNAMIC_HOSPITAL_DIRS
    ):
        read_csv_kwargs["decimal"] = ","
    patient_files = sorted(path for path in dynamic_dir.glob("*.csv") if path.is_file())

    tables = []
    for path in patient_files:
        local_stay_id = extract_local_stay_id(path)
        patient_df = pd.read_csv(path, sep=separator, **read_csv_kwargs)
        patient_df = _ensure_dynamic_time_column(patient_df)
        patient_df = _ensure_stay_id_alias_columns(
            patient_df,
            local_stay_id,
            prefer_supplied_values=True,
            allow_conflicts=True,
        )
        tables.append(patient_df)

    if not tables:
        return pd.DataFrame(columns=list(RAW_STAY_ID_COLUMNS))

    return pd.concat(tables, ignore_index=True)


def load_static_for_hospital(hospital_dir: Path) -> pd.DataFrame:
    static_path = hospital_dir / "static" / STATIC_FILENAME
    static_df = pd.read_csv(static_path, sep=";")

    present_stay_id_columns = [
        column for column in RAW_STAY_ID_COLUMNS if column in static_df.columns
    ]
    if not present_stay_id_columns:
        raise ValueError(
            f"Static table is missing all supported stay-id columns: {static_path}"
        )

    return _ensure_stay_id_alias_columns(
        static_df,
        static_df[present_stay_id_columns[0]],
    )


def load_all_hospitals(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> dict[str, HospitalRawTables]:
    return {
        hospital_name_from_dir(hospital_dir): HospitalRawTables(
            static=load_static_for_hospital(hospital_dir),
            dynamic=load_dynamic_for_hospital(hospital_dir),
        )
        for hospital_dir in get_hospital_dirs(raw_dir)
    }


def load_raw_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
    table_kind: Literal["static", "dynamic"] = "static",
) -> dict[str, pd.DataFrame]:
    loader = (
        load_static_for_hospital if table_kind == "static" else load_dynamic_for_hospital
    )
    return {
        hospital_name_from_dir(hospital_dir): loader(hospital_dir)
        for hospital_dir in get_hospital_dirs(raw_dir)
    }


def load_static_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> dict[str, pd.DataFrame]:
    return load_raw_tables(raw_dir=raw_dir, table_kind="static")


def load_dynamic_tables(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> dict[str, pd.DataFrame]:
    return load_raw_tables(raw_dir=raw_dir, table_kind="dynamic")
