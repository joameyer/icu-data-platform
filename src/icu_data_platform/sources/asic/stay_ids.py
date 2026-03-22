from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pandas as pd


DERIVED_GLOBAL_STAY_ID_SOURCE = "derived from hospital_id and stay_id_local"


def _clean_string_value(value: object) -> str | pd.NA:
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if not text:
        return pd.NA
    return text


def normalize_stay_id_local_value(value: object) -> str | pd.NA:
    cleaned = _clean_string_value(value)
    if cleaned is pd.NA:
        return pd.NA

    try:
        numeric_value = Decimal(cleaned)
    except InvalidOperation:
        return cleaned

    normalized = format(numeric_value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized == "-0":
        normalized = "0"
    return normalized


def normalize_stay_id_local_series(series: pd.Series) -> pd.Series:
    normalized = series.map(normalize_stay_id_local_value)
    return pd.Series(normalized, index=series.index, dtype="string")


def build_stay_id_global_series(
    hospital_id: pd.Series,
    stay_id_local: pd.Series,
) -> pd.Series:
    hospital_clean = pd.Series(hospital_id, index=hospital_id.index, dtype="string").str.strip()
    stay_local_clean = pd.Series(stay_id_local, index=stay_id_local.index, dtype="string").str.strip()
    missing_mask = hospital_clean.isna() | stay_local_clean.isna()
    global_id = hospital_clean + "_" + stay_local_clean
    return global_id.mask(missing_mask, pd.NA).astype("string")
