from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import warnings

import pandas as pd

from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
    load_static_tables,
    load_static_translation,
)
from icu_data_platform.sources.asic.stay_ids import (
    DERIVED_GLOBAL_STAY_ID_SOURCE,
    build_stay_id_global_series,
    normalize_stay_id_local_series,
)


DROPPED_STATIC_CANONICAL_COLUMNS = frozenset({"cluster_id", "study_day", "phase"})
FINAL_STATIC_COLUMNS = [
    "hospital_id",
    "stay_id_global",
    "stay_id_local",
    "age_group",
    "sex",
    "height_group",
    "weight_group",
    "bmi_group",
    "hosp_mortality",
    "icu_mortality",
    "hosp_los",
    "icu_los",
    "icu_readmit",
    "dialysis_free_days",
    "vent_free_days",
    "icd10_codes",
]
STATIC_NUMERIC_COLUMNS = frozenset(
    {
        "hosp_mortality",
        "icu_mortality",
        "hosp_los",
        "icu_los",
        "icu_readmit",
        "dialysis_free_days",
        "vent_free_days",
    }
)
STATIC_STRING_COLUMNS = frozenset(set(FINAL_STATIC_COLUMNS) - STATIC_NUMERIC_COLUMNS)
MINUS_ONE_TO_NA_COLUMNS = frozenset(
    {
        "weight_group",
        "hosp_los",
        "icu_los",
        "icu_readmit",
        "dialysis_free_days",
        "vent_free_days",
    }
)
BMI_GROUP_MAP = {
    "L": "Underweight",
    "M": "Normal Weight",
    "P": "Overweight",
    "1": "Obesity Class 1",
    "2": "Obesity Class 2",
    "3": "Obesity Class 3",
    "X": pd.NA,
    "NAN": pd.NA,
}


@dataclass(frozen=True)
class HarmonizedStaticResult:
    tables_by_hospital: dict[str, pd.DataFrame]
    combined: pd.DataFrame
    source_map: pd.DataFrame
    schema_summary: pd.DataFrame
    categorical_value_summary: pd.DataFrame


def normalize_missing(series: pd.Series) -> pd.Series:
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        return series.replace(r"^\s*$", pd.NA, regex=True)
    return series


def to_clean_string(series: pd.Series) -> pd.Series:
    return normalize_missing(series).astype("string").str.strip()


def series_values_equal(left: pd.Series, right: pd.Series) -> bool:
    left_norm = to_clean_string(left).fillna("<NA>")
    right_norm = to_clean_string(right).fillna("<NA>")
    return left_norm.equals(right_norm)


def replace_minus_one_with_na(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return series.mask(series == -1)
    cleaned = to_clean_string(series)
    return cleaned.replace({"-1": pd.NA, "-1.0": pd.NA})


def empty_columns(df: pd.DataFrame) -> list[str]:
    return [column for column in df.columns if normalize_missing(df[column]).isna().all()]


def build_static_canonical_to_raw(
    translation: dict[str, str],
) -> dict[str, list[str]]:
    canonical_to_raw: dict[str, list[str]] = {}
    for raw_column, canonical_name in translation.items():
        if canonical_name in DROPPED_STATIC_CANONICAL_COLUMNS:
            continue

        final_name = canonical_name
        if canonical_name == "stay_id":
            final_name = "stay_id_local"
        if canonical_name == "discharge_status":
            final_name = "hosp_mortality"
        elif canonical_name == "death_status":
            final_name = "icu_mortality"

        canonical_to_raw.setdefault(final_name, []).append(raw_column)

    return canonical_to_raw


def merge_raw_columns(
    df: pd.DataFrame,
    raw_columns: list[str],
    canonical_name: str,
    hospital: str,
) -> tuple[pd.Series, list[str]]:
    present = [column for column in raw_columns if column in df.columns]
    if not present:
        return pd.Series(pd.NA, index=df.index, dtype="object"), []

    merged = df[present[0]].copy()
    for other in present[1:]:
        if not series_values_equal(merged, df[other]):
            raise ValueError(
                f"Conflicting duplicate columns for {canonical_name!r} in {hospital}: {present}"
            )
        merged = merged.combine_first(df[other])

    return merged, present


def _warn_on_unexpected_values(
    cleaned: pd.Series,
    expected_values: Iterable[str],
    hospital: str,
    canonical_name: str,
) -> None:
    expected = set(expected_values)
    unexpected = cleaned[cleaned.notna() & ~cleaned.isin(expected)].drop_duplicates().tolist()
    if not unexpected:
        return

    warnings.warn(
        (
            f"{hospital} column '{canonical_name}' contains unexpected value(s) after "
            f"static recoding: {unexpected[:10]}"
        ),
        RuntimeWarning,
        stacklevel=2,
    )


def recode_sex(series: pd.Series, hospital: str) -> pd.Series:
    cleaned = to_clean_string(series).str.upper()
    _warn_on_unexpected_values(cleaned, {"W", "M"}, hospital, "sex")
    return cleaned.replace({"W": "F"})


def recode_bmi_group(series: pd.Series, hospital: str) -> pd.Series:
    cleaned = to_clean_string(series).str.upper()
    _warn_on_unexpected_values(
        cleaned,
        set(BMI_GROUP_MAP),
        hospital,
        "bmi_group",
    )
    return cleaned.replace(BMI_GROUP_MAP)


def derive_hosp_mortality(series: pd.Series, hospital: str) -> pd.Series:
    cleaned = to_clean_string(series).str.lower()
    result = pd.Series(pd.NA, index=series.index, dtype="Int64")
    died_mask = cleaned.str.contains("verstorben", na=False) | cleaned.str.contains(
        "verstoben",
        na=False,
    )
    survived_mask = cleaned.str.contains("verlegt", na=False)
    unexpected_mask = cleaned.notna() & ~(died_mask | survived_mask)
    if unexpected_mask.any():
        warnings.warn(
            (
                f"{hospital} column 'hosp_mortality' contains unexpected discharge "
                f"value(s): {cleaned[unexpected_mask].drop_duplicates().tolist()[:10]}"
            ),
            RuntimeWarning,
            stacklevel=2,
        )
    result[died_mask] = 1
    result[survived_mask] = 0
    return result


def derive_icu_mortality(series: pd.Series, hospital: str) -> pd.Series:
    cleaned = to_clean_string(series).str.upper()
    result = pd.Series(pd.NA, index=series.index, dtype="Int64")
    died_mask = cleaned == "ICU"
    survived_mask = cleaned.isin(["0", "KH"])
    unexpected_mask = cleaned.notna() & ~(died_mask | survived_mask)
    if unexpected_mask.any():
        warnings.warn(
            (
                f"{hospital} column 'icu_mortality' contains unexpected death-status "
                f"value(s): {cleaned[unexpected_mask].drop_duplicates().tolist()[:10]}"
            ),
            RuntimeWarning,
            stacklevel=2,
        )
    result[died_mask] = 1
    result[survived_mask] = 0
    return result


def cast_static_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in FINAL_STATIC_COLUMNS:
        if column in STATIC_STRING_COLUMNS:
            result[column] = to_clean_string(result[column])
        elif column in STATIC_NUMERIC_COLUMNS:
            result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
    return result


def build_harmonized_static_table(
    df: pd.DataFrame,
    hospital: str,
    translation: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    if translation is None:
        translation = load_static_translation()

    canonical_to_raw = build_static_canonical_to_raw(translation)
    harmonized = pd.DataFrame(index=df.index)
    harmonized["hospital_id"] = pd.Series(hospital, index=df.index, dtype="string")
    source_map = {"hospital_id": ["derived from filename"]}

    passthrough_columns = [
        "stay_id_local",
        "age_group",
        "sex",
        "height_group",
        "weight_group",
        "bmi_group",
        "hosp_los",
        "icu_los",
        "icu_readmit",
        "dialysis_free_days",
        "vent_free_days",
        "icd10_codes",
    ]
    for canonical_name in passthrough_columns:
        merged, used_columns = merge_raw_columns(
            df,
            canonical_to_raw.get(canonical_name, []),
            canonical_name,
            hospital,
        )
        harmonized[canonical_name] = merged
        source_map[canonical_name] = used_columns

    harmonized["stay_id_local"] = normalize_stay_id_local_series(harmonized["stay_id_local"])
    harmonized["stay_id_global"] = build_stay_id_global_series(
        harmonized["hospital_id"],
        harmonized["stay_id_local"],
    )
    source_map["stay_id_global"] = [DERIVED_GLOBAL_STAY_ID_SOURCE]

    merged, used_columns = merge_raw_columns(
        df,
        canonical_to_raw.get("hosp_mortality", []),
        "hosp_mortality",
        hospital,
    )
    harmonized["hosp_mortality"] = derive_hosp_mortality(merged, hospital)
    source_map["hosp_mortality"] = used_columns

    merged, used_columns = merge_raw_columns(
        df,
        canonical_to_raw.get("icu_mortality", []),
        "icu_mortality",
        hospital,
    )
    harmonized["icu_mortality"] = derive_icu_mortality(merged, hospital)
    source_map["icu_mortality"] = used_columns

    harmonized["sex"] = recode_sex(harmonized["sex"], hospital)
    harmonized["bmi_group"] = recode_bmi_group(harmonized["bmi_group"], hospital)

    for column in MINUS_ONE_TO_NA_COLUMNS:
        harmonized[column] = replace_minus_one_with_na(harmonized[column])

    harmonized = cast_static_columns(harmonized[FINAL_STATIC_COLUMNS])
    return harmonized, source_map


def summarize_static_categorical_values(
    combined_df: pd.DataFrame,
    columns: Iterable[str] = ("sex", "bmi_group", "hosp_mortality", "icu_mortality"),
) -> pd.DataFrame:
    rows = []
    for column in columns:
        summary = (
            combined_df.groupby("hospital_id", dropna=False)[column]
            .value_counts(dropna=False)
            .reset_index(name="count")
        )
        summary.insert(1, "column", column)
        summary = summary.rename(columns={column: "value"})
        rows.append(summary)

    if not rows:
        return pd.DataFrame(columns=["hospital_id", "column", "value", "count"])

    return pd.concat(rows, ignore_index=True).sort_values(
        ["column", "hospital_id", "value"]
    ).reset_index(drop=True)


def harmonize_static_tables(
    raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
    translation_path=DEFAULT_TRANSLATION_PATH,
) -> HarmonizedStaticResult:
    translation = load_static_translation(translation_path)
    raw_tables = load_static_tables(raw_dir)

    tables_by_hospital: dict[str, pd.DataFrame] = {}
    source_map_rows = []
    schema_rows = []

    for hospital, raw_df in raw_tables.items():
        harmonized_df, source_map = build_harmonized_static_table(raw_df, hospital, translation)
        tables_by_hospital[hospital] = harmonized_df

        schema_rows.append(
            {
                "hospital": hospital,
                "rows": harmonized_df.shape[0],
                "final_columns": harmonized_df.shape[1],
                "empty_columns": empty_columns(harmonized_df),
            }
        )

        for canonical_name in FINAL_STATIC_COLUMNS:
            source_map_rows.append(
                {
                    "hospital": hospital,
                    "canonical_name": canonical_name,
                    "raw_source_columns_used": source_map.get(canonical_name, []),
                }
            )

    combined = pd.concat(tables_by_hospital.values(), ignore_index=True)
    source_map_df = pd.DataFrame(source_map_rows)
    schema_summary_df = pd.DataFrame(schema_rows).sort_values("hospital").reset_index(drop=True)
    categorical_value_summary = summarize_static_categorical_values(combined)

    return HarmonizedStaticResult(
        tables_by_hospital=tables_by_hospital,
        combined=combined,
        source_map=source_map_df,
        schema_summary=schema_summary_df,
        categorical_value_summary=categorical_value_summary,
    )
