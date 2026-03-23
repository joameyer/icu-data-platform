from __future__ import annotations

import re
import warnings
from collections import defaultdict
from pathlib import Path

import pandas as pd

from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
    load_dynamic_tables as load_dynamic_raw_tables,
    load_dynamic_translation as load_dynamic_translation_map,
)
from icu_data_platform.sources.asic.stay_ids import (
    DERIVED_GLOBAL_STAY_ID_SOURCE,
    build_stay_id_global_series,
    normalize_stay_id_local_series,
)


DEFAULT_DYNAMIC_DATA_DIR = DEFAULT_ASIC_RAW_DATA_DIR

NON_NUMERIC_CANONICAL_COLUMNS = frozenset(
    {"hospital_id", "stay_id_local", "stay_id_global", "time"}
)
UK04_SPECIAL_STRING_HOSPITAL = "asic_UK04"
IE_RATIO_PATTERN = re.compile(
    r"^\s*([0-9]+(?:\.[0-9]+)?)\s*/\s*([0-9]+(?:\.[0-9]+)?)\s*$"
)


def load_dynamic_translation(
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
) -> dict[str, str]:
    return load_dynamic_translation_map(translation_path)


def load_dynamic_tables(
    raw_dir: Path = DEFAULT_DYNAMIC_DATA_DIR,
) -> dict[str, pd.DataFrame]:
    return load_dynamic_raw_tables(raw_dir)


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


def parse_ie_ratio_value(value: object) -> float | pd.NA:
    if pd.isna(value):
        return pd.NA

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return pd.NA
        ratio_match = IE_RATIO_PATTERN.match(text)
        if ratio_match:
            numerator = float(ratio_match.group(1))
            denominator = float(ratio_match.group(2))
            if denominator == 0:
                return pd.NA
            return numerator / denominator
        try:
            return float(text)
        except ValueError:
            return pd.NA

    try:
        return float(value)
    except (TypeError, ValueError):
        return pd.NA


def parse_ie_ratio_series(series: pd.Series) -> pd.Series:
    parsed = series.map(parse_ie_ratio_value)
    return pd.to_numeric(parsed, errors="coerce")


def clean_uk04_numeric_strings(series: pd.Series) -> pd.Series:
    if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
        return series

    cleaned = to_clean_string(series)
    storniert_mask = cleaned.str.lower().isin({"storniert", "storno"})
    less_than_mask = cleaned.str.contains("<", regex=False, na=False)
    cleaned = cleaned.mask(storniert_mask, pd.NA)
    cleaned = cleaned.mask(less_than_mask, "0")
    return cleaned


def apply_hospital_numeric_cleaning(
    series: pd.Series,
    hospital: str | None = None,
) -> pd.Series:
    normalized = normalize_missing(series)
    if hospital == UK04_SPECIAL_STRING_HOSPITAL:
        return clean_uk04_numeric_strings(normalized)
    return normalized


def unresolved_numeric_string_examples(
    cleaned_series: pd.Series,
    parsed_numeric: pd.Series,
    max_examples: int = 10,
) -> tuple[int, list[str]]:
    string_mask = cleaned_series.map(lambda value: isinstance(value, str))
    unresolved_mask = string_mask & cleaned_series.notna() & parsed_numeric.isna()
    examples = (
        cleaned_series[unresolved_mask]
        .astype("string")
        .dropna()
        .drop_duplicates()
        .tolist()[:max_examples]
    )
    return int(unresolved_mask.sum()), examples


def warn_on_unresolved_numeric_strings(
    cleaned_series: pd.Series,
    parsed_numeric: pd.Series,
    canonical_name: str,
    hospital: str,
    raw_columns: list[str] | None = None,
) -> None:
    unresolved_count, examples = unresolved_numeric_string_examples(
        cleaned_series,
        parsed_numeric,
    )
    if unresolved_count == 0:
        return

    raw_columns_note = ""
    if raw_columns:
        raw_columns_note = f" (raw columns: {', '.join(raw_columns)})"

    warnings.warn(
        (
            f"{hospital} column '{canonical_name}'{raw_columns_note} contains "
            f"{unresolved_count} unexpected string value(s) after custom cleaning; "
            f"examples: {examples}"
        ),
        RuntimeWarning,
        stacklevel=2,
    )


def coerce_numeric_series(
    series: pd.Series,
    canonical_name: str,
    hospital: str | None = None,
    raw_columns: list[str] | None = None,
    warn_on_unparsed_strings: bool = False,
) -> pd.Series:
    cleaned = apply_hospital_numeric_cleaning(series, hospital)
    if canonical_name == "ie_ratio":
        numeric = parse_ie_ratio_series(cleaned)
    else:
        numeric = pd.to_numeric(cleaned, errors="coerce")

    if warn_on_unparsed_strings and hospital is not None:
        warn_on_unresolved_numeric_strings(
            cleaned,
            numeric,
            canonical_name,
            hospital,
            raw_columns=raw_columns,
        )

    return numeric


def build_canonical_to_raw(
    translation: dict[str, str],
) -> dict[str, list[str]]:
    canonical_to_raw: dict[str, list[str]] = defaultdict(list)
    for raw_column, canonical_name in translation.items():
        final_name = "stay_id_local" if canonical_name == "stay_id" else canonical_name
        canonical_to_raw[final_name].append(raw_column)
    return dict(canonical_to_raw)


def merge_raw_columns(
    df: pd.DataFrame,
    raw_columns: list[str],
    canonical_name: str,
    hospital: str,
) -> tuple[pd.Series, list[str]]:
    present = [column for column in raw_columns if column in df.columns]
    if not present:
        return pd.Series(pd.NA, index=df.index, dtype="object"), []

    if canonical_name == "stay_id" and len(present) > 1:
        base = df[present[0]]
        for other in present[1:]:
            if not series_values_equal(base, df[other]):
                raise ValueError(f"Conflicting stay_id columns in {hospital}: {present}")

    merged = df[present[0]].copy()
    for other in present[1:]:
        merged = merged.combine_first(df[other])
    return merged, present


def build_harmonized_dynamic_table(
    df: pd.DataFrame,
    hospital: str,
    translation: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    if translation is None:
        translation = load_dynamic_translation()

    canonical_to_raw = build_canonical_to_raw(translation)
    ordered_canonical_columns = ["hospital_id"] + sorted(canonical_to_raw.keys())

    columns_dict: dict[str, pd.Series] = {
        "hospital_id": pd.Series(hospital, index=df.index, dtype="string")
    }
    source_map = {"hospital_id": ["derived from filename"]}

    for canonical_name, raw_columns in canonical_to_raw.items():
        merged, used_columns = merge_raw_columns(df, raw_columns, canonical_name, hospital)
        if canonical_name in NON_NUMERIC_CANONICAL_COLUMNS:
            columns_dict[canonical_name] = to_clean_string(merged)
        else:
            columns_dict[canonical_name] = coerce_numeric_series(
                merged,
                canonical_name,
                hospital=hospital,
                raw_columns=used_columns,
                warn_on_unparsed_strings=True,
            )
        source_map[canonical_name] = used_columns

    if "stay_id_local" in columns_dict:
        columns_dict["stay_id_local"] = normalize_stay_id_local_series(columns_dict["stay_id_local"])
        columns_dict["stay_id_global"] = build_stay_id_global_series(
            columns_dict["hospital_id"],
            columns_dict["stay_id_local"],
        )
        source_map["stay_id_global"] = [DERIVED_GLOBAL_STAY_ID_SOURCE]

    harmonized = pd.DataFrame(columns_dict, index=df.index)
    ordered_with_global = [
        "hospital_id",
        "stay_id_global",
        "stay_id_local",
        "time",
        "minutes_since_admit",
    ]
    ordered_with_global.extend(
        column
        for column in sorted(harmonized.columns)
        if column not in ordered_with_global
    )
    final_columns = [column for column in ordered_with_global if column in harmonized.columns]
    return harmonized[final_columns], source_map


def build_harmonized_dynamic_tables(
    raw_dir: Path = DEFAULT_DYNAMIC_DATA_DIR,
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
) -> dict[str, pd.DataFrame]:
    translation = load_dynamic_translation(translation_path)
    dynamic_tables = load_dynamic_tables(raw_dir)
    return {
        hospital: build_harmonized_dynamic_table(df, hospital, translation)[0]
        for hospital, df in dynamic_tables.items()
    }


def find_non_numeric_value_issues(
    dynamic_tables: dict[str, pd.DataFrame] | None = None,
    translation: dict[str, str] | None = None,
) -> pd.DataFrame:
    if translation is None:
        translation = load_dynamic_translation()
    if dynamic_tables is None:
        dynamic_tables = load_dynamic_tables()

    rows = []
    for hospital, df in dynamic_tables.items():
        for raw_column, canonical_name in translation.items():
            if raw_column not in df.columns:
                continue
            final_name = "stay_id_local" if canonical_name == "stay_id" else canonical_name
            if final_name in NON_NUMERIC_CANONICAL_COLUMNS:
                continue

            raw_series = normalize_missing(df[raw_column])
            if raw_series.isna().all():
                continue

            raw_numeric = pd.to_numeric(raw_series, errors="coerce")
            raw_non_numeric_mask = raw_series.notna() & raw_numeric.isna()
            if not raw_non_numeric_mask.any():
                continue

            cleaned_series = apply_hospital_numeric_cleaning(raw_series, hospital)
            parsed_numeric = coerce_numeric_series(
                raw_series,
                final_name,
                hospital=hospital,
            )
            unresolved_count, _ = unresolved_numeric_string_examples(
                cleaned_series,
                parsed_numeric,
            )

            bad_examples = (
                raw_series[raw_non_numeric_mask]
                .astype("string")
                .dropna()
                .drop_duplicates()
                .tolist()[:10]
            )

            rows.append(
                {
                    "hospital": hospital,
                    "raw_column": raw_column,
                    "canonical_name": final_name,
                    "non_null_count": int(raw_series.notna().sum()),
                    "raw_non_numeric_count": int(raw_non_numeric_mask.sum()),
                    "resolved_by_custom_parser_count": int(
                        raw_non_numeric_mask.sum() - unresolved_count
                    ),
                    "unresolved_after_custom_parser_count": int(unresolved_count),
                    "non_numeric_examples": bad_examples,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "hospital",
                "raw_column",
                "canonical_name",
                "non_null_count",
                "raw_non_numeric_count",
                "resolved_by_custom_parser_count",
                "unresolved_after_custom_parser_count",
                "non_numeric_examples",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["unresolved_after_custom_parser_count", "raw_non_numeric_count", "hospital", "raw_column"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)


def numeric_distribution_summary(
    harmonized_tables: dict[str, pd.DataFrame],
    min_non_null: int = 20,
) -> pd.DataFrame:
    rows = []
    for hospital, df in harmonized_tables.items():
        for column in df.columns:
            if column in NON_NUMERIC_CANONICAL_COLUMNS:
                continue

            numeric = pd.to_numeric(df[column], errors="coerce").dropna()
            if numeric.shape[0] < min_non_null:
                continue

            q1 = numeric.quantile(0.25)
            q3 = numeric.quantile(0.75)
            rows.append(
                {
                    "hospital": hospital,
                    "canonical_name": column,
                    "n": int(numeric.shape[0]),
                    "min": float(numeric.min()),
                    "q1": float(q1),
                    "median": float(numeric.median()),
                    "q3": float(q3),
                    "max": float(numeric.max()),
                    "iqr": float(q3 - q1),
                    "range_width": float(numeric.max() - numeric.min()),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "hospital",
                "canonical_name",
                "n",
                "min",
                "q1",
                "median",
                "q3",
                "max",
                "iqr",
                "range_width",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["canonical_name", "hospital"]
    ).reset_index(drop=True)


def _metric_outlier_bounds(values: pd.Series, fence_factor: float) -> tuple[float, float]:
    q1 = float(values.quantile(0.25))
    q3 = float(values.quantile(0.75))
    iqr = q3 - q1
    if iqr == 0:
        return q1, q3
    return q1 - fence_factor * iqr, q3 + fence_factor * iqr


def flag_cross_hospital_distribution_issues(
    summary_df: pd.DataFrame,
    min_hospitals: int = 4,
    fence_factor: float = 1.5,
) -> pd.DataFrame:
    if summary_df.empty:
        return pd.DataFrame(
            columns=[
                "canonical_name",
                "hospital",
                "n",
                "flagged_metrics",
                "min",
                "median",
                "iqr",
                "max",
                "range_width",
            ]
        )

    metric_columns = ["min", "median", "iqr", "max", "range_width"]
    issue_rows = []

    for canonical_name, group in summary_df.groupby("canonical_name"):
        if group["hospital"].nunique() < min_hospitals:
            continue

        metric_bounds = {
            metric: _metric_outlier_bounds(group[metric], fence_factor)
            for metric in metric_columns
        }

        for _, row in group.iterrows():
            flagged_metrics = []
            for metric in metric_columns:
                lower, upper = metric_bounds[metric]
                if row[metric] < lower or row[metric] > upper:
                    flagged_metrics.append(metric)

            if flagged_metrics:
                issue_rows.append(
                    {
                        "canonical_name": canonical_name,
                        "hospital": row["hospital"],
                        "n": int(row["n"]),
                        "flagged_metrics": flagged_metrics,
                        "min": row["min"],
                        "median": row["median"],
                        "iqr": row["iqr"],
                        "max": row["max"],
                        "range_width": row["range_width"],
                    }
                )

    if not issue_rows:
        return pd.DataFrame(
            columns=[
                "canonical_name",
                "hospital",
                "n",
                "flagged_metrics",
                "min",
                "median",
                "iqr",
                "max",
                "range_width",
            ]
        )

    return pd.DataFrame(issue_rows).sort_values(
        ["canonical_name", "hospital"]
    ).reset_index(drop=True)
