from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
import warnings

import pandas as pd

from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
    load_dynamic_tables,
    load_dynamic_translation,
)
from icu_data_platform.sources.asic.qc.dynamic_checks import (
    NON_NUMERIC_CANONICAL_COLUMNS,
    build_harmonized_dynamic_table,
    find_non_numeric_value_issues,
    flag_cross_hospital_distribution_issues,
    normalize_missing,
    numeric_distribution_summary,
)


@dataclass(frozen=True)
class HarmonizedDynamicResult:
    tables_by_hospital: dict[str, pd.DataFrame]
    combined: pd.DataFrame
    source_map: pd.DataFrame
    schema_summary: pd.DataFrame
    non_numeric_issues: pd.DataFrame
    distribution_summary: pd.DataFrame
    distribution_issues: pd.DataFrame


def empty_columns(df: pd.DataFrame) -> list[str]:
    return [column for column in df.columns if normalize_missing(df[column]).isna().all()]


def build_dynamic_column_order(translation: dict[str, str]) -> list[str]:
    canonical_columns = sorted(
        "stay_id_local" if column == "stay_id" else column
        for column in set(translation.values())
    )
    preferred_first = [
        "hospital_id",
        "stay_id_global",
        "stay_id_local",
        "time",
        "minutes_since_admit",
    ]
    ordered = [
        column
        for column in preferred_first
        if column in {"hospital_id", "stay_id_global"} or column in canonical_columns
    ]
    ordered.extend(column for column in canonical_columns if column not in ordered)
    return ordered


def _ordered_source_map_rows(
    hospital: str,
    ordered_columns: Iterable[str],
    source_map: dict[str, list[str]],
) -> list[dict[str, object]]:
    return [
        {
            "hospital": hospital,
            "canonical_name": canonical_name,
            "raw_source_columns_used": source_map.get(canonical_name, []),
        }
        for canonical_name in ordered_columns
    ]


def harmonize_dynamic_tables(
    raw_dir=DEFAULT_ASIC_RAW_DATA_DIR,
    translation_path=DEFAULT_TRANSLATION_PATH,
    min_non_null: int = 20,
    min_hospitals: int = 4,
    fence_factor: float = 1.5,
) -> HarmonizedDynamicResult:
    translation = load_dynamic_translation(translation_path)
    raw_tables = load_dynamic_tables(raw_dir)
    ordered_columns = build_dynamic_column_order(translation)

    tables_by_hospital: dict[str, pd.DataFrame] = {}
    source_map_rows = []
    schema_rows = []

    for hospital, raw_df in raw_tables.items():
        harmonized_df, source_map = build_harmonized_dynamic_table(raw_df, hospital, translation)
        harmonized_df = harmonized_df[[column for column in ordered_columns if column in harmonized_df.columns]].copy()

        tables_by_hospital[hospital] = harmonized_df
        source_map_rows.extend(_ordered_source_map_rows(hospital, harmonized_df.columns, source_map))

        schema_rows.append(
            {
                "hospital": hospital,
                "rows": harmonized_df.shape[0],
                "final_columns": harmonized_df.shape[1],
                "empty_columns": empty_columns(harmonized_df),
            }
        )

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                "The behavior of DataFrame concatenation with empty or all-NA entries "
                "is deprecated.*"
            ),
            category=FutureWarning,
        )
        combined = pd.concat(tables_by_hospital.values(), ignore_index=True)
    source_map_df = pd.DataFrame(source_map_rows)
    schema_summary_df = pd.DataFrame(schema_rows).sort_values("hospital").reset_index(drop=True)
    non_numeric_issues = find_non_numeric_value_issues(dynamic_tables=raw_tables, translation=translation)
    distribution_summary = numeric_distribution_summary(tables_by_hospital, min_non_null=min_non_null)
    distribution_issues = flag_cross_hospital_distribution_issues(
        distribution_summary,
        min_hospitals=min_hospitals,
        fence_factor=fence_factor,
    )

    return HarmonizedDynamicResult(
        tables_by_hospital=tables_by_hospital,
        combined=combined,
        source_map=source_map_df,
        schema_summary=schema_summary_df,
        non_numeric_issues=non_numeric_issues,
        distribution_summary=distribution_summary,
        distribution_issues=distribution_issues,
    )
