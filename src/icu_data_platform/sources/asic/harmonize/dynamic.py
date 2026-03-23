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


DYNAMIC_COLUMN_PREFIX = [
    "hospital_id",
    "stay_id_global",
    "stay_id_local",
    "time",
    "minutes_since_admit",
]

DYNAMIC_THEME_GROUPS = {
    "vitals": [
        "heart_rate",
        "sbp",
        "map",
        "dbp",
        "resp_rate",
        "spont_resp_rate",
        "core_temp",
        "spo2",
        "sao2",
        "scvo2",
        "cvp",
    ],
    "ventilation": [
        "fio2",
        "feo2",
        "peep",
        "delta_p",
        "insp_pressure",
        "compliance",
        "ie_ratio",
        "vt",
        "vt_per_kg_ibw",
        "etco2",
        "pf_ratio",
    ],
    "blood_gas": [
        "pao2",
        "paco2",
        "ph_art",
        "bicarbonate_art",
        "base_excess_art",
        "lactate_art",
    ],
    "hemodynamics": [
        "pap_sys",
        "pap_mean",
        "pap_dias",
        "pcwp",
        "cardiac_output_bolus",
        "cardiac_output_cont",
        "cardiac_index_bolus",
        "cardiac_index_cont",
        "stroke_volume_bolus",
        "stroke_volume_cont",
        "stroke_index_bolus",
        "stroke_index_cont",
        "svri",
        "pvri",
        "gedvi",
        "evlwi",
    ],
    "hematology_coag": [
        "hemoglobin",
        "hematocrit",
        "wbc",
        "platelets",
        "lymph_abs",
        "lymph_pct",
        "inr",
        "ptt",
        "d_dimer",
    ],
    "chemistry_inflammation": [
        "albumin",
        "crp",
        "pct",
        "il6",
        "bilirubin_total",
        "urea",
        "creatinine",
        "bnp",
        "ntprobnp",
        "ast",
        "alt",
        "ldh",
        "amylase",
        "lipase",
        "troponin",
        "ck",
        "ck_mb",
    ],
    "support_therapy": [
        "fluid_balance_24h",
        "position_therapy",
        "ecmo",
        "ecmo_o2",
        "extracorp_blood_flow",
        "extracorp_o2_flow",
        "inhaled_no",
        "inhaled_iloprost",
    ],
    "vasoactive_inotrope": [
        "dobutamine_iv_cont",
        "epinephrine_iv_cont",
        "norepinephrine_iv_cont",
        "vasopressin_iv_cont",
        "milrinone_iv_cont",
        "levosimendan_iv_cont",
        "terlipressin_iv_bolus",
    ],
    "sedation_analgesia": [
        "propofol_iv_cont",
        "midazolam_iv_cont",
        "clonidine_iv_cont",
        "dexmedetomidine_iv_cont",
        "ketanest_iv_cont",
        "isoflurane_inh",
        "sevoflurane_inh",
        "sufentanil_iv_cont",
        "fentanyl_iv_cont",
        "morphine_iv_cont",
        "rocuronium_iv_bolus",
    ],
    "other_meds": [
        "furosemide_iv_cont",
        "hydrocortisone_iv_bolus",
        "prednisolone_iv_bolus",
        "dexamethasone_iv_bolus",
        "fludrocortisone_po_bolus",
    ],
    "scores": [
        "sofa",
    ],
}


def _ordered_theme_columns(canonical_columns: list[str]) -> list[str]:
    theme_assignment_counts: dict[str, int] = {}
    ordered_columns: list[str] = []

    for theme_columns in DYNAMIC_THEME_GROUPS.values():
        for column in theme_columns:
            theme_assignment_counts[column] = theme_assignment_counts.get(column, 0) + 1
            if column in canonical_columns and column not in ordered_columns:
                ordered_columns.append(column)

    duplicate_theme_assignments = sorted(
        column
        for column, count in theme_assignment_counts.items()
        if count > 1
    )
    if duplicate_theme_assignments:
        raise ValueError(
            "Dynamic theme groups assign the same column more than once: "
            f"{duplicate_theme_assignments}"
        )

    unassigned_columns = sorted(
        column
        for column in canonical_columns
        if column not in DYNAMIC_COLUMN_PREFIX and column not in ordered_columns
    )
    if unassigned_columns:
        raise ValueError(
            "Dynamic column order is missing theme assignments for non-prefix columns: "
            f"{unassigned_columns}"
        )

    return ordered_columns


def build_dynamic_column_order(translation: dict[str, str]) -> list[str]:
    canonical_columns = sorted(
        "stay_id_local" if column == "stay_id" else column
        for column in set(translation.values())
    )
    ordered = [
        column
        for column in DYNAMIC_COLUMN_PREFIX
        if column in {"hospital_id", "stay_id_global"} or column in canonical_columns
    ]
    ordered.extend(_ordered_theme_columns(canonical_columns))
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
