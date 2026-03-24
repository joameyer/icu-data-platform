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
    semantic_decisions: pd.DataFrame
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

SEMANTIC_TARGET_VARIABLES = (
    "norepinephrine_iv_cont",
    "clonidine_iv_cont",
    "vt_per_kg_ibw",
    "etco2",
    "ie_ratio",
)

DETERMINISTIC_UNIT_CONVERSION = "deterministic unit conversion"
DETERMINISTIC_DEFINITIONAL_RECODING = "deterministic definitional recoding"
UNRESOLVED_MISMATCH = "unresolved mismatch requiring metadata confirmation"
SITE_INVALID = "site-specific invalid variable to set missing or exclude"
NO_ACTION_NEEDED = "no action needed"

ETCO2_PA_SITE = "asic_UK04"
ETCO2_PA_PER_MMHG = 7.50062

SEMANTIC_DECISION_COLUMNS = [
    "hospital",
    "canonical_name",
    "decision_classification",
    "applied_action",
    "metadata_review_required",
    "chapter1_recommendation",
    "non_null_before",
    "non_null_after",
    "median_before",
    "median_after",
    "candidate_reciprocal_median_before",
    "flagged_metrics_before",
    "evidence",
]


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


def _empty_semantic_decisions() -> pd.DataFrame:
    return pd.DataFrame(columns=SEMANTIC_DECISION_COLUMNS)


def _build_summary_lookup(summary_df: pd.DataFrame) -> dict[tuple[str, str], dict[str, object]]:
    if summary_df.empty:
        return {}
    return {
        (str(row["hospital"]), str(row["canonical_name"])): row.to_dict()
        for _, row in summary_df.iterrows()
    }


def _build_issue_lookup(issue_df: pd.DataFrame) -> dict[tuple[str, str], list[str]]:
    if issue_df.empty:
        return {}
    return {
        (str(row["hospital"]), str(row["canonical_name"])): list(row["flagged_metrics"])
        for _, row in issue_df.iterrows()
    }


def _numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="Float64")
    return pd.to_numeric(df[column], errors="coerce")


def _format_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value):.3f}"


def _candidate_reciprocal(value: object) -> float | pd.NA:
    if value is None or pd.isna(value):
        return pd.NA
    numeric_value = float(value)
    if numeric_value == 0:
        return pd.NA
    return 1.0 / numeric_value


def _apply_semantic_action(before_series: pd.Series, applied_action: str) -> pd.Series:
    if applied_action == "convert_pa_to_mmhg":
        return before_series / ETCO2_PA_PER_MMHG
    if applied_action == "set_missing":
        return pd.Series(pd.NA, index=before_series.index, dtype="Float64")
    return before_series.copy()


def _build_semantic_decision(
    hospital: str,
    canonical_name: str,
    before_series: pd.Series,
    summary_row: dict[str, object] | None,
    flagged_metrics: list[str],
) -> tuple[pd.Series, dict[str, object]]:
    non_null_before = int(before_series.notna().sum())
    median_before = summary_row.get("median") if summary_row is not None else pd.NA
    candidate_reciprocal_median = (
        _candidate_reciprocal(median_before)
        if canonical_name == "ie_ratio"
        else pd.NA
    )

    decision_classification = NO_ACTION_NEEDED
    applied_action = "keep_as_is"
    metadata_review_required = False
    chapter1_recommendation = "retain"
    evidence = "Existing harmonization retained; no targeted semantic action needed."

    if non_null_before == 0:
        applied_action = "leave_missing"
        evidence = "Variable is absent or fully missing for this site in the current sample."
    elif canonical_name == "etco2" and hospital == ETCO2_PA_SITE:
        decision_classification = DETERMINISTIC_UNIT_CONVERSION
        applied_action = "convert_pa_to_mmhg"
        evidence = (
            "Pre-harmonization ETCO2 values are Pa-like for this site "
            f"(median {_format_metric(median_before)}); converted using "
            f"1 mmHg = {ETCO2_PA_PER_MMHG:.5f} Pa."
        )
    elif canonical_name == "vt_per_kg_ibw" and hospital == "asic_UK06":
        decision_classification = SITE_INVALID
        applied_action = "set_missing"
        chapter1_recommendation = "site_drop"
        evidence = (
            "Pre-harmonization values show unrecoverable mixed scales for this site "
            f"(median {_format_metric(median_before)}, max "
            f"{_format_metric(summary_row.get('max') if summary_row is not None else pd.NA)})."
        )
    elif canonical_name == "norepinephrine_iv_cont" and hospital == "asic_UK03":
        decision_classification = SITE_INVALID
        applied_action = "set_missing"
        metadata_review_required = True
        chapter1_recommendation = "site_drop"
        evidence = (
            "Pre-harmonization values are off-scale versus peer sites "
            f"(median {_format_metric(median_before)}); no exact source unit/definition "
            "mapping could be confirmed from the current codebase or metadata."
        )
    elif canonical_name == "clonidine_iv_cont" and hospital == "asic_UK08":
        decision_classification = SITE_INVALID
        applied_action = "set_missing"
        metadata_review_required = True
        chapter1_recommendation = "site_drop"
        evidence = (
            "Pre-harmonization values differ sharply from the peer site "
            f"(median {_format_metric(median_before)}); no exact source unit/definition "
            "mapping could be confirmed from the current codebase or metadata."
        )
    elif canonical_name == "ie_ratio":
        decision_classification = UNRESOLVED_MISMATCH
        applied_action = "keep_as_is_pending_metadata_review"
        metadata_review_required = True
        chapter1_recommendation = "exclude"
        evidence = (
            "Observed site distribution could reflect either I:E or E:I semantics "
            f"(median {_format_metric(median_before)}, reciprocal median "
            f"{_format_metric(candidate_reciprocal_median)}); no documented site "
            "convention was found in the current codebase or metadata."
        )
    elif flagged_metrics:
        evidence = (
            "Existing harmonization retained; distribution summary was monitored for this "
            f"variable-site pair and no confirmed semantic rule was needed. "
            f"Pre-harmonization flagged metrics: {flagged_metrics}."
        )

    after_series = _apply_semantic_action(before_series, applied_action)
    non_null_after = int(after_series.notna().sum())
    median_after = float(after_series.median()) if non_null_after else pd.NA

    return after_series, {
        "hospital": hospital,
        "canonical_name": canonical_name,
        "decision_classification": decision_classification,
        "applied_action": applied_action,
        "metadata_review_required": metadata_review_required,
        "chapter1_recommendation": chapter1_recommendation,
        "non_null_before": non_null_before,
        "non_null_after": non_null_after,
        "median_before": median_before,
        "median_after": median_after,
        "candidate_reciprocal_median_before": candidate_reciprocal_median,
        "flagged_metrics_before": flagged_metrics,
        "evidence": evidence,
    }


def apply_dynamic_semantic_harmonization(
    tables_by_hospital: dict[str, pd.DataFrame],
    min_non_null: int = 20,
    min_hospitals: int = 4,
    fence_factor: float = 1.5,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    harmonized_tables = {
        hospital: df.copy()
        for hospital, df in tables_by_hospital.items()
    }
    pre_distribution_summary = numeric_distribution_summary(
        harmonized_tables,
        min_non_null=min_non_null,
    )
    pre_distribution_issues = flag_cross_hospital_distribution_issues(
        pre_distribution_summary,
        min_hospitals=min_hospitals,
        fence_factor=fence_factor,
    )
    summary_lookup = _build_summary_lookup(pre_distribution_summary)
    issue_lookup = _build_issue_lookup(pre_distribution_issues)
    decision_rows = []

    for hospital in sorted(harmonized_tables):
        df = harmonized_tables[hospital]
        for canonical_name in SEMANTIC_TARGET_VARIABLES:
            before_series = _numeric_column(df, canonical_name)
            after_series, decision_row = _build_semantic_decision(
                hospital,
                canonical_name,
                before_series,
                summary_lookup.get((hospital, canonical_name)),
                issue_lookup.get((hospital, canonical_name), []),
            )
            if canonical_name in df.columns and decision_row["applied_action"] in {
                "convert_pa_to_mmhg",
                "set_missing",
            }:
                df[canonical_name] = after_series
            decision_rows.append(decision_row)

    if not decision_rows:
        return harmonized_tables, _empty_semantic_decisions()

    decisions_df = pd.DataFrame(decision_rows).sort_values(
        ["canonical_name", "hospital"]
    ).reset_index(drop=True)
    return harmonized_tables, decisions_df


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

    for hospital, raw_df in raw_tables.items():
        harmonized_df, source_map = build_harmonized_dynamic_table(raw_df, hospital, translation)
        harmonized_df = harmonized_df[[column for column in ordered_columns if column in harmonized_df.columns]].copy()

        tables_by_hospital[hospital] = harmonized_df
        source_map_rows.extend(_ordered_source_map_rows(hospital, harmonized_df.columns, source_map))

    tables_by_hospital, semantic_decisions = apply_dynamic_semantic_harmonization(
        tables_by_hospital,
        min_non_null=min_non_null,
        min_hospitals=min_hospitals,
        fence_factor=fence_factor,
    )

    schema_rows = []
    for hospital, harmonized_df in tables_by_hospital.items():
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
        semantic_decisions=semantic_decisions,
        distribution_summary=distribution_summary,
        distribution_issues=distribution_issues,
    )
