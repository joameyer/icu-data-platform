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
    invalid_value_rules: pd.DataFrame
    invalid_value_qc: pd.DataFrame
    distribution_summary: pd.DataFrame
    distribution_issues: pd.DataFrame


@dataclass(frozen=True)
class InvalidValueRule:
    canonical_name: str
    hard_min: float | None = None
    hard_max: float | None = None
    invalid_zero: bool = False
    description: str = ""


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

INVALID_VALUE_RULE_COLUMNS = [
    "canonical_name",
    "hard_min",
    "hard_max",
    "invalid_zero",
    "description",
]

INVALID_VALUE_QC_COLUMNS = [
    "hospital",
    "canonical_name",
    "non_null_before",
    "non_null_after",
    "invalid_count",
    "invalid_proportion",
    "invalid_examples",
    "variable_total_invalid_count",
    "hospital_share_of_variable_invalids",
    "dominant_invalid_hospital",
    "dominant_invalid_hospital_share",
    "invalidity_concentrated_in_specific_hospitals",
]

INVALID_VALUE_RULES = {
    rule.canonical_name: rule
    for rule in (
        InvalidValueRule(
            "albumin",
            invalid_zero=True,
            description="Albumin concentrations cannot be exactly zero.",
        ),
        InvalidValueRule(
            "cardiac_index_bolus",
            hard_min=0.0,
            hard_max=15.0,
            invalid_zero=True,
            description="Cardiac index must be >0; values above 15 L/min/m2 are not interpretable.",
        ),
        InvalidValueRule(
            "cardiac_index_cont",
            hard_min=0.0,
            hard_max=15.0,
            invalid_zero=True,
            description="Cardiac index must be >0; values above 15 L/min/m2 are not interpretable.",
        ),
        InvalidValueRule(
            "cardiac_output_bolus",
            hard_min=0.0,
            hard_max=25.0,
            invalid_zero=True,
            description="Cardiac output must be >0; values above 25 L/min are not interpretable.",
        ),
        InvalidValueRule(
            "cardiac_output_cont",
            hard_min=0.0,
            hard_max=25.0,
            invalid_zero=True,
            description="Cardiac output must be >0; values above 25 L/min are not interpretable.",
        ),
        InvalidValueRule(
            "compliance",
            hard_min=0.0,
            hard_max=300.0,
            invalid_zero=True,
            description="Respiratory-system compliance must be >0; values above 300 are treated as artifacts.",
        ),
        InvalidValueRule(
            "core_temp",
            hard_min=25.0,
            hard_max=45.0,
            description="Core temperature outside 25-45 C is treated as physiologically impossible.",
        ),
        InvalidValueRule(
            "creatinine",
            invalid_zero=True,
            description="Creatinine cannot be exactly zero.",
        ),
        InvalidValueRule(
            "cvp",
            hard_min=-20.0,
            hard_max=60.0,
            description="CVP outside -20 to 60 mmHg is treated as uninterpretable.",
        ),
        InvalidValueRule(
            "dbp",
            hard_min=0.0,
            hard_max=200.0,
            invalid_zero=True,
            description="Diastolic blood pressure must be >0 and <=200 mmHg.",
        ),
        InvalidValueRule(
            "delta_p",
            hard_min=0.0,
            hard_max=60.0,
            description="Driving pressure below 0 or above 60 cmH2O is treated as invalid.",
        ),
        InvalidValueRule(
            "evlwi",
            hard_min=0.0,
            hard_max=80.0,
            invalid_zero=True,
            description="Extravascular lung water index must be >0 and <=80.",
        ),
        InvalidValueRule(
            "fio2",
            hard_min=20.0,
            hard_max=100.0,
            description="FiO2 is expected in percent and must stay within 20-100.",
        ),
        InvalidValueRule(
            "gedvi",
            hard_min=0.0,
            hard_max=2000.0,
            invalid_zero=True,
            description="GEDVI must be >0 and <=2000.",
        ),
        InvalidValueRule(
            "heart_rate",
            hard_min=0.0,
            hard_max=250.0,
            invalid_zero=True,
            description="Heart rate must be >0 and <=250 bpm.",
        ),
        InvalidValueRule(
            "hematocrit",
            hard_min=0.0,
            hard_max=80.0,
            invalid_zero=True,
            description="Hematocrit must be >0 and <=80.",
        ),
        InvalidValueRule(
            "hemoglobin",
            hard_min=0.0,
            hard_max=25.0,
            invalid_zero=True,
            description="Hemoglobin must be >0 and <=25 in the harmonized units.",
        ),
        InvalidValueRule(
            "ie_ratio",
            hard_min=0.0,
            hard_max=10.0,
            invalid_zero=True,
            description="I:E or E:I ratios must stay >0 and <=10 even before convention review.",
        ),
        InvalidValueRule(
            "inr",
            hard_min=0.0,
            hard_max=15.0,
            invalid_zero=True,
            description="INR must be >0 and <=15.",
        ),
        InvalidValueRule(
            "insp_pressure",
            hard_min=0.0,
            hard_max=80.0,
            description="Inspiratory pressure below 0 or above 80 cmH2O is treated as invalid.",
        ),
        InvalidValueRule(
            "lactate_art",
            hard_min=0.0,
            hard_max=30.0,
            invalid_zero=True,
            description="Arterial lactate must be >0 and <=30 mmol/L.",
        ),
        InvalidValueRule(
            "map",
            hard_min=0.0,
            hard_max=250.0,
            invalid_zero=True,
            description="Mean arterial pressure must be >0 and <=250 mmHg.",
        ),
        InvalidValueRule(
            "paco2",
            hard_min=0.0,
            hard_max=150.0,
            invalid_zero=True,
            description="PaCO2 must be >0 and <=150 mmHg.",
        ),
        InvalidValueRule(
            "pao2",
            hard_min=0.0,
            hard_max=760.0,
            invalid_zero=True,
            description="PaO2 must be >0 and <=760 mmHg.",
        ),
        InvalidValueRule(
            "pap_dias",
            hard_min=0.0,
            hard_max=100.0,
            invalid_zero=True,
            description="Pulmonary artery diastolic pressure must be >0 and <=100 mmHg.",
        ),
        InvalidValueRule(
            "pap_mean",
            hard_min=0.0,
            hard_max=100.0,
            invalid_zero=True,
            description="Pulmonary artery mean pressure must be >0 and <=100 mmHg.",
        ),
        InvalidValueRule(
            "pap_sys",
            hard_min=0.0,
            hard_max=150.0,
            invalid_zero=True,
            description="Pulmonary artery systolic pressure must be >0 and <=150 mmHg.",
        ),
        InvalidValueRule(
            "pcwp",
            hard_min=0.0,
            hard_max=50.0,
            invalid_zero=True,
            description="Pulmonary capillary wedge pressure must be >0 and <=50 mmHg.",
        ),
        InvalidValueRule(
            "peep",
            hard_min=0.0,
            hard_max=30.0,
            description="PEEP below 0 or above 30 cmH2O is treated as invalid.",
        ),
        InvalidValueRule(
            "pf_ratio",
            hard_min=0.0,
            hard_max=2500.0,
            invalid_zero=True,
            description="PaO2/FiO2 ratio must be >0 and <=2500.",
        ),
        InvalidValueRule(
            "ph_art",
            hard_min=6.8,
            hard_max=7.8,
            description="Arterial pH outside 6.8-7.8 is treated as invalid.",
        ),
        InvalidValueRule(
            "platelets",
            hard_min=0.0,
            hard_max=2000.0,
            invalid_zero=True,
            description="Platelet count must be >0 and <=2000.",
        ),
        InvalidValueRule(
            "ptt",
            hard_min=0.0,
            hard_max=300.0,
            invalid_zero=True,
            description="PTT must be >0 and <=300 seconds.",
        ),
        InvalidValueRule(
            "resp_rate",
            hard_min=0.0,
            hard_max=80.0,
            invalid_zero=True,
            description="Total respiratory rate must be >0 and <=80 breaths/min.",
        ),
        InvalidValueRule(
            "sao2",
            hard_min=50.0,
            hard_max=100.0,
            description="Arterial oxygen saturation must stay within 50-100%.",
        ),
        InvalidValueRule(
            "sbp",
            hard_min=0.0,
            hard_max=300.0,
            invalid_zero=True,
            description="Systolic blood pressure must be >0 and <=300 mmHg.",
        ),
        InvalidValueRule(
            "scvo2",
            hard_min=20.0,
            hard_max=100.0,
            description="ScvO2 must stay within 20-100%.",
        ),
        InvalidValueRule(
            "sofa",
            hard_min=0.0,
            hard_max=24.0,
            description="SOFA score must stay within 0-24.",
        ),
        InvalidValueRule(
            "spo2",
            hard_min=30.0,
            hard_max=100.0,
            description="Peripheral oxygen saturation must stay within 30-100%.",
        ),
        InvalidValueRule(
            "stroke_index_cont",
            hard_min=0.0,
            hard_max=100.0,
            invalid_zero=True,
            description="Stroke index must be >0 and <=100.",
        ),
        InvalidValueRule(
            "stroke_volume_cont",
            hard_min=0.0,
            hard_max=300.0,
            invalid_zero=True,
            description="Stroke volume must be >0 and <=300 mL.",
        ),
        InvalidValueRule(
            "svri",
            hard_min=0.0,
            hard_max=10000.0,
            invalid_zero=True,
            description="SVRI must be >0 and <=10000.",
        ),
        InvalidValueRule(
            "vt",
            hard_min=0.0,
            hard_max=2000.0,
            invalid_zero=True,
            description="Tidal volume must be >0 and <=2000 mL.",
        ),
        InvalidValueRule(
            "vt_per_kg_ibw",
            hard_min=0.0,
            hard_max=30.0,
            invalid_zero=True,
            description="Tidal volume per kg IBW must be >0 and <=30 mL/kg.",
        ),
    )
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


def build_invalid_value_rule_table() -> pd.DataFrame:
    rows = [
        {
            "canonical_name": rule.canonical_name,
            "hard_min": rule.hard_min,
            "hard_max": rule.hard_max,
            "invalid_zero": rule.invalid_zero,
            "description": rule.description,
        }
        for rule in INVALID_VALUE_RULES.values()
    ]
    if not rows:
        return pd.DataFrame(columns=INVALID_VALUE_RULE_COLUMNS)
    return pd.DataFrame(rows).sort_values("canonical_name").reset_index(drop=True)


def _empty_invalid_value_qc() -> pd.DataFrame:
    return pd.DataFrame(columns=INVALID_VALUE_QC_COLUMNS)


def _invalid_value_mask(series: pd.Series, rule: InvalidValueRule) -> pd.Series:
    mask = pd.Series(False, index=series.index, dtype="boolean")
    if rule.hard_min is not None:
        mask = mask | (series < rule.hard_min)
    if rule.hard_max is not None:
        mask = mask | (series > rule.hard_max)
    if rule.invalid_zero:
        mask = mask | (series == 0)
    return series.notna() & mask.fillna(False)


def _example_flagged_values(series: pd.Series, mask: pd.Series, max_examples: int = 10) -> list[float]:
    if not mask.any():
        return []
    return (
        series[mask]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()[:max_examples]
    )


def apply_dynamic_invalid_value_cleaning(
    tables_by_hospital: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    cleaned_tables = {
        hospital: df.copy()
        for hospital, df in tables_by_hospital.items()
    }
    rule_table = build_invalid_value_rule_table()
    qc_rows = []

    for hospital in sorted(cleaned_tables):
        df = cleaned_tables[hospital]
        for canonical_name, rule in INVALID_VALUE_RULES.items():
            before_series = _numeric_column(df, canonical_name)
            non_null_before = int(before_series.notna().sum())
            invalid_mask = _invalid_value_mask(before_series, rule)
            invalid_count = int(invalid_mask.sum())
            after_series = before_series.mask(invalid_mask, pd.NA)
            non_null_after = int(after_series.notna().sum())

            if canonical_name in df.columns and invalid_count > 0:
                df[canonical_name] = after_series

            qc_rows.append(
                {
                    "hospital": hospital,
                    "canonical_name": canonical_name,
                    "non_null_before": non_null_before,
                    "non_null_after": non_null_after,
                    "invalid_count": invalid_count,
                    "invalid_proportion": (
                        invalid_count / non_null_before if non_null_before else 0.0
                    ),
                    "invalid_examples": _example_flagged_values(before_series, invalid_mask),
                }
            )

    if not qc_rows:
        return cleaned_tables, rule_table, _empty_invalid_value_qc()

    qc_df = pd.DataFrame(qc_rows).sort_values(
        ["canonical_name", "hospital"]
    ).reset_index(drop=True)
    variable_totals = qc_df.groupby("canonical_name")["invalid_count"].transform("sum")
    qc_df["variable_total_invalid_count"] = variable_totals
    qc_df["hospital_share_of_variable_invalids"] = 0.0

    non_zero_total_mask = variable_totals > 0
    qc_df.loc[non_zero_total_mask, "hospital_share_of_variable_invalids"] = (
        qc_df.loc[non_zero_total_mask, "invalid_count"]
        / variable_totals[non_zero_total_mask]
    )

    dominant_hospitals = {}
    dominant_shares = {}
    concentrated_flags = {}
    for canonical_name, group in qc_df.groupby("canonical_name"):
        total_invalid = int(group["invalid_count"].sum())
        if total_invalid == 0:
            dominant_hospitals[canonical_name] = pd.NA
            dominant_shares[canonical_name] = 0.0
            concentrated_flags[canonical_name] = False
            continue

        dominant_row = group.sort_values(
            ["hospital_share_of_variable_invalids", "hospital"],
            ascending=[False, True],
        ).iloc[0]
        dominant_hospitals[canonical_name] = dominant_row["hospital"]
        dominant_shares[canonical_name] = float(
            dominant_row["hospital_share_of_variable_invalids"]
        )
        concentrated_flags[canonical_name] = (
            total_invalid >= 5 and dominant_shares[canonical_name] >= 0.5
        )

    qc_df["dominant_invalid_hospital"] = qc_df["canonical_name"].map(dominant_hospitals)
    qc_df["dominant_invalid_hospital_share"] = qc_df["canonical_name"].map(dominant_shares)
    qc_df["invalidity_concentrated_in_specific_hospitals"] = qc_df[
        "canonical_name"
    ].map(concentrated_flags)
    qc_df = qc_df[INVALID_VALUE_QC_COLUMNS]
    return cleaned_tables, rule_table, qc_df


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
    tables_by_hospital, invalid_value_rules, invalid_value_qc = (
        apply_dynamic_invalid_value_cleaning(tables_by_hospital)
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
        invalid_value_rules=invalid_value_rules,
        invalid_value_qc=invalid_value_qc,
        distribution_summary=distribution_summary,
        distribution_issues=distribution_issues,
    )
