from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


COHORT_TABLE_COLUMNS = [
    "stay_id_global",
    "hospital_id",
    "readmission",
    "icu_mortality",
    "icd10_codes",
    "icu_admission_time",
    "icu_end_time_proxy",
]

CHAPTER1_CORE_VITALS = (
    "heart_rate",
    "map",
    "sbp",
    "dbp",
    "resp_rate",
    "spo2",
    "sao2",
    "core_temp",
)
CHAPTER1_CORE_PHYSIOLOGIC_GROUPS = {
    "cardiac_rate": ("heart_rate",),
    "blood_pressure": ("map", "sbp", "dbp"),
    "respiratory": ("resp_rate",),
    "oxygenation": ("spo2", "sao2"),
}
CHAPTER1_OPTIONAL_PHYSIOLOGIC_GROUPS = {
    "core_temp_optional": ("core_temp",),
}
CHAPTER1_MIN_USABLE_CORE_GROUPS = 3


@dataclass(frozen=True)
class ASICChapter1CohortResult:
    table: pd.DataFrame
    notes: pd.DataFrame
    core_vital_group_coverage: pd.DataFrame
    site_eligibility: pd.DataFrame
    site_counts_summary: pd.DataFrame
    stay_exclusions: pd.DataFrame
    stay_exclusion_summary_by_hospital: pd.DataFrame
    counts_by_hospital: pd.DataFrame
    retained_hospitals: pd.DataFrame
    retained_stays: pd.DataFrame


@dataclass(frozen=True)
class ASICStayLevelCohortResult:
    table: pd.DataFrame
    summary: pd.DataFrame
    preprocessing_notes: pd.DataFrame
    icu_end_time_proxy_summary_by_hospital: pd.DataFrame
    coding_distribution_by_hospital: pd.DataFrame
    chapter1: ASICChapter1CohortResult


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(f"{table_name} is missing required cohort columns: {missing}")


def _dynamic_end_time_proxy(dynamic_df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(dynamic_df, {"stay_id_global", "time"}, "dynamic_df")

    dynamic_times = dynamic_df[["stay_id_global", "time"]].copy()
    dynamic_times = dynamic_times[dynamic_times["stay_id_global"].notna()].reset_index(drop=True)
    dynamic_times["icu_end_time_proxy_timedelta"] = pd.to_timedelta(
        dynamic_times["time"],
        errors="coerce",
    )
    dynamic_times = dynamic_times.dropna(subset=["icu_end_time_proxy_timedelta"])

    if dynamic_times.empty:
        return pd.DataFrame(
            columns=[
                "stay_id_global",
                "icu_end_time_proxy",
                "icu_end_time_proxy_hours",
            ]
        )

    end_time_proxy = (
        dynamic_times.groupby("stay_id_global", dropna=False)["icu_end_time_proxy_timedelta"]
        .max()
        .reset_index()
    )
    end_time_proxy["icu_end_time_proxy"] = end_time_proxy[
        "icu_end_time_proxy_timedelta"
    ].astype("string")
    end_time_proxy["icu_end_time_proxy_hours"] = (
        end_time_proxy["icu_end_time_proxy_timedelta"].dt.total_seconds() / 3600.0
    )
    return end_time_proxy[
        ["stay_id_global", "icu_end_time_proxy", "icu_end_time_proxy_hours"]
    ]


def _build_preprocessing_notes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "note_id": "icu_admission_time_anchor",
                "category": "timeline_definition",
                "field": "icu_admission_time",
                "note": (
                    "Set to 0 for every stay because the harmonized ASIC dynamic timeline "
                    "is already anchored to administrative ICU admission."
                ),
            },
            {
                "note_id": "icu_end_time_proxy_definition",
                "category": "proxy_definition",
                "field": "icu_end_time_proxy",
                "note": (
                    "ASIC ICU stay end is proxy-based: it is the maximum observed "
                    "harmonized dynamic time per stay and reflects recording extent, "
                    "not a true administrative ICU discharge timestamp."
                ),
            },
            {
                "note_id": "adult_eligibility_inherited",
                "category": "source_cohort_assumption",
                "field": "adult_eligibility",
                "note": (
                    "Adult eligibility was inherited from the provided ASIC source "
                    "cohort and was not rederived in preprocessing."
                ),
            },
            {
                "note_id": "ventilation_24h_inherited",
                "category": "source_cohort_assumption",
                "field": "ventilation_ge_24h",
                "note": (
                    "Ventilation >=24h eligibility was inherited from the provided ASIC "
                    "source cohort and was not rederived in preprocessing."
                ),
            },
            {
                "note_id": "ama_hospice_not_derived",
                "category": "not_derived",
                "field": "ama_hospice_flags",
                "note": "AMA and hospice flags are not derived for ASIC.",
            },
        ]
    )


def _build_summary(cohort_df: pd.DataFrame) -> pd.DataFrame:
    missing_icu_end_time_proxy = int(cohort_df["icu_end_time_proxy"].isna().sum())
    return pd.DataFrame(
        [
            {
                "metric": "cohort_rows_total",
                "value": int(cohort_df.shape[0]),
                "note": "Authoritative ASIC stay-level cohort rows.",
            },
            {
                "metric": "cohort_unique_stay_id_global_total",
                "value": int(cohort_df["stay_id_global"].nunique(dropna=True)),
                "note": "Unique stay_id_global values in the authoritative cohort table.",
            },
            {
                "metric": "stays_with_non_missing_icu_end_time_proxy",
                "value": int(cohort_df["icu_end_time_proxy"].notna().sum()),
                "note": "Stays with at least one observed harmonized dynamic time value.",
            },
            {
                "metric": "stays_missing_icu_end_time_proxy",
                "value": missing_icu_end_time_proxy,
                "note": (
                    "Stays with no usable dynamic time extent and therefore missing "
                    "icu_end_time_proxy."
                ),
            },
            {
                "metric": "icu_end_time_proxy_is_recording_extent_proxy",
                "value": 1,
                "note": (
                    "ASIC ICU stay end is proxy-based rather than a true administrative "
                    "discharge timestamp."
                ),
            },
        ]
    )


def _summarize_icu_end_time_proxy_by_hospital(cohort_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in cohort_df.groupby("hospital_id", dropna=False):
        proxy_hours = (
            pd.to_timedelta(hospital_df["icu_end_time_proxy"], errors="coerce").dt.total_seconds()
            / 3600.0
        )
        non_missing_proxy_hours = proxy_hours.dropna()
        rows.append(
            {
                "hospital_id": hospital,
                "stay_count": int(hospital_df.shape[0]),
                "stays_with_non_missing_icu_end_time_proxy": int(
                    non_missing_proxy_hours.shape[0]
                ),
                "stays_missing_icu_end_time_proxy": int(proxy_hours.isna().sum()),
                "min_icu_end_time_proxy_hours": (
                    float(non_missing_proxy_hours.min())
                    if not non_missing_proxy_hours.empty
                    else pd.NA
                ),
                "mean_icu_end_time_proxy_hours": (
                    float(non_missing_proxy_hours.mean())
                    if not non_missing_proxy_hours.empty
                    else pd.NA
                ),
                "median_icu_end_time_proxy_hours": (
                    float(non_missing_proxy_hours.median())
                    if not non_missing_proxy_hours.empty
                    else pd.NA
                ),
                "max_icu_end_time_proxy_hours": (
                    float(non_missing_proxy_hours.max())
                    if not non_missing_proxy_hours.empty
                    else pd.NA
                ),
            }
        )

    return pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)


def _coding_distribution(
    cohort_df: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in cohort_df.groupby("hospital_id", dropna=False):
        hospital_total = int(hospital_df.shape[0])
        counts = hospital_df[variable].value_counts(dropna=False)
        for code, count in counts.items():
            rows.append(
                {
                    "hospital_id": hospital,
                    "variable": variable,
                    "code": pd.NA if pd.isna(code) else str(code),
                    "count": int(count),
                    "hospital_stay_count": hospital_total,
                    "proportion_of_hospital_stays": float(count / hospital_total),
                }
            )

    return pd.DataFrame(rows)


def _summarize_coding_distribution_by_hospital(cohort_df: pd.DataFrame) -> pd.DataFrame:
    distributions = [
        _coding_distribution(cohort_df, "readmission"),
        _coding_distribution(cohort_df, "icu_mortality"),
    ]
    return (
        pd.concat(distributions, ignore_index=True)
        .sort_values(["hospital_id", "variable", "code"], na_position="last")
        .reset_index(drop=True)
    )


def _chapter1_notes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "note_id": "chapter1_site_restricted_cohort",
                "category": "cohort_definition",
                "note": (
                    "This creates a site-restricted ASIC Chapter 1 cohort rather than using "
                    "every ASIC hospital."
                ),
            },
            {
                "note_id": "chapter1_core_vitals_definition",
                "category": "site_eligibility_rule",
                "note": (
                    "Core physiologic groups are cardiac_rate=heart_rate, "
                    "blood_pressure=map or sbp or dbp, respiratory=resp_rate, and "
                    "oxygenation=spo2 or sao2."
                ),
            },
            {
                "note_id": "chapter1_core_vitals_threshold",
                "category": "site_eligibility_rule",
                "note": (
                    "A hospital is core-vitals eligible if at least 3 of the 4 required "
                    "core physiologic groups have any usable non-missing dynamic data."
                ),
            },
            {
                "note_id": "chapter1_core_temp_optional",
                "category": "site_eligibility_rule",
                "note": "core_temp is optional and is not required for hospital inclusion.",
            },
            {
                "note_id": "chapter1_outcome_rule",
                "category": "site_eligibility_rule",
                "note": (
                    "A hospital is outcome-eligible if harmonized icu_mortality is present "
                    "and not entirely missing in the harmonized static data."
                ),
            },
            {
                "note_id": "chapter1_outcome_verification_rule",
                "category": "site_eligibility_rule",
                "note": (
                    "Before excluding a hospital for missing icu_mortality, the pipeline "
                    "verifies that the all-missing harmonized field reflects true source "
                    "absence rather than an upstream harmonization or sample-extraction issue."
                ),
            },
        ]
    )


def _dynamic_stay_presence(dynamic_df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(dynamic_df, {"stay_id_global", "hospital_id"}, "dynamic_df")

    present = (
        dynamic_df.dropna(subset=["stay_id_global"])[["hospital_id", "stay_id_global"]]
        .copy()
        .assign(dynamic_row_count=1)
        .groupby(["hospital_id", "stay_id_global"], dropna=False)["dynamic_row_count"]
        .sum()
        .reset_index()
    )
    present["has_dynamic_data"] = True
    return present


def _chapter1_group_definitions() -> dict[str, tuple[str, ...]]:
    return {
        **CHAPTER1_CORE_PHYSIOLOGIC_GROUPS,
        **CHAPTER1_OPTIONAL_PHYSIOLOGIC_GROUPS,
    }


def _build_chapter1_core_vital_group_coverage(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(dynamic_df, {"hospital_id"}, "dynamic_df")

    rows = []
    for hospital in sorted(cohort_df["hospital_id"].dropna().astype("string").unique().tolist()):
        hospital_dynamic = dynamic_df[dynamic_df["hospital_id"] == hospital]
        for group_name, candidate_variables in _chapter1_group_definitions().items():
            satisfying_variables: list[str] = []
            non_null_counts_by_variable: dict[str, int] = {}
            for variable in candidate_variables:
                if variable in hospital_dynamic.columns:
                    non_null_count = int(
                        pd.to_numeric(hospital_dynamic[variable], errors="coerce").notna().sum()
                    )
                else:
                    non_null_count = 0
                non_null_counts_by_variable[variable] = non_null_count
                if non_null_count > 0:
                    satisfying_variables.append(variable)

            rows.append(
                {
                    "hospital_id": hospital,
                    "physiologic_group": group_name,
                    "required_for_inclusion": group_name in CHAPTER1_CORE_PHYSIOLOGIC_GROUPS,
                    "candidate_variables": list(candidate_variables),
                    "satisfying_variables": satisfying_variables,
                    "group_usable": bool(satisfying_variables),
                    "non_null_counts_by_variable": non_null_counts_by_variable,
                }
            )

    return pd.DataFrame(rows).sort_values(
        ["hospital_id", "required_for_inclusion", "physiologic_group"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def _normalize_binary_codes(series: pd.Series) -> tuple[pd.Series, list[str]]:
    numeric = pd.to_numeric(series, errors="coerce")
    unique_codes = (
        pd.Series(numeric.dropna().unique())
        .sort_values()
        .map(lambda value: str(int(value)) if float(value).is_integer() else str(float(value)))
        .tolist()
    )
    return numeric, unique_codes


def _normalize_raw_missing(series: pd.Series) -> pd.Series:
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        return series.replace(r"^\s*$", pd.NA, regex=True)
    return series


def _icu_mortality_source_columns_for_hospital(
    hospital: str,
    static_source_map: pd.DataFrame | None,
) -> list[str]:
    if static_source_map is None or static_source_map.empty:
        return []

    rows = static_source_map[
        (static_source_map["hospital"] == hospital)
        & (static_source_map["canonical_name"] == "icu_mortality")
    ]
    if rows.empty:
        return []

    raw_source_columns = rows.iloc[0]["raw_source_columns_used"]
    if isinstance(raw_source_columns, list):
        return raw_source_columns
    return []


def _verify_icu_mortality_availability(
    hospital: str,
    hospital_df: pd.DataFrame,
    static_source_map: pd.DataFrame | None = None,
    raw_static_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[bool, int, list[str], str]:
    outcome_numeric, outcome_codes = _normalize_binary_codes(hospital_df["icu_mortality"])
    outcome_non_null_count = int(outcome_numeric.notna().sum())
    raw_source_columns = _icu_mortality_source_columns_for_hospital(hospital, static_source_map)

    if outcome_non_null_count > 0:
        return (
            True,
            outcome_non_null_count,
            outcome_codes,
            "verified_non_missing_harmonized_icu_mortality",
        )

    if raw_static_tables is None or hospital not in raw_static_tables:
        return (
            False,
            outcome_non_null_count,
            outcome_codes,
            "raw_source_verification_not_available",
        )

    if not raw_source_columns:
        return (
            False,
            outcome_non_null_count,
            outcome_codes,
            "verified_no_raw_icu_mortality_source_column",
        )

    raw_df = raw_static_tables[hospital]
    missing_columns = [column for column in raw_source_columns if column not in raw_df.columns]
    if missing_columns:
        raise ValueError(
            f"{hospital}: icu_mortality source-map columns are missing from the raw static "
            f"table: {missing_columns}"
        )

    raw_non_null_count = 0
    raw_examples: list[str] = []
    for column in raw_source_columns:
        raw_series = _normalize_raw_missing(raw_df[column])
        raw_non_null = raw_series.dropna()
        raw_non_null_count += int(raw_non_null.shape[0])
        raw_examples.extend(raw_non_null.astype("string").drop_duplicates().tolist()[:5])

    if raw_non_null_count == 0:
        return (
            False,
            outcome_non_null_count,
            outcome_codes,
            "verified_raw_icu_mortality_source_present_but_all_missing",
        )

    raise ValueError(
        (
            f"{hospital}: harmonized icu_mortality is entirely missing, but raw source "
            f"column(s) {raw_source_columns} contain non-missing values. This suggests an "
            f"upstream harmonization or sample-extraction issue. Example raw values: "
            f"{raw_examples[:10]}"
        )
    )


def _build_chapter1_site_eligibility(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    static_source_map: pd.DataFrame | None = None,
    raw_static_tables: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    stay_presence = _dynamic_stay_presence(dynamic_df)
    core_vital_group_coverage = _build_chapter1_core_vital_group_coverage(cohort_df, dynamic_df)

    rows = []

    for hospital, hospital_df in cohort_df.groupby("hospital_id", dropna=False):
        hospital_stays = hospital_df[["stay_id_global"]].copy()
        hospital_stays["hospital_id"] = hospital

        (
            icu_mortality_available,
            outcome_non_null_count,
            outcome_codes,
            icu_mortality_verification_status,
        ) = _verify_icu_mortality_availability(
            hospital,
            hospital_df,
            static_source_map=static_source_map,
            raw_static_tables=raw_static_tables,
        )
        outcome_total_stays = int(hospital_df.shape[0])
        raw_source_columns = _icu_mortality_source_columns_for_hospital(hospital, static_source_map)

        hospital_dynamic_stays = hospital_stays.merge(
            stay_presence[stay_presence["hospital_id"] == hospital][
                ["stay_id_global", "dynamic_row_count", "has_dynamic_data"]
            ],
            on="stay_id_global",
            how="left",
        )
        hospital_dynamic_stays["dynamic_row_count"] = (
            hospital_dynamic_stays["dynamic_row_count"].fillna(0).astype("Int64")
        )
        hospital_dynamic_stays["has_dynamic_data"] = hospital_dynamic_stays[
            "has_dynamic_data"
        ].fillna(False)

        dynamic_stay_ids = hospital_dynamic_stays.loc[
            hospital_dynamic_stays["has_dynamic_data"],
            "stay_id_global",
        ]
        hospital_group_rows = core_vital_group_coverage[
            core_vital_group_coverage["hospital_id"] == hospital
        ]
        hospital_group_lookup = hospital_group_rows.set_index("physiologic_group")
        required_group_rows = hospital_group_rows[hospital_group_rows["required_for_inclusion"]]
        usable_core_vital_group_count = int(required_group_rows["group_usable"].sum())
        core_vital_coverage_sufficient = (
            usable_core_vital_group_count >= CHAPTER1_MIN_USABLE_CORE_GROUPS
        )
        missing_core_vital_groups = (
            required_group_rows.loc[~required_group_rows["group_usable"], "physiologic_group"]
            .astype("string")
            .tolist()
        )

        exclusion_reasons: list[str] = []
        if not icu_mortality_available:
            exclusion_reasons.append("missing/unusable icu_mortality")
        if not core_vital_coverage_sufficient:
            exclusion_reasons.append("insufficient core-vitals coverage")

        exclusion_reason = pd.NA
        if exclusion_reasons:
            exclusion_reason = "; ".join(exclusion_reasons)

        rows.append(
            {
                "hospital_id": hospital,
                "site_stay_count": outcome_total_stays,
                "stays_with_any_dynamic_data": int(dynamic_stay_ids.nunique(dropna=True)),
                "icu_mortality_non_null_count": outcome_non_null_count,
                "icu_mortality_codes": outcome_codes,
                "icu_mortality_source_columns": raw_source_columns,
                "icu_mortality_verification_status": icu_mortality_verification_status,
                "icu_mortality_available": icu_mortality_available,
                "cardiac_rate_satisfied_by": hospital_group_lookup.at[
                    "cardiac_rate",
                    "satisfying_variables",
                ],
                "blood_pressure_satisfied_by": hospital_group_lookup.at[
                    "blood_pressure",
                    "satisfying_variables",
                ],
                "respiratory_satisfied_by": hospital_group_lookup.at[
                    "respiratory",
                    "satisfying_variables",
                ],
                "oxygenation_satisfied_by": hospital_group_lookup.at[
                    "oxygenation",
                    "satisfying_variables",
                ],
                "core_temp_optional_satisfied_by": hospital_group_lookup.at[
                    "core_temp_optional",
                    "satisfying_variables",
                ],
                "usable_core_vital_group_count": usable_core_vital_group_count,
                "missing_core_vital_groups": missing_core_vital_groups,
                "core_vitals_eligible": core_vital_coverage_sufficient,
                "site_included_ch1": icu_mortality_available and core_vital_coverage_sufficient,
                "exclusion_reason": exclusion_reason,
                "exclusion_reasons": exclusion_reasons,
            }
        )

    site_eligibility = pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)
    site_eligibility["icu_mortality_available_and_usable"] = site_eligibility[
        "icu_mortality_available"
    ]
    site_eligibility["core_vital_coverage_sufficient"] = site_eligibility[
        "core_vitals_eligible"
    ]
    site_eligibility["include_in_chapter1"] = site_eligibility["site_included_ch1"]
    ordered_prefix = [
        "hospital_id",
        "icu_mortality_available",
        "core_vitals_eligible",
        "site_included_ch1",
        "exclusion_reason",
    ]
    ordered_columns = ordered_prefix + [
        column for column in site_eligibility.columns if column not in ordered_prefix
    ]
    site_eligibility = site_eligibility[ordered_columns]
    return core_vital_group_coverage, site_eligibility


def _summarize_chapter1_site_counts(site_eligibility: pd.DataFrame) -> pd.DataFrame:
    included_count = int(site_eligibility["site_included_ch1"].sum())
    total_count = int(site_eligibility["hospital_id"].nunique(dropna=True))
    excluded_count = total_count - included_count
    return pd.DataFrame(
        [
            {
                "metric": "hospitals_before_site_level_exclusion",
                "value": total_count,
            },
            {
                "metric": "hospitals_after_site_level_exclusion",
                "value": included_count,
            },
            {
                "metric": "hospitals_excluded_at_site_level",
                "value": excluded_count,
            },
        ]
    )


def _build_chapter1_stay_exclusions(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    site_eligibility: pd.DataFrame,
) -> pd.DataFrame:
    stay_presence = _dynamic_stay_presence(dynamic_df)[
        ["stay_id_global", "dynamic_row_count", "has_dynamic_data"]
    ]
    site_lookup = site_eligibility[
        ["hospital_id", "include_in_chapter1", "exclusion_reasons"]
    ].rename(
        columns={
            "include_in_chapter1": "site_include_in_chapter1",
            "exclusion_reasons": "site_exclusion_reasons",
        }
    )

    stay_exclusions = (
        cohort_df.merge(site_lookup, on="hospital_id", how="left")
        .merge(stay_presence, on="stay_id_global", how="left")
        .copy()
    )
    stay_exclusions["dynamic_row_count"] = (
        stay_exclusions["dynamic_row_count"].fillna(0).astype("Int64")
    )
    stay_exclusions["has_dynamic_data"] = stay_exclusions["has_dynamic_data"].fillna(False)
    stay_exclusions["site_included_ch1"] = stay_exclusions["site_include_in_chapter1"].fillna(False)
    stay_exclusions = stay_exclusions.drop(columns=["site_include_in_chapter1"])
    stay_exclusions["site_exclusion_reasons"] = stay_exclusions["site_exclusion_reasons"].map(
        lambda value: value if isinstance(value, list) else []
    )
    stay_exclusions["site_exclusion_reason"] = stay_exclusions["site_exclusion_reasons"].map(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )

    readmission_numeric = pd.to_numeric(stay_exclusions["readmission"], errors="coerce")
    stay_exclusions["exclude_site_ineligible"] = ~stay_exclusions["site_included_ch1"]
    stay_exclusions["exclude_no_dynamic_data"] = (
        stay_exclusions["site_included_ch1"] & ~stay_exclusions["has_dynamic_data"]
    )
    stay_exclusions["exclude_missing_readmission"] = (
        stay_exclusions["site_included_ch1"] & stay_exclusions["readmission"].isna()
    )
    stay_exclusions["exclude_readmission_flagged"] = (
        stay_exclusions["site_included_ch1"] & readmission_numeric.eq(1).fillna(False)
    )
    stay_exclusions["exclude_readmission_equals_1"] = stay_exclusions[
        "exclude_readmission_flagged"
    ]
    stay_exclusions["first_stay_proxy_eligible"] = (
        stay_exclusions["site_included_ch1"] & readmission_numeric.eq(0).fillna(False)
    )
    stay_exclusions["retain_after_site_level_exclusion"] = ~stay_exclusions[
        "exclude_site_ineligible"
    ]
    stay_exclusions["retain_after_no_dynamic_data_exclusion"] = (
        stay_exclusions["retain_after_site_level_exclusion"]
        & ~stay_exclusions["exclude_no_dynamic_data"]
    )
    stay_exclusions["retain_after_missing_readmission_exclusion"] = (
        stay_exclusions["retain_after_no_dynamic_data_exclusion"]
        & ~stay_exclusions["exclude_missing_readmission"]
    )
    stay_exclusions["retain_in_chapter1"] = (
        stay_exclusions["retain_after_missing_readmission_exclusion"]
        & ~stay_exclusions["exclude_readmission_flagged"]
    )
    stay_exclusions["final_retained_ch1"] = stay_exclusions["retain_in_chapter1"]
    stay_exclusions["final_excluded_ch1"] = ~stay_exclusions["final_retained_ch1"]

    stay_reasons: list[list[str]] = []
    final_status: list[str] = []
    for row in stay_exclusions.itertuples(index=False):
        reasons: list[str] = []
        if row.exclude_site_ineligible:
            final_status.append("excluded_site")
            stay_reasons.append(reasons)
            continue
        if row.exclude_no_dynamic_data:
            reasons.append("no dynamic data")
        if row.exclude_missing_readmission:
            reasons.append("missing readmission")
        if row.exclude_readmission_flagged:
            reasons.append("readmission flagged")
        if reasons:
            final_status.append("excluded_stay")
        else:
            final_status.append("retained")
        stay_reasons.append(reasons)

    stay_exclusions["stay_exclusion_reasons"] = stay_reasons
    stay_exclusions["exclusion_reason"] = stay_exclusions["stay_exclusion_reasons"].map(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )
    stay_exclusions["final_ch1_status"] = pd.Series(final_status, index=stay_exclusions.index)
    ordered_prefix = [
        "stay_id_global",
        "hospital_id",
        "site_included_ch1",
        "has_dynamic_data",
        "readmission",
        "first_stay_proxy_eligible",
        "final_retained_ch1",
        "final_ch1_status",
        "exclusion_reason",
    ]
    ordered_columns = ordered_prefix + [
        column for column in stay_exclusions.columns if column not in ordered_prefix
    ]
    stay_exclusions = stay_exclusions[ordered_columns]
    return stay_exclusions


def _summarize_chapter1_counts_by_hospital(stay_exclusions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in stay_exclusions.groupby("hospital_id", dropna=False):
        rows.append(
            {
                "hospital_id": hospital,
                "authoritative_stays_before_site_exclusion": int(hospital_df.shape[0]),
                "after_site_level_exclusion": int(
                    hospital_df["retain_after_site_level_exclusion"].sum()
                ),
                "after_no_dynamic_data_exclusion": int(
                    hospital_df["retain_after_no_dynamic_data_exclusion"].sum()
                ),
                "excluded_no_dynamic_data_stays": int(hospital_df["exclude_no_dynamic_data"].sum()),
                "after_missing_readmission_exclusion": int(
                    hospital_df["retain_after_missing_readmission_exclusion"].sum()
                ),
                "excluded_missing_readmission_stays": int(
                    hospital_df["exclude_missing_readmission"].sum()
                ),
                "after_readmission_flagged_exclusion": int(
                    hospital_df["final_retained_ch1"].sum()
                ),
                "excluded_readmission_flagged_stays": int(
                    hospital_df["exclude_readmission_flagged"].sum()
                ),
                "after_readmission_equals_1_exclusion": int(
                    hospital_df["final_retained_ch1"].sum()
                ),
                "final_retained_stays": int(hospital_df["final_retained_ch1"].sum()),
            }
        )

    return pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)


def _summarize_chapter1_stay_exclusions_by_hospital(stay_exclusions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in stay_exclusions.groupby("hospital_id", dropna=False):
        rows.append(
            {
                "hospital_id": hospital,
                "before_site_level_exclusion": int(hospital_df.shape[0]),
                "after_site_level_exclusion": int(
                    hospital_df["retain_after_site_level_exclusion"].sum()
                ),
                "after_no_dynamic_data_exclusion": int(
                    hospital_df["retain_after_no_dynamic_data_exclusion"].sum()
                ),
                "after_missing_readmission_exclusion": int(
                    hospital_df["retain_after_missing_readmission_exclusion"].sum()
                ),
                "after_readmission_flagged_exclusion": int(
                    hospital_df["final_retained_ch1"].sum()
                ),
                "final_retained_stays": int(hospital_df["final_retained_ch1"].sum()),
                "excluded_no_dynamic_data_stays": int(hospital_df["exclude_no_dynamic_data"].sum()),
                "excluded_missing_readmission_stays": int(
                    hospital_df["exclude_missing_readmission"].sum()
                ),
                "excluded_readmission_flagged_stays": int(
                    hospital_df["exclude_readmission_flagged"].sum()
                ),
            }
        )

    return pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)


def build_asic_chapter1_cohort(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    static_source_map: pd.DataFrame | None = None,
    raw_static_tables: dict[str, pd.DataFrame] | None = None,
) -> ASICChapter1CohortResult:
    core_vital_group_coverage, site_eligibility = _build_chapter1_site_eligibility(
        cohort_df,
        dynamic_df,
        static_source_map=static_source_map,
        raw_static_tables=raw_static_tables,
    )
    site_counts_summary = _summarize_chapter1_site_counts(site_eligibility)
    stay_exclusions = _build_chapter1_stay_exclusions(cohort_df, dynamic_df, site_eligibility)
    counts_by_hospital = _summarize_chapter1_counts_by_hospital(stay_exclusions)
    stay_exclusion_summary_by_hospital = _summarize_chapter1_stay_exclusions_by_hospital(
        stay_exclusions
    )
    retained_hospitals = site_eligibility.loc[
        site_eligibility["site_included_ch1"],
        ["hospital_id"],
    ].reset_index(drop=True)
    retained_stays = stay_exclusions.loc[
        stay_exclusions["retain_in_chapter1"],
        ["stay_id_global", "hospital_id"],
    ].reset_index(drop=True)
    chapter1_table = stay_exclusions.loc[
        stay_exclusions["final_retained_ch1"],
        [
            "stay_id_global",
            "hospital_id",
            "readmission",
            "icu_mortality",
            "icd10_codes",
            "icu_admission_time",
            "icu_end_time_proxy",
            "has_dynamic_data",
            "first_stay_proxy_eligible",
            "final_retained_ch1",
            "final_ch1_status",
            "exclusion_reason",
        ],
    ].reset_index(drop=True)
    if not chapter1_table["hospital_id"].isin(retained_hospitals["hospital_id"]).all():
        raise ValueError(
            "A stay from a site-excluded hospital remained in the final retained Chapter 1 "
            "ASIC stay cohort."
        )
    if not chapter1_table["final_retained_ch1"].all():
        raise ValueError(
            "Non-retained stays were included in the final Chapter 1 ASIC stay-level cohort."
        )

    return ASICChapter1CohortResult(
        table=chapter1_table.reset_index(drop=True),
        notes=_chapter1_notes(),
        core_vital_group_coverage=core_vital_group_coverage,
        site_eligibility=site_eligibility,
        site_counts_summary=site_counts_summary,
        stay_exclusions=stay_exclusions.reset_index(drop=True),
        stay_exclusion_summary_by_hospital=stay_exclusion_summary_by_hospital,
        counts_by_hospital=counts_by_hospital,
        retained_hospitals=retained_hospitals,
        retained_stays=retained_stays,
    )


def build_asic_stay_level_cohort(
    static_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    static_source_map: pd.DataFrame | None = None,
    raw_static_tables: dict[str, pd.DataFrame] | None = None,
) -> ASICStayLevelCohortResult:
    _require_columns(
        static_df,
        {"stay_id_global", "hospital_id", "icu_readmit", "icu_mortality", "icd10_codes"},
        "static_df",
    )

    authoritative_static = static_df[
        ["stay_id_global", "hospital_id", "icu_readmit", "icu_mortality", "icd10_codes"]
    ].copy()
    authoritative_static = authoritative_static.rename(columns={"icu_readmit": "readmission"})
    authoritative_static["stay_id_global"] = authoritative_static["stay_id_global"].astype("string")
    authoritative_static["hospital_id"] = authoritative_static["hospital_id"].astype("string")
    authoritative_static["icd10_codes"] = authoritative_static["icd10_codes"].astype("string")
    authoritative_static["icu_admission_time"] = pd.Series(
        0,
        index=authoritative_static.index,
        dtype="Int64",
    )

    if authoritative_static["stay_id_global"].duplicated().any():
        duplicate_ids = (
            authoritative_static.loc[
                authoritative_static["stay_id_global"].duplicated(keep=False),
                "stay_id_global",
            ]
            .dropna()
            .drop_duplicates()
            .tolist()[:10]
        )
        raise ValueError(
            "ASIC authoritative stay-level cohort requires one row per stay_id_global. "
            f"Duplicate IDs found: {duplicate_ids}"
        )

    dynamic_end_time_proxy = _dynamic_end_time_proxy(dynamic_df)
    cohort_df = authoritative_static.merge(
        dynamic_end_time_proxy,
        on="stay_id_global",
        how="left",
    )
    cohort_df = cohort_df[COHORT_TABLE_COLUMNS]

    if int(cohort_df.shape[0]) != int(cohort_df["stay_id_global"].nunique(dropna=True)):
        raise ValueError(
            "ASIC authoritative stay-level cohort is not one row per stay_id_global after "
            "joining dynamic end-time proxy information."
        )

    summary = _build_summary(cohort_df)
    preprocessing_notes = _build_preprocessing_notes()
    icu_end_time_proxy_summary_by_hospital = _summarize_icu_end_time_proxy_by_hospital(cohort_df)
    coding_distribution_by_hospital = _summarize_coding_distribution_by_hospital(cohort_df)
    chapter1 = build_asic_chapter1_cohort(
        cohort_df,
        dynamic_df,
        static_source_map=static_source_map,
        raw_static_tables=raw_static_tables,
    )

    return ASICStayLevelCohortResult(
        table=cohort_df,
        summary=summary,
        preprocessing_notes=preprocessing_notes,
        icu_end_time_proxy_summary_by_hospital=icu_end_time_proxy_summary_by_hospital,
        coding_distribution_by_hospital=coding_distribution_by_hospital,
        chapter1=chapter1,
    )
