from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from icu_data_platform.analysis_seed.chapter1.config import (
    Chapter1SeedConfig,
    chapter1_group_definitions,
    default_chapter1_seed_config,
)


@dataclass(frozen=True)
class Chapter1CohortResult:
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


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(f"{table_name} is missing required Chapter 1 columns: {missing}")


def _chapter1_notes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "note_id": "chapter1_seed_input_contract",
                "category": "input_contract",
                "note": (
                    "The Chapter 1 seed starts from standardized harmonized ASIC static and "
                    "dynamic artifacts plus generic 8-hour blocked ASIC artifacts."
                ),
            },
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
                "note_id": "chapter1_outcome_rule",
                "category": "site_eligibility_rule",
                "note": (
                    "Within the portable seed, hospital-level outcome availability is "
                    "evaluated from standardized harmonized static data only."
                ),
            },
            {
                "note_id": "chapter1_first_stay_proxy_rule",
                "category": "stay_eligibility_rule",
                "note": (
                    "Readmission=0 is used as the ASIC first-stay proxy; readmission=1 and "
                    "missing readmission are excluded from retained Chapter 1 stays."
                ),
            },
        ]
    )


def _build_authoritative_cohort_from_standardized_inputs(
    static_harmonized: pd.DataFrame,
    stay_block_counts: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        static_harmonized,
        {"stay_id_global", "hospital_id", "icu_readmit", "icu_mortality", "icd10_codes"},
        "static_harmonized",
    )
    _require_columns(
        stay_block_counts,
        {"stay_id_global", "icu_admission_time", "icu_end_time_proxy"},
        "stay_block_counts",
    )

    cohort_df = static_harmonized[
        ["stay_id_global", "hospital_id", "icu_readmit", "icu_mortality", "icd10_codes"]
    ].copy()
    cohort_df = cohort_df.rename(columns={"icu_readmit": "readmission"})
    cohort_df["stay_id_global"] = cohort_df["stay_id_global"].astype("string")
    cohort_df["hospital_id"] = cohort_df["hospital_id"].astype("string")
    cohort_df["icd10_codes"] = cohort_df["icd10_codes"].astype("string")

    if cohort_df["stay_id_global"].duplicated().any():
        duplicate_ids = (
            cohort_df.loc[
                cohort_df["stay_id_global"].duplicated(keep=False),
                "stay_id_global",
            ]
            .dropna()
            .drop_duplicates()
            .tolist()[:10]
        )
        raise ValueError(
            "Chapter 1 seed requires one harmonized static row per stay_id_global. "
            f"Duplicate IDs found: {duplicate_ids}"
        )

    block_summary = stay_block_counts[
        ["stay_id_global", "icu_admission_time", "icu_end_time_proxy", "icu_end_time_proxy_hours"]
    ].copy()
    block_summary["stay_id_global"] = block_summary["stay_id_global"].astype("string")

    cohort_df = cohort_df.merge(block_summary, on="stay_id_global", how="left")
    return cohort_df


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


def _build_chapter1_core_vital_group_coverage(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    config: Chapter1SeedConfig,
) -> pd.DataFrame:
    rows = []
    group_definitions = chapter1_group_definitions()

    for hospital in sorted(cohort_df["hospital_id"].dropna().astype("string").unique().tolist()):
        hospital_dynamic = dynamic_df[dynamic_df["hospital_id"] == hospital]
        for group_name, candidate_variables in group_definitions.items():
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
                    "required_for_inclusion": group_name != "core_temp_optional",
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


def _build_chapter1_site_eligibility(
    cohort_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
    config: Chapter1SeedConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    stay_presence = _dynamic_stay_presence(dynamic_df)
    core_vital_group_coverage = _build_chapter1_core_vital_group_coverage(
        cohort_df,
        dynamic_df,
        config=config,
    )

    rows = []
    for hospital, hospital_df in cohort_df.groupby("hospital_id", dropna=False):
        hospital_stays = hospital_df[["stay_id_global"]].copy()
        hospital_stays["hospital_id"] = hospital

        outcome_numeric, outcome_codes = _normalize_binary_codes(hospital_df["icu_mortality"])
        outcome_non_null_count = int(outcome_numeric.notna().sum())
        icu_mortality_available = outcome_non_null_count > 0
        icu_mortality_verification_status = (
            "harmonized_static_has_non_missing_icu_mortality"
            if icu_mortality_available
            else "harmonized_static_icu_mortality_all_missing"
        )

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

        hospital_group_rows = core_vital_group_coverage[
            core_vital_group_coverage["hospital_id"] == hospital
        ]
        hospital_group_lookup = hospital_group_rows.set_index("physiologic_group")
        required_group_rows = hospital_group_rows[hospital_group_rows["required_for_inclusion"]]
        usable_core_vital_group_count = int(required_group_rows["group_usable"].sum())
        core_vital_coverage_sufficient = (
            usable_core_vital_group_count >= config.min_required_core_groups
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

        rows.append(
            {
                "hospital_id": hospital,
                "site_stay_count": int(hospital_df.shape[0]),
                "stays_with_any_dynamic_data": int(
                    hospital_dynamic_stays.loc[
                        hospital_dynamic_stays["has_dynamic_data"],
                        "stay_id_global",
                    ].nunique(dropna=True)
                ),
                "icu_mortality_non_null_count": outcome_non_null_count,
                "icu_mortality_codes": outcome_codes,
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
                "exclusion_reason": "; ".join(exclusion_reasons) if exclusion_reasons else pd.NA,
                "exclusion_reasons": exclusion_reasons,
            }
        )

    site_eligibility = pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)
    site_eligibility["include_in_chapter1"] = site_eligibility["site_included_ch1"]
    return core_vital_group_coverage, site_eligibility


def _summarize_chapter1_site_counts(site_eligibility: pd.DataFrame) -> pd.DataFrame:
    included_count = int(site_eligibility["site_included_ch1"].sum())
    total_count = int(site_eligibility["hospital_id"].nunique(dropna=True))
    excluded_count = total_count - included_count
    return pd.DataFrame(
        [
            {"metric": "hospitals_before_site_level_exclusion", "value": total_count},
            {"metric": "hospitals_after_site_level_exclusion", "value": included_count},
            {"metric": "hospitals_excluded_at_site_level", "value": excluded_count},
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

    final_status: list[str] = []
    exclusion_reasons: list[list[str]] = []
    for row in stay_exclusions.itertuples(index=False):
        reasons: list[str] = []
        if row.exclude_site_ineligible:
            final_status.append("excluded_site")
            exclusion_reasons.append(reasons)
            continue
        if row.exclude_no_dynamic_data:
            reasons.append("no dynamic data")
        if row.exclude_missing_readmission:
            reasons.append("missing readmission")
        if row.exclude_readmission_flagged:
            reasons.append("readmission flagged")
        final_status.append("retained" if not reasons else "excluded_stay")
        exclusion_reasons.append(reasons)

    stay_exclusions["stay_exclusion_reasons"] = exclusion_reasons
    stay_exclusions["exclusion_reason"] = stay_exclusions["stay_exclusion_reasons"].map(
        lambda reasons: "; ".join(reasons) if reasons else pd.NA
    )
    stay_exclusions["final_ch1_status"] = pd.Series(final_status, index=stay_exclusions.index)
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


def build_chapter1_cohort(
    static_harmonized: pd.DataFrame,
    dynamic_harmonized: pd.DataFrame,
    stay_block_counts: pd.DataFrame,
    config: Chapter1SeedConfig | None = None,
) -> Chapter1CohortResult:
    config = config or default_chapter1_seed_config()

    cohort_df = _build_authoritative_cohort_from_standardized_inputs(
        static_harmonized,
        stay_block_counts,
    )
    core_vital_group_coverage, site_eligibility = _build_chapter1_site_eligibility(
        cohort_df,
        dynamic_harmonized,
        config=config,
    )
    site_counts_summary = _summarize_chapter1_site_counts(site_eligibility)
    stay_exclusions = _build_chapter1_stay_exclusions(
        cohort_df,
        dynamic_harmonized,
        site_eligibility,
    )
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
        [
            "stay_id_global",
            "hospital_id",
            "readmission",
            "icu_mortality",
            "icd10_codes",
            "icu_admission_time",
            "icu_end_time_proxy",
            "icu_end_time_proxy_hours",
            "has_dynamic_data",
            "first_stay_proxy_eligible",
        ],
    ].reset_index(drop=True)

    return Chapter1CohortResult(
        table=retained_stays,
        notes=_chapter1_notes(),
        core_vital_group_coverage=core_vital_group_coverage,
        site_eligibility=site_eligibility,
        site_counts_summary=site_counts_summary,
        stay_exclusions=stay_exclusions.reset_index(drop=True),
        stay_exclusion_summary_by_hospital=stay_exclusion_summary_by_hospital,
        counts_by_hospital=counts_by_hospital,
        retained_hospitals=retained_hospitals,
        retained_stays=retained_stays[["stay_id_global", "hospital_id"]].reset_index(drop=True),
    )
