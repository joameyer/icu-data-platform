from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


STAY_LEVEL_TABLE_COLUMNS = [
    "stay_id_global",
    "hospital_id",
    "readmission",
    "icu_mortality",
    "icd10_codes",
    "icu_admission_time",
    "icu_end_time_proxy",
]


@dataclass(frozen=True)
class ASICStayLevelResult:
    table: pd.DataFrame
    summary: pd.DataFrame
    preprocessing_notes: pd.DataFrame
    icu_end_time_proxy_summary_by_hospital: pd.DataFrame
    coding_distribution_by_hospital: pd.DataFrame


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(f"{table_name} is missing required stay-level columns: {missing}")


def build_asic_dynamic_end_time_proxy(dynamic_df: pd.DataFrame) -> pd.DataFrame:
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


def _authoritative_static_stay_level_input(static_df: pd.DataFrame) -> pd.DataFrame:
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

    return authoritative_static


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


def _build_summary(stay_level_df: pd.DataFrame) -> pd.DataFrame:
    missing_icu_end_time_proxy = int(stay_level_df["icu_end_time_proxy"].isna().sum())
    return pd.DataFrame(
        [
            {
                "metric": "cohort_rows_total",
                "value": int(stay_level_df.shape[0]),
                "note": "Authoritative ASIC stay-level cohort rows.",
            },
            {
                "metric": "cohort_unique_stay_id_global_total",
                "value": int(stay_level_df["stay_id_global"].nunique(dropna=True)),
                "note": "Unique stay_id_global values in the authoritative stay-level table.",
            },
            {
                "metric": "stays_with_non_missing_icu_end_time_proxy",
                "value": int(stay_level_df["icu_end_time_proxy"].notna().sum()),
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


def _summarize_icu_end_time_proxy_by_hospital(stay_level_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in stay_level_df.groupby("hospital_id", dropna=False):
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
    stay_level_df: pd.DataFrame,
    variable: str,
) -> pd.DataFrame:
    rows = []
    for hospital, hospital_df in stay_level_df.groupby("hospital_id", dropna=False):
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


def _summarize_coding_distribution_by_hospital(stay_level_df: pd.DataFrame) -> pd.DataFrame:
    distributions = [
        _coding_distribution(stay_level_df, "readmission"),
        _coding_distribution(stay_level_df, "icu_mortality"),
    ]
    return (
        pd.concat(distributions, ignore_index=True)
        .sort_values(["hospital_id", "variable", "code"], na_position="last")
        .reset_index(drop=True)
    )


def build_asic_stay_level_table_from_dynamic_end_time_proxy(
    static_df: pd.DataFrame,
    dynamic_end_time_proxy: pd.DataFrame,
) -> ASICStayLevelResult:
    authoritative_static = _authoritative_static_stay_level_input(static_df)
    stay_level_df = authoritative_static.merge(
        dynamic_end_time_proxy,
        on="stay_id_global",
        how="left",
    )
    stay_level_df = stay_level_df[STAY_LEVEL_TABLE_COLUMNS]

    if int(stay_level_df.shape[0]) != int(stay_level_df["stay_id_global"].nunique(dropna=True)):
        raise ValueError(
            "ASIC authoritative stay-level cohort is not one row per stay_id_global after "
            "joining dynamic end-time proxy information."
        )

    return ASICStayLevelResult(
        table=stay_level_df,
        summary=_build_summary(stay_level_df),
        preprocessing_notes=_build_preprocessing_notes(),
        icu_end_time_proxy_summary_by_hospital=_summarize_icu_end_time_proxy_by_hospital(
            stay_level_df
        ),
        coding_distribution_by_hospital=_summarize_coding_distribution_by_hospital(stay_level_df),
    )


def build_asic_stay_level_table(
    static_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
) -> ASICStayLevelResult:
    dynamic_end_time_proxy = build_asic_dynamic_end_time_proxy(dynamic_df)
    return build_asic_stay_level_table_from_dynamic_end_time_proxy(
        static_df,
        dynamic_end_time_proxy,
    )
