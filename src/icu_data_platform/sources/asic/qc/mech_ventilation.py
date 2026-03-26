from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


MECH_VENT_QC_MARKERS = ("fio2", "peep", "vt", "vt_per_kg_ibw")
MECH_VENT_QC_MAX_GAP_HOURS = 8.0
MECH_VENT_QC_MIN_DURATION_HOURS = 24.0


@dataclass(frozen=True)
class ASICMechanicalVentilationQCResult:
    stay_level: pd.DataFrame
    episode_level: pd.DataFrame
    hospital_summary: pd.DataFrame
    failed_stays: pd.DataFrame
    documentation: pd.DataFrame


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(
            f"{table_name} is missing required mechanical-ventilation QC columns: {missing}"
        )


def _empty_episode_level() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "stay_id_global",
            "hospital_id",
            "episode_index",
            "episode_start_time",
            "episode_end_time",
            "episode_duration_hours",
            "ventilation_supported_timestamp_count_in_episode",
        ]
    )


def _empty_stay_level() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "stay_id_global",
            "hospital_id",
            "number_of_ventilation_supported_timestamps",
            "number_of_derived_ventilation_episodes",
            "maximum_derived_episode_duration_hours",
            "mech_vent_ge_24h_qc",
        ]
    )


def _empty_hospital_summary() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "hospital_id",
            "stays_checked",
            "stays_satisfying_mech_vent_ge_24h_qc",
            "proportion_satisfying_mech_vent_ge_24h_qc",
            "min_max_derived_episode_duration_hours",
            "p25_max_derived_episode_duration_hours",
            "median_max_derived_episode_duration_hours",
            "p75_max_derived_episode_duration_hours",
            "max_max_derived_episode_duration_hours",
        ]
    )


def _empty_failed_stays() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "stay_id_global",
            "hospital_id",
            "number_of_ventilation_supported_timestamps",
            "number_of_derived_ventilation_episodes",
            "maximum_derived_episode_duration_hours",
            "mech_vent_ge_24h_qc",
            "failure_reason",
        ]
    )


def _build_documentation() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "note_id": "mech_vent_ge_24h_qc_observed_support_markers",
                "category": "observed_support_definition",
                "note": (
                    "This QC marks a harmonized ASIC dynamic timestamp as ventilation-"
                    "supported when any of fio2, peep, vt, or vt_per_kg_ibw is non-missing."
                ),
            },
            {
                "note_id": "mech_vent_ge_24h_qc_continuity_rule",
                "category": "episode_definition",
                "note": (
                    "Observed ventilation-supported timestamps are connected into the same "
                    "episode when the gap between adjacent timestamps within a stay is less "
                    "than or equal to 8 hours; larger gaps start a new episode."
                ),
            },
            {
                "note_id": "mech_vent_ge_24h_qc_direct_timestamp_scope",
                "category": "implementation_scope",
                "note": (
                    "Episode derivation uses harmonized ASIC dynamic timestamps directly "
                    "and does not use blocked 8-hour data."
                ),
            },
            {
                "note_id": "mech_vent_ge_24h_qc_chapter1_contract_verification",
                "category": "intended_use",
                "note": (
                    "This derivation is intended as an upstream cohort-contract "
                    "verification for Chapter 1 and is not a claim of perfect ground-truth "
                    "mechanical ventilation timing."
                ),
            },
        ]
    )


def _validate_stay_identity(dynamic_df: pd.DataFrame) -> pd.DataFrame:
    stay_lookup = (
        dynamic_df[["stay_id_global", "hospital_id"]]
        .drop_duplicates()
        .sort_values(["hospital_id", "stay_id_global"], na_position="last")
        .reset_index(drop=True)
    )

    missing_identity = stay_lookup[
        stay_lookup["stay_id_global"].isna() | stay_lookup["hospital_id"].isna()
    ]
    if not missing_identity.empty:
        examples = missing_identity.head(5).to_dict(orient="records")
        raise ValueError(
            "Mechanical-ventilation QC requires non-missing stay_id_global and hospital_id "
            f"for every checked stay. Examples: {examples}"
        )

    hospital_counts = (
        stay_lookup.groupby("stay_id_global", dropna=False)["hospital_id"]
        .nunique(dropna=False)
        .rename("hospital_count")
        .reset_index()
    )
    inconsistent = hospital_counts[hospital_counts["hospital_count"] > 1]
    if not inconsistent.empty:
        examples = (
            stay_lookup.merge(inconsistent[["stay_id_global"]], on="stay_id_global", how="inner")
            .head(5)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Mechanical-ventilation QC found stay_id_global values mapped to multiple "
            f"hospital_id values. Examples: {examples}"
        )

    return stay_lookup


def _prepare_supported_timestamps(dynamic_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    stay_lookup = _validate_stay_identity(dynamic_df)

    working = dynamic_df[
        ["stay_id_global", "hospital_id", "time", *MECH_VENT_QC_MARKERS]
    ].copy()
    working["ventilation_supported"] = working[list(MECH_VENT_QC_MARKERS)].notna().any(axis=1)
    working["time_timedelta"] = pd.to_timedelta(working["time"], errors="coerce")

    invalid_supported_times = working[
        working["ventilation_supported"] & working["time_timedelta"].isna()
    ][["stay_id_global", "hospital_id", "time"]]
    if not invalid_supported_times.empty:
        examples = invalid_supported_times.head(5).to_dict(orient="records")
        raise ValueError(
            "Mechanical-ventilation QC could not parse time values for ventilation-supported "
            f"rows. Examples: {examples}"
        )

    supported = (
        working[working["ventilation_supported"]]
        .sort_values(["stay_id_global", "time_timedelta", "time"])
        .reset_index(drop=True)
    )
    return stay_lookup, supported


def _derive_episode_level(supported_timestamps: pd.DataFrame) -> pd.DataFrame:
    if supported_timestamps.empty:
        return _empty_episode_level()

    supported = supported_timestamps[
        ["stay_id_global", "hospital_id", "time", "time_timedelta"]
    ].copy()
    supported["gap_since_previous_hours"] = (
        supported.groupby("stay_id_global")["time_timedelta"].diff().dt.total_seconds() / 3600.0
    )
    supported["starts_new_episode"] = (
        supported["gap_since_previous_hours"].isna()
        | supported["gap_since_previous_hours"].gt(MECH_VENT_QC_MAX_GAP_HOURS)
    )
    supported["episode_index"] = (
        supported.groupby("stay_id_global")["starts_new_episode"].cumsum().astype(int)
    )

    episode_level = (
        supported.groupby(["stay_id_global", "hospital_id", "episode_index"], dropna=False)
        .agg(
            episode_start_timedelta=("time_timedelta", "min"),
            episode_end_timedelta=("time_timedelta", "max"),
            ventilation_supported_timestamp_count_in_episode=("time_timedelta", "size"),
        )
        .reset_index()
    )
    episode_level["episode_start_time"] = episode_level["episode_start_timedelta"].astype("string")
    episode_level["episode_end_time"] = episode_level["episode_end_timedelta"].astype("string")
    episode_level["episode_duration_hours"] = (
        episode_level["episode_end_timedelta"] - episode_level["episode_start_timedelta"]
    ).dt.total_seconds() / 3600.0

    return (
        episode_level[
            [
                "stay_id_global",
                "hospital_id",
                "episode_index",
                "episode_start_time",
                "episode_end_time",
                "episode_duration_hours",
                "ventilation_supported_timestamp_count_in_episode",
            ]
        ]
        .sort_values(["hospital_id", "stay_id_global", "episode_index"])
        .reset_index(drop=True)
    )


def _derive_stay_level(
    stay_lookup: pd.DataFrame,
    supported_timestamps: pd.DataFrame,
    episode_level: pd.DataFrame,
) -> pd.DataFrame:
    if stay_lookup.empty:
        return _empty_stay_level()

    timestamp_counts = (
        supported_timestamps.groupby(["stay_id_global", "hospital_id"], dropna=False)
        .size()
        .rename("number_of_ventilation_supported_timestamps")
        .reset_index()
    )
    episode_summary = (
        episode_level.groupby(["stay_id_global", "hospital_id"], dropna=False)
        .agg(
            number_of_derived_ventilation_episodes=("episode_index", "size"),
            maximum_derived_episode_duration_hours=("episode_duration_hours", "max"),
        )
        .reset_index()
    )

    stay_level = stay_lookup.merge(
        timestamp_counts,
        on=["stay_id_global", "hospital_id"],
        how="left",
    ).merge(
        episode_summary,
        on=["stay_id_global", "hospital_id"],
        how="left",
    )
    stay_level["number_of_ventilation_supported_timestamps"] = (
        stay_level["number_of_ventilation_supported_timestamps"].fillna(0).astype(int)
    )
    stay_level["number_of_derived_ventilation_episodes"] = (
        stay_level["number_of_derived_ventilation_episodes"].fillna(0).astype(int)
    )
    stay_level["maximum_derived_episode_duration_hours"] = stay_level[
        "maximum_derived_episode_duration_hours"
    ].fillna(0.0)
    stay_level["mech_vent_ge_24h_qc"] = stay_level[
        "maximum_derived_episode_duration_hours"
    ].ge(MECH_VENT_QC_MIN_DURATION_HOURS)

    return (
        stay_level[
            [
                "stay_id_global",
                "hospital_id",
                "number_of_ventilation_supported_timestamps",
                "number_of_derived_ventilation_episodes",
                "maximum_derived_episode_duration_hours",
                "mech_vent_ge_24h_qc",
            ]
        ]
        .sort_values(["hospital_id", "stay_id_global"])
        .reset_index(drop=True)
    )


def _derive_hospital_summary(stay_level: pd.DataFrame) -> pd.DataFrame:
    if stay_level.empty:
        return _empty_hospital_summary()

    rows = []
    for hospital_id, hospital_df in stay_level.groupby("hospital_id", dropna=False):
        max_durations = hospital_df["maximum_derived_episode_duration_hours"].astype(float)
        rows.append(
            {
                "hospital_id": hospital_id,
                "stays_checked": int(hospital_df.shape[0]),
                "stays_satisfying_mech_vent_ge_24h_qc": int(
                    hospital_df["mech_vent_ge_24h_qc"].sum()
                ),
                "proportion_satisfying_mech_vent_ge_24h_qc": float(
                    hospital_df["mech_vent_ge_24h_qc"].mean()
                ),
                "min_max_derived_episode_duration_hours": float(max_durations.min()),
                "p25_max_derived_episode_duration_hours": float(max_durations.quantile(0.25)),
                "median_max_derived_episode_duration_hours": float(max_durations.median()),
                "p75_max_derived_episode_duration_hours": float(max_durations.quantile(0.75)),
                "max_max_derived_episode_duration_hours": float(max_durations.max()),
            }
        )

    return pd.DataFrame(rows).sort_values("hospital_id").reset_index(drop=True)


def _derive_failed_stays(stay_level: pd.DataFrame) -> pd.DataFrame:
    if stay_level.empty:
        return _empty_failed_stays()

    failed_stays = stay_level.loc[~stay_level["mech_vent_ge_24h_qc"]].copy()
    failed_stays["failure_reason"] = (
        "No observed ventilation episode reached 24 hours under the harmonized-marker "
        "8-hour continuity QC rule."
    )
    return failed_stays.reset_index(drop=True)


def build_asic_mech_vent_ge_24h_qc(
    dynamic_df: pd.DataFrame,
) -> ASICMechanicalVentilationQCResult:
    required_columns = {"stay_id_global", "hospital_id", "time", *MECH_VENT_QC_MARKERS}
    _require_columns(dynamic_df, required_columns, "dynamic_df")

    stay_lookup, supported_timestamps = _prepare_supported_timestamps(dynamic_df)
    episode_level = _derive_episode_level(supported_timestamps)
    stay_level = _derive_stay_level(stay_lookup, supported_timestamps, episode_level)
    hospital_summary = _derive_hospital_summary(stay_level)
    failed_stays = _derive_failed_stays(stay_level)
    documentation = _build_documentation()

    return ASICMechanicalVentilationQCResult(
        stay_level=stay_level,
        episode_level=episode_level,
        hospital_summary=hospital_summary,
        failed_stays=failed_stays,
        documentation=documentation,
    )
