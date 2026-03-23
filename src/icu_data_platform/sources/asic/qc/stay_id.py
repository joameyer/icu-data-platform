from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ASICStayIdQCResult:
    summary: pd.DataFrame
    unique_stays_per_hospital: pd.DataFrame
    duplicated_local_ids_across_hospitals: pd.DataFrame
    missing_id_values: pd.DataFrame
    static_duplicate_global_ids: pd.DataFrame
    mapping_failures: pd.DataFrame
    duplicate_dynamic_time_index: pd.DataFrame


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(f"{table_name} is missing required stay-ID columns: {missing}")


def _expanded_collision_rows(static_ids: pd.DataFrame) -> pd.DataFrame:
    distinct_ids = static_ids[["hospital_id", "stay_id_local", "stay_id_global"]].drop_duplicates()
    hospital_counts = (
        distinct_ids.groupby("stay_id_local", dropna=False)["hospital_id"]
        .nunique()
        .rename("hospital_count_for_stay_id_local")
        .reset_index()
    )
    collisions = hospital_counts[hospital_counts["hospital_count_for_stay_id_local"] > 1]
    if collisions.empty:
        return pd.DataFrame(
            columns=[
                "stay_id_local",
                "hospital_id",
                "stay_id_global",
                "hospital_count_for_stay_id_local",
            ]
        )

    return (
        distinct_ids.merge(collisions, on="stay_id_local", how="inner")
        .sort_values(["stay_id_local", "hospital_id", "stay_id_global"])
        .reset_index(drop=True)
    )


def _missing_id_rows(static_df: pd.DataFrame, dynamic_df: pd.DataFrame) -> pd.DataFrame:
    static_missing = static_df[
        static_df["hospital_id"].isna() | static_df["stay_id_local"].isna()
    ][["hospital_id", "stay_id_local", "stay_id_global"]].copy()
    static_missing.insert(0, "table", "static")
    static_missing["time"] = pd.NA

    dynamic_columns = ["hospital_id", "stay_id_local", "stay_id_global"]
    if "time" in dynamic_df.columns:
        dynamic_columns.append("time")
    dynamic_missing = dynamic_df[
        dynamic_df["hospital_id"].isna() | dynamic_df["stay_id_local"].isna()
    ][dynamic_columns].copy()
    dynamic_missing.insert(0, "table", "dynamic")
    if "time" not in dynamic_missing.columns:
        dynamic_missing["time"] = pd.NA

    return pd.concat([static_missing, dynamic_missing], ignore_index=True)


def _static_duplicate_global_ids(static_df: pd.DataFrame) -> pd.DataFrame:
    duplicate_counts = (
        static_df.dropna(subset=["stay_id_global"])
        .groupby("stay_id_global")
        .size()
        .rename("duplicate_count_per_stay_id_global")
        .reset_index()
    )
    duplicate_counts = duplicate_counts[
        duplicate_counts["duplicate_count_per_stay_id_global"] > 1
    ]
    if duplicate_counts.empty:
        return pd.DataFrame(
            columns=[
                "hospital_id",
                "stay_id_local",
                "stay_id_global",
                "duplicate_count_per_stay_id_global",
            ]
        )

    return (
        static_df[["hospital_id", "stay_id_local", "stay_id_global"]]
        .merge(duplicate_counts, on="stay_id_global", how="inner")
        .sort_values(
            [
                "stay_id_global",
                "hospital_id",
                "stay_id_local",
            ]
        )
        .reset_index(drop=True)
    )


def _dynamic_mapping_failures(static_df: pd.DataFrame, dynamic_df: pd.DataFrame) -> pd.DataFrame:
    static_key_counts = (
        static_df.groupby(["hospital_id", "stay_id_local"], dropna=False)
        .agg(
            static_row_count=("stay_id_global", "size"),
            static_global_id_count=("stay_id_global", lambda series: series.dropna().nunique()),
        )
        .reset_index()
    )
    static_lookup = static_df[
        ["hospital_id", "stay_id_local", "stay_id_global"]
    ].drop_duplicates().rename(columns={"stay_id_global": "stay_id_global_static"})

    dynamic_columns = ["hospital_id", "stay_id_local", "stay_id_global"]
    if "time" in dynamic_df.columns:
        dynamic_columns.append("time")
    mapping = dynamic_df[dynamic_columns].copy().reset_index(drop=True)
    mapping.insert(0, "dynamic_row_number", mapping.index)
    mapping = mapping.merge(
        static_key_counts,
        on=["hospital_id", "stay_id_local"],
        how="left",
    ).merge(
        static_lookup,
        on=["hospital_id", "stay_id_local"],
        how="left",
    )

    issue_type = pd.Series(pd.NA, index=mapping.index, dtype="string")

    missing_static_mask = mapping["static_row_count"].isna()
    issue_type[missing_static_mask] = "missing_static_match"

    ambiguous_mask = mapping["static_row_count"].fillna(0).ne(1) | mapping[
        "static_global_id_count"
    ].fillna(0).ne(1)
    issue_type[ambiguous_mask & issue_type.isna()] = "ambiguous_static_mapping"

    mismatched_global_mask = (
        mapping["stay_id_global"].notna()
        & mapping["stay_id_global_static"].notna()
        & mapping["stay_id_global"].ne(mapping["stay_id_global_static"])
    )
    issue_type[mismatched_global_mask & issue_type.isna()] = "mismatched_stay_id_global"

    failures = mapping[issue_type.notna()].copy()
    failures.insert(1, "issue_type", issue_type[issue_type.notna()].tolist())
    return failures.reset_index(drop=True)


def _duplicate_dynamic_time_rows(dynamic_df: pd.DataFrame) -> pd.DataFrame:
    if "time" not in dynamic_df.columns:
        return pd.DataFrame(
            columns=["hospital_id", "stay_id_local", "stay_id_global", "time", "row_count"]
        )

    duplicate_keys = (
        dynamic_df.groupby(["stay_id_global", "time"], dropna=False)
        .size()
        .rename("row_count")
        .reset_index()
    )
    duplicate_keys = duplicate_keys[duplicate_keys["row_count"] > 1]
    if duplicate_keys.empty:
        return pd.DataFrame(
            columns=["hospital_id", "stay_id_local", "stay_id_global", "time", "row_count"]
        )

    return (
        dynamic_df[["hospital_id", "stay_id_local", "stay_id_global", "time"]]
        .merge(duplicate_keys, on=["stay_id_global", "time"], how="inner")
        .sort_values(["stay_id_global", "time", "hospital_id", "stay_id_local"])
        .reset_index(drop=True)
    )


def build_asic_stay_id_qc(
    static_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
) -> ASICStayIdQCResult:
    required_columns = {"hospital_id", "stay_id_local", "stay_id_global"}
    _require_columns(static_df, required_columns, "static_df")
    _require_columns(dynamic_df, required_columns, "dynamic_df")

    unique_stays_per_hospital = (
        static_df.groupby("hospital_id", dropna=False)["stay_id_global"]
        .nunique(dropna=True)
        .rename("unique_stays")
        .reset_index()
        .sort_values("hospital_id")
        .reset_index(drop=True)
    )
    duplicated_local_ids_across_hospitals = _expanded_collision_rows(static_df)
    missing_id_values = _missing_id_rows(static_df, dynamic_df)
    static_duplicate_global_ids = _static_duplicate_global_ids(static_df)
    mapping_failures = _dynamic_mapping_failures(static_df, dynamic_df)
    duplicate_dynamic_time_index = _duplicate_dynamic_time_rows(dynamic_df)

    summary = pd.DataFrame(
        [
            {
                "metric": "static_rows_total",
                "value": int(static_df.shape[0]),
            },
            {
                "metric": "static_unique_stay_id_global_total",
                "value": int(static_df["stay_id_global"].nunique(dropna=True)),
            },
            {
                "metric": "unique_stays_hospital_count",
                "value": int(unique_stays_per_hospital["hospital_id"].nunique(dropna=True)),
            },
            {
                "metric": "duplicated_stay_id_local_values_across_hospitals",
                "value": int(
                    duplicated_local_ids_across_hospitals["stay_id_local"].nunique(dropna=True)
                ),
            },
            {
                "metric": "failed_or_missing_static_dynamic_mappings",
                "value": int(mapping_failures.shape[0]),
            },
            {
                "metric": "rows_missing_hospital_id_or_stay_id_local",
                "value": int(missing_id_values.shape[0]),
            },
            {
                "metric": "duplicate_static_stay_id_global_rows",
                "value": int(static_duplicate_global_ids.shape[0]),
            },
            {
                "metric": "duplicate_dynamic_rows_within_stay_time",
                "value": int(duplicate_dynamic_time_index.shape[0]),
            },
        ]
    )

    return ASICStayIdQCResult(
        summary=summary,
        unique_stays_per_hospital=unique_stays_per_hospital,
        duplicated_local_ids_across_hospitals=duplicated_local_ids_across_hospitals,
        missing_id_values=missing_id_values,
        static_duplicate_global_ids=static_duplicate_global_ids,
        mapping_failures=mapping_failures,
        duplicate_dynamic_time_index=duplicate_dynamic_time_index,
    )


def assert_valid_asic_stay_ids(qc_result: ASICStayIdQCResult) -> None:
    issues: list[str] = []

    if not qc_result.missing_id_values.empty:
        examples = qc_result.missing_id_values.head(5).to_dict(orient="records")
        issues.append(
            "Missing hospital_id or stay_id_local values used in stay ID construction. "
            f"Examples: {examples}"
        )

    if not qc_result.static_duplicate_global_ids.empty:
        examples = qc_result.static_duplicate_global_ids.head(5).to_dict(orient="records")
        issues.append(
            "stay_id_global is not unique in the pooled static stay table. "
            f"Examples: {examples}"
        )

    if not qc_result.mapping_failures.empty:
        examples = qc_result.mapping_failures.head(5).to_dict(orient="records")
        issues.append(
            "Dynamic rows do not map cleanly to exactly one static stay_id_global. "
            f"Examples: {examples}"
        )

    if not qc_result.duplicate_dynamic_time_index.empty:
        examples = qc_result.duplicate_dynamic_time_index.head(5).to_dict(orient="records")
        issues.append(
            "Duplicate (stay_id_global, time) rows found in harmonized dynamic data. "
            f"Examples: {examples}"
        )

    if issues:
        raise ValueError("ASIC pooled stay-ID QC failed:\n- " + "\n- ".join(issues))
