from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


BLOCK_SIZE_HOURS = 8
BLOCK_INDEX_COLUMNS = [
    "stay_id_global",
    "hospital_id",
    "block_index",
    "block_start_h",
    "block_end_h",
    "prediction_time_h",
]
STAY_BLOCK_COUNT_COLUMNS = [
    "stay_id_global",
    "hospital_id",
    "icu_admission_time",
    "icu_end_time_proxy",
    "icu_end_time_proxy_hours",
    "completed_block_count",
    "has_completed_block",
    "ends_exactly_on_8h_boundary",
    "terminal_block_end_h",
]
NEGATIVE_DYNAMIC_TIME_QC_COLUMNS = [
    "stay_id_global",
    "hospital_id",
    "negative_dynamic_time_row_count",
    "min_negative_time_h",
    "max_negative_time_h",
    "example_negative_times",
]
BLOCK_COUNT_DISTRIBUTION_COLUMNS = [
    "hospital_id",
    "completed_block_count",
    "stay_count",
    "hospital_stay_count",
    "proportion_of_hospital_stays",
]
EXAMPLE_STAY_COLUMNS = [
    "example_type",
    "stay_id_global",
    "hospital_id",
    "icu_end_time_proxy",
    "icu_end_time_proxy_hours",
    "completed_block_count",
    "block_boundaries",
]
BLOCKED_DYNAMIC_EXCLUDED_COLUMNS = {
    "hospital_id",
    "stay_id_global",
    "stay_id_local",
    "time",
    "time_h",
    "minutes_since_admit",
    "source_row_order",
}
BLOCKED_DYNAMIC_BASE_COLUMNS = BLOCK_INDEX_COLUMNS + [
    "dynamic_row_count",
    "non_missing_measurements_in_block",
    "observed_variables_in_block",
]


@dataclass(frozen=True)
class ASICBlockResult:
    block_index: pd.DataFrame
    blocked_dynamic_features: pd.DataFrame
    stay_block_counts: pd.DataFrame
    block_count_distribution_by_hospital: pd.DataFrame
    negative_dynamic_time_qc: pd.DataFrame
    qc_summary: pd.DataFrame
    example_stays: pd.DataFrame


def _require_columns(df: pd.DataFrame, required_columns: set[str], table_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise KeyError(
            f"{table_name} is missing required block-construction columns: {missing}"
        )


def _prepare_stays(stay_level_df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(
        stay_level_df,
        {"stay_id_global", "hospital_id", "icu_admission_time", "icu_end_time_proxy"},
        "stay_level_df",
    )

    stays = stay_level_df[
        ["stay_id_global", "hospital_id", "icu_admission_time", "icu_end_time_proxy"]
    ].copy()
    stays["stay_id_global"] = stays["stay_id_global"].astype("string")
    stays["hospital_id"] = stays["hospital_id"].astype("string")

    if stays["stay_id_global"].duplicated().any():
        duplicate_ids = (
            stays.loc[
                stays["stay_id_global"].duplicated(keep=False),
                "stay_id_global",
            ]
            .dropna()
            .drop_duplicates()
            .tolist()[:10]
        )
        raise ValueError(
            "ASIC stay-level rows must be unique before 8-hour block construction. "
            f"Duplicate stay_id_global values found: {duplicate_ids}"
        )

    admission_numeric = pd.to_numeric(stays["icu_admission_time"], errors="coerce")
    invalid_anchor = ~admission_numeric.eq(0).fillna(False)
    if invalid_anchor.any():
        invalid_stays = (
            stays.loc[invalid_anchor, "stay_id_global"].dropna().astype("string").tolist()[:10]
        )
        raise ValueError(
            "ASIC 8-hour block construction requires icu_admission_time = 0 for every stay. "
            f"Invalid stays: {invalid_stays}"
        )

    proxy_timedelta = pd.to_timedelta(stays["icu_end_time_proxy"], errors="coerce")
    stays["icu_end_time_proxy_hours"] = proxy_timedelta.dt.total_seconds() / 3600.0
    stays["completed_block_count"] = pd.Series(0, index=stays.index, dtype="Int64")

    non_negative_proxy = (
        stays["icu_end_time_proxy_hours"].notna() & stays["icu_end_time_proxy_hours"].ge(0)
    )
    stays.loc[non_negative_proxy, "completed_block_count"] = (
        stays.loc[non_negative_proxy, "icu_end_time_proxy_hours"] // BLOCK_SIZE_HOURS
    ).astype("Int64")
    stays["has_completed_block"] = stays["completed_block_count"].ge(1)
    stays["ends_exactly_on_8h_boundary"] = (
        non_negative_proxy
        & stays["icu_end_time_proxy_hours"].mod(BLOCK_SIZE_HOURS).eq(0)
    )
    stays["terminal_block_end_h"] = stays["completed_block_count"] * BLOCK_SIZE_HOURS

    ordered_columns = STAY_BLOCK_COUNT_COLUMNS + [
        column for column in stays.columns if column not in STAY_BLOCK_COUNT_COLUMNS
    ]
    return stays[ordered_columns].sort_values(
        ["hospital_id", "stay_id_global"]
    ).reset_index(drop=True)


def _dynamic_input(
    dynamic_df: pd.DataFrame,
    stay_block_counts: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(dynamic_df, {"stay_id_global", "hospital_id", "time"}, "dynamic_df")

    stay_ids = stay_block_counts["stay_id_global"].dropna().astype("string")
    retained_dynamic = dynamic_df[
        dynamic_df["stay_id_global"].astype("string").isin(stay_ids)
    ].copy()
    retained_dynamic["stay_id_global"] = retained_dynamic["stay_id_global"].astype("string")
    retained_dynamic["hospital_id"] = retained_dynamic["hospital_id"].astype("string")
    retained_dynamic["time_h"] = (
        pd.to_timedelta(retained_dynamic["time"], errors="coerce").dt.total_seconds() / 3600.0
    )
    retained_dynamic["source_row_order"] = pd.Series(
        range(retained_dynamic.shape[0]),
        index=retained_dynamic.index,
        dtype="Int64",
    )
    return retained_dynamic


def _build_negative_dynamic_time_qc(retained_dynamic: pd.DataFrame) -> pd.DataFrame:
    negative_times = retained_dynamic[retained_dynamic["time_h"].lt(0).fillna(False)].copy()
    if negative_times.empty:
        return pd.DataFrame(columns=NEGATIVE_DYNAMIC_TIME_QC_COLUMNS)

    rows = []
    for (stay_id_global, hospital_id), stay_df in negative_times.groupby(
        ["stay_id_global", "hospital_id"],
        dropna=False,
    ):
        example_times = (
            stay_df["time"].astype("string").dropna().drop_duplicates().tolist()[:5]
        )
        rows.append(
            {
                "stay_id_global": stay_id_global,
                "hospital_id": hospital_id,
                "negative_dynamic_time_row_count": int(stay_df.shape[0]),
                "min_negative_time_h": float(stay_df["time_h"].min()),
                "max_negative_time_h": float(stay_df["time_h"].max()),
                "example_negative_times": example_times,
            }
        )

    return pd.DataFrame(rows)[NEGATIVE_DYNAMIC_TIME_QC_COLUMNS].sort_values(
        ["hospital_id", "stay_id_global"]
    ).reset_index(drop=True)


def _build_block_index(stay_block_counts: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in stay_block_counts.itertuples(index=False):
        completed_block_count = (
            0 if pd.isna(row.completed_block_count) else int(row.completed_block_count)
        )
        for block_index in range(completed_block_count):
            block_start_h = block_index * BLOCK_SIZE_HOURS
            block_end_h = block_start_h + BLOCK_SIZE_HOURS
            rows.append(
                {
                    "stay_id_global": row.stay_id_global,
                    "hospital_id": row.hospital_id,
                    "block_index": block_index,
                    "block_start_h": block_start_h,
                    "block_end_h": block_end_h,
                    "prediction_time_h": block_end_h,
                }
            )

    if not rows:
        return pd.DataFrame(columns=BLOCK_INDEX_COLUMNS)

    return pd.DataFrame(rows)[BLOCK_INDEX_COLUMNS].sort_values(
        ["hospital_id", "stay_id_global", "block_index"]
    ).reset_index(drop=True)


def _blocked_dynamic_feature_columns(retained_dynamic: pd.DataFrame) -> list[str]:
    return [
        column
        for column in retained_dynamic.columns
        if column not in BLOCKED_DYNAMIC_EXCLUDED_COLUMNS
    ]


def _blocked_dynamic_output_columns(feature_columns: list[str]) -> list[str]:
    columns = list(BLOCKED_DYNAMIC_BASE_COLUMNS)
    for column in feature_columns:
        columns.extend(
            [
                f"{column}_obs_count",
                f"{column}_mean",
                f"{column}_median",
                f"{column}_min",
                f"{column}_max",
                f"{column}_last",
            ]
        )
    return columns


def _build_blocked_dynamic_features(
    block_index: pd.DataFrame,
    retained_dynamic: pd.DataFrame,
) -> pd.DataFrame:
    feature_columns = _blocked_dynamic_feature_columns(retained_dynamic)
    output_columns = _blocked_dynamic_output_columns(feature_columns)

    if block_index.empty:
        return pd.DataFrame(columns=output_columns)

    assignable = retained_dynamic[
        retained_dynamic["time_h"].notna() & retained_dynamic["time_h"].ge(0)
    ].copy()
    assignable["block_index"] = (assignable["time_h"] // BLOCK_SIZE_HOURS).astype("Int64")

    block_lookup = block_index[BLOCK_INDEX_COLUMNS].copy()
    assigned = assignable.merge(
        block_lookup,
        on=["stay_id_global", "hospital_id", "block_index"],
        how="inner",
    ).sort_values(
        ["hospital_id", "stay_id_global", "block_index", "time_h", "source_row_order"]
    )

    aggregated = pd.DataFrame(columns=output_columns)
    if not assigned.empty:
        group_columns = BLOCK_INDEX_COLUMNS
        grouped = assigned.groupby(group_columns, dropna=False, sort=False)
        dynamic_row_count = grouped.size().rename("dynamic_row_count")

        aggregated_parts: list[pd.Series | pd.DataFrame] = [dynamic_row_count]
        if feature_columns:
            assigned.loc[:, feature_columns] = assigned[feature_columns].apply(
                pd.to_numeric,
                errors="coerce",
            )
            assigned["row_non_missing_measurements"] = assigned[feature_columns].notna().sum(axis=1)
            grouped = assigned.groupby(group_columns, dropna=False, sort=False)

            counts = grouped[feature_columns].count().rename(
                columns=lambda column: f"{column}_obs_count"
            )
            aggregated_parts.extend(
                [
                    grouped["row_non_missing_measurements"]
                    .sum()
                    .rename("non_missing_measurements_in_block"),
                    counts.gt(0).sum(axis=1).rename("observed_variables_in_block"),
                    counts,
                    grouped[feature_columns].mean().rename(
                        columns=lambda column: f"{column}_mean"
                    ),
                    grouped[feature_columns].median().rename(
                        columns=lambda column: f"{column}_median"
                    ),
                    grouped[feature_columns].min().rename(
                        columns=lambda column: f"{column}_min"
                    ),
                    grouped[feature_columns].max().rename(
                        columns=lambda column: f"{column}_max"
                    ),
                    grouped[feature_columns].agg("last").rename(
                        columns=lambda column: f"{column}_last"
                    ),
                ]
            )
        else:
            aggregated_parts.extend(
                [
                    pd.Series(
                        0,
                        index=dynamic_row_count.index,
                        name="non_missing_measurements_in_block",
                    ),
                    pd.Series(
                        0,
                        index=dynamic_row_count.index,
                        name="observed_variables_in_block",
                    ),
                ]
            )

        aggregated = pd.concat(aggregated_parts, axis=1).reset_index()

    blocked_dynamic = block_index.merge(
        aggregated,
        on=BLOCK_INDEX_COLUMNS,
        how="left",
    )

    count_columns = [
        "dynamic_row_count",
        "non_missing_measurements_in_block",
        "observed_variables_in_block",
        *[f"{column}_obs_count" for column in feature_columns],
    ]
    summary_columns = [
        name
        for name in output_columns
        if name not in BLOCK_INDEX_COLUMNS and name not in count_columns
    ]

    for column in count_columns:
        if column not in blocked_dynamic.columns:
            blocked_dynamic[column] = pd.Series(0, index=blocked_dynamic.index, dtype="Int64")
        else:
            blocked_dynamic[column] = (
                pd.to_numeric(blocked_dynamic[column], errors="coerce").fillna(0).astype("Int64")
            )

    for column in summary_columns:
        if column not in blocked_dynamic.columns:
            blocked_dynamic[column] = pd.Series(pd.NA, index=blocked_dynamic.index, dtype="Float64")
        else:
            blocked_dynamic[column] = pd.to_numeric(blocked_dynamic[column], errors="coerce")

    return blocked_dynamic[output_columns].sort_values(
        ["hospital_id", "stay_id_global", "block_index"]
    ).reset_index(drop=True)


def _build_validation_table(
    stay_block_counts: pd.DataFrame,
    block_index: pd.DataFrame,
) -> pd.DataFrame:
    generated_block_counts = (
        block_index.groupby("stay_id_global", dropna=False)
        .size()
        .rename("generated_block_count")
        .reset_index()
    )
    terminal_block_end = (
        block_index.groupby("stay_id_global", dropna=False)["block_end_h"]
        .max()
        .rename("max_block_end_h")
        .reset_index()
    )

    validation = stay_block_counts.merge(
        generated_block_counts,
        on="stay_id_global",
        how="left",
    ).merge(
        terminal_block_end,
        on="stay_id_global",
        how="left",
    )
    validation["generated_block_count"] = (
        validation["generated_block_count"].fillna(0).astype("Int64")
    )
    validation["max_block_end_h"] = validation["max_block_end_h"].fillna(0).astype("Int64")
    return validation


def _validate_block_index(validation: pd.DataFrame, block_index: pd.DataFrame) -> None:
    if not block_index["prediction_time_h"].equals(block_index["block_end_h"]):
        raise ValueError(
            "prediction_time_h must equal block_end_h for every ASIC 8-hour block."
        )

    mismatched_counts = validation[
        validation["generated_block_count"] != validation["completed_block_count"]
    ]
    if not mismatched_counts.empty:
        examples = mismatched_counts["stay_id_global"].dropna().astype("string").tolist()[:10]
        raise ValueError(
            "Generated ASIC 8-hour block counts did not match the expected completed block "
            f"count for stays: {examples}"
        )

    mismatched_terminal_bounds = validation[
        validation["max_block_end_h"] != validation["terminal_block_end_h"]
    ]
    if not mismatched_terminal_bounds.empty:
        examples = (
            mismatched_terminal_bounds["stay_id_global"]
            .dropna()
            .astype("string")
            .tolist()[:10]
        )
        raise ValueError(
            "ASIC 8-hour block construction produced an unexpected terminal block end for "
            f"stays: {examples}"
        )

    constructed_blocks = validation[validation["generated_block_count"].ge(1)]
    invalid_terminal_bounds = constructed_blocks[
        constructed_blocks["max_block_end_h"] > constructed_blocks["icu_end_time_proxy_hours"]
    ]
    if not invalid_terminal_bounds.empty:
        examples = (
            invalid_terminal_bounds["stay_id_global"].dropna().astype("string").tolist()[:10]
        )
        raise ValueError(
            "ASIC 8-hour block construction created a block with block_end_h greater than "
            f"icu_end_time_proxy for stays: {examples}"
        )


def _build_block_count_distribution_by_hospital(stay_block_counts: pd.DataFrame) -> pd.DataFrame:
    if stay_block_counts.empty:
        return pd.DataFrame(columns=BLOCK_COUNT_DISTRIBUTION_COLUMNS)

    hospital_totals = (
        stay_block_counts.groupby("hospital_id", dropna=False)
        .size()
        .rename("hospital_stay_count")
        .reset_index()
    )
    distribution = (
        stay_block_counts.groupby(["hospital_id", "completed_block_count"], dropna=False)
        .size()
        .rename("stay_count")
        .reset_index()
        .merge(hospital_totals, on="hospital_id", how="left")
    )
    distribution["proportion_of_hospital_stays"] = (
        distribution["stay_count"] / distribution["hospital_stay_count"]
    )
    return distribution[BLOCK_COUNT_DISTRIBUTION_COLUMNS].sort_values(
        ["hospital_id", "completed_block_count"]
    ).reset_index(drop=True)


def _select_example_stays(
    stay_block_counts: pd.DataFrame,
    block_index: pd.DataFrame,
) -> pd.DataFrame:
    if stay_block_counts.empty:
        return pd.DataFrame(columns=EXAMPLE_STAY_COLUMNS)

    ordered_stays = stay_block_counts.sort_values(
        ["completed_block_count", "hospital_id", "stay_id_global"]
    ).reset_index(drop=True)
    selected_stay_ids: set[str] = set()
    selected_rows: list[dict[str, object]] = []

    def add_example(example_type: str, candidates: pd.DataFrame) -> None:
        for candidate in candidates.itertuples(index=False):
            stay_id = str(candidate.stay_id_global)
            if stay_id in selected_stay_ids:
                continue

            stay_blocks = block_index[block_index["stay_id_global"] == candidate.stay_id_global]
            block_boundaries = [
                f"[{int(block.block_start_h)}, {int(block.block_end_h)})"
                for block in stay_blocks.itertuples(index=False)
            ]
            selected_rows.append(
                {
                    "example_type": example_type,
                    "stay_id_global": candidate.stay_id_global,
                    "hospital_id": candidate.hospital_id,
                    "icu_end_time_proxy": candidate.icu_end_time_proxy,
                    "icu_end_time_proxy_hours": candidate.icu_end_time_proxy_hours,
                    "completed_block_count": candidate.completed_block_count,
                    "block_boundaries": block_boundaries,
                }
            )
            selected_stay_ids.add(stay_id)
            return

    add_example("zero_blocks", ordered_stays[ordered_stays["completed_block_count"].eq(0)])
    add_example(
        "exact_8h_boundary",
        ordered_stays[ordered_stays["ends_exactly_on_8h_boundary"]],
    )
    add_example("single_block", ordered_stays[ordered_stays["completed_block_count"].eq(1)])
    add_example("multiple_blocks", ordered_stays[ordered_stays["completed_block_count"].ge(2)])
    add_example(
        "max_block_count",
        stay_block_counts.sort_values(
            ["completed_block_count", "icu_end_time_proxy_hours", "hospital_id", "stay_id_global"],
            ascending=[False, False, True, True],
        ),
    )

    if len(selected_rows) < 5:
        for candidate in ordered_stays.itertuples(index=False):
            if len(selected_rows) >= 5:
                break
            stay_id = str(candidate.stay_id_global)
            if stay_id in selected_stay_ids:
                continue
            add_example("additional_example", pd.DataFrame([candidate._asdict()]))

    return pd.DataFrame(selected_rows)[EXAMPLE_STAY_COLUMNS].reset_index(drop=True)


def _build_qc_summary(
    stay_block_counts: pd.DataFrame,
    dynamic_rows_total: int,
    negative_dynamic_time_qc: pd.DataFrame,
    validation: pd.DataFrame,
    block_index: pd.DataFrame,
) -> pd.DataFrame:
    zero_block_count = int(stay_block_counts["completed_block_count"].eq(0).sum())
    one_plus_block_count = int(stay_block_counts["completed_block_count"].ge(1).sum())
    negative_dynamic_time_rows = int(
        negative_dynamic_time_qc["negative_dynamic_time_row_count"].sum()
    )

    constructed_blocks = validation[validation["generated_block_count"].ge(1)]
    no_block_end_exceeds_proxy = True
    if not constructed_blocks.empty:
        no_block_end_exceeds_proxy = bool(
            constructed_blocks["max_block_end_h"]
            .le(constructed_blocks["icu_end_time_proxy_hours"])
            .all()
        )

    exact_boundary_stays = validation[validation["ends_exactly_on_8h_boundary"]]
    exact_boundary_terminal_block_matches = True
    if not exact_boundary_stays.empty:
        exact_boundary_terminal_block_matches = bool(
            exact_boundary_stays["terminal_block_end_h"]
            .eq(exact_boundary_stays["icu_end_time_proxy_hours"])
            .all()
        )

    return pd.DataFrame(
        [
            {
                "metric": "block_size_hours",
                "value": BLOCK_SIZE_HOURS,
                "note": (
                    "Blocks are fixed, non-overlapping, and half-open: [0, 8), [8, 16), "
                    "[16, 24), ..."
                ),
            },
            {
                "metric": "stays_total",
                "value": int(stay_block_counts.shape[0]),
                "note": "All stay-level ASIC rows entering generic 8-hour block construction.",
            },
            {
                "metric": "dynamic_rows_total",
                "value": int(dynamic_rows_total),
                "note": "Harmonized dynamic rows for stays included in generic block construction.",
            },
            {
                "metric": "stays_with_zero_blocks",
                "value": zero_block_count,
                "note": (
                    "Stays with no fully completed 8-hour block because icu_end_time_proxy "
                    "< 8h or is unusable."
                ),
            },
            {
                "metric": "stays_with_one_or_more_blocks",
                "value": one_plus_block_count,
                "note": "Stays with at least one fully completed 8-hour block.",
            },
            {
                "metric": "block_rows_total",
                "value": int(block_index.shape[0]),
                "note": "Block-level index rows emitted for all ASIC stays.",
            },
            {
                "metric": "negative_dynamic_time_rows_in_input",
                "value": negative_dynamic_time_rows,
                "note": "Invalid input dynamic rows with time < 0 hours.",
            },
            {
                "metric": "negative_dynamic_time_stays_in_input",
                "value": int(negative_dynamic_time_qc.shape[0]),
                "note": "Stays containing one or more invalid negative dynamic times.",
            },
            {
                "metric": "all_constructed_blocks_end_on_or_before_icu_end_time_proxy",
                "value": int(no_block_end_exceeds_proxy),
                "note": (
                    "Confirms that only fully completed blocks with block_end_h <= "
                    "icu_end_time_proxy were constructed."
                ),
            },
            {
                "metric": "stays_ending_exactly_on_8h_boundary",
                "value": int(exact_boundary_stays.shape[0]),
                "note": "Stays whose icu_end_time_proxy lands exactly on an 8-hour boundary.",
            },
            {
                "metric": "exact_boundary_stays_have_terminal_block_end_equal_proxy",
                "value": int(exact_boundary_terminal_block_matches),
                "note": (
                    "Confirms that a stay ending exactly on an 8-hour boundary retains the "
                    "terminal completed block ending at that boundary."
                ),
            },
            {
                "metric": "block_index_is_generic_pre_analytic_structure",
                "value": 1,
                "note": (
                    "Generic block structure is emitted before any analysis-specific stay, "
                    "instance, feature, or label exclusions are applied."
                ),
            },
        ]
    )


def build_asic_8h_example_stays(
    stay_block_counts: pd.DataFrame,
    block_index: pd.DataFrame,
) -> pd.DataFrame:
    return _select_example_stays(stay_block_counts, block_index)


def build_asic_8h_qc_summary(
    stay_block_counts: pd.DataFrame,
    dynamic_rows_total: int,
    negative_dynamic_time_qc: pd.DataFrame,
    block_index: pd.DataFrame,
) -> pd.DataFrame:
    validation = _build_validation_table(stay_block_counts, block_index)
    return _build_qc_summary(
        stay_block_counts=stay_block_counts,
        dynamic_rows_total=dynamic_rows_total,
        negative_dynamic_time_qc=negative_dynamic_time_qc,
        validation=validation,
        block_index=block_index,
    )


def build_asic_8h_blocks(
    stay_level_df: pd.DataFrame,
    dynamic_df: pd.DataFrame,
) -> ASICBlockResult:
    stay_block_counts = _prepare_stays(stay_level_df)
    retained_dynamic = _dynamic_input(dynamic_df, stay_block_counts)
    negative_dynamic_time_qc = _build_negative_dynamic_time_qc(retained_dynamic)
    block_index = _build_block_index(stay_block_counts)
    blocked_dynamic_features = _build_blocked_dynamic_features(block_index, retained_dynamic)
    validation = _build_validation_table(stay_block_counts, block_index)
    _validate_block_index(validation, block_index)
    block_count_distribution_by_hospital = _build_block_count_distribution_by_hospital(
        stay_block_counts
    )
    example_stays = build_asic_8h_example_stays(stay_block_counts, block_index)
    qc_summary = build_asic_8h_qc_summary(
        stay_block_counts=stay_block_counts,
        dynamic_rows_total=int(retained_dynamic.shape[0]),
        negative_dynamic_time_qc=negative_dynamic_time_qc,
        block_index=block_index,
    )

    return ASICBlockResult(
        block_index=block_index,
        blocked_dynamic_features=blocked_dynamic_features,
        stay_block_counts=stay_block_counts,
        block_count_distribution_by_hospital=block_count_distribution_by_hospital,
        negative_dynamic_time_qc=negative_dynamic_time_qc,
        qc_summary=qc_summary,
        example_stays=example_stays,
    )
