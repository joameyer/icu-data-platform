from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from icu_data_platform.analysis_seed.chapter1.config import (
    Chapter1SeedConfig,
    build_chapter1_feature_set_definition,
    default_chapter1_seed_config,
)


@dataclass(frozen=True)
class Chapter1ValidInstanceResult:
    candidate_instances: pd.DataFrame
    valid_instances: pd.DataFrame
    counts_by_horizon: pd.DataFrame
    exclusion_summary: pd.DataFrame


def _feature_obs_count_columns(feature_set_definition: pd.DataFrame) -> list[str]:
    return (
        feature_set_definition.loc[
            feature_set_definition["statistic"].eq("obs_count")
            & feature_set_definition["selected_for_model"],
            "feature_name",
        ]
        .astype("string")
        .tolist()
    )


def build_chapter1_valid_instances(
    retained_cohort: pd.DataFrame,
    block_index: pd.DataFrame,
    blocked_dynamic_features: pd.DataFrame,
    config: Chapter1SeedConfig | None = None,
) -> Chapter1ValidInstanceResult:
    config = config or default_chapter1_seed_config()
    feature_set_definition = build_chapter1_feature_set_definition(
        blocked_dynamic_features,
        config=config,
    )
    obs_count_columns = [
        column
        for column in _feature_obs_count_columns(feature_set_definition)
        if column in blocked_dynamic_features.columns
    ]

    retained_stays = retained_cohort[
        ["stay_id_global", "hospital_id", "icu_end_time_proxy_hours"]
    ].copy()
    retained_stays["stay_id_global"] = retained_stays["stay_id_global"].astype("string")
    retained_stays["hospital_id"] = retained_stays["hospital_id"].astype("string")

    retained_blocks = block_index.merge(
        retained_stays,
        on=["stay_id_global", "hospital_id"],
        how="inner",
    ).merge(
        blocked_dynamic_features,
        on=[
            "stay_id_global",
            "hospital_id",
            "block_index",
            "block_start_h",
            "block_end_h",
            "prediction_time_h",
        ],
        how="left",
    )

    if obs_count_columns:
        retained_blocks["chapter1_feature_obs_count_in_block"] = (
            retained_blocks[obs_count_columns]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum(axis=1)
            .astype("Int64")
        )
    else:
        retained_blocks["chapter1_feature_obs_count_in_block"] = pd.Series(
            0,
            index=retained_blocks.index,
            dtype="Int64",
        )

    retained_blocks["has_chapter1_feature_data_in_block"] = retained_blocks[
        "chapter1_feature_obs_count_in_block"
    ].gt(0)

    rows = []
    for block in retained_blocks.itertuples(index=False):
        for horizon_h in config.horizons_hours:
            future_window_end_h = int(block.prediction_time_h) + int(horizon_h)
            has_full_future_horizon = (
                pd.notna(block.icu_end_time_proxy_hours)
                and float(block.icu_end_time_proxy_hours) >= float(future_window_end_h)
            )
            valid_instance = bool(
                block.has_chapter1_feature_data_in_block and has_full_future_horizon
            )
            exclusion_reason = pd.NA
            if not block.has_chapter1_feature_data_in_block:
                exclusion_reason = "no_chapter1_feature_data_in_block"
            elif not has_full_future_horizon:
                exclusion_reason = "insufficient_future_followup"

            rows.append(
                {
                    "instance_id": (
                        f"{block.stay_id_global}__b{int(block.block_index)}__h{int(horizon_h)}"
                    ),
                    "stay_id_global": block.stay_id_global,
                    "hospital_id": block.hospital_id,
                    "block_index": int(block.block_index),
                    "block_start_h": int(block.block_start_h),
                    "block_end_h": int(block.block_end_h),
                    "prediction_time_h": int(block.prediction_time_h),
                    "horizon_h": int(horizon_h),
                    "future_window_end_h": int(future_window_end_h),
                    "icu_end_time_proxy_hours": block.icu_end_time_proxy_hours,
                    "chapter1_feature_obs_count_in_block": int(
                        block.chapter1_feature_obs_count_in_block
                    ),
                    "has_chapter1_feature_data_in_block": bool(
                        block.has_chapter1_feature_data_in_block
                    ),
                    "has_full_future_horizon": bool(has_full_future_horizon),
                    "valid_instance": valid_instance,
                    "exclusion_reason": exclusion_reason,
                }
            )

    candidate_columns = [
        "instance_id",
        "stay_id_global",
        "hospital_id",
        "block_index",
        "block_start_h",
        "block_end_h",
        "prediction_time_h",
        "horizon_h",
        "future_window_end_h",
        "icu_end_time_proxy_hours",
        "chapter1_feature_obs_count_in_block",
        "has_chapter1_feature_data_in_block",
        "has_full_future_horizon",
        "valid_instance",
        "exclusion_reason",
    ]
    if rows:
        candidate_instances = pd.DataFrame(rows)[candidate_columns].sort_values(
            ["hospital_id", "stay_id_global", "block_index", "horizon_h"]
        ).reset_index(drop=True)
    else:
        candidate_instances = pd.DataFrame(columns=candidate_columns)
    valid_instances = candidate_instances[candidate_instances["valid_instance"]].reset_index(
        drop=True
    )

    if candidate_instances.empty:
        counts_by_horizon = pd.DataFrame(
            columns=["horizon_h", "candidate_instances", "valid_instances", "excluded_instances"]
        )
        exclusion_summary = pd.DataFrame(
            columns=["horizon_h", "exclusion_reason", "instance_count"]
        )
    else:
        counts_by_horizon = (
            candidate_instances.groupby("horizon_h", dropna=False)["valid_instance"]
            .agg(candidate_instances="size", valid_instances="sum")
            .reset_index()
        )
        counts_by_horizon["excluded_instances"] = (
            counts_by_horizon["candidate_instances"] - counts_by_horizon["valid_instances"]
        )

        exclusion_summary = (
            candidate_instances.loc[~candidate_instances["valid_instance"]]
            .groupby(["horizon_h", "exclusion_reason"], dropna=False)
            .size()
            .rename("instance_count")
            .reset_index()
            .sort_values(["horizon_h", "instance_count"], ascending=[True, False])
            .reset_index(drop=True)
        )

    return Chapter1ValidInstanceResult(
        candidate_instances=candidate_instances,
        valid_instances=valid_instances,
        counts_by_horizon=counts_by_horizon,
        exclusion_summary=exclusion_summary,
    )
