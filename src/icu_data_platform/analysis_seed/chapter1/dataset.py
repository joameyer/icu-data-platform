from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Chapter1ModelReadyResult:
    table: pd.DataFrame
    readiness_summary: pd.DataFrame
    feature_availability_by_horizon: pd.DataFrame


def build_chapter1_model_ready_dataset(
    usable_labels: pd.DataFrame,
    blocked_dynamic_features: pd.DataFrame,
    feature_set_definition: pd.DataFrame,
) -> Chapter1ModelReadyResult:
    selected_feature_columns = (
        feature_set_definition.loc[feature_set_definition["selected_for_model"], "feature_name"]
        .astype("string")
        .tolist()
    )

    feature_frame = blocked_dynamic_features[
        [
            "stay_id_global",
            "hospital_id",
            "block_index",
            "block_start_h",
            "block_end_h",
            "prediction_time_h",
            *[column for column in selected_feature_columns if column in blocked_dynamic_features.columns],
        ]
    ].copy()

    model_ready = usable_labels.merge(
        feature_frame,
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

    readiness_summary = pd.DataFrame(
        [
            {
                "metric": "model_ready_rows_total",
                "value": int(model_ready.shape[0]),
                "note": "Rows after valid-instance selection and label availability filtering.",
            },
            {
                "metric": "selected_feature_columns_total",
                "value": int(len(selected_feature_columns)),
                "note": "Blocked dynamic feature columns selected by the Chapter 1 feature config.",
            },
            {
                "metric": "distinct_horizons_total",
                "value": int(model_ready["horizon_h"].nunique(dropna=True)),
                "note": "Configured prediction horizons represented in the model-ready table.",
            },
        ]
    )

    feature_availability_rows = []
    for horizon_h, horizon_df in model_ready.groupby("horizon_h", dropna=False):
        horizon_size = int(horizon_df.shape[0])
        for feature_name in selected_feature_columns:
            non_missing_count = int(horizon_df[feature_name].notna().sum())
            feature_availability_rows.append(
                {
                    "horizon_h": int(horizon_h),
                    "feature_name": feature_name,
                    "non_missing_count": non_missing_count,
                    "rows_in_horizon": horizon_size,
                    "non_missing_proportion": (
                        float(non_missing_count / horizon_size) if horizon_size else pd.NA
                    ),
                }
            )

    if feature_availability_rows:
        feature_availability_by_horizon = pd.DataFrame(feature_availability_rows).sort_values(
            ["horizon_h", "feature_name"]
        ).reset_index(drop=True)
    else:
        feature_availability_by_horizon = pd.DataFrame(
            columns=[
                "horizon_h",
                "feature_name",
                "non_missing_count",
                "rows_in_horizon",
                "non_missing_proportion",
            ]
        )

    return Chapter1ModelReadyResult(
        table=model_ready,
        readiness_summary=readiness_summary,
        feature_availability_by_horizon=feature_availability_by_horizon,
    )
