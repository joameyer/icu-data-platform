from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Chapter1LabelResult:
    labels: pd.DataFrame
    usable_labels: pd.DataFrame
    summary_by_horizon: pd.DataFrame
    notes: pd.DataFrame


def build_chapter1_terminal_labels(
    valid_instances: pd.DataFrame,
    retained_cohort: pd.DataFrame,
) -> Chapter1LabelResult:
    label_lookup = retained_cohort[
        ["stay_id_global", "hospital_id", "icu_mortality"]
    ].copy()
    label_lookup["stay_id_global"] = label_lookup["stay_id_global"].astype("string")
    label_lookup["hospital_id"] = label_lookup["hospital_id"].astype("string")
    label_lookup["icu_mortality"] = pd.to_numeric(label_lookup["icu_mortality"], errors="coerce")

    label_columns = list(valid_instances.columns) + [
        "label_name",
        "label_value",
        "label_available",
        "label_semantics",
    ]

    labels = valid_instances.merge(
        label_lookup,
        on=["stay_id_global", "hospital_id"],
        how="left",
    )
    labels["label_name"] = "terminal_icu_mortality"
    labels["label_value"] = labels["icu_mortality"]
    labels["label_available"] = labels["label_value"].notna()
    labels["label_semantics"] = (
        "Stay-level ICU mortality reused across horizons; true event-time horizon labels "
        "are not available in the standardized ASIC artifacts."
    )
    labels = labels.drop(columns=["icu_mortality"])

    labels = labels[label_columns]
    usable_labels = labels[labels["label_available"]].reset_index(drop=True)

    if labels.empty:
        summary_by_horizon = pd.DataFrame(
            columns=[
                "horizon_h",
                "valid_instances",
                "labeled_instances",
                "positive_labels",
                "negative_labels",
                "missing_label_instances",
            ]
        )
    else:
        summary_by_horizon = (
            labels.groupby("horizon_h", dropna=False)
            .agg(
                valid_instances=("instance_id", "size"),
                labeled_instances=("label_available", "sum"),
                positive_labels=("label_value", lambda series: int(series.fillna(-1).eq(1).sum())),
                negative_labels=("label_value", lambda series: int(series.fillna(-1).eq(0).sum())),
            )
            .reset_index()
        )
        summary_by_horizon["missing_label_instances"] = (
            summary_by_horizon["valid_instances"] - summary_by_horizon["labeled_instances"]
        )

    notes = pd.DataFrame(
        [
            {
                "note_id": "terminal_label_only",
                "category": "label_semantics",
                "note": (
                    "The portable seed emits terminal ICU mortality labels for valid instances. "
                    "Because no event timestamp is available upstream, these labels are not "
                    "true within-horizon event labels."
                ),
            }
        ]
    )

    return Chapter1LabelResult(
        labels=labels,
        usable_labels=usable_labels,
        summary_by_horizon=summary_by_horizon,
        notes=notes,
    )
