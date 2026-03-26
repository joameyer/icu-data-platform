from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Chapter1SeedConfig:
    core_vital_variables: tuple[str, ...]
    optional_variables: tuple[str, ...]
    feature_statistics: tuple[str, ...]
    horizons_hours: tuple[int, ...]
    min_required_core_groups: int = 3


def default_chapter1_seed_config() -> Chapter1SeedConfig:
    return Chapter1SeedConfig(
        core_vital_variables=(
            "heart_rate",
            "map",
            "sbp",
            "dbp",
            "resp_rate",
            "spo2",
            "sao2",
        ),
        optional_variables=("core_temp",),
        feature_statistics=("obs_count", "mean", "median", "min", "max", "last"),
        horizons_hours=(8, 24),
    )


def chapter1_group_definitions() -> dict[str, tuple[str, ...]]:
    return {
        "cardiac_rate": ("heart_rate",),
        "blood_pressure": ("map", "sbp", "dbp"),
        "respiratory": ("resp_rate",),
        "oxygenation": ("spo2", "sao2"),
        "core_temp_optional": ("core_temp",),
    }


def selected_chapter1_feature_columns(
    blocked_dynamic_features: pd.DataFrame,
    config: Chapter1SeedConfig,
) -> list[str]:
    selected: list[str] = []
    variables = (*config.core_vital_variables, *config.optional_variables)
    available_columns = set(blocked_dynamic_features.columns)
    for variable in variables:
        for statistic in config.feature_statistics:
            candidate = f"{variable}_{statistic}"
            if candidate in available_columns:
                selected.append(candidate)
    return selected


def build_chapter1_feature_set_definition(
    blocked_dynamic_features: pd.DataFrame,
    config: Chapter1SeedConfig,
) -> pd.DataFrame:
    group_lookup = {}
    for group_name, variables in chapter1_group_definitions().items():
        for variable in variables:
            group_lookup[variable] = group_name

    rows = []
    available_columns = set(blocked_dynamic_features.columns)
    for variable in (*config.core_vital_variables, *config.optional_variables):
        for statistic in config.feature_statistics:
            feature_name = f"{variable}_{statistic}"
            rows.append(
                {
                    "feature_name": feature_name,
                    "base_variable": variable,
                    "statistic": statistic,
                    "physiologic_group": group_lookup.get(variable, pd.NA),
                    "required_for_site_inclusion": variable in config.core_vital_variables,
                    "selected_for_model": feature_name in available_columns,
                }
            )

    return pd.DataFrame(rows)
