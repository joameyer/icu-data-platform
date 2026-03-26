from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from icu_data_platform.analysis_seed.chapter1.cohort import (
    Chapter1CohortResult,
    build_chapter1_cohort,
)
from icu_data_platform.analysis_seed.chapter1.config import (
    Chapter1SeedConfig,
    build_chapter1_feature_set_definition,
    default_chapter1_seed_config,
)
from icu_data_platform.analysis_seed.chapter1.dataset import (
    Chapter1ModelReadyResult,
    build_chapter1_model_ready_dataset,
)
from icu_data_platform.analysis_seed.chapter1.instances import (
    Chapter1ValidInstanceResult,
    build_chapter1_valid_instances,
)
from icu_data_platform.analysis_seed.chapter1.io import (
    DEFAULT_CHAPTER1_SEED_OUTPUT_DIR,
    Chapter1SeedInputTables,
    load_chapter1_seed_inputs,
)
from icu_data_platform.analysis_seed.chapter1.labels import (
    Chapter1LabelResult,
    build_chapter1_terminal_labels,
)
from icu_data_platform.common.io import ensure_directory, write_dataframe
from icu_data_platform.sources.asic.pipeline import DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR


@dataclass(frozen=True)
class Chapter1SeedDataset:
    cohort: Chapter1CohortResult
    valid_instances: Chapter1ValidInstanceResult
    labels: Chapter1LabelResult
    model_ready: Chapter1ModelReadyResult
    feature_set_definition: pd.DataFrame


def build_chapter1_seed_dataset(
    inputs: Chapter1SeedInputTables,
    config: Chapter1SeedConfig | None = None,
) -> Chapter1SeedDataset:
    config = config or default_chapter1_seed_config()
    cohort = build_chapter1_cohort(
        static_harmonized=inputs.static_harmonized,
        dynamic_harmonized=inputs.dynamic_harmonized,
        stay_block_counts=inputs.stay_block_counts,
        config=config,
    )
    valid_instances = build_chapter1_valid_instances(
        retained_cohort=cohort.table,
        block_index=inputs.block_index,
        blocked_dynamic_features=inputs.blocked_dynamic_features,
        config=config,
    )
    labels = build_chapter1_terminal_labels(
        valid_instances=valid_instances.valid_instances,
        retained_cohort=cohort.table,
    )
    feature_set_definition = build_chapter1_feature_set_definition(
        inputs.blocked_dynamic_features,
        config=config,
    )
    model_ready = build_chapter1_model_ready_dataset(
        usable_labels=labels.usable_labels,
        blocked_dynamic_features=inputs.blocked_dynamic_features,
        feature_set_definition=feature_set_definition,
    )
    return Chapter1SeedDataset(
        cohort=cohort,
        valid_instances=valid_instances,
        labels=labels,
        model_ready=model_ready,
        feature_set_definition=feature_set_definition,
    )


def write_chapter1_seed_dataset(
    dataset: Chapter1SeedDataset,
    output_dir: Path = DEFAULT_CHAPTER1_SEED_OUTPUT_DIR,
    output_format: str = "csv",
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    output_paths: dict[str, Path] = {}

    cohort_dir = ensure_directory(output_dir / "cohort")
    instances_dir = ensure_directory(output_dir / "instances")
    labels_dir = ensure_directory(output_dir / "labels")
    model_ready_dir = ensure_directory(output_dir / "model_ready")

    cohort_outputs = {
        "chapter1_notes": dataset.cohort.notes,
        "chapter1_core_vital_group_coverage": dataset.cohort.core_vital_group_coverage,
        "chapter1_site_eligibility": dataset.cohort.site_eligibility,
        "chapter1_site_counts_summary": dataset.cohort.site_counts_summary,
        "chapter1_stay_exclusions": dataset.cohort.stay_exclusions,
        "chapter1_stay_exclusion_summary_by_hospital": (
            dataset.cohort.stay_exclusion_summary_by_hospital
        ),
        "chapter1_counts_by_hospital": dataset.cohort.counts_by_hospital,
        "chapter1_retained_hospitals": dataset.cohort.retained_hospitals,
        "chapter1_retained_stays": dataset.cohort.retained_stays,
        "chapter1_retained_stay_table": dataset.cohort.table,
    }
    instance_outputs = {
        "chapter1_candidate_instances": dataset.valid_instances.candidate_instances,
        "chapter1_valid_instances": dataset.valid_instances.valid_instances,
        "chapter1_instance_counts_by_horizon": dataset.valid_instances.counts_by_horizon,
        "chapter1_instance_exclusion_summary": dataset.valid_instances.exclusion_summary,
    }
    label_outputs = {
        "chapter1_terminal_icu_mortality_labels": dataset.labels.labels,
        "chapter1_terminal_icu_mortality_usable_labels": dataset.labels.usable_labels,
        "chapter1_terminal_icu_mortality_label_summary_by_horizon": (
            dataset.labels.summary_by_horizon
        ),
        "chapter1_label_notes": dataset.labels.notes,
    }
    model_ready_outputs = {
        "chapter1_feature_set": dataset.feature_set_definition,
        "chapter1_model_ready_dataset": dataset.model_ready.table,
        "chapter1_readiness_summary": dataset.model_ready.readiness_summary,
        "chapter1_feature_availability_by_horizon": (
            dataset.model_ready.feature_availability_by_horizon
        ),
    }

    for name, df in cohort_outputs.items():
        path = cohort_dir / f"{name}.{extension}"
        output_paths[f"cohort_{name}"] = write_dataframe(df, path, output_format=output_format)
    for name, df in instance_outputs.items():
        path = instances_dir / f"{name}.{extension}"
        output_paths[f"instances_{name}"] = write_dataframe(df, path, output_format=output_format)
    for name, df in label_outputs.items():
        path = labels_dir / f"{name}.{extension}"
        output_paths[f"labels_{name}"] = write_dataframe(df, path, output_format=output_format)
    for name, df in model_ready_outputs.items():
        path = model_ready_dir / f"{name}.{extension}"
        output_paths[f"model_ready_{name}"] = write_dataframe(
            df,
            path,
            output_format=output_format,
        )

    return output_paths


def build_and_write_chapter1_seed_dataset(
    input_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_dir: Path = DEFAULT_CHAPTER1_SEED_OUTPUT_DIR,
    input_format: str = "csv",
    output_format: str = "csv",
    config: Chapter1SeedConfig | None = None,
) -> tuple[Chapter1SeedDataset, dict[str, Path]]:
    inputs = load_chapter1_seed_inputs(input_dir=input_dir, input_format=input_format)
    dataset = build_chapter1_seed_dataset(inputs, config=config)
    output_paths = write_chapter1_seed_dataset(
        dataset,
        output_dir=output_dir,
        output_format=output_format,
    )
    return dataset, output_paths
