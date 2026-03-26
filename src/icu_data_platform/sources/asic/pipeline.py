from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from icu_data_platform.common.io import ensure_directory, write_dataframe
from icu_data_platform.sources.asic.blocking import (
    ASICBlockResult,
    build_asic_8h_blocks,
)
from icu_data_platform.sources.asic.blocks import (
    ASICChapter1BlockResult,
    build_asic_chapter1_8h_blocks,
)
from icu_data_platform.sources.asic.cohort import (
    ASICStayLevelCohortResult,
    build_asic_stay_level_cohort,
)
from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
    load_static_tables,
)
from icu_data_platform.sources.asic.harmonize.dynamic import (
    HarmonizedDynamicResult,
    harmonize_dynamic_tables,
)
from icu_data_platform.sources.asic.harmonize.static import (
    HarmonizedStaticResult,
    harmonize_static_tables,
)
from icu_data_platform.sources.asic.stay_level import (
    ASICStayLevelResult,
    build_asic_stay_level_table,
)
from icu_data_platform.sources.asic.qc.stay_id import (
    ASICStayIdQCResult,
    assert_valid_asic_stay_ids,
    build_asic_stay_id_qc,
)
from icu_data_platform.sources.asic.qc.mech_ventilation import (
    ASICMechanicalVentilationQCResult,
    build_asic_mech_vent_ge_24h_qc,
)

DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR = Path("artifacts") / "asic_harmonized"


@dataclass(frozen=True)
class ASICHarmonizedDataset:
    static: HarmonizedStaticResult
    dynamic: HarmonizedDynamicResult
    stay_id_qc: ASICStayIdQCResult
    mech_vent_ge_24h_qc: ASICMechanicalVentilationQCResult


@dataclass(frozen=True)
class ASICStandardizedDataset:
    stay_level: ASICStayLevelResult
    blocked_8h: ASICBlockResult


@dataclass(frozen=True)
class ASICChapter1Dataset:
    cohort: ASICStayLevelCohortResult
    chapter1_8h_blocks: ASICChapter1BlockResult


def build_asic_harmonized_dataset(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
    min_non_null: int = 20,
    min_hospitals: int = 4,
    fence_factor: float = 1.5,
) -> ASICHarmonizedDataset:
    static_result = harmonize_static_tables(
        raw_dir=raw_dir,
        translation_path=translation_path,
    )
    dynamic_result = harmonize_dynamic_tables(
        raw_dir=raw_dir,
        translation_path=translation_path,
        min_non_null=min_non_null,
        min_hospitals=min_hospitals,
        fence_factor=fence_factor,
    )
    stay_id_qc = build_asic_stay_id_qc(
        static_df=static_result.combined,
        dynamic_df=dynamic_result.combined,
    )
    assert_valid_asic_stay_ids(stay_id_qc)
    mech_vent_ge_24h_qc = build_asic_mech_vent_ge_24h_qc(dynamic_result.combined)
    return ASICHarmonizedDataset(
        static=static_result,
        dynamic=dynamic_result,
        stay_id_qc=stay_id_qc,
        mech_vent_ge_24h_qc=mech_vent_ge_24h_qc,
    )


def build_asic_chapter1_dataset(
    harmonized_dataset: ASICHarmonizedDataset,
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
) -> ASICChapter1Dataset:
    raw_static_tables = load_static_tables(raw_dir)
    cohort = build_asic_stay_level_cohort(
        static_df=harmonized_dataset.static.combined,
        dynamic_df=harmonized_dataset.dynamic.combined,
        static_source_map=harmonized_dataset.static.source_map,
        raw_static_tables=raw_static_tables,
    )
    chapter1_8h_blocks = build_asic_chapter1_8h_blocks(
        chapter1_cohort_df=cohort.chapter1.table,
        dynamic_df=harmonized_dataset.dynamic.combined,
    )
    return ASICChapter1Dataset(
        cohort=cohort,
        chapter1_8h_blocks=chapter1_8h_blocks,
    )


def build_asic_standardized_dataset(
    harmonized_dataset: ASICHarmonizedDataset,
) -> ASICStandardizedDataset:
    stay_level = build_asic_stay_level_table(
        static_df=harmonized_dataset.static.combined,
        dynamic_df=harmonized_dataset.dynamic.combined,
    )
    blocked_8h = build_asic_8h_blocks(
        stay_level_df=stay_level.table,
        dynamic_df=harmonized_dataset.dynamic.combined,
    )
    return ASICStandardizedDataset(
        stay_level=stay_level,
        blocked_8h=blocked_8h,
    )


def write_asic_harmonized_dataset(
    dataset: ASICHarmonizedDataset,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    output_paths: dict[str, Path] = {}

    static_dir = ensure_directory(output_dir / "static")
    dynamic_dir = ensure_directory(output_dir / "dynamic")
    qc_dir = ensure_directory(output_dir / "qc")

    static_outputs = {
        "harmonized": dataset.static.combined,
        "source_map": dataset.static.source_map,
        "schema_summary": dataset.static.schema_summary,
        "categorical_value_summary": dataset.static.categorical_value_summary,
    }
    dynamic_outputs = {
        "harmonized": dataset.dynamic.combined,
        "source_map": dataset.dynamic.source_map,
        "schema_summary": dataset.dynamic.schema_summary,
        "non_numeric_issues": dataset.dynamic.non_numeric_issues,
        "semantic_decisions": dataset.dynamic.semantic_decisions,
        "invalid_value_rules": dataset.dynamic.invalid_value_rules,
        "invalid_value_qc": dataset.dynamic.invalid_value_qc,
        "distribution_summary": dataset.dynamic.distribution_summary,
        "distribution_issues": dataset.dynamic.distribution_issues,
    }
    qc_outputs = {
        "stay_id_summary": dataset.stay_id_qc.summary,
        "stay_id_unique_stays_per_hospital": dataset.stay_id_qc.unique_stays_per_hospital,
        "stay_id_local_id_collisions": dataset.stay_id_qc.duplicated_local_ids_across_hospitals,
        "stay_id_missing_id_values": dataset.stay_id_qc.missing_id_values,
        "stay_id_duplicate_static_global_ids": dataset.stay_id_qc.static_duplicate_global_ids,
        "stay_id_mapping_failures": dataset.stay_id_qc.mapping_failures,
        "stay_id_duplicate_dynamic_time_index": dataset.stay_id_qc.duplicate_dynamic_time_index,
        "mech_vent_ge_24h_stay_level": dataset.mech_vent_ge_24h_qc.stay_level,
        "mech_vent_ge_24h_episode_level": dataset.mech_vent_ge_24h_qc.episode_level,
        "mech_vent_ge_24h_hospital_summary": dataset.mech_vent_ge_24h_qc.hospital_summary,
        "mech_vent_ge_24h_failed_stays": dataset.mech_vent_ge_24h_qc.failed_stays,
        "mech_vent_ge_24h_documentation": dataset.mech_vent_ge_24h_qc.documentation,
    }

    for name, df in static_outputs.items():
        path = static_dir / f"{name}.{extension}"
        output_paths[f"static_{name}"] = write_dataframe(df, path, output_format=output_format)

    for name, df in dynamic_outputs.items():
        path = dynamic_dir / f"{name}.{extension}"
        output_paths[f"dynamic_{name}"] = write_dataframe(df, path, output_format=output_format)

    for name, df in qc_outputs.items():
        path = qc_dir / f"{name}.{extension}"
        output_paths[f"qc_{name}"] = write_dataframe(df, path, output_format=output_format)

    return output_paths


def build_and_write_asic_harmonized_dataset(
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
    translation_path: Path = DEFAULT_TRANSLATION_PATH,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
    min_non_null: int = 20,
    min_hospitals: int = 4,
    fence_factor: float = 1.5,
) -> tuple[ASICHarmonizedDataset, dict[str, Path]]:
    dataset = build_asic_harmonized_dataset(
        raw_dir=raw_dir,
        translation_path=translation_path,
        min_non_null=min_non_null,
        min_hospitals=min_hospitals,
        fence_factor=fence_factor,
    )
    output_paths = write_asic_harmonized_dataset(
        dataset,
        output_dir=output_dir,
        output_format=output_format,
    )
    return dataset, output_paths


def write_asic_standardized_dataset(
    dataset: ASICStandardizedDataset,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    output_paths: dict[str, Path] = {}

    cohort_dir = ensure_directory(output_dir / "cohort")
    blocked_dir = ensure_directory(output_dir / "blocked")

    cohort_outputs = {
        "stay_level": dataset.stay_level.table,
        "summary": dataset.stay_level.summary,
        "preprocessing_notes": dataset.stay_level.preprocessing_notes,
        "icu_end_time_proxy_summary_by_hospital": (
            dataset.stay_level.icu_end_time_proxy_summary_by_hospital
        ),
        "coding_distribution_by_hospital": dataset.stay_level.coding_distribution_by_hospital,
    }
    blocked_outputs = {
        "asic_8h_block_index": dataset.blocked_8h.block_index,
        "asic_8h_blocked_dynamic_features": dataset.blocked_8h.blocked_dynamic_features,
        "asic_8h_stay_block_counts": dataset.blocked_8h.stay_block_counts,
        "asic_8h_block_count_distribution_by_hospital": (
            dataset.blocked_8h.block_count_distribution_by_hospital
        ),
        "asic_8h_negative_dynamic_time_qc": dataset.blocked_8h.negative_dynamic_time_qc,
        "asic_8h_qc_summary": dataset.blocked_8h.qc_summary,
        "asic_8h_example_stays": dataset.blocked_8h.example_stays,
    }

    for name, df in cohort_outputs.items():
        path = cohort_dir / f"{name}.{extension}"
        output_paths[f"cohort_{name}"] = write_dataframe(df, path, output_format=output_format)

    for name, df in blocked_outputs.items():
        path = blocked_dir / f"{name}.{extension}"
        output_paths[f"blocked_{name}"] = write_dataframe(df, path, output_format=output_format)

    return output_paths


def build_and_write_asic_standardized_dataset(
    harmonized_dataset: ASICHarmonizedDataset,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
) -> tuple[ASICStandardizedDataset, dict[str, Path]]:
    dataset = build_asic_standardized_dataset(harmonized_dataset)
    output_paths = write_asic_standardized_dataset(
        dataset,
        output_dir=output_dir,
        output_format=output_format,
    )
    return dataset, output_paths


def write_asic_chapter1_dataset(
    dataset: ASICChapter1Dataset,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    output_paths: dict[str, Path] = {}

    cohort_dir = ensure_directory(output_dir / "cohort")
    blocks_dir = ensure_directory(output_dir / "blocks")

    cohort_outputs = {
        "stay_level": dataset.cohort.table,
        "summary": dataset.cohort.summary,
        "preprocessing_notes": dataset.cohort.preprocessing_notes,
        "icu_end_time_proxy_summary_by_hospital": (
            dataset.cohort.icu_end_time_proxy_summary_by_hospital
        ),
        "coding_distribution_by_hospital": dataset.cohort.coding_distribution_by_hospital,
        "chapter1_stay_level": dataset.cohort.chapter1.table,
        "chapter1_notes": dataset.cohort.chapter1.notes,
        "chapter1_core_vital_group_coverage": dataset.cohort.chapter1.core_vital_group_coverage,
        "chapter1_site_eligibility": dataset.cohort.chapter1.site_eligibility,
        "chapter1_site_counts_summary": dataset.cohort.chapter1.site_counts_summary,
        "chapter1_stay_exclusions": dataset.cohort.chapter1.stay_exclusions,
        "chapter1_stay_exclusion_summary_by_hospital": (
            dataset.cohort.chapter1.stay_exclusion_summary_by_hospital
        ),
        "chapter1_counts_by_hospital": dataset.cohort.chapter1.counts_by_hospital,
        "chapter1_retained_hospitals": dataset.cohort.chapter1.retained_hospitals,
        "chapter1_retained_stays": dataset.cohort.chapter1.retained_stays,
    }
    blocks_outputs = {
        "chapter1_8h_block_index": dataset.chapter1_8h_blocks.block_index,
        "chapter1_8h_blocked_dynamic_features": (
            dataset.chapter1_8h_blocks.blocked_dynamic_features
        ),
        "chapter1_8h_stay_block_counts": dataset.chapter1_8h_blocks.stay_block_counts,
        "chapter1_8h_block_count_distribution_by_hospital": (
            dataset.chapter1_8h_blocks.block_count_distribution_by_hospital
        ),
        "chapter1_8h_negative_dynamic_time_qc": (
            dataset.chapter1_8h_blocks.negative_dynamic_time_qc
        ),
        "chapter1_8h_qc_summary": dataset.chapter1_8h_blocks.qc_summary,
        "chapter1_8h_example_stays": dataset.chapter1_8h_blocks.example_stays,
    }

    for name, df in cohort_outputs.items():
        path = cohort_dir / f"{name}.{extension}"
        output_paths[f"cohort_{name}"] = write_dataframe(df, path, output_format=output_format)

    for name, df in blocks_outputs.items():
        path = blocks_dir / f"{name}.{extension}"
        output_paths[f"blocks_{name}"] = write_dataframe(df, path, output_format=output_format)

    return output_paths


def build_and_write_asic_chapter1_dataset(
    harmonized_dataset: ASICHarmonizedDataset,
    raw_dir: Path = DEFAULT_ASIC_RAW_DATA_DIR,
    output_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_format: str = "csv",
) -> tuple[ASICChapter1Dataset, dict[str, Path]]:
    dataset = build_asic_chapter1_dataset(
        harmonized_dataset,
        raw_dir=raw_dir,
    )
    output_paths = write_asic_chapter1_dataset(
        dataset,
        output_dir=output_dir,
        output_format=output_format,
    )
    return dataset, output_paths
