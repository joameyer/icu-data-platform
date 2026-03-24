from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from icu_data_platform.common.io import ensure_directory, write_dataframe
from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
)
from icu_data_platform.sources.asic.harmonize.dynamic import (
    HarmonizedDynamicResult,
    harmonize_dynamic_tables,
)
from icu_data_platform.sources.asic.harmonize.static import (
    HarmonizedStaticResult,
    harmonize_static_tables,
)
from icu_data_platform.sources.asic.qc.stay_id import (
    ASICStayIdQCResult,
    assert_valid_asic_stay_ids,
    build_asic_stay_id_qc,
)

DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR = Path("artifacts") / "asic_harmonized"


@dataclass(frozen=True)
class ASICHarmonizedDataset:
    static: HarmonizedStaticResult
    dynamic: HarmonizedDynamicResult
    stay_id_qc: ASICStayIdQCResult


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
    return ASICHarmonizedDataset(
        static=static_result,
        dynamic=dynamic_result,
        stay_id_qc=stay_id_qc,
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
