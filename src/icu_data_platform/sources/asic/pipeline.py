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


@dataclass(frozen=True)
class ASICHarmonizedDataset:
    static: HarmonizedStaticResult
    dynamic: HarmonizedDynamicResult


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
    return ASICHarmonizedDataset(static=static_result, dynamic=dynamic_result)


def write_asic_harmonized_dataset(
    dataset: ASICHarmonizedDataset,
    output_dir: Path,
    output_format: str = "csv",
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    output_paths: dict[str, Path] = {}

    static_dir = ensure_directory(output_dir / "static")
    dynamic_dir = ensure_directory(output_dir / "dynamic")

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
        "distribution_summary": dataset.dynamic.distribution_summary,
        "distribution_issues": dataset.dynamic.distribution_issues,
    }

    for name, df in static_outputs.items():
        path = static_dir / f"{name}.{extension}"
        output_paths[f"static_{name}"] = write_dataframe(df, path, output_format=output_format)

    for name, df in dynamic_outputs.items():
        path = dynamic_dir / f"{name}.{extension}"
        output_paths[f"dynamic_{name}"] = write_dataframe(df, path, output_format=output_format)

    return output_paths
