from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from icu_data_platform.common.io import (
    append_dataframe_csv,
    ensure_directory,
    read_dataframe,
    write_dataframe,
)
from icu_data_platform.sources.asic.blocking import (
    ASICBlockResult,
    build_asic_8h_blocks,
    build_asic_8h_example_stays,
    build_asic_8h_qc_summary,
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
    build_asic_stay_level_table_from_dynamic_end_time_proxy,
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


def _harmonized_artifact_path(
    input_dir: Path,
    section: str,
    name: str,
    input_format: str,
) -> Path:
    extension = "csv" if input_format == "csv" else "parquet"
    return input_dir / section / f"{name}.{extension}"


def _dynamic_end_time_proxy_from_lookup(
    max_time_by_stay: dict[str, pd.Timedelta],
) -> pd.DataFrame:
    if not max_time_by_stay:
        return pd.DataFrame(
            columns=[
                "stay_id_global",
                "icu_end_time_proxy",
                "icu_end_time_proxy_hours",
            ]
        )

    stay_ids = pd.Series(list(max_time_by_stay.keys()), dtype="string")
    timedeltas = pd.Series(list(max_time_by_stay.values()))
    dynamic_end_time_proxy = pd.DataFrame(
        {
            "stay_id_global": stay_ids,
            "icu_end_time_proxy_timedelta": timedeltas,
        }
    )
    dynamic_end_time_proxy["icu_end_time_proxy"] = dynamic_end_time_proxy[
        "icu_end_time_proxy_timedelta"
    ].astype("string")
    dynamic_end_time_proxy["icu_end_time_proxy_hours"] = (
        dynamic_end_time_proxy["icu_end_time_proxy_timedelta"].dt.total_seconds() / 3600.0
    )
    return dynamic_end_time_proxy[
        ["stay_id_global", "icu_end_time_proxy", "icu_end_time_proxy_hours"]
    ]


def _partition_harmonized_dynamic_csv_by_hospital(
    dynamic_path: Path,
    temp_dir: Path,
    stay_ids: set[str],
    dynamic_chunksize: int,
) -> tuple[dict[str, Path], pd.DataFrame, int, list[str]]:
    hospital_paths: dict[str, Path] = {}
    max_time_by_stay: dict[str, pd.Timedelta] = {}
    dynamic_rows_total = 0
    dynamic_columns = pd.read_csv(dynamic_path, nrows=0).columns.tolist()

    for chunk in pd.read_csv(dynamic_path, chunksize=dynamic_chunksize):
        chunk_stay_ids = chunk["stay_id_global"].astype("string")
        retain_mask = chunk_stay_ids.isin(stay_ids)
        if not retain_mask.any():
            continue

        retained_chunk = chunk.loc[retain_mask].copy()
        retained_chunk["stay_id_global"] = chunk_stay_ids.loc[retain_mask]
        dynamic_rows_total += int(retained_chunk.shape[0])

        parsed_time = pd.to_timedelta(retained_chunk["time"], errors="coerce")
        valid_time_mask = retained_chunk["stay_id_global"].notna() & parsed_time.notna()
        if valid_time_mask.any():
            time_summary = (
                pd.DataFrame(
                    {
                        "stay_id_global": retained_chunk.loc[
                            valid_time_mask,
                            "stay_id_global",
                        ],
                        "parsed_time": parsed_time.loc[valid_time_mask],
                    }
                )
                .groupby("stay_id_global", dropna=False)["parsed_time"]
                .max()
            )
            for stay_id, parsed in time_summary.items():
                stay_key = str(stay_id)
                current = max_time_by_stay.get(stay_key)
                if current is None or parsed > current:
                    max_time_by_stay[stay_key] = parsed

        for hospital_id, hospital_df in retained_chunk.groupby(
            "hospital_id",
            dropna=False,
            sort=False,
        ):
            if pd.isna(hospital_id):
                raise ValueError(
                    "Encountered missing hospital_id while partitioning harmonized ASIC "
                    "dynamic rows for per-hospital blocking."
                )
            hospital_key = str(hospital_id)
            hospital_path = hospital_paths.setdefault(
                hospital_key,
                temp_dir / f"{hospital_key}.csv",
            )
            hospital_df.to_csv(
                hospital_path,
                mode="a",
                header=not hospital_path.exists(),
                index=False,
            )

    dynamic_end_time_proxy = _dynamic_end_time_proxy_from_lookup(max_time_by_stay)
    return hospital_paths, dynamic_end_time_proxy, dynamic_rows_total, dynamic_columns


def _cohort_output_paths(
    output_dir: Path,
    output_format: str,
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    cohort_dir = ensure_directory(output_dir / "cohort")
    return {
        "cohort_stay_level": cohort_dir / f"stay_level.{extension}",
        "cohort_summary": cohort_dir / f"summary.{extension}",
        "cohort_preprocessing_notes": cohort_dir / f"preprocessing_notes.{extension}",
        "cohort_icu_end_time_proxy_summary_by_hospital": (
            cohort_dir / f"icu_end_time_proxy_summary_by_hospital.{extension}"
        ),
        "cohort_coding_distribution_by_hospital": (
            cohort_dir / f"coding_distribution_by_hospital.{extension}"
        ),
    }


def _blocked_output_paths(
    output_dir: Path,
    output_format: str,
) -> dict[str, Path]:
    extension = "csv" if output_format == "csv" else "parquet"
    blocked_dir = ensure_directory(output_dir / "blocked")
    return {
        "blocked_asic_8h_block_index": blocked_dir / f"asic_8h_block_index.{extension}",
        "blocked_asic_8h_blocked_dynamic_features": (
            blocked_dir / f"asic_8h_blocked_dynamic_features.{extension}"
        ),
        "blocked_asic_8h_stay_block_counts": (
            blocked_dir / f"asic_8h_stay_block_counts.{extension}"
        ),
        "blocked_asic_8h_block_count_distribution_by_hospital": (
            blocked_dir / f"asic_8h_block_count_distribution_by_hospital.{extension}"
        ),
        "blocked_asic_8h_negative_dynamic_time_qc": (
            blocked_dir / f"asic_8h_negative_dynamic_time_qc.{extension}"
        ),
        "blocked_asic_8h_qc_summary": blocked_dir / f"asic_8h_qc_summary.{extension}",
        "blocked_asic_8h_example_stays": blocked_dir / f"asic_8h_example_stays.{extension}",
    }


def _unlink_if_exists(path: Path) -> None:
    if path.exists():
        path.unlink()


def build_and_write_asic_standardized_dataset_from_harmonized_outputs(
    input_dir: Path = DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    output_dir: Path | None = None,
    input_format: str = "csv",
    output_format: str = "csv",
    dynamic_chunksize: int = 250_000,
) -> dict[str, Path]:
    if input_format != "csv" or output_format != "csv":
        raise ValueError(
            "Per-hospital ASIC standardized rebuild currently supports csv input and "
            "csv output only."
        )
    if dynamic_chunksize <= 0:
        raise ValueError("dynamic_chunksize must be positive.")

    output_root = ensure_directory(input_dir if output_dir is None else output_dir)
    static_path = _harmonized_artifact_path(input_dir, "static", "harmonized", input_format)
    dynamic_path = _harmonized_artifact_path(input_dir, "dynamic", "harmonized", input_format)
    if not static_path.exists():
        raise FileNotFoundError(f"Missing harmonized ASIC static artifact: {static_path}")
    if not dynamic_path.exists():
        raise FileNotFoundError(f"Missing harmonized ASIC dynamic artifact: {dynamic_path}")

    static_df = read_dataframe(static_path)
    stay_ids = set(static_df["stay_id_global"].dropna().astype("string").tolist())

    with TemporaryDirectory(prefix="asic_blocking_tmp_", dir=output_root) as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        hospital_paths, dynamic_end_time_proxy, dynamic_rows_total, dynamic_columns = (
            _partition_harmonized_dynamic_csv_by_hospital(
                dynamic_path=dynamic_path,
                temp_dir=temp_dir,
                stay_ids=stay_ids,
                dynamic_chunksize=dynamic_chunksize,
            )
        )

        stay_level = build_asic_stay_level_table_from_dynamic_end_time_proxy(
            static_df=static_df,
            dynamic_end_time_proxy=dynamic_end_time_proxy,
        )

        output_paths: dict[str, Path] = {}
        cohort_paths = _cohort_output_paths(output_root, output_format)
        output_paths.update(cohort_paths)
        write_dataframe(stay_level.table, cohort_paths["cohort_stay_level"], output_format)
        write_dataframe(stay_level.summary, cohort_paths["cohort_summary"], output_format)
        write_dataframe(
            stay_level.preprocessing_notes,
            cohort_paths["cohort_preprocessing_notes"],
            output_format,
        )
        write_dataframe(
            stay_level.icu_end_time_proxy_summary_by_hospital,
            cohort_paths["cohort_icu_end_time_proxy_summary_by_hospital"],
            output_format,
        )
        write_dataframe(
            stay_level.coding_distribution_by_hospital,
            cohort_paths["cohort_coding_distribution_by_hospital"],
            output_format,
        )

        blocked_paths = _blocked_output_paths(output_root, output_format)
        output_paths.update(blocked_paths)
        _unlink_if_exists(blocked_paths["blocked_asic_8h_blocked_dynamic_features"])

        stay_block_count_parts: list[pd.DataFrame] = []
        block_index_parts: list[pd.DataFrame] = []
        block_distribution_parts: list[pd.DataFrame] = []
        negative_time_parts: list[pd.DataFrame] = []
        blocked_dynamic_written = False

        hospital_ids = (
            stay_level.table["hospital_id"].dropna().astype("string").sort_values().unique().tolist()
        )

        for hospital_id in hospital_ids:
            hospital_dynamic_path = hospital_paths.get(hospital_id)
            if hospital_dynamic_path is None:
                hospital_dynamic_df = pd.DataFrame(columns=dynamic_columns)
            else:
                hospital_dynamic_df = pd.read_csv(hospital_dynamic_path)
            hospital_stay_level = stay_level.table[
                stay_level.table["hospital_id"].astype("string").eq(hospital_id)
            ].copy()
            block_result = build_asic_8h_blocks(
                stay_level_df=hospital_stay_level,
                dynamic_df=hospital_dynamic_df,
            )

            stay_block_count_parts.append(block_result.stay_block_counts)
            block_index_parts.append(block_result.block_index)
            block_distribution_parts.append(block_result.block_count_distribution_by_hospital)
            negative_time_parts.append(block_result.negative_dynamic_time_qc)

            append_dataframe_csv(
                block_result.blocked_dynamic_features,
                blocked_paths["blocked_asic_8h_blocked_dynamic_features"],
                include_header=not blocked_dynamic_written,
            )
            blocked_dynamic_written = True

        combined_stay_block_counts = (
            pd.concat(stay_block_count_parts, ignore_index=True)
            if stay_block_count_parts
            else pd.DataFrame()
        )
        combined_block_index = (
            pd.concat(block_index_parts, ignore_index=True)
            if block_index_parts
            else pd.DataFrame()
        )
        combined_block_distribution = (
            pd.concat(block_distribution_parts, ignore_index=True)
            if block_distribution_parts
            else pd.DataFrame()
        )
        combined_negative_time_qc = (
            pd.concat(negative_time_parts, ignore_index=True)
            if negative_time_parts
            else pd.DataFrame()
        )

        qc_summary = build_asic_8h_qc_summary(
            stay_block_counts=combined_stay_block_counts,
            dynamic_rows_total=dynamic_rows_total,
            negative_dynamic_time_qc=combined_negative_time_qc,
            block_index=combined_block_index,
        )
        example_stays = build_asic_8h_example_stays(
            combined_stay_block_counts,
            combined_block_index,
        )

        write_dataframe(
            combined_block_index,
            blocked_paths["blocked_asic_8h_block_index"],
            output_format,
        )
        write_dataframe(
            combined_stay_block_counts,
            blocked_paths["blocked_asic_8h_stay_block_counts"],
            output_format,
        )
        write_dataframe(
            combined_block_distribution,
            blocked_paths["blocked_asic_8h_block_count_distribution_by_hospital"],
            output_format,
        )
        write_dataframe(
            combined_negative_time_qc,
            blocked_paths["blocked_asic_8h_negative_dynamic_time_qc"],
            output_format,
        )
        write_dataframe(
            qc_summary,
            blocked_paths["blocked_asic_8h_qc_summary"],
            output_format,
        )
        write_dataframe(
            example_stays,
            blocked_paths["blocked_asic_8h_example_stays"],
            output_format,
        )

        if not blocked_dynamic_written:
            write_dataframe(
                pd.DataFrame(),
                blocked_paths["blocked_asic_8h_blocked_dynamic_features"],
                output_format,
            )

    return output_paths


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
