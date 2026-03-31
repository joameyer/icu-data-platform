from __future__ import annotations

import argparse
from pathlib import Path

from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
)
from icu_data_platform.sources.asic.pipeline import (
    DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    build_and_write_asic_harmonized_dataset,
    build_and_write_asic_standardized_dataset,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build ASIC harmonized tables plus standardized stay-level and generic 8-hour "
            "blocked artifacts."
        )
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_ASIC_RAW_DATA_DIR,
        help=(
            "ASIC root directory containing UK_* hospital folders with static/ and "
            "dynamic_filtered/ or dynamic/ subfolders."
        ),
    )
    parser.add_argument(
        "--translation-path",
        type=Path,
        default=DEFAULT_TRANSLATION_PATH,
        help="Path to the ASIC raw-to-canonical column translation JSON file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
        help="Directory where the harmonized outputs should be written.",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for written tables.",
    )
    parser.add_argument(
        "--min-non-null",
        type=int,
        default=20,
        help="Minimum non-null count for dynamic numeric distribution summaries.",
    )
    parser.add_argument(
        "--min-hospitals",
        type=int,
        default=4,
        help="Minimum hospital count required for dynamic cross-hospital issue checks.",
    )
    parser.add_argument(
        "--fence-factor",
        type=float,
        default=1.5,
        help="IQR fence multiplier for dynamic cross-hospital issue checks.",
    )
    parser.add_argument(
        "--skip-standardized",
        action="store_true",
        help=(
            "Build only harmonized tables and QC artifacts; skip standardized "
            "stay-level and generic 8-hour blocked outputs."
        ),
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    dataset, output_paths = build_and_write_asic_harmonized_dataset(
        raw_dir=args.raw_dir,
        translation_path=args.translation_path,
        output_dir=args.output_dir,
        output_format=args.format,
        min_non_null=args.min_non_null,
        min_hospitals=args.min_hospitals,
        fence_factor=args.fence_factor,
    )
    standardized_dataset = None
    if not args.skip_standardized:
        standardized_dataset, standardized_output_paths = build_and_write_asic_standardized_dataset(
            dataset,
            output_dir=args.output_dir,
            output_format=args.format,
        )
        output_paths = {**output_paths, **standardized_output_paths}

    print(f"Built ASIC static rows: {dataset.static.combined.shape[0]}")
    print(f"Built ASIC dynamic rows: {dataset.dynamic.combined.shape[0]}")
    print(
        "Built ASIC mech vent >=24h QC stays: "
        f"{dataset.mech_vent_ge_24h_qc.stay_level.shape[0]}"
    )
    print(
        "Observed mech vent >=24h QC-positive stays: "
        f"{int(dataset.mech_vent_ge_24h_qc.stay_level['mech_vent_ge_24h_qc'].sum())}"
    )
    if standardized_dataset is None:
        print("Skipped ASIC standardized stay-level and generic 8h blocks.")
    else:
        print(f"Built ASIC stay-level rows: {standardized_dataset.stay_level.table.shape[0]}")
        print(
            "Built ASIC generic 8h blocks: "
            f"{standardized_dataset.blocked_8h.block_index.shape[0]}"
        )
    for name, path in sorted(output_paths.items()):
        print(f"{name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
