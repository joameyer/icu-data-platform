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
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build harmonized ASIC static and dynamic tables.")
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

    print(f"Built ASIC static rows: {dataset.static.combined.shape[0]}")
    print(f"Built ASIC dynamic rows: {dataset.dynamic.combined.shape[0]}")
    print(f"Built ASIC Chapter 1 8h blocks: {dataset.chapter1_8h_blocks.block_index.shape[0]}")
    for name, path in sorted(output_paths.items()):
        print(f"{name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
