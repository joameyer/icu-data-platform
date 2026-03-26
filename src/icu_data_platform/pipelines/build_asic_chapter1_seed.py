from __future__ import annotations

import argparse
from pathlib import Path

from icu_data_platform.analysis_seed.chapter1.io import DEFAULT_CHAPTER1_SEED_OUTPUT_DIR
from icu_data_platform.analysis_seed.chapter1.pipeline import (
    build_and_write_chapter1_seed_dataset,
)
from icu_data_platform.sources.asic.pipeline import DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the portable Chapter 1 preprocessing layer from standardized upstream ASIC "
            "artifacts."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
        help="Directory containing standardized upstream ASIC artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_CHAPTER1_SEED_OUTPUT_DIR,
        help="Directory where Chapter 1 seed outputs should be written.",
    )
    parser.add_argument(
        "--input-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Format of the standardized input artifacts.",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Format for written Chapter 1 seed outputs.",
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    dataset, output_paths = build_and_write_chapter1_seed_dataset(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        input_format=args.input_format,
        output_format=args.output_format,
    )

    print(f"Built Chapter 1 retained stays: {dataset.cohort.table.shape[0]}")
    print(f"Built Chapter 1 valid instances: {dataset.valid_instances.valid_instances.shape[0]}")
    print(f"Built Chapter 1 model-ready rows: {dataset.model_ready.table.shape[0]}")
    for name, path in sorted(output_paths.items()):
        print(f"{name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
