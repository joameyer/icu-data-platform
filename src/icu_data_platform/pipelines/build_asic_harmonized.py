from __future__ import annotations

import argparse
from pathlib import Path

from icu_data_platform.sources.asic.extract.raw_tables import (
    DEFAULT_ASIC_RAW_DATA_DIR,
    DEFAULT_TRANSLATION_PATH,
)
from icu_data_platform.sources.asic.pipeline import (
    build_asic_harmonized_dataset,
    write_asic_harmonized_dataset,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build harmonized ASIC static and dynamic tables.")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_ASIC_RAW_DATA_DIR,
        help="Directory containing asic_*_static.csv and asic_*_dynamic.csv files.",
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
        default=Path("artifacts") / "asic_harmonized",
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

    dataset = build_asic_harmonized_dataset(
        raw_dir=args.raw_dir,
        translation_path=args.translation_path,
        min_non_null=args.min_non_null,
        min_hospitals=args.min_hospitals,
        fence_factor=args.fence_factor,
    )
    output_paths = write_asic_harmonized_dataset(
        dataset,
        output_dir=args.output_dir,
        output_format=args.format,
    )

    print(f"Built ASIC static rows: {dataset.static.combined.shape[0]}")
    print(f"Built ASIC dynamic rows: {dataset.dynamic.combined.shape[0]}")
    for name, path in sorted(output_paths.items()):
        print(f"{name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
