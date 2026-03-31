from __future__ import annotations

import argparse
from pathlib import Path

from icu_data_platform.sources.asic.pipeline import (
    DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
    build_and_write_asic_standardized_dataset_from_harmonized_outputs,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build ASIC standardized stay-level and generic 8-hour blocked artifacts "
            "from previously written harmonized outputs."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_ASIC_HARMONIZED_OUTPUT_DIR,
        help="Directory containing previously written harmonized ASIC artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory where the standardized outputs should be written. Defaults to "
            "the input directory."
        ),
    )
    parser.add_argument(
        "--input-format",
        choices=["csv"],
        default="csv",
        help="Format of the existing harmonized ASIC artifacts.",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv"],
        default="csv",
        help="Output format for written standardized tables.",
    )
    parser.add_argument(
        "--dynamic-chunksize",
        type=int,
        default=250_000,
        help="Chunk size used when streaming the harmonized dynamic CSV.",
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    output_paths = build_and_write_asic_standardized_dataset_from_harmonized_outputs(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        input_format=args.input_format,
        output_format=args.output_format,
        dynamic_chunksize=args.dynamic_chunksize,
    )

    for name, path in sorted(output_paths.items()):
        print(f"{name}: {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
