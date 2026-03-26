from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_CHAPTER1_SEED_OUTPUT_DIR = Path("artifacts") / "analysis_seed" / "chapter1"


@dataclass(frozen=True)
class Chapter1SeedInputTables:
    static_harmonized: pd.DataFrame
    dynamic_harmonized: pd.DataFrame
    block_index: pd.DataFrame
    blocked_dynamic_features: pd.DataFrame
    stay_block_counts: pd.DataFrame
    stay_level: pd.DataFrame | None = None


def _read_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported artifact extension for {path}")


def load_chapter1_seed_inputs(
    input_dir: Path,
    input_format: str = "csv",
) -> Chapter1SeedInputTables:
    extension = "csv" if input_format == "csv" else "parquet"

    required_paths = {
        "static_harmonized": input_dir / "static" / f"harmonized.{extension}",
        "dynamic_harmonized": input_dir / "dynamic" / f"harmonized.{extension}",
        "block_index": input_dir / "blocked" / f"asic_8h_block_index.{extension}",
        "blocked_dynamic_features": (
            input_dir / "blocked" / f"asic_8h_blocked_dynamic_features.{extension}"
        ),
        "stay_block_counts": input_dir / "blocked" / f"asic_8h_stay_block_counts.{extension}",
    }

    missing_paths = [
        str(path)
        for path in required_paths.values()
        if not path.exists()
    ]
    if missing_paths:
        raise FileNotFoundError(
            "Missing standardized ASIC input artifacts for Chapter 1 seed: "
            + ", ".join(missing_paths)
        )

    stay_level_path = input_dir / "cohort" / f"stay_level.{extension}"
    stay_level = _read_dataframe(stay_level_path) if stay_level_path.exists() else None

    return Chapter1SeedInputTables(
        static_harmonized=_read_dataframe(required_paths["static_harmonized"]),
        dynamic_harmonized=_read_dataframe(required_paths["dynamic_harmonized"]),
        block_index=_read_dataframe(required_paths["block_index"]),
        blocked_dynamic_features=_read_dataframe(required_paths["blocked_dynamic_features"]),
        stay_block_counts=_read_dataframe(required_paths["stay_block_counts"]),
        stay_level=stay_level,
    )
