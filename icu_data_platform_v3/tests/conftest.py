from __future__ import annotations

from pathlib import Path

import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths


HOSPITAL_FOLDERS = ("00", "01", "02", "03", "04", "06", "07", "08")


def write_text(path: Path, value: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding=encoding, newline="")


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture
def synthetic_inventory(tmp_path: Path, project_root: Path) -> tuple[InventoryConfig, Path]:
    demo_root = tmp_path / "demo"
    raw_root = demo_root / "raw"
    reports = demo_root / "reports"
    runs = demo_root / "runs"
    raw_root.mkdir(parents=True)
    write_text(raw_root / "README.md", "Synthetic non-production inventory fixture.\n")
    for folder in HOSPITAL_FOLDERS:
        stay_id = f"synthetic_stay_{folder}"
        static = "Pseudo-ID;age\n" f"{stay_id};42\n"
        write_text(raw_root / folder / "static.csv", static)
        if folder == "00":
            write_text(
                raw_root / folder / "andere_variablen_kds_patienten.csv",
                static,
            )
            dynamic_name = f"{stay_id}.csv"
        else:
            dynamic_name = f"dynamische_variablen_kds_patient_{stay_id}.csv"
        write_text(
            raw_root / folder / dynamic_name,
            "Pseudo-ID;Zeit_ab_Aufnahme;I:E\n"
            f"{stay_id};0;1:2\n"
            f"{stay_id};60;0,5\n",
        )

    # Both trees contain deliberately malformed content. A safe traversal must
    # prune the directories without attempting to enumerate or decode it.
    write_text(
        raw_root / "01" / ".ipynb_checkpoints" / "protected_stay_name.csv",
        "not,a,valid,inventory,file\n",
    )
    write_text(raw_root / "pooled" / "lossy.parquet", "not parquet\n")
    config = InventoryConfig(
        dataset_context="demo",
        policy_path=project_root / "asic/config/inventory/policy.yaml",
        comparison_reference_path=None,
        paths=InventoryPaths(raw_root=raw_root, reports=reports, runs=runs),
        source_path=project_root / "asic/config/datasets/demo.yaml",
    )
    return config, raw_root
