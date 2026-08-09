from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from asic_pipeline.config import (
    InventoryConfig,
    load_inventory_config,
    load_yaml_mapping,
    required_string,
    resolve_path,
    validate_context_path,
    validate_path_within,
)
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class IngestionConfig:
    inventory: InventoryConfig
    policy_path: Path
    data_root: Path
    rows_per_batch: int

    @property
    def dataset_context(self) -> str:
        return self.inventory.dataset_context

    @property
    def output_root(self) -> Path:
        return self.data_root / "ingested"


def load_ingestion_config(path: str | Path) -> IngestionConfig:
    config_path = Path(path).expanduser().resolve()
    inventory = load_inventory_config(config_path)
    raw = load_yaml_mapping(config_path, "Ingestion configuration")
    policy_path = resolve_path(
        required_string(raw, "ingestion_policy", "config"), config_path
    )
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigurationError("config.paths must be a YAML mapping")
    data_root = resolve_path(
        required_string(paths, "data", "config.paths"), config_path
    )
    validate_context_path(data_root, inventory.dataset_context, "config.paths.data")
    project_root = config_path.parents[3]
    validate_path_within(data_root, project_root / "asic", "config.paths.data")
    if data_root in {
        inventory.paths.raw_root,
        inventory.paths.reports,
        inventory.paths.runs,
    }:
        raise ConfigurationError("Raw, report, run, and data roots must be distinct")
    ingestion = raw.get("ingestion")
    if not isinstance(ingestion, dict):
        raise ConfigurationError("config.ingestion must be a YAML mapping")
    rows_per_batch = ingestion.get("rows_per_batch")
    if (
        not isinstance(rows_per_batch, int)
        or isinstance(rows_per_batch, bool)
        or rows_per_batch <= 0
    ):
        raise ConfigurationError("config.ingestion.rows_per_batch must be positive")
    return IngestionConfig(
        inventory=inventory,
        policy_path=policy_path,
        data_root=data_root,
        rows_per_batch=rows_per_batch,
    )
