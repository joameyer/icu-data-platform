from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.config import (
    ALLOWED_DATASET_CONTEXTS,
    validate_context_output_path,
)
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class CategoricalInventoryConfig:
    dataset_context: str
    source_contract_path: Path
    policy_path: Path
    source_pooled_dir: Path
    reports_dir: Path
    batch_size: int
    max_distinct_values_per_column: int
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Categorical-inventory configuration does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Invalid YAML in categorical-inventory configuration {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            "Categorical-inventory configuration must contain a YAML mapping: "
            f"{path}"
        )
    return value


def _required_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _resolve_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _positive_integer(
    raw: dict[str, Any],
    key: str,
    default: int,
    location: str,
) -> int:
    value = raw.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location}.{key} must be a positive integer")
    return value


def load_categorical_inventory_config(
    path: str | Path,
) -> CategoricalInventoryConfig:
    config_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(config_path)

    dataset_context = _required_string(raw, "dataset_context", "config")
    if dataset_context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError(
            "config.dataset_context must be one of "
            f"{sorted(ALLOWED_DATASET_CONTEXTS)}, got {dataset_context!r}"
        )
    contract_path = _resolve_path(
        _required_string(raw, "contract", "config"),
        config_path,
    )
    policy_path = _resolve_path(
        _required_string(raw, "translation_policy", "config"),
        config_path,
    )

    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigurationError("config.paths must be a mapping")
    source_pooled = _resolve_path(
        _required_string(paths, "pooled", "config.paths"),
        config_path,
    )
    reports = _resolve_path(
        _required_string(paths, "reports", "config.paths"),
        config_path,
    )
    for value, label in (
        (source_pooled, "config.paths.pooled"),
        (reports, "config.paths.reports"),
    ):
        validate_context_output_path(value, dataset_context, label)
    if source_pooled.name != "pooled":
        raise ConfigurationError(
            "config.paths.pooled must end in a 'pooled' directory"
        )

    settings = raw.get("categorical_inventory", {})
    if not isinstance(settings, dict):
        raise ConfigurationError("config.categorical_inventory must be a mapping")

    return CategoricalInventoryConfig(
        dataset_context=dataset_context,
        source_contract_path=contract_path,
        policy_path=policy_path,
        source_pooled_dir=source_pooled,
        reports_dir=reports,
        batch_size=_positive_integer(
            settings,
            "batch_size",
            100_000,
            "config.categorical_inventory",
        ),
        max_distinct_values_per_column=_positive_integer(
            settings,
            "max_distinct_values_per_column",
            1_000,
            "config.categorical_inventory",
        ),
        source_path=config_path,
    )
