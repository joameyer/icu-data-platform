from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.config import validate_context_output_path
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class DemoGenerationConfig:
    source_contract_path: Path
    source_pooled_dir: Path
    output_pooled_dir: Path
    manifest_path: Path
    seed: int
    stays_per_hospital: int
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Demo-generation configuration does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Invalid YAML in demo-generation configuration {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"Demo-generation configuration must contain a YAML mapping: {path}"
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


def load_demo_generation_config(path: str | Path) -> DemoGenerationConfig:
    config_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(config_path)

    contract_path = _resolve_path(
        _required_string(raw, "contract", "config"),
        config_path,
    )
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigurationError("config.paths must be a mapping")
    source_pooled = _resolve_path(
        _required_string(paths, "source_pooled", "config.paths"),
        config_path,
    )
    output_pooled = _resolve_path(
        _required_string(paths, "output_pooled", "config.paths"),
        config_path,
    )
    manifest = _resolve_path(
        _required_string(paths, "manifest", "config.paths"),
        config_path,
    )
    validate_context_output_path(
        source_pooled,
        "production",
        "config.paths.source_pooled",
    )
    validate_context_output_path(
        output_pooled,
        "demo",
        "config.paths.output_pooled",
    )
    validate_context_output_path(
        manifest.parent,
        "demo",
        "config.paths.manifest",
    )
    if source_pooled == output_pooled:
        raise ConfigurationError("Source and demo output pooled paths must differ")

    sampling = raw.get("sampling")
    if not isinstance(sampling, dict):
        raise ConfigurationError("config.sampling must be a mapping")
    seed = sampling.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        raise ConfigurationError(
            "config.sampling.seed must be a non-negative integer"
        )
    stays_per_hospital = sampling.get("stays_per_hospital")
    if (
        not isinstance(stays_per_hospital, int)
        or isinstance(stays_per_hospital, bool)
        or stays_per_hospital <= 0
    ):
        raise ConfigurationError(
            "config.sampling.stays_per_hospital must be a positive integer"
        )

    return DemoGenerationConfig(
        source_contract_path=contract_path,
        source_pooled_dir=source_pooled,
        output_pooled_dir=output_pooled,
        manifest_path=manifest,
        seed=seed,
        stays_per_hospital=stays_per_hospital,
        source_path=config_path,
    )
