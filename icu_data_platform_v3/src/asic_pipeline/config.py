from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.errors import ConfigurationError


ALLOWED_DATASET_CONTEXTS = frozenset({"demo", "production"})
PRODUCTION_RAW_ROOT = Path("/hpcwork/jrc_combine/richard/asic_2026")


@dataclass(frozen=True)
class InventoryPaths:
    raw_root: Path
    reports: Path
    runs: Path


@dataclass(frozen=True)
class InventoryConfig:
    dataset_context: str
    policy_path: Path
    comparison_reference_path: Path | None
    paths: InventoryPaths
    source_path: Path


def load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"{label} does not exist: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigurationError(f"{label} must contain a YAML mapping: {path}")
    return value


def required_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def resolve_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def validate_context_path(path: Path, context: str, label: str) -> None:
    if context not in path.parts:
        raise ConfigurationError(
            f"{label} must contain a dedicated {context!r} path component: {path}"
        )
    other_contexts = sorted(
        candidate
        for candidate in ALLOWED_DATASET_CONTEXTS
        if candidate != context and candidate in path.parts
    )
    if other_contexts:
        raise ConfigurationError(
            f"{label} crosses dataset contexts {other_contexts}: {path}"
        )


def validate_path_within(path: Path, parent: Path, label: str) -> None:
    try:
        path.relative_to(parent)
    except ValueError as exc:
        raise ConfigurationError(
            f"{label} must remain inside the v3 project tree {parent}: {path}"
        ) from exc


def load_inventory_config(path: str | Path) -> InventoryConfig:
    config_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(config_path, "Inventory configuration")
    context = required_string(raw, "dataset_context", "config")
    if context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError(
            f"config.dataset_context must be one of {sorted(ALLOWED_DATASET_CONTEXTS)}"
        )

    policy_path = resolve_path(
        required_string(raw, "inventory_policy", "config"),
        config_path,
    )
    comparison_value = raw.get("comparison_reference")
    if comparison_value is None:
        comparison_path = None
    elif isinstance(comparison_value, str) and comparison_value.strip():
        comparison_path = resolve_path(comparison_value.strip(), config_path)
    else:
        raise ConfigurationError(
            "config.comparison_reference must be a non-empty string or null"
        )

    paths_raw = raw.get("paths")
    if not isinstance(paths_raw, dict):
        raise ConfigurationError("config.paths must be a YAML mapping")
    raw_root = resolve_path(
        required_string(paths_raw, "raw_root", "config.paths"),
        config_path,
    )
    reports = resolve_path(
        required_string(paths_raw, "reports", "config.paths"),
        config_path,
    )
    runs = resolve_path(
        required_string(paths_raw, "runs", "config.paths"),
        config_path,
    )
    validate_context_path(reports, context, "config.paths.reports")
    validate_context_path(runs, context, "config.paths.runs")
    if len(config_path.parents) < 4 or config_path.parents[2].name != "asic":
        raise ConfigurationError(
            "Inventory configuration must remain under asic/config/datasets"
        )
    project_root = config_path.parents[3]
    v3_asic_root = project_root / "asic"
    validate_path_within(reports, v3_asic_root, "config.paths.reports")
    validate_path_within(runs, v3_asic_root, "config.paths.runs")
    if context == "production":
        canonical_production_raw_root = PRODUCTION_RAW_ROOT.resolve()
        if raw_root != canonical_production_raw_root:
            raise ConfigurationError(
                "Production raw_root must resolve to the approved authoritative "
                f"source {PRODUCTION_RAW_ROOT} (canonical path "
                f"{canonical_production_raw_root}); observed {raw_root}"
            )
    if raw_root == reports or raw_root == runs:
        raise ConfigurationError("Raw input and v3 output directories must differ")

    return InventoryConfig(
        dataset_context=context,
        policy_path=policy_path,
        comparison_reference_path=comparison_path,
        paths=InventoryPaths(raw_root=raw_root, reports=reports, runs=runs),
        source_path=config_path,
    )
