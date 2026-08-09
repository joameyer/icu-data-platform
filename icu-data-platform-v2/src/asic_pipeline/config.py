from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.errors import ConfigurationError


ALLOWED_DATASET_CONTEXTS = frozenset({"demo", "mock", "production"})


@dataclass(frozen=True)
class PathConfig:
    pooled: Path
    reports: Path


@dataclass(frozen=True)
class AuditConfig:
    batch_size: int = 100_000
    check_duplicate_dynamic_keys: bool = True
    time_anchor: str = "2020-01-01 00:00:00"
    time_tolerance_seconds: float = 0.0


@dataclass(frozen=True)
class PipelineConfig:
    dataset_context: str
    contract_path: Path
    paths: PathConfig
    audit: AuditConfig
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"Configuration file does not exist: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in configuration file {path}: {exc}") from exc

    if not isinstance(value, dict):
        raise ConfigurationError(f"Configuration must contain a YAML mapping: {path}")
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


def validate_context_output_path(path: Path, dataset_context: str, label: str) -> None:
    parts = set(path.parts)
    other_contexts = sorted(
        context
        for context in ALLOWED_DATASET_CONTEXTS
        if context != dataset_context and context in parts
    )
    if other_contexts:
        raise ConfigurationError(
            f"{label} points into a different dataset-context tree "
            f"{other_contexts!r} while the configured "
            f"dataset_context is {dataset_context!r}: {path}"
        )
    if dataset_context not in parts:
        raise ConfigurationError(
            f"{label} must contain a dedicated {dataset_context!r} path component: {path}"
        )


def load_config(path: str | Path) -> PipelineConfig:
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

    paths_raw = raw.get("paths")
    if not isinstance(paths_raw, dict):
        raise ConfigurationError("config.paths must be a YAML mapping")
    pooled_path = _resolve_path(
        _required_string(paths_raw, "pooled", "config.paths"),
        config_path,
    )
    reports_path = _resolve_path(
        _required_string(paths_raw, "reports", "config.paths"),
        config_path,
    )
    validate_context_output_path(reports_path, dataset_context, "config.paths.reports")

    audit_raw = raw.get("audit", {})
    if not isinstance(audit_raw, dict):
        raise ConfigurationError("config.audit must be a YAML mapping")

    batch_size = audit_raw.get("batch_size", 100_000)
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise ConfigurationError("config.audit.batch_size must be a positive integer")

    check_duplicates = audit_raw.get("check_duplicate_dynamic_keys", True)
    if not isinstance(check_duplicates, bool):
        raise ConfigurationError(
            "config.audit.check_duplicate_dynamic_keys must be true or false"
        )

    time_anchor = audit_raw.get("time_anchor", "2020-01-01 00:00:00")
    if not isinstance(time_anchor, str) or not time_anchor.strip():
        raise ConfigurationError("config.audit.time_anchor must be a non-empty string")

    tolerance = audit_raw.get("time_tolerance_seconds", 0.0)
    if not isinstance(tolerance, (int, float)) or isinstance(tolerance, bool) or tolerance < 0:
        raise ConfigurationError(
            "config.audit.time_tolerance_seconds must be a non-negative number"
        )

    return PipelineConfig(
        dataset_context=dataset_context,
        contract_path=contract_path,
        paths=PathConfig(pooled=pooled_path, reports=reports_path),
        audit=AuditConfig(
            batch_size=batch_size,
            check_duplicate_dynamic_keys=check_duplicates,
            time_anchor=time_anchor.strip(),
            time_tolerance_seconds=float(tolerance),
        ),
        source_path=config_path,
    )
