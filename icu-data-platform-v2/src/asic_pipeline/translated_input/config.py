from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.config import (
    ALLOWED_DATASET_CONTEXTS,
    AuditConfig,
    validate_context_output_path,
)
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class TranslatedInputConfig:
    dataset_context: str
    contract_path: Path
    translated_dir: Path
    cleaned_dir: Path
    reports_dir: Path
    audit: AuditConfig
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Translated input configuration does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Invalid YAML in translated input configuration {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"Translated input configuration must contain a mapping: {path}"
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


def load_translated_input_config(path: str | Path) -> TranslatedInputConfig:
    config_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(config_path)
    dataset_context = _required_string(raw, "dataset_context", "config")
    if dataset_context not in ALLOWED_DATASET_CONTEXTS:
        raise ConfigurationError(
            "config.dataset_context must be one of "
            f"{sorted(ALLOWED_DATASET_CONTEXTS)}, got {dataset_context!r}"
        )

    contract_path = _resolve_path(
        _required_string(raw, "translated_contract", "config"),
        config_path,
    )
    paths = raw.get("paths")
    if not isinstance(paths, dict):
        raise ConfigurationError("config.paths must be a mapping")
    translated_dir = _resolve_path(
        _required_string(paths, "translated", "config.paths"),
        config_path,
    )
    cleaned_dir = _resolve_path(
        _required_string(paths, "cleaned", "config.paths"),
        config_path,
    )
    reports_dir = _resolve_path(
        _required_string(paths, "reports", "config.paths"),
        config_path,
    )
    for value, label in (
        (translated_dir, "config.paths.translated"),
        (cleaned_dir, "config.paths.cleaned"),
        (reports_dir, "config.paths.reports"),
    ):
        validate_context_output_path(value, dataset_context, label)
    if translated_dir.name != "translated":
        raise ConfigurationError(
            "config.paths.translated must end in a 'translated' directory"
        )
    if cleaned_dir.name != "cleaned":
        raise ConfigurationError(
            "config.paths.cleaned must end in a 'cleaned' directory"
        )
    if translated_dir == cleaned_dir:
        raise ConfigurationError(
            "Translated input and cleaned output directories must differ"
        )

    settings = raw.get("translated_audit", raw.get("audit", {}))
    if not isinstance(settings, dict):
        raise ConfigurationError("config.translated_audit must be a mapping")
    batch_size = settings.get("batch_size", 100_000)
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ConfigurationError(
            "config.translated_audit.batch_size must be a positive integer"
        )
    check_duplicates = settings.get("check_duplicate_dynamic_keys", True)
    if not isinstance(check_duplicates, bool):
        raise ConfigurationError(
            "config.translated_audit.check_duplicate_dynamic_keys must be true or false"
        )
    time_anchor = settings.get("time_anchor", "2020-01-01 00:00:00")
    if not isinstance(time_anchor, str) or not time_anchor.strip():
        raise ConfigurationError(
            "config.translated_audit.time_anchor must be a non-empty string"
        )
    tolerance = settings.get("time_tolerance_seconds", 0.0)
    if (
        not isinstance(tolerance, (int, float))
        or isinstance(tolerance, bool)
        or tolerance < 0
    ):
        raise ConfigurationError(
            "config.translated_audit.time_tolerance_seconds must be non-negative"
        )

    return TranslatedInputConfig(
        dataset_context=dataset_context,
        contract_path=contract_path,
        translated_dir=translated_dir,
        cleaned_dir=cleaned_dir,
        reports_dir=reports_dir,
        audit=AuditConfig(
            batch_size=batch_size,
            check_duplicate_dynamic_keys=check_duplicates,
            time_anchor=time_anchor.strip(),
            time_tolerance_seconds=float(tolerance),
        ),
        source_path=config_path,
    )
