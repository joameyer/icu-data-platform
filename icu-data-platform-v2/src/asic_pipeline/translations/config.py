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
class TranslationConfig:
    dataset_context: str
    source_contract_path: Path
    policy_path: Path
    source_pooled_dir: Path
    output_translated_dir: Path
    manifest_path: Path
    batch_size: int
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Translation configuration does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Invalid YAML in translation configuration {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"Translation configuration must contain a YAML mapping: {path}"
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


def load_translation_config(path: str | Path) -> TranslationConfig:
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
    output_translated = _resolve_path(
        _required_string(paths, "translated", "config.paths"),
        config_path,
    )
    manifest = output_translated / "translation_manifest.json"
    for value, label in (
        (source_pooled, "config.paths.pooled"),
        (output_translated, "config.paths.translated"),
    ):
        validate_context_output_path(value, dataset_context, label)
    if source_pooled == output_translated:
        raise ConfigurationError(
            "Source pooled and translated output directories must differ"
        )
    if output_translated.name != "translated":
        raise ConfigurationError(
            "config.paths.translated must end in a 'translated' directory"
        )

    settings = raw.get("pooled_to_translated", {})
    if not isinstance(settings, dict):
        raise ConfigurationError("config.pooled_to_translated must be a mapping")
    batch_size = settings.get("batch_size", 100_000)
    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ConfigurationError(
            "config.pooled_to_translated.batch_size must be a positive integer"
        )

    return TranslationConfig(
        dataset_context=dataset_context,
        source_contract_path=contract_path,
        policy_path=policy_path,
        source_pooled_dir=source_pooled,
        output_translated_dir=output_translated,
        manifest_path=manifest,
        batch_size=batch_size,
        source_path=config_path,
    )
