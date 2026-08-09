from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml

from asic_pipeline.config import validate_context_output_path
from asic_pipeline.errors import ConfigurationError


_BLOCK_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_REQUIRED_STATIC_PROTECTED = frozenset({"Pseudo-ID", "Liegedauer_ICU", "hid"})
_REQUIRED_DYNAMIC_PROTECTED = frozenset(
    {"Pseudo-ID", "Zeit_ab_Aufnahme", "hid", "timeidx"}
)


@dataclass(frozen=True)
class ShuffleBlock:
    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class MockGenerationConfig:
    source_contract_path: Path
    source_pooled_dir: Path
    output_pooled_dir: Path
    manifest_path: Path
    seed: int
    stays_per_hospital: int
    protected_static_columns: tuple[str, ...]
    protected_dynamic_columns: tuple[str, ...]
    static_shuffle_blocks: tuple[ShuffleBlock, ...]
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"Mock-generation configuration does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ConfigurationError(
            f"Invalid YAML in mock-generation configuration {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"Mock-generation configuration must contain a YAML mapping: {path}"
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


def _required_string_list(
    mapping: dict[str, Any],
    key: str,
    location: str,
) -> tuple[str, ...]:
    value = mapping.get(key)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item.strip() for item in value)
    ):
        raise ConfigurationError(f"{location}.{key} must be a non-empty string list")
    cleaned = tuple(item.strip() for item in value)
    if len(cleaned) != len(set(cleaned)):
        raise ConfigurationError(f"{location}.{key} contains duplicates")
    return cleaned


def _parse_shuffle_blocks(raw: Any) -> tuple[ShuffleBlock, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigurationError("config.static_shuffle_blocks must be a non-empty list")

    blocks: list[ShuffleBlock] = []
    all_columns: list[str] = []
    for index, item in enumerate(raw):
        location = f"config.static_shuffle_blocks[{index}]"
        if not isinstance(item, dict):
            raise ConfigurationError(f"{location} must be a mapping")
        name = _required_string(item, "name", location)
        if not _BLOCK_NAME_PATTERN.fullmatch(name):
            raise ConfigurationError(f"{location}.name must use lower snake_case")
        columns = _required_string_list(item, "columns", location)
        blocks.append(ShuffleBlock(name=name, columns=columns))
        all_columns.extend(columns)

    names = [block.name for block in blocks]
    if len(names) != len(set(names)):
        raise ConfigurationError("config.static_shuffle_blocks contains duplicate names")
    duplicated_columns = sorted(
        {column for column in all_columns if all_columns.count(column) > 1}
    )
    if duplicated_columns:
        raise ConfigurationError(
            "Static shuffle-block columns may occur in only one block: "
            f"{duplicated_columns}"
        )
    return tuple(blocks)


def load_mock_generation_config(path: str | Path) -> MockGenerationConfig:
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
        "mock",
        "config.paths.output_pooled",
    )
    validate_context_output_path(
        manifest.parent,
        "mock",
        "config.paths.manifest",
    )
    if source_pooled == output_pooled:
        raise ConfigurationError("Source and mock output pooled paths must differ")

    sampling = raw.get("sampling")
    if not isinstance(sampling, dict):
        raise ConfigurationError("config.sampling must be a mapping")
    seed = sampling.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        raise ConfigurationError("config.sampling.seed must be a non-negative integer")
    stays_per_hospital = sampling.get("stays_per_hospital")
    if (
        not isinstance(stays_per_hospital, int)
        or isinstance(stays_per_hospital, bool)
        or stays_per_hospital <= 0
    ):
        raise ConfigurationError(
            "config.sampling.stays_per_hospital must be a positive integer"
        )

    protected = raw.get("protected_columns")
    if not isinstance(protected, dict):
        raise ConfigurationError("config.protected_columns must be a mapping")
    protected_static = _required_string_list(
        protected,
        "static",
        "config.protected_columns",
    )
    protected_dynamic = _required_string_list(
        protected,
        "dynamic",
        "config.protected_columns",
    )
    missing_static_protected = sorted(
        _REQUIRED_STATIC_PROTECTED - set(protected_static)
    )
    missing_dynamic_protected = sorted(
        _REQUIRED_DYNAMIC_PROTECTED - set(protected_dynamic)
    )
    if missing_static_protected or missing_dynamic_protected:
        raise ConfigurationError(
            "Required privacy/identity columns may not be removed from protection; "
            f"static_missing={missing_static_protected}, "
            f"dynamic_missing={missing_dynamic_protected}"
        )

    blocks = _parse_shuffle_blocks(raw.get("static_shuffle_blocks"))
    overlap = sorted(
        set(protected_static).intersection(
            column for block in blocks for column in block.columns
        )
    )
    if overlap:
        raise ConfigurationError(
            "Static columns cannot be both protected and shuffled in a block: "
            f"{overlap}"
        )

    return MockGenerationConfig(
        source_contract_path=contract_path,
        source_pooled_dir=source_pooled,
        output_pooled_dir=output_pooled,
        manifest_path=manifest,
        seed=seed,
        stays_per_hospital=stays_per_hospital,
        protected_static_columns=protected_static,
        protected_dynamic_columns=protected_dynamic,
        static_shuffle_blocks=blocks,
        source_path=config_path,
    )
