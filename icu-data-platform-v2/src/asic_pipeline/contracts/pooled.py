from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.errors import ContractError


_SCHEMA_CONTEXT_ALIASES = {"demo": "production"}


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    arrow_types: tuple[tuple[str, str], ...]

    def arrow_type_for(self, dataset_context: str) -> str:
        types = dict(self.arrow_types)
        # Demo artifacts are row subsets written without any physical cast.
        schema_context = _SCHEMA_CONTEXT_ALIASES.get(
            dataset_context,
            dataset_context,
        )
        if schema_context in types:
            return types[schema_context]
        if "default" in types:
            return types["default"]
        raise ContractError(
            f"Column {self.name!r} has no Arrow type for dataset context "
            f"{dataset_context!r}"
        )


@dataclass(frozen=True)
class TableContract:
    filename: str
    columns: tuple[ColumnSpec, ...]


@dataclass(frozen=True)
class InputContract:
    contract_version: str
    dataset: str
    expected_hospital_codes: tuple[int, ...]
    static: TableContract
    dynamic: TableContract
    stay_id_column: str
    hospital_id_column: str
    relative_time_column: str
    anchored_time_column: str
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ContractError(f"Input contract does not exist: {path}") from exc
    except yaml.YAMLError as exc:
        raise ContractError(f"Invalid YAML in input contract {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"Input contract must contain a YAML mapping: {path}")
    return value


def _required_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def parse_table_contract(raw: Any, name: str) -> TableContract:
    if not isinstance(raw, dict):
        raise ContractError(f"contract.tables.{name} must be a YAML mapping")
    filename = _required_string(raw, "filename", f"contract.tables.{name}")
    raw_columns = raw.get("columns")
    if not isinstance(raw_columns, list) or not raw_columns:
        raise ContractError(f"contract.tables.{name}.columns must be a non-empty list")

    columns: list[ColumnSpec] = []
    for index, raw_column in enumerate(raw_columns):
        location = f"contract.tables.{name}.columns[{index}]"
        if not isinstance(raw_column, dict):
            raise ContractError(f"{location} must be a YAML mapping")
        columns.append(
            ColumnSpec(
                name=_required_string(raw_column, "name", location),
                arrow_types=_parse_arrow_types(raw_column.get("arrow_type"), location),
            )
        )

    names = [column.name for column in columns]
    duplicates = sorted({column for column in names if names.count(column) > 1})
    if duplicates:
        raise ContractError(
            f"contract.tables.{name}.columns contains duplicate names: {duplicates}"
        )
    return TableContract(filename=filename, columns=tuple(columns))


def _parse_arrow_types(value: Any, location: str) -> tuple[tuple[str, str], ...]:
    if isinstance(value, str) and value.strip():
        return (("default", value.strip()),)

    if not isinstance(value, dict) or not value:
        raise ContractError(
            f"{location}.arrow_type must be a non-empty string or context mapping"
        )

    allowed_contexts = {"mock", "production"}
    contexts = set(value)
    invalid_contexts = sorted(contexts - allowed_contexts)
    if invalid_contexts:
        raise ContractError(
            f"{location}.arrow_type has unsupported contexts: {invalid_contexts}"
        )
    missing_contexts = sorted(allowed_contexts - contexts)
    if missing_contexts:
        raise ContractError(
            f"{location}.arrow_type context mapping is missing: {missing_contexts}"
        )

    parsed: list[tuple[str, str]] = []
    for context in ("production", "mock"):
        arrow_type = value[context]
        if not isinstance(arrow_type, str) or not arrow_type.strip():
            raise ContractError(
                f"{location}.arrow_type.{context} must be a non-empty string"
            )
        parsed.append((context, arrow_type.strip()))
    return tuple(parsed)


def load_input_contract(path: str | Path) -> InputContract:
    contract_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(contract_path)

    hospitals = raw.get("expected_hospital_codes")
    if (
        not isinstance(hospitals, list)
        or not hospitals
        or any(not isinstance(value, int) or isinstance(value, bool) for value in hospitals)
    ):
        raise ContractError("contract.expected_hospital_codes must be a non-empty integer list")
    if len(hospitals) != len(set(hospitals)):
        raise ContractError("contract.expected_hospital_codes contains duplicates")

    tables = raw.get("tables")
    if not isinstance(tables, dict):
        raise ContractError("contract.tables must be a YAML mapping")

    identifiers = raw.get("identifiers")
    if not isinstance(identifiers, dict):
        raise ContractError("contract.identifiers must be a YAML mapping")
    time = raw.get("time")
    if not isinstance(time, dict):
        raise ContractError("contract.time must be a YAML mapping")

    return InputContract(
        contract_version=_required_string(raw, "contract_version", "contract"),
        dataset=_required_string(raw, "dataset", "contract"),
        expected_hospital_codes=tuple(hospitals),
        static=parse_table_contract(tables.get("static"), "static"),
        dynamic=parse_table_contract(tables.get("dynamic"), "dynamic"),
        stay_id_column=_required_string(
            identifiers,
            "stay_id_column",
            "contract.identifiers",
        ),
        hospital_id_column=_required_string(
            identifiers,
            "hospital_id_column",
            "contract.identifiers",
        ),
        relative_time_column=_required_string(
            time,
            "relative_time_column",
            "contract.time",
        ),
        anchored_time_column=_required_string(
            time,
            "anchored_time_column",
            "contract.time",
        ),
        source_path=contract_path,
    )
