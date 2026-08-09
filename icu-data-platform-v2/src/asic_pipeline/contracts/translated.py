from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from asic_pipeline.contracts.pooled import TableContract, parse_table_contract
from asic_pipeline.errors import ContractError


@dataclass(frozen=True)
class TranslatedProducerContract:
    pooled_contract_version: str
    translation_policy_version: str
    compatible_generator_versions: tuple[str, ...]
    documentation: str


@dataclass(frozen=True)
class TranslatedManifestContract:
    filename: str
    artifact: str
    audit_status: str
    required_checks: tuple[tuple[str, bool | int], ...]

    def checks(self) -> dict[str, bool | int]:
        return dict(self.required_checks)


@dataclass(frozen=True)
class TranslatedInputContract:
    contract_version: str
    dataset: str
    expected_hospital_codes: tuple[int, ...]
    producer: TranslatedProducerContract
    manifest: TranslatedManifestContract
    static: TableContract
    dynamic: TableContract
    stay_id_column: str
    canonical_hospital_id_column: str
    source_hospital_code_column: str
    relative_time_column: str
    anchored_time_column: str
    preserve_all_input_columns: bool
    preserve_row_count_and_order: bool
    preserve_identifiers_and_time_keys: bool
    value_changes_require_approved_cleaning_rules: bool
    source_path: Path


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ContractError(
            f"Translated input contract does not exist: {path}"
        ) from exc
    except yaml.YAMLError as exc:
        raise ContractError(
            f"Invalid YAML in translated input contract {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ContractError(
            f"Translated input contract must contain a YAML mapping: {path}"
        )
    return value


def _required_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _required_true(mapping: dict[str, Any], key: str, location: str) -> bool:
    value = mapping.get(key)
    if value is not True:
        raise ContractError(f"{location}.{key} must be true")
    return True


def _parse_producer(raw: Any) -> TranslatedProducerContract:
    location = "contract.producer"
    if not isinstance(raw, dict):
        raise ContractError(f"{location} must be a mapping")
    versions = raw.get("compatible_generator_versions")
    if (
        not isinstance(versions, list)
        or not versions
        or any(not isinstance(value, str) or not value.strip() for value in versions)
    ):
        raise ContractError(
            f"{location}.compatible_generator_versions must be a non-empty string list"
        )
    normalized_versions = tuple(value.strip() for value in versions)
    if len(normalized_versions) != len(set(normalized_versions)):
        raise ContractError(
            f"{location}.compatible_generator_versions contains duplicates"
        )
    return TranslatedProducerContract(
        pooled_contract_version=_required_string(
            raw,
            "pooled_contract_version",
            location,
        ),
        translation_policy_version=_required_string(
            raw,
            "translation_policy_version",
            location,
        ),
        compatible_generator_versions=normalized_versions,
        documentation=_required_string(raw, "documentation", location),
    )


def _parse_manifest(raw: Any) -> TranslatedManifestContract:
    location = "contract.manifest"
    if not isinstance(raw, dict):
        raise ContractError(f"{location} must be a mapping")
    checks = raw.get("required_checks")
    if not isinstance(checks, dict) or not checks:
        raise ContractError(f"{location}.required_checks must be a non-empty mapping")
    parsed_checks: list[tuple[str, bool | int]] = []
    for name, expected in checks.items():
        if not isinstance(name, str) or not name.strip():
            raise ContractError(
                f"{location}.required_checks keys must be non-empty strings"
            )
        if isinstance(expected, bool):
            parsed_expected: bool | int = expected
        elif isinstance(expected, int):
            parsed_expected = expected
        else:
            raise ContractError(
                f"{location}.required_checks.{name} must be a boolean or integer"
            )
        parsed_checks.append((name.strip(), parsed_expected))
    return TranslatedManifestContract(
        filename=_required_string(raw, "filename", location),
        artifact=_required_string(raw, "artifact", location),
        audit_status=_required_string(raw, "audit_status", location),
        required_checks=tuple(parsed_checks),
    )


def _require_columns(
    contract: TranslatedInputContract,
) -> None:
    static_names = {column.name for column in contract.static.columns}
    dynamic_names = {column.name for column in contract.dynamic.columns}
    required_static = {
        contract.stay_id_column,
        contract.canonical_hospital_id_column,
        contract.source_hospital_code_column,
    }
    required_dynamic = required_static | {
        contract.relative_time_column,
        contract.anchored_time_column,
    }
    missing_static = sorted(required_static - static_names)
    missing_dynamic = sorted(required_dynamic - dynamic_names)
    if missing_static or missing_dynamic:
        raise ContractError(
            "Translated audit columns are absent from the frozen schemas; "
            f"static_missing={missing_static}, dynamic_missing={missing_dynamic}"
        )


def load_translated_input_contract(
    path: str | Path,
) -> TranslatedInputContract:
    contract_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(contract_path)

    hospitals = raw.get("expected_hospital_codes")
    if (
        not isinstance(hospitals, list)
        or not hospitals
        or any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in hospitals
        )
    ):
        raise ContractError(
            "contract.expected_hospital_codes must be a non-empty integer list"
        )
    if len(hospitals) != len(set(hospitals)):
        raise ContractError("contract.expected_hospital_codes contains duplicates")

    identifiers = raw.get("identifiers")
    if not isinstance(identifiers, dict):
        raise ContractError("contract.identifiers must be a mapping")
    time = raw.get("time")
    if not isinstance(time, dict):
        raise ContractError("contract.time must be a mapping")
    tables = raw.get("tables")
    if not isinstance(tables, dict):
        raise ContractError("contract.tables must be a mapping")
    downstream = raw.get("downstream")
    if not isinstance(downstream, dict):
        raise ContractError("contract.downstream must be a mapping")

    contract = TranslatedInputContract(
        contract_version=_required_string(raw, "contract_version", "contract"),
        dataset=_required_string(raw, "dataset", "contract"),
        expected_hospital_codes=tuple(hospitals),
        producer=_parse_producer(raw.get("producer")),
        manifest=_parse_manifest(raw.get("manifest")),
        static=parse_table_contract(tables.get("static"), "static"),
        dynamic=parse_table_contract(tables.get("dynamic"), "dynamic"),
        stay_id_column=_required_string(
            identifiers,
            "stay_id_column",
            "contract.identifiers",
        ),
        canonical_hospital_id_column=_required_string(
            identifiers,
            "canonical_hospital_id_column",
            "contract.identifiers",
        ),
        source_hospital_code_column=_required_string(
            identifiers,
            "source_hospital_code_column",
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
        preserve_all_input_columns=_required_true(
            downstream,
            "preserve_all_input_columns",
            "contract.downstream",
        ),
        preserve_row_count_and_order=_required_true(
            downstream,
            "preserve_row_count_and_order",
            "contract.downstream",
        ),
        preserve_identifiers_and_time_keys=_required_true(
            downstream,
            "preserve_identifiers_and_time_keys",
            "contract.downstream",
        ),
        value_changes_require_approved_cleaning_rules=_required_true(
            downstream,
            "value_changes_require_approved_cleaning_rules",
            "contract.downstream",
        ),
        source_path=contract_path,
    )
    _require_columns(contract)
    return contract
