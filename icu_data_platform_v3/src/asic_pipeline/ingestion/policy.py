from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError


PROVENANCE_KEYS = (
    "source_hospital_folder",
    "canonical_hospital_id",
    "source_filename",
    "source_file_id",
    "source_file_order",
    "source_row_number",
    "source_order",
    "filename_stay_id",
    "in_file_stay_id",
    "source_schema_variant_id",
)


@dataclass(frozen=True)
class SourceSelectionPolicy:
    static_classification: str
    dynamic_classification: str
    excluded_classifications: tuple[str, ...]
    require_complete_csv_inspection: bool
    revalidate_source_sha256: bool
    revalidate_header_and_row_count: bool
    reject_symbolic_links: bool


@dataclass(frozen=True)
class DuplicatePhysicalNameRule:
    rule_id: str
    hospital_folder: str
    table: str
    raw_column_name: str
    expected_occurrences: int
    physical_names: tuple[str, ...]
    meaning: str


@dataclass(frozen=True)
class IngestionPolicy:
    policy_version: str
    status: str
    required_inventory_artifact_version: str
    allowed_inventory_blockers: tuple[str, ...]
    source_selection: SourceSelectionPolicy
    duplicate_physical_name_rules: tuple[DuplicatePhysicalNameRule, ...]
    provenance_columns: dict[str, str]
    parquet_compression: str
    ingestion_manifest_version: str
    source_path: Path

    def duplicate_physical_name_rule_for(
        self,
        hospital_folder: str,
        table: str,
        raw_column_name: str,
        occurrence_count: int,
    ) -> DuplicatePhysicalNameRule | None:
        return next(
            (
                rule
                for rule in self.duplicate_physical_name_rules
                if rule.hospital_folder == hospital_folder
                and rule.table == table
                and rule.raw_column_name == raw_column_name
                and rule.expected_occurrences == occurrence_count
            ),
            None,
        )


def _mapping(raw: Any, location: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(raw)


def _string_list(raw: Any, location: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise ConfigurationError(f"{location} must be a list")
    if any(not isinstance(value, str) or not value for value in raw):
        raise ConfigurationError(f"{location} must contain only non-empty strings")
    values = tuple(raw)
    if len(values) != len(set(values)):
        raise ConfigurationError(f"{location} contains duplicates")
    return values


def _required_boolean(mapping: dict[str, Any], key: str, location: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location}.{key} must be a boolean")
    return value


def _positive_int(raw: Any, location: str) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool) or raw <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return raw


def _parse_duplicate_physical_name_rules(
    raw: Any,
) -> tuple[DuplicatePhysicalNameRule, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigurationError(
            "policy.duplicate_physical_names must be a non-empty list"
        )
    rules: list[DuplicatePhysicalNameRule] = []
    for index, raw_rule in enumerate(raw):
        location = f"policy.duplicate_physical_names[{index}]"
        rule = _mapping(raw_rule, location)
        if rule.get("approval_status") != "approved_by_data_owner":
            raise ConfigurationError(f"{location} is not approved by the data owner")
        hospital_folder = required_string(rule, "hospital_folder", location)
        if len(hospital_folder) != 2 or not hospital_folder.isdigit():
            raise ConfigurationError(
                f"{location}.hospital_folder must be a two-digit folder"
            )
        table = required_string(rule, "table", location)
        if table not in {"static", "dynamic"}:
            raise ConfigurationError(f"{location}.table must be static or dynamic")
        expected = _positive_int(
            rule.get("expected_occurrences"),
            f"{location}.expected_occurrences",
        )
        physical_raw = rule.get("physical_names")
        if not isinstance(physical_raw, list) or len(physical_raw) != expected:
            raise ConfigurationError(
                f"{location}.physical_names must define every occurrence"
            )
        physical_names: list[str] = []
        for occurrence, raw_name in enumerate(physical_raw, start=1):
            name_location = f"{location}.physical_names[{occurrence - 1}]"
            name_rule = _mapping(raw_name, name_location)
            if name_rule.get("occurrence") != occurrence:
                raise ConfigurationError(
                    f"{name_location}.occurrence must be {occurrence}"
                )
            physical_name = required_string(name_rule, "name", name_location)
            if physical_name.startswith("__v3_"):
                raise ConfigurationError(
                    f"{name_location}.name cannot use the provenance prefix"
                )
            physical_names.append(physical_name)
        if len(physical_names) != len(set(physical_names)):
            raise ConfigurationError(f"{location}.physical_names must be unique")
        rules.append(
            DuplicatePhysicalNameRule(
                rule_id=required_string(rule, "rule_id", location),
                hospital_folder=hospital_folder,
                table=table,
                raw_column_name=required_string(
                    rule, "raw_column_name", location
                ),
                expected_occurrences=expected,
                physical_names=tuple(physical_names),
                meaning=required_string(rule, "meaning", location),
            )
        )
    rule_ids = [rule.rule_id for rule in rules]
    scopes = [
        (rule.hospital_folder, rule.table, rule.raw_column_name)
        for rule in rules
    ]
    if len(rule_ids) != len(set(rule_ids)) or len(scopes) != len(set(scopes)):
        raise ConfigurationError("Duplicate physical-name rules must be unique")
    return tuple(rules)


def load_ingestion_policy(path: str | Path) -> IngestionPolicy:
    policy_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(policy_path, "Ingestion policy")
    blockers = _string_list(
        raw.get("allowed_inventory_blockers"),
        "policy.allowed_inventory_blockers",
    )
    if blockers != ("provisional_archive_owner_confirmation",):
        raise ConfigurationError(
            "The only allowed inventory blocker is provisional archive owner confirmation"
        )

    selection_raw = _mapping(raw.get("source_selection"), "policy.source_selection")
    selection = SourceSelectionPolicy(
        static_classification=required_string(
            selection_raw, "static_classification", "policy.source_selection"
        ),
        dynamic_classification=required_string(
            selection_raw, "dynamic_classification", "policy.source_selection"
        ),
        excluded_classifications=_string_list(
            selection_raw.get("excluded_classifications"),
            "policy.source_selection.excluded_classifications",
        ),
        require_complete_csv_inspection=_required_boolean(
            selection_raw,
            "require_complete_csv_inspection",
            "policy.source_selection",
        ),
        revalidate_source_sha256=_required_boolean(
            selection_raw, "revalidate_source_sha256", "policy.source_selection"
        ),
        revalidate_header_and_row_count=_required_boolean(
            selection_raw,
            "revalidate_header_and_row_count",
            "policy.source_selection",
        ),
        reject_symbolic_links=_required_boolean(
            selection_raw, "reject_symbolic_links", "policy.source_selection"
        ),
    )
    if selection.static_classification != "selected_static_candidate":
        raise ConfigurationError("Ingestion must select only the default static source")
    if selection.dynamic_classification != "dynamic_candidate":
        raise ConfigurationError("Ingestion must select only classified dynamic CSVs")
    required_exclusions = {
        "additional_static_candidate",
        "provisionally_excluded_archive",
    }
    if not required_exclusions.issubset(selection.excluded_classifications):
        raise ConfigurationError(
            "Ingestion policy must exclude duplicate static candidates and the reviewed archive"
        )
    if not all(
        (
            selection.require_complete_csv_inspection,
            selection.revalidate_source_sha256,
            selection.revalidate_header_and_row_count,
            selection.reject_symbolic_links,
        )
    ):
        raise ConfigurationError("All lossless source validation controls must remain enabled")

    value_contract = _mapping(raw.get("raw_value_contract"), "policy.raw_value_contract")
    exact_values = {
        "arrow_type": "string",
        "empty_cell_representation": "empty_string",
        "absent_source_column_representation": "parquet_null",
        "infer_clinical_types": False,
        "parse_clinical_values": False,
        "apply_textual_missing_policy": False,
        "rename_raw_columns": False,
        "duplicate_physical_name_disambiguation": "approved_rules_only",
    }
    for key, expected in exact_values.items():
        if value_contract.get(key) != expected:
            raise ConfigurationError(
                f"policy.raw_value_contract.{key} must be {expected!r}"
            )

    duplicate_name_rules = _parse_duplicate_physical_name_rules(
        raw.get("duplicate_physical_names")
    )

    provenance = _mapping(raw.get("provenance_columns"), "policy.provenance_columns")
    if set(provenance) != set(PROVENANCE_KEYS):
        raise ConfigurationError(
            "policy.provenance_columns must define the complete provenance contract"
        )
    if any(not isinstance(value, str) or not value for value in provenance.values()):
        raise ConfigurationError("Provenance column names must be non-empty strings")
    if len(set(provenance.values())) != len(provenance):
        raise ConfigurationError("Provenance column names must be unique")
    if any(not value.startswith("__v3_") for value in provenance.values()):
        raise ConfigurationError("Provenance columns must use the reserved __v3_ prefix")

    parquet = _mapping(raw.get("parquet"), "policy.parquet")
    reporting = _mapping(raw.get("reporting"), "policy.reporting")
    return IngestionPolicy(
        policy_version=required_string(raw, "policy_version", "policy"),
        status=required_string(raw, "status", "policy"),
        required_inventory_artifact_version=required_string(
            raw, "required_inventory_artifact_version", "policy"
        ),
        allowed_inventory_blockers=blockers,
        source_selection=selection,
        duplicate_physical_name_rules=duplicate_name_rules,
        provenance_columns={str(key): str(value) for key, value in provenance.items()},
        parquet_compression=required_string(parquet, "compression", "policy.parquet"),
        ingestion_manifest_version=required_string(
            reporting, "ingestion_manifest_version", "policy.reporting"
        ),
        source_path=policy_path,
    )
