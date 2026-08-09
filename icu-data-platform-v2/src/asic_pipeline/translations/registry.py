from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import re
from typing import Any

import yaml

from asic_pipeline.contracts import InputContract
from asic_pipeline.errors import ContractError


_ENGLISH_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
MERGE_POLICY_FAIL_ON_UNEQUAL = "fail_if_both_nonmissing_and_unequal"
MERGE_POLICY_BINARY_OR = "binary_or_fail_on_nonbinary"
_APPROVED_MERGE_POLICIES = {
    MERGE_POLICY_FAIL_ON_UNEQUAL,
    MERGE_POLICY_BINARY_OR,
}
VALUE_NORMALIZATION_STRIP = "strip"
VALUE_NORMALIZATION_STRIP_LOWER = "strip_lower"
VALUE_NORMALIZATION_STRIP_UPPER = "strip_upper"
_APPROVED_VALUE_NORMALIZATIONS = {
    VALUE_NORMALIZATION_STRIP,
    VALUE_NORMALIZATION_STRIP_LOWER,
    VALUE_NORMALIZATION_STRIP_UPPER,
}
_APPROVED_VALUE_OUTPUT_TYPES = {"large_string", "boolean"}
_TRANSLATED_ADDITIVE_FIELDS = {
    "static": ("hospital_id",),
    "dynamic": ("hospital_id",),
}


@dataclass(frozen=True)
class TranslationEntry:
    source_name: str
    english_name: str | None


@dataclass(frozen=True)
class MergeRule:
    name: str
    table: str
    source_columns: tuple[str, ...]
    target_name: str
    conflict_policy: str
    allowed_nonmissing_hospitals: tuple[tuple[str, tuple[int, ...]], ...]

    def allowed_hospitals_for(self, source_column: str) -> tuple[int, ...] | None:
        return dict(self.allowed_nonmissing_hospitals).get(source_column)


@dataclass(frozen=True)
class DropRule:
    table: str
    source_column: str
    precondition: str
    reason: str


@dataclass(frozen=True)
class OutputGroup:
    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class CategoricalValueMapping:
    table: str
    target_name: str
    normalization: str
    output_arrow_type: str
    source_to_target: tuple[tuple[str, str | bool | None], ...]

    def mapping(self) -> dict[str, str | bool | None]:
        return dict(self.source_to_target)


@dataclass(frozen=True)
class NumericMissingSentinelRule:
    table: str
    target_name: str
    values: tuple[int | float, ...]
    reason: str


@dataclass(frozen=True)
class HospitalSpecificSemanticSplitRule:
    name: str
    table: str
    source_column: str
    base_target_name: str
    new_target_name: str
    selected_hospital_codes: tuple[int, ...]
    reason: str


@dataclass(frozen=True)
class TranslationRegistry:
    policy_version: str
    status: str
    source_contract_version: str
    documentation: str
    static: tuple[TranslationEntry, ...]
    dynamic: tuple[TranslationEntry, ...]
    static_output_groups: tuple[OutputGroup, ...]
    dynamic_output_groups: tuple[OutputGroup, ...]
    merge_rules: tuple[MergeRule, ...]
    drop_rules: tuple[DropRule, ...]
    categorical_value_mappings: tuple[CategoricalValueMapping, ...]
    numeric_missing_sentinel_rules: tuple[NumericMissingSentinelRule, ...]
    hospital_specific_semantic_split_rules: tuple[
        HospitalSpecificSemanticSplitRule,
        ...,
    ]
    collision_group_count: int
    approved_merge_count: int
    unresolved_collision_group_count: int
    categorical_inventory_counts: dict[str, int]
    categorical_inventory_columns: dict[str, tuple[str, ...]]
    semantic_review_counts: dict[str, int]
    source_path: Path

    def entries_for(self, table: str) -> tuple[TranslationEntry, ...]:
        if table == "static":
            return self.static
        if table == "dynamic":
            return self.dynamic
        raise KeyError(f"Unknown registry table: {table}")

    def merge_rules_for(self, table: str) -> tuple[MergeRule, ...]:
        return tuple(rule for rule in self.merge_rules if rule.table == table)

    def output_groups_for(self, table: str) -> tuple[OutputGroup, ...]:
        if table == "static":
            return self.static_output_groups
        if table == "dynamic":
            return self.dynamic_output_groups
        raise KeyError(f"Unknown registry table: {table}")

    def output_order_for(self, table: str) -> tuple[str, ...]:
        return tuple(
            column
            for group in self.output_groups_for(table)
            for column in group.columns
        )

    def drop_rules_for(self, table: str) -> tuple[DropRule, ...]:
        return tuple(rule for rule in self.drop_rules if rule.table == table)

    def categorical_columns_for(self, table: str) -> tuple[str, ...]:
        try:
            return self.categorical_inventory_columns[table]
        except KeyError as exc:
            raise KeyError(f"Unknown registry table: {table}") from exc

    def value_mappings_for(
        self,
        table: str,
    ) -> tuple[CategoricalValueMapping, ...]:
        if table not in {"static", "dynamic"}:
            raise KeyError(f"Unknown registry table: {table}")
        return tuple(
            rule for rule in self.categorical_value_mappings if rule.table == table
        )

    def value_mapping_for(
        self,
        table: str,
        target_name: str,
    ) -> CategoricalValueMapping | None:
        return next(
            (
                rule
                for rule in self.value_mappings_for(table)
                if rule.target_name == target_name
            ),
            None,
        )

    def numeric_missing_sentinel_rules_for(
        self,
        table: str,
    ) -> tuple[NumericMissingSentinelRule, ...]:
        if table not in {"static", "dynamic"}:
            raise KeyError(f"Unknown registry table: {table}")
        return tuple(
            rule
            for rule in self.numeric_missing_sentinel_rules
            if rule.table == table
        )

    def numeric_missing_sentinel_rule_for(
        self,
        table: str,
        target_name: str,
    ) -> NumericMissingSentinelRule | None:
        return next(
            (
                rule
                for rule in self.numeric_missing_sentinel_rules_for(table)
                if rule.target_name == target_name
            ),
            None,
        )

    def hospital_specific_semantic_split_rules_for(
        self,
        table: str,
    ) -> tuple[HospitalSpecificSemanticSplitRule, ...]:
        if table not in {"static", "dynamic"}:
            raise KeyError(f"Unknown registry table: {table}")
        return tuple(
            rule
            for rule in self.hospital_specific_semantic_split_rules
            if rule.table == table
        )

    def hospital_specific_semantic_split_for_base_target(
        self,
        table: str,
        target_name: str,
    ) -> HospitalSpecificSemanticSplitRule | None:
        return next(
            (
                rule
                for rule in self.hospital_specific_semantic_split_rules_for(table)
                if rule.base_target_name == target_name
            ),
            None,
        )


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            value = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ContractError(f"Translation registry does not exist: {path}") from exc
    except yaml.YAMLError as exc:
        raise ContractError(
            f"Invalid YAML in translation registry {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ContractError(
            f"Translation registry must contain a YAML mapping: {path}"
        )
    return value


def _required_string(mapping: dict[str, Any], key: str, location: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{location}.{key} must be a non-empty string")
    return value.strip()


def _source_names(contract: InputContract, table: str) -> list[str]:
    table_contract = contract.static if table == "static" else contract.dynamic
    return [column.name for column in table_contract.columns]


def _parse_translations(
    raw: Any,
    table: str,
    contract: InputContract,
) -> tuple[TranslationEntry, ...]:
    if not isinstance(raw, dict) or not raw:
        raise ContractError(
            f"registry.translations.{table} must be a non-empty mapping"
        )

    source_names = list(raw)
    expected_source_names = _source_names(contract, table)
    missing = [name for name in expected_source_names if name not in raw]
    extra = [name for name in source_names if name not in expected_source_names]
    if missing or extra or source_names != expected_source_names:
        raise ContractError(
            f"registry.translations.{table} must match contract names and order "
            f"exactly; missing={missing}, extra={extra}, order_matches="
            f"{source_names == expected_source_names}"
        )

    entries: list[TranslationEntry] = []
    for source_name, english_name in raw.items():
        if english_name is not None and (
            not isinstance(english_name, str)
            or not _ENGLISH_NAME_PATTERN.fullmatch(english_name)
        ):
            raise ContractError(
                f"English name for {table}.{source_name} must be null or lower "
                f"snake_case, got {english_name!r}"
            )
        entries.append(
            TranslationEntry(
                source_name=source_name,
                english_name=english_name,
            )
        )
    return tuple(entries)


def _parse_allowed_hospitals(
    raw: Any,
    location: str,
    sources: tuple[str, ...],
    contract: InputContract,
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    if raw is None:
        return ()
    if not isinstance(raw, dict):
        raise ContractError(f"{location} must be a mapping")
    unknown_sources = sorted(set(raw) - set(sources))
    if unknown_sources:
        raise ContractError(
            f"{location} references sources outside its merge group: "
            f"{unknown_sources}"
        )
    expected_codes = set(contract.expected_hospital_codes)
    parsed: list[tuple[str, tuple[int, ...]]] = []
    for source in sources:
        if source not in raw:
            continue
        codes = raw[source]
        if (
            not isinstance(codes, list)
            or not codes
            or any(not isinstance(code, int) or isinstance(code, bool) for code in codes)
        ):
            raise ContractError(f"{location}.{source} must be a non-empty integer list")
        if len(codes) != len(set(codes)):
            raise ContractError(f"{location}.{source} contains duplicate hospitals")
        invalid_codes = sorted(set(codes) - expected_codes)
        if invalid_codes:
            raise ContractError(
                f"{location}.{source} contains hospitals outside the contract: "
                f"{invalid_codes}"
            )
        parsed.append((source, tuple(codes)))
    return tuple(parsed)


def _parse_collision_groups(
    raw: Any,
    contract: InputContract,
    translations: dict[str, tuple[TranslationEntry, ...]],
) -> tuple[tuple[MergeRule, ...], int, int]:
    if not isinstance(raw, dict):
        raise ContractError("policy.column_relationships must be a mapping")

    merge_rules: list[MergeRule] = []
    unresolved_count = 0
    grouped_sources: set[tuple[str, str]] = set()
    for group_name, group in raw.items():
        location = f"policy.column_relationships.{group_name}"
        if not isinstance(group, dict):
            raise ContractError(f"{location} must be a mapping")
        table = group.get("table")
        if table not in {"static", "dynamic"}:
            raise ContractError(f"{location}.table is invalid")
        sources_raw = group.get("source_columns")
        if (
            not isinstance(sources_raw, list)
            or len(sources_raw) < 2
            or any(not isinstance(source, str) for source in sources_raw)
        ):
            raise ContractError(
                f"{location}.source_columns must contain at least two names"
            )
        sources = tuple(sources_raw)
        if len(sources) != len(set(sources)):
            raise ContractError(f"{location}.source_columns contains duplicates")
        valid_sources = set(_source_names(contract, table))
        unknown = sorted(set(sources) - valid_sources)
        if unknown:
            raise ContractError(f"{location} has unknown sources: {unknown}")
        overlaps = sorted(
            source for source in sources if (table, source) in grouped_sources
        )
        if overlaps:
            raise ContractError(
                f"{location} reuses sources from another collision group: {overlaps}"
            )
        grouped_sources.update((table, source) for source in sources)

        translation_map = {
            entry.source_name: entry.english_name for entry in translations[table]
        }
        targets = [translation_map[source] for source in sources]
        status = group.get("status")
        if status == "unresolved_do_not_merge":
            if None in targets or len(targets) != len(set(targets)):
                raise ContractError(
                    f"{location} must retain distinct non-null English names"
                )
            unresolved_count += 1
            continue
        if status != "approved_merge":
            raise ContractError(
                f"{location}.status must be 'approved_merge' or "
                "'unresolved_do_not_merge'"
            )

        target_name = _required_string(group, "target_name", location)
        if not _ENGLISH_NAME_PATTERN.fullmatch(target_name):
            raise ContractError(f"{location}.target_name must be lower snake_case")
        if any(target != target_name for target in targets):
            raise ContractError(
                f"{location} sources must all translate to {target_name!r}"
            )
        conflict_policy = _required_string(group, "conflict_policy", location)
        if conflict_policy not in _APPROVED_MERGE_POLICIES:
            raise ContractError(
                f"{location}.conflict_policy must be one of "
                f"{sorted(_APPROVED_MERGE_POLICIES)}"
            )
        if conflict_policy == MERGE_POLICY_BINARY_OR and len(sources) != 2:
            raise ContractError(
                f"{location} binary-OR policy requires exactly two sources"
            )
        allowed = _parse_allowed_hospitals(
            group.get("allowed_nonmissing_hospitals"),
            f"{location}.allowed_nonmissing_hospitals",
            sources,
            contract,
        )
        merge_rules.append(
            MergeRule(
                name=group_name,
                table=table,
                source_columns=sources,
                target_name=target_name,
                conflict_policy=conflict_policy,
                allowed_nonmissing_hospitals=allowed,
            )
        )

    return tuple(merge_rules), unresolved_count, len(raw)


def _parse_drop_rules(
    raw: Any,
    contract: InputContract,
    translations: dict[str, tuple[TranslationEntry, ...]],
    merge_rules: tuple[MergeRule, ...],
) -> tuple[DropRule, ...]:
    if not isinstance(raw, dict):
        raise ContractError("registry.drop_rules must be a mapping")
    merged_sources = {
        (rule.table, source)
        for rule in merge_rules
        for source in rule.source_columns
    }
    parsed: list[DropRule] = []
    for table in ("static", "dynamic"):
        table_raw = raw.get(table)
        if not isinstance(table_raw, dict):
            raise ContractError(f"registry.drop_rules.{table} must be a mapping")
        valid_sources = set(_source_names(contract, table))
        translation_map = {
            entry.source_name: entry.english_name for entry in translations[table]
        }
        for source, definition in table_raw.items():
            location = f"registry.drop_rules.{table}.{source}"
            if source not in valid_sources:
                raise ContractError(f"{location} references an unknown source column")
            if (table, source) in merged_sources:
                raise ContractError(f"{location} cannot also be in a merge group")
            if translation_map[source] is not None:
                raise ContractError(
                    f"{location} source must have a null translation target"
                )
            if not isinstance(definition, dict):
                raise ContractError(f"{location} must be a mapping")
            if definition.get("status") != "approved_drop":
                raise ContractError(f"{location}.status must be 'approved_drop'")
            if definition.get("precondition") != "all_missing":
                raise ContractError(f"{location}.precondition must be 'all_missing'")
            parsed.append(
                DropRule(
                    table=table,
                    source_column=source,
                    precondition="all_missing",
                    reason=_required_string(definition, "reason", location),
                )
            )

    null_targets = {
        (table, entry.source_name)
        for table in ("static", "dynamic")
        for entry in translations[table]
        if entry.english_name is None
    }
    dropped_sources = {(rule.table, rule.source_column) for rule in parsed}
    if null_targets != dropped_sources:
        raise ContractError(
            "Null translation targets and approved drop rules must match exactly; "
            f"without_drop={sorted(null_targets - dropped_sources)}, "
            f"without_null_target={sorted(dropped_sources - null_targets)}"
        )
    return tuple(parsed)


def _validate_output_name_uniqueness(
    translations: dict[str, tuple[TranslationEntry, ...]],
    merge_rules: tuple[MergeRule, ...],
) -> None:
    approved_groups = {
        (rule.table, rule.target_name): set(rule.source_columns)
        for rule in merge_rules
    }
    for table in ("static", "dynamic"):
        target_sources: dict[str, set[str]] = {}
        for entry in translations[table]:
            if entry.english_name is not None:
                target_sources.setdefault(entry.english_name, set()).add(
                    entry.source_name
                )
        invalid = {
            target: sorted(sources)
            for target, sources in target_sources.items()
            if len(sources) > 1
            and approved_groups.get((table, target)) != sources
        }
        if invalid:
            raise ContractError(
                f"registry.translations.{table} has unapproved duplicate English "
                f"targets: {invalid}"
            )


def _parse_hospital_specific_semantic_splits(
    raw: Any,
    contract: InputContract,
    translations: dict[str, tuple[TranslationEntry, ...]],
) -> tuple[HospitalSpecificSemanticSplitRule, ...]:
    if not isinstance(raw, dict):
        raise ContractError(
            "registry.hospital_specific_semantic_splits must be a mapping"
        )

    translated_sources: dict[str, dict[str, set[str]]] = {
        table: {} for table in ("static", "dynamic")
    }
    translation_maps: dict[str, dict[str, str | None]] = {}
    for table in ("static", "dynamic"):
        translation_maps[table] = {
            entry.source_name: entry.english_name for entry in translations[table]
        }
        for entry in translations[table]:
            if entry.english_name is not None:
                translated_sources[table].setdefault(
                    entry.english_name,
                    set(),
                ).add(entry.source_name)

    parsed: list[HospitalSpecificSemanticSplitRule] = []
    used_base_targets: set[tuple[str, str]] = set()
    used_new_targets: set[tuple[str, str]] = set()
    expected_codes = set(contract.expected_hospital_codes)
    for rule_name, definition in raw.items():
        location = f"registry.hospital_specific_semantic_splits.{rule_name}"
        if (
            not isinstance(rule_name, str)
            or not _ENGLISH_NAME_PATTERN.fullmatch(rule_name)
        ):
            raise ContractError(
                f"{location} rule name must be lower snake_case"
            )
        if not isinstance(definition, dict):
            raise ContractError(f"{location} must be a mapping")
        if definition.get("status") != "approved_semantic_split":
            raise ContractError(
                f"{location}.status must be 'approved_semantic_split'"
            )
        if definition.get("action") != "move_selected_hospital_values":
            raise ContractError(
                f"{location}.action must be 'move_selected_hospital_values'"
            )

        table = definition.get("table")
        if table not in {"static", "dynamic"}:
            raise ContractError(f"{location}.table is invalid")
        source_column = _required_string(definition, "source_column", location)
        if source_column not in translation_maps[table]:
            raise ContractError(
                f"{location}.source_column references an unknown source column"
            )
        base_target_name = _required_string(
            definition,
            "base_target_name",
            location,
        )
        if translation_maps[table][source_column] != base_target_name:
            raise ContractError(
                f"{location}.source_column must translate to base_target_name "
                f"{base_target_name!r}"
            )
        if translated_sources[table].get(base_target_name) != {source_column}:
            raise ContractError(
                f"{location}.base_target_name must be produced by exactly its "
                "declared source column"
            )

        new_target_name = _required_string(
            definition,
            "new_target_name",
            location,
        )
        if not _ENGLISH_NAME_PATTERN.fullmatch(new_target_name):
            raise ContractError(
                f"{location}.new_target_name must be lower snake_case"
            )
        if (
            new_target_name in translated_sources[table]
            or new_target_name in _TRANSLATED_ADDITIVE_FIELDS[table]
        ):
            raise ContractError(
                f"{location}.new_target_name conflicts with another output"
            )
        base_key = (table, base_target_name)
        new_key = (table, new_target_name)
        if base_key in used_base_targets:
            raise ContractError(
                f"{location}.base_target_name is already conditionally split"
            )
        if new_key in used_new_targets:
            raise ContractError(
                f"{location}.new_target_name is duplicated"
            )

        hospital_codes = definition.get("selected_hospital_codes")
        if (
            not isinstance(hospital_codes, list)
            or not hospital_codes
            or any(
                not isinstance(code, int) or isinstance(code, bool)
                for code in hospital_codes
            )
        ):
            raise ContractError(
                f"{location}.selected_hospital_codes must be a non-empty "
                "integer list"
            )
        if len(hospital_codes) != len(set(hospital_codes)):
            raise ContractError(
                f"{location}.selected_hospital_codes contains duplicates"
            )
        invalid_codes = sorted(set(hospital_codes) - expected_codes)
        if invalid_codes:
            raise ContractError(
                f"{location}.selected_hospital_codes contains hospitals outside "
                f"the contract: {invalid_codes}"
            )

        parsed.append(
            HospitalSpecificSemanticSplitRule(
                name=rule_name,
                table=table,
                source_column=source_column,
                base_target_name=base_target_name,
                new_target_name=new_target_name,
                selected_hospital_codes=tuple(hospital_codes),
                reason=_required_string(definition, "reason", location),
            )
        )
        used_base_targets.add(base_key)
        used_new_targets.add(new_key)
    return tuple(parsed)


def _parse_output_groups(
    raw: Any,
    translations: dict[str, tuple[TranslationEntry, ...]],
    semantic_split_rules: tuple[HospitalSpecificSemanticSplitRule, ...],
) -> dict[str, tuple[OutputGroup, ...]]:
    if not isinstance(raw, dict):
        raise ContractError("registry.translated_output_groups must be a mapping")

    parsed: dict[str, tuple[OutputGroup, ...]] = {}
    for table in ("static", "dynamic"):
        table_raw = raw.get(table)
        location = f"registry.translated_output_groups.{table}"
        if not isinstance(table_raw, dict) or not table_raw:
            raise ContractError(f"{location} must be a non-empty mapping")

        groups: list[OutputGroup] = []
        flattened: list[str] = []
        for group_name, columns_raw in table_raw.items():
            group_location = f"{location}.{group_name}"
            if (
                not isinstance(group_name, str)
                or not _ENGLISH_NAME_PATTERN.fullmatch(group_name)
            ):
                raise ContractError(
                    f"Output group name {table}.{group_name!r} must be lower "
                    "snake_case"
                )
            if (
                not isinstance(columns_raw, list)
                or not columns_raw
                or any(
                    not isinstance(column, str)
                    or not _ENGLISH_NAME_PATTERN.fullmatch(column)
                    for column in columns_raw
                )
            ):
                raise ContractError(
                    f"{group_location} must be a non-empty list of lower "
                    "snake_case output names"
                )
            columns = tuple(columns_raw)
            groups.append(OutputGroup(name=group_name, columns=columns))
            flattened.extend(columns)

        duplicate_names = sorted(
            {name for name in flattened if flattened.count(name) > 1}
        )
        expected_names = {
            entry.english_name
            for entry in translations[table]
            if entry.english_name is not None
        }
        expected_names.update(_TRANSLATED_ADDITIVE_FIELDS[table])
        expected_names.update(
            rule.new_target_name
            for rule in semantic_split_rules
            if rule.table == table
        )
        observed_names = set(flattened)
        missing_names = sorted(expected_names - observed_names)
        unknown_names = sorted(observed_names - expected_names)
        if duplicate_names or missing_names or unknown_names:
            raise ContractError(
                f"{location} must contain every translated output exactly once; "
                f"duplicates={duplicate_names}, missing={missing_names}, "
                f"unknown={unknown_names}"
            )
        parsed[table] = tuple(groups)
    return parsed


def _validate_reference_list(
    raw: Any,
    location: str,
    valid_sources: set[str],
) -> int:
    if not isinstance(raw, list):
        raise ContractError(f"{location} must be a list")
    if len(raw) != len(set(raw)):
        raise ContractError(f"{location} contains duplicate source names")
    unknown = sorted(set(raw) - valid_sources)
    if unknown:
        raise ContractError(f"{location} references unknown source names: {unknown}")
    return len(raw)


def _validate_reference_mapping(
    raw: Any,
    location: str,
    valid_sources: set[str],
) -> int:
    if not isinstance(raw, dict):
        raise ContractError(f"{location} must be a mapping")
    unknown = sorted(set(raw) - valid_sources)
    if unknown:
        raise ContractError(f"{location} references unknown source names: {unknown}")
    return len(raw)


def _validate_table_reference_section(
    raw: Any,
    location: str,
    contract: InputContract,
    mapping_values: bool,
) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise ContractError(f"{location} must be a mapping")
    counts: dict[str, int] = {}
    for table in ("static", "dynamic"):
        valid_sources = set(_source_names(contract, table))
        table_raw = raw.get(table)
        if mapping_values:
            counts[table] = _validate_reference_mapping(
                table_raw,
                f"{location}.{table}",
                valid_sources,
            )
        else:
            counts[table] = _validate_reference_list(
                table_raw,
                f"{location}.{table}",
                valid_sources,
            )
    return counts


def _translated_reference_names(
    raw: Any,
    location: str,
    translations: dict[str, tuple[TranslationEntry, ...]],
) -> dict[str, tuple[str, ...]]:
    translated: dict[str, tuple[str, ...]] = {}
    for table in ("static", "dynamic"):
        source_names = raw[table]
        translation_map = {
            entry.source_name: entry.english_name for entry in translations[table]
        }
        output_names: list[str] = []
        for source_name in source_names:
            output_name = translation_map[source_name]
            if output_name is None:
                raise ContractError(
                    f"{location}.{table} references conditionally dropped source "
                    f"column {source_name!r}"
                )
            if output_name not in output_names:
                output_names.append(output_name)
        translated[table] = tuple(output_names)
    return translated


def normalize_categorical_source_value(value: str, normalization: str) -> str:
    normalized = value.strip()
    if normalization == VALUE_NORMALIZATION_STRIP_LOWER:
        return normalized.lower()
    if normalization == VALUE_NORMALIZATION_STRIP_UPPER:
        return normalized.upper()
    if normalization == VALUE_NORMALIZATION_STRIP:
        return normalized
    raise ValueError(f"Unsupported categorical value normalization: {normalization}")


def _parse_categorical_value_mappings(
    raw: Any,
    translations: dict[str, tuple[TranslationEntry, ...]],
    categorical_columns: dict[str, tuple[str, ...]],
) -> tuple[CategoricalValueMapping, ...]:
    if not isinstance(raw, dict):
        raise ContractError("registry.categorical_value_mappings must be a mapping")

    parsed: list[CategoricalValueMapping] = []
    for table in ("static", "dynamic"):
        table_raw = raw.get(table)
        location = f"registry.categorical_value_mappings.{table}"
        if not isinstance(table_raw, dict):
            raise ContractError(f"{location} must be a mapping")

        translated_names = {
            entry.english_name
            for entry in translations[table]
            if entry.english_name is not None
        }
        eligible_names = set(categorical_columns[table])
        for target_name, definition in table_raw.items():
            rule_location = f"{location}.{target_name}"
            if target_name not in translated_names:
                raise ContractError(
                    f"{rule_location} is not a translated {table} output"
                )
            if target_name not in eligible_names:
                raise ContractError(
                    f"{rule_location} was not included in the categorical inventory"
                )
            if not isinstance(definition, dict):
                raise ContractError(f"{rule_location} must be a mapping")
            if definition.get("status") != "approved_mapping":
                raise ContractError(
                    f"{rule_location}.status must be 'approved_mapping'"
                )
            normalization = _required_string(
                definition,
                "normalization",
                rule_location,
            )
            if normalization not in _APPROVED_VALUE_NORMALIZATIONS:
                raise ContractError(
                    f"{rule_location}.normalization must be one of "
                    f"{sorted(_APPROVED_VALUE_NORMALIZATIONS)}"
                )
            output_arrow_type = _required_string(
                definition,
                "output_arrow_type",
                rule_location,
            )
            if output_arrow_type not in _APPROVED_VALUE_OUTPUT_TYPES:
                raise ContractError(
                    f"{rule_location}.output_arrow_type must be one of "
                    f"{sorted(_APPROVED_VALUE_OUTPUT_TYPES)}"
                )
            values = definition.get("values")
            if not isinstance(values, dict) or not values:
                raise ContractError(f"{rule_location}.values must be a non-empty mapping")

            source_to_target: list[tuple[str, str | bool | None]] = []
            normalized_sources: set[str] = set()
            for source_value, target_value in values.items():
                value_location = f"{rule_location}.values.{source_value}"
                if not isinstance(source_value, str):
                    raise ContractError(
                        f"{rule_location}.values keys must all be strings"
                    )
                normalized_source = normalize_categorical_source_value(
                    source_value,
                    normalization,
                )
                if not normalized_source:
                    raise ContractError(
                        f"{value_location} normalizes to an empty source value"
                    )
                if normalized_source in normalized_sources:
                    raise ContractError(
                        f"{rule_location}.values has duplicate normalized source "
                        f"value {normalized_source!r}"
                    )
                normalized_sources.add(normalized_source)

                if output_arrow_type == "large_string":
                    valid_target = target_value is None or (
                        isinstance(target_value, str) and bool(target_value)
                    )
                else:
                    valid_target = target_value is None or isinstance(
                        target_value,
                        bool,
                    )
                if not valid_target:
                    raise ContractError(
                        f"{value_location} is incompatible with output type "
                        f"{output_arrow_type!r}: {target_value!r}"
                    )
                source_to_target.append((normalized_source, target_value))

            parsed.append(
                CategoricalValueMapping(
                    table=table,
                    target_name=target_name,
                    normalization=normalization,
                    output_arrow_type=output_arrow_type,
                    source_to_target=tuple(source_to_target),
                )
            )
    return tuple(parsed)


def _parse_numeric_missing_sentinels(
    raw: Any,
    translations: dict[str, tuple[TranslationEntry, ...]],
    contract: InputContract,
) -> tuple[NumericMissingSentinelRule, ...]:
    if not isinstance(raw, dict):
        raise ContractError("registry.numeric_missing_sentinels must be a mapping")

    parsed: list[NumericMissingSentinelRule] = []
    for table in ("static", "dynamic"):
        table_raw = raw.get(table)
        location = f"registry.numeric_missing_sentinels.{table}"
        if not isinstance(table_raw, dict):
            raise ContractError(f"{location} must be a mapping")

        translated_sources: dict[str, list[str]] = {}
        for entry in translations[table]:
            if entry.english_name is not None:
                translated_sources.setdefault(entry.english_name, []).append(
                    entry.source_name
                )
        table_contract = contract.static if table == "static" else contract.dynamic
        source_types = {
            column.name: {
                arrow_type for _, arrow_type in column.arrow_types
            }
            for column in table_contract.columns
        }

        for target_name, definition in table_raw.items():
            rule_location = f"{location}.{target_name}"
            sources = translated_sources.get(target_name)
            if sources is None:
                raise ContractError(
                    f"{rule_location} is not a translated {table} output"
                )
            if not isinstance(definition, dict):
                raise ContractError(f"{rule_location} must be a mapping")
            if definition.get("status") != "approved_missing_sentinel":
                raise ContractError(
                    f"{rule_location}.status must be "
                    "'approved_missing_sentinel'"
                )

            non_numeric_types = sorted(
                {
                    arrow_type
                    for source in sources
                    for arrow_type in source_types[source]
                    if arrow_type
                    not in {
                        "int8",
                        "int16",
                        "int32",
                        "int64",
                        "uint8",
                        "uint16",
                        "uint32",
                        "uint64",
                        "float",
                        "double",
                    }
                }
            )
            if non_numeric_types:
                raise ContractError(
                    f"{rule_location} requires numeric source columns, found "
                    f"{non_numeric_types}"
                )

            values = definition.get("values")
            if not isinstance(values, list) or not values:
                raise ContractError(
                    f"{rule_location}.values must be a non-empty numeric list"
                )
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                for value in values
            ):
                raise ContractError(
                    f"{rule_location}.values must contain only finite numbers"
                )
            if len(values) != len(set(values)):
                raise ContractError(f"{rule_location}.values contains duplicates")

            parsed.append(
                NumericMissingSentinelRule(
                    table=table,
                    target_name=target_name,
                    values=tuple(values),
                    reason=_required_string(definition, "reason", rule_location),
                )
            )
    return tuple(parsed)


def _validate_planned_additive_fields(
    raw: Any,
    translations: dict[str, tuple[TranslationEntry, ...]],
) -> None:
    if not isinstance(raw, dict):
        raise ContractError("registry.planned_additive_fields must be a mapping")
    for table in ("static", "dynamic"):
        fields = raw.get(table)
        if not isinstance(fields, dict):
            raise ContractError(
                f"registry.planned_additive_fields.{table} must be a mapping"
            )
        translated_names = {
            entry.english_name
            for entry in translations[table]
            if entry.english_name is not None
        }
        for name, definition in fields.items():
            if not isinstance(name, str) or not _ENGLISH_NAME_PATTERN.fullmatch(name):
                raise ContractError(
                    f"Planned additive field {table}.{name!r} is not lower snake_case"
                )
            if name in translated_names:
                raise ContractError(
                    f"Planned additive field {table}.{name} conflicts with a source "
                    "translation"
                )
            if not isinstance(definition, dict):
                raise ContractError(
                    f"registry.planned_additive_fields.{table}.{name} must be a mapping"
                )


def load_translation_registry(
    path: str | Path,
    contract: InputContract,
) -> TranslationRegistry:
    registry_path = Path(path).expanduser().resolve()
    raw = _load_yaml_mapping(registry_path)
    source_contract_version = _required_string(
        raw,
        "source_contract_version",
        "registry",
    )
    if source_contract_version != contract.contract_version:
        raise ContractError(
            "Translation registry source_contract_version does not match the loaded "
            f"contract: {source_contract_version!r} != {contract.contract_version!r}"
        )

    translations_raw = raw.get("translations")
    if not isinstance(translations_raw, dict):
        raise ContractError("registry.translations must be a mapping")
    static = _parse_translations(translations_raw.get("static"), "static", contract)
    dynamic = _parse_translations(
        translations_raw.get("dynamic"),
        "dynamic",
        contract,
    )
    translations = {"static": static, "dynamic": dynamic}

    merge_rules, unresolved_count, collision_count = _parse_collision_groups(
        raw.get("column_relationships"),
        contract,
        translations,
    )
    drop_rules = _parse_drop_rules(
        raw.get("drop_rules"),
        contract,
        translations,
        merge_rules,
    )
    _validate_output_name_uniqueness(translations, merge_rules)
    hospital_specific_semantic_split_rules = (
        _parse_hospital_specific_semantic_splits(
            raw.get("hospital_specific_semantic_splits"),
            contract,
            translations,
        )
    )
    output_groups = _parse_output_groups(
        raw.get("translated_output_groups"),
        translations,
        hospital_specific_semantic_split_rules,
    )
    _validate_planned_additive_fields(
        raw.get("planned_additive_fields"),
        translations,
    )
    categorical_raw = raw.get("categorical_value_inventory_required")
    categorical_counts = _validate_table_reference_section(
        categorical_raw,
        "registry.categorical_value_inventory_required",
        contract,
        mapping_values=False,
    )
    categorical_columns = _translated_reference_names(
        categorical_raw,
        "registry.categorical_value_inventory_required",
        translations,
    )
    categorical_counts = {
        table: len(columns) for table, columns in categorical_columns.items()
    }
    categorical_value_mappings = _parse_categorical_value_mappings(
        raw.get("categorical_value_mappings"),
        translations,
        categorical_columns,
    )
    numeric_missing_sentinel_rules = _parse_numeric_missing_sentinels(
        raw.get("numeric_missing_sentinels"),
        translations,
        contract,
    )
    categorical_targets = {
        (rule.table, rule.target_name) for rule in categorical_value_mappings
    }
    numeric_sentinel_targets = {
        (rule.table, rule.target_name)
        for rule in numeric_missing_sentinel_rules
    }
    overlap = sorted(categorical_targets & numeric_sentinel_targets)
    if overlap:
        raise ContractError(
            "Categorical value mappings and numeric missing sentinels overlap: "
            f"{overlap}"
        )
    semantic_counts = _validate_table_reference_section(
        raw.get("semantic_review_required"),
        "registry.semantic_review_required",
        contract,
        mapping_values=True,
    )

    return TranslationRegistry(
        policy_version=_required_string(raw, "policy_version", "registry"),
        status=_required_string(raw, "status", "registry"),
        source_contract_version=source_contract_version,
        documentation=_required_string(
            raw,
            "documentation",
            "registry",
        ),
        static=static,
        dynamic=dynamic,
        static_output_groups=output_groups["static"],
        dynamic_output_groups=output_groups["dynamic"],
        merge_rules=merge_rules,
        drop_rules=drop_rules,
        categorical_value_mappings=categorical_value_mappings,
        numeric_missing_sentinel_rules=numeric_missing_sentinel_rules,
        hospital_specific_semantic_split_rules=(
            hospital_specific_semantic_split_rules
        ),
        collision_group_count=collision_count,
        approved_merge_count=len(merge_rules),
        unresolved_collision_group_count=unresolved_count,
        categorical_inventory_counts=categorical_counts,
        categorical_inventory_columns=categorical_columns,
        semantic_review_counts=semantic_counts,
        source_path=registry_path,
    )
