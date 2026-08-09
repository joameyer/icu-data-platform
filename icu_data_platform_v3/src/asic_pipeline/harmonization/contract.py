from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError, HarmonizationError


RULE_ID = re.compile(r"^[A-Z][A-Z0-9-]*$")
HOSPITAL_ID = re.compile(r"^asic_UK\d{2}$")
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
REVIEW_ITEM_ID = re.compile(r"^R\d{4}$")
RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$")


@dataclass(frozen=True)
class RegistryContractPolicy:
    version: str
    required_schema_token_artifact_version: str
    required_registry_review_artifact_version: str
    required_complete_registry_version: str
    allowed_actions: frozenset[str]
    allowed_kinds: frozenset[str]
    allowed_value_types: frozenset[str]
    allowed_merge_policies: frozenset[str]
    allow_synthetic_schema_planning: bool
    allow_production_registry: bool
    allow_production_reads: bool
    allow_value_transforms: bool
    allow_artifact_writes: bool
    allow_hospital_concatenation: bool
    allow_union_schema_freeze: bool
    source_path: Path

    def require_production_ready(self) -> None:
        if not all(
            (
                self.allow_production_registry,
                self.allow_production_reads,
                self.allow_value_transforms,
                self.allow_artifact_writes,
                self.allow_hospital_concatenation,
                self.allow_union_schema_freeze,
            )
        ):
            raise HarmonizationError(
                "Production harmonization remains blocked by the synthetic-only registry contract"
            )


@dataclass(frozen=True)
class ReviewedRegistryRule:
    rule_id: str
    review_item_id: str
    hospital: str
    table: str
    raw_name: str
    occurrence: int
    action: str
    raw_nonempty_count: int
    target: str | None
    kind: str | None
    value_type: str | None
    unit: str | None
    parser_rule_ids: tuple[str, ...]
    missing_rule_ids: tuple[str, ...]
    categorical_rule_ids: tuple[str, ...]
    unit_rule_ids: tuple[str, ...]
    semantic_rule_ids: tuple[str, ...]
    alias_group_id: str | None
    merge_policy: str | None
    source_position: int | None

    @property
    def scope(self) -> tuple[str, str, str, int]:
        return self.hospital, self.table, self.raw_name, self.occurrence


@dataclass(frozen=True)
class FullyReviewedRegistry:
    version: str
    approved_by_role: str
    approved_at: str
    schema_token_run_id: str
    registry_review_run_id: str
    expected_occurrence_count: int
    rules: tuple[ReviewedRegistryRule, ...]
    source_path: Path

    def rules_for(self, hospital: str, table: str) -> tuple[ReviewedRegistryRule, ...]:
        return tuple(
            rule
            for rule in self.rules
            if rule.hospital == hospital and rule.table == table
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must contain non-empty strings")
    result = tuple(value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def _rule_id_list(value: Any, location: str) -> tuple[str, ...]:
    result = _string_list(value, location)
    if any(not RULE_ID.fullmatch(item) for item in result):
        raise ConfigurationError(f"{location} contains an invalid rule ID")
    return result


def _required_string_list(value: Any, location: str) -> frozenset[str]:
    result = _string_list(value, location)
    if not result:
        raise ConfigurationError(f"{location} must not be empty")
    return frozenset(result)


def _required_bool(mapping: dict[str, Any], key: str, location: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location}.{key} must be true or false")
    return value


def load_registry_contract_policy(path: str | Path) -> RegistryContractPolicy:
    source_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source_path, "Harmonization registry contract policy")
    if raw.get("registry_contract_policy_version") != "0.1":
        raise ConfigurationError("Registry contract policy version must be 0.1")
    if raw.get("status") != "approved_for_synthetic_contract_validation_only":
        raise ConfigurationError("Registry contract policy must remain synthetic-only")
    gates = _mapping(raw.get("gates"), "registry contract.gates")
    allow_synthetic = _required_bool(
        gates, "allow_synthetic_schema_planning", "registry contract.gates"
    )
    closed = {
        key: _required_bool(gates, key, "registry contract.gates")
        for key in (
            "allow_production_registry",
            "allow_production_reads",
            "allow_value_transforms",
            "allow_artifact_writes",
            "allow_hospital_concatenation",
            "allow_union_schema_freeze",
        )
    }
    if not allow_synthetic or any(closed.values()):
        raise ConfigurationError("Registry contract policy production gates must be closed")
    return RegistryContractPolicy(
        version="0.1",
        required_schema_token_artifact_version=required_string(
            raw, "required_schema_token_artifact_version", "registry contract"
        ),
        required_registry_review_artifact_version=required_string(
            raw, "required_registry_review_artifact_version", "registry contract"
        ),
        required_complete_registry_version=required_string(
            raw, "required_complete_registry_version", "registry contract"
        ),
        allowed_actions=_required_string_list(
            raw.get("allowed_actions"), "registry contract.allowed_actions"
        ),
        allowed_kinds=_required_string_list(
            raw.get("allowed_kinds"), "registry contract.allowed_kinds"
        ),
        allowed_value_types=_required_string_list(
            raw.get("allowed_value_types"), "registry contract.allowed_value_types"
        ),
        allowed_merge_policies=_required_string_list(
            raw.get("allowed_merge_policies"),
            "registry contract.allowed_merge_policies",
        ),
        allow_synthetic_schema_planning=allow_synthetic,
        source_path=source_path,
        **closed,
    )


def _optional_string(value: Any, location: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{location} must be a non-empty string or null")
    return value


def _load_rule(
    value: Any,
    index: int,
    policy: RegistryContractPolicy,
) -> ReviewedRegistryRule:
    location = f"complete registry.rules[{index}]"
    raw = _mapping(value, location)
    rule_id = required_string(raw, "rule_id", location)
    review_item_id = required_string(raw, "review_item_id", location)
    hospital = required_string(raw, "hospital", location)
    table = required_string(raw, "table", location)
    raw_name = raw.get("raw_name")
    occurrence = raw.get("occurrence")
    action = required_string(raw, "action", location)
    review_status = required_string(raw, "review_status", location)
    nonempty = raw.get("evidence_raw_nonempty_count")
    if not RULE_ID.fullmatch(rule_id):
        raise ConfigurationError(f"{location}.rule_id is invalid")
    if not REVIEW_ITEM_ID.fullmatch(review_item_id):
        raise ConfigurationError(f"{location}.review_item_id is invalid")
    if not HOSPITAL_ID.fullmatch(hospital):
        raise ConfigurationError(f"{location}.hospital is invalid")
    if table not in {"static", "dynamic"}:
        raise ConfigurationError(f"{location}.table is invalid")
    if not isinstance(raw_name, str) or not raw_name:
        raise ConfigurationError(f"{location}.raw_name must be non-empty")
    if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence <= 0:
        raise ConfigurationError(f"{location}.occurrence must be positive")
    if action not in policy.allowed_actions:
        raise ConfigurationError(f"{location}.action is invalid")
    if review_status != "human_approved_raw_v3":
        raise ConfigurationError(f"{location} is not human-approved raw-v3")
    if not isinstance(nonempty, int) or isinstance(nonempty, bool) or nonempty < 0:
        raise ConfigurationError(f"{location}.evidence_raw_nonempty_count is invalid")
    target = _optional_string(raw.get("target"), f"{location}.target")
    kind = _optional_string(raw.get("kind"), f"{location}.kind")
    value_type = _optional_string(raw.get("value_type"), f"{location}.value_type")
    unit = _optional_string(raw.get("unit"), f"{location}.unit")
    alias_group = _optional_string(
        raw.get("alias_group_id"), f"{location}.alias_group_id"
    )
    merge_policy = _optional_string(
        raw.get("merge_policy"), f"{location}.merge_policy"
    )
    source_position = raw.get("source_position")
    if source_position is not None and (
        not isinstance(source_position, int)
        or isinstance(source_position, bool)
        or source_position < 0
    ):
        raise ConfigurationError(f"{location}.source_position must be non-negative or null")
    if action == "retain":
        if target is None or not SNAKE_CASE.fullmatch(target):
            raise ConfigurationError(f"{location}.target must be snake_case")
        if kind not in policy.allowed_kinds:
            raise ConfigurationError(f"{location}.kind is invalid")
        if value_type not in policy.allowed_value_types:
            raise ConfigurationError(f"{location}.value_type is invalid")
        if unit is None:
            raise ConfigurationError(f"{location}.unit must be explicit")
    else:
        if any(item is not None for item in (target, kind, value_type, unit)):
            raise ConfigurationError(f"{location} drop rule cannot define a target contract")
        if nonempty != 0:
            raise ConfigurationError(f"{location} drop precondition requires zero non-empty values")
        if alias_group is not None or merge_policy is not None or source_position is not None:
            raise ConfigurationError(f"{location} drop rule cannot define an alias")
    if (alias_group is None) != (merge_policy is None):
        raise ConfigurationError(f"{location} alias group and merge policy must occur together")
    if merge_policy is not None and merge_policy not in policy.allowed_merge_policies:
        raise ConfigurationError(f"{location}.merge_policy is invalid")
    if merge_policy == "assemble_fixed_position_list":
        if source_position is None:
            raise ConfigurationError(
                f"{location}.source_position is required for fixed-position assembly"
            )
    elif source_position is not None:
        raise ConfigurationError(
            f"{location}.source_position is allowed only for fixed-position assembly"
        )
    return ReviewedRegistryRule(
        rule_id=rule_id,
        review_item_id=review_item_id,
        hospital=hospital,
        table=table,
        raw_name=raw_name,
        occurrence=occurrence,
        action=action,
        raw_nonempty_count=nonempty,
        target=target,
        kind=kind,
        value_type=value_type,
        unit=unit,
        parser_rule_ids=_rule_id_list(raw.get("parser_rule_ids"), f"{location}.parser_rule_ids"),
        missing_rule_ids=_rule_id_list(raw.get("missing_rule_ids"), f"{location}.missing_rule_ids"),
        categorical_rule_ids=_rule_id_list(
            raw.get("categorical_rule_ids"), f"{location}.categorical_rule_ids"
        ),
        unit_rule_ids=_rule_id_list(raw.get("unit_rule_ids"), f"{location}.unit_rule_ids"),
        semantic_rule_ids=_rule_id_list(
            raw.get("semantic_rule_ids"), f"{location}.semantic_rule_ids"
        ),
        alias_group_id=alias_group,
        merge_policy=merge_policy,
        source_position=source_position,
    )


def load_fully_reviewed_registry(
    path: str | Path,
    policy: RegistryContractPolicy,
) -> FullyReviewedRegistry:
    source_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source_path, "Complete reviewed harmonization registry")
    if raw.get("registry_version") != policy.required_complete_registry_version:
        raise ConfigurationError("Complete registry version is incompatible")
    if raw.get("status") != "human_approved_complete_raw_v3_registry":
        raise ConfigurationError("Complete registry is not human-approved")
    review = _mapping(raw.get("review"), "complete registry.review")
    approved_by = required_string(review, "approved_by_role", "complete registry.review")
    approved_at = required_string(review, "approved_at", "complete registry.review")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", approved_at):
        raise ConfigurationError("Complete registry approval date must be YYYY-MM-DD")
    schema_run = required_string(
        review, "schema_token_run_id", "complete registry.review"
    )
    registry_run = required_string(
        review, "registry_review_run_id", "complete registry.review"
    )
    if not RUN_ID.fullmatch(schema_run) or not RUN_ID.fullmatch(registry_run):
        raise ConfigurationError("Complete registry evidence run ID is invalid")
    expected_count = review.get("expected_occurrence_count")
    if not isinstance(expected_count, int) or isinstance(expected_count, bool) or expected_count <= 0:
        raise ConfigurationError("Complete registry expected occurrence count is invalid")
    raw_rules = raw.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ConfigurationError("Complete registry rules must be a non-empty list")
    rules = tuple(_load_rule(value, index, policy) for index, value in enumerate(raw_rules))
    if len(rules) != expected_count:
        raise ConfigurationError("Complete registry rule count differs from approved evidence")
    for label, values in (
        ("rule IDs", [rule.rule_id for rule in rules]),
        ("review item IDs", [rule.review_item_id for rule in rules]),
        ("rule scopes", [rule.scope for rule in rules]),
    ):
        if len(values) != len(set(values)):
            raise ConfigurationError(f"Complete registry contains duplicate {label}")
    contracts: dict[tuple[str, str], tuple[str, str, str]] = {}
    collisions: dict[tuple[str, str, str], list[ReviewedRegistryRule]] = {}
    for rule in rules:
        if rule.action != "retain":
            continue
        assert rule.target is not None
        contract_key = (rule.table, rule.target)
        contract_value = (rule.kind or "", rule.value_type or "", rule.unit or "")
        if contract_key in contracts and contracts[contract_key] != contract_value:
            raise ConfigurationError(
                "Complete registry gives one canonical target inconsistent kind/type/unit contracts"
            )
        contracts[contract_key] = contract_value
        collisions.setdefault((rule.hospital, rule.table, rule.target), []).append(rule)
    for collision_rules in collisions.values():
        if len(collision_rules) < 2:
            continue
        alias_pairs = {
            (rule.alias_group_id, rule.merge_policy) for rule in collision_rules
        }
        if len(alias_pairs) != 1 or None in next(iter(alias_pairs)):
            raise ConfigurationError(
                "Same-target source collisions require one explicit alias group and merge policy"
            )
        _, merge_policy = next(iter(alias_pairs))
        if merge_policy == "assemble_fixed_position_list":
            positions = sorted(rule.source_position for rule in collision_rules)
            if positions != list(range(len(collision_rules))):
                raise ConfigurationError(
                    "Fixed-position source assembly requires unique contiguous positions"
                )
    return FullyReviewedRegistry(
        version=policy.required_complete_registry_version,
        approved_by_role=approved_by,
        approved_at=approved_at,
        schema_token_run_id=schema_run,
        registry_review_run_id=registry_run,
        expected_occurrence_count=expected_count,
        rules=rules,
        source_path=source_path,
    )
