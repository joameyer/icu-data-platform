from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError


ALLOWED_KINDS = frozenset(
    {"numeric", "categorical", "identifier", "free_text", "temporal"}
)
ALLOWED_PARSERS = frozenset({"decimal_comma", "numeric_list"})
ALLOWED_NONBLOCKING_TRUNCATION_CLASSES = frozenset(
    {
        "decimal_comma_syntax_candidate",
        "custom_parsed_numeric_list",
        "custom_parsed_numeric_list_with_missing_elements",
    }
)
ALLOWED_CANONICAL_VALUE_TYPES = frozenset({"list_float64"})
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
HOSPITAL_ID = re.compile(r"^asic_UK\d{2}$")
RULE_ID = re.compile(r"^[A-Z][A-Z0-9-]*$")


@dataclass(frozen=True)
class CandidateRule:
    hospital: str | None
    table: str
    raw_name: str
    occurrence: int | None
    target: str | None
    kind: str
    status: str
    review_rule_id: str | None = None
    approved_parser: str | None = None
    approved_list_missing_tokens: tuple[str, ...] = ()
    allowed_tokens: tuple[str, ...] = ()
    approved_missing_sentinel_tokens: tuple[str, ...] = ()
    nonblocking_truncation_classes: tuple[str, ...] = ()
    evidence_nonempty_count: int | None = None
    evidence_run_id: str | None = None
    canonical_value_type: str | None = None
    expected_unit: str | None = None
    preserve_list_order: bool = False
    preserve_duplicates: bool = False
    supersedes_rule_id: str | None = None


@dataclass(frozen=True)
class SchemaTokenPolicy:
    policy_version: str
    required_inventory_artifact_version: str
    required_ingestion_manifest_version: str
    required_ingestion_audit_artifact_version: str
    allowed_input_blockers: tuple[str, ...]
    rows_per_batch: int
    max_examples_per_token_class: int
    store_direct_numeric_examples: bool
    store_identifier_examples: bool
    store_free_text_examples: bool
    textual_missing_candidates: frozenset[str]
    candidate_rules: tuple[CandidateRule, ...]
    reviewed_rule_count: int
    reviewed_by: str
    reviewed_at: str
    reviewed_evidence_run_id: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path
    registry_path: Path
    reviewed_registry_path: Path

    def candidate_for(
        self,
        table: str,
        raw_name: str,
        occurrence: int,
        hospital: str | None = None,
    ) -> CandidateRule | None:
        hospital_scopes = (hospital, None) if hospital is not None else (None,)
        for hospital_scope in hospital_scopes:
            for occurrence_scope in (occurrence, None):
                rule = next(
                    (
                        item
                        for item in self.candidate_rules
                        if item.hospital == hospital_scope
                        and item.table == table
                        and item.raw_name == raw_name
                        and item.occurrence == occurrence_scope
                    ),
                    None,
                )
                if rule is not None:
                    return rule
        return None


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _boolean(value: Any, location: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location} must be a boolean")
    return value


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must contain non-empty strings")
    items = tuple(value)
    if len(items) != len(set(items)):
        raise ConfigurationError(f"{location} contains duplicates")
    return items


def _load_candidate_rules(registry_path: Path) -> tuple[CandidateRule, ...]:
    raw = load_yaml_mapping(registry_path, "Candidate harmonization registry")
    if raw.get("registry_version") != "0.1":
        raise ConfigurationError("Candidate registry version must be 0.1")
    if raw.get("status") != "prior_decisions_seed_only_requires_raw_revalidation":
        raise ConfigurationError("Candidate registry must remain explicitly unapproved")
    defaults = _mapping(raw.get("default_candidate_kind"), "registry.default_candidate_kind")
    targets = _mapping(raw.get("candidate_targets"), "registry.candidate_targets")
    overrides = _mapping(
        raw.get("candidate_kind_overrides"),
        "registry.candidate_kind_overrides",
    )
    occurrence_overrides = _mapping(
        raw.get("occurrence_overrides", {}),
        "registry.occurrence_overrides",
    )
    rules: list[CandidateRule] = []
    for table in ("static", "dynamic"):
        default_kind = defaults.get(table)
        if default_kind not in ALLOWED_KINDS:
            raise ConfigurationError(
                f"registry.default_candidate_kind.{table} is invalid"
            )
        table_targets = _mapping(targets.get(table), f"registry.candidate_targets.{table}")
        table_kinds = _mapping(
            overrides.get(table, {}),
            f"registry.candidate_kind_overrides.{table}",
        )
        unknown_kind_names = sorted(set(table_kinds) - set(table_targets))
        if unknown_kind_names:
            raise ConfigurationError(
                f"Candidate kind overrides lack targets: {unknown_kind_names}"
            )
        for raw_name, target in table_targets.items():
            if not isinstance(raw_name, str) or not raw_name:
                raise ConfigurationError("Candidate raw names must be non-empty strings")
            if target is not None and (
                not isinstance(target, str) or not SNAKE_CASE.fullmatch(target)
            ):
                raise ConfigurationError(
                    f"Candidate target for {table}.{raw_name} must be snake_case or null"
                )
            kind = table_kinds.get(raw_name, default_kind)
            if kind not in ALLOWED_KINDS:
                raise ConfigurationError(
                    f"Candidate kind for {table}.{raw_name} is invalid"
                )
            rules.append(
                CandidateRule(
                    hospital=None,
                    table=table,
                    raw_name=raw_name,
                    occurrence=None,
                    target=target,
                    kind=kind,
                    status="prior_reference_candidate_requires_raw_revalidation",
                )
            )
        table_occurrences = _mapping(
            occurrence_overrides.get(table, {}),
            f"registry.occurrence_overrides.{table}",
        )
        for raw_name, raw_occurrences in table_occurrences.items():
            if raw_name not in table_targets:
                raise ConfigurationError(
                    f"Occurrence override lacks a base target: {table}.{raw_name}"
                )
            occurrences = _mapping(
                raw_occurrences,
                f"registry.occurrence_overrides.{table}.{raw_name}",
            )
            for occurrence_text, raw_rule in occurrences.items():
                try:
                    occurrence = int(occurrence_text)
                except (TypeError, ValueError) as exc:
                    raise ConfigurationError("Occurrence keys must be positive integers") from exc
                if occurrence <= 0:
                    raise ConfigurationError("Occurrence keys must be positive integers")
                rule = _mapping(
                    raw_rule,
                    f"registry.occurrence_overrides.{table}.{raw_name}.{occurrence_text}",
                )
                target = rule.get("candidate_target")
                if target is not None and (
                    not isinstance(target, str) or not SNAKE_CASE.fullmatch(target)
                ):
                    raise ConfigurationError("Occurrence target must be snake_case or null")
                kind = rule.get("candidate_kind")
                if kind not in ALLOWED_KINDS:
                    raise ConfigurationError("Occurrence candidate kind is invalid")
                status = required_string(rule, "candidate_status", "occurrence override")
                rules.append(
                    CandidateRule(None, table, raw_name, occurrence, target, kind, status)
                )
    scopes = [
        (rule.hospital, rule.table, rule.raw_name, rule.occurrence)
        for rule in rules
    ]
    if len(scopes) != len(set(scopes)):
        raise ConfigurationError("Candidate registry contains duplicate scopes")
    return tuple(rules)


def _optional_string_list(value: Any, location: str) -> tuple[str, ...]:
    if value is None:
        return ()
    return _string_list(value, location)


def _load_reviewed_rules(
    registry_path: Path,
    candidate_rules: tuple[CandidateRule, ...],
) -> tuple[tuple[CandidateRule, ...], str, str, str]:
    raw = load_yaml_mapping(registry_path, "Reviewed raw-v3 rules")
    if raw.get("review_registry_version") != "0.2":
        raise ConfigurationError("Reviewed-rule registry version must be 0.2")
    if raw.get("status") != "approved_partial_raw_v3_rules":
        raise ConfigurationError("Reviewed-rule registry is not approved")
    review = _mapping(raw.get("review"), "reviewed_registry.review")
    reviewed_by = required_string(
        review, "reviewer_role", "reviewed_registry.review"
    )
    reviewed_at = required_string(
        review, "approved_at", "reviewed_registry.review"
    )
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", reviewed_at):
        raise ConfigurationError("Reviewed-rule approval date must be YYYY-MM-DD")
    evidence_run_id = required_string(
        review,
        "evidence_schema_token_inventory_run_id",
        "reviewed_registry.review",
    )
    raw_rules = raw.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ConfigurationError("reviewed_registry.rules must be a non-empty list")
    rules: list[CandidateRule] = []
    for index, raw_rule in enumerate(raw_rules):
        location = f"reviewed_registry.rules[{index}]"
        rule = _mapping(raw_rule, location)
        rule_id = required_string(rule, "rule_id", location)
        if not RULE_ID.fullmatch(rule_id):
            raise ConfigurationError(f"{location}.rule_id is invalid")
        hospital = required_string(rule, "hospital", location)
        if not HOSPITAL_ID.fullmatch(hospital):
            raise ConfigurationError(f"{location}.hospital is invalid")
        table = required_string(rule, "table", location)
        if table not in {"static", "dynamic"}:
            raise ConfigurationError(f"{location}.table is invalid")
        raw_name = rule.get("raw_name")
        if not isinstance(raw_name, str) or not raw_name:
            raise ConfigurationError(f"{location}.raw_name must be non-empty")
        occurrence = rule.get("occurrence")
        if occurrence is not None and (
            not isinstance(occurrence, int)
            or isinstance(occurrence, bool)
            or occurrence <= 0
        ):
            raise ConfigurationError(f"{location}.occurrence must be positive or null")
        target = rule.get("candidate_target")
        if target is not None and (
            not isinstance(target, str) or not SNAKE_CASE.fullmatch(target)
        ):
            raise ConfigurationError(f"{location}.candidate_target is invalid")
        kind = rule.get("candidate_kind")
        if kind not in ALLOWED_KINDS:
            raise ConfigurationError(f"{location}.candidate_kind is invalid")
        parser = rule.get("approved_parser")
        if parser is not None and parser not in ALLOWED_PARSERS:
            raise ConfigurationError(f"{location}.approved_parser is invalid")
        allowed_tokens = _optional_string_list(
            rule.get("allowed_tokens"), f"{location}.allowed_tokens"
        )
        list_missing_tokens = _optional_string_list(
            rule.get("approved_list_missing_tokens"),
            f"{location}.approved_list_missing_tokens",
        )
        sentinel_tokens = _optional_string_list(
            rule.get("approved_missing_sentinel_tokens"),
            f"{location}.approved_missing_sentinel_tokens",
        )
        nonblocking_classes = _optional_string_list(
            rule.get("nonblocking_truncation_classes"),
            f"{location}.nonblocking_truncation_classes",
        )
        if not set(nonblocking_classes).issubset(
            ALLOWED_NONBLOCKING_TRUNCATION_CLASSES
        ):
            raise ConfigurationError(
                f"{location}.nonblocking_truncation_classes contains an unapproved class"
            )
        if allowed_tokens and kind != "categorical":
            raise ConfigurationError(
                f"{location}.allowed_tokens requires a categorical rule"
            )
        if sentinel_tokens and kind != "numeric":
            raise ConfigurationError(
                f"{location}.approved_missing_sentinel_tokens requires numeric kind"
            )
        canonical_value_type = rule.get("canonical_value_type")
        expected_unit = rule.get("expected_unit")
        preserve_list_order = rule.get("preserve_list_order", False)
        preserve_duplicates = rule.get("preserve_duplicates", False)
        if parser == "numeric_list":
            if canonical_value_type not in ALLOWED_CANONICAL_VALUE_TYPES:
                raise ConfigurationError(
                    f"{location}.canonical_value_type must be list_float64"
                )
            if not isinstance(expected_unit, str) or not expected_unit:
                raise ConfigurationError(
                    f"{location}.expected_unit must be non-empty"
                )
            if not list_missing_tokens:
                raise ConfigurationError(
                    f"{location}.approved_list_missing_tokens must be explicit"
                )
            if preserve_list_order is not True or preserve_duplicates is not True:
                raise ConfigurationError(
                    f"{location} must preserve list order and duplicates"
                )
        elif any(
            value not in {None, False, ()}
            for value in (
                canonical_value_type,
                expected_unit,
                list_missing_tokens,
                preserve_list_order,
                preserve_duplicates,
            )
        ):
            raise ConfigurationError(
                f"{location} has list settings without the numeric_list parser"
            )
        evidence_count = rule.get("evidence_nonempty_count")
        if (
            not isinstance(evidence_count, int)
            or isinstance(evidence_count, bool)
            or evidence_count < 0
        ):
            raise ConfigurationError(
                f"{location}.evidence_nonempty_count must be a non-negative integer"
            )
        base = next(
            (
                item
                for item in candidate_rules
                if item.hospital is None
                and item.table == table
                and item.raw_name == raw_name
                and item.occurrence in {occurrence, None}
            ),
            None,
        )
        supersedes_candidate_target = rule.get("supersedes_candidate_target")
        if base is not None and base.target != target:
            if supersedes_candidate_target != base.target:
                raise ConfigurationError(
                    f"{location} changes the candidate target from {base.target!r} "
                    "without an explicit supersedes_candidate_target"
                )
        elif supersedes_candidate_target is not None:
            raise ConfigurationError(
                f"{location}.supersedes_candidate_target is unnecessary or invalid"
            )
        evidence_rule_id = rule.get("evidence_run_id", evidence_run_id)
        if not isinstance(evidence_rule_id, str) or not evidence_rule_id:
            raise ConfigurationError(f"{location}.evidence_run_id is invalid")
        supersedes_rule_id = rule.get("supersedes_rule_id")
        if supersedes_rule_id is not None and (
            not isinstance(supersedes_rule_id, str)
            or not RULE_ID.fullmatch(supersedes_rule_id)
        ):
            raise ConfigurationError(f"{location}.supersedes_rule_id is invalid")
        rules.append(
            CandidateRule(
                hospital=hospital,
                table=table,
                raw_name=raw_name,
                occurrence=occurrence,
                target=target,
                kind=kind,
                status="human_approved_raw_v3_rule",
                review_rule_id=rule_id,
                approved_parser=parser,
                approved_list_missing_tokens=list_missing_tokens,
                allowed_tokens=allowed_tokens,
                approved_missing_sentinel_tokens=sentinel_tokens,
                nonblocking_truncation_classes=nonblocking_classes,
                evidence_nonempty_count=evidence_count,
                evidence_run_id=evidence_rule_id,
                canonical_value_type=canonical_value_type,
                expected_unit=expected_unit,
                preserve_list_order=preserve_list_order,
                preserve_duplicates=preserve_duplicates,
                supersedes_rule_id=supersedes_rule_id,
            )
        )
    ids = [rule.review_rule_id for rule in rules]
    if len(ids) != len(set(ids)):
        raise ConfigurationError("Reviewed-rule registry contains duplicate rule IDs")
    scopes = [
        (rule.hospital, rule.table, rule.raw_name, rule.occurrence)
        for rule in rules
    ]
    if len(scopes) != len(set(scopes)):
        raise ConfigurationError("Reviewed-rule registry contains duplicate scopes")
    superseded_raw = raw.get("superseded_rules")
    if not isinstance(superseded_raw, list):
        raise ConfigurationError("reviewed_registry.superseded_rules must be a list")
    superseded_ids: set[str] = set()
    active_ids = {rule.review_rule_id for rule in rules}
    for index, raw_superseded in enumerate(superseded_raw):
        location = f"reviewed_registry.superseded_rules[{index}]"
        superseded = _mapping(raw_superseded, location)
        old_id = required_string(superseded, "rule_id", location)
        replacement = required_string(superseded, "superseded_by", location)
        required_string(superseded, "reason", location)
        if not RULE_ID.fullmatch(old_id) or not RULE_ID.fullmatch(replacement):
            raise ConfigurationError(f"{location} contains an invalid rule ID")
        if replacement not in active_ids:
            raise ConfigurationError(f"{location}.superseded_by is not active")
        if old_id in superseded_ids:
            raise ConfigurationError("Superseded rule IDs must be unique")
        superseded_ids.add(old_id)
    for rule in rules:
        if rule.supersedes_rule_id is not None and (
            rule.supersedes_rule_id not in superseded_ids
        ):
            raise ConfigurationError(
                f"Reviewed rule {rule.review_rule_id} lacks superseded-rule history"
            )
    return tuple(rules), reviewed_by, reviewed_at, evidence_run_id


def load_schema_token_policy(path: str | Path) -> SchemaTokenPolicy:
    policy_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(policy_path, "Schema-token inventory policy")
    if raw.get("policy_version") != "0.3":
        raise ConfigurationError("Schema-token inventory policy version must be 0.3")
    if raw.get("status") != "approved_for_read_only_schema_token_inventory":
        raise ConfigurationError("Schema-token inventory policy is not approved")
    blockers = _string_list(raw.get("allowed_input_blockers"), "policy.allowed_input_blockers")
    if blockers != ("provisional_archive_owner_confirmation",):
        raise ConfigurationError("Only archive owner confirmation may cross this gate")
    registry_path = resolve_path(
        required_string(raw, "candidate_registry", "policy"),
        policy_path,
    )
    reviewed_registry_path = resolve_path(
        required_string(raw, "reviewed_rules", "policy"),
        policy_path,
    )
    candidate_rules = _load_candidate_rules(registry_path)
    (
        reviewed_rules,
        reviewed_by,
        reviewed_at,
        reviewed_evidence_run_id,
    ) = _load_reviewed_rules(reviewed_registry_path, candidate_rules)
    scan = _mapping(raw.get("scan"), "policy.scan")
    classification = _mapping(raw.get("classification"), "policy.classification")
    textual = frozenset(
        value.casefold()
        for value in _string_list(
            classification.get("textual_missing_candidates"),
            "policy.classification.textual_missing_candidates",
        )
    )
    reporting = _mapping(raw.get("reporting"), "policy.reporting")
    return SchemaTokenPolicy(
        policy_version="0.3",
        required_inventory_artifact_version=required_string(
            raw, "required_inventory_artifact_version", "policy"
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "policy"
        ),
        required_ingestion_audit_artifact_version=required_string(
            raw, "required_ingestion_audit_artifact_version", "policy"
        ),
        allowed_input_blockers=blockers,
        rows_per_batch=_positive_int(scan.get("rows_per_batch"), "policy.scan.rows_per_batch"),
        max_examples_per_token_class=_positive_int(
            scan.get("max_examples_per_token_class"),
            "policy.scan.max_examples_per_token_class",
        ),
        store_direct_numeric_examples=_boolean(
            scan.get("store_direct_numeric_examples"),
            "policy.scan.store_direct_numeric_examples",
        ),
        store_identifier_examples=_boolean(
            scan.get("store_identifier_examples"),
            "policy.scan.store_identifier_examples",
        ),
        store_free_text_examples=_boolean(
            scan.get("store_free_text_examples"),
            "policy.scan.store_free_text_examples",
        ),
        textual_missing_candidates=textual,
        candidate_rules=(*candidate_rules, *reviewed_rules),
        reviewed_rule_count=len(reviewed_rules),
        reviewed_by=reviewed_by,
        reviewed_at=reviewed_at,
        reviewed_evidence_run_id=reviewed_evidence_run_id,
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "policy.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "policy.reporting"
        ),
        source_path=policy_path,
        registry_path=registry_path,
        reviewed_registry_path=reviewed_registry_path,
    )
