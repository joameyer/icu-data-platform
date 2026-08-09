from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.schema_tokens.policy import CandidateRule, load_schema_token_policy


@dataclass(frozen=True)
class HarmonizationRule:
    rule_id: str
    hospital: str
    table: str
    raw_name: str
    occurrence: int | None
    target: str
    kind: str
    approved_parser: str | None
    approved_list_missing_tokens: tuple[str, ...]
    allowed_tokens: tuple[str, ...]
    approved_missing_sentinel_tokens: tuple[str, ...]
    canonical_value_type: str | None
    expected_unit: str | None
    preserve_list_order: bool
    preserve_duplicates: bool
    evidence_nonempty_count: int
    evidence_run_id: str

    @classmethod
    def from_candidate(cls, candidate: CandidateRule) -> HarmonizationRule:
        if (
            candidate.review_rule_id is None
            or candidate.hospital is None
            or candidate.target is None
            or candidate.evidence_nonempty_count is None
            or candidate.evidence_run_id is None
            or candidate.status != "human_approved_raw_v3_rule"
        ):
            raise ConfigurationError(
                "Only complete human-approved raw-v3 rules may enter harmonization"
            )
        return cls(
            rule_id=candidate.review_rule_id,
            hospital=candidate.hospital,
            table=candidate.table,
            raw_name=candidate.raw_name,
            occurrence=candidate.occurrence,
            target=candidate.target,
            kind=candidate.kind,
            approved_parser=candidate.approved_parser,
            approved_list_missing_tokens=candidate.approved_list_missing_tokens,
            allowed_tokens=candidate.allowed_tokens,
            approved_missing_sentinel_tokens=(
                candidate.approved_missing_sentinel_tokens
            ),
            canonical_value_type=candidate.canonical_value_type,
            expected_unit=candidate.expected_unit,
            preserve_list_order=candidate.preserve_list_order,
            preserve_duplicates=candidate.preserve_duplicates,
            evidence_nonempty_count=candidate.evidence_nonempty_count,
            evidence_run_id=candidate.evidence_run_id,
        )


@dataclass(frozen=True)
class HarmonizationBootstrapRegistry:
    bootstrap_policy_version: str
    status: str
    rules: tuple[HarmonizationRule, ...]
    production_execution_enabled: bool
    complete_harmonization_registry: bool
    union_schema_status: str
    variable_dictionary_status: str
    allow_synthetic_batch_transforms: bool
    allow_production_reads: bool
    allow_artifact_writes: bool
    allow_hospital_concatenation: bool
    source_path: Path
    reviewed_registry_path: Path

    def rule_for(
        self,
        hospital: str,
        table: str,
        raw_name: str,
        occurrence: int = 1,
    ) -> HarmonizationRule | None:
        for occurrence_scope in (occurrence, None):
            rule = next(
                (
                    item
                    for item in self.rules
                    if item.hospital == hospital
                    and item.table == table
                    and item.raw_name == raw_name
                    and item.occurrence == occurrence_scope
                ),
                None,
            )
            if rule is not None:
                return rule
        return None

    def require_production_ready(self) -> None:
        if not (
            self.production_execution_enabled
            and self.complete_harmonization_registry
            and self.union_schema_status == "approved"
            and self.variable_dictionary_status == "approved"
        ):
            raise HarmonizationError(
                "Harmonization production execution is blocked pending the "
                "complete reviewed registry, union schema, and variable dictionary"
            )


def _required_bool(mapping: dict[str, object], key: str, location: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location}.{key} must be true or false")
    return value


def load_harmonization_bootstrap_registry(
    path: str | Path,
) -> HarmonizationBootstrapRegistry:
    source_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source_path, "Harmonization bootstrap policy")
    if raw.get("bootstrap_policy_version") != "0.1":
        raise ConfigurationError("Harmonization bootstrap policy version must be 0.1")
    status = required_string(raw, "status", "harmonization bootstrap")
    if status != "partial_reviewed_rules_only_not_production_ready":
        raise ConfigurationError("Harmonization bootstrap status is invalid")
    schema_policy_path = resolve_path(
        required_string(raw, "schema_token_policy", "harmonization bootstrap"),
        source_path,
    )
    schema_policy = load_schema_token_policy(schema_policy_path)
    reviewed = tuple(
        HarmonizationRule.from_candidate(rule)
        for rule in schema_policy.candidate_rules
        if rule.status == "human_approved_raw_v3_rule"
    )
    if len(reviewed) != schema_policy.reviewed_rule_count:
        raise ConfigurationError(
            "Harmonization bootstrap does not contain every active reviewed rule"
        )
    scopes = [
        (rule.hospital, rule.table, rule.raw_name, rule.occurrence)
        for rule in reviewed
    ]
    if len(scopes) != len(set(scopes)):
        raise ConfigurationError("Harmonization bootstrap rule scopes collide")
    gates = raw.get("gates")
    scope = raw.get("scope")
    if not isinstance(gates, dict) or not isinstance(scope, dict):
        raise ConfigurationError("Harmonization bootstrap gates and scope are required")
    production_enabled = _required_bool(
        gates, "production_execution_enabled", "harmonization bootstrap.gates"
    )
    complete_registry = _required_bool(
        gates, "complete_harmonization_registry", "harmonization bootstrap.gates"
    )
    union_status = required_string(
        gates, "union_schema_status", "harmonization bootstrap.gates"
    )
    dictionary_status = required_string(
        gates, "variable_dictionary_status", "harmonization bootstrap.gates"
    )
    allow_synthetic = _required_bool(
        scope,
        "allow_synthetic_batch_transforms",
        "harmonization bootstrap.scope",
    )
    allow_production_reads = _required_bool(
        scope, "allow_production_reads", "harmonization bootstrap.scope"
    )
    allow_artifact_writes = _required_bool(
        scope, "allow_artifact_writes", "harmonization bootstrap.scope"
    )
    allow_concatenation = _required_bool(
        scope,
        "allow_hospital_concatenation",
        "harmonization bootstrap.scope",
    )
    if any(
        (
            production_enabled,
            complete_registry,
            allow_production_reads,
            allow_artifact_writes,
            allow_concatenation,
        )
    ) or union_status != "not_approved" or dictionary_status != "not_approved":
        raise ConfigurationError(
            "Bootstrap policy must remain fail-closed for production and artifacts"
        )
    if not allow_synthetic:
        raise ConfigurationError("Bootstrap policy must allow synthetic verification")
    return HarmonizationBootstrapRegistry(
        bootstrap_policy_version="0.1",
        status=status,
        rules=reviewed,
        production_execution_enabled=production_enabled,
        complete_harmonization_registry=complete_registry,
        union_schema_status=union_status,
        variable_dictionary_status=dictionary_status,
        allow_synthetic_batch_transforms=allow_synthetic,
        allow_production_reads=allow_production_reads,
        allow_artifact_writes=allow_artifact_writes,
        allow_hospital_concatenation=allow_concatenation,
        source_path=source_path,
        reviewed_registry_path=schema_policy.reviewed_registry_path,
    )
