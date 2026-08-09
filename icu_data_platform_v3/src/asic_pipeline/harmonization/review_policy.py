from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.schema_tokens.config import (
    SchemaTokenConfig,
    load_schema_token_config,
)


SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")
RULE_ID = re.compile(r"^[A-Z][A-Z0-9-]*$")
HOSPITAL_ID = re.compile(r"^asic_UK\d{2}$")
ALLOWED_KINDS = frozenset(
    {"identifier", "free_text", "categorical", "numeric", "temporal"}
)


@dataclass(frozen=True)
class TargetProposal:
    value_type: str
    value_type_status: str
    unit: str
    unit_status: str


@dataclass(frozen=True)
class CategoricalValueCandidate:
    review_status: str
    normalization: str
    values: dict[str, object]


@dataclass(frozen=True)
class SemanticSplitCandidate:
    candidate_id: str
    hospital: str
    table: str
    current_target: str
    proposed_target: str
    proposed_value_type: str
    proposed_unit: str
    status: str
    evidence_reference: str


@dataclass(frozen=True)
class CandidateContract:
    version: str
    status: str
    provenance: tuple[str, ...]
    defaults: dict[str, TargetProposal]
    overrides: dict[tuple[str, str], TargetProposal]
    categorical_candidates: dict[tuple[str, str], CategoricalValueCandidate]
    semantic_split_candidates: tuple[SemanticSplitCandidate, ...]
    source_path: Path

    def proposal_for(self, table: str, target: str, kind: str) -> TargetProposal:
        try:
            return self.overrides.get((table, target), self.defaults[kind])
        except KeyError as exc:
            raise ConfigurationError(
                f"No candidate contract default exists for kind {kind!r}"
            ) from exc

    def categorical_for(
        self, table: str, target: str
    ) -> CategoricalValueCandidate | None:
        return self.categorical_candidates.get((table, target))

    def semantic_split_for(
        self, hospital: str, table: str, target: str
    ) -> SemanticSplitCandidate | None:
        return next(
            (
                item
                for item in self.semantic_split_candidates
                if item.hospital == hospital
                and item.table == table
                and item.current_target == target
            ),
            None,
        )


@dataclass(frozen=True)
class HarmonizationReviewPolicy:
    version: str
    required_schema_token_private_artifact_version: str
    required_schema_token_review_artifact_version: str
    required_schema_token_policy_version: str
    allowed_schema_token_blockers: tuple[str, ...]
    resolved_schema_token_blockers: tuple[str, ...]
    allow_private_schema_token_evidence_reads: bool
    private_artifact_version: str
    review_artifact_version: str
    schema_token_policy_path: Path
    candidate_contract: CandidateContract
    source_path: Path


@dataclass(frozen=True)
class HarmonizationReviewConfig:
    schema_tokens: SchemaTokenConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.schema_tokens.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.schema_tokens.ingestion.inventory.paths.reports


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must contain non-empty strings")
    result = tuple(value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def _required_bool(mapping: dict[str, Any], key: str, location: str) -> bool:
    value = mapping.get(key)
    if not isinstance(value, bool):
        raise ConfigurationError(f"{location}.{key} must be true or false")
    return value


def _proposal(value: Any, location: str) -> TargetProposal:
    raw = _mapping(value, location)
    return TargetProposal(
        value_type=required_string(raw, "value_type", location),
        value_type_status=required_string(raw, "value_type_status", location),
        unit=required_string(raw, "unit", location),
        unit_status=required_string(raw, "unit_status", location),
    )


def _load_candidate_contract(path: Path) -> CandidateContract:
    raw = load_yaml_mapping(path, "Candidate harmonization contract")
    if raw.get("candidate_contract_version") != "0.3":
        raise ConfigurationError("Candidate contract version must be 0.3")
    status = required_string(raw, "status", "candidate contract")
    if status != "prior_v2_legacy_and_partial_raw_v3_proposals_only":
        raise ConfigurationError("Candidate contract must remain explicitly unapproved")
    provenance = _string_list(raw.get("provenance"), "candidate contract.provenance")
    defaults_raw = _mapping(
        raw.get("default_by_kind"), "candidate contract.default_by_kind"
    )
    if set(defaults_raw) != set(ALLOWED_KINDS):
        raise ConfigurationError("Candidate contract must define every candidate kind")
    defaults = {
        kind: _proposal(value, f"candidate contract.default_by_kind.{kind}")
        for kind, value in defaults_raw.items()
    }
    overrides: dict[tuple[str, str], TargetProposal] = {}
    overrides_raw = _mapping(
        raw.get("target_overrides", {}), "candidate contract.target_overrides"
    )
    unknown_tables = set(overrides_raw) - {"static", "dynamic"}
    if unknown_tables:
        raise ConfigurationError(
            f"Candidate target overrides contain unknown tables: {sorted(unknown_tables)}"
        )
    for table, table_value in overrides_raw.items():
        table_overrides = _mapping(
            table_value, f"candidate contract.target_overrides.{table}"
        )
        for target, value in table_overrides.items():
            if not isinstance(target, str) or not SNAKE_CASE.fullmatch(target):
                raise ConfigurationError("Candidate target override names must be snake_case")
            partial = _mapping(
                value, f"candidate contract.target_overrides.{table}.{target}"
            )
            # An override may intentionally change only part of the numeric default.
            base = defaults["numeric"]
            merged = {
                "value_type": partial.get("value_type", base.value_type),
                "value_type_status": partial.get(
                    "value_type_status", base.value_type_status
                ),
                "unit": partial.get("unit", base.unit),
                "unit_status": partial.get("unit_status", base.unit_status),
            }
            overrides[(table, target)] = _proposal(
                merged, f"candidate contract.target_overrides.{table}.{target}"
            )
    categorical: dict[tuple[str, str], CategoricalValueCandidate] = {}
    categorical_raw = _mapping(
        raw.get("categorical_value_candidates", {}),
        "candidate contract.categorical_value_candidates",
    )
    unknown_tables = set(categorical_raw) - {"static", "dynamic"}
    if unknown_tables:
        raise ConfigurationError(
            f"Categorical candidates contain unknown tables: {sorted(unknown_tables)}"
        )
    for table, table_value in categorical_raw.items():
        table_candidates = _mapping(
            table_value, f"candidate contract.categorical_value_candidates.{table}"
        )
        for target, value in table_candidates.items():
            if not isinstance(target, str) or not SNAKE_CASE.fullmatch(target):
                raise ConfigurationError("Categorical target names must be snake_case")
            definition = _mapping(
                value,
                f"candidate contract.categorical_value_candidates.{table}.{target}",
            )
            review_status = required_string(
                definition,
                "review_status",
                f"candidate contract.categorical_value_candidates.{table}.{target}",
            )
            if review_status not in {
                "frozen_v2_candidate_requires_raw_v3_review",
                "human_approved_raw_v3_cross_hospital_domain",
            }:
                raise ConfigurationError(
                    "Categorical candidate review status is invalid"
                )
            normalization = required_string(
                definition,
                "normalization",
                f"candidate contract.categorical_value_candidates.{table}.{target}",
            )
            values = _mapping(
                definition.get("values"),
                f"candidate contract.categorical_value_candidates.{table}.{target}.values",
            )
            if not values or any(not isinstance(key, str) or not key for key in values):
                raise ConfigurationError("Categorical candidate keys must be non-empty strings")
            categorical[(table, target)] = CategoricalValueCandidate(
                review_status=review_status,
                normalization=normalization,
                values=values,
            )
    splits_raw = raw.get("semantic_split_candidates")
    if not isinstance(splits_raw, list):
        raise ConfigurationError(
            "candidate contract.semantic_split_candidates must be a list"
        )
    splits: list[SemanticSplitCandidate] = []
    for index, value in enumerate(splits_raw):
        location = f"candidate contract.semantic_split_candidates[{index}]"
        definition = _mapping(value, location)
        candidate_id = required_string(definition, "candidate_id", location)
        hospital = required_string(definition, "hospital", location)
        table = required_string(definition, "table", location)
        current_target = required_string(definition, "current_target", location)
        proposed_target = required_string(definition, "proposed_target", location)
        proposed_value_type = required_string(
            definition, "proposed_value_type", location
        )
        proposed_unit = required_string(definition, "proposed_unit", location)
        candidate_status = required_string(definition, "status", location)
        evidence_reference = required_string(
            definition, "evidence_reference", location
        )
        if not RULE_ID.fullmatch(candidate_id):
            raise ConfigurationError(f"{location}.candidate_id is invalid")
        if not HOSPITAL_ID.fullmatch(hospital):
            raise ConfigurationError(f"{location}.hospital is invalid")
        if table not in {"static", "dynamic"}:
            raise ConfigurationError(f"{location}.table is invalid")
        if not SNAKE_CASE.fullmatch(current_target) or not SNAKE_CASE.fullmatch(
            proposed_target
        ):
            raise ConfigurationError(f"{location} target is invalid")
        if current_target == proposed_target:
            raise ConfigurationError(f"{location} must change the candidate target")
        if proposed_value_type not in {
            "large_string",
            "float64",
            "list_float64",
            "bool",
            "timestamp_ns",
        }:
            raise ConfigurationError(f"{location}.proposed_value_type is invalid")
        if candidate_status != "frozen_v2_candidate_requires_raw_v3_review":
            raise ConfigurationError(f"{location}.status is invalid")
        splits.append(
            SemanticSplitCandidate(
                candidate_id=candidate_id,
                hospital=hospital,
                table=table,
                current_target=current_target,
                proposed_target=proposed_target,
                proposed_value_type=proposed_value_type,
                proposed_unit=proposed_unit,
                status=candidate_status,
                evidence_reference=evidence_reference,
            )
        )
    split_ids = [item.candidate_id for item in splits]
    split_scopes = [
        (item.hospital, item.table, item.current_target) for item in splits
    ]
    if len(split_ids) != len(set(split_ids)) or len(split_scopes) != len(
        set(split_scopes)
    ):
        raise ConfigurationError(
            "Semantic split candidates must have unique IDs and scopes"
        )
    return CandidateContract(
        version="0.3",
        status=status,
        provenance=provenance,
        defaults=defaults,
        overrides=overrides,
        categorical_candidates=categorical,
        semantic_split_candidates=tuple(splits),
        source_path=path,
    )


def load_harmonization_review_config(path: str | Path) -> HarmonizationReviewConfig:
    config_path = Path(path).expanduser().resolve()
    schema_tokens = load_schema_token_config(config_path)
    raw = load_yaml_mapping(config_path, "Harmonization-review configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonization_review_policy", "config"), config_path
    )
    return HarmonizationReviewConfig(schema_tokens=schema_tokens, policy_path=policy_path)


def load_harmonization_review_policy(
    path: str | Path,
) -> HarmonizationReviewPolicy:
    policy_path = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(policy_path, "Harmonization registry-review policy")
    if raw.get("review_policy_version") != "0.3":
        raise ConfigurationError("Harmonization review policy version must be 0.3")
    if raw.get("status") != "approved_for_read_only_candidate_registry_review":
        raise ConfigurationError("Harmonization review policy is not approved")
    blockers = _string_list(
        raw.get("allowed_schema_token_blockers"),
        "harmonization review.allowed_schema_token_blockers",
    )
    if blockers != (
        "candidate_harmonization_registry_approved",
        "provisional_archive_owner_confirmation",
    ):
        raise ConfigurationError("Harmonization review input blockers changed unexpectedly")
    resolved_raw = _mapping(
        raw.get("resolved_schema_token_blockers"),
        "harmonization review.resolved_schema_token_blockers",
    )
    if set(resolved_raw) != {"provisional_archive_owner_confirmation"}:
        raise ConfigurationError(
            "Harmonization review blocker resolutions changed unexpectedly"
        )
    archive_resolution = _mapping(
        resolved_raw["provisional_archive_owner_confirmation"],
        "harmonization review.resolved_schema_token_blockers."
        "provisional_archive_owner_confirmation",
    )
    expected_archive_resolution = {
        "decision_status": "approved_by_data_owner",
        "decision_date": "2026-08-05",
        "resolution": "confirmed_legacy_snapshot_excluded_from_authoritative_inputs",
        "required_handling": (
            "exclude_from_ingestion_and_retain_only_as_untrusted_reference"
        ),
    }
    if archive_resolution != expected_archive_resolution:
        raise ConfigurationError("Archive blocker resolution is invalid")
    scope = _mapping(raw.get("scope"), "harmonization review.scope")
    allow_private = _required_bool(
        scope,
        "allow_private_schema_token_evidence_reads",
        "harmonization review.scope",
    )
    closed_gates = (
        "allow_ingested_data_reads",
        "allow_harmonized_artifact_writes",
        "allow_hospital_concatenation",
        "allow_registry_approval",
        "allow_union_schema_approval",
        "allow_variable_dictionary_approval",
    )
    if not allow_private or any(
        _required_bool(scope, key, "harmonization review.scope")
        for key in closed_gates
    ):
        raise ConfigurationError("Harmonization registry review must remain fail-closed")
    reporting = _mapping(raw.get("reporting"), "harmonization review.reporting")
    schema_policy_path = resolve_path(
        required_string(raw, "schema_token_policy", "harmonization review"),
        policy_path,
    )
    contract_path = resolve_path(
        required_string(raw, "candidate_contract", "harmonization review"),
        policy_path,
    )
    return HarmonizationReviewPolicy(
        version="0.3",
        required_schema_token_private_artifact_version=required_string(
            raw,
            "required_schema_token_private_artifact_version",
            "harmonization review",
        ),
        required_schema_token_review_artifact_version=required_string(
            raw,
            "required_schema_token_review_artifact_version",
            "harmonization review",
        ),
        required_schema_token_policy_version=required_string(
            raw,
            "required_schema_token_policy_version",
            "harmonization review",
        ),
        allowed_schema_token_blockers=blockers,
        resolved_schema_token_blockers=tuple(sorted(resolved_raw)),
        allow_private_schema_token_evidence_reads=allow_private,
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "harmonization review.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "harmonization review.reporting"
        ),
        schema_token_policy_path=schema_policy_path,
        candidate_contract=_load_candidate_contract(contract_path),
        source_path=policy_path,
    )
