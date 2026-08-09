from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
import json
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.icd10_contract import load_reviewed_icd10_contract
from asic_pipeline.harmonization.review_policy import (
    HarmonizationReviewConfig,
    load_harmonization_review_config,
    load_harmonization_review_policy,
)
from asic_pipeline.harmonization.static_decisions import (
    ReviewedStaticBooleanDecision,
    ReviewedStaticNumericDecision,
    load_reviewed_static_decision_registry,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import load_inventory_policy
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.schema_tokens.policy import load_schema_token_policy


APPROVED_STATUS_PREFIXES = ("approved_", "human_approved_")
TECHNICAL_CHECKS = frozenset(
    {
        "registry_review_evidence_valid",
        "decision_sources_hash_bound_and_valid",
        "static_occurrence_coverage_complete",
        "approved_decision_conflicts_absent",
        "proposed_schema_accounts_for_all_targets",
        "no_production_data_reads_or_artifact_writes",
    }
)
CHECK_DETAILS = {
    "registry_review_evidence_valid": (
        "The immutable private and sanitized registry-review artifacts must match the approved run, version, context, and row counts."
    ),
    "decision_sources_hash_bound_and_valid": (
        "Every reviewed decision source must retain its configured SHA-256 and pass its own fail-closed loader."
    ),
    "static_occurrence_coverage_complete": (
        "Every static raw-column occurrence must appear exactly once and retain an explicit candidate or reviewed disposition."
    ),
    "approved_decision_conflicts_absent": (
        "Approved sources must not assign conflicting targets, physical types, or units to the same source or canonical variable."
    ),
    "static_occurrence_dispositions_approved": (
        "Every retained or retired static occurrence requires an explicit raw-v3 human-reviewed disposition."
    ),
    "static_value_types_approved": (
        "Every proposed static output variable requires one unambiguous reviewed physical value type."
    ),
    "static_units_approved": (
        "Every applicable static output variable requires one unambiguous reviewed unit; not-applicable units remain explicit."
    ),
    "static_parsing_and_categorical_policies_approved": (
        "Every retained static occurrence requiring numeric, categorical, temporal, missing, or free-text handling needs an explicit reviewed policy."
    ),
    "static_multi_output_and_alias_policies_approved": (
        "Every coexisting source alias or source producing multiple canonical outputs requires a reviewed provenance-preserving policy."
    ),
    "proposed_schema_accounts_for_all_targets": (
        "The proposed order must include every retained or approved additional static output exactly once."
    ),
    "ordered_harmonized_static_schema_approved": (
        "The complete ordered harmonized static schema is a proposal until separately frozen by the data owner."
    ),
    "static_variable_dictionary_approved": (
        "Definitions, representations, units, availability, caveats, and provenance in the static dictionary require separate human approval."
    ),
    "no_production_data_reads_or_artifact_writes": (
        "This audit may read immutable report evidence and decision files only; it must not read clinical data or create harmonized data."
    ),
}


DRAFT_DEFINITIONS = {
    "stay_id_global": "Globally unique ICU-stay identifier constructed from the preserved local stay identifier and approved hospital suffix.",
    "stay_id_local": "Exact non-empty ICU-stay identifier supplied by the hospital source.",
    "hospital_id": "Canonical hospital identifier derived from the approved raw-folder-to-hospital mapping.",
    "cluster_id": "ASIC cluster assignment recorded for the ICU stay.",
    "time_since_study_start": "Elapsed time since the study began, measured in days.",
    "study_implementation_phase": "Study implementation phase: calibration, roll-in, or app implementation.",
    "sex": "Source-reported sex category for the ICU stay.",
    "age_group": "Source-reported patient age band.",
    "height_group": "Source-reported patient height band.",
    "height_measurements_cm": "Ordered repeated bedside height estimates in centimetres; missing list elements are omitted and physiologic cleaning is deferred.",
    "weight_group": "Source-reported patient weight band.",
    "weight_kg": "Source-reported patient weight in kilograms.",
    "bmi_group": "Source-reported body-mass-index category.",
    "hosp_los": "Hospital length of stay in days.",
    "icu_los": "ICU length of stay in days.",
    "icu_readmit": "Whether the ICU stay is reported as a readmission.",
    "discharge_status": "Source-reported discharge disposition.",
    "death_status": "Source-reported mortality or discharge status category.",
    "hospital_mortality_reported": "Source-reported hospital mortality flag, retained separately from other mortality fields.",
    "dialysis_free_days": "Number of dialysis-free days under the source definition.",
    "vent_free_days": "Number of ventilation-free days under the source definition.",
    "icd10_codes_source_text": "Exact non-missing source cell containing the source ICD-10 notation.",
    "icd10_codes": "Ordered unique ICD-10 component list produced by the reviewed bounded comma parser.",
    "hospital_code_source": "Hospital code as supplied inside the source file; retained for agreement auditing.",
}


@dataclass(frozen=True)
class StaticRegistryCompletionAuditPolicy:
    version: str
    registry_review_basis_run_id: str
    required_private_artifact_version: str
    required_review_artifact_version: str
    expected_production_static_occurrence_count: int
    inventory_policy_path: Path
    inventory_policy_sha256: str
    reviewed_static_decisions_path: Path
    reviewed_static_decisions_sha256: str
    reviewed_icd10_contract_path: Path
    reviewed_icd10_contract_sha256: str
    schema_token_policy_path: Path
    schema_token_policy_sha256: str
    reviewed_schema_rules_sha256: str
    candidate_contract_path: Path
    candidate_contract_sha256: str
    approved_hospitals: tuple[str, ...]
    proposed_order: tuple[str, ...]
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class StaticRegistryCompletionAuditConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root


@dataclass(frozen=True)
class StaticRegistryCompletionAuditResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_occurrence_coverage: tuple[dict[str, Any], ...]
    private_variable_dictionary: tuple[dict[str, Any], ...]
    private_ordered_schema: dict[str, Any]
    review_payload: dict[str, Any]
    private_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


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


def load_static_registry_completion_audit_config(
    path: str | Path,
) -> StaticRegistryCompletionAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "Static registry-completion audit configuration")
    policy_path = resolve_path(
        required_string(raw, "static_registry_completion_audit_policy", "config"),
        source,
    )
    return StaticRegistryCompletionAuditConfig(harmonization, policy_path)


def load_static_registry_completion_audit_policy(
    path: str | Path,
) -> StaticRegistryCompletionAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Static registry-completion audit policy")
    if raw.get("static_registry_completion_audit_policy_version") != "0.1":
        raise ConfigurationError("Static registry-completion policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_static_registry_completion_audit":
        raise ConfigurationError("Static registry-completion audit is not approved")
    basis = required_string(raw, "registry_review_basis_run_id", "static completion")
    if basis != "20260805T104417Z":
        raise ConfigurationError("Static registry-review evidence basis changed")
    expected_count = raw.get("expected_production_static_occurrence_count")
    if expected_count != 130:
        raise ConfigurationError("Expected production static occurrence count changed")
    sources = _mapping(raw.get("decision_sources"), "static completion.decision_sources")
    scope = _mapping(raw.get("scope"), "static completion.scope")
    expected_scope = {
        "allow_private_registry_review_reads": True,
        "allow_policy_and_decision_registry_reads": True,
        "allow_raw_data_reads": False,
        "allow_ingested_data_reads": False,
        "allow_value_transforms": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_registry_freeze": False,
        "allow_union_schema_freeze": False,
        "allow_variable_dictionary_approval": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static registry-completion audit must remain read-only")
    hospitals = _string_list(raw.get("approved_hospitals"), "approved_hospitals")
    if hospitals != (
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    ):
        raise ConfigurationError("Approved static hospital set changed")
    order = _string_list(raw.get("proposed_order"), "proposed_order")
    reporting = _mapping(raw.get("reporting"), "static completion.reporting")
    if reporting.get("private_directory_mode") != "0700" or reporting.get(
        "private_file_mode"
    ) != "0600":
        raise ConfigurationError("Private static completion modes changed")
    return StaticRegistryCompletionAuditPolicy(
        version="0.1",
        registry_review_basis_run_id=basis,
        required_private_artifact_version=required_string(
            raw, "required_registry_private_artifact_version", "static completion"
        ),
        required_review_artifact_version=required_string(
            raw, "required_registry_review_artifact_version", "static completion"
        ),
        expected_production_static_occurrence_count=expected_count,
        inventory_policy_path=resolve_path(
            required_string(sources, "inventory_policy", "decision_sources"),
            source,
        ),
        inventory_policy_sha256=required_string(
            sources, "inventory_policy_sha256", "decision_sources"
        ),
        reviewed_static_decisions_path=resolve_path(
            required_string(sources, "reviewed_static_decisions", "decision_sources"),
            source,
        ),
        reviewed_static_decisions_sha256=required_string(
            sources, "reviewed_static_decisions_sha256", "decision_sources"
        ),
        reviewed_icd10_contract_path=resolve_path(
            required_string(sources, "reviewed_icd10_contract", "decision_sources"),
            source,
        ),
        reviewed_icd10_contract_sha256=required_string(
            sources, "reviewed_icd10_contract_sha256", "decision_sources"
        ),
        schema_token_policy_path=resolve_path(
            required_string(sources, "schema_token_policy", "decision_sources"),
            source,
        ),
        schema_token_policy_sha256=required_string(
            sources, "schema_token_policy_sha256", "decision_sources"
        ),
        reviewed_schema_rules_sha256=required_string(
            sources, "reviewed_schema_rules_sha256", "decision_sources"
        ),
        candidate_contract_path=resolve_path(
            required_string(sources, "candidate_contract", "decision_sources"),
            source,
        ),
        candidate_contract_sha256=required_string(
            sources, "candidate_contract_sha256", "decision_sources"
        ),
        approved_hospitals=hospitals,
        proposed_order=order,
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "static completion.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "static completion.reporting"
        ),
        source_path=source,
    )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _read_rows(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        return pq.read_table(path).to_pylist()
    except Exception as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc


def _approved(status: str) -> bool:
    return status == "not_applicable" or status.startswith(APPROVED_STATUS_PREFIXES)


def _decision_hashes_valid(policy: StaticRegistryCompletionAuditPolicy) -> bool:
    pairs = (
        (policy.inventory_policy_path, policy.inventory_policy_sha256),
        (policy.reviewed_static_decisions_path, policy.reviewed_static_decisions_sha256),
        (policy.reviewed_icd10_contract_path, policy.reviewed_icd10_contract_sha256),
        (policy.schema_token_policy_path, policy.schema_token_policy_sha256),
        (
            policy.schema_token_policy_path.parent / "reviewed_rules.yaml",
            policy.reviewed_schema_rules_sha256,
        ),
        (policy.candidate_contract_path, policy.candidate_contract_sha256),
    )
    return all(path.is_file() and sha256_file(path) == expected for path, expected in pairs)


def build_static_registry_completion_audit(
    config: StaticRegistryCompletionAuditConfig,
    registry_review_run_id: str,
) -> StaticRegistryCompletionAuditResult:
    policy = load_static_registry_completion_audit_policy(config.policy_path)
    if registry_review_run_id != policy.registry_review_basis_run_id:
        raise HarmonizationError("Unexpected harmonization registry-review basis run")
    if not RUN_ID_PATTERN.fullmatch(registry_review_run_id):
        raise HarmonizationError("Invalid registry-review run ID")

    private_dir = (
        config.reports_root
        / "private"
        / "harmonization_registry_review"
        / registry_review_run_id
    )
    manifest_path = private_dir / "harmonization_registry_review_manifest.json"
    occurrences_path = private_dir / "occurrences.parquet"
    variables_path = private_dir / "variables.parquet"
    review_path = (
        config.reports_root
        / "review"
        / "harmonization_registry_review"
        / f"{registry_review_run_id}.json"
    )
    required = (manifest_path, occurrences_path, variables_path, review_path)
    if any(not path.is_file() for path in required):
        raise HarmonizationError("Registry-review completion evidence is incomplete")

    manifest = _read_json(manifest_path, "Registry-review private manifest")
    review = _read_json(review_path, "Registry-review sanitized report")
    all_occurrences = _read_rows(occurrences_path, "Registry-review occurrences")
    all_variables = _read_rows(variables_path, "Registry-review variables")
    source_metrics = review.get("metrics")
    evidence_valid = bool(
        manifest.get("artifact") == "asic_v3_harmonization_registry_review_private"
        and manifest.get("artifact_version") == policy.required_private_artifact_version
        and manifest.get("dataset_context") == config.dataset_context
        and manifest.get("occurrence_review_row_count") == len(all_occurrences)
        and manifest.get("variable_review_row_count") == len(all_variables)
        and review.get("artifact") == "asic_v3_harmonization_registry_review"
        and review.get("artifact_version") == policy.required_review_artifact_version
        and review.get("dataset_context") == config.dataset_context
        and isinstance(source_metrics, dict)
        and source_metrics.get("raw_column_occurrence_count") == len(all_occurrences)
        and source_metrics.get("technical_blocking_finding_count") == 0
    )

    hashes_valid = _decision_hashes_valid(policy)
    inventory_policy = load_inventory_policy(policy.inventory_policy_path)
    static_registry = load_reviewed_static_decision_registry(
        policy.reviewed_static_decisions_path
    )
    icd10 = load_reviewed_icd10_contract(policy.reviewed_icd10_contract_path)
    schema_policy = load_schema_token_policy(policy.schema_token_policy_path)
    review_policy = load_harmonization_review_policy(
        config.harmonization_review.policy_path
    )
    decision_sources_valid = bool(
        hashes_valid
        and inventory_policy.policy_version == "0.3"
        and inventory_policy.status == "approved_for_read_only_inventory"
        and tuple(
            mapping.canonical_hospital_id
            for mapping in inventory_policy.hospital_mappings
            if mapping.cohort_action == "include"
        )
        == policy.approved_hospitals
        and static_registry.version == "0.3"
        and icd10.version == "0.2"
        and icd10.allow_parser_activation
        and schema_policy.policy_version == "0.3"
        and review_policy.candidate_contract.source_path == policy.candidate_contract_path
        and not static_registry.allow_production_reads
        and not static_registry.allow_artifact_writes
        and not icd10.allow_harmonized_artifact_writes
        and not icd10.allow_hospital_concatenation
        and not icd10.allow_union_schema_freeze
    )

    static_rows = [row for row in all_occurrences if row.get("table") == "static"]
    identities: set[tuple[str, str, int]] = set()
    occurrence_coverage: list[dict[str, Any]] = []
    conflict_count = 0
    unaccounted_count = 0
    pending_mapping_count = 0
    pending_parser_count = 0
    hospital_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"occurrences": 0, "approved": 0, "pending": 0}
    )
    additional_targets: dict[str, dict[str, str]] = {}

    for row in static_rows:
        hospital = row.get("hospital")
        raw_name = row.get("raw_name")
        occurrence = row.get("raw_occurrence")
        physical_name = row.get("physical_name")
        candidate_target = row.get("candidate_target")
        kind = row.get("candidate_kind")
        if (
            not isinstance(hospital, str)
            or not isinstance(raw_name, str)
            or not isinstance(physical_name, str)
            or not isinstance(occurrence, int)
            or isinstance(occurrence, bool)
            or occurrence <= 0
            or not isinstance(kind, str)
        ):
            raise HarmonizationError("Static occurrence identity is invalid")
        identity = (hospital, physical_name, occurrence)
        if identity in identities:
            raise HarmonizationError("Static occurrence identities are not unique")
        identities.add(identity)
        hospital_counts[hospital]["occurrences"] += 1

        sources: list[dict[str, str]] = []
        type_sources: list[tuple[str, str]] = []
        unit_sources: list[tuple[str, str]] = []
        parser_sources: list[str] = []

        reviewed_rule = schema_policy.candidate_for(
            "static", raw_name, occurrence, hospital
        )
        if reviewed_rule is not None and reviewed_rule.status == "human_approved_raw_v3_rule":
            if reviewed_rule.target is not None:
                sources.append(
                    {
                        "source": "reviewed_schema_rule",
                        "rule_id": str(reviewed_rule.review_rule_id),
                        "target": reviewed_rule.target,
                    }
                )
            if reviewed_rule.canonical_value_type is not None:
                type_sources.append(
                    (reviewed_rule.canonical_value_type, "reviewed_schema_rule")
                )
            if reviewed_rule.expected_unit is not None:
                unit_sources.append((reviewed_rule.expected_unit, "reviewed_schema_rule"))
            if (
                reviewed_rule.approved_parser is not None
                or reviewed_rule.allowed_tokens
                or reviewed_rule.approved_missing_sentinel_tokens
            ):
                parser_sources.append("reviewed_schema_rule")

        static_decision = static_registry.rule_for(
            hospital, "static", raw_name, occurrence
        )
        if static_decision is not None:
            sources.append(
                {
                    "source": "reviewed_static_decisions_0_3",
                    "rule_id": static_decision.rule_id,
                    "target": static_decision.target,
                }
            )
            type_sources.append(
                (static_decision.output_value_type, "reviewed_static_decisions_0_3")
            )
            unit_sources.append(
                (static_decision.output_unit, "reviewed_static_decisions_0_3")
            )
            parser_sources.append("reviewed_static_decisions_0_3")

        identifier = static_registry.identifier_decision
        if (
            identifier is not None
            and occurrence == identifier.source_occurrence
            and raw_name in identifier.source_raw_name_variants
        ):
            sources.append(
                {
                    "source": "reviewed_identifier_contract",
                    "rule_id": identifier.rule_id,
                    "target": identifier.global_target,
                }
            )
            type_sources.append((identifier.output_value_type, "reviewed_identifier_contract"))
            unit_sources.append(("not_applicable", "reviewed_identifier_contract"))
            parser_sources.append("reviewed_identifier_contract")
            additional_targets[identifier.local_target] = {
                "kind": "identifier",
                "value_type": identifier.output_value_type,
                "unit": "not_applicable",
                "source": "reviewed_identifier_contract",
            }

        if candidate_target == icd10.list_target:
            sources.append(
                {
                    "source": "reviewed_icd10_contract_0_2",
                    "rule_id": "HARM-STATIC-ICD10-CONTRACT-002",
                    "target": icd10.list_target,
                }
            )
            type_sources.append((icd10.list_value_type, "reviewed_icd10_contract_0_2"))
            unit_sources.append(("not_applicable", "reviewed_icd10_contract_0_2"))
            parser_sources.append("reviewed_icd10_contract_0_2")
            additional_targets[icd10.source_text_target] = {
                "kind": "free_text",
                "value_type": icd10.source_text_value_type,
                "unit": "not_applicable",
                "source": "reviewed_icd10_contract_0_2",
            }

        approved_targets = {item["target"] for item in sources}
        conflict = len(approved_targets) > 1
        conflict_count += int(conflict)
        target = (
            next(iter(approved_targets))
            if len(approved_targets) == 1
            else candidate_target
        )
        if target is None:
            unaccounted_count += 1
        mapping_status = (
            "conflicting_approved_sources"
            if conflict
            else "approved"
            if sources
            else "pending_candidate_mapping"
        )
        if mapping_status == "pending_candidate_mapping":
            pending_mapping_count += 1
            hospital_counts[hospital]["pending"] += 1
        elif mapping_status == "approved":
            hospital_counts[hospital]["approved"] += 1

        if isinstance(target, str):
            proposal = review_policy.candidate_contract.proposal_for(
                "static", target, kind
            )
            if _approved(proposal.value_type_status):
                type_sources.append((proposal.value_type, "candidate_contract_raw_v3_approval"))
            if _approved(proposal.unit_status):
                unit_sources.append((proposal.unit, "candidate_contract_raw_v3_approval"))
            categorical = review_policy.candidate_contract.categorical_for(
                "static", target
            )
            if categorical is not None and categorical.review_status.startswith(
                "human_approved_"
            ):
                parser_sources.append("candidate_contract_raw_v3_categorical_approval")

        type_values = {value for value, _source in type_sources}
        unit_values = {value for value, _source in unit_sources}
        conflict_count += int(len(type_values) > 1) + int(len(unit_values) > 1)
        parser_status = "approved" if parser_sources else "pending"
        if parser_status == "pending" and mapping_status != "conflicting_approved_sources":
            pending_parser_count += 1
        occurrence_coverage.append(
            {
                "review_item_id": row.get("review_item_id"),
                "hospital": hospital,
                "table": "static",
                "physical_name": physical_name,
                "raw_name": raw_name,
                "raw_occurrence": occurrence,
                "candidate_target": candidate_target,
                "resolved_target": target,
                "candidate_kind": kind,
                "mapping_status": mapping_status,
                "parser_policy_status": parser_status,
                "approved_type": next(iter(type_values)) if len(type_values) == 1 else None,
                "approved_unit": next(iter(unit_values)) if len(unit_values) == 1 else None,
                "approved_decision_sources_json": json.dumps(
                    sources, ensure_ascii=False, sort_keys=True
                ),
                "type_sources_json": json.dumps(type_sources, ensure_ascii=False),
                "unit_sources_json": json.dumps(unit_sources, ensure_ascii=False),
                "parser_sources_json": json.dumps(sorted(set(parser_sources))),
                "all_missing_or_empty": row.get("all_missing_or_empty"),
                "raw_nonempty_count": row.get("raw_nonempty_count"),
            }
        )

    observed_hospitals = {row["hospital"] for row in occurrence_coverage}
    additional_targets["hospital_id"] = {
        "kind": "identifier",
        "value_type": "large_string",
        "unit": "not_applicable",
        "source": "approved_inventory_hospital_mapping",
    }
    coverage_valid = bool(
        len(occurrence_coverage) == len(static_rows)
        and len(identities) == len(static_rows)
        and unaccounted_count == 0
        and observed_hospitals.issubset(set(policy.approved_hospitals))
        and (
            config.dataset_context != "production"
            or (
                len(static_rows) == policy.expected_production_static_occurrence_count
                and observed_hospitals == set(policy.approved_hospitals)
            )
        )
    )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in occurrence_coverage:
        target = row["resolved_target"]
        if isinstance(target, str):
            grouped[target].append(row)
    for target, details in additional_targets.items():
        grouped.setdefault(target, [])

    dictionary: list[dict[str, Any]] = []
    pending_type_count = 0
    pending_unit_count = 0
    variable_conflict_count = 0
    for target, rows in grouped.items():
        if rows:
            kinds = {str(row["candidate_kind"]) for row in rows}
            types = {str(row["approved_type"]) for row in rows if row["approved_type"]}
            units = {str(row["approved_unit"]) for row in rows if row["approved_unit"]}
            hospitals = sorted({str(row["hospital"]) for row in rows})
            raw_names = sorted({str(row["raw_name"]) for row in rows})
            mapping_approved_count = sum(row["mapping_status"] == "approved" for row in rows)
            parser_pending = sum(row["parser_policy_status"] == "pending" for row in rows)
        else:
            detail = additional_targets[target]
            kinds = {detail["kind"]}
            types = {detail["value_type"]}
            units = {detail["unit"]}
            hospitals = sorted(observed_hospitals)
            raw_names = []
            mapping_approved_count = 0
            parser_pending = 0
        type_status = "approved" if len(types) == 1 else "pending" if not types else "conflict"
        unit_status = "approved" if len(units) == 1 else "pending" if not units else "conflict"
        pending_type_count += int(type_status == "pending")
        pending_unit_count += int(unit_status == "pending")
        variable_conflict_count += int(len(kinds) > 1 or type_status == "conflict" or unit_status == "conflict")
        dictionary.append(
            {
                "canonical_name": target,
                "table": "static",
                "candidate_kind": next(iter(kinds)) if len(kinds) == 1 else "conflict",
                "proposed_value_type": next(iter(types)) if len(types) == 1 else None,
                "value_type_status": type_status,
                "proposed_unit": next(iter(units)) if len(units) == 1 else None,
                "unit_status": unit_status,
                "definition": DRAFT_DEFINITIONS.get(
                    target,
                    f"Provisional definition for {target}; source definition requires review.",
                ),
                "definition_status": "draft_requires_human_review",
                "source_occurrence_count": len(rows),
                "mapping_approved_occurrence_count": mapping_approved_count,
                "parser_pending_occurrence_count": parser_pending,
                "hospital_count": len(hospitals),
                "hospital_availability_json": json.dumps(hospitals),
                "source_raw_names_json": json.dumps(raw_names, ensure_ascii=False),
                "provenance_status": "draft_from_immutable_occurrence_and_reviewed_decision_evidence",
                "caveat": "Clinical meaning and source-specific availability remain subject to dictionary approval.",
            }
        )
    conflict_count += variable_conflict_count

    targets = set(grouped)
    ordered = [target for target in policy.proposed_order if target in targets]
    unordered_targets = sorted(targets - set(policy.proposed_order))
    ordered.extend(unordered_targets)
    schema_complete = len(ordered) == len(targets) == len(set(ordered))
    position = {target: index for index, target in enumerate(ordered, start=1)}
    dictionary.sort(key=lambda row: position[str(row["canonical_name"])])
    ordered_schema = {
        "status": "proposal_requires_human_freeze",
        "table": "static",
        "columns": [
            {
                "position": position[str(row["canonical_name"])],
                "name": row["canonical_name"],
                "value_type": row["proposed_value_type"],
                "unit": row["proposed_unit"],
                "value_type_status": row["value_type_status"],
                "unit_status": row["unit_status"],
            }
            for row in dictionary
        ],
        "unlisted_targets_appended": unordered_targets,
    }

    mapping_blocker_count = pending_mapping_count + conflict_count
    parser_blocker_count = pending_parser_count
    alias_groups = {
        (row["hospital"], row["resolved_target"])
        for row in occurrence_coverage
        if row["resolved_target"] is not None
        and sum(
            other["hospital"] == row["hospital"]
            and other["resolved_target"] == row["resolved_target"]
            for other in occurrence_coverage
        )
        > 1
    }
    unresolved_alias_count = sum(
        not all(
            item["mapping_status"] == "approved"
            for item in occurrence_coverage
            if item["hospital"] == hospital and item["resolved_target"] == target
        )
        for hospital, target in alias_groups
    )
    failure_counts = {
        "registry_review_evidence_valid": int(not evidence_valid),
        "decision_sources_hash_bound_and_valid": int(not decision_sources_valid),
        "static_occurrence_coverage_complete": int(not coverage_valid),
        "approved_decision_conflicts_absent": conflict_count,
        "static_occurrence_dispositions_approved": mapping_blocker_count,
        "static_value_types_approved": pending_type_count,
        "static_units_approved": pending_unit_count,
        "static_parsing_and_categorical_policies_approved": parser_blocker_count,
        "static_multi_output_and_alias_policies_approved": unresolved_alias_count,
        "proposed_schema_accounts_for_all_targets": int(
            not schema_complete or bool(unordered_targets)
        ),
        "ordered_harmonized_static_schema_approved": 1,
        "static_variable_dictionary_approved": 1,
        "no_production_data_reads_or_artifact_writes": 0,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed={"failure_count": count},
            expected={"failure_count": 0},
            details=CHECK_DETAILS[name],
        )
        for name, count in failure_counts.items()
    ]
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(item for item in blockers if item["check"] in TECHNICAL_CHECKS)
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_static_registry_completion_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": overall_status(checks),
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "authoritative_evidence_boundary": "immutable_harmonization_registry_review_bundle",
        },
        "metrics": {
            "static_raw_occurrence_count": len(static_rows),
            "accounted_static_occurrence_count": len(occurrence_coverage),
            "approved_mapping_occurrence_count": sum(
                row["mapping_status"] == "approved" for row in occurrence_coverage
            ),
            "pending_mapping_occurrence_count": pending_mapping_count,
            "approved_decision_conflict_count": conflict_count,
            "candidate_static_variable_count": len(dictionary),
            "pending_value_type_variable_count": pending_type_count,
            "pending_unit_variable_count": pending_unit_count,
            "pending_parser_occurrence_count": pending_parser_count,
            "coexisting_alias_group_count": len(alias_groups),
            "unresolved_alias_group_count": unresolved_alias_count,
            "approved_additional_output_count": len(additional_targets),
            "proposed_schema_column_count": len(ordered),
            "unlisted_schema_target_count": len(unordered_targets),
            "technical_blocking_finding_count": len(technical),
        },
        "hospital_summaries": dict(sorted(hospital_counts.items())),
        "proposed_ordered_schema": [
            {
                "position": position[str(row["canonical_name"])],
                "name": row["canonical_name"],
                "value_type": row["proposed_value_type"],
                "unit": row["proposed_unit"],
                "value_type_status": row["value_type_status"],
                "unit_status": row["unit_status"],
                "hospital_count": row["hospital_count"],
            }
            for row in dictionary
        ],
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only private static registry-completion bundle on the authorized cluster",
        },
        "publication": {
            "clinical_data_read": False,
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "registry_frozen": False,
            "ordered_schema_frozen": False,
            "variable_dictionary_approved": False,
        },
        "limitations": [
            "This audit reads immutable registry-review evidence and reviewed policy files only; it does not read raw or ingested clinical data.",
            "Candidate mappings inherited only from v2 or legacy remain pending even when their English names appear in the proposal.",
            "The draft dictionary carries reviewed representations where available, but every definition and caveat remains subject to human review.",
            "Cross-hospital raw-name variants are provenance, not automatic coalescence approval.",
            "No harmonized, cleaned, derived, concatenated, or pooled production artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_static_registry_completion_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": review_payload["overall_status"],
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "registry_manifest_sha256": sha256_file(manifest_path),
            "registry_occurrences_sha256": sha256_file(occurrences_path),
            "registry_variables_sha256": sha256_file(variables_path),
            "registry_review_sha256": sha256_file(review_path),
        },
        "decision_sources": {
            "inventory_policy_sha256": sha256_file(policy.inventory_policy_path),
            "reviewed_static_decisions_sha256": sha256_file(
                policy.reviewed_static_decisions_path
            ),
            "reviewed_icd10_contract_sha256": sha256_file(
                policy.reviewed_icd10_contract_path
            ),
            "schema_token_policy_sha256": sha256_file(policy.schema_token_policy_path),
            "reviewed_schema_rules_sha256": sha256_file(
                schema_policy.reviewed_registry_path
            ),
            "candidate_contract_sha256": sha256_file(policy.candidate_contract_path),
        },
        "occurrence_coverage_row_count": len(occurrence_coverage),
        "variable_dictionary_row_count": len(dictionary),
        "ordered_schema_column_count": len(ordered),
        "checks": [asdict(item) for item in checks],
        "production_data_artifacts_generated": False,
        "publication_ready": False,
    }
    return StaticRegistryCompletionAuditResult(
        overall_status=review_payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_manifest=private_manifest,
        private_occurrence_coverage=tuple(occurrence_coverage),
        private_variable_dictionary=tuple(dictionary),
        private_ordered_schema=ordered_schema,
        review_payload=review_payload,
    )


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 static-registry completion audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input registry-review run: `{payload['inputs']['registry_review_run_id']}`",
        "- Protected filenames, identifiers, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    if payload["blocking_findings"]:
        lines.extend(
            f"- `{item['check']}`: {item['details']}"
            for item in payload["blocking_findings"]
        )
    else:
        lines.append("- None")
    lines.extend(["", "## Audit totals", ""])
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    lines.extend(["", "## Hospital coverage", ""])
    for hospital, counts in payload["hospital_summaries"].items():
        lines.append(
            f"- `{hospital}`: occurrences `{counts['occurrences']}`; "
            f"approved `{counts['approved']}`; pending `{counts['pending']}`"
        )
    lines.extend(["", "## Proposed ordered harmonized static schema", ""])
    for column in payload["proposed_ordered_schema"]:
        lines.append(
            f"- `{column['position']:02d}` `{column['name']}`: type "
            f"`{column['value_type']}` ({column['value_type_status']}); unit "
            f"`{column['unit']}` ({column['unit_status']}); hospitals "
            f"`{column['hospital_count']}`"
        )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the owner-only occurrence coverage and draft dictionary. Do not freeze the static registry, ordered schema, or begin production harmonization until every technical issue and selected human decision is resolved."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def _write_json(path: Path, value: Any, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def write_static_registry_completion_audit_bundle(
    result: StaticRegistryCompletionAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> StaticRegistryCompletionAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid static registry-completion audit run ID")
    private_dir = reports_root / "private" / "static_registry_completion_audit" / selected
    review_dir = reports_root / "review" / "static_registry_completion_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "Static registry-completion audit run already exists and will not be overwritten"
        )
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    try:
        _write_json(
            private_dir / "static_registry_completion_audit_manifest.json",
            result.private_manifest,
            0o600,
        )
        _write_private_parquet(
            private_dir / "occurrence_coverage.parquet",
            result.private_occurrence_coverage,
        )
        _write_private_parquet(
            private_dir / "variable_dictionary.parquet",
            result.private_variable_dictionary,
        )
        for path in private_dir.glob("*.parquet"):
            path.chmod(0o600)
        _write_json(private_dir / "ordered_schema.json", result.private_ordered_schema, 0o600)
        _write_json(review_json, result.review_payload, 0o640)
        temporary = review_md.with_suffix(".md.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(_markdown(result.review_payload))
            temporary.replace(review_md)
            review_md.chmod(0o640)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    except Exception:
        # Keep any partial private bundle visible for owner review; never overwrite it.
        raise
    return replace(
        result,
        private_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
