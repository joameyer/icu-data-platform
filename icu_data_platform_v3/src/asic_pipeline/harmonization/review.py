from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.review_policy import (
    CandidateContract,
    HarmonizationReviewConfig,
    HarmonizationReviewPolicy,
    TargetProposal,
    load_harmonization_review_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.schema_tokens.policy import load_schema_token_policy


TECHNICAL_CHECKS = frozenset(
    {
        "schema_token_evidence_is_complete_and_compatible",
        "all_raw_occurrences_accounted",
        "all_occurrences_have_explicit_dispositions",
        "all_retained_targets_have_candidate_types",
    }
)
CHECK_ORDER = (
    "schema_token_evidence_is_complete_and_compatible",
    "all_raw_occurrences_accounted",
    "all_occurrences_have_explicit_dispositions",
    "all_retained_targets_have_candidate_types",
    "candidate_column_mappings_approved",
    "candidate_value_types_approved",
    "candidate_units_approved",
    "candidate_categorical_policies_approved",
    "candidate_parsing_and_missing_policies_approved",
    "candidate_aliases_and_semantic_splits_approved",
    "ordered_harmonized_union_schema_approved",
    "variable_dictionary_approved",
    "provisional_archive_owner_confirmation",
)
CHECK_DETAILS = {
    "schema_token_evidence_is_complete_and_compatible": "The immutable schema/token private and sanitized artifacts must agree and satisfy policy 0.3 technical gates.",
    "all_raw_occurrences_accounted": "Every raw column occurrence must appear exactly once in the private registry-review workbook.",
    "all_occurrences_have_explicit_dispositions": "Every occurrence must have an explicit retain, validate, drop-after-precondition, or unresolved disposition.",
    "all_retained_targets_have_candidate_types": "Every retained candidate target must have an explicit candidate physical value type.",
    "candidate_column_mappings_approved": "Every raw-v3 occurrence-to-canonical mapping requires human approval or an explicit reviewed retirement decision.",
    "candidate_value_types_approved": "Every canonical candidate value type requires raw-v3 human approval.",
    "candidate_units_approved": "Every applicable canonical unit and hospital-specific conversion policy requires raw-v3 human approval.",
    "candidate_categorical_policies_approved": "Categorical domains and frozen-v2 value translations require raw-v3 token review and approval.",
    "candidate_parsing_and_missing_policies_approved": "Direct/custom parsing, textual missing, sentinel, threshold, percentage, and temporal policies require explicit scope approval.",
    "candidate_aliases_and_semantic_splits_approved": "Every alias/coalescence group and source-to-target semantic split requires raw-v3 overlap and provenance review.",
    "ordered_harmonized_union_schema_approved": "The common ordered static and dynamic harmonized schemas require a separate human freeze.",
    "variable_dictionary_approved": "Canonical definitions, types, units, availability, caveats, and provenance require a separately approved dictionary.",
    "provisional_archive_owner_confirmation": "The excluded archive remains a publication blocker pending owner confirmation.",
}

APPROVED_STATUSES = frozenset({"human_approved_raw_v3", "not_applicable"})
CUSTOM_CLASS_ACTIONS = {
    "ratio_syntax_candidate": "ratio_parser_and_direction_review",
    "decimal_comma_syntax_candidate": "decimal_comma_parser_review",
    "threshold_syntax_candidate": "threshold_semantics_review",
    "percentage_syntax_candidate": "percentage_scale_review",
    "textual_missing_candidate": "textual_missing_policy_review",
    "whitespace_only_candidate": "whitespace_missing_policy_review",
    "temporal_token_candidate": "temporal_parser_review",
    "categorical_token": "categorical_domain_review",
    "unapproved_categorical_token": "categorical_domain_violation_review",
    "unresolved_token": "unresolved_token_review",
    "unresolved_ratio_zero_denominator": "invalid_ratio_review",
}


@dataclass(frozen=True)
class HarmonizationRegistryReviewResult:
    dataset_context: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_occurrences: tuple[dict[str, Any], ...]
    private_variables: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass(frozen=True)
class _SchemaTokenEvidence:
    private_directory: Path
    private_manifest: dict[str, Any]
    review_payload: dict[str, Any]
    columns_path: Path
    tokens_path: Path
    columns: tuple[dict[str, Any], ...]
    carried_blockers: tuple[str, ...]
    input_hashes: dict[str, str]


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _load_schema_token_evidence(
    config: HarmonizationReviewConfig,
    policy: HarmonizationReviewPolicy,
    run_id: str,
) -> _SchemaTokenEvidence:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise HarmonizationError("Invalid schema-token inventory run ID")
    private_directory = (
        config.reports_root / "private" / "schema_token_inventory" / run_id
    )
    manifest_path = private_directory / "schema_token_inventory_manifest.json"
    columns_path = private_directory / "columns.parquet"
    tokens_path = private_directory / "tokens.parquet"
    review_path = (
        config.reports_root / "review" / "schema_token_inventory" / f"{run_id}.json"
    )
    required = (manifest_path, columns_path, tokens_path, review_path)
    if any(not path.is_file() for path in required):
        raise HarmonizationError("Schema-token inventory evidence is incomplete")
    manifest = _read_json(manifest_path, "Private schema-token manifest")
    review = _read_json(review_path, "Sanitized schema-token review")
    if manifest.get("artifact") != "asic_v3_schema_token_inventory_private" or (
        manifest.get("artifact_version")
        != policy.required_schema_token_private_artifact_version
    ):
        raise HarmonizationError("Private schema-token artifact identity is invalid")
    if review.get("artifact") != "asic_v3_schema_token_inventory_review" or (
        review.get("artifact_version")
        != policy.required_schema_token_review_artifact_version
    ):
        raise HarmonizationError("Sanitized schema-token artifact identity is invalid")
    if manifest.get("dataset_context") != config.dataset_context or review.get(
        "dataset_context"
    ) != config.dataset_context:
        raise HarmonizationError("Schema-token dataset context differs")
    if manifest.get("generated_at_utc") != review.get("generated_at_utc"):
        raise HarmonizationError("Private and sanitized schema-token timestamps differ")
    manifest_inputs = manifest.get("inputs")
    review_inputs = review.get("inputs")
    if not isinstance(manifest_inputs, dict) or not isinstance(review_inputs, dict):
        raise HarmonizationError("Schema-token input provenance is invalid")
    for key in ("inventory_run_id", "ingestion_audit_run_id"):
        if manifest_inputs.get(key) != review_inputs.get(key):
            raise HarmonizationError("Private and sanitized input provenance differs")
    policies = manifest.get("policies")
    if not isinstance(policies, dict) or policies.get(
        "schema_token_policy_version"
    ) != policy.required_schema_token_policy_version:
        raise HarmonizationError("Schema-token policy provenance is incompatible")
    blockers_raw = review.get("blocking_findings")
    if not isinstance(blockers_raw, list):
        raise HarmonizationError("Schema-token blocking findings are invalid")
    blockers: list[str] = []
    for item in blockers_raw:
        if not isinstance(item, dict) or not isinstance(item.get("check"), str):
            raise HarmonizationError("Schema-token blocking finding is invalid")
        blockers.append(item["check"])
    unexpected = sorted(set(blockers) - set(policy.allowed_schema_token_blockers))
    if unexpected:
        raise HarmonizationError(
            f"Schema-token evidence has unresolved prerequisite checks: {unexpected}"
        )
    metrics = review.get("metrics")
    if not isinstance(metrics, dict):
        raise HarmonizationError("Schema-token metrics are invalid")
    required_zero = (
        "unmapped_raw_column_occurrence_count",
        "numeric_unresolved_nonempty_token_count",
        "columns_with_truncated_review_examples",
        "reviewed_drop_candidate_nonempty_count",
        "reviewed_rule_token_domain_violation_count",
        "numeric_list_unresolved_element_count",
    )
    if any(metrics.get(key) != 0 for key in required_zero):
        raise HarmonizationError(
            "Schema-token evidence has unresolved technical or token findings"
        )
    try:
        table = pq.read_table(columns_path)
    except (OSError, pa.ArrowException) as exc:
        raise HarmonizationError("Schema-token column evidence cannot be read") from exc
    columns = tuple(table.to_pylist())
    expected_rows = manifest.get("column_evidence_row_count")
    review_rows = metrics.get("raw_column_occurrence_count")
    if (
        not isinstance(expected_rows, int)
        or expected_rows <= 0
        or expected_rows != review_rows
        or len(columns) != expected_rows
    ):
        raise HarmonizationError("Schema-token occurrence counts do not agree")
    if pq.ParquetFile(tokens_path).metadata is None:
        raise HarmonizationError("Schema-token token evidence metadata is unavailable")
    return _SchemaTokenEvidence(
        private_directory=private_directory,
        private_manifest=manifest,
        review_payload=review,
        columns_path=columns_path,
        tokens_path=tokens_path,
        columns=columns,
        carried_blockers=tuple(sorted(set(blockers))),
        input_hashes={
            "schema_token_manifest_sha256": sha256_file(manifest_path),
            "schema_token_review_sha256": sha256_file(review_path),
            "columns_parquet_sha256": sha256_file(columns_path),
            "tokens_parquet_sha256": sha256_file(tokens_path),
        },
    )


def _required_string(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise HarmonizationError(f"Schema-token occurrence lacks {key}")
    return value


def _classification_counts(row: dict[str, Any]) -> dict[str, int]:
    raw = row.get("classification_counts")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HarmonizationError(
                "Occurrence classification counts contain invalid JSON"
            ) from exc
    if not isinstance(raw, dict) or any(
        not isinstance(key, str)
        or (
            value is not None
            and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            )
        )
        for key, value in raw.items()
    ):
        raise HarmonizationError("Occurrence classification counts are invalid")
    # Arrow stores heterogeneous per-row dictionaries as a union struct; keys
    # absent from a source row return as null and are not counts for that row.
    return {key: value for key, value in raw.items() if value is not None}


def _json_string_list(row: dict[str, Any], key: str) -> list[str]:
    raw = row.get(key)
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HarmonizationError(f"Occurrence {key} contains invalid JSON") from exc
    if raw is None:
        return []
    if not isinstance(raw, list) or any(
        not isinstance(item, str) or not item for item in raw
    ):
        raise HarmonizationError(f"Occurrence {key} is invalid")
    if len(raw) != len(set(raw)):
        raise HarmonizationError(f"Occurrence {key} contains duplicates")
    return list(raw)


def _approved_disposition(row: dict[str, Any]) -> bool:
    status = row.get("candidate_status")
    return bool(row.get("review_rule_id")) or status == (
        "reviewed_all_empty_drop_candidate_requires_inventory_revalidation"
    )


def _candidate_action(row: dict[str, Any]) -> tuple[str, str]:
    target = row.get("candidate_target")
    kind = row.get("candidate_kind")
    if target is None:
        if row.get("candidate_status") == (
            "reviewed_all_empty_drop_candidate_requires_inventory_revalidation"
        ) and row.get("all_missing_or_empty") is True:
            return (
                "drop_after_all_missing_precondition",
                "human_approved_raw_v3_precondition_revalidated",
            )
        return "unresolved_no_canonical_target", "requires_human_review"
    if kind == "identifier":
        action = "retain_map_and_validate_identifier"
    elif kind == "free_text":
        action = "retain_map_and_preserve_free_text"
    else:
        action = "retain_map_and_harmonize"
    return (
        action,
        "human_approved_raw_v3" if _approved_disposition(row) else "requires_human_review",
    )


def _parsing_proposal(row: dict[str, Any]) -> tuple[list[str], str]:
    counts = _classification_counts(row)
    actions = sorted(
        action
        for token_class, action in CUSTOM_CLASS_ACTIONS.items()
        if counts.get(token_class, 0) > 0
    )
    parser = row.get("approved_parser")
    sentinels = _json_string_list(row, "approved_missing_sentinel_tokens")
    if parser:
        actions.append(f"approved_parser:{parser}")
    if sentinels:
        actions.append("approved_scoped_numeric_missing_sentinel")
    if row.get("candidate_kind") == "numeric" and counts.get("direct_numeric", 0):
        actions.append("direct_numeric_grammar")
    actions = sorted(set(actions))
    pending = [
        item
        for item in actions
        if not item.startswith("approved_parser:")
        and item != "approved_scoped_numeric_missing_sentinel"
        and not (
            item == "direct_numeric_grammar" and _approved_disposition(row)
        )
    ]
    if row.get("candidate_kind") == "numeric" and not _approved_disposition(row):
        pending.append("numeric_scope_requires_registry_approval")
    if row.get("candidate_kind") == "temporal":
        pending.append("temporal_scope_requires_registry_approval")
    status = "human_approved_raw_v3" if not pending else "requires_human_review"
    return actions, status


def _categorical_proposal(
    row: dict[str, Any], contract: CandidateContract
) -> tuple[str, str | None, int]:
    if row.get("candidate_kind") != "categorical":
        return "not_applicable", None, 0
    allowed = _json_string_list(row, "approved_allowed_tokens")
    if allowed:
        return "human_approved_raw_v3_hospital_domain", "exact", len(allowed)
    target = row.get("candidate_target")
    if not isinstance(target, str):
        return "requires_human_review", None, 0
    candidate = contract.categorical_for(_required_string(row, "table"), target)
    if candidate is None:
        return "unresolved_requires_raw_v3_token_review", None, 0
    return (
        candidate.review_status,
        candidate.normalization,
        len(candidate.values),
    )


def _not_applicable_proposal() -> TargetProposal:
    return TargetProposal(
        value_type="not_applicable",
        value_type_status="not_applicable",
        unit="not_applicable",
        unit_status="not_applicable",
    )


def build_harmonization_registry_review(
    config: HarmonizationReviewConfig,
    schema_token_run_id: str,
) -> HarmonizationRegistryReviewResult:
    policy = load_harmonization_review_policy(config.policy_path)
    schema_policy = load_schema_token_policy(policy.schema_token_policy_path)
    if schema_policy.policy_version != policy.required_schema_token_policy_version:
        raise HarmonizationError("Configured schema-token policy version differs")
    if config.schema_tokens.policy_path.resolve() != policy.schema_token_policy_path:
        raise HarmonizationError("Dataset and harmonization review schema policies differ")
    evidence = _load_schema_token_evidence(config, policy, schema_token_run_id)
    identities: set[tuple[str, str, str, int]] = set()
    raw_targets: dict[tuple[str, str], set[str]] = defaultdict(set)
    target_sources: dict[tuple[str, str], set[str]] = defaultdict(set)
    hospital_target_occurrence_counts: Counter[tuple[str, str, str]] = Counter()
    for row in evidence.columns:
        hospital = _required_string(row, "hospital")
        table = _required_string(row, "table")
        physical_name = _required_string(row, "physical_name")
        raw_name = _required_string(row, "raw_name")
        occurrence = row.get("raw_occurrence")
        if table not in {"static", "dynamic"} or not isinstance(occurrence, int) or occurrence <= 0:
            raise HarmonizationError("Schema-token occurrence identity is invalid")
        identity = (hospital, table, physical_name, occurrence)
        if identity in identities:
            raise HarmonizationError("Schema-token occurrence identities are not unique")
        identities.add(identity)
        target = row.get("candidate_target")
        if target is not None:
            if not isinstance(target, str) or not target:
                raise HarmonizationError("Candidate target is invalid")
            raw_targets[(table, raw_name)].add(target)
            target_sources[(table, target)].add(raw_name)
            hospital_target_occurrence_counts[(hospital, table, target)] += 1

    private_occurrences: list[dict[str, Any]] = []
    for index, row in enumerate(evidence.columns, start=1):
        hospital = _required_string(row, "hospital")
        table = _required_string(row, "table")
        raw_name = _required_string(row, "raw_name")
        kind = _required_string(row, "candidate_kind")
        if kind not in {"identifier", "free_text", "categorical", "numeric", "temporal"}:
            raise HarmonizationError("Candidate occurrence kind is invalid")
        target = row.get("candidate_target")
        action, mapping_status = _candidate_action(row)
        proposal = (
            policy.candidate_contract.proposal_for(table, target, kind)
            if isinstance(target, str)
            else _not_applicable_proposal()
        )
        parser_actions, parser_status = _parsing_proposal(row)
        categorical_status, categorical_normalization, categorical_value_count = (
            _categorical_proposal(row, policy.candidate_contract)
        )
        cross_hospital_source_name_count = (
            len(target_sources[(table, target)]) if isinstance(target, str) else 0
        )
        same_hospital_target_occurrence_count = (
            hospital_target_occurrence_counts[(hospital, table, target)]
            if isinstance(target, str)
            else 0
        )
        target_split_count = len(raw_targets[(table, raw_name)])
        semantic_candidate = (
            policy.candidate_contract.semantic_split_for(hospital, table, target)
            if isinstance(target, str)
            else None
        )
        private_occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": hospital,
                "table": table,
                "physical_name": _required_string(row, "physical_name"),
                "raw_name": raw_name,
                "raw_occurrence": row["raw_occurrence"],
                "candidate_target": target,
                "candidate_kind": kind,
                "candidate_status": _required_string(row, "candidate_status"),
                "review_rule_id": row.get("review_rule_id"),
                "proposed_action": action,
                "mapping_review_status": mapping_status,
                "candidate_value_type": proposal.value_type,
                "value_type_review_status": proposal.value_type_status,
                "candidate_unit": proposal.unit,
                "unit_review_status": proposal.unit_status,
                "parser_actions": parser_actions,
                "parser_review_status": parser_status,
                "categorical_review_status": categorical_status,
                "categorical_normalization_candidate": categorical_normalization,
                "categorical_mapping_candidate_value_count": categorical_value_count,
                "cross_hospital_source_name_count": cross_hospital_source_name_count,
                "same_hospital_target_occurrence_count": (
                    same_hospital_target_occurrence_count
                ),
                "coexisting_alias_review_status": (
                    "requires_human_review"
                    if same_hospital_target_occurrence_count > 1
                    else "not_applicable"
                ),
                "source_name_target_count": target_split_count,
                "semantic_split_candidate_id": (
                    semantic_candidate.candidate_id
                    if semantic_candidate is not None
                    else None
                ),
                "semantic_split_proposed_target": (
                    semantic_candidate.proposed_target
                    if semantic_candidate is not None
                    else None
                ),
                "semantic_split_proposed_value_type": (
                    semantic_candidate.proposed_value_type
                    if semantic_candidate is not None
                    else None
                ),
                "semantic_split_proposed_unit": (
                    semantic_candidate.proposed_unit
                    if semantic_candidate is not None
                    else None
                ),
                "semantic_split_evidence_reference": (
                    semantic_candidate.evidence_reference
                    if semantic_candidate is not None
                    else None
                ),
                "semantic_split_review_status": (
                    "requires_human_review"
                    if target_split_count > 1 or semantic_candidate is not None
                    else "not_applicable"
                ),
                "all_missing_or_empty": row.get("all_missing_or_empty"),
                "source_file_count_available": row.get("source_file_count_available"),
                "source_schema_variant_count_available": row.get(
                    "source_schema_variant_count_available"
                ),
                "source_schema_present_row_count": row.get(
                    "source_schema_present_row_count"
                ),
                "expected_row_count": row.get("expected_row_count"),
                "literal_empty_count": row.get("literal_empty_count"),
                "raw_nonempty_count": row.get("raw_nonempty_count"),
                "classification_counts": _classification_counts(row),
                "approved_allowed_tokens": _json_string_list(
                    row, "approved_allowed_tokens"
                ),
                "approved_missing_sentinel_tokens": _json_string_list(
                    row, "approved_missing_sentinel_tokens"
                ),
                "approved_parser": row.get("approved_parser"),
                "review_evidence_run_id": row.get("review_evidence_run_id"),
            }
        )

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in private_occurrences:
        target = row["candidate_target"]
        if isinstance(target, str):
            grouped[(row["table"], target)].append(row)
    private_variables: list[dict[str, Any]] = []
    for index, ((table, target), rows) in enumerate(sorted(grouped.items()), start=1):
        kinds = {row["candidate_kind"] for row in rows}
        types = {row["candidate_value_type"] for row in rows}
        units = {row["candidate_unit"] for row in rows}
        if len(kinds) != 1 or len(types) != 1 or len(units) != 1:
            raise HarmonizationError(
                "A canonical candidate has inconsistent kind, type, or unit proposals"
            )
        categorical_statuses = {row["categorical_review_status"] for row in rows}
        private_variables.append(
            {
                "variable_review_id": f"V{index:04d}",
                "table": table,
                "candidate_target": target,
                "candidate_kind": next(iter(kinds)),
                "candidate_value_type": next(iter(types)),
                "value_type_review_statuses": sorted(
                    {row["value_type_review_status"] for row in rows}
                ),
                "candidate_unit": next(iter(units)),
                "unit_review_statuses": sorted(
                    {row["unit_review_status"] for row in rows}
                ),
                "raw_occurrence_count": len(rows),
                "hospital_count": len({row["hospital"] for row in rows}),
                "source_raw_name_count": len({row["raw_name"] for row in rows}),
                "source_raw_names": sorted({row["raw_name"] for row in rows}),
                "approved_mapping_occurrence_count": sum(
                    row["mapping_review_status"].startswith("human_approved")
                    for row in rows
                ),
                "pending_mapping_occurrence_count": sum(
                    row["mapping_review_status"] == "requires_human_review"
                    for row in rows
                ),
                "all_missing_or_empty_occurrence_count": sum(
                    row["all_missing_or_empty"] is True for row in rows
                ),
                "cross_hospital_name_variant": (
                    len({row["raw_name"] for row in rows}) > 1
                ),
                "coexisting_alias_group": any(
                    row["same_hospital_target_occurrence_count"] > 1
                    for row in rows
                ),
                "semantic_split_candidate_ids": sorted(
                    {
                        row["semantic_split_candidate_id"]
                        for row in rows
                        if row["semantic_split_candidate_id"] is not None
                    }
                ),
                "categorical_review_statuses": sorted(categorical_statuses),
                "parser_review_statuses": sorted(
                    {row["parser_review_status"] for row in rows}
                ),
            }
        )

    occurrence_count = len(private_occurrences)
    disposition_missing = sum(not row["proposed_action"] for row in private_occurrences)
    type_missing = sum(
        row["candidate_target"] is not None
        and row["candidate_value_type"] in {None, "", "not_applicable"}
        for row in private_occurrences
    )
    pending_mapping = sum(
        row["mapping_review_status"] == "requires_human_review"
        for row in private_occurrences
    )
    pending_types = sum(
        not set(row["value_type_review_statuses"]).issubset(APPROVED_STATUSES)
        for row in private_variables
    )
    pending_units = sum(
        not set(row["unit_review_statuses"]).issubset(APPROVED_STATUSES)
        for row in private_variables
    )
    pending_categorical = sum(
        row["candidate_kind"] == "categorical"
        and not set(row["categorical_review_statuses"]).issubset(
            {
                "not_applicable",
                "human_approved_raw_v3_hospital_domain",
                "human_approved_raw_v3_cross_hospital_domain",
            }
        )
        for row in private_variables
    )
    pending_parsers = sum(
        row["parser_review_status"] == "requires_human_review"
        for row in private_occurrences
    )
    cross_hospital_name_variant_groups = sum(
        row["cross_hospital_name_variant"] for row in private_variables
    )
    coexisting_alias_groups = sum(
        row["coexisting_alias_group"] for row in private_variables
    )
    observed_multi_target_source_groups = sum(
        len(targets) > 1 for targets in raw_targets.values()
    )
    known_semantic_split_candidate_ids = {
        row["semantic_split_candidate_id"]
        for row in private_occurrences
        if row["semantic_split_candidate_id"] is not None
    }
    unmatched_semantic_candidates = {
        item.candidate_id
        for item in policy.candidate_contract.semantic_split_candidates
    } - known_semantic_split_candidate_ids
    if unmatched_semantic_candidates:
        raise HarmonizationError(
            "Candidate contract semantic split scope is absent from raw-v3 occurrence evidence"
        )
    semantic_split_groups = (
        observed_multi_target_source_groups
        + len(known_semantic_split_candidate_ids)
    )
    resolved_carried_blockers = sorted(
        set(evidence.carried_blockers)
        & set(policy.resolved_schema_token_blockers)
    )
    unresolved_carried_blockers = sorted(
        set(evidence.carried_blockers)
        - set(policy.resolved_schema_token_blockers)
    )
    carried_archive = int(
        "provisional_archive_owner_confirmation" in unresolved_carried_blockers
    )
    failure_counts = {
        "schema_token_evidence_is_complete_and_compatible": 0,
        "all_raw_occurrences_accounted": int(
            occurrence_count != len(evidence.columns)
        ),
        "all_occurrences_have_explicit_dispositions": disposition_missing,
        "all_retained_targets_have_candidate_types": type_missing,
        "candidate_column_mappings_approved": pending_mapping,
        "candidate_value_types_approved": pending_types,
        "candidate_units_approved": pending_units,
        "candidate_categorical_policies_approved": pending_categorical,
        "candidate_parsing_and_missing_policies_approved": pending_parsers,
        "candidate_aliases_and_semantic_splits_approved": (
            coexisting_alias_groups + semantic_split_groups
        ),
        "ordered_harmonized_union_schema_approved": 1,
        "variable_dictionary_approved": 1,
        "provisional_archive_owner_confirmation": carried_archive,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if failure_counts[name] == 0 else "fail",
            severity="blocking",
            observed={"failure_count": failure_counts[name]},
            expected={"failure_count": 0},
            details=CHECK_DETAILS[name],
        )
        for name in CHECK_ORDER
    ]
    status = overall_status(checks)
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(
        item for item in blockers if item["check"] in TECHNICAL_CHECKS
    )
    hospital_summaries: dict[str, dict[str, int]] = {}
    for hospital in sorted({row["hospital"] for row in private_occurrences}):
        selected = [row for row in private_occurrences if row["hospital"] == hospital]
        hospital_summaries[hospital] = {
            "raw_occurrence_count": len(selected),
            "approved_mapping_occurrence_count": sum(
                row["mapping_review_status"].startswith("human_approved")
                for row in selected
            ),
            "pending_mapping_occurrence_count": sum(
                row["mapping_review_status"] == "requires_human_review"
                for row in selected
            ),
            "pending_parser_occurrence_count": sum(
                row["parser_review_status"] == "requires_human_review"
                for row in selected
            ),
        }
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_harmonization_registry_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "schema_token_inventory_run_id": schema_token_run_id,
            "authoritative_evidence_boundary": "immutable_schema_token_inventory_bundle",
        },
        "metrics": {
            "raw_column_occurrence_count": occurrence_count,
            "accounted_occurrence_count": len(private_occurrences),
            "candidate_canonical_variable_count": len(private_variables),
            "candidate_canonical_variable_count_including_split_targets": (
                len(private_variables) + len(known_semantic_split_candidate_ids)
            ),
            "approved_mapping_occurrence_count": occurrence_count - pending_mapping,
            "pending_mapping_occurrence_count": pending_mapping,
            "explicit_drop_after_precondition_count": sum(
                row["proposed_action"] == "drop_after_all_missing_precondition"
                for row in private_occurrences
            ),
            "cross_hospital_name_variant_group_count": (
                cross_hospital_name_variant_groups
            ),
            "coexisting_alias_group_count": coexisting_alias_groups,
            "candidate_semantic_split_group_count": semantic_split_groups,
            "observed_multi_target_source_group_count": (
                observed_multi_target_source_groups
            ),
            "known_semantic_split_candidate_count": len(
                known_semantic_split_candidate_ids
            ),
            "pending_value_type_variable_count": pending_types,
            "pending_unit_variable_count": pending_units,
            "unresolved_unit_variable_count": sum(
                "unresolved_requires" in status
                for row in private_variables
                for status in row["unit_review_statuses"]
            ),
            "pending_categorical_variable_count": pending_categorical,
            "pending_parser_occurrence_count": pending_parsers,
            "technical_blocking_finding_count": len(technical),
            "resolved_carried_input_blocker_count": len(
                resolved_carried_blockers
            ),
        },
        "hospital_summaries": hospital_summaries,
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only private harmonization registry-review bundle on the authorized cluster",
        },
        "publication": {
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "ingested_data_read": False,
            "harmonization_transformations_applied": False,
            "registry_approved": False,
            "union_schema_approved": False,
            "variable_dictionary_approved": False,
        },
        "resolved_input_findings": [
            {
                "check": check,
                "resolution": (
                    "confirmed_legacy_snapshot_excluded_from_authoritative_inputs"
                ),
            }
            for check in resolved_carried_blockers
        ],
        "limitations": [
            "Candidate names and representations preserve frozen-v2 and legacy knowledge but are not approved merely because they appear in this review.",
            "Unknown units remain explicitly unresolved; distribution similarity is not treated as unit evidence.",
            "Cross-hospital raw-name variants require mapping review but are not alias merges unless multiple sources share a target within one hospital; only those coexisting alias groups require overlap and merge-policy review.",
            "Exact raw headers and bounded token examples remain only in owner-only private bundles on the cluster.",
            "The command reads no ingested Parquet and generates no harmonized, cleaned, derived, concatenated, or pooled data artifact.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_harmonization_registry_review_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "schema_token_inventory_run_id": schema_token_run_id,
            "schema_token_private_directory": str(evidence.private_directory),
            "schema_token_tokens_path": str(evidence.tokens_path),
            "hashes": evidence.input_hashes,
        },
        "policies": {
            "review_policy_version": policy.version,
            "review_policy_path": str(policy.source_path),
            "candidate_contract_version": policy.candidate_contract.version,
            "candidate_contract_path": str(policy.candidate_contract.source_path),
            "schema_token_policy_version": schema_policy.policy_version,
            "resolved_schema_token_blockers": resolved_carried_blockers,
        },
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "occurrence_review_row_count": len(private_occurrences),
        "variable_review_row_count": len(private_variables),
        "production_data_artifacts_generated": False,
        "publication_ready": False,
    }
    return HarmonizationRegistryReviewResult(
        dataset_context=config.dataset_context,
        overall_status=status,
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_manifest=private_manifest,
        private_occurrences=tuple(private_occurrences),
        private_variables=tuple(private_variables),
        review_payload=review_payload,
    )


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


def _review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 candidate harmonization-registry review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input schema/token run: `{payload['inputs']['schema_token_inventory_run_id']}`",
        "- Protected filenames, identifiers, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    lines.extend(
        f"- `{item['check']}`: {item['details']}"
        for item in payload["blocking_findings"]
    )
    lines.extend(["", "## Review totals", ""])
    lines.extend(
        f"- {key}: `{value}`"
        for key, value in payload["metrics"].items()
        if isinstance(value, (str, int))
    )
    lines.extend(["", "## Hospital summaries", ""])
    for hospital, values in payload["hospital_summaries"].items():
        lines.append(
            f"- `{hospital}`: occurrences `{values['raw_occurrence_count']}`; "
            f"mapping-approved `{values['approved_mapping_occurrence_count']}`; "
            f"mapping-pending `{values['pending_mapping_occurrence_count']}`; "
            f"parser-pending `{values['pending_parser_occurrence_count']}`"
        )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the owner-only occurrence and variable workbooks before approving "
        "mappings, types, units, parsers, categorical policies, aliases, semantic "
        "splits, the ordered union schema, or the variable dictionary."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def _private_readme(result: HarmonizationRegistryReviewResult) -> str:
    return (
        "# Owner-only harmonization registry review\n\n"
        "Keep this directory on the authorized cluster. `occurrences.parquet` "
        "maps stable R#### review IDs to exact raw headers and per-occurrence "
        "evidence. `variables.parquet` groups those rows by candidate canonical "
        "variable. Bounded raw-token examples remain in the immutable schema-token "
        "bundle referenced by the private manifest; they are not duplicated here.\n\n"
        "This bundle contains proposals, not approvals, and generates no production "
        "data artifact.\n"
    )


def write_harmonization_registry_review_bundle(
    result: HarmonizationRegistryReviewResult,
    reports_root: Path,
    run_id: str | None = None,
) -> HarmonizationRegistryReviewResult:
    selected_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run_id):
        raise HarmonizationError("Invalid harmonization registry-review run ID")
    private_directory = (
        reports_root / "private" / "harmonization_registry_review" / selected_run_id
    )
    review_directory = reports_root / "review" / "harmonization_registry_review"
    review_json = review_directory / f"{selected_run_id}.json"
    review_markdown = review_directory / f"{selected_run_id}.md"
    if private_directory.exists() or review_json.exists() or review_markdown.exists():
        raise HarmonizationError(
            "Harmonization registry-review run already exists and will not be overwritten"
        )
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.chmod(0o700)
    review_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_directory.chmod(0o750)
    _write_json(
        private_directory / "harmonization_registry_review_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(
        private_directory / "occurrences.parquet", result.private_occurrences
    )
    _write_private_parquet(
        private_directory / "variables.parquet", result.private_variables
    )
    readme_path = private_directory / "README.md"
    descriptor = os.open(readme_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_private_readme(result))
    readme_path.chmod(0o600)
    assert_review_payload_is_safe(result.review_payload)
    _write_json(review_json, result.review_payload, 0o640)
    temporary = review_markdown.with_suffix(".md.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_review_markdown(result.review_payload))
        temporary.replace(review_markdown)
        review_markdown.chmod(0o640)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return HarmonizationRegistryReviewResult(
        **{
            **asdict(result),
            "private_report_directory": private_directory,
            "review_json_path": review_json,
            "review_markdown_path": review_markdown,
        }
    )
