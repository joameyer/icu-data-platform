from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pyarrow as pa

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.contract import (
    FullyReviewedRegistry,
    RegistryContractPolicy,
    ReviewedRegistryRule,
)


@dataclass(frozen=True)
class RegistryEvidenceValidation:
    expected_occurrence_count: int
    observed_occurrence_count: int
    retained_occurrence_count: int
    reviewed_drop_occurrence_count: int


@dataclass(frozen=True)
class PlannedSourceField:
    source_field_name: str
    raw_name: str
    occurrence: int
    rule: ReviewedRegistryRule


@dataclass(frozen=True)
class HarmonizationTablePlan:
    hospital: str
    table: str
    source_fields: tuple[PlannedSourceField, ...]
    candidate_output_schema: pa.Schema
    dropped_source_count: int
    alias_group_count: int
    production_execution_allowed: bool
    union_schema_approved: bool

    def require_execution_ready(self) -> None:
        if not self.production_execution_allowed or not self.union_schema_approved:
            raise HarmonizationError(
                "Harmonization table execution is blocked pending production and union-schema approval"
            )


def _required_string(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise HarmonizationError(f"Registry-review occurrence lacks {key}")
    return value


def validate_registry_against_review_occurrences(
    registry: FullyReviewedRegistry,
    occurrence_rows: Iterable[dict[str, Any]],
) -> RegistryEvidenceValidation:
    rows = tuple(occurrence_rows)
    if len(rows) != registry.expected_occurrence_count:
        raise HarmonizationError(
            "Reviewed registry and private occurrence workbook have different row counts"
        )
    by_review_id: dict[str, dict[str, Any]] = {}
    scopes: set[tuple[str, str, str, int]] = set()
    for row in rows:
        review_id = _required_string(row, "review_item_id")
        hospital = _required_string(row, "hospital")
        table = _required_string(row, "table")
        raw_name = _required_string(row, "raw_name")
        occurrence = row.get("raw_occurrence")
        nonempty = row.get("raw_nonempty_count")
        if review_id in by_review_id:
            raise HarmonizationError("Private occurrence workbook has duplicate review IDs")
        if table not in {"static", "dynamic"}:
            raise HarmonizationError("Private occurrence workbook table is invalid")
        if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence <= 0:
            raise HarmonizationError("Private occurrence position is invalid")
        if not isinstance(nonempty, int) or isinstance(nonempty, bool) or nonempty < 0:
            raise HarmonizationError("Private occurrence non-empty count is invalid")
        scope = (hospital, table, raw_name, occurrence)
        if scope in scopes:
            raise HarmonizationError("Private occurrence workbook has duplicate scopes")
        scopes.add(scope)
        by_review_id[review_id] = row
    registry_ids = {rule.review_item_id for rule in registry.rules}
    if registry_ids != set(by_review_id):
        raise HarmonizationError(
            "Reviewed registry does not cover the exact private occurrence review IDs"
        )
    for rule in registry.rules:
        row = by_review_id[rule.review_item_id]
        observed_scope = (
            row["hospital"],
            row["table"],
            row["raw_name"],
            row["raw_occurrence"],
        )
        if rule.scope != observed_scope:
            raise HarmonizationError(
                f"Reviewed registry rule {rule.rule_id} differs from its private occurrence scope"
            )
        if rule.raw_nonempty_count != row["raw_nonempty_count"]:
            raise HarmonizationError(
                f"Reviewed registry rule {rule.rule_id} evidence count changed"
            )
        if rule.action == "drop_after_all_missing" and (
            row["raw_nonempty_count"] != 0 or row.get("all_missing_or_empty") is not True
        ):
            raise HarmonizationError(
                f"Reviewed registry drop rule {rule.rule_id} failed its all-missing precondition"
            )
    return RegistryEvidenceValidation(
        expected_occurrence_count=registry.expected_occurrence_count,
        observed_occurrence_count=len(rows),
        retained_occurrence_count=sum(rule.action == "retain" for rule in registry.rules),
        reviewed_drop_occurrence_count=sum(
            rule.action == "drop_after_all_missing" for rule in registry.rules
        ),
    )


def _raw_identity(field: pa.Field) -> tuple[str, int]:
    metadata = field.metadata or {}
    try:
        role = metadata[b"asic_v3_role"].decode("utf-8")
        raw_name = metadata[b"raw_name"].decode("utf-8")
        occurrence = int(metadata[b"raw_occurrence"].decode("ascii"))
    except (KeyError, UnicodeError, ValueError) as exc:
        raise HarmonizationError("Lossless raw field metadata is incomplete") from exc
    if role != "raw_clinical_string" or not raw_name or occurrence <= 0:
        raise HarmonizationError("Lossless raw field metadata is invalid")
    return raw_name, occurrence


def _arrow_type(value_type: str) -> pa.DataType:
    types: dict[str, pa.DataType] = {
        "large_string": pa.large_string(),
        "float64": pa.float64(),
        "list_float64": pa.list_(pa.float64()),
        "bool": pa.bool_(),
        "timestamp_ns": pa.timestamp("ns"),
        "fixed_size_list_bool_2": pa.list_(pa.bool_(), 2),
    }
    try:
        return types[value_type]
    except KeyError as exc:
        raise HarmonizationError(
            f"Reviewed registry contains unsupported value type {value_type!r}"
        ) from exc


def plan_harmonization_table(
    policy: RegistryContractPolicy,
    registry: FullyReviewedRegistry,
    hospital: str,
    table: str,
    source_schema: pa.Schema,
    provenance_columns: Iterable[str],
) -> HarmonizationTablePlan:
    if not policy.allow_synthetic_schema_planning:
        raise HarmonizationError("Synthetic harmonization schema planning is disabled")
    if policy.allow_production_reads or policy.allow_value_transforms:
        raise HarmonizationError("Synthetic planner policy unexpectedly enables production")
    if table not in {"static", "dynamic"}:
        raise HarmonizationError("Harmonization table must be static or dynamic")
    rules = registry.rules_for(hospital, table)
    rule_by_scope = {(rule.raw_name, rule.occurrence): rule for rule in rules}
    if len(rule_by_scope) != len(rules):
        raise HarmonizationError("Reviewed table registry contains duplicate source scopes")
    provenance = set(provenance_columns)
    planned: list[PlannedSourceField] = []
    observed_scopes: set[tuple[str, int]] = set()
    for field in source_schema:
        if field.name in provenance:
            continue
        if not pa.types.is_string(field.type) and not pa.types.is_large_string(field.type):
            raise HarmonizationError(
                "Lossless clinical source fields must remain Arrow string values"
            )
        raw_name, occurrence = _raw_identity(field)
        scope = (raw_name, occurrence)
        if scope in observed_scopes:
            raise HarmonizationError("Lossless source schema contains a duplicate raw scope")
        observed_scopes.add(scope)
        rule = rule_by_scope.get(scope)
        if rule is None:
            raise HarmonizationError("Lossless source schema contains an unreviewed raw field")
        planned.append(
            PlannedSourceField(
                source_field_name=field.name,
                raw_name=raw_name,
                occurrence=occurrence,
                rule=rule,
            )
        )
    if observed_scopes != set(rule_by_scope):
        raise HarmonizationError(
            "Reviewed table registry and lossless source schema do not have exact coverage"
        )
    output_fields: list[pa.Field] = []
    emitted_targets: set[str] = set()
    alias_groups: set[str] = set()
    for item in planned:
        rule = item.rule
        if rule.action != "retain":
            continue
        assert rule.target is not None and rule.value_type is not None
        if rule.alias_group_id is not None:
            alias_groups.add(rule.alias_group_id)
        if rule.target in emitted_targets:
            continue
        emitted_targets.add(rule.target)
        output_fields.append(
            pa.field(
                rule.target,
                _arrow_type(rule.value_type),
                metadata={
                    b"asic_v3_contract_status": b"candidate_not_union_approved",
                    b"source_registry_version": registry.version.encode("ascii"),
                },
            )
        )
    return HarmonizationTablePlan(
        hospital=hospital,
        table=table,
        source_fields=tuple(planned),
        candidate_output_schema=pa.schema(output_fields),
        dropped_source_count=sum(
            item.rule.action == "drop_after_all_missing" for item in planned
        ),
        alias_group_count=len(alias_groups),
        production_execution_allowed=False,
        union_schema_approved=False,
    )
