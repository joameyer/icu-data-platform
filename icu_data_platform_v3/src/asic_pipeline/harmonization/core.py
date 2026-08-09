from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import math

import pyarrow as pa

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization.parsing import (
    parse_decimal_comma_numeric,
    parse_direct_numeric,
    parse_numeric_list,
)
from asic_pipeline.harmonization.registry import HarmonizationRule
from asic_pipeline.harmonization.static_decisions import (
    ReviewedStaticBooleanDecision,
    ReviewedStaticNumericDecision,
    ReviewedStayIdentifierDecision,
)


@dataclass
class HarmonizationMetrics:
    input_cell_count: int = 0
    source_schema_absent_count: int = 0
    literal_empty_missing_count: int = 0
    direct_numeric_count: int = 0
    custom_parsed_count: int = 0
    approved_missing_sentinel_count: int = 0
    approved_categorical_count: int = 0
    unresolved_count: int = 0
    numeric_list_cell_count: int = 0
    numeric_list_element_count: int = 0
    numeric_list_numeric_element_count: int = 0
    numeric_list_approved_missing_element_count: int = 0
    numeric_list_unresolved_element_count: int = 0

    @property
    def cell_accounting_is_conserved(self) -> bool:
        return self.input_cell_count == (
            self.source_schema_absent_count
            + self.literal_empty_missing_count
            + self.direct_numeric_count
            + self.custom_parsed_count
            + self.approved_missing_sentinel_count
            + self.approved_categorical_count
            + self.unresolved_count
        )

    @property
    def list_element_accounting_is_conserved(self) -> bool:
        return self.numeric_list_element_count == (
            self.numeric_list_numeric_element_count
            + self.numeric_list_approved_missing_element_count
            + self.numeric_list_unresolved_element_count
        )


@dataclass(frozen=True)
class HarmonizedColumnBatch:
    rule: HarmonizationRule
    values: pa.Array
    parse_status: pa.StringArray
    metrics: HarmonizationMetrics

    @property
    def publication_blocked(self) -> bool:
        return self.metrics.unresolved_count > 0

    def require_resolved(self) -> None:
        if self.publication_blocked:
            raise HarmonizationError(
                f"Harmonization rule {self.rule.rule_id} has unresolved source tokens"
            )
        if not self.metrics.cell_accounting_is_conserved:
            raise HarmonizationError(
                f"Harmonization rule {self.rule.rule_id} failed cell accounting"
            )
        if not self.metrics.list_element_accounting_is_conserved:
            raise HarmonizationError(
                f"Harmonization rule {self.rule.rule_id} failed list-element accounting"
            )


@dataclass(frozen=True)
class HarmonizedStaticBooleanBatch:
    decision: ReviewedStaticBooleanDecision
    values: pa.BooleanArray
    parse_status: pa.StringArray
    metrics: HarmonizationMetrics

    @property
    def publication_blocked(self) -> bool:
        return self.metrics.unresolved_count > 0

    def require_resolved(self) -> None:
        if self.publication_blocked:
            raise HarmonizationError(
                f"Harmonization rule {self.decision.rule_id} has unresolved source tokens"
            )
        if not self.metrics.cell_accounting_is_conserved:
            raise HarmonizationError(
                f"Harmonization rule {self.decision.rule_id} failed cell accounting"
            )


@dataclass
class IdentifierHarmonizationMetrics:
    input_cell_count: int = 0
    valid_identifier_count: int = 0
    source_missing_count: int = 0
    literal_empty_count: int = 0
    surrounding_whitespace_count: int = 0
    separator_collision_count: int = 0
    duplicate_local_identifier_row_count: int = 0

    @property
    def accounting_is_conserved(self) -> bool:
        return self.input_cell_count == (
            self.valid_identifier_count
            + self.source_missing_count
            + self.literal_empty_count
            + self.surrounding_whitespace_count
            + self.separator_collision_count
            + self.duplicate_local_identifier_row_count
        )

    @property
    def unresolved_count(self) -> int:
        return (
            self.source_missing_count
            + self.literal_empty_count
            + self.surrounding_whitespace_count
            + self.separator_collision_count
            + self.duplicate_local_identifier_row_count
        )


@dataclass(frozen=True)
class HarmonizedStaticIdentifierBatch:
    decision: ReviewedStayIdentifierDecision
    hospital: str
    stay_id_local: pa.LargeStringArray
    stay_id_global: pa.LargeStringArray
    parse_status: pa.StringArray
    metrics: IdentifierHarmonizationMetrics

    @property
    def publication_blocked(self) -> bool:
        return self.metrics.unresolved_count > 0

    def require_resolved(self) -> None:
        if self.publication_blocked:
            raise HarmonizationError(
                f"Harmonization rule {self.decision.rule_id} has invalid or duplicate identifiers"
            )
        if not self.metrics.accounting_is_conserved:
            raise HarmonizationError(
                f"Harmonization rule {self.decision.rule_id} failed identifier accounting"
            )


def _output_type(rule: HarmonizationRule) -> pa.DataType:
    if rule.canonical_value_type == "list_float64":
        return pa.list_(pa.float64())
    if rule.kind == "numeric":
        return pa.float64()
    if rule.kind == "categorical":
        return pa.string()
    raise HarmonizationError(
        f"Harmonization rule {rule.rule_id} has unsupported bootstrap kind {rule.kind}"
    )


def _finite(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value


def harmonize_reviewed_column_batch(
    source: pa.Array | pa.ChunkedArray,
    rule: HarmonizationRule,
) -> HarmonizedColumnBatch:
    if isinstance(source, pa.ChunkedArray):
        source = source.combine_chunks()
    if not pa.types.is_string(source.type) and not pa.types.is_large_string(source.type):
        raise HarmonizationError(
            f"Harmonization rule {rule.rule_id} requires lossless raw strings"
        )
    metrics = HarmonizationMetrics(input_cell_count=len(source))
    output: list[object] = []
    statuses: list[str] = []
    sentinels = set(rule.approved_missing_sentinel_tokens)
    allowed = set(rule.allowed_tokens)
    for raw_value in source.to_pylist():
        if raw_value is None:
            metrics.source_schema_absent_count += 1
            output.append(None)
            statuses.append("source_schema_absent")
            continue
        if not isinstance(raw_value, str):
            raise HarmonizationError(
                f"Harmonization rule {rule.rule_id} encountered a non-string source cell"
            )
        if raw_value == "":
            metrics.literal_empty_missing_count += 1
            output.append(None)
            statuses.append("literal_empty_missing")
            continue
        stripped = raw_value.strip()
        if rule.kind == "categorical":
            if stripped in allowed:
                metrics.approved_categorical_count += 1
                output.append(stripped)
                statuses.append("approved_categorical")
            else:
                metrics.unresolved_count += 1
                output.append(None)
                statuses.append("unresolved")
            continue
        if rule.kind != "numeric":
            raise HarmonizationError(
                f"Harmonization rule {rule.rule_id} has unsupported kind {rule.kind}"
            )
        if stripped in sentinels:
            metrics.approved_missing_sentinel_count += 1
            output.append(None)
            statuses.append("approved_missing_sentinel")
            continue
        if rule.approved_parser == "approved_missing_only":
            metrics.unresolved_count += 1
            output.append(None)
            statuses.append("unresolved")
            continue
        if rule.approved_parser == "numeric_list":
            parsed_list = parse_numeric_list(
                raw_value, rule.approved_list_missing_tokens
            )
            if parsed_list.recognized_list:
                metrics.numeric_list_cell_count += 1
                metrics.numeric_list_element_count += parsed_list.element_count
                metrics.numeric_list_numeric_element_count += (
                    parsed_list.numeric_element_count
                )
                metrics.numeric_list_approved_missing_element_count += (
                    parsed_list.approved_missing_element_count
                )
                metrics.numeric_list_unresolved_element_count += (
                    parsed_list.unresolved_element_count
                )
            if parsed_list.resolved:
                metrics.custom_parsed_count += 1
                output.append(list(parsed_list.values))
                statuses.append(
                    "numeric_list_parsed_with_missing_elements"
                    if parsed_list.approved_missing_element_count
                    else "numeric_list_parsed"
                )
            else:
                metrics.unresolved_count += 1
                output.append(None)
                statuses.append("unresolved")
            continue
        direct = _finite(parse_direct_numeric(raw_value))
        if direct is not None:
            metrics.direct_numeric_count += 1
            output.append(direct)
            statuses.append("direct_numeric")
            continue
        if rule.approved_parser == "decimal_comma":
            decimal_comma = _finite(parse_decimal_comma_numeric(raw_value))
            if decimal_comma is not None:
                metrics.custom_parsed_count += 1
                output.append(decimal_comma)
                statuses.append("decimal_comma_parsed")
                continue
        metrics.unresolved_count += 1
        output.append(None)
        statuses.append("unresolved")
    result = HarmonizedColumnBatch(
        rule=rule,
        values=pa.array(output, type=_output_type(rule)),
        parse_status=pa.array(statuses, type=pa.string()),
        metrics=metrics,
    )
    if not metrics.cell_accounting_is_conserved:
        raise HarmonizationError(
            f"Harmonization rule {rule.rule_id} failed cell accounting"
        )
    if not metrics.list_element_accounting_is_conserved:
        raise HarmonizationError(
            f"Harmonization rule {rule.rule_id} failed list-element accounting"
        )
    return result


def harmonize_reviewed_static_boolean_batch(
    source: pa.Array | pa.ChunkedArray,
    decision: ReviewedStaticBooleanDecision,
) -> HarmonizedStaticBooleanBatch:
    if isinstance(source, pa.ChunkedArray):
        source = source.combine_chunks()
    if not pa.types.is_string(source.type) and not pa.types.is_large_string(source.type):
        raise HarmonizationError(
            f"Harmonization rule {decision.rule_id} requires lossless raw strings"
        )
    metrics = HarmonizationMetrics(input_cell_count=len(source))
    values: list[bool | None] = []
    statuses: list[str] = []
    mapping = dict(decision.value_mapping)
    for raw_value in source.to_pylist():
        if raw_value is None:
            metrics.source_schema_absent_count += 1
            values.append(None)
            statuses.append("source_schema_absent")
            continue
        if not isinstance(raw_value, str):
            raise HarmonizationError(
                f"Harmonization rule {decision.rule_id} encountered a non-string source cell"
            )
        if raw_value == "":
            metrics.literal_empty_missing_count += 1
            values.append(None)
            statuses.append("literal_empty_missing")
            continue
        normalized = decision.normalized_value(raw_value)
        if normalized in mapping:
            metrics.approved_categorical_count += 1
            values.append(mapping[normalized])
            statuses.append("approved_boolean_mapping")
            continue
        metrics.unresolved_count += 1
        values.append(None)
        statuses.append("unresolved")
    result = HarmonizedStaticBooleanBatch(
        decision=decision,
        values=pa.array(values, type=pa.bool_()),
        parse_status=pa.array(statuses, type=pa.string()),
        metrics=metrics,
    )
    if not metrics.cell_accounting_is_conserved:
        raise HarmonizationError(
            f"Harmonization rule {decision.rule_id} failed cell accounting"
        )
    return result


def harmonize_reviewed_static_numeric_batch(
    source: pa.Array | pa.ChunkedArray,
    decision: ReviewedStaticNumericDecision,
) -> HarmonizedColumnBatch:
    return harmonize_reviewed_column_batch(source, decision.as_harmonization_rule())


def harmonize_reviewed_static_identifier_batch(
    source: pa.Array | pa.ChunkedArray,
    hospital: str,
    decision: ReviewedStayIdentifierDecision,
) -> HarmonizedStaticIdentifierBatch:
    if isinstance(source, pa.ChunkedArray):
        source = source.combine_chunks()
    if not pa.types.is_string(source.type) and not pa.types.is_large_string(source.type):
        raise HarmonizationError(
            f"Harmonization rule {decision.rule_id} requires lossless raw strings"
        )
    suffix = decision.suffix_for(hospital)
    raw_values = source.to_pylist()
    eligible = [
        value
        for value in raw_values
        if isinstance(value, str)
        and value != ""
        and value.strip() == value
        and decision.separator not in value
    ]
    counts = Counter(eligible)
    duplicate_values = {value for value, count in counts.items() if count > 1}
    metrics = IdentifierHarmonizationMetrics(input_cell_count=len(raw_values))
    local_values: list[str | None] = []
    global_values: list[str | None] = []
    statuses: list[str] = []
    for raw_value in raw_values:
        if raw_value is None:
            metrics.source_missing_count += 1
            local_values.append(None)
            global_values.append(None)
            statuses.append("source_missing_identifier")
            continue
        if not isinstance(raw_value, str):
            raise HarmonizationError(
                f"Harmonization rule {decision.rule_id} encountered a non-string source cell"
            )
        if raw_value == "":
            metrics.literal_empty_count += 1
            local_values.append(None)
            global_values.append(None)
            statuses.append("literal_empty_identifier")
            continue
        if raw_value.strip() != raw_value:
            metrics.surrounding_whitespace_count += 1
            local_values.append(None)
            global_values.append(None)
            statuses.append("invalid_identifier_whitespace")
            continue
        if decision.separator in raw_value:
            metrics.separator_collision_count += 1
            local_values.append(None)
            global_values.append(None)
            statuses.append("identifier_separator_collision")
            continue
        if raw_value in duplicate_values:
            metrics.duplicate_local_identifier_row_count += 1
            local_values.append(None)
            global_values.append(None)
            statuses.append("duplicate_local_identifier")
            continue
        metrics.valid_identifier_count += 1
        local_values.append(raw_value)
        global_values.append(f"{raw_value}{decision.separator}{suffix}")
        statuses.append("approved_identifier_derived")
    result = HarmonizedStaticIdentifierBatch(
        decision=decision,
        hospital=hospital,
        stay_id_local=pa.array(local_values, type=pa.large_string()),
        stay_id_global=pa.array(global_values, type=pa.large_string()),
        parse_status=pa.array(statuses, type=pa.string()),
        metrics=metrics,
    )
    if not metrics.accounting_is_conserved:
        raise HarmonizationError(
            f"Harmonization rule {decision.rule_id} failed identifier accounting"
        )
    return result
