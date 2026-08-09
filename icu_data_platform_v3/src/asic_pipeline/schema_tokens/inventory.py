from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.errors import SchemaTokenError
from asic_pipeline.ingestion.policy import load_ingestion_policy
from asic_pipeline.harmonization.parsing import parse_numeric_list
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.policy import load_inventory_policy
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.schema_tokens.config import SchemaTokenConfig
from asic_pipeline.schema_tokens.policy import (
    CandidateRule,
    SchemaTokenPolicy,
    load_schema_token_policy,
)


NUMBER_PATTERN = r"[+-]?(?:(?:\d+(?:[.,]\d*)?)|(?:[.,]\d+))(?:[eE][+-]?\d+)?"
DIRECT_NUMERIC = re.compile(
    r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$"
)
DECIMAL_COMMA = re.compile(
    r"^[+-]?(?:(?:\d+(?:,\d*)?)|(?:,\d+))(?:[eE][+-]?\d+)?$"
)
RATIO = re.compile(rf"^({NUMBER_PATTERN})\s*[:/]\s*({NUMBER_PATTERN})$")
THRESHOLD = re.compile(rf"^[<>]=?\s*({NUMBER_PATTERN})$")
PERCENTAGE = re.compile(rf"^({NUMBER_PATTERN})\s*%$")

TECHNICAL_CHECKS = frozenset(
    {
        "complete_input_hospital_set",
        "input_parquet_hashes_match",
        "raw_schema_matches_ingestion_manifests",
        "source_schema_null_conservation",
        "token_accounting_conservation",
        "reviewed_rule_evidence_counts",
    }
)
CHECK_ORDER = (
    "complete_input_hospital_set",
    "input_parquet_hashes_match",
    "raw_schema_matches_ingestion_manifests",
    "source_schema_null_conservation",
    "token_accounting_conservation",
    "reviewed_rule_evidence_counts",
    "reviewed_rule_token_domains",
    "all_raw_columns_in_candidate_registry",
    "numeric_candidate_unresolved_tokens",
    "token_examples_complete_for_review",
    "reviewed_all_empty_precondition",
    "candidate_harmonization_registry_approved",
    "provisional_archive_owner_confirmation",
)
CHECK_DETAILS = {
    "complete_input_hospital_set": "Every approved hospital must have both validated ingested tables.",
    "input_parquet_hashes_match": "Every scanned Parquet file must match its immutable ingestion and audit hashes.",
    "raw_schema_matches_ingestion_manifests": "Every raw physical field and positional identity must match ingestion metadata.",
    "source_schema_null_conservation": "Parquet nulls must represent only rows whose source schema lacked that raw occurrence.",
    "token_accounting_conservation": "Every cell and every non-empty token must be classified exactly once.",
    "reviewed_rule_evidence_counts": "Every production rule must still cover the exact non-empty count approved from its immutable review evidence.",
    "reviewed_rule_token_domains": "Every token governed by an approved categorical domain must match its hospital-scoped allowed set.",
    "all_raw_columns_in_candidate_registry": "Every raw column occurrence requires a reviewed harmonization decision.",
    "numeric_candidate_unresolved_tokens": "Every non-empty candidate numeric token must be directly numeric or assigned a reviewable custom/missing class.",
    "token_examples_complete_for_review": "Reviewable categorical, custom, unknown, and unresolved token examples must not exceed the bounded evidence cap.",
    "reviewed_all_empty_precondition": "The reviewed second positional UK00 duplicate must remain non-empty zero times before a harmonization drop is considered.",
    "candidate_harmonization_registry_approved": "Prior v2 and legacy mappings are candidates only and require raw-v3 human review.",
    "provisional_archive_owner_confirmation": "The excluded archive remains a publication blocker pending owner confirmation.",
}
EXHAUSTIVE_REVIEW_CLASSES = frozenset(
    {
        "categorical_token",
        "textual_missing_candidate",
        "whitespace_only_candidate",
        "ratio_syntax_candidate",
        "decimal_comma_syntax_candidate",
        "threshold_syntax_candidate",
        "percentage_syntax_candidate",
        "custom_parsed_numeric_list",
        "custom_parsed_numeric_list_with_missing_elements",
        "unresolved_token",
        "unresolved_ratio_zero_denominator",
    }
)


@dataclass(frozen=True)
class SchemaTokenInventoryResult:
    dataset_context: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_columns: tuple[dict[str, Any], ...]
    private_tokens: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass(frozen=True)
class _AuditEvidence:
    directory: Path
    manifest: dict[str, Any]
    blockers: tuple[str, ...]


@dataclass
class _FailureCollector:
    counts: Counter[str] = field(default_factory=Counter)
    hospitals: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    details: list[dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        check: str,
        detail: str,
        hospital: str | None = None,
        table: str | None = None,
    ) -> None:
        self.counts[check] += 1
        if hospital is not None:
            self.hospitals[check].add(hospital)
        if len(self.details) < 1000:
            item: dict[str, Any] = {"check": check, "detail": detail}
            if hospital is not None:
                item["hospital"] = hospital
            if table is not None:
                item["table"] = table
            self.details.append(item)


@dataclass
class _NumericSummary:
    count: int = 0
    minimum: float | None = None
    maximum: float | None = None
    nonfinite_count: int = 0
    magnitude_counts: Counter[str] = field(default_factory=Counter)

    def add(self, value: float, count: int) -> None:
        if not math.isfinite(value):
            self.nonfinite_count += count
            return
        self.count += count
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        absolute = abs(value)
        if value < 0:
            label = "negative"
        elif value == 0:
            label = "zero"
        elif absolute < 0.01:
            label = "positive_lt_0_01"
        elif absolute < 1:
            label = "positive_0_01_to_lt_1"
        elif absolute < 10:
            label = "positive_1_to_lt_10"
        elif absolute < 100:
            label = "positive_10_to_lt_100"
        elif absolute < 1000:
            label = "positive_100_to_lt_1000"
        else:
            label = "positive_ge_1000"
        self.magnitude_counts[label] += count

    def to_dict(self) -> dict[str, Any]:
        return {
            "finite_count": self.count,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "nonfinite_count": self.nonfinite_count,
            "magnitude_counts": dict(sorted(self.magnitude_counts.items())),
        }


@dataclass(frozen=True)
class _TokenClassification:
    name: str
    numeric_values: tuple[float, ...] = ()
    is_numeric_list: bool = False
    list_element_count: int = 0
    list_approved_missing_element_count: int = 0
    list_unresolved_element_count: int = 0


@dataclass
class _TokenExamples:
    maximum: int
    counts: dict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter)
    )
    truncated_classes: set[str] = field(default_factory=set)
    untracked_rows: Counter[str] = field(default_factory=Counter)

    def add(self, classification: str, token: str, count: int) -> None:
        class_counts = self.counts[classification]
        if token in class_counts:
            class_counts[token] += count
        elif len(class_counts) < self.maximum:
            class_counts[token] = count
        else:
            self.truncated_classes.add(classification)
            self.untracked_rows[classification] += count


@dataclass
class _ColumnStats:
    hospital: str
    table: str
    physical_name: str
    raw_name: str
    occurrence: int
    candidate: CandidateRule | None
    source_file_count: int
    source_schema_variant_count: int
    source_schema_present_row_count: int
    expected_rows: int
    maximum_examples: int
    store_direct_numeric_examples: bool
    store_identifier_examples: bool
    store_free_text_examples: bool
    scanned_rows: int = 0
    null_count: int = 0
    literal_empty_count: int = 0
    raw_nonempty_count: int = 0
    classification_counts: Counter[str] = field(default_factory=Counter)
    sentinel_candidate_counts: Counter[str] = field(default_factory=Counter)
    numeric_summaries: dict[str, _NumericSummary] = field(
        default_factory=lambda: defaultdict(_NumericSummary)
    )
    examples: _TokenExamples = field(init=False)
    numeric_list_cell_count: int = 0
    numeric_list_element_count: int = 0
    numeric_list_numeric_element_count: int = 0
    numeric_list_approved_missing_element_count: int = 0
    numeric_list_unresolved_element_count: int = 0

    def __post_init__(self) -> None:
        self.examples = _TokenExamples(self.maximum_examples)

    @property
    def kind(self) -> str:
        return self.candidate.kind if self.candidate is not None else "unknown"

    @property
    def target(self) -> str | None:
        return self.candidate.target if self.candidate is not None else None

    @property
    def candidate_status(self) -> str:
        return (
            self.candidate.status
            if self.candidate is not None
            else "unmapped_raw_column_requires_review"
        )

    @property
    def review_rule_id(self) -> str | None:
        return self.candidate.review_rule_id if self.candidate is not None else None

    @property
    def reviewed_domain_violation_count(self) -> int:
        return self.classification_counts["unapproved_categorical_token"]

    def _should_store(self, classification: str) -> bool:
        if classification == "direct_numeric":
            return self.store_direct_numeric_examples
        if classification == "identifier_token_redacted":
            return self.store_identifier_examples
        if classification == "free_text_token_redacted":
            return self.store_free_text_examples
        return True

    def add_batch(self, array: pa.Array, policy: SchemaTokenPolicy) -> None:
        self.scanned_rows += len(array)
        redacted_class = None
        if self.kind == "identifier" and not self.store_identifier_examples:
            redacted_class = "identifier_token_redacted"
        elif self.kind == "free_text" and not self.store_free_text_examples:
            redacted_class = "free_text_token_redacted"
        if redacted_class is not None:
            null_count = array.null_count
            empty_count = pc.sum(
                pc.cast(
                    pc.fill_null(pc.equal(array, ""), False),
                    pa.int64(),
                )
            ).as_py()
            empty_count = int(empty_count or 0)
            nonempty_count = len(array) - null_count - empty_count
            self.null_count += null_count
            self.literal_empty_count += empty_count
            self.raw_nonempty_count += nonempty_count
            self.classification_counts[redacted_class] += nonempty_count
            return
        try:
            value_counts = pc.value_counts(array).to_pylist()
        except pa.ArrowException as exc:
            raise SchemaTokenError("Raw string values cannot be counted") from exc
        for item in value_counts:
            token = item["values"]
            count = int(item["counts"])
            if token is None:
                self.null_count += count
                continue
            if not isinstance(token, str):
                raise SchemaTokenError("Ingested raw clinical fields must be strings")
            if token == "":
                self.literal_empty_count += count
                continue
            self.raw_nonempty_count += count
            classified = _classify_token_detail(
                token, self.candidate, policy.textual_missing_candidates
            )
            classification = classified.name
            self.classification_counts[classification] += count
            if classified.is_numeric_list:
                self.numeric_list_cell_count += count
                self.numeric_list_element_count += (
                    classified.list_element_count * count
                )
                self.numeric_list_numeric_element_count += (
                    len(classified.numeric_values) * count
                )
                self.numeric_list_approved_missing_element_count += (
                    classified.list_approved_missing_element_count * count
                )
                self.numeric_list_unresolved_element_count += (
                    classified.list_unresolved_element_count * count
                )
            sentinel_tokens = (
                self.candidate.approved_missing_sentinel_tokens
                if self.candidate is not None
                else ()
            )
            if token.strip() in sentinel_tokens:
                self.sentinel_candidate_counts[token.strip()] += count
            for numeric_value in classified.numeric_values:
                self.numeric_summaries[classification].add(numeric_value, count)
            if self._should_store(classification):
                self.examples.add(classification, token, count)

    def to_private_row(self) -> dict[str, Any]:
        return {
            "hospital": self.hospital,
            "table": self.table,
            "physical_name": self.physical_name,
            "raw_name": self.raw_name,
            "raw_occurrence": self.occurrence,
            "candidate_target": self.target,
            "candidate_kind": self.kind,
            "candidate_status": self.candidate_status,
            "review_rule_id": self.review_rule_id,
            "approved_parser": (
                self.candidate.approved_parser if self.candidate is not None else None
            ),
            "approved_list_missing_tokens": (
                list(self.candidate.approved_list_missing_tokens)
                if self.candidate is not None
                else []
            ),
            "canonical_value_type": (
                self.candidate.canonical_value_type
                if self.candidate is not None
                else None
            ),
            "expected_unit": (
                self.candidate.expected_unit if self.candidate is not None else None
            ),
            "preserve_list_order": (
                self.candidate.preserve_list_order
                if self.candidate is not None
                else False
            ),
            "preserve_duplicates": (
                self.candidate.preserve_duplicates
                if self.candidate is not None
                else False
            ),
            "approved_allowed_tokens": (
                list(self.candidate.allowed_tokens)
                if self.candidate is not None
                else []
            ),
            "approved_missing_sentinel_tokens": (
                list(self.candidate.approved_missing_sentinel_tokens)
                if self.candidate is not None
                else []
            ),
            "review_evidence_nonempty_count": (
                self.candidate.evidence_nonempty_count
                if self.candidate is not None
                else None
            ),
            "review_evidence_run_id": (
                self.candidate.evidence_run_id
                if self.candidate is not None
                else None
            ),
            "numeric_list_cell_count": self.numeric_list_cell_count,
            "numeric_list_element_count": self.numeric_list_element_count,
            "numeric_list_numeric_element_count": self.numeric_list_numeric_element_count,
            "numeric_list_approved_missing_element_count": (
                self.numeric_list_approved_missing_element_count
            ),
            "numeric_list_unresolved_element_count": (
                self.numeric_list_unresolved_element_count
            ),
            "source_file_count_available": self.source_file_count,
            "source_schema_variant_count_available": self.source_schema_variant_count,
            "source_schema_present_row_count": self.source_schema_present_row_count,
            "expected_row_count": self.expected_rows,
            "scanned_row_count": self.scanned_rows,
            "parquet_null_count": self.null_count,
            "literal_empty_count": self.literal_empty_count,
            "raw_nonempty_count": self.raw_nonempty_count,
            "classification_counts": dict(sorted(self.classification_counts.items())),
            "numeric_summary_by_class": {
                key: value.to_dict()
                for key, value in sorted(self.numeric_summaries.items())
            },
            "numeric_missing_sentinel_candidate_counts": dict(
                sorted(self.sentinel_candidate_counts.items())
            ),
            "token_example_classes_truncated": sorted(
                self.examples.truncated_classes
            ),
            "untracked_token_rows_by_class": dict(
                sorted(self.examples.untracked_rows.items())
            ),
            "all_missing_or_empty": self.raw_nonempty_count == 0,
        }

    def token_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for classification, counts in sorted(self.examples.counts.items()):
            for token, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
                rows.append(
                    {
                        "hospital": self.hospital,
                        "table": self.table,
                        "physical_name": self.physical_name,
                        "raw_name": self.raw_name,
                        "raw_occurrence": self.occurrence,
                        "candidate_target": self.target,
                        "candidate_kind": self.kind,
                        "review_rule_id": self.review_rule_id,
                        "classification": classification,
                        "raw_token": token,
                        "count": count,
                    }
                )
        return rows

    def has_review_blocking_truncation(self) -> bool:
        nonblocking = (
            set(self.candidate.nonblocking_truncation_classes)
            if self.candidate is not None
            else set()
        )
        return any(
            classification not in nonblocking
            and (
                classification in EXHAUSTIVE_REVIEW_CLASSES
                or classification.startswith("unknown_")
            )
            for classification in self.examples.truncated_classes
        )


def _parse_candidate_number(value: str) -> float:
    return float(value.replace(",", "."))


def _numeric_list_classification(
    token: str,
    approved_missing_tokens: tuple[str, ...],
) -> _TokenClassification:
    parsed = parse_numeric_list(token, approved_missing_tokens)
    if not parsed.recognized_list:
        return _TokenClassification("unresolved_token")
    numeric_values = tuple(
        value for value in parsed.values if value is not None
    )
    if parsed.unresolved_element_count:
        classification = "unresolved_token"
    elif parsed.approved_missing_element_count:
        classification = "custom_parsed_numeric_list_with_missing_elements"
    else:
        classification = "custom_parsed_numeric_list"
    return _TokenClassification(
        classification,
        numeric_values=numeric_values,
        is_numeric_list=True,
        list_element_count=parsed.element_count,
        list_approved_missing_element_count=(
            parsed.approved_missing_element_count
        ),
        list_unresolved_element_count=parsed.unresolved_element_count,
    )


def _scalar_shape_classification(
    token: str,
    textual_missing_candidates: frozenset[str],
) -> _TokenClassification:
    stripped = token.strip()
    if stripped == "":
        return _TokenClassification("whitespace_only_candidate")
    if stripped.casefold() in textual_missing_candidates:
        return _TokenClassification("textual_missing_candidate")
    percentage = PERCENTAGE.fullmatch(stripped)
    if percentage:
        try:
            return _TokenClassification(
                "percentage_syntax_candidate",
                (_parse_candidate_number(percentage.group(1)),),
            )
        except ValueError:
            return _TokenClassification("unresolved_token")
    threshold = THRESHOLD.fullmatch(stripped)
    if threshold:
        try:
            return _TokenClassification(
                "threshold_syntax_candidate",
                (_parse_candidate_number(threshold.group(1)),),
            )
        except ValueError:
            return _TokenClassification("unresolved_token")
    ratio = RATIO.fullmatch(stripped)
    if ratio:
        try:
            numerator = _parse_candidate_number(ratio.group(1))
            denominator = _parse_candidate_number(ratio.group(2))
        except ValueError:
            return _TokenClassification("unresolved_token")
        if denominator == 0:
            return _TokenClassification("unresolved_ratio_zero_denominator")
        return _TokenClassification(
            "ratio_syntax_candidate", (numerator / denominator,)
        )
    if DIRECT_NUMERIC.fullmatch(stripped):
        try:
            return _TokenClassification("direct_numeric", (float(stripped),))
        except ValueError:
            return _TokenClassification("unresolved_token")
    if "," in stripped and "." not in stripped and DECIMAL_COMMA.fullmatch(stripped):
        try:
            return _TokenClassification(
                "decimal_comma_syntax_candidate",
                (_parse_candidate_number(stripped),),
            )
        except ValueError:
            return _TokenClassification("unresolved_token")
    return _TokenClassification("unresolved_token")


def _shape_classification_detail(
    token: str,
    textual_missing_candidates: frozenset[str],
    approved_parser: str | None = None,
    approved_list_missing_tokens: tuple[str, ...] = (),
) -> _TokenClassification:
    if approved_parser == "numeric_list":
        return _numeric_list_classification(token, approved_list_missing_tokens)
    return _scalar_shape_classification(token, textual_missing_candidates)


def _shape_classification(
    token: str,
    textual_missing_candidates: frozenset[str],
    approved_parser: str | None = None,
    approved_list_missing_tokens: tuple[str, ...] = (),
) -> tuple[str, float | None]:
    classified = _shape_classification_detail(
        token,
        textual_missing_candidates,
        approved_parser,
        approved_list_missing_tokens,
    )
    numeric_value = (
        classified.numeric_values[0]
        if len(classified.numeric_values) == 1
        else None
    )
    return classified.name, numeric_value


def _classify_token_detail(
    token: str,
    candidate: CandidateRule | None,
    textual_missing_candidates: frozenset[str],
) -> _TokenClassification:
    kind = candidate.kind if candidate is not None else "unknown"
    if candidate is not None and candidate.allowed_tokens:
        if token in candidate.allowed_tokens:
            return _TokenClassification("approved_categorical_token")
        return _TokenClassification("unapproved_categorical_token")
    if kind == "identifier":
        return _TokenClassification("identifier_token_redacted")
    if kind == "free_text":
        return _TokenClassification("free_text_token_redacted")
    if kind == "categorical":
        return _TokenClassification("categorical_token")
    if kind == "temporal":
        return _TokenClassification("temporal_token_candidate")
    classified = _shape_classification_detail(
        token,
        textual_missing_candidates,
        candidate.approved_parser if candidate is not None else None,
        (
            candidate.approved_list_missing_tokens
            if candidate is not None
            else ()
        ),
    )
    if kind == "unknown":
        return _TokenClassification(
            f"unknown_{classified.name}",
            classified.numeric_values,
            classified.is_numeric_list,
            classified.list_element_count,
            classified.list_approved_missing_element_count,
            classified.list_unresolved_element_count,
        )
    return classified


def _classify_token(
    token: str,
    candidate: CandidateRule | None,
    textual_missing_candidates: frozenset[str],
) -> tuple[str, float | None]:
    classified = _classify_token_detail(
        token, candidate, textual_missing_candidates
    )
    numeric_value = (
        classified.numeric_values[0]
        if len(classified.numeric_values) == 1
        else None
    )
    return classified.name, numeric_value


def _load_json(path: Path, label: str) -> Any:
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaTokenError(f"{label} cannot be read: {path}") from exc


def _load_audit_evidence(
    config: SchemaTokenConfig,
    policy: SchemaTokenPolicy,
    inventory_run_id: str,
    audit_run_id: str,
) -> _AuditEvidence:
    if not RUN_ID_PATTERN.fullmatch(inventory_run_id) or not RUN_ID_PATTERN.fullmatch(
        audit_run_id
    ):
        raise SchemaTokenError("Invalid inventory or ingestion-audit run ID")
    directory = (
        config.ingestion.inventory.paths.reports
        / "private"
        / "ingestion_audit"
        / audit_run_id
    )
    manifest_path = directory / "ingestion_audit_manifest.json"
    failures_path = directory / "failures.json"
    if not manifest_path.is_file() or not failures_path.is_file():
        raise SchemaTokenError("Private ingestion-audit evidence is incomplete")
    manifest = _load_json(manifest_path, "Private ingestion-audit manifest")
    failures = _load_json(failures_path, "Private ingestion-audit failures")
    if manifest.get("artifact") != "asic_v3_ingestion_audit_private" or manifest.get(
        "artifact_version"
    ) != policy.required_ingestion_audit_artifact_version:
        raise SchemaTokenError("Ingestion-audit artifact identity is invalid")
    if manifest.get("dataset_context") != config.dataset_context:
        raise SchemaTokenError("Ingestion-audit dataset context differs")
    input_value = manifest.get("input")
    if not isinstance(input_value, dict) or input_value.get(
        "inventory_run_id"
    ) != inventory_run_id:
        raise SchemaTokenError("Ingestion audit references a different inventory run")
    if Path(str(input_value.get("ingested_root"))).resolve() != (
        config.ingestion.output_root.resolve()
    ):
        raise SchemaTokenError("Ingestion audit references a different ingested root")
    if failures != []:
        raise SchemaTokenError(
            "Private ingestion audit contains technical failures; schema inventory is blocked"
        )
    checks = manifest.get("checks")
    if not isinstance(checks, list):
        raise SchemaTokenError("Ingestion-audit checks are invalid")
    failed_names = {
        check.get("name")
        for check in checks
        if isinstance(check, dict) and check.get("status") != "pass"
    }
    if any(not isinstance(name, str) for name in failed_names):
        raise SchemaTokenError("Ingestion-audit failed check names are invalid")
    unexpected = sorted(failed_names - set(policy.allowed_input_blockers))
    if unexpected:
        raise SchemaTokenError(
            f"Ingestion audit has unexpected blockers: {unexpected}"
        )
    blockers = tuple(sorted(failed_names))
    return _AuditEvidence(directory, manifest, blockers)


def _field_raw_identity(field: pa.Field) -> tuple[str, int, str]:
    metadata = field.metadata or {}
    try:
        role = metadata[b"asic_v3_role"].decode()
        raw_name = metadata[b"raw_name"].decode("utf-8")
        occurrence = int(metadata[b"raw_occurrence"].decode())
    except (KeyError, UnicodeError, ValueError) as exc:
        raise SchemaTokenError("Raw Arrow field metadata is incomplete") from exc
    if role != "raw_clinical_string" or occurrence <= 0:
        raise SchemaTokenError("Raw Arrow field metadata is invalid")
    return raw_name, occurrence, metadata.get(b"physical_name_rule_id", b"").decode()


def _header_identities(header: list[Any]) -> set[tuple[str, int]]:
    occurrences: Counter[str] = Counter()
    identities: set[tuple[str, int]] = set()
    for value in header:
        if not isinstance(value, str):
            raise SchemaTokenError("Ingestion manifest header evidence is invalid")
        occurrences[value] += 1
        identities.add((value, occurrences[value]))
    return identities


def _source_availability(
    files: list[dict[str, Any]], raw_name: str, occurrence: int
) -> tuple[int, int, int]:
    source_files = 0
    source_rows = 0
    variants: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            raise SchemaTokenError("Ingestion manifest file evidence is invalid")
        header = item.get("header")
        if not isinstance(header, list):
            raise SchemaTokenError("Ingestion manifest header evidence is absent")
        if (raw_name, occurrence) in _header_identities(header):
            source_files += 1
            row_count = item.get("source_row_count")
            schema_variant = item.get("schema_variant_id")
            if not isinstance(row_count, int) or not isinstance(schema_variant, str):
                raise SchemaTokenError("Ingestion manifest source metrics are invalid")
            source_rows += row_count
            variants.add(schema_variant)
    return source_files, len(variants), source_rows


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


def _check_results(
    failures: _FailureCollector,
    policy: SchemaTokenPolicy,
    carried_blockers: tuple[str, ...],
    unmapped_count: int,
    unresolved_count: int,
    truncated_count: int,
    reviewed_drop_nonempty_count: int,
    reviewed_domain_violation_count: int,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    computed_failures = {
        "all_raw_columns_in_candidate_registry": unmapped_count,
        "numeric_candidate_unresolved_tokens": unresolved_count,
        "token_examples_complete_for_review": truncated_count,
        "reviewed_all_empty_precondition": reviewed_drop_nonempty_count,
        "reviewed_rule_token_domains": reviewed_domain_violation_count,
        "candidate_harmonization_registry_approved": 1,
        "provisional_archive_owner_confirmation": int(
            "provisional_archive_owner_confirmation" in carried_blockers
        ),
    }
    for name in CHECK_ORDER:
        failure_count = failures.counts[name] + computed_failures.get(name, 0)
        results.append(
            CheckResult(
                name=name,
                status="pass" if failure_count == 0 else "fail",
                severity="blocking",
                observed={
                    "failure_count": failure_count,
                    "affected_hospital_count": len(failures.hospitals[name]),
                },
                expected={"failure_count": 0},
                details=CHECK_DETAILS[name],
            )
        )
    return results


def build_schema_token_inventory(
    config: SchemaTokenConfig,
    inventory_run_id: str,
    ingestion_audit_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> SchemaTokenInventoryResult:
    reporter = progress or (lambda _: None)
    policy = load_schema_token_policy(config.policy_path)
    ingestion_policy = load_ingestion_policy(config.ingestion.policy_path)
    inventory_policy = load_inventory_policy(config.ingestion.inventory.policy_path)
    evidence = _load_audit_evidence(
        config, policy, inventory_run_id, ingestion_audit_run_id
    )
    failures = _FailureCollector()
    mappings = tuple(
        mapping
        for mapping in inventory_policy.hospital_mappings
        if mapping.cohort_action == "include"
    )
    audit_hospitals = evidence.manifest.get("hospital_evidence")
    if not isinstance(audit_hospitals, dict) or set(audit_hospitals) != {
        mapping.canonical_hospital_id for mapping in mappings
    }:
        failures.add(
            "complete_input_hospital_set",
            "Ingestion-audit hospital evidence differs from the approved mapping.",
        )

    private_columns: list[dict[str, Any]] = []
    private_tokens: list[dict[str, Any]] = []
    hospital_summaries: dict[str, Any] = {}
    aggregate_classes: Counter[str] = Counter()
    unmapped_count = 0
    unresolved_count = 0
    truncated_count = 0
    reviewed_drop_nonempty_count = 0
    sentinel_candidate_count = 0
    reviewed_domain_violation_count = 0
    numeric_list_counts: Counter[str] = Counter()
    scanned_table_count = 0
    total_rows = {"static": 0, "dynamic": 0}

    for mapping in mappings:
        hospital = mapping.canonical_hospital_id
        hospital_directory = config.ingestion.output_root / hospital
        audit_hospital = (
            audit_hospitals.get(hospital, {})
            if isinstance(audit_hospitals, dict)
            else {}
        )
        summary_tables: dict[str, Any] = {}
        reporter(f"schema_token_inventory_start hospital={hospital}")
        manifest_path = hospital_directory / "ingestion_manifest.json"
        manifest = _load_json(manifest_path, "Ingestion manifest")
        if not isinstance(manifest, dict) or manifest.get(
            "artifact_version"
        ) != policy.required_ingestion_manifest_version:
            failures.add(
                "raw_schema_matches_ingestion_manifests",
                "Ingestion manifest version is invalid.",
                hospital,
            )
            continue
        manifest_input = manifest.get("input")
        manifest_policies = manifest.get("policies")
        if (
            not isinstance(manifest_input, dict)
            or manifest_input.get("inventory_run_id") != inventory_run_id
            or manifest_input.get("inventory_artifact_version")
            != policy.required_inventory_artifact_version
            or not isinstance(manifest_policies, dict)
            or manifest_policies.get("ingestion_policy_version")
            != ingestion_policy.policy_version
        ):
            failures.add(
                "raw_schema_matches_ingestion_manifests",
                "Hospital manifest input or policy linkage differs from the approved contract.",
                hospital,
            )
            continue
        manifest_tables = manifest.get("tables")
        raw_columns_manifest = manifest.get("raw_columns")
        outputs = manifest.get("outputs")
        if not all(
            isinstance(value, dict)
            for value in (manifest_tables, raw_columns_manifest, outputs)
        ):
            failures.add(
                "raw_schema_matches_ingestion_manifests",
                "Ingestion manifest lacks table, column, or output evidence.",
                hospital,
            )
            continue

        for table_name in ("static", "dynamic"):
            parquet_path = hospital_directory / f"{table_name}.parquet"
            reporter(f"schema_token_hash_start hospital={hospital} table={table_name}")
            try:
                observed_hash = sha256_file(parquet_path)
                parquet = pq.ParquetFile(parquet_path)
            except (OSError, pa.ArrowException) as exc:
                failures.add(
                    "input_parquet_hashes_match",
                    f"Input Parquet cannot be hashed or opened: {type(exc).__name__}: {exc}",
                    hospital,
                    table_name,
                )
                continue
            output_evidence = outputs.get(table_name, {})
            audit_table = (
                audit_hospital.get("tables", {}).get(table_name, {})
                if isinstance(audit_hospital, dict)
                else {}
            )
            if observed_hash not in {
                output_evidence.get("sha256"),
                audit_table.get("sha256"),
            } or output_evidence.get("sha256") != audit_table.get("sha256"):
                failures.add(
                    "input_parquet_hashes_match",
                    "Input Parquet hash differs from ingestion or audit evidence.",
                    hospital,
                    table_name,
                )
                continue
            table_manifest = manifest_tables.get(table_name)
            raw_manifest = raw_columns_manifest.get(table_name)
            if not isinstance(table_manifest, dict) or not isinstance(raw_manifest, list):
                failures.add(
                    "raw_schema_matches_ingestion_manifests",
                    "Table manifest or raw-column evidence is invalid.",
                    hospital,
                    table_name,
                )
                continue
            expected_rows = table_manifest.get("output_row_count")
            files = table_manifest.get("files")
            if not isinstance(expected_rows, int) or not isinstance(files, list):
                failures.add(
                    "raw_schema_matches_ingestion_manifests",
                    "Table row or file evidence is invalid.",
                    hospital,
                    table_name,
                )
                continue
            total_rows[table_name] += expected_rows
            provenance_names = set(ingestion_policy.provenance_columns.values())
            raw_fields = [
                field for field in parquet.schema_arrow if field.name not in provenance_names
            ]
            if len(raw_fields) != len(raw_manifest):
                failures.add(
                    "raw_schema_matches_ingestion_manifests",
                    "Parquet raw field count differs from the ingestion manifest.",
                    hospital,
                    table_name,
                )
                continue
            stats: list[_ColumnStats] = []
            manifest_by_physical = {
                item.get("physical_name"): item
                for item in raw_manifest
                if isinstance(item, dict)
            }
            for field_value in raw_fields:
                raw_name, occurrence, rule_id = _field_raw_identity(field_value)
                manifest_item = manifest_by_physical.get(field_value.name)
                if not isinstance(manifest_item, dict) or (
                    manifest_item.get("raw_name") != raw_name
                    or manifest_item.get("occurrence") != occurrence
                    or (manifest_item.get("physical_name_rule_id") or "identity")
                    != (rule_id or "identity")
                ):
                    failures.add(
                        "raw_schema_matches_ingestion_manifests",
                        "Raw field positional metadata differs from the manifest.",
                        hospital,
                        table_name,
                    )
                candidate = policy.candidate_for(
                    table_name,
                    raw_name,
                    occurrence,
                    hospital=hospital,
                )
                file_count, variant_count, present_rows = _source_availability(
                    files, raw_name, occurrence
                )
                stats.append(
                    _ColumnStats(
                        hospital=hospital,
                        table=table_name,
                        physical_name=field_value.name,
                        raw_name=raw_name,
                        occurrence=occurrence,
                        candidate=candidate,
                        source_file_count=file_count,
                        source_schema_variant_count=variant_count,
                        source_schema_present_row_count=present_rows,
                        expected_rows=expected_rows,
                        maximum_examples=policy.max_examples_per_token_class,
                        store_direct_numeric_examples=policy.store_direct_numeric_examples,
                        store_identifier_examples=policy.store_identifier_examples,
                        store_free_text_examples=policy.store_free_text_examples,
                    )
                )
            physical_names = [item.physical_name for item in stats]
            batch_count = 0
            scanned_batch_rows = 0
            for batch in parquet.iter_batches(
                batch_size=policy.rows_per_batch,
                columns=physical_names,
                use_threads=True,
            ):
                batch_count += 1
                scanned_batch_rows += batch.num_rows
                for index, column_stats in enumerate(stats):
                    column_stats.add_batch(batch.column(index), policy)
                if batch_count % 100 == 0:
                    reporter(
                        f"schema_token_progress hospital={hospital} table={table_name} "
                        f"batches={batch_count} rows={scanned_batch_rows}"
                    )
            table_classes: Counter[str] = Counter()
            for column_stats in stats:
                if column_stats.scanned_rows != expected_rows or (
                    column_stats.null_count
                    + column_stats.literal_empty_count
                    + column_stats.raw_nonempty_count
                    != expected_rows
                ) or sum(column_stats.classification_counts.values()) != (
                    column_stats.raw_nonempty_count
                ):
                    failures.add(
                        "token_accounting_conservation",
                        "Column cell or non-empty token accounting failed.",
                        hospital,
                        table_name,
                    )
                if column_stats.numeric_list_element_count != (
                    column_stats.numeric_list_numeric_element_count
                    + column_stats.numeric_list_approved_missing_element_count
                    + column_stats.numeric_list_unresolved_element_count
                ):
                    failures.add(
                        "token_accounting_conservation",
                        "Numeric-list element accounting failed.",
                        hospital,
                        table_name,
                    )
                expected_nulls = expected_rows - column_stats.source_schema_present_row_count
                if column_stats.null_count != expected_nulls:
                    failures.add(
                        "source_schema_null_conservation",
                        "Parquet null count differs from source-schema absence evidence.",
                        hospital,
                        table_name,
                    )
                if column_stats.candidate is None:
                    unmapped_count += 1
                if (
                    config.dataset_context == "production"
                    and column_stats.candidate is not None
                    and column_stats.candidate.review_rule_id is not None
                    and column_stats.candidate.evidence_nonempty_count
                    != column_stats.raw_nonempty_count
                ):
                    failures.add(
                        "reviewed_rule_evidence_counts",
                        "Reviewed production non-empty count differs from its approved evidence.",
                        hospital,
                        table_name,
                    )
                unresolved_count += sum(
                    count
                    for name, count in column_stats.classification_counts.items()
                    if column_stats.kind == "numeric"
                    and name in {
                        "unresolved_token",
                        "unresolved_ratio_zero_denominator",
                        "whitespace_only_candidate",
                    }
                )
                if column_stats.has_review_blocking_truncation():
                    truncated_count += 1
                sentinel_candidate_count += sum(
                    column_stats.sentinel_candidate_counts.values()
                )
                reviewed_domain_violation_count += (
                    column_stats.reviewed_domain_violation_count
                )
                numeric_list_counts.update(
                    {
                        "cell_count": column_stats.numeric_list_cell_count,
                        "element_count": column_stats.numeric_list_element_count,
                        "numeric_element_count": (
                            column_stats.numeric_list_numeric_element_count
                        ),
                        "approved_missing_element_count": (
                            column_stats.numeric_list_approved_missing_element_count
                        ),
                        "unresolved_element_count": (
                            column_stats.numeric_list_unresolved_element_count
                        ),
                    }
                )
                if (
                    column_stats.candidate_status
                    == "reviewed_all_empty_drop_candidate_requires_inventory_revalidation"
                ):
                    reviewed_drop_nonempty_count += column_stats.raw_nonempty_count
                table_classes.update(column_stats.classification_counts)
                aggregate_classes.update(column_stats.classification_counts)
                private_columns.append(column_stats.to_private_row())
                private_tokens.extend(column_stats.token_rows())
            scanned_table_count += 1
            summary_tables[table_name] = {
                "row_count": expected_rows,
                "raw_column_occurrence_count": len(stats),
                "mapped_candidate_occurrence_count": sum(
                    item.candidate is not None for item in stats
                ),
                "unmapped_occurrence_count": sum(
                    item.candidate is None for item in stats
                ),
                "all_missing_or_empty_occurrence_count": sum(
                    item.raw_nonempty_count == 0 for item in stats
                ),
                "candidate_kind_counts": dict(
                    sorted(Counter(item.kind for item in stats).items())
                ),
                "token_classification_counts": dict(sorted(table_classes.items())),
            }
            reporter(f"schema_token_complete hospital={hospital} table={table_name}")
        hospital_summaries[hospital] = summary_tables

    checks = _check_results(
        failures,
        policy,
        evidence.blockers,
        unmapped_count,
        unresolved_count,
        truncated_count,
        reviewed_drop_nonempty_count,
        reviewed_domain_violation_count,
    )
    status = overall_status(checks)
    blocking_findings = tuple(
        {"check": check.name, "details": check.details}
        for check in checks
        if check.status != "pass" and check.severity == "blocking"
    )
    technical_findings = tuple(
        finding
        for finding in blocking_findings
        if finding["check"] in TECHNICAL_CHECKS
    )
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_schema_token_inventory_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "inventory_run_id": inventory_run_id,
            "ingestion_audit_run_id": ingestion_audit_run_id,
            "authoritative_boundary": "validated_lossless_ingested_parquet",
        },
        "metrics": {
            "approved_hospital_count": len(mappings),
            "scanned_table_count": scanned_table_count,
            "static_row_count": total_rows["static"],
            "dynamic_row_count": total_rows["dynamic"],
            "raw_column_occurrence_count": len(private_columns),
            "unmapped_raw_column_occurrence_count": unmapped_count,
            "numeric_unresolved_nonempty_token_count": unresolved_count,
            "columns_with_truncated_review_examples": truncated_count,
            "reviewed_drop_candidate_nonempty_count": reviewed_drop_nonempty_count,
            "numeric_missing_sentinel_candidate_count": sentinel_candidate_count,
            "reviewed_rule_count": policy.reviewed_rule_count,
            "reviewed_rule_token_domain_violation_count": reviewed_domain_violation_count,
            "numeric_list_cell_count": numeric_list_counts["cell_count"],
            "numeric_list_element_count": numeric_list_counts["element_count"],
            "numeric_list_numeric_element_count": numeric_list_counts[
                "numeric_element_count"
            ],
            "numeric_list_approved_missing_element_count": numeric_list_counts[
                "approved_missing_element_count"
            ],
            "numeric_list_unresolved_element_count": numeric_list_counts[
                "unresolved_element_count"
            ],
            "token_classification_counts": dict(sorted(aggregate_classes.items())),
        },
        "hospital_summaries": hospital_summaries,
        "checks": [asdict(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
            "protected_evidence_location": "owner-only private schema/token inventory bundle on the authorized cluster",
        },
        "publication": {
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "harmonization_transformations_applied": False,
        },
        "limitations": [
            "Seed mappings originate from frozen v2 and legacy references and remain unapproved unless covered by the partial reviewed raw-v3 registry.",
            "Approved parser and sentinel rules classify read-only evidence here; they do not mutate or normalize ingested values.",
            "Unreviewed ratio, threshold, percentage, textual-missing, unit, and semantic policies remain pending.",
            "Exact raw headers and token examples remain only in the owner-only private bundle.",
            "Numeric summaries describe candidate interpretations for review and do not change ingested values.",
            "No harmonized, cleaned, derived, concatenated, or pooled production artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_schema_token_inventory_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "inventory_run_id": inventory_run_id,
            "ingestion_audit_run_id": ingestion_audit_run_id,
            "ingestion_audit_directory": str(evidence.directory),
            "ingested_root": str(config.ingestion.output_root),
        },
        "policies": {
            "schema_token_policy_version": policy.policy_version,
            "schema_token_policy_path": str(policy.source_path),
            "candidate_registry_path": str(policy.registry_path),
            "reviewed_registry_path": str(policy.reviewed_registry_path),
            "reviewed_rule_count": policy.reviewed_rule_count,
            "reviewed_by": policy.reviewed_by,
            "reviewed_at": policy.reviewed_at,
            "reviewed_evidence_run_id": policy.reviewed_evidence_run_id,
        },
        "checks": [asdict(check) for check in checks],
        "blocking_findings": list(blocking_findings),
        "technical_failure_details": failures.details,
        "column_evidence_row_count": len(private_columns),
        "token_evidence_row_count": len(private_tokens),
        "publication_ready": False,
    }
    return SchemaTokenInventoryResult(
        dataset_context=config.dataset_context,
        overall_status=status,
        blocking_findings=blocking_findings,
        technical_blocking_findings=technical_findings,
        private_manifest=private_manifest,
        private_columns=tuple(private_columns),
        private_tokens=tuple(private_tokens),
        review_payload=review_payload,
    )


def _review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 raw-schema and parsing-token inventory review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input ingestion audit: `{payload['inputs']['ingestion_audit_run_id']}`",
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
        lines.append("- None.")
    lines.extend(["", "## Inventory totals", ""])
    for key, value in payload["metrics"].items():
        if isinstance(value, (str, int)):
            lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Hospital summaries", ""])
    for hospital, tables in payload["hospital_summaries"].items():
        for table, values in tables.items():
            lines.append(
                f"- `{hospital}` `{table}`: rows `{values['row_count']}`; "
                f"raw occurrences `{values['raw_column_occurrence_count']}`; "
                f"unmapped `{values['unmapped_occurrence_count']}`"
            )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the owner-only column and token evidence before approving any parser, "
        "missing-sentinel rule, canonical mapping, unit, notation, or harmonized union schema."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_schema_token_inventory_bundle(
    result: SchemaTokenInventoryResult,
    reports_root: Path,
    run_id: str | None = None,
) -> SchemaTokenInventoryResult:
    selected_run_id = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run_id):
        raise SchemaTokenError("Invalid schema-token inventory run ID")
    private_directory = (
        reports_root / "private" / "schema_token_inventory" / selected_run_id
    )
    review_directory = reports_root / "review" / "schema_token_inventory"
    review_json = review_directory / f"{selected_run_id}.json"
    review_markdown = review_directory / f"{selected_run_id}.md"
    if private_directory.exists() or review_json.exists() or review_markdown.exists():
        raise SchemaTokenError(
            "Schema-token inventory run already exists and will not be overwritten"
        )
    private_directory.mkdir(parents=True, mode=0o700)
    private_directory.chmod(0o700)
    review_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_directory.chmod(0o750)
    _write_json(
        private_directory / "schema_token_inventory_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(
        private_directory / "columns.parquet", result.private_columns
    )
    _write_private_parquet(
        private_directory / "tokens.parquet", result.private_tokens
    )
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
    return SchemaTokenInventoryResult(
        **{
            **asdict(result),
            "private_report_directory": private_directory,
            "review_json_path": review_json,
            "review_markdown_path": review_markdown,
        }
    )
