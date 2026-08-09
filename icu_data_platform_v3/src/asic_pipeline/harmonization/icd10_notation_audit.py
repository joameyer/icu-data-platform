from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
import json
import os
from pathlib import Path
import re
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.review_policy import (
    HarmonizationReviewConfig,
    load_harmonization_review_config,
)
from asic_pipeline.harmonization.static_decisions import (
    load_reviewed_static_decision_registry,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


EXPECTED_HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)
TECHNICAL_CHECKS = frozenset(
    {
        "static_contract_evidence_valid",
        "reviewed_static_registry_valid",
        "icd10_source_occurrences_complete",
        "ingested_static_hashes_match_manifests",
        "icd10_source_bindings_valid",
        "icd10_row_accounting_conserved",
        "static_contract_counts_reproduced",
        "icd10_source_cells_are_strings",
        "no_production_data_artifacts_generated",
    }
)
CHECK_DETAILS = {
    "static_contract_evidence_valid": (
        "The immutable static-contract private and sanitized artifacts must match the approved run and context."
    ),
    "reviewed_static_registry_valid": (
        "Partial static registry 0.3 must use the same evidence and keep every production gate closed."
    ),
    "icd10_source_occurrences_complete": (
        "Exactly one reviewed static ICD-10 source occurrence is required for every approved hospital."
    ),
    "ingested_static_hashes_match_manifests": (
        "Every scanned static Parquet must match its immutable ingestion-manifest hash."
    ),
    "icd10_source_bindings_valid": (
        "Each selected physical field must match its lossless raw-name and occurrence metadata."
    ),
    "icd10_row_accounting_conserved": (
        "Every selected source row must be counted exactly once as source-null, literal-empty, or non-empty."
    ),
    "static_contract_counts_reproduced": (
        "The ICD-10 source-null, literal-empty, non-empty, and row counts must reproduce the immutable static-contract audit."
    ),
    "icd10_source_cells_are_strings": (
        "Every non-null ICD-10 source cell must remain a lossless raw string."
    ),
    "icd10_textual_missing_policy_approved": (
        "Candidate textual-missing spellings require explicit review before conversion to null."
    ),
    "icd10_notation_patterns_reviewed": (
        "Every observed syntax-feature pattern requires review before a canonical code parser is designed."
    ),
    "icd10_source_text_contract_approved": (
        "The exact source-text field name, type, and null policy require explicit human approval."
    ),
    "icd10_canonical_parser_scope_approved": (
        "Parsing a canonical ICD-10 list must remain deferred or receive a separately reviewed complete grammar."
    ),
    "no_production_data_artifacts_generated": (
        "The audit may write reports only and must not transform values or generate harmonized data."
    ),
}


@dataclass(frozen=True)
class ICD10NotationAuditPolicy:
    version: str
    static_contract_audit_basis_run_id: str
    required_static_contract_private_artifact_version: str
    required_static_contract_review_artifact_version: str
    required_ingestion_manifest_version: str
    reviewed_static_decisions_path: Path
    textual_missing_candidate_tokens: tuple[str, ...]
    single_code_candidate_regex: str
    maximum_private_examples_per_hospital_pattern: int
    rows_per_batch: int
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class ICD10NotationAuditConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root

    @property
    def ingested_root(self) -> Path:
        return self.harmonization_review.schema_tokens.ingestion.output_root


@dataclass(frozen=True)
class ICD10NotationAuditResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_patterns: tuple[dict[str, Any], ...]
    private_examples: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass
class _HospitalStats:
    row_count: int = 0
    source_null_count: int = 0
    literal_empty_count: int = 0
    nonempty_count: int = 0
    nonstring_count: int = 0
    textual_missing_candidate_count: int = 0
    single_code_candidate_count: int = 0
    minimum_character_count: int | None = None
    maximum_character_count: int | None = None
    distinct_tokens: set[str] | None = None
    patterns: Counter[str] | None = None
    examples: dict[str, Counter[str]] | None = None

    @classmethod
    def create(cls) -> _HospitalStats:
        return cls(
            distinct_tokens=set(),
            patterns=Counter(),
            examples=defaultdict(Counter),
        )

    def add_length(self, value: str) -> None:
        length = len(value)
        self.minimum_character_count = (
            length
            if self.minimum_character_count is None
            else min(self.minimum_character_count, length)
        )
        self.maximum_character_count = (
            length
            if self.maximum_character_count is None
            else max(self.maximum_character_count, length)
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(mapping: dict[str, Any], key: str, location: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location}.{key} must be a positive integer")
    return value


def _string_list(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must contain non-empty strings")
    result = tuple(value)
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicates")
    return result


def load_icd10_notation_audit_config(
    path: str | Path,
) -> ICD10NotationAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "ICD-10 notation-audit configuration")
    policy_path = resolve_path(
        required_string(raw, "icd10_notation_audit_policy", "config"),
        source,
    )
    return ICD10NotationAuditConfig(harmonization, policy_path)


def load_icd10_notation_audit_policy(
    path: str | Path,
) -> ICD10NotationAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "ICD-10 notation-audit policy")
    if raw.get("icd10_notation_audit_policy_version") != "0.1":
        raise ConfigurationError("ICD-10 notation-audit policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_icd10_notation_evidence":
        raise ConfigurationError("ICD-10 notation-audit policy is not approved")
    basis = required_string(
        raw, "static_contract_audit_basis_run_id", "ICD-10 audit"
    )
    if basis != "20260805T112746Z":
        raise ConfigurationError("ICD-10 evidence basis changed unexpectedly")
    classification = _mapping(raw.get("classification"), "ICD-10 classification")
    missing_tokens = _string_list(
        classification.get("textual_missing_candidate_tokens"),
        "ICD-10 classification.textual_missing_candidate_tokens",
    )
    if missing_tokens != ("nan", "na", "n/a", "null", "none"):
        raise ConfigurationError("ICD-10 textual-missing candidates changed")
    code_regex = required_string(
        classification,
        "single_code_candidate_regex",
        "ICD-10 classification",
    )
    try:
        re.compile(code_regex)
    except re.error as exc:
        raise ConfigurationError("ICD-10 code-candidate regex is invalid") from exc
    scope = _mapping(raw.get("scope"), "ICD-10 audit.scope")
    expected_scope = {
        "allow_static_contract_private_evidence_reads": True,
        "allow_ingested_static_icd10_reads": True,
        "allow_report_writes": True,
        "allow_raw_csv_reads": False,
        "allow_other_ingested_column_reads": False,
        "allow_value_transforms": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_registry_approval": False,
        "allow_union_schema_approval": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("ICD-10 notation audit must remain read-only")
    scan = _mapping(raw.get("scan"), "ICD-10 audit.scan")
    reporting = _mapping(raw.get("reporting"), "ICD-10 audit.reporting")
    if reporting.get("private_directory_mode") != "0700" or reporting.get(
        "private_file_mode"
    ) != "0600":
        raise ConfigurationError("ICD-10 private report modes changed")
    return ICD10NotationAuditPolicy(
        version="0.1",
        static_contract_audit_basis_run_id=basis,
        required_static_contract_private_artifact_version=required_string(
            raw,
            "required_static_contract_private_artifact_version",
            "ICD-10 audit",
        ),
        required_static_contract_review_artifact_version=required_string(
            raw,
            "required_static_contract_review_artifact_version",
            "ICD-10 audit",
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "ICD-10 audit"
        ),
        reviewed_static_decisions_path=resolve_path(
            required_string(raw, "reviewed_static_decisions", "ICD-10 audit"),
            source,
        ),
        textual_missing_candidate_tokens=missing_tokens,
        single_code_candidate_regex=code_regex,
        maximum_private_examples_per_hospital_pattern=_positive_int(
            classification,
            "maximum_private_examples_per_hospital_pattern",
            "ICD-10 classification",
        ),
        rows_per_batch=_positive_int(scan, "rows_per_batch", "ICD-10 audit.scan"),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "ICD-10 audit.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "ICD-10 audit.reporting"
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


def _syntax_signature(value: str, single_code_pattern: re.Pattern[str]) -> str:
    stripped = value.strip()
    features: list[str] = []
    if value != stripped:
        features.append("surrounding_whitespace")
    if "\n" in value or "\r" in value:
        features.append("line_break")
    if "\t" in value:
        features.append("tab")
    for character, label in (
        (";", "semicolon"),
        (",", "comma"),
        ("|", "pipe"),
        ("/", "slash"),
        ("\\", "backslash"),
        (":", "colon"),
        ("-", "hyphen"),
    ):
        if character in value:
            features.append(label)
    if "[" in value or "]" in value:
        features.append("square_bracket")
    if "(" in value or ")" in value:
        features.append("round_bracket")
    if "{" in value or "}" in value:
        features.append("curly_bracket")
    if "'" in value or '"' in value:
        features.append("quote")
    if any(character.isspace() for character in stripped):
        features.append("whitespace")
    if single_code_pattern.fullmatch(stripped):
        features.append("single_code_candidate")
    if not features:
        features.append("plain_other")
    return "+".join(features)


def _consume(
    stats: _HospitalStats,
    raw: object,
    missing_candidates: frozenset[str],
    single_code_pattern: re.Pattern[str],
) -> None:
    stats.row_count += 1
    if raw is None:
        stats.source_null_count += 1
        return
    if not isinstance(raw, str):
        stats.nonempty_count += 1
        stats.nonstring_count += 1
        assert stats.patterns is not None
        stats.patterns["nonstring_source_cell"] += 1
        return
    if raw == "":
        stats.literal_empty_count += 1
        return
    stats.nonempty_count += 1
    stats.add_length(raw)
    assert stats.distinct_tokens is not None
    assert stats.patterns is not None
    assert stats.examples is not None
    stats.distinct_tokens.add(raw)
    stripped = raw.strip()
    if stripped.casefold() in missing_candidates:
        signature = "textual_missing_candidate"
        stats.textual_missing_candidate_count += 1
    else:
        signature = _syntax_signature(raw, single_code_pattern)
        if "single_code_candidate" in signature.split("+"):
            stats.single_code_candidate_count += 1
    stats.patterns[signature] += 1
    stats.examples[signature][raw] += 1


def build_icd10_notation_audit(
    config: ICD10NotationAuditConfig,
    static_contract_audit_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> ICD10NotationAuditResult:
    policy = load_icd10_notation_audit_policy(config.policy_path)
    if static_contract_audit_run_id != policy.static_contract_audit_basis_run_id:
        raise HarmonizationError("Unexpected static-contract audit basis run")
    if not RUN_ID_PATTERN.fullmatch(static_contract_audit_run_id):
        raise HarmonizationError("Invalid static-contract audit run ID")
    static_private = (
        config.reports_root
        / "private"
        / "static_contract_audit"
        / static_contract_audit_run_id
    )
    manifest_path = static_private / "static_contract_audit_manifest.json"
    occurrences_path = static_private / "occurrences.parquet"
    source_review_path = (
        config.reports_root
        / "review"
        / "static_contract_audit"
        / f"{static_contract_audit_run_id}.json"
    )
    required = (
        manifest_path,
        occurrences_path,
        source_review_path,
        policy.reviewed_static_decisions_path,
    )
    if any(not path.is_file() for path in required):
        raise HarmonizationError("ICD-10 notation-audit input evidence is incomplete")
    manifest = _read_json(manifest_path, "Static-contract private manifest")
    source_review = _read_json(source_review_path, "Static-contract review")
    try:
        occurrence_rows = pq.read_table(occurrences_path).to_pylist()
    except Exception as exc:
        raise HarmonizationError("Static-contract occurrences cannot be read") from exc
    evidence_valid = bool(
        manifest.get("artifact") == "asic_v3_static_contract_audit_private"
        and manifest.get("artifact_version")
        == policy.required_static_contract_private_artifact_version
        and manifest.get("dataset_context") == config.dataset_context
        and manifest.get("selected_occurrence_count") == len(occurrence_rows)
        and source_review.get("artifact") == "asic_v3_static_contract_audit_review"
        and source_review.get("artifact_version")
        == policy.required_static_contract_review_artifact_version
        and source_review.get("dataset_context") == config.dataset_context
        and source_review.get("metrics", {}).get(
            "technical_blocking_finding_count"
        )
        == 0
    )
    registry = load_reviewed_static_decision_registry(
        policy.reviewed_static_decisions_path
    )
    registry_valid = bool(
        registry.version == "0.3"
        and registry.static_contract_audit_run_id == static_contract_audit_run_id
        and not registry.allow_production_reads
        and not registry.allow_artifact_writes
        and not registry.allow_hospital_concatenation
        and not registry.allow_union_schema_freeze
    )
    selected = [
        row
        for row in occurrence_rows
        if row.get("table") == "static"
        and row.get("candidate_target") == "icd10_codes"
    ]
    by_hospital = {row.get("hospital"): row for row in selected}
    occurrences_complete = bool(
        len(selected) == len(EXPECTED_HOSPITALS)
        and tuple(sorted(by_hospital)) == tuple(sorted(EXPECTED_HOSPITALS))
    )
    if not occurrences_complete:
        raise HarmonizationError("ICD-10 source occurrence evidence is incomplete")

    missing_candidates = frozenset(
        token.casefold() for token in policy.textual_missing_candidate_tokens
    )
    single_code_pattern = re.compile(policy.single_code_candidate_regex)
    stats_by_hospital: dict[str, _HospitalStats] = {}
    input_hashes: dict[str, dict[str, str]] = {}
    hashes_match = True
    bindings_valid = True
    for hospital in EXPECTED_HOSPITALS:
        row = by_hospital[hospital]
        physical_name = row.get("physical_name")
        raw_name = row.get("raw_name")
        raw_occurrence = row.get("raw_occurrence")
        if (
            not isinstance(physical_name, str)
            or not isinstance(raw_name, str)
            or not isinstance(raw_occurrence, int)
        ):
            raise HarmonizationError("ICD-10 source occurrence identity is invalid")
        hospital_dir = config.ingested_root / hospital
        static_path = hospital_dir / "static.parquet"
        ingestion_manifest_path = hospital_dir / "ingestion_manifest.json"
        if not static_path.is_file() or not ingestion_manifest_path.is_file():
            raise HarmonizationError(f"ICD-10 ingested input is unavailable for {hospital}")
        ingestion_manifest = _read_json(
            ingestion_manifest_path, f"Ingestion manifest for {hospital}"
        )
        expected_hash = ingestion_manifest.get("outputs", {}).get("static", {}).get(
            "sha256"
        )
        observed_hash = sha256_file(static_path)
        hashes_match &= bool(
            ingestion_manifest.get("artifact")
            == "asic_v3_lossless_hospital_ingestion"
            and ingestion_manifest.get("artifact_version")
            == policy.required_ingestion_manifest_version
            and ingestion_manifest.get("hospital", {}).get(
                "canonical_hospital_id"
            )
            == hospital
            and isinstance(expected_hash, str)
            and expected_hash == observed_hash
        )
        input_hashes[hospital] = {
            "ingestion_manifest_sha256": sha256_file(ingestion_manifest_path),
            "static_parquet_sha256": observed_hash,
        }
        parquet = pq.ParquetFile(static_path)
        field_index = parquet.schema_arrow.get_field_index(physical_name)
        if field_index < 0:
            raise HarmonizationError(f"ICD-10 source field is unavailable for {hospital}")
        field = parquet.schema_arrow.field(field_index)
        metadata = field.metadata or {}
        binding = bool(
            (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
            and metadata.get(b"raw_name", b"").decode("utf-8") == raw_name
            and int(metadata.get(b"raw_occurrence", b"0")) == raw_occurrence
        )
        bindings_valid &= binding
        stats = _HospitalStats.create()
        for batch in parquet.iter_batches(
            batch_size=policy.rows_per_batch,
            columns=[physical_name],
            use_threads=False,
        ):
            for raw in batch.column(0).to_pylist():
                _consume(stats, raw, missing_candidates, single_code_pattern)
        stats_by_hospital[hospital] = stats
        if progress is not None:
            progress(f"icd10_notation_hospital_scanned={hospital} rows={stats.row_count}")

    accounting_valid = True
    prior_counts_reproduced = True
    nonstring_count = 0
    private_patterns: list[dict[str, Any]] = []
    private_examples: list[dict[str, Any]] = []
    sanitized_hospitals: list[dict[str, Any]] = []
    pattern_count_total = 0
    truncated_pattern_count = 0
    for hospital in EXPECTED_HOSPITALS:
        stats = stats_by_hospital[hospital]
        row = by_hospital[hospital]
        accounting_valid &= stats.row_count == (
            stats.source_null_count + stats.literal_empty_count + stats.nonempty_count
        )
        prior_counts_reproduced &= bool(
            stats.row_count == row.get("scan_row_count")
            and stats.source_null_count == row.get("scan_source_null_count")
            and stats.literal_empty_count == row.get("scan_literal_empty_count")
            and stats.nonempty_count == row.get("scan_nonempty_count")
        )
        nonstring_count += stats.nonstring_count
        assert stats.patterns is not None
        assert stats.examples is not None
        assert stats.distinct_tokens is not None
        for signature, count in sorted(stats.patterns.items()):
            examples = stats.examples.get(signature, Counter())
            selected_examples = examples.most_common(
                policy.maximum_private_examples_per_hospital_pattern
            )
            truncated = len(examples) > len(selected_examples)
            truncated_pattern_count += int(truncated)
            pattern_count_total += 1
            pattern_row = {
                "hospital": hospital,
                "syntax_signature": signature,
                "cell_count": count,
                "distinct_token_count": len(examples),
                "private_example_count": len(selected_examples),
                "private_examples_truncated": truncated,
            }
            private_patterns.append(pattern_row)
            for raw_token, token_count in selected_examples:
                private_examples.append(
                    {
                        "hospital": hospital,
                        "syntax_signature": signature,
                        "raw_token": raw_token,
                        "cell_count": token_count,
                    }
                )
        sanitized_hospitals.append(
            {
                "hospital": hospital,
                "row_count": stats.row_count,
                "source_null_count": stats.source_null_count,
                "literal_empty_count": stats.literal_empty_count,
                "nonempty_count": stats.nonempty_count,
                "distinct_nonempty_token_count": len(stats.distinct_tokens),
                "syntax_pattern_count": len(stats.patterns),
                "textual_missing_candidate_count": (
                    stats.textual_missing_candidate_count
                ),
                "single_code_candidate_count": stats.single_code_candidate_count,
                "minimum_character_count": stats.minimum_character_count,
                "maximum_character_count": stats.maximum_character_count,
            }
        )

    failure_counts = {
        "static_contract_evidence_valid": int(not evidence_valid),
        "reviewed_static_registry_valid": int(not registry_valid),
        "icd10_source_occurrences_complete": int(not occurrences_complete),
        "ingested_static_hashes_match_manifests": int(not hashes_match),
        "icd10_source_bindings_valid": int(not bindings_valid),
        "icd10_row_accounting_conserved": int(not accounting_valid),
        "static_contract_counts_reproduced": int(not prior_counts_reproduced),
        "icd10_source_cells_are_strings": nonstring_count,
        "icd10_textual_missing_policy_approved": 1,
        "icd10_notation_patterns_reviewed": pattern_count_total,
        "icd10_source_text_contract_approved": 1,
        "icd10_canonical_parser_scope_approved": 1,
        "no_production_data_artifacts_generated": 0,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed=count,
            expected=0,
            details=CHECK_DETAILS[name],
        )
        for name, count in failure_counts.items()
    ]
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(
        item for item in blockers if item["check"] in TECHNICAL_CHECKS
    )
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_icd10_notation_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": overall_status(checks),
        "inputs": {
            "static_contract_audit_run_id": static_contract_audit_run_id,
            "reviewed_static_decisions_version": registry.version,
        },
        "metrics": {
            "audited_hospital_count": len(EXPECTED_HOSPITALS),
            "scanned_row_count": sum(
                item["row_count"] for item in sanitized_hospitals
            ),
            "nonempty_cell_count": sum(
                item["nonempty_count"] for item in sanitized_hospitals
            ),
            "textual_missing_candidate_count": sum(
                item["textual_missing_candidate_count"]
                for item in sanitized_hospitals
            ),
            "single_code_candidate_count": sum(
                item["single_code_candidate_count"]
                for item in sanitized_hospitals
            ),
            "hospital_pattern_count": pattern_count_total,
            "patterns_with_truncated_private_examples": truncated_pattern_count,
            "nonstring_source_cell_count": nonstring_count,
            "technical_blocking_finding_count": len(technical),
        },
        "hospital_summaries": sanitized_hospitals,
        "syntax_patterns": private_patterns,
        "proposal": {
            "source_preservation_target": "icd10_codes_source_text",
            "source_preservation_value_type": "large_string",
            "source_nonempty_policy": "preserve_exact",
            "literal_empty_and_source_absence_policy": "null",
            "canonical_icd10_codes_target": "deferred_pending_notation_review",
        },
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
        },
        "publication": {
            "source_data_modified": False,
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "registry_approved": False,
        },
        "limitations": [
            "Syntax signatures describe characters and shapes; they do not validate diagnoses or parse ICD-10 semantics.",
            "Raw token examples remain only in the owner-only private bundle on the authorized cluster.",
            "Bounded example truncation is informational because every cell is still counted in a deterministic syntax signature; it is not parser approval.",
            "The proposed exact source-text field and any future parsed code-list field remain unapproved.",
            "No harmonized, cleaned, derived, concatenated, or pooled data artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_icd10_notation_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": review_payload["overall_status"],
        "inputs": {
            "static_contract_audit_run_id": static_contract_audit_run_id,
            "static_contract_manifest_sha256": sha256_file(manifest_path),
            "static_contract_occurrences_sha256": sha256_file(occurrences_path),
            "static_contract_review_sha256": sha256_file(source_review_path),
            "reviewed_static_decisions_sha256": sha256_file(
                policy.reviewed_static_decisions_path
            ),
            "static_inputs": input_hashes,
        },
        "policy": {"version": policy.version, "path": str(policy.source_path)},
        "checks": [asdict(item) for item in checks],
        "private_pattern_count": len(private_patterns),
        "private_example_count": len(private_examples),
        "production_data_artifacts_generated": False,
    }
    return ICD10NotationAuditResult(
        overall_status=review_payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_manifest=private_manifest,
        private_patterns=tuple(private_patterns),
        private_examples=tuple(private_examples),
        review_payload=review_payload,
    )


def _write_json(path: Path, value: dict[str, Any], mode: int) -> None:
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


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 ICD-10 notation audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input static-contract run: `{payload['inputs']['static_contract_audit_run_id']}`",
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
    lines.extend(["", "## Hospital summaries", ""])
    for item in payload["hospital_summaries"]:
        lines.append(
            f"- `{item['hospital']}`: rows `{item['row_count']}`; non-empty "
            f"`{item['nonempty_count']}`; distinct `{item['distinct_nonempty_token_count']}`; "
            f"patterns `{item['syntax_pattern_count']}`; textual-missing candidates "
            f"`{item['textual_missing_candidate_count']}`; single-code candidates "
            f"`{item['single_code_candidate_count']}`"
        )
    lines.extend(["", "## Syntax patterns", ""])
    for item in payload["syntax_patterns"]:
        lines.append(
            f"- `{item['hospital']}` `{item['syntax_signature']}`: cells "
            f"`{item['cell_count']}`; distinct tokens `{item['distinct_token_count']}`; "
            f"private examples truncated `{str(item['private_examples_truncated']).lower()}`"
        )
    lines.extend(["", "## Proposed representation", ""])
    lines.append(
        "Preserve the exact source cell as nullable `icd10_codes_source_text` (`large_string`); defer canonical `icd10_codes` parsing until the notation patterns are reviewed."
    )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the syntax-pattern counts and owner-only examples before approving missing tokens, source-text preservation, or a canonical parser."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_icd10_notation_audit_bundle(
    result: ICD10NotationAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> ICD10NotationAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid ICD-10 notation-audit run ID")
    private_dir = reports_root / "private" / "icd10_notation_audit" / selected
    review_dir = reports_root / "review" / "icd10_notation_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "ICD-10 notation-audit run already exists and will not be overwritten"
        )
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(
        private_dir / "icd10_notation_audit_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(private_dir / "patterns.parquet", result.private_patterns)
    _write_private_parquet(private_dir / "examples.parquet", result.private_examples)
    readme = private_dir / "README.md"
    descriptor = os.open(readme, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(
            "# Owner-only ICD-10 notation evidence\n\n"
            "Keep this directory on the authorized cluster. `examples.parquet` "
            "contains exact source strings and must not be copied into source "
            "control or sanitized reports. This audit does not parse or transform "
            "clinical values.\n"
        )
    readme.chmod(0o600)
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
    return replace(
        result,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
