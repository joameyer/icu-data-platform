from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from asic_pipeline.config import load_yaml_mapping, required_string
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file


RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$")


@dataclass(frozen=True)
class ReviewedICD10MissingDecision:
    rule_id: str
    hospital: str
    normalization: str
    token: str
    evidence_cell_count: int

    def matches(self, raw_value: str) -> bool:
        if self.normalization != "strip_casefold":
            raise HarmonizationError(
                f"ICD-10 missing rule {self.rule_id} has unsupported normalization"
            )
        return raw_value.strip().casefold() == self.token


@dataclass(frozen=True)
class ReviewedICD10Contract:
    version: str
    approved_by_role: str
    approved_at: str
    static_contract_audit_run_id: str
    notation_audit_run_id: str
    notation_review_generated_at_utc: str
    audited_hospital_count: int
    scanned_row_count: int
    nonempty_cell_count: int
    comma_delimited_cell_count: int
    single_code_cell_count: int
    approved_textual_missing_cell_count: int
    source_text_target: str
    source_text_value_type: str
    missing_decisions: tuple[ReviewedICD10MissingDecision, ...]
    list_target: str
    list_value_type: str
    delimiter: str
    component_normalization: str
    preserve_source_order: bool
    preserve_duplicates: bool
    detail_audit_run_id: str | None
    detail_review_generated_at_utc: str | None
    component_code_candidate_regex: str | None
    trailing_empty_evidence_by_hospital: tuple[tuple[str, int], ...]
    expected_source_component_count: int | None
    expected_valid_component_count_before_deduplication: int | None
    expected_reviewed_incomplete_component_count: int | None
    expected_duplicate_component_count: int | None
    expected_retained_component_count: int | None
    expected_trailing_empty_cell_count: int | None
    maximum_trailing_empty_components_per_cell: int | None
    incomplete_component_hospital: str | None
    incomplete_component_private_detail_class: str | None
    allow_private_review_evidence_reads: bool
    allow_synthetic_batch_transform: bool
    allow_component_audit_reads: bool
    allow_report_writes: bool
    allow_value_transforms: bool
    allow_harmonized_artifact_writes: bool
    allow_hospital_concatenation: bool
    allow_union_schema_freeze: bool
    allow_parser_activation: bool
    source_path: Path

    def missing_decision_for(
        self, hospital: str
    ) -> ReviewedICD10MissingDecision | None:
        return next(
            (item for item in self.missing_decisions if item.hospital == hospital),
            None,
        )

    def require_parser_active(self) -> None:
        if not self.allow_parser_activation:
            raise HarmonizationError(
                "ICD-10 canonical parser remains blocked pending component-audit review"
            )

    def approved_trailing_empty_count(self, hospital: str) -> int:
        return dict(self.trailing_empty_evidence_by_hospital).get(hospital, 0)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _load_reviewed_icd10_contract_v01(path: str | Path) -> ReviewedICD10Contract:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed ICD-10 contract")
    if raw.get("reviewed_icd10_contract_version") != "0.1":
        raise ConfigurationError("Reviewed ICD-10 contract version must be 0.1")
    if raw.get("status") != "approved_for_component_audit_not_parser_activation":
        raise ConfigurationError("Reviewed ICD-10 contract status is invalid")

    review = _mapping(raw.get("review"), "ICD-10 contract.review")
    expected_review = {
        "approved_by_role": "project_data_owner",
        "approved_at": "2026-08-05",
        "static_contract_audit_run_id": "20260805T112746Z",
        "icd10_notation_audit_run_id": "20260805T131735Z",
        "icd10_notation_review_generated_at_utc": "2026-08-05T13:17:35+00:00",
    }
    if {key: review.get(key) for key in expected_review} != expected_review:
        raise ConfigurationError("Reviewed ICD-10 evidence basis changed")
    if not all(
        RUN_ID.fullmatch(str(review[key]))
        for key in ("static_contract_audit_run_id", "icd10_notation_audit_run_id")
    ):
        raise ConfigurationError("Reviewed ICD-10 run ID is invalid")

    evidence = _mapping(raw.get("evidence"), "ICD-10 contract.evidence")
    expected_evidence = {
        "audited_hospital_count": 8,
        "scanned_row_count": 16054,
        "nonempty_cell_count": 16053,
        "nonstring_source_cell_count": 0,
        "comma_delimited_cell_count": 16022,
        "single_code_cell_count": 7,
        "approved_textual_missing_cell_count": 24,
    }
    if evidence != expected_evidence:
        raise ConfigurationError("Reviewed ICD-10 evidence counts changed")
    if (
        evidence["nonempty_cell_count"]
        != evidence["comma_delimited_cell_count"]
        + evidence["single_code_cell_count"]
        + evidence["approved_textual_missing_cell_count"]
    ):
        raise ConfigurationError("Reviewed ICD-10 cell accounting is invalid")

    source_text = _mapping(
        raw.get("source_text_contract"), "ICD-10 contract.source_text_contract"
    )
    expected_source_text = {
        "target": "icd10_codes_source_text",
        "output_value_type": "large_string",
        "nonmissing_policy": "preserve_exact",
        "literal_empty_handling": "null",
        "source_absence_handling": "null",
        "unknown_nonempty_handling": "block",
    }
    if source_text != expected_source_text:
        raise ConfigurationError("Reviewed ICD-10 source-text contract changed")

    missing_raw = raw.get("approved_textual_missing")
    if not isinstance(missing_raw, list) or len(missing_raw) != 1:
        raise ConfigurationError("Exactly one ICD-10 textual-missing rule is required")
    missing = _mapping(missing_raw[0], "ICD-10 contract.approved_textual_missing[0]")
    expected_missing = {
        "rule_id": "HARM-STATIC-ICD10-MISSING-UK03-001",
        "hospital": "asic_UK03",
        "normalization": "strip_casefold",
        "token": "nan",
        "evidence_cell_count": 24,
        "output": "null",
    }
    if missing != expected_missing:
        raise ConfigurationError("Reviewed ICD-10 textual-missing rule changed")
    missing_decision = ReviewedICD10MissingDecision(
        rule_id=expected_missing["rule_id"],
        hospital=expected_missing["hospital"],
        normalization=expected_missing["normalization"],
        token=expected_missing["token"],
        evidence_cell_count=expected_missing["evidence_cell_count"],
    )

    candidate = _mapping(
        raw.get("candidate_list_contract"), "ICD-10 contract.candidate_list_contract"
    )
    expected_candidate = {
        "target": "icd10_codes",
        "output_value_type": "list_large_string",
        "delimiter": ",",
        "component_normalization": "strip_surrounding_whitespace_only",
        "single_code_handling": "one_element_list",
        "empty_component_handling": "block",
        "preserve_source_order": True,
        "preserve_duplicates": True,
        "preserve_case": True,
        "preserve_component_punctuation": True,
        "semantic_code_validation": "deferred",
        "activation": "blocked_pending_component_audit_review",
    }
    if candidate != expected_candidate:
        raise ConfigurationError("Reviewed ICD-10 candidate list contract changed")

    scope = _mapping(raw.get("scope"), "ICD-10 contract.scope")
    expected_scope = {
        "allow_component_audit_reads": True,
        "allow_report_writes": True,
        "allow_value_transforms": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_union_schema_freeze": False,
        "allow_parser_activation": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Reviewed ICD-10 contract must remain fail-closed")

    return ReviewedICD10Contract(
        version="0.1",
        approved_by_role="project_data_owner",
        approved_at="2026-08-05",
        static_contract_audit_run_id="20260805T112746Z",
        notation_audit_run_id="20260805T131735Z",
        notation_review_generated_at_utc="2026-08-05T13:17:35+00:00",
        audited_hospital_count=8,
        scanned_row_count=16054,
        nonempty_cell_count=16053,
        comma_delimited_cell_count=16022,
        single_code_cell_count=7,
        approved_textual_missing_cell_count=24,
        source_text_target="icd10_codes_source_text",
        source_text_value_type="large_string",
        missing_decisions=(missing_decision,),
        list_target="icd10_codes",
        list_value_type="list_large_string",
        delimiter=",",
        component_normalization="strip_surrounding_whitespace_only",
        preserve_source_order=True,
        preserve_duplicates=True,
        detail_audit_run_id=None,
        detail_review_generated_at_utc=None,
        component_code_candidate_regex=None,
        trailing_empty_evidence_by_hospital=(),
        expected_source_component_count=None,
        expected_valid_component_count_before_deduplication=None,
        expected_reviewed_incomplete_component_count=None,
        expected_duplicate_component_count=None,
        expected_retained_component_count=None,
        expected_trailing_empty_cell_count=None,
        maximum_trailing_empty_components_per_cell=None,
        incomplete_component_hospital=None,
        incomplete_component_private_detail_class=None,
        allow_private_review_evidence_reads=False,
        allow_synthetic_batch_transform=False,
        allow_component_audit_reads=True,
        allow_report_writes=True,
        allow_value_transforms=False,
        allow_harmonized_artifact_writes=False,
        allow_hospital_concatenation=False,
        allow_union_schema_freeze=False,
        allow_parser_activation=False,
        source_path=source,
    )


def _load_reviewed_icd10_contract_v02(path: str | Path) -> ReviewedICD10Contract:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed ICD-10 contract")
    if raw.get("reviewed_icd10_contract_version") != "0.2":
        raise ConfigurationError("Reviewed ICD-10 contract version must be 0.2")
    if raw.get("status") != "approved_for_bounded_parser_execution":
        raise ConfigurationError("Reviewed ICD-10 contract 0.2 status is invalid")
    if raw.get("extends") != "reviewed_icd10_contract_0_1.yaml":
        raise ConfigurationError("Reviewed ICD-10 predecessor is invalid")
    base_path = source.parent / "reviewed_icd10_contract_0_1.yaml"
    base_hash = required_string(raw, "extends_sha256", "ICD-10 contract")
    if (
        base_hash
        != "f6edf01c21587ae124fa0631cb5cc8ac4e297ef8a2e1e77a966a51ef83d24da1"
        or not base_path.is_file()
        or sha256_file(base_path) != base_hash
    ):
        raise ConfigurationError("Reviewed ICD-10 predecessor hash changed")
    base = _load_reviewed_icd10_contract_v01(base_path)

    review = _mapping(raw.get("review"), "ICD-10 contract.review")
    expected_review = {
        "approved_by_role": "project_data_owner",
        "approved_at": "2026-08-05",
        "icd10_component_audit_run_id": "20260805T134245Z",
        "icd10_component_detail_audit_run_id": "20260805T135859Z",
        "icd10_component_detail_review_generated_at_utc": (
            "2026-08-05T13:58:59+00:00"
        ),
    }
    if {key: review.get(key) for key in expected_review} != expected_review:
        raise ConfigurationError("Reviewed ICD-10 contract 0.2 evidence changed")
    for key in (
        "icd10_component_audit_run_id",
        "icd10_component_detail_audit_run_id",
    ):
        if not RUN_ID.fullmatch(str(review[key])):
            raise ConfigurationError("Reviewed ICD-10 contract 0.2 run ID is invalid")

    evidence = _mapping(
        raw.get("approved_component_evidence"),
        "ICD-10 contract.approved_component_evidence",
    )
    expected_evidence = {
        "source_component_count": 466002,
        "valid_code_candidate_count_before_deduplication": 465975,
        "trailing_empty_component_count": 26,
        "cells_with_trailing_empty_component": 26,
        "leading_empty_component_count": 0,
        "interior_empty_component_count": 0,
        "reviewed_incomplete_component_count": 1,
        "duplicate_component_count": 96505,
        "retained_unique_valid_component_count": 369470,
        "cells_empty_after_approved_removals": 0,
    }
    if evidence != expected_evidence:
        raise ConfigurationError("Reviewed ICD-10 component evidence changed")
    if evidence["source_component_count"] != (
        evidence["retained_unique_valid_component_count"]
        + evidence["duplicate_component_count"]
        + evidence["trailing_empty_component_count"]
        + evidence["reviewed_incomplete_component_count"]
    ):
        raise ConfigurationError("Reviewed ICD-10 component accounting is invalid")

    final = _mapping(raw.get("final_list_contract"), "ICD-10 final list contract")
    expected_fixed = {
        "target": "icd10_codes",
        "output_value_type": "list_large_string",
        "delimiter": ",",
        "component_normalization": "strip_surrounding_whitespace_only",
        "component_code_candidate_regex": (
            r"^[A-Za-z][0-9]{2}(?:\.[A-Za-z0-9]{1,4})?[*!†‡]?$"
        ),
        "single_code_handling": "one_element_list",
        "preserve_case": True,
        "preserve_component_punctuation": True,
        "source_order_policy": "preserve_first_occurrence_order",
        "duplicate_policy": "ordered_first_occurrence_deduplication",
        "leading_empty_component_handling": "block",
        "interior_empty_component_handling": "block",
        "trailing_empty_component_handling": (
            "remove_only_in_reviewed_hospital_scopes"
        ),
        "maximum_trailing_empty_components_per_cell": 1,
        "incomplete_component_handling": (
            "remove_only_if_bound_to_owner_only_review_evidence"
        ),
        "every_other_nonempty_non_code_component_handling": "block",
        "empty_list_after_component_rules_handling": "block",
        "semantic_code_validation": "deferred",
    }
    if {key: final.get(key) for key in expected_fixed} != expected_fixed:
        raise ConfigurationError("Reviewed ICD-10 final list policy changed")
    try:
        re.compile(str(expected_fixed["component_code_candidate_regex"]))
    except re.error as exc:
        raise ConfigurationError("Reviewed ICD-10 component regex is invalid") from exc
    trailing = _mapping(
        final.get("trailing_empty_component_evidence_by_hospital"),
        "ICD-10 final trailing-empty evidence",
    )
    expected_trailing = {"asic_UK03": 20, "asic_UK08": 6}
    if trailing != expected_trailing or sum(trailing.values()) != 26:
        raise ConfigurationError("Reviewed ICD-10 trailing-empty scope changed")
    private = _mapping(
        final.get("incomplete_component_private_evidence"),
        "ICD-10 final incomplete-component evidence",
    )
    expected_private = {
        "audit_run_id": "20260805T135859Z",
        "hospital": "asic_UK08",
        "detail_class": "incomplete_component_candidate",
        "expected_distinct_component_count": 1,
        "expected_component_count": 1,
    }
    if private != expected_private:
        raise ConfigurationError("Reviewed ICD-10 private evidence binding changed")

    accounting = _mapping(
        raw.get("accounting_invariant"), "ICD-10 contract.accounting_invariant"
    )
    expected_accounting = {
        "expression": (
            "source_components_equals_retained_plus_duplicates_plus_trailing_empty_plus_reviewed_incomplete"
        ),
        "source_component_count": 466002,
        "retained_unique_valid_component_count": 369470,
        "duplicate_component_count": 96505,
        "trailing_empty_component_count": 26,
        "reviewed_incomplete_component_count": 1,
    }
    if accounting != expected_accounting:
        raise ConfigurationError("Reviewed ICD-10 accounting invariant changed")

    scope = _mapping(raw.get("scope"), "ICD-10 contract.scope")
    expected_scope = {
        "allow_private_review_evidence_reads": True,
        "allow_synthetic_batch_transform": True,
        "allow_parser_activation": True,
        "allow_production_reads": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_union_schema_freeze": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Reviewed ICD-10 contract 0.2 must remain fail-closed")

    return ReviewedICD10Contract(
        version="0.2",
        approved_by_role="project_data_owner",
        approved_at="2026-08-05",
        static_contract_audit_run_id=base.static_contract_audit_run_id,
        notation_audit_run_id=base.notation_audit_run_id,
        notation_review_generated_at_utc=base.notation_review_generated_at_utc,
        audited_hospital_count=base.audited_hospital_count,
        scanned_row_count=base.scanned_row_count,
        nonempty_cell_count=base.nonempty_cell_count,
        comma_delimited_cell_count=base.comma_delimited_cell_count,
        single_code_cell_count=base.single_code_cell_count,
        approved_textual_missing_cell_count=base.approved_textual_missing_cell_count,
        source_text_target=base.source_text_target,
        source_text_value_type=base.source_text_value_type,
        missing_decisions=base.missing_decisions,
        list_target="icd10_codes",
        list_value_type="list_large_string",
        delimiter=",",
        component_normalization="strip_surrounding_whitespace_only",
        preserve_source_order=True,
        preserve_duplicates=False,
        detail_audit_run_id="20260805T135859Z",
        detail_review_generated_at_utc="2026-08-05T13:58:59+00:00",
        component_code_candidate_regex=str(
            expected_fixed["component_code_candidate_regex"]
        ),
        trailing_empty_evidence_by_hospital=tuple(expected_trailing.items()),
        expected_source_component_count=466002,
        expected_valid_component_count_before_deduplication=465975,
        expected_reviewed_incomplete_component_count=1,
        expected_duplicate_component_count=96505,
        expected_retained_component_count=369470,
        expected_trailing_empty_cell_count=26,
        maximum_trailing_empty_components_per_cell=1,
        incomplete_component_hospital="asic_UK08",
        incomplete_component_private_detail_class=(
            "incomplete_component_candidate"
        ),
        allow_private_review_evidence_reads=True,
        allow_synthetic_batch_transform=True,
        allow_component_audit_reads=True,
        allow_report_writes=False,
        allow_value_transforms=True,
        allow_harmonized_artifact_writes=False,
        allow_hospital_concatenation=False,
        allow_union_schema_freeze=False,
        allow_parser_activation=True,
        source_path=source,
    )


def load_reviewed_icd10_contract(path: str | Path) -> ReviewedICD10Contract:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed ICD-10 contract")
    version = raw.get("reviewed_icd10_contract_version")
    if version == "0.1":
        return _load_reviewed_icd10_contract_v01(source)
    if version == "0.2":
        return _load_reviewed_icd10_contract_v02(source)
    raise ConfigurationError("Reviewed ICD-10 contract version must be 0.1 or 0.2")
