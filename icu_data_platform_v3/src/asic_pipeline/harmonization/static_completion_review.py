from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import json
import os
from pathlib import Path
from typing import Any

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
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


TARGETS = (
    "icu_los",
    "dialysis_free_days",
    "vent_free_days",
    "icd10_codes",
)
TECHNICAL_CHECKS = frozenset(
    {
        "static_contract_evidence_valid",
        "current_partial_decision_registry_valid",
        "remaining_static_occurrences_complete",
        "remaining_static_accounting_conserved",
        "sanitized_review_matches_private_evidence",
        "no_production_data_artifacts_generated",
    }
)
CHECK_DETAILS = {
    "static_contract_evidence_valid": (
        "The immutable static-contract private and sanitized artifacts must match the approved run and context."
    ),
    "current_partial_decision_registry_valid": (
        "The current fail-closed partial registry must be version 0.2 and use the same evidence run."
    ),
    "remaining_static_occurrences_complete": (
        "Each selected remaining static target must have one or more occurrence rows and one variable summary."
    ),
    "remaining_static_accounting_conserved": (
        "Every selected occurrence must conserve rows; numeric non-empty values must be direct, reviewed missing, or unresolved."
    ),
    "sanitized_review_matches_private_evidence": (
        "Aggregate selected counts must reproduce the immutable sanitized static-contract review."
    ),
    "remaining_static_numeric_tokens_resolved": (
        "Every selected numeric non-empty token must already be direct numeric or an approved scoped missing sentinel."
    ),
    "remaining_static_mappings_approved": (
        "Every selected raw occurrence-to-canonical mapping requires explicit human approval."
    ),
    "remaining_static_value_types_approved": (
        "The proposed physical value type for each remaining variable requires explicit human approval."
    ),
    "remaining_static_units_approved": (
        "The proposed unit for each remaining numeric variable requires explicit human approval."
    ),
    "remaining_static_parsing_and_missing_policies_approved": (
        "Direct-numeric and hospital-scoped missing-sentinel policies require explicit human approval."
    ),
    "icd10_exact_preservation_policy_approved": (
        "Preserving each non-empty ICD-10 source cell as one exact string requires explicit human approval."
    ),
    "no_production_data_artifacts_generated": (
        "This review may write sanitized reports only and must not read raw or ingested data or create harmonized data."
    ),
}


@dataclass(frozen=True)
class RemainingStaticProposal:
    target: str
    kind: str
    value_type: str
    unit: str
    policy: str


@dataclass(frozen=True)
class StaticCompletionReviewPolicy:
    version: str
    static_contract_audit_basis_run_id: str
    required_private_artifact_version: str
    required_review_artifact_version: str
    reviewed_static_decisions_path: Path
    proposals: tuple[RemainingStaticProposal, ...]
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class StaticCompletionReviewConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root


@dataclass(frozen=True)
class StaticCompletionReviewResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def load_static_completion_review_config(
    path: str | Path,
) -> StaticCompletionReviewConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "Static-completion review configuration")
    policy_path = resolve_path(
        required_string(raw, "static_completion_review_policy", "config"),
        source,
    )
    return StaticCompletionReviewConfig(harmonization, policy_path)


def load_static_completion_review_policy(
    path: str | Path,
) -> StaticCompletionReviewPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Static-completion review policy")
    if raw.get("static_completion_review_policy_version") != "0.1":
        raise ConfigurationError("Static-completion review policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_remaining_static_review":
        raise ConfigurationError("Static-completion review policy is not approved")
    basis = required_string(
        raw, "static_contract_audit_basis_run_id", "static completion review"
    )
    if basis != "20260805T112746Z":
        raise ConfigurationError("Static-completion evidence basis changed unexpectedly")
    proposals_raw = raw.get("remaining_variables")
    if not isinstance(proposals_raw, list) or len(proposals_raw) != len(TARGETS):
        raise ConfigurationError("Static-completion review must contain four variables")
    proposals: list[RemainingStaticProposal] = []
    for index, value in enumerate(proposals_raw):
        location = f"static completion.remaining_variables[{index}]"
        item = _mapping(value, location)
        proposals.append(
            RemainingStaticProposal(
                target=required_string(item, "target", location),
                kind=required_string(item, "kind", location),
                value_type=required_string(item, "proposed_value_type", location),
                unit=required_string(item, "proposed_unit", location),
                policy=required_string(item, "proposed_policy", location),
            )
        )
    expected = (
        ("icu_los", "numeric", "float64", "day", "direct_numeric_with_null_missing"),
        (
            "dialysis_free_days",
            "numeric",
            "float64",
            "day",
            "direct_numeric_with_reviewed_scoped_sentinels",
        ),
        (
            "vent_free_days",
            "numeric",
            "float64",
            "day",
            "direct_numeric_with_reviewed_scoped_sentinels",
        ),
        (
            "icd10_codes",
            "free_text",
            "large_string",
            "not_applicable",
            "preserve_exact_nonempty_source_string",
        ),
    )
    observed = tuple(
        (item.target, item.kind, item.value_type, item.unit, item.policy)
        for item in proposals
    )
    if observed != expected:
        raise ConfigurationError("Static-completion proposals changed unexpectedly")
    scope = _mapping(raw.get("scope"), "static completion.scope")
    expected_scope = {
        "allow_static_contract_private_evidence_reads": True,
        "allow_review_report_writes": True,
        "allow_ingested_data_reads": False,
        "allow_raw_data_reads": False,
        "allow_value_transforms": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_registry_approval": False,
        "allow_union_schema_approval": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static-completion review must remain read-only")
    reporting = _mapping(raw.get("reporting"), "static completion.reporting")
    return StaticCompletionReviewPolicy(
        version="0.1",
        static_contract_audit_basis_run_id=basis,
        required_private_artifact_version=required_string(
            raw,
            "required_static_contract_private_artifact_version",
            "static completion review",
        ),
        required_review_artifact_version=required_string(
            raw,
            "required_static_contract_review_artifact_version",
            "static completion review",
        ),
        reviewed_static_decisions_path=resolve_path(
            required_string(
                raw, "reviewed_static_decisions", "static completion review"
            ),
            source,
        ),
        proposals=tuple(proposals),
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


def _integer(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise HarmonizationError(f"Static-completion evidence field {key} is invalid")
    return value


def build_static_completion_review(
    config: StaticCompletionReviewConfig,
    static_contract_audit_run_id: str,
) -> StaticCompletionReviewResult:
    policy = load_static_completion_review_policy(config.policy_path)
    if static_contract_audit_run_id != policy.static_contract_audit_basis_run_id:
        raise HarmonizationError("Unexpected static-contract audit basis run")
    if not RUN_ID_PATTERN.fullmatch(static_contract_audit_run_id):
        raise HarmonizationError("Invalid static-contract audit run ID")
    private_dir = (
        config.reports_root
        / "private"
        / "static_contract_audit"
        / static_contract_audit_run_id
    )
    manifest_path = private_dir / "static_contract_audit_manifest.json"
    occurrences_path = private_dir / "occurrences.parquet"
    variables_path = private_dir / "variables.parquet"
    source_review_path = (
        config.reports_root
        / "review"
        / "static_contract_audit"
        / f"{static_contract_audit_run_id}.json"
    )
    required_paths = (
        manifest_path,
        occurrences_path,
        variables_path,
        source_review_path,
        policy.reviewed_static_decisions_path,
    )
    if any(not path.is_file() for path in required_paths):
        raise HarmonizationError("Static-completion input evidence is incomplete")
    manifest = _read_json(manifest_path, "Static-contract private manifest")
    source_review = _read_json(source_review_path, "Static-contract review")
    occurrences = _read_rows(occurrences_path, "Static-contract occurrences")
    variables = _read_rows(variables_path, "Static-contract variables")
    registry = load_reviewed_static_decision_registry(
        policy.reviewed_static_decisions_path
    )
    registry_valid = bool(
        registry.version == "0.2"
        and registry.static_contract_audit_run_id == static_contract_audit_run_id
        and not registry.allow_production_reads
        and not registry.allow_artifact_writes
        and not registry.allow_hospital_concatenation
        and not registry.allow_union_schema_freeze
    )
    evidence_valid = bool(
        manifest.get("artifact") == "asic_v3_static_contract_audit_private"
        and manifest.get("artifact_version")
        == policy.required_private_artifact_version
        and manifest.get("dataset_context") == config.dataset_context
        and manifest.get("selected_occurrence_count") == len(occurrences)
        and manifest.get("selected_variable_count") == len(variables)
        and source_review.get("artifact") == "asic_v3_static_contract_audit_review"
        and source_review.get("artifact_version")
        == policy.required_review_artifact_version
        and source_review.get("dataset_context") == config.dataset_context
        and source_review.get("metrics", {}).get(
            "technical_blocking_finding_count"
        )
        == 0
    )
    proposals = {item.target: item for item in policy.proposals}
    selected_occurrences = [
        row for row in occurrences if row.get("candidate_target") in proposals
    ]
    selected_variables = [
        row for row in variables if row.get("candidate_target") in proposals
    ]
    occurrence_targets = {row.get("candidate_target") for row in selected_occurrences}
    variable_targets = [row.get("candidate_target") for row in selected_variables]
    complete = bool(
        occurrence_targets == set(TARGETS)
        and set(variable_targets) == set(TARGETS)
        and len(variable_targets) == len(set(variable_targets)) == len(TARGETS)
    )
    accounting_valid = True
    numeric_unresolved_count = 0
    sanitized_occurrences: list[dict[str, Any]] = []
    for row in selected_occurrences:
        target = str(row.get("candidate_target"))
        proposal = proposals[target]
        row_count = _integer(row, "scan_row_count")
        source_null = _integer(row, "scan_source_null_count")
        literal_empty = _integer(row, "scan_literal_empty_count")
        nonempty = _integer(row, "scan_nonempty_count")
        direct = _integer(row, "scan_direct_numeric_count")
        approved_missing = _integer(row, "scan_approved_missing_sentinel_count")
        unresolved = _integer(row, "scan_unresolved_nonempty_count")
        accounting_valid &= row_count == source_null + literal_empty + nonempty
        if proposal.kind == "numeric":
            accounting_valid &= nonempty == direct + approved_missing + unresolved
            numeric_unresolved_count += unresolved
        sanitized_occurrences.append(
            {
                "hospital": row.get("hospital"),
                "target": target,
                "row_count": row_count,
                "source_null_count": source_null,
                "literal_empty_count": literal_empty,
                "nonempty_count": nonempty,
                "direct_numeric_count": direct,
                "approved_missing_sentinel_count": approved_missing,
                "unresolved_nonempty_count": unresolved,
            }
        )
    source_summaries = source_review.get("variables")
    if not isinstance(source_summaries, list):
        raise HarmonizationError("Static-contract sanitized variable evidence is invalid")
    review_by_target = {
        row.get("candidate_target"): row
        for row in source_summaries
        if isinstance(row, dict) and row.get("candidate_target") in proposals
    }
    review_matches = set(review_by_target) == set(TARGETS)
    sanitized_variables: list[dict[str, Any]] = []
    for proposal in policy.proposals:
        rows = [row for row in sanitized_occurrences if row["target"] == proposal.target]
        totals = {
            key: sum(int(row[key]) for row in rows)
            for key in (
                "row_count",
                "source_null_count",
                "literal_empty_count",
                "nonempty_count",
                "direct_numeric_count",
                "approved_missing_sentinel_count",
                "unresolved_nonempty_count",
            )
        }
        prior = review_by_target.get(proposal.target, {})
        comparisons = {
            "row_count": "scan_row_count",
            "source_null_count": "source_null_count",
            "literal_empty_count": "literal_empty_count",
            "nonempty_count": "nonempty_count",
            "direct_numeric_count": "direct_numeric_count",
            "approved_missing_sentinel_count": "approved_missing_sentinel_count",
            "unresolved_nonempty_count": "unresolved_nonempty_count",
        }
        review_matches &= all(
            prior.get(prior_key) == totals[current_key]
            for current_key, prior_key in comparisons.items()
        )
        sanitized_variables.append(
            {
                "target": proposal.target,
                "kind": proposal.kind,
                "proposed_value_type": proposal.value_type,
                "proposed_unit": proposal.unit,
                "proposed_policy": proposal.policy,
                "hospital_count": len({row["hospital"] for row in rows}),
                "occurrence_count": len(rows),
                **totals,
            }
        )
    failure_counts = {
        "static_contract_evidence_valid": int(not evidence_valid),
        "current_partial_decision_registry_valid": int(not registry_valid),
        "remaining_static_occurrences_complete": int(not complete),
        "remaining_static_accounting_conserved": int(not accounting_valid),
        "sanitized_review_matches_private_evidence": int(not review_matches),
        "remaining_static_numeric_tokens_resolved": numeric_unresolved_count,
        "remaining_static_mappings_approved": len(selected_occurrences),
        "remaining_static_value_types_approved": len(TARGETS),
        "remaining_static_units_approved": 3,
        "remaining_static_parsing_and_missing_policies_approved": sum(
            item.kind == "numeric" for item in policy.proposals
        ),
        "icd10_exact_preservation_policy_approved": 1,
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
    payload = {
        "artifact": "asic_v3_static_completion_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "overall_status": overall_status(checks),
        "inputs": {
            "static_contract_audit_run_id": static_contract_audit_run_id,
            "static_contract_manifest_sha256": sha256_file(manifest_path),
            "static_contract_occurrences_sha256": sha256_file(occurrences_path),
            "static_contract_variables_sha256": sha256_file(variables_path),
            "static_contract_review_sha256": sha256_file(source_review_path),
            "reviewed_static_decisions_version": registry.version,
            "reviewed_static_decisions_sha256": sha256_file(
                policy.reviewed_static_decisions_path
            ),
        },
        "metrics": {
            "selected_variable_count": len(TARGETS),
            "selected_occurrence_count": len(selected_occurrences),
            "selected_hospital_count": len(
                {row["hospital"] for row in sanitized_occurrences}
            ),
            "numeric_unresolved_nonempty_token_count": numeric_unresolved_count,
            "technical_blocking_finding_count": len(technical),
        },
        "variables": sanitized_variables,
        "hospital_evidence": sorted(
            sanitized_occurrences,
            key=lambda row: (str(row["target"]), str(row["hospital"])),
        ),
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
        },
        "publication": {
            "production_data_read": False,
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "registry_approved": False,
        },
        "limitations": [
            "This report re-aggregates immutable static-contract evidence; it does not rescan raw, ingested, or harmonized data.",
            "Approved missing-sentinel counts reflect previously reviewed hospital-scoped rules; raw sentinel tokens remain outside this sanitized report.",
            "ICD-10 content examples remain owner-only and are not copied into this report.",
            "No mapping, type, unit, parser, missing, or exact-string preservation proposal is approved by this command.",
            "No harmonized, cleaned, derived, concatenated, or pooled data artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(payload)
    return StaticCompletionReviewResult(
        overall_status=payload["overall_status"],
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        review_payload=payload,
    )


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 remaining-static contract review",
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
    lines.extend(["", "## Review totals", ""])
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    lines.extend(["", "## Proposed remaining variables", ""])
    for item in payload["variables"]:
        lines.append(
            f"- `{item['target']}`: type `{item['proposed_value_type']}`; "
            f"unit `{item['proposed_unit']}`; policy `{item['proposed_policy']}`; "
            f"hospitals `{item['hospital_count']}`; non-empty `{item['nonempty_count']}`; "
            f"approved missing `{item['approved_missing_sentinel_count']}`; "
            f"unresolved `{item['unresolved_nonempty_count']}`"
        )
    lines.extend(["", "## Per-hospital aggregate evidence", ""])
    for row in payload["hospital_evidence"]:
        lines.append(
            f"- `{row['target']}` `{row['hospital']}`: rows `{row['row_count']}`; "
            f"non-empty `{row['nonempty_count']}`; direct numeric "
            f"`{row['direct_numeric_count']}`; approved missing "
            f"`{row['approved_missing_sentinel_count']}`; unresolved "
            f"`{row['unresolved_nonempty_count']}`"
        )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Approve or revise the four proposed contracts before extending the executable static registry."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


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


def write_static_completion_review_bundle(
    result: StaticCompletionReviewResult,
    reports_root: Path,
    run_id: str | None = None,
) -> StaticCompletionReviewResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid static-completion review run ID")
    review_dir = reports_root / "review" / "static_completion_review"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "Static-completion review run already exists and will not be overwritten"
        )
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
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
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
