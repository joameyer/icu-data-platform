from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.cleaning.pipeline import load_cleaning_policy
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.decisions import load_candidate_unit_decisions
from asic_pipeline.unit_resolution.harmonized_0_2_promotion import (
    load_harmonized_0_2_promotion_policy,
)
from asic_pipeline.unit_resolution.reviewed_decisions import (
    load_reviewed_unit_decisions,
)
from asic_pipeline.unit_resolution.reviewed_medication_semantics import (
    load_reviewed_medication_semantics,
)


EXPECTED_BOUNDARY = {
    "write_reports_only": True,
    "read_clinical_rows": False,
    "activate_policy_0_2": False,
    "build_cleaned_candidate": False,
    "modify_harmonized_release": False,
    "modify_existing_cleaned_release": False,
    "modify_existing_derived_release": False,
    "filter_rows_or_stays": False,
    "authorize_external_data_export": False,
}

EXPECTED_PROPOSED_POLICY = {
    "version": "0.2",
    "replay_every_cleaning_0_1_rule": True,
    "preserve_cleaning_0_1_rule_semantics": True,
    "apply_general_negative_medication_rule": True,
    "negative_medication_rule_id": "mask_negative_medication_or_therapy_value",
    "explicit_zero_remains_zero": True,
    "null_remains_null": True,
    "activate_all_twelve_unit_aware_range_dispositions": True,
    "preserve_rows_and_stays": True,
    "preserve_columns": True,
    "create_derived_variables": False,
    "perform_time_blocking": False,
}

EXPECTED_METRICS = {
    "static_rows": 16_054,
    "dynamic_rows": 24_069_379,
    "baseline_legacy_applied_rule_count": 34,
    "baseline_additional_scalar_rule_count": 15,
    "baseline_list_rule_count": 1,
    "baseline_hospital_mask_rule_count": 1,
    "medication_or_therapy_variable_count": 34,
    "currently_audited_negative_medication_value_count": 201,
    "priority_range_variable_count": 12,
    "historical_outside_legacy_range_count": 10_021,
    "clinical_rows_read_by_review": 0,
}


@dataclass(frozen=True)
class Cleaning02PolicyReviewConfig:
    dataset_context: str
    data_root: Path
    reports_root: Path
    policy_path: Path


@dataclass(frozen=True)
class Cleaning02PolicyReviewPolicy:
    harmonized_release_id: str
    harmonized_contract_version: str
    promotion_policy_path: Path
    baseline_cleaning_policy_path: Path
    candidate_decisions_path: Path
    reviewed_decisions_path: Path
    medication_semantics_path: Path
    frozen_contract_policy_path: Path
    frozen_contract_directory: Path
    proposed_policy: dict[str, Any]
    expected: dict[str, int]
    review_directory_name: str
    artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class Cleaning02PolicyReviewResult:
    run_id: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    review_json_path: Path
    review_markdown_path: Path

    @property
    def overall_status(self) -> str:
        return "pending_human_review"


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _verify_hash(path: Path, expected: str, label: str) -> None:
    if sha256_file(path) != expected:
        raise ConfigurationError(f"Immutable {label} changed")


def load_cleaning_0_2_policy_review_config(
    path: str | Path,
) -> Cleaning02PolicyReviewConfig:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning 0.2 review configuration")
    if raw.get("dataset_context") != "production":
        raise ConfigurationError("Cleaning 0.2 policy review is production-only")
    paths = _mapping(raw.get("paths"), "config.paths")
    return Cleaning02PolicyReviewConfig(
        dataset_context="production",
        data_root=resolve_path(required_string(paths, "data", "config.paths"), source),
        reports_root=resolve_path(
            required_string(paths, "reports", "config.paths"), source
        ),
        policy_path=resolve_path(
            required_string(raw, "cleaning_policy_0_2_review", "config"), source
        ),
    )


def load_cleaning_0_2_policy_review_policy(
    path: str | Path,
) -> Cleaning02PolicyReviewPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning 0.2 policy review")
    if (
        raw.get("cleaning_policy_0_2_review_version") != "0.1"
        or raw.get("status")
        != "complete_candidate_policy_pending_data_owner_approval"
    ):
        raise ConfigurationError("Cleaning 0.2 policy-review status is invalid")
    inputs = _mapping(raw.get("inputs"), "inputs")

    def immutable_path(path_key: str, hash_key: str, label: str) -> Path:
        resolved = resolve_path(required_string(inputs, path_key, "inputs"), source)
        _verify_hash(resolved, required_string(inputs, hash_key, "inputs"), label)
        return resolved

    promotion = immutable_path(
        "harmonized_promotion_policy",
        "harmonized_promotion_policy_sha256",
        "harmonized 0.2 promotion policy",
    )
    baseline = immutable_path(
        "baseline_cleaning_policy",
        "baseline_cleaning_policy_sha256",
        "cleaning 0.1 policy",
    )
    candidate = immutable_path(
        "candidate_unit_decisions",
        "candidate_unit_decisions_sha256",
        "candidate unit decisions",
    )
    reviewed = immutable_path(
        "reviewed_unit_decisions",
        "reviewed_unit_decisions_sha256",
        "reviewed unit decisions",
    )
    medication = immutable_path(
        "reviewed_medication_semantics",
        "reviewed_medication_semantics_sha256",
        "reviewed medication semantics",
    )
    freeze = immutable_path(
        "frozen_contract_policy",
        "frozen_contract_policy_sha256",
        "frozen contract 0.2 policy",
    )
    proposed = _mapping(raw.get("proposed_policy"), "proposed_policy")
    expected = _mapping(raw.get("expected"), "expected")
    outputs = _mapping(raw.get("outputs"), "outputs")
    if proposed != EXPECTED_PROPOSED_POLICY:
        raise ConfigurationError("Proposed cleaning 0.2 boundary changed")
    if expected != EXPECTED_METRICS:
        raise ConfigurationError("Cleaning 0.2 review accounting changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Cleaning 0.2 review safety boundary changed")
    if (
        inputs.get("harmonized_release_id") != "20260807T112402Z"
        or inputs.get("harmonized_contract_version") != "0.2"
        or inputs.get("frozen_contract_version") != "0.2"
        or inputs.get("frozen_contract_directory")
        != "harmonized_schema_dictionary/0.2"
        or outputs.get("review_directory_name") != "cleaning_policy_0_2_review"
        or outputs.get("artifact_version") != "0.1"
    ):
        raise ConfigurationError("Cleaning 0.2 review scope changed")
    return Cleaning02PolicyReviewPolicy(
        harmonized_release_id="20260807T112402Z",
        harmonized_contract_version="0.2",
        promotion_policy_path=promotion,
        baseline_cleaning_policy_path=baseline,
        candidate_decisions_path=candidate,
        reviewed_decisions_path=reviewed,
        medication_semantics_path=medication,
        frozen_contract_policy_path=freeze,
        frozen_contract_directory=Path("harmonized_schema_dictionary/0.2"),
        proposed_policy=proposed,
        expected={key: int(value) for key, value in expected.items()},
        review_directory_name=str(outputs["review_directory_name"]),
        artifact_version=str(outputs["artifact_version"]),
        source_path=source,
    )


def _range_summary(row: dict[str, Any]) -> str:
    rule = str(row["rule"])
    if rule == "mask_zero":
        return "mask exact zero; preserve every finite nonzero value"
    if rule == "recover_unique_power_of_ten_else_mask_outside_open_closed_range":
        return (
            f"require > {row['hard_min_exclusive']} and <= {row['hard_max_inclusive']}; "
            "repair only a uniquely matching approved power of ten, otherwise mask"
        )
    if rule in {
        "mask_outside_open_closed_range_no_ambiguous_power_recovery",
        "mask_zero_or_outside_open_closed_range",
    }:
        return (
            f"require > {row['hard_min_exclusive']} and <= {row['hard_max_inclusive']}; "
            "mask violations without power-of-ten recovery"
        )
    if rule == "preserve_values_within_closed_range":
        return (
            f"require >= {row['hard_min_inclusive']} and <= {row['hard_max_inclusive']}"
        )
    if rule == "preserve_schema_and_unit_regardless_of_current_missingness":
        return (
            f"retain the field and enforce the reviewed future domain "
            f"[{row['hard_min_inclusive']}, {row['hard_max_inclusive']}] when values exist"
        )
    raise ConfigurationError(f"Unknown priority cleaning rule {rule}")


def _range_rows(candidate: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variable, decision in sorted(candidate.priority_cleaning_range_decisions.items()):
        rows.append(
            {
                "variable": variable,
                "unit": decision["unit"],
                "rule": decision["rule"],
                "reviewed_action": _range_summary(decision),
                "historical_outside_legacy_range_count": decision[
                    "legacy_outside_count"
                ],
            }
        )
    return rows


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 cleaning policy 0.2 consolidated review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input harmonized release: `{payload['harmonized_release_id']}`",
        f"- Frozen harmonized contract: `{payload['harmonized_contract_version']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Technical blocking findings: `0`",
        "- Human blocking findings: `1`",
        "- Clinical rows read: `0`",
        "- Clinical data written or modified: `false`",
        "",
        "## Complete proposed cleaning 0.2 boundary",
        "",
        "1. Replay every cleaning 0.1 rule without changing its semantics.",
        "2. Apply the approved dictionary-driven negative-medication rule to all 34 medication/therapy variables: finite values below zero become null; zero, positive values, and null remain distinct.",
        "3. Activate all 12 unit-aware range dispositions listed below.",
        "4. Preserve all rows, stays, columns, provenance fields, and globally all-missing variables; do not derive, aggregate, form cohorts, or time-block.",
        "",
        "## Rule accounting",
        "",
        f"- Cleaning 0.1 applied legacy rules replayed: `{metrics['baseline_legacy_applied_rule_count']}`",
        f"- Cleaning 0.1 additional scalar rules replayed: `{metrics['baseline_additional_scalar_rule_count']}`",
        f"- Cleaning 0.1 list rules replayed: `{metrics['baseline_list_rule_count']}`",
        f"- Cleaning 0.1 hospital masks replayed: `{metrics['baseline_hospital_mask_rule_count']}`",
        f"- Medication/therapy variables covered: `{metrics['medication_or_therapy_variable_count']}`",
        f"- Negative medication values in the prior complete audit: `{metrics['currently_audited_negative_medication_value_count']}`",
        f"- Unit-aware priority range variables: `{metrics['priority_range_variable_count']}`",
        f"- Historical values outside the old legacy ranges: `{metrics['historical_outside_legacy_range_count']}`",
        "",
        "The historical count of 10,021 is evidence, not the number that cleaning 0.2 will necessarily mask. The new candidate audit will independently count the exact effects after harmonization 0.2 conversions.",
        "",
        "## Twelve unit-aware range dispositions",
        "",
    ]
    for row in payload["range_decisions"]:
        lines.append(
            f"- `{row['variable']}` ({row['unit']}): {row['reviewed_action']}. "
            f"Historical old-range findings: `{row['historical_outside_legacy_range_count']}`."
        )
    lines.extend(
        [
            "",
            "## Human review gate",
            "",
            "Approve or revise this complete policy before a cleaned 0.2 candidate is built. After approval, one streaming job will build the candidate and independently audit every output cell and every rule-level count.",
            "",
            "Suggested approval statement:",
            "",
            f"> I approve ASIC v3 cleaning policy 0.2 as described by cleaning-policy review {payload['run_id']}, including replay of cleaning 0.1, the general negative-medication rule, and all 12 unit-aware range dispositions.",
            "",
            "## Boundary",
            "",
            "This review reads only immutable policies, manifests, and release pointers. It does not read or write a clinical row, activate a rule, build a candidate, change any existing release, or authorize external export.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_exclusive(path: Path, value: str, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)


def run_cleaning_0_2_policy_review(
    config: Cleaning02PolicyReviewConfig,
    run_id: str | None = None,
) -> Cleaning02PolicyReviewResult:
    policy = load_cleaning_0_2_policy_review_policy(config.policy_path)
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise ConfigurationError("Cleaning 0.2 policy-review run ID is invalid")

    baseline = load_cleaning_policy(policy.baseline_cleaning_policy_path)
    candidate = load_candidate_unit_decisions(policy.candidate_decisions_path)
    reviewed = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    medication = load_reviewed_medication_semantics(policy.medication_semantics_path)
    promotion = load_harmonized_0_2_promotion_policy(policy.promotion_policy_path)
    if (
        promotion.release_id != policy.harmonized_release_id
        or promotion.contract_version != policy.harmonized_contract_version
        or len(baseline.additional_rules)
        != policy.expected["baseline_additional_scalar_rule_count"]
        or baseline.expected_applied_legacy_count
        != policy.expected["baseline_legacy_applied_rule_count"]
        or len(baseline.list_rules) != policy.expected["baseline_list_rule_count"]
        or len(baseline.hospital_masks)
        != policy.expected["baseline_hospital_mask_rule_count"]
        or len(reviewed.medication_variables)
        != policy.expected["medication_or_therapy_variable_count"] - 9
        or medication.negative_cleaning_rule["expected_current_mask_count"]
        != policy.expected["currently_audited_negative_medication_value_count"]
    ):
        raise HarmonizationError("Cleaning 0.2 policy lineage or accounting changed")

    range_rows = _range_rows(candidate)
    if (
        len(range_rows) != policy.expected["priority_range_variable_count"]
        or sum(row["historical_outside_legacy_range_count"] for row in range_rows)
        != policy.expected["historical_outside_legacy_range_count"]
    ):
        raise HarmonizationError("Priority range accounting changed")

    contract_manifest = (
        config.data_root
        / "contracts"
        / policy.frozen_contract_directory
        / "freeze_manifest.json"
    )
    contract = _read_json(contract_manifest, "Frozen harmonized contract 0.2")
    pointer_path = config.data_root / "harmonized/current_release.json"
    pointer = _read_json(pointer_path, "Current harmonized release pointer")
    manifest_path = Path(str(pointer.get("release_manifest", ""))).resolve()
    if not manifest_path.is_file():
        raise HarmonizationError("Current harmonized release manifest is unavailable")
    manifest = _read_json(manifest_path, "Current harmonized release manifest")
    if (
        contract.get("contract_version") != policy.harmonized_contract_version
        or contract.get("schema_frozen") is not True
        or contract.get("dictionary_frozen") is not True
        or pointer.get("artifact") != "asic_v3_current_harmonized_release"
        or pointer.get("release_id") != policy.harmonized_release_id
        or pointer.get("frozen_contract_version") != policy.harmonized_contract_version
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
        or pointer.get("harmonized_layer_ready") is not True
        or pointer.get("cleaning_input_approved") is not True
        or manifest.get("artifact") != "asic_v3_harmonized_release"
        or manifest.get("release_id") != policy.harmonized_release_id
        or manifest.get("frozen_contract_version") != policy.harmonized_contract_version
        or manifest.get("cleaning_input_approved") is not True
    ):
        raise HarmonizationError("Current harmonized 0.2 release is not the approved input")

    blocker = {
        "check": "cleaning_0_2_policy_approved",
        "details": (
            "The complete replay, negative-medication, and twelve unit-aware "
            "range decisions require one explicit data-owner approval."
        ),
    }
    payload = {
        "artifact": "asic_v3_cleaning_policy_0_2_review",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": utc_timestamp(),
        "run_id": selected,
        "harmonized_release_id": policy.harmonized_release_id,
        "harmonized_contract_version": policy.harmonized_contract_version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": [blocker],
        "technical_blocking_findings": [],
        "metrics": policy.expected,
        "proposed_policy": policy.proposed_policy,
        "range_decisions": range_rows,
        "historical_count_is_not_projected_mask_count": True,
        "clinical_rows_read": 0,
        "clinical_data_written_or_modified": False,
        "existing_releases_modified": False,
        "policy_0_2_activated": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(payload)
    output = config.reports_root / "review" / policy.review_directory_name
    output.mkdir(parents=True, mode=0o750, exist_ok=True)
    output.chmod(0o750)
    json_path = output / f"{selected}.json"
    markdown_path = output / f"{selected}.md"
    if json_path.exists() or markdown_path.exists():
        raise HarmonizationError("Cleaning 0.2 policy-review output already exists")
    _write_exclusive(
        json_path,
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        0o640,
    )
    _write_exclusive(markdown_path, _markdown(payload), 0o640)
    return Cleaning02PolicyReviewResult(
        run_id=selected,
        blocking_findings=(blocker,),
        technical_blocking_findings=(),
        review_json_path=json_path,
        review_markdown_path=markdown_path,
    )
