from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Callable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.cleaning.pipeline import (
    CleaningPolicy,
    CleaningRule,
    _bool_count,
    _inside_range,
    _load_rule_registry,
    load_cleaning_policy,
    transform_cleaning_batch,
)
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build import (
    _load_frozen_schemas,
    _read_json,
)
from asic_pipeline.harmonization.harmonized_build_audit import (
    _next_slice,
    _parquet_schema_matches,
    _require_output_exhausted,
    load_harmonized_build_audit_policy,
)
from asic_pipeline.harmonization.harmonized_promotion import (
    load_harmonized_promotion_policy,
)
from asic_pipeline.harmonization.schema_dictionary_freeze import (
    load_schema_dictionary_freeze_policy,
)
from asic_pipeline.harmonization.harmonized_build import (
    load_harmonized_build_policy,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.harmonized_0_2 import (
    _target_schemas,
    load_harmonized_0_2_audit_policy,
    load_harmonized_0_2_build_policy,
)
from asic_pipeline.unit_resolution.harmonized_0_2_promotion import (
    Harmonized02PromotionConfig,
    load_harmonized_0_2_promotion_config,
    load_harmonized_0_2_promotion_policy,
)
from asic_pipeline.unit_resolution.reviewed_decisions import (
    load_reviewed_unit_decisions,
)
from asic_pipeline.unit_resolution.reviewed_medication_semantics import (
    load_reviewed_medication_semantics,
)


EXPECTED_ACTIONS = frozenset(
    {
        "mask_outside_open_closed_range",
        "mask_nonpositive_preserve_positive_extremes",
        "recover_unique_power_of_ten_else_mask_outside_open_closed_range",
        "mask_nonpositive_preserve_above_review_threshold",
        "require_integer_closed_range",
    }
)
EXPECTED_VARIABLES = (
    "albumin",
    "creatinine",
    "evlwi",
    "gedvi",
    "hemoglobin",
    "inr",
    "lactate_art",
    "platelets",
    "ptt",
    "sofa_score_unspecified",
    "sofa_score_without_gcs",
    "svri",
)
EXPECTED_BOUNDARY = {
    "read_released_harmonized_0_2": True,
    "read_frozen_contract_0_2": True,
    "modify_harmonized_release": False,
    "modify_existing_cleaned_release": False,
    "modify_existing_derived_release": False,
    "overwrite_outputs": False,
    "filter_rows": False,
    "filter_stays": False,
    "drop_columns": False,
    "aggregate_height_measurements": False,
    "derive_variables": False,
    "define_cohorts": False,
    "perform_time_blocking": False,
    "approve_release_automatically": False,
    "update_release_pointer": False,
    "authorize_external_data_export": False,
}
EXPECTED_EXECUTION = {
    "replay_every_cleaning_0_1_rule": True,
    "preserve_cleaning_0_1_rule_semantics": True,
    "mask_nonfinite_in_every_float64_field": True,
    "apply_general_negative_medication_rule": True,
    "preserve_medication_zero": True,
    "preserve_medication_null": True,
    "prohibit_null_to_zero_imputation": True,
    "preserve_rows_and_stays": True,
    "preserve_columns_and_order": True,
    "create_cleaned_dictionary_annotations": True,
    "create_derived_variables": False,
    "perform_time_blocking": False,
}
EXPECTED_AUDIT = {
    "verify_source_hashes_before_and_after": True,
    "verify_candidate_hashes": True,
    "compare_every_output_cell": True,
    "recompute_cleaning_0_1_rules": True,
    "recompute_all_twelve_priority_rules": True,
    "recompute_general_negative_medication_rule": True,
    "verify_preserved_positive_extremes": True,
    "verify_zero_null_and_positive_medication_conservation": True,
    "verify_cleaned_dictionary_annotations": True,
    "verify_rows_schema_order_and_provenance": True,
    "verify_rule_level_counts": True,
}
EXPECTED_AUDIT_BOUNDARY = {
    "read_released_harmonized_0_2": True,
    "read_cleaned_0_2_candidate": True,
    "modify_clinical_data": False,
    "write_reports_only": True,
    "approve_release_automatically": False,
    "update_release_pointer": False,
    "publish": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class Cleaning02Rule:
    rule_id: str
    variable: str
    action: str
    unit: str
    hard_min: float | None
    hard_max: float | None
    review_threshold: float | None
    integer_tolerance: float | None
    factors: tuple[float, ...]
    rationale: str


@dataclass(frozen=True)
class Cleaning02Policy:
    version: str
    release_id: str
    contract_version: str
    promotion_policy_path: Path
    promotion_policy_sha256: str
    contract_directory: Path
    freeze_policy_path: Path
    freeze_policy_sha256: str
    baseline_policy_path: Path
    baseline_policy_sha256: str
    unit_decisions_path: Path
    unit_decisions_sha256: str
    medication_semantics_path: Path
    medication_semantics_sha256: str
    range_policy_path: Path
    range_policy_sha256: str
    range_run_id: str
    range_generated_at: str
    range_artifact_version: str
    range_expected: dict[str, int]
    approval: dict[str, str]
    rules: tuple[Cleaning02Rule, ...]
    expected: dict[str, int]
    rows_per_batch: int
    compression: str
    candidate_directory_name: str
    build_review_directory_name: str
    artifact_version: str
    candidate_status: str
    dictionary_filename: str
    dictionary_markdown_filename: str
    source_path: Path


@dataclass(frozen=True)
class Cleaning02AuditPolicy:
    version: str
    build_policy_path: Path
    build_policy_sha256: str
    rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class Cleaning02BuildConfig:
    promotion: Harmonized02PromotionConfig
    source_config_path: Path
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.promotion.dataset_context

    @property
    def data_root(self) -> Path:
        return self.promotion.data_root

    @property
    def reports_root(self) -> Path:
        return self.promotion.reports_root


@dataclass(frozen=True)
class Cleaning02AuditConfig:
    build: Cleaning02BuildConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.build.dataset_context

    @property
    def data_root(self) -> Path:
        return self.build.data_root

    @property
    def reports_root(self) -> Path:
        return self.build.reports_root


@dataclass(frozen=True)
class Cleaning02Input:
    release_manifest_path: Path
    release_manifest: dict[str, Any]
    source_files: dict[str, Path]
    source_schemas: dict[str, pa.Schema]
    contract_manifest_path: Path
    contract_manifest: dict[str, Any]
    dictionary_path: Path


@dataclass(frozen=True)
class Cleaning02BuildResult:
    run_id: str
    candidate_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class Cleaning02AuditResult:
    run_id: str
    build_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _nonnegative_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location} must be a nonnegative integer")
    return value


def _optional_number(value: Any, location: str) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigurationError(f"{location} must be numeric or null")
    result = float(value)
    if not math.isfinite(result):
        raise ConfigurationError(f"{location} must be finite")
    return result


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


def _write_text(path: Path, value: str, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(value)
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _write_parquet(path: Path, table: pa.Table, compression: str, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    if temporary.exists():
        raise HarmonizationError(f"Temporary output already exists: {temporary}")
    pq.write_table(table, temporary, compression=compression)
    temporary.chmod(mode)
    temporary.replace(path)
    path.chmod(mode)


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def load_cleaning_0_2_build_config(
    path: str | Path,
) -> Cleaning02BuildConfig:
    source = Path(path).expanduser().resolve()
    promotion = load_harmonized_0_2_promotion_config(source)
    raw = load_yaml_mapping(source, "Cleaning 0.2 build configuration")
    policy_path = resolve_path(
        required_string(raw, "cleaning_0_2_policy", "config"), source
    )
    return Cleaning02BuildConfig(
        promotion=promotion,
        source_config_path=source,
        policy_path=policy_path,
    )


def load_cleaning_0_2_audit_config(
    path: str | Path,
) -> Cleaning02AuditConfig:
    source = Path(path).expanduser().resolve()
    build = load_cleaning_0_2_build_config(source)
    raw = load_yaml_mapping(source, "Cleaning 0.2 audit configuration")
    policy_path = resolve_path(
        required_string(raw, "cleaning_0_2_audit_policy", "config"), source
    )
    return Cleaning02AuditConfig(build=build, policy_path=policy_path)


def _load_rule(value: Any, location: str) -> Cleaning02Rule:
    row = _mapping(value, location)
    action = required_string(row, "action", location)
    if action not in EXPECTED_ACTIONS:
        raise ConfigurationError(f"{location}.action is not approved")
    factors = row.get("factors", [])
    if not isinstance(factors, list) or any(
        not isinstance(item, (int, float)) or isinstance(item, bool)
        for item in factors
    ):
        raise ConfigurationError(f"{location}.factors must be numeric")
    rule = Cleaning02Rule(
        rule_id=required_string(row, "id", location),
        variable=required_string(row, "variable", location),
        action=action,
        unit=required_string(row, "unit", location),
        hard_min=_optional_number(
            row.get("hard_min_exclusive", row.get("hard_min_inclusive")),
            f"{location}.hard_min",
        ),
        hard_max=_optional_number(row.get("hard_max_inclusive"), f"{location}.hard_max"),
        review_threshold=_optional_number(
            row.get("review_threshold"), f"{location}.review_threshold"
        ),
        integer_tolerance=_optional_number(
            row.get("integer_tolerance"), f"{location}.integer_tolerance"
        ),
        factors=tuple(float(item) for item in factors),
        rationale=required_string(row, "rationale", location),
    )
    if action == "recover_unique_power_of_ten_else_mask_outside_open_closed_range":
        if (
            rule.hard_min != 0.0
            or rule.hard_max is None
            or rule.factors != (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0)
        ):
            raise ConfigurationError("EVLWI recovery rule changed")
    elif rule.factors:
        raise ConfigurationError(f"{location} unexpectedly defines factors")
    if action == "mask_nonpositive_preserve_above_review_threshold" and (
        rule.hard_min != 0.0 or rule.review_threshold is None
    ):
        raise ConfigurationError(f"{location} preserve-above rule is incomplete")
    if action == "require_integer_closed_range" and (
        rule.hard_min is None
        or rule.hard_max is None
        or rule.integer_tolerance is None
    ):
        raise ConfigurationError(f"{location} score rule is incomplete")
    return rule


def load_cleaning_0_2_policy(path: str | Path) -> Cleaning02Policy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed cleaning 0.2 policy")
    if (
        raw.get("cleaning_policy_version") != "0.2"
        or raw.get("status")
        != "human_approved_for_nonpublishable_candidate_and_complete_audit"
    ):
        raise ConfigurationError("Cleaning 0.2 approval is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    evidence = _mapping(raw.get("range_evidence"), "range_evidence")
    human = _mapping(raw.get("human_decision"), "human_decision")
    if _mapping(raw.get("execution"), "execution") != EXPECTED_EXECUTION:
        raise ConfigurationError("Cleaning 0.2 execution boundary changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Cleaning 0.2 safety boundary changed")
    rules_raw = raw.get("priority_rules")
    if not isinstance(rules_raw, list):
        raise ConfigurationError("priority_rules must be a list")
    rules = tuple(
        _load_rule(value, f"priority_rules[{index}]")
        for index, value in enumerate(rules_raw)
    )
    if tuple(rule.variable for rule in rules) != EXPECTED_VARIABLES:
        raise ConfigurationError("Cleaning 0.2 variable order changed")
    paths_and_hashes = (
        ("promotion_policy_path", "harmonized_promotion_policy", "harmonized_promotion_policy_sha256"),
        ("freeze_policy_path", "frozen_contract_policy", "frozen_contract_policy_sha256"),
        ("baseline_policy_path", "baseline_cleaning_policy", "baseline_cleaning_policy_sha256"),
        ("unit_decisions_path", "reviewed_unit_decisions", "reviewed_unit_decisions_sha256"),
        ("medication_semantics_path", "reviewed_medication_semantics", "reviewed_medication_semantics_sha256"),
    )
    resolved: dict[str, Any] = {}
    for attribute, path_key, hash_key in paths_and_hashes:
        resolved[attribute] = resolve_path(
            required_string(input_raw, path_key, "input"), source
        )
        expected_hash = required_string(input_raw, hash_key, "input")
        if sha256_file(resolved[attribute]) != expected_hash:
            raise ConfigurationError(f"Immutable cleaning input changed: {path_key}")
        resolved[attribute.replace("_path", "_sha256")] = expected_hash
    range_policy_path = resolve_path(
        required_string(evidence, "policy", "range_evidence"), source
    )
    range_policy_hash = required_string(
        evidence, "policy_sha256", "range_evidence"
    )
    if sha256_file(range_policy_path) != range_policy_hash:
        raise ConfigurationError("Range-evidence policy changed")
    expected_raw = _mapping(raw.get("expected"), "expected")
    expected = {
        key: _nonnegative_int(value, f"expected.{key}")
        for key, value in expected_raw.items()
    }
    expected_keys = {
        "static_rows",
        "dynamic_rows",
        "static_clinical_variables",
        "dynamic_clinical_variables",
        "provenance_fields_per_table",
        "baseline_legacy_applied_rules",
        "baseline_additional_scalar_rules",
        "baseline_deferred_rules_replaced",
        "baseline_list_rules",
        "baseline_hospital_masks",
        "priority_rules",
        "medication_or_therapy_variables",
        "known_priority_zero_masks",
        "known_priority_negative_masks",
        "expected_evlwi_factor_0_001_corrections",
        "expected_evlwi_ambiguous_masks",
        "expected_positive_upper_values_preserved",
        "expected_negative_medication_masks",
        "expected_current_score_violations",
    }
    if set(expected) != expected_keys or expected["priority_rules"] != len(rules):
        raise ConfigurationError("Cleaning 0.2 expected accounting changed")
    range_expected = {
        "dynamic_rows": _positive_int(
            evidence.get("expected_dynamic_rows"),
            "range_evidence.expected_dynamic_rows",
        ),
        "hospital_profiles": _positive_int(
            evidence.get("expected_hospital_profiles"),
            "range_evidence.expected_hospital_profiles",
        ),
        "legacy_findings": _positive_int(
            evidence.get("expected_legacy_findings"),
            "range_evidence.expected_legacy_findings",
        ),
        "zeros": _positive_int(evidence.get("expected_zeros"), "range_evidence.expected_zeros"),
        "negatives": _positive_int(evidence.get("expected_negatives"), "range_evidence.expected_negatives"),
        "above_upper": _positive_int(evidence.get("expected_above_upper"), "range_evidence.expected_above_upper"),
        "fractional_scores": _nonnegative_int(
            evidence.get("expected_fractional_scores"),
            "range_evidence.expected_fractional_scores",
        ),
    }
    streaming = _mapping(raw.get("streaming"), "streaming")
    outputs = _mapping(raw.get("outputs"), "outputs")
    policy = Cleaning02Policy(
        version="0.2",
        release_id=required_string(input_raw, "harmonized_release_id", "input"),
        contract_version=required_string(
            input_raw, "harmonized_contract_version", "input"
        ),
        contract_directory=Path(
            required_string(input_raw, "frozen_contract_directory", "input")
        ),
        range_policy_path=range_policy_path,
        range_policy_sha256=range_policy_hash,
        range_run_id=required_string(evidence, "run_id", "range_evidence"),
        range_generated_at=required_string(
            evidence, "generated_at_utc", "range_evidence"
        ),
        range_artifact_version=required_string(
            evidence, "artifact_version", "range_evidence"
        ),
        range_expected=range_expected,
        approval={
            "decision_date": required_string(human, "decision_date", "human_decision"),
            "reviewer_role": required_string(human, "reviewer_role", "human_decision"),
            "decision": required_string(human, "decision", "human_decision"),
            "approval_record": required_string(human, "approval_record", "human_decision"),
        },
        rules=rules,
        expected=expected,
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        compression=required_string(
            streaming, "parquet_compression", "streaming"
        ),
        candidate_directory_name=required_string(
            outputs, "candidate_directory_name", "outputs"
        ),
        build_review_directory_name=required_string(
            outputs, "build_review_directory_name", "outputs"
        ),
        artifact_version=required_string(
            outputs, "candidate_artifact_version", "outputs"
        ),
        candidate_status=required_string(outputs, "candidate_status", "outputs"),
        dictionary_filename=required_string(
            outputs, "cleaned_dictionary_filename", "outputs"
        ),
        dictionary_markdown_filename=required_string(
            outputs, "cleaned_dictionary_markdown_filename", "outputs"
        ),
        source_path=source,
        **resolved,
    )
    if (
        policy.release_id != "20260807T112402Z"
        or policy.contract_version != "0.2"
        or policy.range_run_id != "20260807T133609Z"
        or policy.range_generated_at != "2026-08-07T13:37:29+00:00"
        or policy.approval["decision_date"] != "2026-08-07"
        or policy.approval["reviewer_role"] != "data_owner"
        or policy.artifact_version != "0.2"
    ):
        raise ConfigurationError("Cleaning 0.2 approved scope changed")
    return policy


def load_cleaning_0_2_audit_policy(
    path: str | Path,
) -> Cleaning02AuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning 0.2 audit policy")
    if (
        raw.get("cleaning_0_2_audit_policy_version") != "0.1"
        or raw.get("status") != "approved_for_complete_independent_read_only_audit"
    ):
        raise ConfigurationError("Cleaning 0.2 audit policy is invalid")
    build_path = resolve_path(required_string(raw, "build_policy", "audit"), source)
    build_hash = required_string(raw, "build_policy_sha256", "audit")
    if sha256_file(build_path) != build_hash:
        raise ConfigurationError("Cleaning 0.2 build policy changed")
    audit = _mapping(raw.get("audit"), "audit")
    if {key: audit.get(key) for key in EXPECTED_AUDIT} != EXPECTED_AUDIT:
        raise ConfigurationError("Cleaning 0.2 audit coverage changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_AUDIT_BOUNDARY:
        raise ConfigurationError("Cleaning 0.2 audit boundary changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    return Cleaning02AuditPolicy(
        version="0.1",
        build_policy_path=build_path,
        build_policy_sha256=build_hash,
        rows_per_batch=_positive_int(audit.get("rows_per_batch"), "audit.rows_per_batch"),
        private_directory_name=required_string(
            outputs, "private_directory_name", "outputs"
        ),
        review_directory_name=required_string(
            outputs, "review_directory_name", "outputs"
        ),
        private_artifact_version=required_string(
            outputs, "private_artifact_version", "outputs"
        ),
        review_artifact_version=required_string(
            outputs, "review_artifact_version", "outputs"
        ),
        source_path=source,
    )


def _validate_range_evidence(
    config: Cleaning02BuildConfig,
    policy: Cleaning02Policy,
) -> dict[str, Any]:
    review_path = (
        config.reports_root
        / "review"
        / "cleaning_range_evidence_0_2"
        / f"{policy.range_run_id}.json"
    )
    private_dir = (
        config.reports_root
        / "private"
        / "cleaning_range_evidence_0_2"
        / policy.range_run_id
    )
    private_manifest_path = private_dir / "audit_manifest.json"
    profiles_path = private_dir / "hospital_variable_profiles.parquet"
    frequencies_path = private_dir / "decision_relevant_value_frequencies.parquet"
    review = _read_json(review_path, "Approved cleaning range-evidence review")
    private = _read_json(
        private_manifest_path, "Approved private cleaning range evidence"
    )
    if not isinstance(review, dict) or not isinstance(private, dict):
        raise HarmonizationError("Approved range evidence is invalid")
    metrics = review.get("metrics")
    expected_metrics = {
        "dynamic_rows_scanned": policy.range_expected["dynamic_rows"],
        "hospital_variable_profile_count": policy.range_expected[
            "hospital_profiles"
        ],
        "legacy_rule_violation_count_after_harmonization_0_2": (
            policy.range_expected["legacy_findings"]
        ),
        "negative_count": policy.range_expected["negatives"],
        "zero_count": policy.range_expected["zeros"],
        "above_upper_count": policy.range_expected["above_upper"],
        "fractional_score_count": policy.range_expected["fractional_scores"],
        "technical_blocking_finding_count": 0,
    }
    review_expected = {
        "artifact": "asic_v3_cleaning_range_evidence_0_2_review",
        "artifact_version": policy.range_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": policy.range_generated_at,
        "run_id": policy.range_run_id,
        "harmonized_release_id": policy.release_id,
        "harmonized_contract_version": policy.contract_version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "technical_blocking_findings": [],
        "clinical_data_written_or_modified": False,
        "cleaning_rules_activated": False,
        "publication_ready": False,
    }
    review_mismatches = [
        key for key, expected in review_expected.items() if review.get(key) != expected
    ]
    blockers = review.get("blocking_findings")
    if not (
        isinstance(blockers, list)
        and len(blockers) == 1
        and isinstance(blockers[0], dict)
        and blockers[0].get("check")
        == "cleaning_0_2_range_dispositions_approved"
    ):
        review_mismatches.append("blocking_findings")
    if not isinstance(metrics, dict):
        review_mismatches.append("metrics")
    else:
        review_mismatches.extend(
            f"metrics.{key}"
            for key, expected in expected_metrics.items()
            if metrics.get(key) != expected
        )
    if review_mismatches:
        raise HarmonizationError(
            "Approved range-evidence review changed: "
            + ", ".join(sorted(review_mismatches))
        )
    if (
        private.get("artifact")
        != "asic_v3_cleaning_range_evidence_0_2_private"
        or private.get("artifact_version") != policy.range_artifact_version
        or private.get("dataset_context") != config.dataset_context
        or private.get("generated_at_utc") != policy.range_generated_at
        or private.get("run_id") != policy.range_run_id
        or private.get("harmonized_release_id") != policy.release_id
        or private.get("metrics") != metrics
        or private.get("complete_frequency_evidence") is not True
        or private.get("patient_rows_or_identifiers_written") is not False
        or private.get("clinical_data_modified") is not False
        or not profiles_path.is_file()
        or not frequencies_path.is_file()
        or pq.ParquetFile(profiles_path).metadata.num_rows
        != policy.range_expected["hospital_profiles"]
    ):
        raise HarmonizationError("Approved private range evidence changed")
    evidence_lineage = {
        "review_path": str(review_path),
        "review_sha256": sha256_file(review_path),
        "private_manifest_path": str(private_manifest_path),
        "private_manifest_sha256": sha256_file(private_manifest_path),
        "profiles_sha256": sha256_file(profiles_path),
        "frequencies_sha256": sha256_file(frequencies_path),
    }
    # Validate manifest compatibility before any expensive clinical-data scan.
    json.dumps(evidence_lineage, allow_nan=False)
    return evidence_lineage


def _baseline_registry(
    config: Cleaning02BuildConfig,
    policy: Cleaning02Policy,
) -> tuple[
    CleaningPolicy,
    dict[tuple[str, str], CleaningRule],
    dict[tuple[str, str], CleaningRule],
]:
    baseline = load_cleaning_policy(policy.baseline_policy_path)
    promotion = load_harmonized_promotion_policy(baseline.promotion_policy_path)
    audit_policy = load_harmonized_build_audit_policy(promotion.audit_policy_path)
    build_policy = load_harmonized_build_policy(audit_policy.build_policy_path)
    freeze_policy = load_schema_dictionary_freeze_policy(
        build_policy.freeze_policy_path
    )
    schemas, _ = _load_frozen_schemas(
        config.data_root / build_policy.frozen_contract_directory,
        freeze_policy,
    )
    applied, deferred = _load_rule_registry(baseline, schemas)
    if (
        len(applied)
        != policy.expected["baseline_legacy_applied_rules"]
        + policy.expected["baseline_additional_scalar_rules"]
        or len(deferred) != policy.expected["baseline_deferred_rules_replaced"]
        or set(variable for _, variable in deferred) != set(EXPECTED_VARIABLES)
        or len(baseline.list_rules) != policy.expected["baseline_list_rules"]
        or len(baseline.hospital_masks)
        != policy.expected["baseline_hospital_masks"]
    ):
        raise HarmonizationError("Cleaning 0.1 replay registry changed")
    return baseline, applied, deferred


def _validate_release_file(
    release_dir: Path,
    manifest: dict[str, Any],
    table: str,
    schema: pa.Schema,
    expected_rows: int,
) -> Path:
    summaries = manifest.get("files")
    summary = summaries.get(table) if isinstance(summaries, dict) else None
    path = release_dir / f"{table}.parquet"
    if not isinstance(summary, dict) or not path.is_file():
        raise HarmonizationError(f"Released harmonized 0.2 {table} is unavailable")
    parquet = pq.ParquetFile(path)
    if (
        summary.get("sha256") != sha256_file(path)
        or summary.get("row_count") != expected_rows
        or parquet.metadata.num_rows != expected_rows
        or summary.get("schema_sha256") != _schema_digest(schema)
        or not _parquet_schema_matches(parquet.schema_arrow, schema)
    ):
        raise HarmonizationError(f"Released harmonized 0.2 {table} changed")
    return path


def _load_input(
    config: Cleaning02BuildConfig,
    policy: Cleaning02Policy,
) -> Cleaning02Input:
    if config.promotion.policy_path.resolve() != policy.promotion_policy_path.resolve():
        raise HarmonizationError("Cleaning and promotion policies disagree")
    promotion = load_harmonized_0_2_promotion_policy(policy.promotion_policy_path)
    audit_policy = load_harmonized_0_2_audit_policy(promotion.audit_policy_path)
    build_policy = load_harmonized_0_2_build_policy(audit_policy.build_policy_path)
    schemas, contract_manifest = _target_schemas(
        config.promotion.audit.build, build_policy
    )
    if (
        promotion.release_id != policy.release_id
        or promotion.contract_version != policy.contract_version
        or build_policy.target_contract_directory != policy.contract_directory
        or config.promotion.audit.build.policy_path.resolve()
        != audit_policy.build_policy_path.resolve()
    ):
        raise HarmonizationError("Cleaning 0.2 release lineage changed")
    pointer_path = config.data_root / promotion.current_release_pointer
    pointer = _read_json(pointer_path, "Current harmonized 0.2 pointer")
    release_dir = config.data_root / promotion.releases_directory / policy.release_id
    manifest_path = release_dir / "release_manifest.json"
    if not isinstance(pointer, dict) or (
        pointer.get("artifact") != "asic_v3_current_harmonized_release"
        or pointer.get("release_id") != policy.release_id
        or pointer.get("frozen_contract_version") != policy.contract_version
        or Path(str(pointer.get("release_manifest", ""))).resolve()
        != manifest_path.resolve()
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
        or pointer.get("harmonized_layer_ready") is not True
        or pointer.get("cleaning_input_approved") is not True
        or pointer.get("publication_ready") is not True
        or pointer.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Current harmonized 0.2 pointer changed")
    manifest = _read_json(manifest_path, "Released harmonized 0.2 manifest")
    if not isinstance(manifest, dict) or (
        manifest.get("artifact") != "asic_v3_harmonized_release"
        or manifest.get("artifact_version") != "0.2"
        or manifest.get("release_id") != policy.release_id
        or manifest.get("status") != "released_harmonized_layer"
        or manifest.get("frozen_contract_version") != policy.contract_version
        or manifest.get("payload_byte_identity_preserved") is not True
        or manifest.get("harmonized_layer_ready") is not True
        or manifest.get("cleaning_input_approved") is not True
        or manifest.get("cleaning_applied") is not False
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not True
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Released harmonized 0.2 manifest changed")
    source_files = {
        table: _validate_release_file(
            release_dir,
            manifest,
            table,
            schemas[table],
            policy.expected[f"{table}_rows"],
        )
        for table in ("static", "dynamic")
    }
    contract_dir = config.data_root / policy.contract_directory
    contract_manifest_path = contract_dir / "freeze_manifest.json"
    if (
        sha256_file(policy.freeze_policy_path) != policy.freeze_policy_sha256
        or not contract_manifest_path.is_file()
        or contract_manifest.get("contract_version") != policy.contract_version
        or contract_manifest.get("schema_frozen") is not True
        or contract_manifest.get("dictionary_frozen") is not True
    ):
        raise HarmonizationError("Frozen harmonized 0.2 contract changed")
    dictionary_path = contract_dir / "variable_dictionary.parquet"
    files = contract_manifest.get("files")
    if (
        not isinstance(files, dict)
        or files.get(dictionary_path.name) != sha256_file(dictionary_path)
    ):
        raise HarmonizationError("Frozen harmonized 0.2 dictionary changed")
    return Cleaning02Input(
        release_manifest_path=manifest_path,
        release_manifest=manifest,
        source_files=source_files,
        source_schemas=schemas,
        contract_manifest_path=contract_manifest_path,
        contract_manifest=contract_manifest,
        dictionary_path=dictionary_path,
    )


def _cleaned_schema(source: pa.Schema, contract_version: str) -> pa.Schema:
    return pa.schema(
        list(source),
        metadata={
            b"asic_v3_stage": b"cleaned_0_2_candidate",
            b"asic_v3_harmonized_contract_version": contract_version.encode(),
            b"asic_v3_cleaning_policy_version": b"0.2",
            b"publication_ready": b"false",
            b"cleaning_applied": b"true",
            b"derivation_applied": b"false",
        },
    )


def _finite_masks(values: pa.Array) -> tuple[pa.Array, pa.Array, pa.Array, pa.Array]:
    finite = pc.fill_null(pc.is_finite(values), False)
    negative = pc.and_(finite, pc.less(values, 0.0))
    zero = pc.and_(finite, pc.equal(values, 0.0))
    positive = pc.and_(finite, pc.greater(values, 0.0))
    return finite, negative, zero, positive


def _apply_priority_rule(
    source: pa.Array,
    rule: Cleaning02Rule,
    counts: Counter[str],
) -> pa.Array:
    values = pc.cast(source, pa.float64(), safe=True)
    finite, negative, zero, positive = _finite_masks(values)
    counts["input_null"] += values.null_count
    counts["finite"] += _bool_count(finite)
    counts["negative"] += _bool_count(negative)
    counts["zero"] += _bool_count(zero)
    counts["positive"] += _bool_count(positive)
    if rule.action in {
        "mask_nonpositive_preserve_positive_extremes",
        "mask_nonpositive_preserve_above_review_threshold",
    }:
        counts["masked_nonpositive"] += _bool_count(pc.or_(negative, zero))
        if rule.review_threshold is not None:
            above = pc.and_(positive, pc.greater(values, rule.review_threshold))
            counts["preserved_above_review_threshold"] += _bool_count(above)
        return pc.if_else(positive, values, pa.scalar(None, pa.float64()))
    if rule.action == "mask_outside_open_closed_range":
        if rule.hard_max is None:
            raise HarmonizationError("Approved bounded rule lost its upper limit")
        within = pc.and_(positive, pc.less_equal(values, rule.hard_max))
        above = pc.and_(positive, pc.greater(values, rule.hard_max))
        counts["kept_in_range"] += _bool_count(within)
        counts["masked_nonpositive"] += _bool_count(pc.or_(negative, zero))
        counts["masked_above_upper"] += _bool_count(above)
        return pc.if_else(within, values, pa.scalar(None, pa.float64()))
    if (
        rule.action
        == "recover_unique_power_of_ten_else_mask_outside_open_closed_range"
    ):
        if rule.hard_max is None:
            raise HarmonizationError("EVLWI rule lost its upper limit")
        within = pc.and_(positive, pc.less_equal(values, rule.hard_max))
        above = pc.and_(positive, pc.greater(values, rule.hard_max))
        candidate_total = pa.repeat(pa.scalar(0, pa.int8()), len(values))
        candidates: list[tuple[float, pa.Array, pa.Array]] = []
        for factor in rule.factors:
            transformed = pc.multiply(values, factor)
            candidate = pc.and_(
                above,
                pc.and_(
                    pc.greater(transformed, 0.0),
                    pc.less_equal(transformed, rule.hard_max),
                ),
            )
            candidate_total = pc.add(candidate_total, pc.cast(candidate, pa.int8()))
            candidates.append((factor, candidate, transformed))
        unique = pc.and_(above, pc.equal(candidate_total, 1))
        ambiguous = pc.and_(above, pc.greater(candidate_total, 1))
        no_factor = pc.and_(above, pc.equal(candidate_total, 0))
        output = values
        for factor, candidate, transformed in candidates:
            selected = pc.and_(unique, candidate)
            selected_count = _bool_count(selected)
            counts[f"corrected_factor_{factor:g}"] += selected_count
            output = pc.if_else(selected, transformed, output)
        counts["kept_in_range"] += _bool_count(within)
        counts["masked_nonpositive"] += _bool_count(pc.or_(negative, zero))
        counts["masked_ambiguous_upper"] += _bool_count(ambiguous)
        counts["masked_unrecoverable_upper"] += _bool_count(no_factor)
        keep = pc.or_(within, unique)
        return pc.if_else(keep, output, pa.scalar(None, pa.float64()))
    if rule.action == "require_integer_closed_range":
        if (
            rule.hard_min is None
            or rule.hard_max is None
            or rule.integer_tolerance is None
        ):
            raise HarmonizationError("Approved score rule is incomplete")
        rounded = pc.round(values, ndigits=0)
        integer = pc.less_equal(
            pc.abs(pc.subtract(values, rounded)), rule.integer_tolerance
        )
        in_range = pc.and_(
            finite,
            pc.and_(
                pc.greater_equal(values, rule.hard_min),
                pc.less_equal(values, rule.hard_max),
            ),
        )
        keep = pc.and_(in_range, integer)
        counts["kept_score"] += _bool_count(keep)
        counts["masked_fractional_score"] += _bool_count(
            pc.and_(finite, pc.invert(integer))
        )
        counts["masked_score_outside_range"] += _bool_count(
            pc.and_(finite, pc.invert(in_range))
        )
        return pc.if_else(keep, values, pa.scalar(None, pa.float64()))
    raise HarmonizationError(f"Unknown approved cleaning action: {rule.action}")


def _mask_negative_medication(
    source: pa.Array,
    hospitals: pa.Array,
    variable: str,
    counts: dict[str, Counter[str]],
) -> pa.Array:
    values = pc.cast(source, pa.float64(), safe=True)
    finite, negative, zero, positive = _finite_masks(values)
    output = pc.if_else(negative, pa.scalar(None, pa.float64()), values)
    summary = counts[variable]
    summary["input_null"] += values.null_count
    summary["input_zero"] += _bool_count(zero)
    summary["input_positive"] += _bool_count(positive)
    summary["masked_negative"] += _bool_count(negative)
    summary["output_null"] += output.null_count
    summary["output_zero"] += _bool_count(pc.and_(finite, pc.equal(output, 0.0)))
    summary["output_positive"] += _bool_count(pc.and_(finite, pc.greater(output, 0.0)))
    for hospital in sorted(set(str(value) for value in hospitals.to_pylist())):
        site = pc.fill_null(pc.equal(hospitals, hospital), False)
        site_negative = pc.and_(site, negative)
        count = _bool_count(site_negative)
        if count:
            summary[f"masked_negative_hospital_{hospital}"] += count
    return output


def _reviewed_medication_variables(decisions: Any) -> set[str]:
    """Return the complete 0.2 medication scope, including split targets."""
    source_variables = {str(value) for value in decisions.medication_variables}
    split_targets = {
        str(row["target_variable"]) for row in decisions.semantic_splits
    }
    overlap = source_variables & split_targets
    result = source_variables | split_targets
    if overlap or len(source_variables) != 25 or len(split_targets) != 9 or len(result) != 34:
        raise HarmonizationError(
            "Reviewed medication/therapy scope no longer contains 25 source "
            "variables and nine distinct semantic-split targets"
        )
    return result


def _transform_batch(
    batch: pa.RecordBatch,
    table: str,
    output_schema: pa.Schema,
    baseline: CleaningPolicy,
    baseline_applied: dict[tuple[str, str], CleaningRule],
    policy: Cleaning02Policy,
    medication_variables: set[str],
    baseline_rule_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    hospital_mask_counts: dict[str, Counter[str]],
    priority_counts: dict[str, Counter[str]],
    medication_counts: dict[str, Counter[str]],
) -> pa.RecordBatch:
    base = transform_cleaning_batch(
        batch,
        table,
        output_schema,
        baseline,
        baseline_applied,
        {},
        baseline_rule_counts,
        {},
        nonfinite_counts,
        list_counts,
        hospital_mask_counts,
    )
    hospitals = base.column(base.schema.get_field_index("hospital_id"))
    rules = {rule.variable: rule for rule in policy.rules} if table == "dynamic" else {}
    arrays: list[pa.Array] = []
    for field in output_schema:
        values = base.column(base.schema.get_field_index(field.name))
        rule = rules.get(field.name)
        if rule is not None:
            values = _apply_priority_rule(
                values, rule, priority_counts[rule.rule_id]
            )
        if table == "dynamic" and field.name in medication_variables:
            values = _mask_negative_medication(
                values, hospitals, field.name, medication_counts
            )
        if values.type != field.type:
            values = pc.cast(values, field.type, safe=True)
        arrays.append(values)
    return pa.RecordBatch.from_arrays(arrays, schema=output_schema)


def _domain_text(rule: Cleaning02Rule) -> str:
    if rule.action == "mask_outside_open_closed_range":
        return f"0 < value <= {rule.hard_max:g} {rule.unit}"
    if rule.action == "mask_nonpositive_preserve_positive_extremes":
        return f"value > 0 {rule.unit}; no hard positive ceiling"
    if rule.action == "mask_nonpositive_preserve_above_review_threshold":
        return (
            f"value > 0 {rule.unit}; values above {rule.review_threshold:g} "
            "are retained and audit-reported"
        )
    if (
        rule.action
        == "recover_unique_power_of_ten_else_mask_outside_open_closed_range"
    ):
        return (
            f"0 < value <= {rule.hard_max:g} {rule.unit} after at most one "
            "uniquely determined power-of-ten repair"
        )
    if rule.action == "require_integer_closed_range":
        return f"integer {rule.hard_min:g} <= value <= {rule.hard_max:g}"
    raise AssertionError(rule.action)


def _baseline_domain(rule: CleaningRule) -> str:
    parts: list[str] = []
    if rule.hard_min is not None:
        parts.append(f"value >= {rule.hard_min:g}")
    if rule.hard_max is not None:
        parts.append(f"value <= {rule.hard_max:g}")
    if rule.invalid_zero:
        parts.append("value != 0")
    return "; ".join(parts) if parts else "finite values"


def _dictionary_annotations(
    dictionary: pa.Table,
    policy: Cleaning02Policy,
    baseline: CleaningPolicy,
    baseline_applied: dict[tuple[str, str], CleaningRule],
    medication_variables: set[str],
) -> pa.Table:
    rows = dictionary.to_pylist()
    priority = {rule.variable: rule for rule in policy.rules}
    list_rules = {(rule.table, rule.variable): rule for rule in baseline.list_rules}
    hospital_masks = {
        (rule.table, rule.variable): rule for rule in baseline.hospital_masks
    }
    cleaning_rule_ids: list[list[str]] = []
    domains: list[str | None] = []
    notes: list[str] = []
    for row in rows:
        table = str(row["table"])
        variable = str(row["variable"])
        key = (table, variable)
        identifiers: list[str] = []
        domain: str | None = None
        note_parts: list[str] = []
        baseline_rule = baseline_applied.get(key)
        if baseline_rule is not None:
            identifiers.append(baseline_rule.rule_id)
            domain = _baseline_domain(baseline_rule)
            note_parts.append("Cleaning 0.1 scalar rule replayed unchanged.")
        if key in list_rules:
            list_rule = list_rules[key]
            identifiers.append(list_rule.rule_id)
            domain = (
                f"ordered list elements {list_rule.element_hard_min:g}-"
                f"{list_rule.element_hard_max:g} cm; duplicates and empty lists preserved"
            )
            note_parts.append("Invalid height elements are removed without aggregation.")
        if key in hospital_masks:
            mask = hospital_masks[key]
            identifiers.append(mask.rule_id)
            note_parts.append(
                f"All {mask.hospital} values are unavailable because of the reviewed mixed-scale finding."
            )
        rule = priority.get(variable) if table == "dynamic" else None
        if rule is not None:
            identifiers.append(rule.rule_id)
            domain = _domain_text(rule)
            note_parts.append(rule.rationale)
        if table == "dynamic" and variable in medication_variables:
            identifiers.append("mask_negative_medication_or_therapy_value")
            note_parts.append(
                "Finite negative medication values become null; explicit zero and null remain distinct."
            )
            if domain is None:
                domain = "value >= 0; null is not interpreted as zero"
        if not identifiers:
            identifiers.append(
                "mask_nonfinite_float64" if row["physical_type"] == "double" else "preserve_unchanged"
            )
            note_parts.append(
                "Non-finite floating values become null."
                if row["physical_type"] == "double"
                else "Values are preserved unchanged by cleaning."
            )
        cleaning_rule_ids.append(identifiers)
        domains.append(domain)
        notes.append(" ".join(note_parts))
    additions = (
        (
            "cleaning_policy_version",
            pa.array(["0.2"] * len(rows), type=pa.large_string()),
        ),
        (
            "cleaning_rule_ids",
            pa.array(cleaning_rule_ids, type=pa.list_(pa.large_string())),
        ),
        ("cleaned_valid_domain", pa.array(domains, type=pa.large_string())),
        ("cleaning_note", pa.array(notes, type=pa.large_string())),
        (
            "cleaned_dictionary_status",
            pa.array(
                ["candidate_pending_complete_audit"] * len(rows),
                type=pa.large_string(),
            ),
        ),
        (
            "cleaning_policy_source",
            pa.array(
                ["reviewed_cleaning_policy_0_2"] * len(rows),
                type=pa.large_string(),
            ),
        ),
    )
    output = dictionary
    for name, values in additions:
        if name in output.column_names:
            raise HarmonizationError("Frozen dictionary already has cleaned annotations")
        output = output.append_column(name, values)
    return output


def _escape_markdown(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _dictionary_markdown(table: pa.Table, run_id: str) -> str:
    lines = [
        "# ASIC v3 cleaned 0.2 variable dictionary",
        "",
        f"- Candidate run: `{run_id}`",
        "- Harmonized contract: `0.2`",
        "- Cleaning policy: `0.2`",
        "- Status: **CANDIDATE — PENDING COMPLETE AUDIT AND HUMAN RELEASE APPROVAL**",
        "",
        "Units and definitions originate from the frozen harmonized 0.2 dictionary. "
        "Cleaning annotations describe value handling; they do not authorize external export.",
        "",
        "| Table | Position | Variable | Definition | Unit | Eligibility | Cleaned domain | Cleaning rules | Cleaning note |",
        "|---|---:|---|---|---|---|---|---|---|",
    ]
    for row in table.to_pylist():
        rules = ", ".join(str(item) for item in row["cleaning_rule_ids"])
        lines.append(
            "| "
            + " | ".join(
                _escape_markdown(value)
                for value in (
                    row["table"],
                    row["ordered_position"],
                    row["variable"],
                    row["definition"],
                    row["unit"],
                    row["analysis_eligibility"],
                    row["cleaned_valid_domain"],
                    rules,
                    row["cleaning_note"],
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Medication missingness",
            "",
            "Explicit numeric zero remains zero. Null remains unavailable or unobserved and is never imputed to zero. Finite negative medication values are masked to null.",
            "",
            "## Provenance",
            "",
            "The five operational provenance fields remain in both data tables but are not clinical dictionary variables.",
            "",
        ]
    )
    return "\n".join(lines)


def _accounting(
    baseline_rule_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    hospital_mask_counts: dict[str, Counter[str]],
    priority_counts: dict[str, Counter[str]],
    medication_counts: dict[str, Counter[str]],
) -> dict[str, Any]:
    return {
        "baseline_scalar_rules": {
            key: dict(sorted(value.items()))
            for key, value in sorted(baseline_rule_counts.items())
        },
        "nonfinite_only_fields": dict(sorted(nonfinite_counts.items())),
        "baseline_list_rules": {
            key: dict(sorted(value.items()))
            for key, value in sorted(list_counts.items())
        },
        "baseline_hospital_masks": {
            key: dict(sorted(value.items()))
            for key, value in sorted(hospital_mask_counts.items())
        },
        "priority_rules_0_2": {
            key: dict(sorted(value.items()))
            for key, value in sorted(priority_counts.items())
        },
        "negative_medication_rule_by_variable": {
            key: dict(sorted(value.items()))
            for key, value in sorted(medication_counts.items())
        },
    }


def _priority_summaries(
    policy: Cleaning02Policy,
    priority_counts: dict[str, Counter[str]],
) -> list[dict[str, Any]]:
    return [
        {
            "rule_id": rule.rule_id,
            "variable": rule.variable,
            "action": rule.action,
            "unit": rule.unit,
            "cleaned_domain": _domain_text(rule),
            **dict(sorted(priority_counts[rule.rule_id].items())),
        }
        for rule in policy.rules
    ]


def _metrics(
    policy: Cleaning02Policy,
    baseline_rule_counts: dict[str, Counter[str]],
    nonfinite_counts: Counter[str],
    list_counts: dict[str, Counter[str]],
    hospital_mask_counts: dict[str, Counter[str]],
    priority_counts: dict[str, Counter[str]],
    medication_counts: dict[str, Counter[str]],
) -> dict[str, int]:
    baseline_corrections = sum(
        count
        for counts in baseline_rule_counts.values()
        for key, count in counts.items()
        if key.startswith("corrected_factor_")
    )
    baseline_masks = sum(
        counts["masked_out_of_range"]
        + counts["masked_ambiguous_power_of_ten"]
        for counts in baseline_rule_counts.values()
    )
    priority_corrections = sum(
        count
        for counts in priority_counts.values()
        for key, count in counts.items()
        if key.startswith("corrected_factor_")
    )
    priority_masks = sum(
        counts["masked_nonpositive"]
        + counts["masked_above_upper"]
        + counts["masked_ambiguous_upper"]
        + counts["masked_unrecoverable_upper"]
        + counts["masked_fractional_score"]
        + counts["masked_score_outside_range"]
        for counts in priority_counts.values()
    )
    return {
        "baseline_scalar_rule_count": len(baseline_rule_counts),
        "priority_rule_count": len(priority_counts),
        "medication_variable_count": len(medication_counts),
        "baseline_power_of_ten_correction_count": baseline_corrections,
        "baseline_range_mask_count": baseline_masks,
        "nonfinite_mask_count": sum(nonfinite_counts.values())
        + sum(
            counts["masked_nonfinite"] for counts in baseline_rule_counts.values()
        ),
        "height_element_removal_count": sum(
            counts["removed_null_element_count"]
            + counts["removed_nonfinite_element_count"]
            + counts["removed_out_of_range_element_count"]
            for counts in list_counts.values()
        ),
        "hospital_mask_count": sum(
            counts["site_non_null_values_masked"]
            for counts in hospital_mask_counts.values()
        ),
        "priority_power_of_ten_correction_count": priority_corrections,
        "priority_mask_count": priority_masks,
        "priority_nonpositive_mask_count": sum(
            counts["masked_nonpositive"] for counts in priority_counts.values()
        ),
        "priority_above_upper_mask_count": sum(
            counts["masked_above_upper"]
            + counts["masked_ambiguous_upper"]
            + counts["masked_unrecoverable_upper"]
            for counts in priority_counts.values()
        ),
        "positive_upper_values_preserved": sum(
            counts["preserved_above_review_threshold"]
            for counts in priority_counts.values()
        ),
        "negative_medication_mask_count": sum(
            counts["masked_negative"] for counts in medication_counts.values()
        ),
    }


def _validate_known_counts(
    policy: Cleaning02Policy,
    priority_counts: dict[str, Counter[str]],
    medication_counts: dict[str, Counter[str]],
) -> None:
    rules = {rule.variable: rule for rule in policy.rules}
    counts = {
        variable: priority_counts[rule.rule_id] for variable, rule in rules.items()
    }
    zero_masks = sum(row["zero"] for row in counts.values()) - counts[
        "sofa_score_unspecified"
    ]["zero"]
    negative_masks = sum(row["negative"] for row in counts.values())
    evlwi = counts["evlwi"]
    preserved = sum(
        row["preserved_above_review_threshold"] for row in counts.values()
    )
    score_violations = sum(
        row["masked_fractional_score"] + row["masked_score_outside_range"]
        for variable, row in counts.items()
        if variable.startswith("sofa_score")
    )
    medication_masks = sum(
        row["masked_negative"] for row in medication_counts.values()
    )
    observed = {
        "known_priority_zero_masks": zero_masks,
        "known_priority_negative_masks": negative_masks,
        "expected_evlwi_factor_0_001_corrections": evlwi[
            "corrected_factor_0.001"
        ],
        "expected_evlwi_ambiguous_masks": evlwi["masked_ambiguous_upper"]
        + evlwi["masked_unrecoverable_upper"],
        "expected_positive_upper_values_preserved": preserved,
        "expected_negative_medication_masks": medication_masks,
        "expected_current_score_violations": score_violations,
    }
    if any(policy.expected[key] != value for key, value in observed.items()):
        raise HarmonizationError("Approved cleaning 0.2 evidence counts changed")


def _build_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    return "\n".join(
        (
            "# ASIC v3 cleaned 0.2 candidate build review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Candidate: `{payload['run_id']}`",
            f"- Input harmonized release: `{payload['harmonized_release_id']}`",
            "- Build status: **PASS**",
            "- Complete audit status: **NOT YET RUN**",
            f"- Static rows: `{payload['row_counts']['static']}`",
            f"- Dynamic rows: `{payload['row_counts']['dynamic']}`",
            f"- Cleaning 0.1 scalar rules replayed: `{metrics['baseline_scalar_rule_count']}`",
            f"- Priority 0.2 rules applied: `{metrics['priority_rule_count']}`",
            f"- Priority values masked: `{metrics['priority_mask_count']}`",
            f"- Priority power-of-ten corrections: `{metrics['priority_power_of_ten_correction_count']}`",
            f"- Positive extreme values preserved and audit-reported: `{metrics['positive_upper_values_preserved']}`",
            f"- Negative medication values masked: `{metrics['negative_medication_mask_count']}`",
            "- Rows or stays filtered: `false`",
            "- Columns dropped: `false`",
            "- Derived variables created: `false`",
            "- Publication ready: `false`",
            "",
            "## Next gate",
            "",
            "Run the complete independent cleaning 0.2 audit. The candidate and its cleaned dictionary remain non-publishable until that audit passes and the data owner explicitly approves promotion.",
            "",
        )
    )


def run_cleaning_0_2_build(
    config: Cleaning02BuildConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> Cleaning02BuildResult:
    policy = load_cleaning_0_2_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("Cleaning 0.2 candidate build is production-only")
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid cleaning 0.2 build run ID")
    evidence = _validate_range_evidence(config, policy)
    source = _load_input(config, policy)
    baseline, baseline_applied, _ = _baseline_registry(config, policy)
    decisions = load_reviewed_unit_decisions(policy.unit_decisions_path)
    medication = load_reviewed_medication_semantics(
        policy.medication_semantics_path
    )
    medication_variables = _reviewed_medication_variables(decisions)
    if (
        len(medication_variables)
        != policy.expected["medication_or_therapy_variables"]
        or medication.negative_cleaning_rule["expected_current_mask_count"]
        != policy.expected["expected_negative_medication_masks"]
    ):
        raise HarmonizationError("Reviewed medication cleaning scope changed")
    output_schemas = {
        table: _cleaned_schema(source.source_schemas[table], policy.contract_version)
        for table in ("static", "dynamic")
    }
    output_parent = config.data_root / policy.candidate_directory_name
    final_output = output_parent / selected
    staging_output = output_parent / f".{selected}.incomplete"
    review_dir = config.reports_root / "review" / policy.build_review_directory_name
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if any(
        path.exists()
        for path in (final_output, staging_output, review_json, review_md)
    ):
        raise HarmonizationError("Cleaning 0.2 candidate run already exists")
    staging_output.mkdir(parents=True, mode=0o700)
    staging_output.chmod(0o700)
    baseline_counts = {
        rule.rule_id: Counter() for rule in baseline_applied.values()
    }
    nonfinite_counts: Counter[str] = Counter()
    list_counts = {rule.rule_id: Counter() for rule in baseline.list_rules}
    hospital_mask_counts = {
        rule.rule_id: Counter() for rule in baseline.hospital_masks
    }
    priority_counts = {rule.rule_id: Counter() for rule in policy.rules}
    medication_counts = {
        variable: Counter() for variable in sorted(medication_variables)
    }
    row_counts: Counter[str] = Counter()
    output_files: dict[str, dict[str, Any]] = {}
    globally_all_missing = Counter()
    for table in ("static", "dynamic"):
        input_path = source.source_files[table]
        expected_source_hash = source.release_manifest["files"][table]["sha256"]
        parquet = pq.ParquetFile(input_path)
        output_path = staging_output / f"{table}.parquet"
        schema = output_schemas[table]
        with pq.ParquetWriter(
            output_path, schema, compression=policy.compression
        ) as writer:
            for batch in parquet.iter_batches(
                batch_size=policy.rows_per_batch, use_threads=True
            ):
                transformed = _transform_batch(
                    batch,
                    table,
                    schema,
                    baseline,
                    baseline_applied,
                    policy,
                    medication_variables,
                    baseline_counts,
                    nonfinite_counts,
                    list_counts,
                    hospital_mask_counts,
                    priority_counts,
                    medication_counts,
                )
                writer.write_batch(transformed)
                row_counts[table] += batch.num_rows
                for variable in baseline.globally_all_missing:
                    index = transformed.schema.get_field_index(variable)
                    if index >= 0:
                        values = transformed.column(index)
                        globally_all_missing[variable] += len(values) - values.null_count
                if (
                    progress is not None
                    and row_counts[table] % (policy.rows_per_batch * 20) == 0
                ):
                    progress(
                        f"cleaning_0_2_build_progress table={table} rows={row_counts[table]}"
                    )
        output_path.chmod(0o600)
        if row_counts[table] != policy.expected[f"{table}_rows"]:
            raise HarmonizationError("Cleaning 0.2 changed an approved row count")
        if sha256_file(input_path) != expected_source_hash:
            raise HarmonizationError("Harmonized 0.2 input changed during cleaning")
        output_files[table] = {
            "path": str(final_output / output_path.name),
            "sha256": sha256_file(output_path),
            "row_count": row_counts[table],
            "schema_sha256": _schema_digest(schema),
        }
        if progress is not None:
            progress(f"cleaning_0_2_build_table_complete={table}")
    if any(globally_all_missing.values()):
        raise HarmonizationError("A globally all-missing field changed")
    _validate_known_counts(policy, priority_counts, medication_counts)
    frozen_dictionary = pq.read_table(source.dictionary_path)
    if len(frozen_dictionary) != (
        policy.expected["static_clinical_variables"]
        + policy.expected["dynamic_clinical_variables"]
    ):
        raise HarmonizationError("Frozen variable dictionary row count changed")
    cleaned_dictionary = _dictionary_annotations(
        frozen_dictionary,
        policy,
        baseline,
        baseline_applied,
        medication_variables,
    )
    dictionary_path = staging_output / policy.dictionary_filename
    _write_parquet(dictionary_path, cleaned_dictionary, policy.compression, 0o600)
    dictionary_markdown_path = staging_output / policy.dictionary_markdown_filename
    _write_text(
        dictionary_markdown_path,
        _dictionary_markdown(cleaned_dictionary, selected),
        0o600,
    )
    metrics = _metrics(
        policy,
        baseline_counts,
        nonfinite_counts,
        list_counts,
        hospital_mask_counts,
        priority_counts,
        medication_counts,
    )
    accounting = _accounting(
        baseline_counts,
        nonfinite_counts,
        list_counts,
        hospital_mask_counts,
        priority_counts,
        medication_counts,
    )
    generated = utc_timestamp()
    manifest_path = staging_output / "cleaning_0_2_manifest.json"
    manifest = {
        "artifact": "asic_v3_cleaning_0_2_candidate",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "status": policy.candidate_status,
        "lineage": {
            "harmonized_release_id": policy.release_id,
            "harmonized_release_manifest_sha256": sha256_file(
                source.release_manifest_path
            ),
            "harmonized_contract_version": policy.contract_version,
            "frozen_contract_manifest_sha256": sha256_file(
                source.contract_manifest_path
            ),
            "cleaning_0_2_policy_sha256": sha256_file(policy.source_path),
            "baseline_cleaning_policy_sha256": policy.baseline_policy_sha256,
            "reviewed_unit_decisions_sha256": policy.unit_decisions_sha256,
            "reviewed_medication_semantics_sha256": (
                policy.medication_semantics_sha256
            ),
            "range_evidence": evidence,
        },
        "human_approval": policy.approval,
        "outputs": {
            **output_files,
            "cleaned_variable_dictionary": {
                "path": str(final_output / dictionary_path.name),
                "sha256": sha256_file(dictionary_path),
                "row_count": len(cleaned_dictionary),
            },
            "cleaned_variable_dictionary_markdown": {
                "path": str(final_output / dictionary_markdown_path.name),
                "sha256": sha256_file(dictionary_markdown_path),
            },
        },
        "rule_accounting": accounting,
        "metrics": metrics,
        "priority_rule_summaries": _priority_summaries(policy, priority_counts),
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "columns_dropped": False,
        "height_aggregation_applied": False,
        "cleaning_applied": True,
        "derivation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging_output.replace(final_output)
    final_output.chmod(0o700)
    manifest_path = final_output / manifest_path.name
    review_payload = {
        "artifact": "asic_v3_cleaning_0_2_build_review",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "harmonized_release_id": policy.release_id,
        "overall_status": "pass_build_requires_complete_audit",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "row_counts": dict(row_counts),
        "metrics": metrics,
        "priority_rule_summaries": _priority_summaries(policy, priority_counts),
        "cleaned_dictionary_generated": True,
        "rows_or_stays_filtered": False,
        "columns_dropped": False,
        "cleaning_applied": True,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _build_markdown(review_payload), 0o640)
    return Cleaning02BuildResult(
        run_id=selected,
        candidate_directory=final_output,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )


def _priority_invalid_mask(values: pa.Array, rule: Cleaning02Rule) -> pa.Array:
    numeric = pc.cast(values, pa.float64(), safe=True)
    finite = pc.fill_null(pc.is_finite(numeric), False)
    if rule.action in {
        "mask_nonpositive_preserve_positive_extremes",
        "mask_nonpositive_preserve_above_review_threshold",
    }:
        return pc.and_(finite, pc.less_equal(numeric, 0.0))
    if rule.action in {
        "mask_outside_open_closed_range",
        "recover_unique_power_of_ten_else_mask_outside_open_closed_range",
    }:
        if rule.hard_max is None:
            raise AssertionError(rule)
        return pc.and_(
            finite,
            pc.or_(
                pc.less_equal(numeric, 0.0),
                pc.greater(numeric, rule.hard_max),
            ),
        )
    if rule.action == "require_integer_closed_range":
        if (
            rule.hard_min is None
            or rule.hard_max is None
            or rule.integer_tolerance is None
        ):
            raise AssertionError(rule)
        integer = pc.less_equal(
            pc.abs(pc.subtract(numeric, pc.round(numeric, ndigits=0))),
            rule.integer_tolerance,
        )
        in_range = pc.and_(
            pc.greater_equal(numeric, rule.hard_min),
            pc.less_equal(numeric, rule.hard_max),
        )
        return pc.and_(finite, pc.invert(pc.and_(integer, in_range)))
    raise AssertionError(rule.action)


def _audit_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 cleaned 0.2 candidate audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Cleaning candidate: `{payload['build_run_id']}`",
        f"- Input harmonized release: `{payload['harmonized_release_id']}`",
        "- Technical audit status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Technical blocking findings: `0`",
        "- Human blocking findings: `1`",
        f"- Compared static rows: `{metrics['compared_static_rows']}`",
        f"- Compared dynamic rows: `{metrics['compared_dynamic_rows']}`",
        f"- Compared output cells: `{metrics['compared_output_cells']}`",
        f"- Priority values masked: `{metrics['priority_mask_count']}`",
        f"- Priority power-of-ten corrections: `{metrics['priority_power_of_ten_correction_count']}`",
        f"- Positive upper-tail values preserved: `{metrics['positive_upper_values_preserved']}`",
        f"- Negative medication values masked: `{metrics['negative_medication_mask_count']}`",
        f"- Albumin values above 1000 dg/L masked: `{metrics['albumin_above_1000_mask_count']}`",
        "- Every output column and value recomputed exactly: `true`",
        "- Cleaned dictionary annotations recomputed exactly: `true`",
        "- Rows or stays filtered: `false`",
        "- Columns dropped: `false`",
        "- Derived variables created: `false`",
        "- Publication ready: `false`",
        "",
        "## Priority rule accounting",
        "",
    ]
    for row in payload["priority_rule_summaries"]:
        lines.append(
            f"- `{row['variable']}`: action `{row['action']}`; finite "
            f"`{row.get('finite', 0)}`; zeros `{row.get('zero', 0)}`; "
            f"negatives `{row.get('negative', 0)}`; nonpositive masked "
            f"`{row.get('masked_nonpositive', 0)}`; upper masked "
            f"`{row.get('masked_above_upper', 0) + row.get('masked_ambiguous_upper', 0) + row.get('masked_unrecoverable_upper', 0)}`; "
            f"upper preserved `{row.get('preserved_above_review_threshold', 0)}`."
        )
    lines.extend(
        [
            "",
            "## Human review gate",
            "",
            "Review this complete audit and explicitly approve the exact candidate before immutable cleaned 0.2 promotion. Promotion must copy the audited Parquet and dictionary bytes without rerunning cleaning.",
            "",
            "## Boundary",
            "",
            "Uncertain positive INR, lactate, PTT, GEDVI, and SVRI extremes remain present and are aggregate-audited rather than silently changed. Medication zero remains distinct from null. No cohort, time block, or derived value is created.",
            "",
        ]
    )
    return "\n".join(lines)


def run_cleaning_0_2_audit(
    config: Cleaning02AuditConfig,
    build_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> Cleaning02AuditResult:
    audit_policy = load_cleaning_0_2_audit_policy(config.policy_path)
    policy = load_cleaning_0_2_policy(audit_policy.build_policy_path)
    if (
        config.dataset_context != "production"
        or config.build.policy_path.resolve()
        != audit_policy.build_policy_path.resolve()
    ):
        raise HarmonizationError("Cleaning 0.2 audit lineage changed")
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected) or not RUN_ID_PATTERN.fullmatch(
        build_run_id
    ):
        raise HarmonizationError("Invalid cleaning 0.2 audit run ID")
    evidence = _validate_range_evidence(config.build, policy)
    source = _load_input(config.build, policy)
    baseline, baseline_applied, _ = _baseline_registry(config.build, policy)
    decisions = load_reviewed_unit_decisions(policy.unit_decisions_path)
    load_reviewed_medication_semantics(policy.medication_semantics_path)
    medication_variables = _reviewed_medication_variables(decisions)
    output_schemas = {
        table: _cleaned_schema(source.source_schemas[table], policy.contract_version)
        for table in ("static", "dynamic")
    }
    candidate_dir = config.data_root / policy.candidate_directory_name / build_run_id
    manifest_path = candidate_dir / "cleaning_0_2_manifest.json"
    manifest = _read_json(manifest_path, "Cleaning 0.2 candidate manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Cleaning 0.2 candidate manifest is invalid")
    lineage = manifest.get("lineage")
    if not isinstance(lineage, dict) or (
        manifest.get("artifact") != "asic_v3_cleaning_0_2_candidate"
        or manifest.get("artifact_version") != policy.artifact_version
        or manifest.get("dataset_context") != config.dataset_context
        or manifest.get("run_id") != build_run_id
        or manifest.get("status") != policy.candidate_status
        or lineage.get("harmonized_release_id") != policy.release_id
        or lineage.get("harmonized_release_manifest_sha256")
        != sha256_file(source.release_manifest_path)
        or lineage.get("frozen_contract_manifest_sha256")
        != sha256_file(source.contract_manifest_path)
        or lineage.get("cleaning_0_2_policy_sha256")
        != sha256_file(policy.source_path)
        or lineage.get("range_evidence") != evidence
        or manifest.get("row_filtering_applied") is not False
        or manifest.get("stay_filtering_applied") is not False
        or manifest.get("columns_dropped") is not False
        or manifest.get("cleaning_applied") is not True
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not False
    ):
        raise HarmonizationError("Cleaning 0.2 candidate manifest changed")
    private_dir = (
        config.reports_root
        / "private"
        / audit_policy.private_directory_name
        / selected
    )
    review_dir = config.reports_root / "review" / audit_policy.review_directory_name
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Cleaning 0.2 audit run already exists")
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Cleaning 0.2 candidate outputs are absent")
    baseline_counts = {
        rule.rule_id: Counter() for rule in baseline_applied.values()
    }
    nonfinite_counts: Counter[str] = Counter()
    list_counts = {rule.rule_id: Counter() for rule in baseline.list_rules}
    hospital_mask_counts = {
        rule.rule_id: Counter() for rule in baseline.hospital_masks
    }
    priority_counts = {rule.rule_id: Counter() for rule in policy.rules}
    medication_counts = {
        variable: Counter() for variable in sorted(medication_variables)
    }
    compared_rows: Counter[str] = Counter()
    compared_cells = 0
    post_nonfinite = 0
    post_baseline_invalid = 0
    post_priority_invalid = 0
    post_negative_medication = 0
    all_missing_non_null: Counter[str] = Counter()
    priority_by_variable = {rule.variable: rule for rule in policy.rules}
    for table in ("static", "dynamic"):
        output_path = candidate_dir / f"{table}.parquet"
        summary = outputs.get(table)
        output_hash = sha256_file(output_path)
        output_parquet = pq.ParquetFile(output_path)
        if not isinstance(summary, dict) or (
            summary.get("sha256") != output_hash
            or summary.get("row_count") != policy.expected[f"{table}_rows"]
            or output_parquet.metadata.num_rows
            != policy.expected[f"{table}_rows"]
            or summary.get("schema_sha256")
            != _schema_digest(output_schemas[table])
            or not _parquet_schema_matches(
                output_parquet.schema_arrow, output_schemas[table]
            )
        ):
            raise HarmonizationError("Cleaning 0.2 candidate table changed")
        input_path = source.source_files[table]
        source_hash = source.release_manifest["files"][table]["sha256"]
        if sha256_file(input_path) != source_hash:
            raise HarmonizationError("Harmonized input changed before cleaning audit")
        input_iterator = pq.ParquetFile(input_path).iter_batches(
            batch_size=audit_policy.rows_per_batch, use_threads=True
        )
        output_iterator = output_parquet.iter_batches(
            batch_size=audit_policy.rows_per_batch, use_threads=True
        )
        output_state: list[Any] = [None, 0]
        for input_batch in input_iterator:
            observed = _next_slice(
                output_iterator, output_state, input_batch.num_rows
            )
            expected = _transform_batch(
                input_batch,
                table,
                output_schemas[table],
                baseline,
                baseline_applied,
                policy,
                medication_variables,
                baseline_counts,
                nonfinite_counts,
                list_counts,
                hospital_mask_counts,
                priority_counts,
                medication_counts,
            )
            for field in output_schemas[table]:
                index = observed.schema.get_field_index(field.name)
                expected_values = expected.column(index)
                observed_values = observed.column(index)
                if not expected_values.equals(observed_values):
                    raise HarmonizationError(
                        f"Cleaning 0.2 value mismatch: {table}.{field.name}"
                    )
                compared_cells += input_batch.num_rows
                if field.type == pa.float64():
                    finite = pc.fill_null(pc.is_finite(observed_values), False)
                    post_nonfinite += _bool_count(
                        pc.and_(pc.is_valid(observed_values), pc.invert(finite))
                    )
                    baseline_rule = baseline_applied.get((table, field.name))
                    if baseline_rule is not None:
                        post_baseline_invalid += _bool_count(
                            pc.and_(
                                finite,
                                pc.invert(_inside_range(observed_values, baseline_rule)),
                            )
                        )
                    priority_rule = (
                        priority_by_variable.get(field.name)
                        if table == "dynamic"
                        else None
                    )
                    if priority_rule is not None:
                        post_priority_invalid += _bool_count(
                            _priority_invalid_mask(observed_values, priority_rule)
                        )
                    if table == "dynamic" and field.name in medication_variables:
                        post_negative_medication += _bool_count(
                            pc.and_(finite, pc.less(observed_values, 0.0))
                        )
                if field.name in baseline.globally_all_missing:
                    all_missing_non_null[field.name] += (
                        len(observed_values) - observed_values.null_count
                    )
            compared_rows[table] += input_batch.num_rows
            if (
                progress is not None
                and compared_rows[table] % (audit_policy.rows_per_batch * 20) == 0
            ):
                progress(
                    f"cleaning_0_2_audit_progress table={table} rows={compared_rows[table]}"
                )
        _require_output_exhausted(output_iterator, output_state)
        if compared_rows[table] != policy.expected[f"{table}_rows"]:
            raise HarmonizationError("Cleaning 0.2 audit row count changed")
        if sha256_file(input_path) != source_hash:
            raise HarmonizationError("Harmonized input changed during cleaning audit")
        if sha256_file(output_path) != output_hash:
            raise HarmonizationError("Cleaning candidate changed during audit")
        if progress is not None:
            progress(f"cleaning_0_2_audit_table_complete={table}")
    if (
        post_nonfinite
        or post_baseline_invalid
        or post_priority_invalid
        or post_negative_medication
        or any(all_missing_non_null.values())
    ):
        raise HarmonizationError("Cleaned 0.2 output violates an approved rule")
    _validate_known_counts(policy, priority_counts, medication_counts)
    accounting = _accounting(
        baseline_counts,
        nonfinite_counts,
        list_counts,
        hospital_mask_counts,
        priority_counts,
        medication_counts,
    )
    metrics = _metrics(
        policy,
        baseline_counts,
        nonfinite_counts,
        list_counts,
        hospital_mask_counts,
        priority_counts,
        medication_counts,
    )
    metrics.update(
        {
            "compared_static_rows": compared_rows["static"],
            "compared_dynamic_rows": compared_rows["dynamic"],
            "compared_output_cells": compared_cells,
            "post_clean_nonfinite_count": post_nonfinite,
            "post_clean_baseline_invalid_count": post_baseline_invalid,
            "post_clean_priority_invalid_count": post_priority_invalid,
            "post_clean_negative_medication_count": post_negative_medication,
            "albumin_above_1000_mask_count": priority_counts["C02-ALBUMIN"][
                "masked_above_upper"
            ],
        }
    )
    manifest_metrics = manifest.get("metrics")
    if (
        manifest.get("rule_accounting") != accounting
        or not isinstance(manifest_metrics, dict)
        or any(
            key not in metrics or metrics[key] != value
            for key, value in manifest_metrics.items()
        )
    ):
        raise HarmonizationError("Cleaning 0.2 accounting differs from audit")
    frozen_dictionary = pq.read_table(source.dictionary_path)
    expected_dictionary = _dictionary_annotations(
        frozen_dictionary,
        policy,
        baseline,
        baseline_applied,
        medication_variables,
    )
    dictionary_path = candidate_dir / policy.dictionary_filename
    dictionary_summary = outputs.get("cleaned_variable_dictionary")
    if not isinstance(dictionary_summary, dict) or (
        dictionary_summary.get("sha256") != sha256_file(dictionary_path)
        or dictionary_summary.get("row_count") != len(expected_dictionary)
        or not pq.read_table(dictionary_path).equals(expected_dictionary)
    ):
        raise HarmonizationError("Cleaned 0.2 dictionary annotations changed")
    expected_markdown = _dictionary_markdown(expected_dictionary, build_run_id)
    markdown_path = candidate_dir / policy.dictionary_markdown_filename
    markdown_summary = outputs.get("cleaned_variable_dictionary_markdown")
    if not isinstance(markdown_summary, dict) or (
        markdown_summary.get("sha256") != sha256_file(markdown_path)
        or markdown_path.read_text(encoding="utf-8") != expected_markdown
    ):
        raise HarmonizationError("Cleaned 0.2 dictionary document changed")
    generated = utc_timestamp()
    blockers = (
        {
            "check": "cleaned_0_2_candidate_release_approved",
            "details": (
                "The completely audited cleaned 0.2 candidate requires explicit "
                "data-owner approval before immutable promotion."
            ),
        },
    )
    summaries = _priority_summaries(policy, priority_counts)
    private_payload = {
        "artifact": "asic_v3_cleaning_0_2_audit_private",
        "artifact_version": audit_policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "build_run_id": build_run_id,
        "build_manifest_sha256": sha256_file(manifest_path),
        "harmonized_release_manifest_sha256": sha256_file(
            source.release_manifest_path
        ),
        "metrics": metrics,
        "rule_accounting": accounting,
        "dictionary_sha256": sha256_file(dictionary_path),
        "source_data_modified": False,
        "candidate_data_modified": False,
        "publication_ready": False,
    }
    review_payload = {
        "artifact": "asic_v3_cleaning_0_2_audit_review",
        "artifact_version": audit_policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "build_run_id": build_run_id,
        "harmonized_release_id": policy.release_id,
        "cleaning_policy_version": policy.version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "priority_rule_summaries": summaries,
        "every_output_cell_recomputed": True,
        "cleaned_dictionary_recomputed": True,
        "uncertain_positive_extremes_preserved": True,
        "rows_or_stays_filtered": False,
        "columns_dropped": False,
        "derivation_applied": False,
        "candidate_data_modified": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(private_dir / "cleaning_0_2_audit_manifest.json", private_payload, 0o600)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _audit_markdown(review_payload), 0o640)
    return Cleaning02AuditResult(
        run_id=selected,
        build_run_id=build_run_id,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
