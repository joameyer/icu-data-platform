from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pyarrow as pa

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.time_blocking.evidence import (
    TimeBlockingEvidencePolicy,
    load_time_blocking_evidence_policy,
)


EXPECTED_BOUNDARY = {
    "local_implementation_and_tests_authorized": True,
    "production_candidate_authorized": False,
    "production_audit_authorized": False,
    "production_release_promotion_authorized": False,
    "modify_core_derived_release": False,
    "modify_any_current_release_pointer": False,
    "filter_rows_or_stays": False,
    "create_analysis_cohort": False,
    "apply_carry_forward_or_imputation": False,
    "authorize_external_data_export": False,
}

EXPECTED_LAST_OUTPUTS = (
    "last_observation_value",
    "last_observation_time_h",
    "last_observation_age_h",
)

EXPECTED_TECHNICAL_CHECKS = frozenset(
    {
        "static_row_count_matches",
        "static_keys_are_valid",
        "dynamic_row_count_matches",
        "negative_time_count_matches",
        "all_finite_rows_assigned",
        "time_fields_are_complete_and_consistent",
        "stay_set_conserved",
        "stay_rows_are_contiguous",
        "hospital_identity_is_consistent",
        "source_order_is_deterministic",
        "recording_extent_reproduced",
        "registry_covers_schema",
        "cross_batch_assignment_reproducible",
        "core_derived_bytes_unchanged",
    }
)


@dataclass(frozen=True)
class FrozenEvidence:
    run_id: str
    review_relative_path: Path
    review_sha256: str
    expected_dynamic_rows: int
    expected_static_rows: int
    expected_negative_time_rows: int
    expected_total_grid_blocks: int
    expected_empty_grid_blocks: int
    expected_terminal_partial_blocks: int
    expected_terminal_partial_source_rows: int


@dataclass(frozen=True)
class OutputSpec:
    source_variable: str
    operation: str
    output_name: str
    output_type: pa.DataType
    output_unit: str
    nullable: bool
    source_group: str
    eligibility: str
    caveat: str
    component_index: int | None = None
    pair_state: str | None = None


@dataclass(frozen=True)
class TimeBlockingContract:
    version: str
    resolution_minutes: int
    resolution_label: str
    artificial_anchor: str
    evidence: FrozenEvidence
    evidence_policy: TimeBlockingEvidencePolicy
    evidence_policy_sha256: str
    core_derived_release_id: str
    core_derived_contract_version: str
    cleaned_release_id: str
    harmonized_release_id: str
    family_operations: dict[str, tuple[str, ...]]
    block_file: str
    stay_summary_file: str
    dictionary_parquet: str
    dictionary_markdown: str
    candidate_manifest: str
    candidate_directory: Path
    release_directory: Path
    current_release_pointer: Path
    source_path: Path


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


def _contained_path(value: Any, location: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{location} must be a non-empty path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


def _string_tuple(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ConfigurationError(f"{location} must be a list of strings")
    return tuple(value)


def load_time_blocking_contract(path: str | Path) -> TimeBlockingContract:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed time-blocking contract")
    if (
        raw.get("time_blocking_contract_version") != "0.1"
        or raw.get("status")
        != "human_approved_and_frozen_for_local_implementation_and_tests_only"
    ):
        raise ConfigurationError("Time-blocking contract approval is invalid")
    approval = _mapping(raw.get("approval"), "approval")
    if (
        approval.get("role") != "data_owner"
        or approval.get("recorded_date") != "2026-08-08"
        or "20260808T114902Z" not in required_string(
            approval, "statement", "approval"
        )
        or "dose totals deferred" not in approval["statement"]
    ):
        raise ConfigurationError("Time-blocking approval statement changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Time-blocking authorization boundary changed")

    evidence_raw = _mapping(raw.get("evidence"), "evidence")
    evidence = FrozenEvidence(
        run_id=required_string(evidence_raw, "run_id", "evidence"),
        review_relative_path=_contained_path(
            evidence_raw.get("review_relative_path"),
            "evidence.review_relative_path",
        ),
        review_sha256=required_string(
            evidence_raw, "review_sha256", "evidence"
        ),
        expected_dynamic_rows=_positive_int(
            evidence_raw.get("expected_dynamic_rows"),
            "evidence.expected_dynamic_rows",
        ),
        expected_static_rows=_positive_int(
            evidence_raw.get("expected_static_rows"),
            "evidence.expected_static_rows",
        ),
        expected_negative_time_rows=_positive_int(
            evidence_raw.get("expected_negative_time_rows"),
            "evidence.expected_negative_time_rows",
        ),
        expected_total_grid_blocks=_positive_int(
            evidence_raw.get("expected_total_grid_blocks"),
            "evidence.expected_total_grid_blocks",
        ),
        expected_empty_grid_blocks=_nonnegative_int(
            evidence_raw.get("expected_empty_grid_blocks"),
            "evidence.expected_empty_grid_blocks",
        ),
        expected_terminal_partial_blocks=_positive_int(
            evidence_raw.get("expected_terminal_partial_blocks"),
            "evidence.expected_terminal_partial_blocks",
        ),
        expected_terminal_partial_source_rows=_positive_int(
            evidence_raw.get("expected_terminal_partial_source_rows"),
            "evidence.expected_terminal_partial_source_rows",
        ),
    )
    if (
        evidence.run_id != "20260808T114902Z"
        or evidence_raw.get("artifact")
        != "asic_v3_time_blocking_8h_contract_evidence_review"
        or evidence_raw.get("artifact_version") != "0.1"
        or evidence_raw.get("expected_overall_status") != "pending_human_review"
        or evidence_raw.get("expected_technical_blocking_finding_count") != 0
        or evidence_raw.get("require_cross_batch_assignment_reproducible")
        is not True
    ):
        raise ConfigurationError("Frozen time-blocking evidence binding changed")

    inputs = _mapping(raw.get("immutable_inputs"), "immutable_inputs")
    evidence_policy_path = resolve_path(
        required_string(inputs, "evidence_policy", "immutable_inputs"),
        source,
    )
    evidence_policy_hash = required_string(
        inputs, "evidence_policy_sha256", "immutable_inputs"
    )
    if sha256_file(evidence_policy_path) != evidence_policy_hash:
        raise ConfigurationError("Frozen evidence policy changed")
    evidence_policy = load_time_blocking_evidence_policy(evidence_policy_path)

    time_index = _mapping(raw.get("time_index"), "time_index")
    if (
        time_index.get("resolution_minutes") != 480
        or time_index.get("resolution_label") != "8h"
        or time_index.get("pre_admission_interval")
        != "left_closed_right_open"
        or time_index.get("admission_interval") != "singleton_zero"
        or time_index.get("post_admission_interval")
        != "left_open_right_closed"
        or time_index.get("exact_positive_boundary_belongs_to_endpoint_label")
        is not True
        or time_index.get("retain_every_negative_row") is not True
        or time_index.get("retain_admission_row_for_every_static_stay") is not True
        or time_index.get("retain_empty_intervening_blocks") is not True
        or time_index.get("retain_terminal_partial_block") is not True
        or time_index.get("prediction_time_field") != "omitted"
        or time_index.get("artificial_time_has_real_calendar_meaning") is not False
    ):
        raise ConfigurationError("Frozen time-index contract changed")
    last = _mapping(raw.get("last_observation"), "last_observation")
    if (
        tuple(
            last.get(key)
            for key in (
                "value_output_suffix",
                "time_output_suffix",
                "age_output_suffix",
            )
        )
        != EXPECTED_LAST_OUTPUTS
        or last.get("select") != "final_non_missing_value_within_block"
        or last.get("tie_break")
        != "hospital_scoped_source_order_ascending"
        or last.get("carry_forward") != "prohibited"
    ):
        raise ConfigurationError("Frozen last-observation contract changed")

    family_raw = _mapping(raw.get("aggregation_families"), "aggregation_families")
    family_operations = {
        name: _string_tuple(
            _mapping(family_raw.get(name), f"aggregation_families.{name}").get(
                "operations"
            ),
            f"aggregation_families.{name}.operations",
        )
        for name in (
            "numeric_measurement",
            "medication_or_therapy",
            "score",
            "rolling_value",
            "boolean",
            "position_therapy",
        )
    }
    medication = _mapping(
        family_raw.get("medication_or_therapy"),
        "aggregation_families.medication_or_therapy",
    )
    if (
        medication.get("observed_dose_total") != "prohibited_and_deferred"
        or medication.get("administration_count") != "prohibited_and_deferred"
        or medication.get("time_weighted_exposure")
        != "prohibited_and_deferred"
    ):
        raise ConfigurationError("Medication total deferral changed")
    pair = _mapping(
        family_raw.get("therapy_read_confirmation_utc"),
        "aggregation_families.therapy_read_confirmation_utc",
    )
    family_operations["therapy_read_confirmation_component"] = _string_tuple(
        pair.get("component_operations"),
        "aggregation_families.therapy_read_confirmation_utc.component_operations",
    )
    family_operations["therapy_read_confirmation_pair_states"] = _string_tuple(
        pair.get("pair_state_counts"),
        "aggregation_families.therapy_read_confirmation_utc.pair_state_counts",
    )

    registry = _mapping(raw.get("registry_policy"), "registry_policy")
    missingness = _mapping(raw.get("missingness"), "missingness")
    if (
        registry.get("registry_source")
        != "exact_147_field_registry_from_evidence_policy"
        or registry.get("analysis_ineligible_action")
        != "dictionary_and_accounting_only"
        or registry.get("conditionally_ineligible_action")
        != "dictionary_and_accounting_only"
        or registry.get("copy_static_table") is not False
        or registry.get("repeat_static_on_blocks") is not False
        or registry.get("keep_reported_and_computed_driving_pressure_separate")
        is not True
        or registry.get("keep_sofa_and_isofa_variants_separate") is not True
        or any(
            missingness.get(key) != "prohibited"
            for key in (
                "null_to_zero",
                "zero_to_null",
                "interpolation",
                "imputation",
                "carry_forward",
            )
        )
    ):
        raise ConfigurationError("Registry or missingness contract changed")

    artifacts = _mapping(raw.get("artifact_contract"), "artifact_contract")
    contract = TimeBlockingContract(
        version="0.1",
        resolution_minutes=480,
        resolution_label="8h",
        artificial_anchor=required_string(
            time_index, "artificial_anchor", "time_index"
        ),
        evidence=evidence,
        evidence_policy=evidence_policy,
        evidence_policy_sha256=evidence_policy_hash,
        core_derived_release_id=required_string(
            inputs, "core_derived_release_id", "immutable_inputs"
        ),
        core_derived_contract_version=required_string(
            inputs, "core_derived_contract_version", "immutable_inputs"
        ),
        cleaned_release_id=required_string(
            inputs, "cleaned_release_id", "immutable_inputs"
        ),
        harmonized_release_id=required_string(
            inputs, "harmonized_release_id", "immutable_inputs"
        ),
        family_operations=family_operations,
        block_file=required_string(artifacts, "block_file", "artifact_contract"),
        stay_summary_file=required_string(
            artifacts, "stay_summary_file", "artifact_contract"
        ),
        dictionary_parquet=required_string(
            artifacts, "dictionary_parquet", "artifact_contract"
        ),
        dictionary_markdown=required_string(
            artifacts, "dictionary_markdown", "artifact_contract"
        ),
        candidate_manifest=required_string(
            artifacts, "candidate_manifest", "artifact_contract"
        ),
        candidate_directory=_contained_path(
            artifacts.get("candidate_directory"),
            "artifact_contract.candidate_directory",
        ),
        release_directory=_contained_path(
            artifacts.get("release_directory"),
            "artifact_contract.release_directory",
        ),
        current_release_pointer=_contained_path(
            artifacts.get("current_release_pointer"),
            "artifact_contract.current_release_pointer",
        ),
        source_path=source,
    )
    if (
        contract.core_derived_release_id
        != evidence_policy.core_derived_release_id
        or contract.core_derived_contract_version
        != evidence_policy.core_derived_contract_version
        or contract.cleaned_release_id != evidence_policy.cleaned_release_id
        or contract.harmonized_release_id != evidence_policy.harmonized_release_id
        or contract.evidence.expected_dynamic_rows
        != evidence_policy.expected_dynamic_rows
        or contract.evidence.expected_static_rows
        != evidence_policy.expected_static_rows
        or contract.evidence.expected_negative_time_rows
        != evidence_policy.expected_negative_time_rows
    ):
        raise ConfigurationError("Frozen contract and evidence policy disagree")
    return contract


def load_and_validate_frozen_evidence(
    reports_root: Path,
    contract: TimeBlockingContract,
) -> dict[str, Any]:
    path = reports_root / contract.evidence.review_relative_path
    if not path.is_file() or sha256_file(path) != contract.evidence.review_sha256:
        raise HarmonizationError("Approved time-blocking evidence bytes changed")
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError("Approved time-blocking evidence is invalid") from exc
    if not isinstance(payload, dict):
        raise HarmonizationError("Approved time-blocking evidence must be an object")
    technical = payload.get("technical_checks")
    accounting = payload.get("assignment_evidence", {}).get(
        "row_and_stay_accounting"
    )
    if (
        payload.get("artifact")
        != "asic_v3_time_blocking_8h_contract_evidence_review"
        or payload.get("artifact_version") != "0.1"
        or payload.get("run_id") != contract.evidence.run_id
        or payload.get("overall_status") != "pending_human_review"
        or payload.get("technical_blocking_findings") != []
        or not isinstance(technical, dict)
        or set(technical) != EXPECTED_TECHNICAL_CHECKS
        or any(value is not True for value in technical.values())
        or not isinstance(accounting, dict)
        or accounting.get("dynamic_row_count")
        != contract.evidence.expected_dynamic_rows
        or payload.get("static_accounting", {}).get("row_count")
        != contract.evidence.expected_static_rows
        or accounting.get("pre_admission_source_row_count")
        != contract.evidence.expected_negative_time_rows
        or accounting.get("total_grid_block_count")
        != contract.evidence.expected_total_grid_blocks
        or accounting.get("total_empty_grid_block_count")
        != contract.evidence.expected_empty_grid_blocks
        or accounting.get("terminal_partial_block_count")
        != contract.evidence.expected_terminal_partial_blocks
        or accounting.get("terminal_partial_source_row_count")
        != contract.evidence.expected_terminal_partial_source_rows
        or payload.get("cross_batch_assignment_reproducible") is not True
    ):
        raise HarmonizationError("Approved time-blocking evidence changed")
    return payload


def _eligibility(variable: str, policy: TimeBlockingEvidencePolicy) -> str:
    if variable in policy.analysis_ineligible:
        return "analysis_ineligible"
    if variable in policy.conditionally_ineligible:
        return "conditionally_ineligible"
    return "eligible"


def _numeric_specs(
    variable: str,
    group: str,
    unit: str,
    operations: tuple[str, ...],
) -> list[OutputSpec]:
    output: list[OutputSpec] = []
    for operation in operations:
        if operation == "observation_count" or operation.startswith("observed_"):
            output_type = pa.int64()
            output_unit = "count"
            nullable = False
        elif operation in {
            "last_observation_time_h",
            "last_observation_age_h",
        }:
            output_type = pa.float64()
            output_unit = "h"
            nullable = True
        else:
            output_type = pa.float64()
            output_unit = unit
            nullable = True
        output.append(
            OutputSpec(
                source_variable=variable,
                operation=operation,
                output_name=f"{variable}__{operation}",
                output_type=output_type,
                output_unit=output_unit,
                nullable=nullable,
                source_group=group,
                eligibility="eligible",
                caveat="Observation-based; no interpolation, weighting, or carry-forward.",
            )
        )
    return output


def build_output_specs(
    contract: TimeBlockingContract,
    source_schema: pa.Schema,
) -> tuple[OutputSpec, ...]:
    policy = contract.evidence_policy
    if set(source_schema.names) != set(policy.registry_variables):
        raise HarmonizationError("Source schema and frozen registry disagree")
    group_by_variable = policy.group_by_variable
    specs: list[OutputSpec] = []
    for field in source_schema:
        variable = field.name
        group = group_by_variable[variable]
        eligibility = _eligibility(variable, policy)
        if eligibility != "eligible" or group in {
            "key",
            "time",
            "operational_provenance",
        }:
            continue
        metadata = field.metadata or {}
        unit = metadata.get(b"asic_v3_unit", b"unavailable").decode(
            "utf-8", errors="replace"
        )
        if group in {
            "numeric_measurement",
            "medication_or_therapy",
            "score",
            "rolling_value",
        }:
            specs.extend(
                _numeric_specs(
                    variable,
                    group,
                    unit,
                    contract.family_operations[group],
                )
            )
        elif group == "boolean":
            for operation in contract.family_operations["boolean"]:
                if operation in {
                    "observation_count",
                    "observed_true_count",
                    "observed_false_count",
                }:
                    output_type, output_unit, nullable = pa.int64(), "count", False
                elif operation in {
                    "last_observation_time_h",
                    "last_observation_age_h",
                }:
                    output_type, output_unit, nullable = pa.float64(), "h", True
                else:
                    output_type, output_unit, nullable = pa.bool_(), "not_applicable", True
                specs.append(
                    OutputSpec(
                        variable,
                        operation,
                        f"{variable}__{operation}",
                        output_type,
                        output_unit,
                        nullable,
                        group,
                        eligibility,
                        "Observed false remains distinct from null.",
                    )
                )
        elif variable == "position_therapy":
            specs.extend(
                _numeric_specs(
                    variable,
                    group,
                    unit,
                    contract.family_operations["position_therapy"],
                )
            )
        elif variable == "therapy_read_confirmation_utc":
            for component in (1, 2):
                for operation in contract.family_operations[
                    "therapy_read_confirmation_component"
                ]:
                    if operation in {
                        "observation_count",
                        "observed_true_count",
                        "observed_false_count",
                    }:
                        output_type, output_unit, nullable = (
                            pa.int64(),
                            "count",
                            False,
                        )
                    elif operation in {
                        "last_observation_time_h",
                        "last_observation_age_h",
                    }:
                        output_type, output_unit, nullable = pa.float64(), "h", True
                    else:
                        output_type, output_unit, nullable = (
                            pa.bool_(),
                            "not_applicable",
                            True,
                        )
                    specs.append(
                        OutputSpec(
                            variable,
                            operation,
                            f"{variable}__component_{component}__{operation}",
                            output_type,
                            output_unit,
                            nullable,
                            group,
                            eligibility,
                            "Fixed-position component retained without coalescence.",
                            component_index=component - 1,
                        )
                    )
            for state in contract.family_operations[
                "therapy_read_confirmation_pair_states"
            ]:
                specs.append(
                    OutputSpec(
                        variable,
                        "pair_state_count",
                        f"{variable}__pair_state_{state}_count",
                        pa.int64(),
                        "count",
                        False,
                        group,
                        eligibility,
                        "Count of the exact nullable two-position pair state.",
                        pair_state=state,
                    )
                )
        else:
            raise HarmonizationError(
                f"Eligible variable lacks a frozen aggregation family: {variable}"
            )
    names = [spec.output_name for spec in specs]
    if len(names) != len(set(names)):
        raise HarmonizationError("Frozen aggregation registry emits duplicate names")
    if any("dose_total" in name or "administration_count" in name for name in names):
        raise HarmonizationError("Deferred medication totals reached output registry")
    return tuple(specs)
