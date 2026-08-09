from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.derivation.review import (
    CleanedReleaseInput,
    DerivationContractReviewConfig,
    load_cleaned_release_input,
    load_derivation_contract_review_config,
    load_derivation_contract_review_policy,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.consolidated_audit import (
    NumericAccumulator,
    _bool_count,
    _sample_positions,
    _stable_seed,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.audit import HOSPITALS, _write_json, _write_text
from asic_pipeline.unit_resolution.decisions import (
    CandidateUnitDecisions,
    load_candidate_unit_decisions,
)


EXPECTED_BOUNDARY = {
    "read_released_cleaned_data": True,
    "read_candidate_unit_decisions": True,
    "modify_harmonized_release": False,
    "modify_cleaned_release": False,
    "modify_derived_release": False,
    "write_clinical_data": False,
    "write_reports_only": True,
    "activate_candidate_decisions": False,
    "convert_persisted_values": False,
    "mask_persisted_values": False,
    "filter_rows_or_stays": False,
    "include_patient_rows_or_identifiers_in_reports": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class UnitDecisionAuditPolicy:
    version: str
    cleaned_release_id: str
    expected_static_rows: int
    expected_dynamic_rows: int
    decisions_path: Path
    decisions_sha256: str
    source_unit_audit_run_id: str
    expected_variable_count: int
    expected_action_count: int
    expected_mask_count: int
    expected_target_variable_count: int
    expected_weight_action_count: int
    rows_per_batch: int
    sample_capacity: int
    sample_rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class UnitDecisionAuditConfig:
    derivation: DerivationContractReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.derivation.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.derivation.reports_root


@dataclass(frozen=True)
class UnitDecisionAuditResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
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


def load_unit_decision_audit_config(path: str | Path) -> UnitDecisionAuditConfig:
    source = Path(path).expanduser().resolve()
    derivation = load_derivation_contract_review_config(source)
    raw = load_yaml_mapping(source, "Unit-decision audit configuration")
    policy_path = resolve_path(
        required_string(raw, "unit_decision_audit_policy", "config"), source
    )
    return UnitDecisionAuditConfig(derivation=derivation, policy_path=policy_path)


def load_unit_decision_audit_policy(path: str | Path) -> UnitDecisionAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Unit-decision audit policy")
    if raw.get("unit_decision_audit_policy_version") != "0.2" or raw.get(
        "status"
    ) != "approved_for_complete_read_only_candidate_unit_decision_audit":
        raise ConfigurationError("Unit-decision audit policy is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    scope = _mapping(raw.get("scope"), "scope")
    streaming = _mapping(raw.get("streaming"), "streaming")
    reporting = _mapping(raw.get("reporting"), "reporting")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Unit-decision audit boundary changed")
    required_scope_flags = {
        "require_full_hospital_profiles_for_every_target_variable": True,
        "require_pre_and_post_profiles_for_every_conversion": True,
        "require_peer_profile_for_every_conversion_and_mask": True,
        "require_exact_static_weight_linkage_accounting": True,
        "require_finite_aware_quantile_sampling": True,
        "require_positive_value_profiles": True,
    }
    if {key: scope.get(key) for key in required_scope_flags} != required_scope_flags:
        raise ConfigurationError("Unit-decision audit evidence requirements changed")
    decisions_path = resolve_path(
        required_string(input_raw, "candidate_unit_decisions", "input"), source
    )
    decisions_sha = required_string(
        input_raw, "candidate_unit_decisions_sha256", "input"
    )
    if sha256_file(decisions_path) != decisions_sha:
        raise ConfigurationError("Candidate unit decisions changed after audit approval")
    policy = UnitDecisionAuditPolicy(
        version="0.2",
        cleaned_release_id=required_string(
            input_raw, "cleaned_release_id", "input"
        ),
        expected_static_rows=_positive_int(
            input_raw.get("expected_static_rows"), "input.expected_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            input_raw.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        decisions_path=decisions_path,
        decisions_sha256=decisions_sha,
        source_unit_audit_run_id=required_string(
            input_raw, "source_unit_resolution_audit_run_id", "input"
        ),
        expected_variable_count=_positive_int(
            scope.get("expected_variable_count"), "scope.expected_variable_count"
        ),
        expected_action_count=_positive_int(
            scope.get("expected_harmonization_action_count"),
            "scope.expected_harmonization_action_count",
        ),
        expected_mask_count=_positive_int(
            scope.get("expected_cleaning_mask_count"),
            "scope.expected_cleaning_mask_count",
        ),
        expected_target_variable_count=_positive_int(
            scope.get("expected_target_variable_count"),
            "scope.expected_target_variable_count",
        ),
        expected_weight_action_count=_positive_int(
            scope.get("expected_weight_linked_action_count"),
            "scope.expected_weight_linked_action_count",
        ),
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        sample_capacity=_positive_int(
            streaming.get("quantile_sample_capacity_per_profile"),
            "streaming.quantile_sample_capacity_per_profile",
        ),
        sample_rows_per_batch=_positive_int(
            streaming.get("quantile_sample_rows_per_batch"),
            "streaming.quantile_sample_rows_per_batch",
        ),
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
        ),
        source_path=source,
    )
    if (
        policy.cleaned_release_id != "20260806T114234Z"
        or policy.expected_static_rows != 16_054
        or policy.expected_dynamic_rows != 24_069_379
        or policy.expected_variable_count != 72
        or policy.expected_action_count != 9
        or policy.expected_mask_count != 9
        or policy.expected_target_variable_count != 16
        or policy.expected_weight_action_count != 1
        or not RUN_ID_PATTERN.fullmatch(policy.source_unit_audit_run_id)
    ):
        raise ConfigurationError("Approved unit-decision audit scope changed")
    return policy


def _add_values(
    accumulator: NumericAccumulator,
    values: pa.Array,
    policy: UnitDecisionAuditPolicy,
) -> None:
    if pa.types.is_floating(values.type):
        finite_mask = pc.fill_null(pc.is_finite(values), False)
    else:
        finite_mask = pc.is_valid(values)
    finite_positions = pc.indices_nonzero(finite_mask)
    if len(finite_positions) <= policy.sample_rows_per_batch:
        positions = pc.cast(finite_positions, pa.int64())
    else:
        selections = _sample_positions(
            len(finite_positions),
            policy.sample_rows_per_batch,
            accumulator.row_count,
        )
        positions = pc.cast(pc.take(finite_positions, selections), pa.int64())
    accumulator.add(values, positions)


def _positive_values(values: pa.Array) -> pa.Array:
    mask = pc.fill_null(
        pc.and_(pc.is_finite(values), pc.greater(values, 0.0)),
        False,
    )
    return pc.filter(values, mask)


def _positive_fields(accumulator: NumericAccumulator) -> dict[str, Any]:
    profile = accumulator.profile(
        "dynamic", "positive_values", "positive_values", "positive_values"
    )
    return {
        "positive_finite_count": profile["finite_count"],
        "positive_min": profile["min"],
        "positive_q01": profile["q01"],
        "positive_q05": profile["q05"],
        "positive_q25": profile["q25"],
        "positive_median": profile["median"],
        "positive_mean": profile["mean"],
        "positive_q75": profile["q75"],
        "positive_q95": profile["q95"],
        "positive_q99": profile["q99"],
        "positive_max": profile["max"],
        "positive_std": profile["std"],
        "positive_quantile_sample_count": profile["quantile_sample_count"],
        "positive_quantiles_are_exact": profile["quantiles_are_exact"],
    }


def _load_static_weights(
    source: CleanedReleaseInput,
    policy: UnitDecisionAuditPolicy,
) -> tuple[dict[str, float], dict[str, int]]:
    schema = source.schemas["static"]
    required = {"stay_id_global", "hospital_id", "weight_kg"}
    if not required.issubset(schema.names):
        raise HarmonizationError("Cleaned static schema lacks weight-linkage fields")
    weights: dict[str, float] = {}
    seen_uk00_stays: set[str] = set()
    rows = 0
    duplicate_stays = 0
    missing_stays = 0
    uk00_rows = 0
    valid_weights = 0
    null_weights = 0
    invalid_weights = 0
    parquet = pq.ParquetFile(source.source_files["static"])
    for batch in parquet.iter_batches(
        batch_size=policy.rows_per_batch,
        columns=["stay_id_global", "hospital_id", "weight_kg"],
    ):
        values = batch.to_pydict()
        for stay, hospital, weight in zip(
            values["stay_id_global"],
            values["hospital_id"],
            values["weight_kg"],
            strict=True,
        ):
            rows += 1
            if stay is None:
                missing_stays += 1
                continue
            stay_text = str(stay)
            if hospital != "asic_UK00":
                continue
            uk00_rows += 1
            if stay_text in seen_uk00_stays:
                duplicate_stays += 1
                continue
            seen_uk00_stays.add(stay_text)
            if weight is None:
                null_weights += 1
                continue
            numeric = float(weight)
            if not math.isfinite(numeric) or numeric <= 0:
                invalid_weights += 1
                continue
            weights[stay_text] = numeric
            valid_weights += 1
    if rows != policy.expected_static_rows or missing_stays or duplicate_stays:
        raise HarmonizationError("Static weight-linkage accounting is invalid")
    return weights, {
        "static_rows_scanned": rows,
        "uk00_static_rows": uk00_rows,
        "uk00_valid_weight_count": valid_weights,
        "uk00_null_weight_count": null_weights,
        "uk00_invalid_weight_count": invalid_weights,
        "duplicate_static_stay_count": duplicate_stays,
        "missing_static_stay_count": missing_stays,
    }


def _simple_transform(values: pa.Array, action: dict[str, Any]) -> pa.Array:
    operation = action.get("action")
    factor = action.get("factor")
    if not isinstance(factor, (int, float)) or isinstance(factor, bool):
        raise HarmonizationError("Candidate conversion factor is invalid")
    numeric_factor = float(factor)
    if not math.isfinite(numeric_factor) or numeric_factor <= 0:
        raise HarmonizationError("Candidate conversion factor must be positive")
    if operation == "multiply":
        return pc.multiply(values, pa.scalar(numeric_factor, type=pa.float64()))
    if operation == "divide":
        return pc.divide(values, pa.scalar(numeric_factor, type=pa.float64()))
    raise HarmonizationError(f"Unsupported candidate conversion operation {operation}")


def _weight_transform(
    values: pa.Array,
    stay_values: pa.Array,
    weights: dict[str, float],
    accounting: dict[str, int],
) -> pa.Array:
    converted: list[float | None] = []
    for value, stay in zip(values.to_pylist(), stay_values.to_pylist(), strict=True):
        if value is None or not math.isfinite(float(value)):
            converted.append(None)
            continue
        accounting["finite_input_count"] += 1
        if stay is None:
            accounting["missing_stay_id_count"] += 1
            converted.append(None)
            continue
        weight = weights.get(str(stay))
        if weight is None:
            accounting["missing_or_invalid_weight_count"] += 1
            converted.append(None)
            continue
        converted.append(float(value) * weight)
        accounting["converted_with_weight_count"] += 1
    return pa.array(converted, type=pa.float64())


def _target_variables(decisions: CandidateUnitDecisions) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(row["variable"])
                for row in (
                    *decisions.hospital_harmonization_actions,
                    *decisions.hospital_cleaning_masks,
                )
            }
        )
    )


def _scan_candidate_evidence(
    source: CleanedReleaseInput,
    policy: UnitDecisionAuditPolicy,
    decisions: CandidateUnitDecisions,
    progress: Callable[[str], None] | None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, int],
    dict[str, dict[str, int]],
]:
    variables = _target_variables(decisions)
    if len(variables) != policy.expected_target_variable_count:
        raise HarmonizationError("Candidate target-variable scope changed")
    schema = source.schemas["dynamic"]
    required = {"stay_id_global", "hospital_id", *variables}
    if not required.issubset(schema.names):
        raise HarmonizationError("Cleaned dynamic schema lacks unit-decision fields")
    if any(not pa.types.is_float64(schema.field(name).type) for name in variables):
        raise HarmonizationError("A candidate unit-decision field is not float64")

    weights, static_accounting = _load_static_weights(source, policy)
    pre = {
        (hospital, variable): NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_decision_pre", hospital, variable),
        )
        for hospital in HOSPITALS
        for variable in variables
    }
    pre_positive = {
        (hospital, variable): NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_decision_pre_positive", hospital, variable),
        )
        for hospital in HOSPITALS
        for variable in variables
    }
    post = {
        str(action["decision_id"]): NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_decision_post", str(action["decision_id"])),
        )
        for action in decisions.hospital_harmonization_actions
    }
    post_positive = {
        str(action["decision_id"]): NumericAccumulator(
            policy.sample_capacity,
            _stable_seed(
                "unit_decision_post_positive", str(action["decision_id"])
            ),
        )
        for action in decisions.hospital_harmonization_actions
    }
    excluded_by_variable: dict[str, set[str]] = {name: set() for name in variables}
    for row in (
        *decisions.hospital_harmonization_actions,
        *decisions.hospital_cleaning_masks,
    ):
        excluded_by_variable[str(row["variable"])].add(str(row["hospital"]))
    peers = {
        variable: NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_decision_peers", variable),
        )
        for variable in variables
    }
    peers_positive = {
        variable: NumericAccumulator(
            policy.sample_capacity,
            _stable_seed("unit_decision_peers_positive", variable),
        )
        for variable in variables
    }
    actions_by_scope = {
        (str(row["hospital"]), str(row["variable"])): row
        for row in decisions.hospital_harmonization_actions
    }
    if len(actions_by_scope) != policy.expected_action_count:
        raise HarmonizationError("Candidate conversion scopes are not unique")
    weight_accounting = {
        str(row["decision_id"]): {
            "finite_input_count": 0,
            "converted_with_weight_count": 0,
            "missing_or_invalid_weight_count": 0,
            "missing_stay_id_count": 0,
        }
        for row in decisions.hospital_harmonization_actions
        if row.get("action") == "multiply_by_static_weight_kg"
    }
    if len(weight_accounting) != policy.expected_weight_action_count:
        raise HarmonizationError("Weight-linked action scope changed")

    rows = 0
    missing_hospital_count = 0
    unknown_hospital_count = 0
    hospital_rows = {hospital: 0 for hospital in HOSPITALS}
    next_progress = 1_000_000
    columns = ["stay_id_global", "hospital_id", *variables]
    parquet = pq.ParquetFile(source.source_files["dynamic"])
    for batch in parquet.iter_batches(batch_size=policy.rows_per_batch, columns=columns):
        stay_values = batch.column(0)
        hospital_values = batch.column(1)
        missing_hospital_count += len(hospital_values) - _bool_count(
            pc.fill_null(pc.is_valid(hospital_values), False)
        )
        observed_hospitals = [
            str(value)
            for value in pc.unique(hospital_values).to_pylist()
            if value is not None
        ]
        for hospital in observed_hospitals:
            hospital_mask = pc.fill_null(pc.equal(hospital_values, hospital), False)
            hospital_count = _bool_count(hospital_mask)
            if hospital not in hospital_rows:
                unknown_hospital_count += hospital_count
                continue
            hospital_rows[hospital] += hospital_count
            filtered_stays = pc.filter(stay_values, hospital_mask)
            for offset, variable in enumerate(variables, start=2):
                values = pc.filter(batch.column(offset), hospital_mask)
                _add_values(pre[(hospital, variable)], values, policy)
                _add_values(
                    pre_positive[(hospital, variable)],
                    _positive_values(values),
                    policy,
                )
                if hospital not in excluded_by_variable[variable]:
                    _add_values(peers[variable], values, policy)
                    _add_values(
                        peers_positive[variable],
                        _positive_values(values),
                        policy,
                    )
                action = actions_by_scope.get((hospital, variable))
                if action is None:
                    continue
                identifier = str(action["decision_id"])
                if action.get("action") == "multiply_by_static_weight_kg":
                    converted = _weight_transform(
                        values,
                        filtered_stays,
                        weights,
                        weight_accounting[identifier],
                    )
                else:
                    converted = _simple_transform(values, action)
                _add_values(post[identifier], converted, policy)
                _add_values(
                    post_positive[identifier],
                    _positive_values(converted),
                    policy,
                )
        rows += batch.num_rows
        if progress is not None and rows >= next_progress:
            progress(f"unit_decision_audit_progress dynamic_rows={rows}")
            next_progress = ((rows // 1_000_000) + 1) * 1_000_000
    if (
        rows != policy.expected_dynamic_rows
        or missing_hospital_count
        or unknown_hospital_count
        or any(count <= 0 for count in hospital_rows.values())
    ):
        raise HarmonizationError("Unit-decision dynamic row accounting changed")
    if any(
        pre[(hospital, variable)].row_count != hospital_rows[hospital]
        for hospital in HOSPITALS
        for variable in variables
    ):
        raise HarmonizationError("A hospital-variable profile is incomplete")
    if any(
        post[str(action["decision_id"])].row_count
        != hospital_rows[str(action["hospital"])]
        for action in decisions.hospital_harmonization_actions
    ):
        raise HarmonizationError("A post-conversion profile is incomplete")
    if any(
        peers[variable].row_count
        != sum(
            hospital_rows[hospital]
            for hospital in HOSPITALS
            if hospital not in excluded_by_variable[variable]
        )
        for variable in variables
    ):
        raise HarmonizationError("An unaffected-peer profile is incomplete")

    pre_profiles = [
        pre[(hospital, variable)].profile(
            "dynamic", hospital, variable, "cleaned_release_pre_decision"
        )
        | _positive_fields(pre_positive[(hospital, variable)])
        for hospital in HOSPITALS
        for variable in variables
    ]
    post_profiles = [
        post[str(action["decision_id"])].profile(
            "dynamic",
            str(action["hospital"]),
            str(action["variable"]),
            "candidate_post_harmonization_conversion",
        )
        | _positive_fields(post_positive[str(action["decision_id"])])
        | {"decision_id": str(action["decision_id"])}
        for action in decisions.hospital_harmonization_actions
    ]
    peer_profiles = [
        peers[variable].profile(
            "dynamic", "unaffected_peer_pool", variable, "cleaned_release_peer_pool"
        )
        | _positive_fields(peers_positive[variable])
        | {"excluded_hospitals": sorted(excluded_by_variable[variable])}
        for variable in variables
    ]
    return (
        pre_profiles,
        post_profiles,
        peer_profiles,
        static_accounting | {"dynamic_rows_scanned": rows} | hospital_rows,
        weight_accounting,
    )


def _positive_ratio(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    left_value = float(left)
    right_value = float(right)
    if left_value <= 0 or right_value <= 0:
        return None
    return left_value / right_value


def _log_distance(ratio: float | None) -> float | None:
    if ratio is None:
        return None
    return abs(math.log10(ratio))


def _within(value: Any, lower: Any, upper: Any) -> bool | None:
    if value is None or lower is None or upper is None:
        return None
    return float(lower) <= float(value) <= float(upper)


def _build_action_evidence(
    decisions: CandidateUnitDecisions,
    pre_profiles: list[dict[str, Any]],
    post_profiles: list[dict[str, Any]],
    peer_profiles: list[dict[str, Any]],
    weight_accounting: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    pre = {
        (str(row["hospital"]), str(row["variable"])): row for row in pre_profiles
    }
    post = {str(row["decision_id"]): row for row in post_profiles}
    peers = {str(row["variable"]): row for row in peer_profiles}
    rows: list[dict[str, Any]] = []
    for action in decisions.hospital_harmonization_actions:
        identifier = str(action["decision_id"])
        variable = str(action["variable"])
        before = pre[(str(action["hospital"]), variable)]
        after = post[identifier]
        peer = peers[variable]
        before_ratio = _positive_ratio(before.get("median"), peer.get("median"))
        after_ratio = _positive_ratio(after.get("median"), peer.get("median"))
        positive_before_ratio = _positive_ratio(
            before.get("positive_median"), peer.get("positive_median")
        )
        positive_after_ratio = _positive_ratio(
            after.get("positive_median"), peer.get("positive_median")
        )
        before_distance = _log_distance(before_ratio)
        after_distance = _log_distance(after_ratio)
        rows.append(
            {
                "decision_id": identifier,
                "hospital": action["hospital"],
                "variable": variable,
                "operation": action["action"],
                "factor": action.get("factor"),
                "source_unit": action["source_unit"],
                "target_unit": action["target_unit"],
                "candidate_confidence": action["confidence"],
                "target_row_count": before["row_count"],
                "finite_count_before": before["finite_count"],
                "finite_count_after": after["finite_count"],
                "peer_finite_count": peer["finite_count"],
                "minimum_before": before["min"],
                "q01_before": before["q01"],
                "q05_before": before["q05"],
                "q25_before": before["q25"],
                "median_before": before["median"],
                "mean_before": before["mean"],
                "q75_before": before["q75"],
                "q95_before": before["q95"],
                "q99_before": before["q99"],
                "maximum_before": before["max"],
                "minimum_after": after["min"],
                "q01_after": after["q01"],
                "q05_after": after["q05"],
                "q25_after": after["q25"],
                "median_after": after["median"],
                "mean_after": after["mean"],
                "q75_after": after["q75"],
                "q95_after": after["q95"],
                "q99_after": after["q99"],
                "maximum_after": after["max"],
                "peer_q01": peer["q01"],
                "peer_q05": peer["q05"],
                "peer_q25": peer["q25"],
                "peer_median": peer["median"],
                "peer_mean": peer["mean"],
                "peer_q75": peer["q75"],
                "peer_q95": peer["q95"],
                "peer_q99": peer["q99"],
                "positive_finite_count_before": before["positive_finite_count"],
                "positive_finite_count_after": after["positive_finite_count"],
                "peer_positive_finite_count": peer["positive_finite_count"],
                "positive_q01_before": before["positive_q01"],
                "positive_q05_before": before["positive_q05"],
                "positive_q25_before": before["positive_q25"],
                "positive_median_before": before["positive_median"],
                "positive_mean_before": before["positive_mean"],
                "positive_q75_before": before["positive_q75"],
                "positive_q95_before": before["positive_q95"],
                "positive_q99_before": before["positive_q99"],
                "positive_q01_after": after["positive_q01"],
                "positive_q05_after": after["positive_q05"],
                "positive_q25_after": after["positive_q25"],
                "positive_median_after": after["positive_median"],
                "positive_mean_after": after["positive_mean"],
                "positive_q75_after": after["positive_q75"],
                "positive_q95_after": after["positive_q95"],
                "positive_q99_after": after["positive_q99"],
                "peer_positive_q01": peer["positive_q01"],
                "peer_positive_q05": peer["positive_q05"],
                "peer_positive_q25": peer["positive_q25"],
                "peer_positive_median": peer["positive_median"],
                "peer_positive_mean": peer["positive_mean"],
                "peer_positive_q75": peer["positive_q75"],
                "peer_positive_q95": peer["positive_q95"],
                "peer_positive_q99": peer["positive_q99"],
                "median_ratio_to_peer_before": before_ratio,
                "median_ratio_to_peer_after": after_ratio,
                "positive_median_ratio_to_peer_before": positive_before_ratio,
                "positive_median_ratio_to_peer_after": positive_after_ratio,
                "log10_median_scale_distance_before": before_distance,
                "log10_median_scale_distance_after": after_distance,
                "positive_log10_median_scale_distance_before": _log_distance(
                    positive_before_ratio
                ),
                "positive_log10_median_scale_distance_after": _log_distance(
                    positive_after_ratio
                ),
                "median_alignment_improved": (
                    after_distance < before_distance
                    if before_distance is not None and after_distance is not None
                    else None
                ),
                "post_median_within_peer_q05_q95": _within(
                    after.get("median"), peer.get("q05"), peer.get("q95")
                ),
                "post_positive_median_within_peer_positive_q05_q95": _within(
                    after.get("positive_median"),
                    peer.get("positive_q05"),
                    peer.get("positive_q95"),
                ),
                "post_q05_q95_within_peer_q01_q99": (
                    _within(after.get("q05"), peer.get("q01"), peer.get("q99"))
                    is True
                    and _within(after.get("q95"), peer.get("q01"), peer.get("q99"))
                    is True
                ),
                "quantiles_are_bounded_deterministic_samples": True,
                "target_quantile_sample_count": before["quantile_sample_count"],
                "post_quantile_sample_count": after["quantile_sample_count"],
                "peer_quantile_sample_count": peer["quantile_sample_count"],
                "target_positive_quantile_sample_count": before[
                    "positive_quantile_sample_count"
                ],
                "post_positive_quantile_sample_count": after[
                    "positive_quantile_sample_count"
                ],
                "peer_positive_quantile_sample_count": peer[
                    "positive_quantile_sample_count"
                ],
                **weight_accounting.get(identifier, {}),
                "values_modified_by_this_audit": False,
            }
        )
    return rows


def _build_mask_evidence(
    decisions: CandidateUnitDecisions,
    pre_profiles: list[dict[str, Any]],
    peer_profiles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    pre = {
        (str(row["hospital"]), str(row["variable"])): row for row in pre_profiles
    }
    peers = {str(row["variable"]): row for row in peer_profiles}
    rows: list[dict[str, Any]] = []
    for mask in decisions.hospital_cleaning_masks:
        variable = str(mask["variable"])
        target = pre[(str(mask["hospital"]), variable)]
        peer = peers[variable]
        ratio = _positive_ratio(target.get("median"), peer.get("median"))
        rows.append(
            {
                "decision_id": mask["decision_id"],
                "hospital": mask["hospital"],
                "variable": variable,
                "reason": mask["reason"],
                "target_row_count": target["row_count"],
                "finite_values_proposed_for_masking": target["finite_count"],
                "peer_finite_count": peer["finite_count"],
                "target_minimum": target["min"],
                "target_q01": target["q01"],
                "target_q05": target["q05"],
                "target_q25": target["q25"],
                "target_median": target["median"],
                "target_mean": target["mean"],
                "target_q75": target["q75"],
                "target_q95": target["q95"],
                "target_q99": target["q99"],
                "target_maximum": target["max"],
                "peer_q01": peer["q01"],
                "peer_q05": peer["q05"],
                "peer_q25": peer["q25"],
                "peer_median": peer["median"],
                "peer_mean": peer["mean"],
                "peer_q75": peer["q75"],
                "peer_q95": peer["q95"],
                "peer_q99": peer["q99"],
                "target_positive_finite_count": target["positive_finite_count"],
                "peer_positive_finite_count": peer["positive_finite_count"],
                "target_positive_q01": target["positive_q01"],
                "target_positive_q05": target["positive_q05"],
                "target_positive_q25": target["positive_q25"],
                "target_positive_median": target["positive_median"],
                "target_positive_mean": target["positive_mean"],
                "target_positive_q75": target["positive_q75"],
                "target_positive_q95": target["positive_q95"],
                "target_positive_q99": target["positive_q99"],
                "peer_positive_q01": peer["positive_q01"],
                "peer_positive_q05": peer["positive_q05"],
                "peer_positive_q25": peer["positive_q25"],
                "peer_positive_median": peer["positive_median"],
                "peer_positive_mean": peer["positive_mean"],
                "peer_positive_q75": peer["positive_q75"],
                "peer_positive_q95": peer["positive_q95"],
                "peer_positive_q99": peer["positive_q99"],
                "median_ratio_to_peer": ratio,
                "log10_median_scale_distance": _log_distance(ratio),
                "target_median_within_peer_q05_q95": _within(
                    target.get("median"), peer.get("q05"), peer.get("q95")
                ),
                "target_positive_median_within_peer_positive_q05_q95": _within(
                    target.get("positive_median"),
                    peer.get("positive_q05"),
                    peer.get("positive_q95"),
                ),
                "quantiles_are_bounded_deterministic_samples": True,
                "target_quantile_sample_count": target["quantile_sample_count"],
                "peer_quantile_sample_count": peer["quantile_sample_count"],
                "target_positive_quantile_sample_count": target[
                    "positive_quantile_sample_count"
                ],
                "peer_positive_quantile_sample_count": peer[
                    "positive_quantile_sample_count"
                ],
                "values_modified_by_this_audit": False,
            }
        )
    return rows


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 candidate unit-decision audit review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input cleaned release: `{payload['cleaned_release_id']}`",
        f"- Candidate unit decisions: `{payload['candidate_unit_decisions_version']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Clinical data written or modified: `false`",
        "",
        "## Audit totals",
        "",
        f"- Dynamic rows scanned: `{metrics['dynamic_rows_scanned']}`",
        f"- Static rows scanned for weight linkage: `{metrics['static_rows_scanned']}`",
        f"- Target variables: `{metrics['target_variable_count']}`",
        f"- Hospital-variable pre-conversion profiles: `{metrics['hospital_profile_count']}`",
        f"- Proposed conversions audited: `{metrics['conversion_count']}`",
        f"- Proposed site masks audited: `{metrics['mask_count']}`",
        f"- Finite values proposed for masking: `{metrics['finite_values_proposed_for_masking']}`",
        f"- Technical blocking findings: `0`",
        "",
        "## Proposed conversion evidence",
        "",
        "Every row uses exact counts, minima, maxima, means, and bounded deterministic finite-aware Q1/Q5/Q25/median/Q75/Q95/Q99 estimates. Positive-value profiles are reported separately so structural or charted zeros cannot hide a scale discrepancy.",
        "",
    ]
    for row in payload["conversion_summary"]:
        lines.append(
            f"- `{row['decision_id']}`: finite `{row['finite_count_before']}` → "
            f"`{row['finite_count_after']}`; median `{row['median_before']}` → "
            f"`{row['median_after']}`; unaffected-peer median `{row['peer_median']}`; "
            f"alignment improved `{str(row['median_alignment_improved']).lower()}`; "
            f"positive median `{row['positive_median_before']}` → "
            f"`{row['positive_median_after']}`; peer positive median "
            f"`{row['peer_positive_median']}`."
        )
    lines.extend(["", "## Proposed mask evidence", ""])
    for row in payload["mask_summary"]:
        lines.append(
            f"- `{row['decision_id']}`: finite values `{row['finite_values_proposed_for_masking']}`; "
            f"target median `{row['target_median']}`; unaffected-peer median "
            f"`{row['peer_median']}`; target median within peer Q5–Q95 "
            f"`{str(row['target_median_within_peer_q05_q95']).lower()}`; positive "
            f"median `{row['target_positive_median']}` versus peer positive median "
            f"`{row['peer_positive_median']}`."
        )
    lines.extend(
        [
            "",
            "## Human review gate",
            "",
            "Review the complete conversion and mask workbooks. Explicitly approve, revise, or reject each action before freezing harmonized contract 0.2. This audit does not activate any unit, conversion, range rule, or mask.",
            "",
            "## Quantile boundary",
            "",
            "Counts, missingness, non-finite counts, positive counts, minima, maxima, means, and standard deviations are exact streaming results. Quantiles use deterministic finite-aware bounded samples of up to 100,000 values per profile. Every finite value is included when the profile is sparse enough; each private row records its sample count and whether the quantiles are exact.",
            "",
            "## Safety boundary",
            "",
            "No patient row, stay identifier, raw token, or filename is written. Existing harmonized, cleaned, and derived releases are unchanged. No external export is authorized.",
            "",
        ]
    )
    return "\n".join(lines)


def run_unit_decision_audit(
    config: UnitDecisionAuditConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> UnitDecisionAuditResult:
    if config.dataset_context != "production":
        raise HarmonizationError("The candidate unit-decision audit is production-only")
    policy = load_unit_decision_audit_policy(config.policy_path)
    decisions = load_candidate_unit_decisions(policy.decisions_path)
    if (
        len(decisions.variables) != policy.expected_variable_count
        or len(decisions.hospital_harmonization_actions) != policy.expected_action_count
        or len(decisions.hospital_cleaning_masks) != policy.expected_mask_count
    ):
        raise HarmonizationError("Candidate unit-decision coverage changed")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid unit-decision audit run ID")
    private_dir = config.reports_root / "private" / policy.private_directory_name / selected_run
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Unit-decision audit run ID already exists")

    derivation_policy = load_derivation_contract_review_policy(
        config.derivation.policy_path
    )
    if (
        derivation_policy.cleaned_release_id != policy.cleaned_release_id
        or derivation_policy.expected_static_rows != policy.expected_static_rows
        or derivation_policy.expected_dynamic_rows != policy.expected_dynamic_rows
    ):
        raise HarmonizationError("Unit-decision cleaned-release lineage changed")
    source = load_cleaned_release_input(config.derivation, derivation_policy)
    pre_profiles, post_profiles, peer_profiles, scan_accounting, weight_accounting = (
        _scan_candidate_evidence(source, policy, decisions, progress)
    )
    action_evidence = _build_action_evidence(
        decisions, pre_profiles, post_profiles, peer_profiles, weight_accounting
    )
    mask_evidence = _build_mask_evidence(decisions, pre_profiles, peer_profiles)
    if (
        len(pre_profiles) != len(HOSPITALS) * policy.expected_target_variable_count
        or len(post_profiles) != policy.expected_action_count
        or len(peer_profiles) != policy.expected_target_variable_count
        or len(action_evidence) != policy.expected_action_count
        or len(mask_evidence) != policy.expected_mask_count
    ):
        raise HarmonizationError("Unit-decision audit profile coverage is incomplete")
    for row in action_evidence:
        identifier = str(row["decision_id"])
        if row["operation"] == "multiply_by_static_weight_kg":
            if (
                row["finite_count_before"] != row["finite_input_count"]
                or row["finite_count_after"] != row["converted_with_weight_count"]
                or row["finite_input_count"]
                != row["converted_with_weight_count"]
                + row["missing_or_invalid_weight_count"]
                + row["missing_stay_id_count"]
            ):
                raise HarmonizationError(
                    f"Weight-linked conversion accounting failed for {identifier}"
                )
        elif row["finite_count_before"] != row["finite_count_after"]:
            raise HarmonizationError(
                f"Finite-value conversion conservation failed for {identifier}"
            )

    blocker = {
        "check": "candidate_unit_decisions_approved_after_complete_distribution_review",
        "details": "Every conversion and unrecoverable site mask requires explicit data-owner approval after review of complete aggregate distribution evidence.",
    }
    generated = utc_timestamp()
    metrics = {
        "dynamic_rows_scanned": policy.expected_dynamic_rows,
        "static_rows_scanned": policy.expected_static_rows,
        "target_variable_count": policy.expected_target_variable_count,
        "hospital_profile_count": len(pre_profiles),
        "post_conversion_profile_count": len(post_profiles),
        "peer_profile_count": len(peer_profiles),
        "conversion_count": len(action_evidence),
        "mask_count": len(mask_evidence),
        "finite_values_proposed_for_masking": sum(
            int(row["finite_values_proposed_for_masking"]) for row in mask_evidence
        ),
        "weight_linked_conversion_count": len(weight_accounting),
        "technical_blocking_finding_count": 0,
        "clinical_data_artifacts_generated": False,
    }
    conversion_summary = [
        {
            "decision_id": row["decision_id"],
            "finite_count_before": row["finite_count_before"],
            "finite_count_after": row["finite_count_after"],
            "median_before": row["median_before"],
            "median_after": row["median_after"],
            "peer_median": row["peer_median"],
            "median_alignment_improved": row["median_alignment_improved"],
            "positive_median_before": row["positive_median_before"],
            "positive_median_after": row["positive_median_after"],
            "peer_positive_median": row["peer_positive_median"],
        }
        for row in action_evidence
    ]
    mask_summary = [
        {
            "decision_id": row["decision_id"],
            "finite_values_proposed_for_masking": row[
                "finite_values_proposed_for_masking"
            ],
            "target_median": row["target_median"],
            "peer_median": row["peer_median"],
            "target_median_within_peer_q05_q95": row[
                "target_median_within_peer_q05_q95"
            ],
            "target_positive_median": row["target_positive_median"],
            "peer_positive_median": row["peer_positive_median"],
        }
        for row in mask_evidence
    ]
    review_payload = {
        "artifact": "asic_v3_unit_decision_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "candidate_unit_decisions_version": decisions.version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": [blocker],
        "technical_blocking_findings": [],
        "metrics": metrics,
        "conversion_summary": conversion_summary,
        "mask_summary": mask_summary,
        "clinical_data_written": False,
        "harmonized_release_modified": False,
        "cleaned_release_modified": False,
        "derived_release_modified": False,
        "values_converted": False,
        "values_masked": False,
        "rows_or_stays_filtered": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
        "privacy": {
            "contains_patient_rows": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_headers": False,
        },
    }
    assert_review_payload_is_safe(review_payload)

    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    private_tables: dict[str, list[dict[str, Any]]] = {
        "hospital_predecision_profiles.parquet": pre_profiles,
        "candidate_postconversion_profiles.parquet": post_profiles,
        "unaffected_peer_profiles.parquet": peer_profiles,
        "conversion_evidence.parquet": action_evidence,
        "mask_evidence.parquet": mask_evidence,
    }
    for filename, rows in private_tables.items():
        _write_private_parquet(private_dir / filename, tuple(rows))
        (private_dir / filename).chmod(0o600)
    private_readme = private_dir / "README.md"
    _write_text(
        private_readme,
        "# Owner-only candidate unit-decision audit\n\n"
        "Start with `conversion_evidence.parquet` and `mask_evidence.parquet`. "
        "They contain aggregate counts and distributions only. No patient row, "
        "stay identifier, raw token, or filename is written. Quantiles are "
        "deterministic bounded estimates; exactness and sample counts are recorded.\n",
        0o600,
    )
    manifest = {
        "artifact": "asic_v3_unit_decision_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "candidate_unit_decisions_version": decisions.version,
        "lineage": {
            "cleaned_release_manifest_sha256": sha256_file(
                source.release_manifest_path
            ),
            "candidate_unit_decisions_sha256": sha256_file(decisions.source_path),
            "unit_decision_audit_policy_sha256": sha256_file(policy.source_path),
            "source_unit_resolution_audit_run_id": policy.source_unit_audit_run_id,
        },
        "metrics": metrics,
        "scan_accounting": scan_accounting,
        "weight_conversion_accounting": weight_accounting,
        "table_row_counts": {
            filename: len(rows) for filename, rows in private_tables.items()
        },
        "file_sha256": {
            **{
                filename: sha256_file(private_dir / filename)
                for filename in private_tables
            },
            private_readme.name: sha256_file(private_readme),
        },
        "clinical_data_written": False,
        "publication_ready": False,
    }
    _write_json(private_dir / "unit_decision_audit_manifest.json", manifest, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _markdown(review_payload), 0o640)
    return UnitDecisionAuditResult(
        run_id=selected_run,
        overall_status="pending_human_review",
        blocking_findings=(blocker,),
        technical_blocking_findings=(),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
