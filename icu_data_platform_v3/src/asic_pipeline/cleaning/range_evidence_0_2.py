from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
import math
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.decisions import load_candidate_unit_decisions
from asic_pipeline.unit_resolution.harmonized_0_2_promotion import (
    load_harmonized_0_2_promotion_policy,
)


HOSPITALS = (
    "asic_UK00",
    "asic_UK01",
    "asic_UK02",
    "asic_UK03",
    "asic_UK04",
    "asic_UK06",
    "asic_UK07",
    "asic_UK08",
)
VARIABLES = (
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
SCORE_VARIABLES = frozenset(
    {"sofa_score_unspecified", "sofa_score_without_gcs"}
)
EXPECTED_BOUNDARY = {
    "read_released_harmonized_data": True,
    "write_reports_only": True,
    "modify_harmonized_release": False,
    "modify_existing_cleaned_release": False,
    "modify_existing_derived_release": False,
    "activate_cleaning_rule": False,
    "mask_or_convert_values": False,
    "filter_rows_or_stays": False,
    "write_patient_rows_or_identifiers": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class CleaningRangeEvidence02Config:
    dataset_context: str
    data_root: Path
    reports_root: Path
    policy_path: Path


@dataclass(frozen=True)
class CleaningRangeEvidence02Policy:
    release_id: str
    contract_version: str
    promotion_policy_path: Path
    candidate_decisions_path: Path
    prior_review_path: Path
    contract_directory: Path
    expected_rows: int
    rows_per_batch: int
    factors: tuple[float, ...]
    distinct_capacity: int
    integer_tolerance: float
    sofa_without_gcs_semantic_max: float
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class CleaningRangeEvidence02Result:
    run_id: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path

    @property
    def overall_status(self) -> str:
        return "pending_human_review"


@dataclass
class RangeState:
    hospital: str
    variable: str
    unit: str
    rule: str
    upper: float | None
    factors: tuple[float, ...]
    integer_tolerance: float
    sofa_without_gcs_semantic_max: float
    counts: Counter[str] = field(default_factory=Counter)
    factor_counts: Counter[str] = field(default_factory=Counter)
    outlier_frequencies: Counter[float] = field(default_factory=Counter)
    finite_min: float | None = None
    finite_max: float | None = None

    def _count(self, mask: pa.Array) -> int:
        result = pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py()
        return int(result or 0)

    def add(self, source: pa.Array, distinct_capacity: int) -> None:
        values = pc.cast(source, pa.float64(), safe=True)
        finite = pc.fill_null(pc.is_finite(values), False)
        negative = pc.and_(finite, pc.less(values, 0.0))
        zero = pc.and_(finite, pc.equal(values, 0.0))
        positive = pc.and_(finite, pc.greater(values, 0.0))
        above = (
            pc.and_(finite, pc.greater(values, self.upper))
            if self.upper is not None
            else pa.array([False] * len(values), type=pa.bool_())
        )
        within = (
            pc.and_(positive, pc.less_equal(values, self.upper))
            if self.upper is not None
            else positive
        )
        self.counts["cell_count"] += len(values)
        self.counts["finite_count"] += self._count(finite)
        self.counts["null_or_nonfinite_count"] += len(values) - self._count(finite)
        self.counts["negative_count"] += self._count(negative)
        self.counts["zero_count"] += self._count(zero)
        self.counts["positive_within_count"] += self._count(within)
        self.counts["above_upper_count"] += self._count(above)

        finite_values = pc.filter(values, finite)
        if len(finite_values):
            batch_min = float(pc.min(finite_values).as_py())
            batch_max = float(pc.max(finite_values).as_py())
            self.finite_min = (
                batch_min if self.finite_min is None else min(self.finite_min, batch_min)
            )
            self.finite_max = (
                batch_max if self.finite_max is None else max(self.finite_max, batch_max)
            )

        fractional = pa.array([False] * len(values), type=pa.bool_())
        if self.variable in SCORE_VARIABLES:
            rounded = pc.round(values, ndigits=0)
            fractional = pc.and_(
                finite,
                pc.greater(pc.abs(pc.subtract(values, rounded)), self.integer_tolerance),
            )
            self.counts["fractional_score_count"] += self._count(fractional)
            if self.variable == "sofa_score_without_gcs":
                semantic_above = pc.and_(
                    finite,
                    pc.greater(values, self.sofa_without_gcs_semantic_max),
                )
                self.counts["above_semantic_max_20_count"] += self._count(
                    semantic_above
                )

        if self.upper is None:
            legacy_invalid = zero
        else:
            legacy_invalid = pc.or_(pc.or_(negative, zero), above)
        self.counts["legacy_rule_violation_count"] += self._count(legacy_invalid)

        if self.upper is not None and self._count(above):
            candidate_masks: list[pa.Array] = []
            for factor in self.factors:
                transformed = pc.multiply(values, factor)
                valid = pc.and_(
                    above,
                    pc.and_(
                        pc.greater(transformed, 0.0),
                        pc.less_equal(transformed, self.upper),
                    ),
                )
                count = self._count(valid)
                self.factor_counts[str(factor)] += count
                candidate_masks.append(pc.cast(valid, pa.int8()))
            recovery_count = candidate_masks[0]
            for candidate in candidate_masks[1:]:
                recovery_count = pc.add(recovery_count, candidate)
            self.counts["above_no_factor_count"] += self._count(
                pc.and_(above, pc.equal(recovery_count, 0))
            )
            self.counts["above_unique_factor_count"] += self._count(
                pc.and_(above, pc.equal(recovery_count, 1))
            )
            self.counts["above_multiple_factor_count"] += self._count(
                pc.and_(above, pc.greater(recovery_count, 1))
            )

        decision_relevant = pc.or_(pc.or_(negative, zero), pc.or_(above, fractional))
        relevant_values = pc.filter(values, decision_relevant)
        if len(relevant_values):
            for item in pc.value_counts(relevant_values).to_pylist():
                self.outlier_frequencies[float(item["values"])] += int(item["counts"])
            if len(self.outlier_frequencies) > distinct_capacity:
                raise HarmonizationError(
                    "Complete outlier-frequency evidence exceeds the reviewed capacity"
                )

    def profile(self) -> dict[str, Any]:
        return {
            "hospital": self.hospital,
            "variable": self.variable,
            "unit": self.unit,
            "rule": self.rule,
            "reviewed_upper": self.upper,
            **{key: int(value) for key, value in sorted(self.counts.items())},
            "finite_min": self.finite_min,
            "finite_max": self.finite_max,
            "factor_candidate_counts": dict(sorted(self.factor_counts.items())),
            "distinct_decision_relevant_value_count": len(self.outlier_frequencies),
            "complete_frequency_evidence": True,
        }


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _verify_hash(path: Path, expected: str, label: str) -> None:
    if sha256_file(path) != expected:
        raise ConfigurationError(f"Immutable {label} changed")


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def load_cleaning_range_evidence_0_2_config(
    path: str | Path,
) -> CleaningRangeEvidence02Config:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning range-evidence configuration")
    if raw.get("dataset_context") != "production":
        raise ConfigurationError("Cleaning range-evidence audit is production-only")
    paths = _mapping(raw.get("paths"), "config.paths")
    return CleaningRangeEvidence02Config(
        dataset_context="production",
        data_root=resolve_path(required_string(paths, "data", "config.paths"), source),
        reports_root=resolve_path(
            required_string(paths, "reports", "config.paths"), source
        ),
        policy_path=resolve_path(
            required_string(raw, "cleaning_range_evidence_0_2", "config"), source
        ),
    )


def load_cleaning_range_evidence_0_2_policy(
    path: str | Path,
) -> CleaningRangeEvidence02Policy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Cleaning range-evidence policy")
    if (
        raw.get("cleaning_range_evidence_audit_version") != "0.1"
        or raw.get("status")
        != "approved_for_read_only_direction_and_recoverability_audit"
    ):
        raise ConfigurationError("Cleaning range-evidence policy status is invalid")
    inputs = _mapping(raw.get("inputs"), "inputs")

    def immutable(path_key: str, hash_key: str, label: str) -> Path:
        resolved = resolve_path(required_string(inputs, path_key, "inputs"), source)
        _verify_hash(resolved, required_string(inputs, hash_key, "inputs"), label)
        return resolved

    promotion = immutable(
        "harmonized_promotion_policy",
        "harmonized_promotion_policy_sha256",
        "harmonized promotion policy",
    )
    candidate = immutable(
        "candidate_unit_decisions",
        "candidate_unit_decisions_sha256",
        "candidate unit decisions",
    )
    prior_review = immutable(
        "prior_cleaning_policy_review",
        "prior_cleaning_policy_review_sha256",
        "prior cleaning-policy review",
    )
    scope = _mapping(raw.get("scope"), "scope")
    scan = _mapping(raw.get("scan"), "scan")
    reporting = _mapping(raw.get("reporting"), "reporting")
    if (
        inputs.get("harmonized_release_id") != "20260807T112402Z"
        or inputs.get("harmonized_contract_version") != "0.2"
        or inputs.get("frozen_contract_directory")
        != "harmonized_schema_dictionary/0.2"
        or scope.get("expected_dynamic_rows") != 24_069_379
        or scope.get("expected_hospital_count") != 8
        or scope.get("expected_variable_count") != 12
        or tuple(scope.get("hospitals", ())) != HOSPITALS
        or tuple(scope.get("variables", ())) != VARIABLES
        or scan.get("rows_per_batch") != 50_000
        or scan.get("power_of_ten_factors")
        != [0.001, 0.01, 0.1, 10, 100, 1000]
        or scan.get("require_complete_outlier_frequency_evidence") is not True
        or scan.get("maximum_distinct_outlier_values_per_hospital_variable")
        != 100_000
        or float(scan.get("score_integer_tolerance")) != 1e-9
        or scan.get("sofa_without_gcs_semantic_max_candidate") != 20
        or _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY
    ):
        raise ConfigurationError("Cleaning range-evidence scope changed")
    return CleaningRangeEvidence02Policy(
        release_id="20260807T112402Z",
        contract_version="0.2",
        promotion_policy_path=promotion,
        candidate_decisions_path=candidate,
        prior_review_path=prior_review,
        contract_directory=Path("harmonized_schema_dictionary/0.2"),
        expected_rows=24_069_379,
        rows_per_batch=50_000,
        factors=(0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        distinct_capacity=100_000,
        integer_tolerance=1e-9,
        sofa_without_gcs_semantic_max=20.0,
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


def _contained(path: Path, root: Path, label: str) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise HarmonizationError(f"{label} escaped the v3 data root") from exc
    return resolved


def _dynamic_input(
    config: CleaningRangeEvidence02Config,
    policy: CleaningRangeEvidence02Policy,
) -> tuple[Path, str]:
    promotion = load_harmonized_0_2_promotion_policy(policy.promotion_policy_path)
    if (
        promotion.release_id != policy.release_id
        or promotion.contract_version != policy.contract_version
    ):
        raise HarmonizationError("Approved harmonized input lineage changed")
    contract_path = (
        config.data_root
        / "contracts"
        / policy.contract_directory
        / "freeze_manifest.json"
    )
    contract = _read_json(contract_path, "Frozen contract 0.2")
    pointer = _read_json(
        config.data_root / "harmonized/current_release.json",
        "Current harmonized release pointer",
    )
    manifest_path = _contained(
        Path(str(pointer.get("release_manifest", ""))),
        config.data_root,
        "Current release manifest",
    )
    manifest = _read_json(manifest_path, "Harmonized release manifest")
    if (
        contract.get("contract_version") != policy.contract_version
        or contract.get("schema_frozen") is not True
        or contract.get("dictionary_frozen") is not True
        or pointer.get("release_id") != policy.release_id
        or pointer.get("frozen_contract_version") != policy.contract_version
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
        or pointer.get("cleaning_input_approved") is not True
        or manifest.get("release_id") != policy.release_id
        or manifest.get("frozen_contract_version") != policy.contract_version
        or manifest.get("cleaning_input_approved") is not True
    ):
        raise HarmonizationError("Current harmonized release is not approved input 0.2")
    files = _mapping(manifest.get("files"), "release_manifest.files")
    dynamic_record = _mapping(files.get("dynamic"), "release_manifest.files.dynamic")
    dynamic_path = _contained(
        Path(required_string(dynamic_record, "path", "release_manifest.files.dynamic")),
        config.data_root,
        "Harmonized dynamic Parquet",
    )
    dynamic_hash = sha256_file(dynamic_path)
    if (
        dynamic_record.get("row_count") != policy.expected_rows
        or dynamic_record.get("sha256") != dynamic_hash
        or pq.ParquetFile(dynamic_path).metadata.num_rows != policy.expected_rows
    ):
        raise HarmonizationError("Harmonized dynamic input changed")
    return dynamic_path, dynamic_hash


def _upper(decision: dict[str, Any]) -> float | None:
    value = decision.get("hard_max_inclusive")
    return float(value) if value is not None else None


def _weighted_quantile(frequencies: Counter[float], probability: float) -> float | None:
    total = sum(frequencies.values())
    if not total:
        return None
    target = probability * (total - 1)
    lower_index = math.floor(target)
    upper_index = math.ceil(target)

    def at(index: int) -> float:
        cumulative = 0
        for value, count in sorted(frequencies.items()):
            cumulative += count
            if cumulative > index:
                return value
        raise AssertionError("Weighted quantile accounting failed")

    lower = at(lower_index)
    upper = at(upper_index)
    return lower + (upper - lower) * (target - lower_index)


def _frequency_category(value: float, state: RangeState) -> str:
    if value < 0:
        return "negative"
    if value == 0:
        return "zero"
    if state.upper is not None and value > state.upper:
        return "above_upper"
    if state.variable in SCORE_VARIABLES and abs(value - round(value)) > state.integer_tolerance:
        return "fractional_score"
    return "other_review_relevant"


def _aggregate(states: list[RangeState]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for variable in VARIABLES:
        matching = [state for state in states if state.variable == variable]
        counts: Counter[str] = Counter()
        frequencies: Counter[float] = Counter()
        factors: Counter[str] = Counter()
        for state in matching:
            counts.update(state.counts)
            frequencies.update(state.outlier_frequencies)
            factors.update(state.factor_counts)
        first = matching[0]
        summaries.append(
            {
                "variable": variable,
                "unit": first.unit,
                "rule": first.rule,
                "reviewed_upper": first.upper,
                **{key: int(value) for key, value in sorted(counts.items())},
                "affected_hospital_count": sum(
                    state.counts["legacy_rule_violation_count"] > 0
                    or state.counts["fractional_score_count"] > 0
                    for state in matching
                ),
                "outlier_min": min(frequencies) if frequencies else None,
                "outlier_q05": _weighted_quantile(frequencies, 0.05),
                "outlier_q25": _weighted_quantile(frequencies, 0.25),
                "outlier_median": _weighted_quantile(frequencies, 0.5),
                "outlier_q75": _weighted_quantile(frequencies, 0.75),
                "outlier_q95": _weighted_quantile(frequencies, 0.95),
                "outlier_max": max(frequencies) if frequencies else None,
                "outlier_quantiles_exact": True,
                "factor_candidate_counts": dict(sorted(factors.items())),
            }
        )
    return summaries


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 cleaning 0.2 range-direction evidence review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input harmonized release: `{payload['harmonized_release_id']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Technical blocking findings: `0`",
        "- Human blocking findings: `1`",
        f"- Dynamic rows scanned: `{payload['metrics']['dynamic_rows_scanned']}`",
        f"- Hospital-variable profiles: `{payload['metrics']['hospital_variable_profile_count']}`",
        "- Clinical data written or modified: `false`",
        "",
        "## Complete variable accounting",
        "",
    ]
    for row in payload["variable_summaries"]:
        lines.append(
            f"- `{row['variable']}` ({row['unit']}): finite `{row.get('finite_count', 0)}`; "
            f"negative `{row.get('negative_count', 0)}`; zero `{row.get('zero_count', 0)}`; "
            f"within `{row.get('positive_within_count', 0)}`; above upper `{row.get('above_upper_count', 0)}`; "
            f"fractional score `{row.get('fractional_score_count', 0)}`; "
            f"unique factor `{row.get('above_unique_factor_count', 0)}`; "
            f"multiple factors `{row.get('above_multiple_factor_count', 0)}`; "
            f"no factor `{row.get('above_no_factor_count', 0)}`."
        )
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "- Negative, zero, and upper-tail findings are reported separately; no category is automatically declared an input error.",
            "- Power-of-ten evidence is deterministic arithmetic screening, not permission to correct a value.",
            "- Exact decision-relevant value frequencies and all hospital profiles remain in the owner-only bundle.",
            "- SOFA-without-GCS is evaluated against both the inherited 24-point ceiling and the structural 20-point candidate; its current all-missing state is preserved.",
            "",
            "## Human review gate",
            "",
            "Use these complete aggregates to revise or approve each range disposition. No cleaned 0.2 candidate may be built from the earlier policy review alone.",
            "",
            "## Boundary",
            "",
            "This audit does not mask, convert, filter, clean, derive, or publish any clinical value. It writes aggregate reports only and preserves every existing release.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_json(path: Path, value: Any, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def _write_text(path: Path, value: str, mode: int) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value)


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
    path.chmod(0o600)


def run_cleaning_range_evidence_0_2(
    config: CleaningRangeEvidence02Config,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> CleaningRangeEvidence02Result:
    policy = load_cleaning_range_evidence_0_2_policy(config.policy_path)
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise ConfigurationError("Cleaning range-evidence run ID is invalid")
    dynamic_path, dynamic_hash = _dynamic_input(config, policy)
    decisions = load_candidate_unit_decisions(policy.candidate_decisions_path)
    if set(decisions.priority_cleaning_range_decisions) != set(VARIABLES):
        raise HarmonizationError("Priority cleaning-range coverage changed")

    parquet = pq.ParquetFile(dynamic_path)
    required = ("hospital_id", *VARIABLES)
    missing = set(required) - set(parquet.schema_arrow.names)
    if missing:
        raise HarmonizationError("Harmonized input lacks priority range variables")
    for variable in VARIABLES:
        field = parquet.schema_arrow.field(variable)
        if not pa.types.is_float64(field.type):
            raise HarmonizationError("Priority range variables must be float64")

    states = {
        (hospital, variable): RangeState(
            hospital=hospital,
            variable=variable,
            unit=str(decisions.priority_cleaning_range_decisions[variable]["unit"]),
            rule=str(decisions.priority_cleaning_range_decisions[variable]["rule"]),
            upper=_upper(decisions.priority_cleaning_range_decisions[variable]),
            factors=policy.factors,
            integer_tolerance=policy.integer_tolerance,
            sofa_without_gcs_semantic_max=policy.sofa_without_gcs_semantic_max,
        )
        for hospital in HOSPITALS
        for variable in VARIABLES
    }
    rows = 0
    hospital_rows: Counter[str] = Counter()
    next_progress = 1_000_000
    for batch in parquet.iter_batches(
        batch_size=policy.rows_per_batch,
        columns=list(required),
    ):
        hospital_column = batch.column(0)
        if pc.any(pc.is_null(hospital_column)).as_py():
            raise HarmonizationError("Harmonized hospital identifier is missing")
        observed = {str(item) for item in pc.unique(hospital_column).to_pylist()}
        if not observed <= set(HOSPITALS):
            raise HarmonizationError("Unknown hospital in harmonized dynamic data")
        for hospital in observed:
            mask = pc.fill_null(pc.equal(hospital_column, hospital), False)
            count = int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)
            hospital_rows[hospital] += count
            for index, variable in enumerate(VARIABLES, start=1):
                states[(hospital, variable)].add(
                    pc.filter(batch.column(index), mask),
                    policy.distinct_capacity,
                )
        rows += batch.num_rows
        if progress is not None and rows >= next_progress:
            progress(f"cleaning_range_evidence_progress dynamic_rows={rows}")
            next_progress = ((rows // 1_000_000) + 1) * 1_000_000
    if (
        rows != policy.expected_rows
        or set(hospital_rows) != set(HOSPITALS)
        or sum(hospital_rows.values()) != rows
    ):
        raise HarmonizationError("Dynamic row or hospital accounting changed")

    ordered_states = [
        states[(hospital, variable)]
        for variable in VARIABLES
        for hospital in HOSPITALS
    ]
    profiles = [state.profile() for state in ordered_states]
    frequencies = [
        {
            "hospital": state.hospital,
            "variable": state.variable,
            "category": _frequency_category(value, state),
            "value": value,
            "count": count,
        }
        for state in ordered_states
        for value, count in sorted(state.outlier_frequencies.items())
    ]
    summaries = _aggregate(ordered_states)
    blocker = {
        "check": "cleaning_0_2_range_dispositions_approved",
        "details": (
            "The data owner must use the complete direction, hospital, score, "
            "and recoverability evidence to approve or revise all 12 rules."
        ),
    }
    metrics = {
        "dynamic_rows_scanned": rows,
        "hospital_count": len(hospital_rows),
        "range_variable_count": len(VARIABLES),
        "hospital_variable_profile_count": len(profiles),
        "decision_relevant_distinct_frequency_row_count": len(frequencies),
        "legacy_rule_violation_count_after_harmonization_0_2": sum(
            int(row.get("legacy_rule_violation_count", 0)) for row in summaries
        ),
        "negative_count": sum(int(row.get("negative_count", 0)) for row in summaries),
        "zero_count": sum(int(row.get("zero_count", 0)) for row in summaries),
        "above_upper_count": sum(
            int(row.get("above_upper_count", 0)) for row in summaries
        ),
        "fractional_score_count": sum(
            int(row.get("fractional_score_count", 0)) for row in summaries
        ),
        "clinical_values_modified": 0,
        "technical_blocking_finding_count": 0,
    }
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_cleaning_range_evidence_0_2_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "harmonized_release_id": policy.release_id,
        "harmonized_contract_version": policy.contract_version,
        "technical_status": "pass",
        "overall_status": "pending_human_review",
        "blocking_findings": [blocker],
        "technical_blocking_findings": [],
        "metrics": metrics,
        "hospital_row_counts": dict(sorted(hospital_rows.items())),
        "variable_summaries": summaries,
        "complete_hospital_profiles_owner_only": True,
        "complete_outlier_frequencies_owner_only": True,
        "clinical_data_written_or_modified": False,
        "existing_releases_modified": False,
        "cleaning_rules_activated": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_dir = (
        config.reports_root / "private" / policy.private_directory_name / selected
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Cleaning range-evidence output already exists")
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_parquet(private_dir / "hospital_variable_profiles.parquet", profiles)
    _write_parquet(
        private_dir / "decision_relevant_value_frequencies.parquet", frequencies
    )
    private_manifest = {
        "artifact": "asic_v3_cleaning_range_evidence_0_2_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected,
        "harmonized_release_id": policy.release_id,
        "metrics": metrics,
        "dynamic_input_sha256": dynamic_hash,
        "complete_frequency_evidence": True,
        "patient_rows_or_identifiers_written": False,
        "clinical_data_modified": False,
    }
    _write_json(private_dir / "audit_manifest.json", private_manifest, 0o600)
    _write_json(review_json, review_payload, 0o640)
    _write_text(review_md, _markdown(review_payload), 0o640)
    return CleaningRangeEvidence02Result(
        run_id=selected,
        blocking_findings=(blocker,),
        technical_blocking_findings=(),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
