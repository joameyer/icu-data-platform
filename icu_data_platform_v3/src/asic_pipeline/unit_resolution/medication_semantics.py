from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.derivation.review import (
    DerivationContractReviewConfig,
    load_cleaned_release_input,
    load_derivation_contract_review_config,
    load_derivation_contract_review_policy,
)
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.audit import HOSPITALS, _write_json, _write_text
from asic_pipeline.unit_resolution.reviewed_decisions import (
    ReviewedUnitDecisions,
    load_reviewed_unit_decisions,
)


EXPECTED_BOUNDARY = {
    "read_released_cleaned_data": True,
    "read_static_weight_for_approved_weight_linked_conversion": True,
    "read_reviewed_unit_decisions": True,
    "modify_harmonized_release": False,
    "modify_cleaned_release": False,
    "modify_derived_release": False,
    "write_clinical_data": False,
    "write_reports_only": True,
    "activate_unit_conversions": False,
    "activate_semantic_splits": False,
    "impute_null_as_zero": False,
    "convert_zero_to_null": False,
    "infer_inactive_medication_from_null": False,
    "apply_carry_forward": False,
    "filter_rows_or_stays": False,
    "include_patient_rows_or_identifiers_in_reports": False,
    "authorize_external_data_export": False,
}

EXPECTED_SCOPE_FLAGS = {
    "require_exact_null_zero_positive_negative_nonfinite_accounting": True,
    "require_every_hospital_variable_profile": True,
    "require_all_missing_hospital_variable_classification": True,
    "require_zero_and_positive_coexistence_classification": True,
    "require_conversion_zero_null_conservation": True,
    "require_semantic_split_zero_null_conservation": True,
    "require_weight_linkage_for_nonzero_weight_dependent_conversion": True,
    "prohibit_patient_rows_identifiers_raw_tokens_and_filenames_in_reports": True,
}


@dataclass(frozen=True)
class MedicationSemanticsAuditPolicy:
    version: str
    cleaned_release_id: str
    expected_static_rows: int
    expected_dynamic_rows: int
    reviewed_decisions_path: Path
    reviewed_decisions_sha256: str
    derivation_policy_path: Path
    derivation_policy_sha256: str
    expected_hospital_count: int
    expected_source_variable_count: int
    expected_split_count: int
    expected_proposed_variable_count: int
    expected_conversion_scope_count: int
    rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class MedicationSemanticsAuditConfig:
    derivation: DerivationContractReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.derivation.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.derivation.reports_root


@dataclass(frozen=True)
class MedicationSemanticsAuditResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass
class ValueCounts:
    row_count: int = 0
    null_count: int = 0
    zero_count: int = 0
    positive_count: int = 0
    negative_count: int = 0
    nonfinite_count: int = 0

    def add(self, values: pa.Array) -> None:
        if not pa.types.is_float64(values.type):
            raise HarmonizationError("Medication audit fields must be float64")
        valid = pc.fill_null(pc.is_valid(values), False)
        finite = pc.fill_null(pc.is_finite(values), False)
        zero = pc.and_(finite, pc.fill_null(pc.equal(values, 0.0), False))
        positive = pc.and_(finite, pc.fill_null(pc.greater(values, 0.0), False))
        negative = pc.and_(finite, pc.fill_null(pc.less(values, 0.0), False))
        nonfinite = pc.and_(valid, pc.invert(finite))
        self.row_count += len(values)
        self.null_count += values.null_count
        self.zero_count += _count_true(zero)
        self.positive_count += _count_true(positive)
        self.negative_count += _count_true(negative)
        self.nonfinite_count += _count_true(nonfinite)
        if not self.is_conserved:
            raise HarmonizationError("Medication value-class accounting is invalid")

    @property
    def finite_count(self) -> int:
        return self.zero_count + self.positive_count + self.negative_count

    @property
    def is_conserved(self) -> bool:
        return self.row_count == (
            self.null_count
            + self.zero_count
            + self.positive_count
            + self.negative_count
            + self.nonfinite_count
        )

    def as_dict(self) -> dict[str, int]:
        return {
            "row_count": self.row_count,
            "null_count": self.null_count,
            "finite_count": self.finite_count,
            "zero_count": self.zero_count,
            "positive_count": self.positive_count,
            "negative_count": self.negative_count,
            "nonfinite_count": self.nonfinite_count,
        }


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _count_true(mask: pa.Array) -> int:
    return int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)


def load_medication_semantics_audit_config(
    path: str | Path,
) -> MedicationSemanticsAuditConfig:
    source = Path(path).expanduser().resolve()
    derivation = load_derivation_contract_review_config(source)
    raw = load_yaml_mapping(source, "Medication value-semantics audit configuration")
    policy_path = resolve_path(
        required_string(
            raw,
            "medication_value_semantics_audit_policy",
            "config",
        ),
        source,
    )
    return MedicationSemanticsAuditConfig(
        derivation=derivation,
        policy_path=policy_path,
    )


def load_medication_semantics_audit_policy(
    path: str | Path,
) -> MedicationSemanticsAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Medication value-semantics audit policy")
    if (
        raw.get("medication_value_semantics_audit_policy_version") != "0.1"
        or raw.get("status")
        != "approved_for_complete_read_only_zero_missing_audit"
    ):
        raise ConfigurationError("Medication value-semantics audit policy is invalid")
    inputs = _mapping(raw.get("input"), "input")
    scope = _mapping(raw.get("scope"), "scope")
    streaming = _mapping(raw.get("streaming"), "streaming")
    reporting = _mapping(raw.get("reporting"), "reporting")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Medication value-semantics audit boundary changed")
    if {key: scope.get(key) for key in EXPECTED_SCOPE_FLAGS} != EXPECTED_SCOPE_FLAGS:
        raise ConfigurationError("Medication audit evidence requirements changed")
    reviewed_path = resolve_path(
        required_string(inputs, "reviewed_unit_decisions", "input"), source
    )
    reviewed_sha = required_string(
        inputs, "reviewed_unit_decisions_sha256", "input"
    )
    derivation_path = resolve_path(
        required_string(
            inputs, "derivation_contract_review_policy", "input"
        ),
        source,
    )
    derivation_sha = required_string(
        inputs, "derivation_contract_review_policy_sha256", "input"
    )
    if sha256_file(reviewed_path) != reviewed_sha:
        raise ConfigurationError("Reviewed medication semantics changed")
    if sha256_file(derivation_path) != derivation_sha:
        raise ConfigurationError("Cleaned-release lineage policy changed")
    policy = MedicationSemanticsAuditPolicy(
        version="0.1",
        cleaned_release_id=required_string(
            inputs, "cleaned_release_id", "input"
        ),
        expected_static_rows=_positive_int(
            inputs.get("expected_static_rows"), "input.expected_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            inputs.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        reviewed_decisions_path=reviewed_path,
        reviewed_decisions_sha256=reviewed_sha,
        derivation_policy_path=derivation_path,
        derivation_policy_sha256=derivation_sha,
        expected_hospital_count=_positive_int(
            scope.get("expected_hospital_count"), "scope.expected_hospital_count"
        ),
        expected_source_variable_count=_positive_int(
            scope.get("expected_source_medication_variable_count"),
            "scope.expected_source_medication_variable_count",
        ),
        expected_split_count=_positive_int(
            scope.get("expected_semantic_split_count"),
            "scope.expected_semantic_split_count",
        ),
        expected_proposed_variable_count=_positive_int(
            scope.get("expected_proposed_medication_variable_count"),
            "scope.expected_proposed_medication_variable_count",
        ),
        expected_conversion_scope_count=_positive_int(
            scope.get("expected_medication_conversion_scope_count"),
            "scope.expected_medication_conversion_scope_count",
        ),
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
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
        or policy.expected_hospital_count != 8
        or policy.expected_source_variable_count != 25
        or policy.expected_split_count != 9
        or policy.expected_proposed_variable_count != 34
        or policy.expected_conversion_scope_count != 4
    ):
        raise ConfigurationError("Medication audit scope changed")
    return policy


def _medication_kind(variable: str) -> str:
    if variable.endswith("_iv_cont"):
        return "continuous_infusion"
    if variable.endswith("_bolus"):
        return "bolus"
    return "inhaled_or_other_therapy"


def _load_uk00_weights(
    path: Path,
    policy: MedicationSemanticsAuditPolicy,
) -> tuple[dict[str, float], dict[str, int]]:
    weights: dict[str, float] = {}
    rows = 0
    uk00_rows = 0
    duplicate_uk00_stays = 0
    missing_identifier_count = 0
    valid_weight_count = 0
    invalid_or_missing_weight_count = 0
    parquet = pq.ParquetFile(path)
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
            if stay is None or hospital is None:
                missing_identifier_count += 1
                continue
            if hospital != "asic_UK00":
                continue
            uk00_rows += 1
            key = str(stay)
            if key in weights:
                duplicate_uk00_stays += 1
                continue
            if weight is None or not math.isfinite(float(weight)) or float(weight) <= 0:
                invalid_or_missing_weight_count += 1
                continue
            weights[key] = float(weight)
            valid_weight_count += 1
    if (
        rows != policy.expected_static_rows
        or missing_identifier_count
        or duplicate_uk00_stays
        or uk00_rows != valid_weight_count + invalid_or_missing_weight_count
    ):
        raise HarmonizationError("Medication audit static weight accounting changed")
    return weights, {
        "static_rows_scanned": rows,
        "uk00_static_rows": uk00_rows,
        "uk00_valid_weight_count": valid_weight_count,
        "uk00_invalid_or_missing_weight_count": invalid_or_missing_weight_count,
    }


def _scan_values(
    dynamic_path: Path,
    static_path: Path,
    schema: pa.Schema,
    policy: MedicationSemanticsAuditPolicy,
    reviewed: ReviewedUnitDecisions,
    progress: Callable[[str], None] | None,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, int],
]:
    variables = reviewed.medication_variables
    required = {"stay_id_global", "hospital_id", *variables}
    if not required.issubset(schema.names):
        raise HarmonizationError("Cleaned dynamic schema lacks medication fields")
    if any(not pa.types.is_float64(schema.field(name).type) for name in variables):
        raise HarmonizationError("A medication audit field is not float64")
    weights, weight_summary = _load_uk00_weights(static_path, policy)
    profiles = {
        (hospital, variable): ValueCounts()
        for hospital in HOSPITALS
        for variable in variables
    }
    hospital_rows = {hospital: 0 for hospital in HOSPITALS}
    weight_linkage = {
        "zero_with_valid_weight_count": 0,
        "zero_without_valid_weight_count": 0,
        "nonzero_with_valid_weight_count": 0,
        "nonzero_without_valid_weight_count": 0,
    }
    rows = 0
    missing_hospital_count = 0
    unknown_hospital_count = 0
    next_progress = 1_000_000
    columns = ["stay_id_global", "hospital_id", *variables]
    parquet = pq.ParquetFile(dynamic_path)
    for batch in parquet.iter_batches(
        batch_size=policy.rows_per_batch,
        columns=columns,
    ):
        stays = batch.column(0)
        hospitals = batch.column(1)
        missing_hospital_count += hospitals.null_count
        observed = [
            str(value)
            for value in pc.unique(hospitals).to_pylist()
            if value is not None
        ]
        for hospital in observed:
            mask = pc.fill_null(pc.equal(hospitals, hospital), False)
            count = _count_true(mask)
            if hospital not in hospital_rows:
                unknown_hospital_count += count
                continue
            hospital_rows[hospital] += count
            filtered_stays = pc.filter(stays, mask)
            for offset, variable in enumerate(variables, start=2):
                values = pc.filter(batch.column(offset), mask)
                profiles[(hospital, variable)].add(values)
                if hospital == "asic_UK00" and variable == "vasopressin_iv_cont":
                    for stay, value in zip(
                        filtered_stays.to_pylist(), values.to_pylist(), strict=True
                    ):
                        if value is None or not math.isfinite(float(value)):
                            continue
                        has_weight = stay is not None and str(stay) in weights
                        label = "zero" if float(value) == 0.0 else "nonzero"
                        suffix = "with_valid_weight_count" if has_weight else "without_valid_weight_count"
                        weight_linkage[f"{label}_{suffix}"] += 1
        rows += batch.num_rows
        if progress is not None and rows >= next_progress:
            progress(f"medication_semantics_audit_progress dynamic_rows={rows}")
            next_progress = ((rows // 1_000_000) + 1) * 1_000_000
    if (
        rows != policy.expected_dynamic_rows
        or missing_hospital_count
        or unknown_hospital_count
        or any(count <= 0 for count in hospital_rows.values())
    ):
        raise HarmonizationError("Medication audit dynamic row accounting changed")
    if any(
        profiles[(hospital, variable)].row_count != hospital_rows[hospital]
        for hospital in HOSPITALS
        for variable in variables
    ):
        raise HarmonizationError("A medication hospital-variable profile is incomplete")

    profile_rows = []
    for hospital in HOSPITALS:
        for variable in variables:
            counts = profiles[(hospital, variable)]
            profile_rows.append(
                {
                    "hospital": hospital,
                    "variable": variable,
                    "medication_kind": _medication_kind(variable),
                    **counts.as_dict(),
                    "all_missing": counts.null_count == counts.row_count,
                    "has_explicit_zero": counts.zero_count > 0,
                    "has_positive_value": counts.positive_count > 0,
                    "zero_and_positive_coexist": (
                        counts.zero_count > 0 and counts.positive_count > 0
                    ),
                    "null_is_imputed_as_zero": False,
                    "zero_is_converted_to_null": False,
                }
            )

    variable_rows = []
    for variable in variables:
        combined = ValueCounts()
        for hospital in HOSPITALS:
            counts = profiles[(hospital, variable)]
            combined.row_count += counts.row_count
            combined.null_count += counts.null_count
            combined.zero_count += counts.zero_count
            combined.positive_count += counts.positive_count
            combined.negative_count += counts.negative_count
            combined.nonfinite_count += counts.nonfinite_count
        if not combined.is_conserved:
            raise HarmonizationError("Medication variable summary is not conserved")
        variable_rows.append(
            {
                "variable": variable,
                "medication_kind": _medication_kind(variable),
                **combined.as_dict(),
                "hospital_count_with_observed_value": sum(
                    profiles[(hospital, variable)].finite_count > 0
                    for hospital in HOSPITALS
                ),
                "hospital_count_with_zero": sum(
                    profiles[(hospital, variable)].zero_count > 0
                    for hospital in HOSPITALS
                ),
                "hospital_count_with_positive": sum(
                    profiles[(hospital, variable)].positive_count > 0
                    for hospital in HOSPITALS
                ),
                "hospital_count_all_missing": sum(
                    profiles[(hospital, variable)].null_count
                    == profiles[(hospital, variable)].row_count
                    for hospital in HOSPITALS
                ),
            }
        )

    actions = [
        row
        for row in reviewed.base_candidate.hospital_harmonization_actions
        if str(row["variable"]) in variables
    ]
    if len(actions) != policy.expected_conversion_scope_count:
        raise HarmonizationError("Medication conversion scope changed")
    conversion_rows = []
    for action in actions:
        hospital = str(action["hospital"])
        variable = str(action["variable"])
        counts = profiles[(hospital, variable)]
        weight_dependent = action.get("action") == "multiply_by_static_weight_kg"
        nonzero_missing_weight = (
            weight_linkage["nonzero_without_valid_weight_count"]
            if weight_dependent
            else 0
        )
        if nonzero_missing_weight:
            raise HarmonizationError(
                "A nonzero weight-dependent medication value lacks valid weight"
            )
        conversion_rows.append(
            {
                "decision_id": action["decision_id"],
                "hospital": hospital,
                "variable": variable,
                "operation": action["action"],
                "row_count": counts.row_count,
                "null_count_before": counts.null_count,
                "null_count_projected_after": counts.null_count,
                "zero_count_before": counts.zero_count,
                "zero_count_projected_after": counts.zero_count,
                "positive_count_before": counts.positive_count,
                "positive_count_projected_after": counts.positive_count,
                "negative_count_before": counts.negative_count,
                "negative_count_projected_after": counts.negative_count,
                "nonfinite_count_before": counts.nonfinite_count,
                "nonfinite_count_projected_after": counts.nonfinite_count,
                "zero_to_null_count": 0,
                "null_to_zero_count": 0,
                "weight_dependent": weight_dependent,
                "nonzero_without_valid_weight_count": nonzero_missing_weight,
                "classification_conserved": True,
                "values_modified_by_this_audit": False,
            }
        )

    split_rows = []
    for split in reviewed.semantic_splits:
        hospital = str(split["hospital"])
        variable = str(split["source_variable"])
        counts = profiles[(hospital, variable)]
        split_rows.append(
            {
                "decision_id": split["decision_id"],
                "hospital": hospital,
                "source_variable": variable,
                "target_variable": split["target_variable"],
                "row_count": counts.row_count,
                "source_null_count": counts.null_count,
                "source_zero_count": counts.zero_count,
                "source_positive_count": counts.positive_count,
                "source_negative_count": counts.negative_count,
                "source_nonfinite_count": counts.nonfinite_count,
                "parallel_target_null_count_projected": counts.null_count,
                "parallel_target_zero_count_projected": counts.zero_count,
                "parallel_target_positive_count_projected": counts.positive_count,
                "parallel_target_negative_count_projected": counts.negative_count,
                "parallel_target_nonfinite_count_projected": counts.nonfinite_count,
                "canonical_target_null_count_projected": counts.row_count,
                "zero_to_null_in_parallel_target": 0,
                "null_to_zero_in_parallel_target": 0,
                "classification_conserved_in_parallel_target": True,
                "values_modified_by_this_audit": False,
            }
        )
    if len(split_rows) != policy.expected_split_count:
        raise HarmonizationError("Medication semantic-split scope changed")
    return (
        profile_rows,
        variable_rows,
        conversion_rows,
        split_rows,
        weight_summary
        | weight_linkage
        | {"dynamic_rows_scanned": rows}
        | {f"{hospital}_rows": count for hospital, count in hospital_rows.items()},
    )


def _markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
        "# ASIC v3 medication zero and missing-value semantics review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input cleaned release: `{payload['cleaned_release_id']}`",
        "- Technical status: **PASS**",
        "- Overall status: **PENDING HUMAN REVIEW**",
        "- Clinical data written or modified: `false`",
        "",
        "## Exact accounting",
        "",
        f"- Dynamic rows scanned: `{metrics['dynamic_rows_scanned']}`",
        f"- Medication/therapy source variables: `{metrics['source_medication_variable_count']}`",
        f"- Hospital-variable profiles: `{metrics['hospital_variable_profile_count']}`",
        f"- Explicit numeric zero values: `{metrics['explicit_zero_count']}`",
        f"- Positive values: `{metrics['positive_value_count']}`",
        f"- Negative values preserved for separate review: `{metrics['negative_value_count']}`",
        f"- Non-finite values: `{metrics['nonfinite_value_count']}`",
        f"- Null cells: `{metrics['null_count']}`",
        f"- Variables containing explicit zero: `{metrics['variables_with_explicit_zero_count']}`",
        f"- All-missing hospital-variable profiles: `{metrics['all_missing_hospital_variable_profile_count']}`",
        f"- Profiles where zero and positive values coexist: `{metrics['zero_and_positive_profile_count']}`",
        "",
        "## Variable summaries",
        "",
    ]
    for row in payload["variable_summaries"]:
        lines.append(
            f"- `{row['variable']}`: zero `{row['zero_count']}`; positive `{row['positive_count']}`; "
            f"negative `{row['negative_count']}`; null `{row['null_count']}`; hospitals with observed values "
            f"`{row['hospital_count_with_observed_value']}`."
        )
    lines.extend(
        [
            "",
            "## Transformation conservation",
            "",
            f"- Medication conversion scopes checked: `{metrics['medication_conversion_scope_count']}`",
            f"- Semantic-split scopes checked: `{metrics['semantic_split_scope_count']}`",
            "- Projected zero-to-null conversions: `0`",
            "- Projected null-to-zero imputations: `0`",
            "- Null is not interpreted as inactive medication.",
            "- Carry-forward or interval state remains deferred to a separate reviewed derivation contract.",
            "",
            "## Human review gate",
            "",
            "Approve the aggregate evidence and the explicit zero-versus-null semantics before freezing harmonized schema and dictionary contract 0.2.",
            "",
            "## Boundary",
            "",
            "No patient rows, identifiers, raw tokens, or filenames are written. This command changes no clinical value or existing release and activates no conversion, split, cleaning rule, carry-forward rule, or external export.",
            "",
        ]
    )
    return "\n".join(lines)


def run_medication_semantics_audit(
    config: MedicationSemanticsAuditConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> MedicationSemanticsAuditResult:
    if config.dataset_context != "production":
        raise HarmonizationError("Medication value-semantics audit is production-only")
    policy = load_medication_semantics_audit_policy(config.policy_path)
    reviewed = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    derivation_policy = load_derivation_contract_review_policy(
        config.derivation.policy_path
    )
    if (
        config.derivation.policy_path.resolve()
        != policy.derivation_policy_path.resolve()
        or derivation_policy.cleaned_release_id != policy.cleaned_release_id
        or derivation_policy.expected_static_rows != policy.expected_static_rows
        or derivation_policy.expected_dynamic_rows != policy.expected_dynamic_rows
        or len(reviewed.medication_variables)
        != policy.expected_source_variable_count
        or len(reviewed.semantic_splits) != policy.expected_split_count
        or len(reviewed.medication_variables) + len(reviewed.semantic_splits)
        != policy.expected_proposed_variable_count
    ):
        raise HarmonizationError("Medication audit lineage or scope changed")
    source = load_cleaned_release_input(config.derivation, derivation_policy)
    (
        profile_rows,
        variable_rows,
        conversion_rows,
        split_rows,
        scan_summary,
    ) = _scan_values(
        source.source_files["dynamic"],
        source.source_files["static"],
        source.schemas["dynamic"],
        policy,
        reviewed,
        progress,
    )
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid medication value-semantics audit run ID")
    private_dir = (
        config.reports_root
        / "private"
        / policy.private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Medication audit run ID already exists")

    explicit_zero_count = sum(int(row["zero_count"]) for row in variable_rows)
    metrics = {
        "dynamic_rows_scanned": scan_summary["dynamic_rows_scanned"],
        "static_rows_scanned": scan_summary["static_rows_scanned"],
        "source_medication_variable_count": len(reviewed.medication_variables),
        "proposed_medication_variable_count": (
            len(reviewed.medication_variables) + len(reviewed.semantic_splits)
        ),
        "hospital_variable_profile_count": len(profile_rows),
        "explicit_zero_count": explicit_zero_count,
        "positive_value_count": sum(
            int(row["positive_count"]) for row in variable_rows
        ),
        "negative_value_count": sum(
            int(row["negative_count"]) for row in variable_rows
        ),
        "nonfinite_value_count": sum(
            int(row["nonfinite_count"]) for row in variable_rows
        ),
        "null_count": sum(int(row["null_count"]) for row in variable_rows),
        "variables_with_explicit_zero_count": sum(
            int(row["zero_count"]) > 0 for row in variable_rows
        ),
        "all_missing_hospital_variable_profile_count": sum(
            row["all_missing"] is True for row in profile_rows
        ),
        "zero_and_positive_profile_count": sum(
            row["zero_and_positive_coexist"] is True for row in profile_rows
        ),
        "medication_conversion_scope_count": len(conversion_rows),
        "semantic_split_scope_count": len(split_rows),
        "projected_zero_to_null_count": 0,
        "projected_null_to_zero_count": 0,
        "nonzero_weight_dependent_values_without_valid_weight": scan_summary[
            "nonzero_without_valid_weight_count"
        ],
        "technical_blocking_finding_count": 0,
    }
    if (
        metrics["hospital_variable_profile_count"]
        != policy.expected_hospital_count * policy.expected_source_variable_count
        or metrics["medication_conversion_scope_count"]
        != policy.expected_conversion_scope_count
        or metrics["semantic_split_scope_count"] != policy.expected_split_count
        or metrics["projected_zero_to_null_count"]
        or metrics["projected_null_to_zero_count"]
        or metrics["nonzero_weight_dependent_values_without_valid_weight"]
    ):
        raise HarmonizationError("Medication zero/null conservation is invalid")

    blocker = {
        "check": "medication_zero_missing_evidence_approved",
        "details": (
            "The complete aggregate medication zero, null, sign, availability, "
            "conversion, and semantic-split accounting requires explicit review."
        ),
    }
    generated = utc_timestamp()
    payload = {
        "artifact": "asic_v3_medication_value_semantics_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "overall_status": "pending_human_review",
        "blocking_findings": [blocker],
        "technical_blocking_findings": [],
        "metrics": metrics,
        "variable_summaries": variable_rows,
        "value_semantics": reviewed.medication_value_semantics,
        "clinical_data_written": False,
        "existing_releases_modified": False,
        "unit_conversions_activated": False,
        "semantic_splits_activated": False,
        "schema_frozen": False,
        "dictionary_frozen": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
        "privacy": {
            "contains_patient_rows": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_source_filenames": False,
        },
    }
    assert_review_payload_is_safe(payload)

    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    files = {
        "hospital_variable_counts.parquet": profile_rows,
        "variable_counts.parquet": variable_rows,
        "conversion_conservation.parquet": conversion_rows,
        "semantic_split_conservation.parquet": split_rows,
    }
    output_hashes: dict[str, str] = {}
    for name, rows in files.items():
        path = private_dir / name
        _write_private_parquet(path, tuple(rows))
        path.chmod(0o600)
        output_hashes[name] = sha256_file(path)
    manifest_path = private_dir / "medication_value_semantics_audit_manifest.json"
    manifest = {
        "artifact": "asic_v3_medication_value_semantics_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "lineage_sha256": {
            "cleaned_release_manifest": sha256_file(source.release_manifest_path),
            "reviewed_unit_decisions": sha256_file(reviewed.source_path),
            "policy": sha256_file(policy.source_path),
        },
        "output_sha256": output_hashes,
        "metrics": metrics,
        "scan_summary": scan_summary,
        "clinical_data_written": False,
        "publication_ready": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, payload, 0o640)
    _write_text(review_md, _markdown(payload), 0o640)
    return MedicationSemanticsAuditResult(
        run_id=selected_run,
        overall_status="pending_human_review",
        blocking_findings=(blocker,),
        technical_blocking_findings=(),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
