from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
from typing import Any, Callable

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
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
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
EXPECTED_FIELDS = {
    "hospital": "hospital_id",
    "inspiratory_pressure": "insp_pressure",
    "peep": "peep",
    "reported_driving_pressure": "delta_p_reported",
}
EXPECTED_INTERPRETATIONS = {
    "absolute_inspiratory_pressure": "insp_pressure_minus_peep",
    "pressure_above_peep": "insp_pressure_unchanged",
}
EXPECTED_PRIOR_EVIDENCE = {
    "both_input_count": 6_386_755,
    "absolute_interpretation_below_zero_count": 3_625,
    "absolute_interpretation_above_sixty_count": 11,
    "reported_overlap_count": 3_231_499,
    "absolute_interpretation_within_0_01_count": 839_792,
}


@dataclass(frozen=True)
class DrivingPressureSemanticsPolicy:
    version: str
    cleaned_release_id: str
    expected_dynamic_rows: int
    rows_per_batch: int
    agreement_tolerances: tuple[float, ...]
    candidate_minimum: float
    candidate_maximum: float
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class DrivingPressureSemanticsConfig:
    derivation_review: DerivationContractReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.derivation_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.derivation_review.reports_root


@dataclass(frozen=True)
class DrivingPressureSemanticsResult:
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


def _finite_float_sequence(value: Any, location: str) -> tuple[float, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError(f"{location} must be a non-empty YAML list")
    result: list[float] = []
    for item in value:
        if not isinstance(item, (int, float)) or isinstance(item, bool):
            raise ConfigurationError(f"{location} must contain only numbers")
        numeric = float(item)
        if not math.isfinite(numeric) or numeric <= 0:
            raise ConfigurationError(f"{location} must contain positive finite numbers")
        result.append(numeric)
    if result != sorted(set(result)):
        raise ConfigurationError(f"{location} must be unique and increasing")
    return tuple(result)


def load_driving_pressure_semantics_config(
    path: str | Path,
) -> DrivingPressureSemanticsConfig:
    source = Path(path).expanduser().resolve()
    derivation_review = load_derivation_contract_review_config(source)
    raw = load_yaml_mapping(source, "Driving-pressure semantics configuration")
    policy_path = resolve_path(
        required_string(
            raw, "driving_pressure_semantics_review_policy", "config"
        ),
        source,
    )
    return DrivingPressureSemanticsConfig(
        derivation_review=derivation_review,
        policy_path=policy_path,
    )


def load_driving_pressure_semantics_policy(
    path: str | Path,
) -> DrivingPressureSemanticsPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Driving-pressure semantics review policy")
    if raw.get("driving_pressure_semantics_review_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_read_only_hospital_semantics_review":
        raise ConfigurationError("Driving-pressure semantics review policy is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    fields = _mapping(raw.get("fields"), "fields")
    interpretations = _mapping(
        raw.get("candidate_interpretations"), "candidate_interpretations"
    )
    prior = _mapping(raw.get("prior_evidence_cross_check"), "prior_evidence_cross_check")
    if fields != EXPECTED_FIELDS:
        raise ConfigurationError("Driving-pressure source fields changed")
    if interpretations != EXPECTED_INTERPRETATIONS:
        raise ConfigurationError("Driving-pressure candidate interpretations changed")
    if prior != EXPECTED_PRIOR_EVIDENCE:
        raise ConfigurationError("Prior driving-pressure evidence cross-check changed")
    expected_hospitals = input_raw.get("expected_hospitals")
    if expected_hospitals != list(EXPECTED_HOSPITALS):
        raise ConfigurationError("Approved hospital set changed")
    comparison = _mapping(raw.get("comparison"), "comparison")
    minimum = comparison.get("candidate_valid_min")
    maximum = comparison.get("candidate_valid_max")
    if minimum != 0 or maximum != 60:
        raise ConfigurationError("Driving-pressure candidate range changed")
    boundary = _mapping(raw.get("boundary"), "boundary")
    if boundary != {
        "read_released_cleaned_dynamic_rows": True,
        "write_aggregate_reports_only": True,
        "write_clinical_data": False,
        "modify_cleaned_release": False,
        "activate_formula": False,
        "mask_values": False,
        "filter_rows_or_stays": False,
        "authorize_external_data_export": False,
    }:
        raise ConfigurationError("Driving-pressure review boundary changed")
    streaming = _mapping(raw.get("streaming"), "streaming")
    outputs = _mapping(raw.get("outputs"), "outputs")
    return DrivingPressureSemanticsPolicy(
        version="0.1",
        cleaned_release_id=required_string(
            input_raw, "cleaned_release_id", "input"
        ),
        expected_dynamic_rows=_positive_int(
            input_raw.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        agreement_tolerances=_finite_float_sequence(
            comparison.get("agreement_tolerances"),
            "comparison.agreement_tolerances",
        ),
        candidate_minimum=float(minimum),
        candidate_maximum=float(maximum),
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


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _tolerance_key(value: float) -> str:
    text = f"{value:.12g}".replace(".", "_")
    return f"within_{text}"


def _range_label(value: float, minimum: float, maximum: float) -> str:
    if value < minimum:
        return "below_candidate_range_count"
    if value > maximum:
        return "above_candidate_range_count"
    return "within_candidate_range_count"


def _new_profile() -> dict[str, Any]:
    return {
        "counts": Counter(),
        "absolute_error_sum": {
            "insp_minus_peep": 0.0,
            "insp_unchanged": 0.0,
        },
        "absolute_error_max": {
            "insp_minus_peep": None,
            "insp_unchanged": None,
        },
    }


def _update_profile(
    profile: dict[str, Any],
    insp_value: Any,
    peep_value: Any,
    reported_value: Any,
    tolerances: tuple[float, ...],
    minimum: float,
    maximum: float,
) -> None:
    counts: Counter[str] = profile["counts"]
    counts["row_count"] += 1
    insp = _finite(insp_value)
    peep = _finite(peep_value)
    reported = _finite(reported_value)
    if insp_value is not None and insp is None:
        counts["nonfinite_insp_pressure_count"] += 1
    if peep_value is not None and peep is None:
        counts["nonfinite_peep_count"] += 1
    if reported_value is not None and reported is None:
        counts["nonfinite_reported_count"] += 1
    if insp is not None:
        counts["finite_insp_pressure_count"] += 1
        counts[f"insp_unchanged_{_range_label(insp, minimum, maximum)}"] += 1
    if peep is not None:
        counts["finite_peep_count"] += 1
    if reported is not None:
        counts["finite_reported_count"] += 1
        counts[f"reported_{_range_label(reported, minimum, maximum)}"] += 1
    if insp is None or peep is None:
        return

    counts["both_input_count"] += 1
    if insp < peep:
        counts["insp_less_than_peep_count"] += 1
    elif insp > peep:
        counts["insp_greater_than_peep_count"] += 1
    else:
        counts["insp_equal_to_peep_count"] += 1
    subtract = insp - peep
    counts[f"insp_minus_peep_{_range_label(subtract, minimum, maximum)}"] += 1
    if reported is None:
        return

    counts["reported_overlap_count"] += 1
    subtract_error = abs(reported - subtract)
    direct_error = abs(reported - insp)
    profile["absolute_error_sum"]["insp_minus_peep"] += subtract_error
    profile["absolute_error_sum"]["insp_unchanged"] += direct_error
    for name, error in (
        ("insp_minus_peep", subtract_error),
        ("insp_unchanged", direct_error),
    ):
        previous = profile["absolute_error_max"][name]
        profile["absolute_error_max"][name] = (
            error if previous is None else max(previous, error)
        )
        for tolerance in tolerances:
            if error <= tolerance:
                counts[f"{name}_{_tolerance_key(tolerance)}_count"] += 1

    primary = tolerances[0]
    subtract_matches = subtract_error <= primary
    direct_matches = direct_error <= primary
    if subtract_matches and direct_matches:
        counts["primary_both_interpretations_match_count"] += 1
    elif subtract_matches:
        counts["primary_only_insp_minus_peep_matches_count"] += 1
    elif direct_matches:
        counts["primary_only_insp_unchanged_matches_count"] += 1
    else:
        counts["primary_neither_interpretation_matches_count"] += 1
    if subtract < minimum:
        counts["negative_subtraction_with_reported_count"] += 1
        if subtract_matches:
            counts["negative_subtraction_reported_matches_subtraction_count"] += 1
        if direct_matches:
            counts["negative_subtraction_reported_matches_insp_count"] += 1
    elif subtract > maximum:
        counts["high_subtraction_with_reported_count"] += 1
        if subtract_matches:
            counts["high_subtraction_reported_matches_subtraction_count"] += 1
        if direct_matches:
            counts["high_subtraction_reported_matches_insp_count"] += 1


def _finalize_profile(profile: dict[str, Any]) -> dict[str, Any]:
    counts = dict(sorted(profile["counts"].items()))
    overlap = counts.get("reported_overlap_count", 0)
    comparison = {
        "mean_absolute_error_insp_minus_peep": (
            profile["absolute_error_sum"]["insp_minus_peep"] / overlap
            if overlap
            else None
        ),
        "mean_absolute_error_insp_unchanged": (
            profile["absolute_error_sum"]["insp_unchanged"] / overlap
            if overlap
            else None
        ),
        "maximum_absolute_error_insp_minus_peep": profile["absolute_error_max"][
            "insp_minus_peep"
        ],
        "maximum_absolute_error_insp_unchanged": profile["absolute_error_max"][
            "insp_unchanged"
        ],
    }
    return {"counts": counts, "comparison_errors": comparison}


def scan_driving_pressure_semantics(
    path: Path,
    policy: DrivingPressureSemanticsPolicy,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    columns = [
        EXPECTED_FIELDS["hospital"],
        EXPECTED_FIELDS["inspiratory_pressure"],
        EXPECTED_FIELDS["peep"],
        EXPECTED_FIELDS["reported_driving_pressure"],
    ]
    parquet = pq.ParquetFile(path)
    missing = [column for column in columns if column not in parquet.schema_arrow.names]
    if missing:
        raise HarmonizationError(
            "Cleaned dynamic table lacks required driving-pressure fields"
        )
    profiles = {hospital: _new_profile() for hospital in EXPECTED_HOSPITALS}
    overall = _new_profile()
    unexpected_hospitals: Counter[str] = Counter()
    missing_hospital_count = 0
    rows = 0
    for batch in parquet.iter_batches(batch_size=policy.rows_per_batch, columns=columns):
        values = batch.to_pydict()
        for hospital, insp, peep, reported in zip(
            *(values[column] for column in columns), strict=True
        ):
            rows += 1
            _update_profile(
                overall,
                insp,
                peep,
                reported,
                policy.agreement_tolerances,
                policy.candidate_minimum,
                policy.candidate_maximum,
            )
            if hospital is None:
                missing_hospital_count += 1
                continue
            hospital_key = str(hospital)
            profile = profiles.get(hospital_key)
            if profile is None:
                unexpected_hospitals[hospital_key] += 1
                continue
            _update_profile(
                profile,
                insp,
                peep,
                reported,
                policy.agreement_tolerances,
                policy.candidate_minimum,
                policy.candidate_maximum,
            )
        if progress is not None and rows % (policy.rows_per_batch * 20) == 0:
            progress(f"driving_pressure_semantics_progress dynamic_rows={rows}")
    return {
        "row_count": rows,
        "missing_hospital_count": missing_hospital_count,
        "unexpected_hospital_row_count": sum(unexpected_hospitals.values()),
        "unexpected_hospital_count": len(unexpected_hospitals),
        "overall": _finalize_profile(overall),
        "hospitals": {
            hospital: _finalize_profile(profile)
            for hospital, profile in sorted(profiles.items())
        },
    }


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


def _write_markdown(path: Path, value: str, mode: int) -> None:
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


def _markdown(payload: dict[str, Any]) -> str:
    technical = payload["technical_blocking_findings"]
    overall = payload["evidence"]["overall"]
    counts = overall["counts"]
    status = "PASS — PENDING HUMAN REVIEW" if not technical else "FAIL"
    lines = [
        "# ASIC v3 driving-pressure semantics review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input cleaned release: `{payload['cleaned_release_id']}`",
        f"- Technical status: **{'PASS' if not technical else 'FAIL'}**",
        f"- Overall status: **{status}**",
        f"- Technical blocking findings: `{len(technical)}`",
        "- Clinical data written: `false`",
        "- Cleaned release modified: `false`",
        "- Derived formula activated: `false`",
        "",
        "## Global accounting",
        "",
        f"- Dynamic rows scanned: `{payload['evidence']['row_count']}`",
        f"- Rows with both inputs: `{counts.get('both_input_count', 0)}`",
        f"- `insp_pressure - peep` below 0: `{counts.get('insp_minus_peep_below_candidate_range_count', 0)}`",
        f"- `insp_pressure - peep` above 60: `{counts.get('insp_minus_peep_above_candidate_range_count', 0)}`",
        f"- Rows also having reported driving pressure: `{counts.get('reported_overlap_count', 0)}`",
        f"- Reported agreement with subtraction within 0.01: `{counts.get('insp_minus_peep_within_0_01_count', 0)}`",
        f"- Reported agreement with unchanged inspiratory pressure within 0.01: `{counts.get('insp_unchanged_within_0_01_count', 0)}`",
        "",
        "## Hospital comparison",
        "",
        "Each row below compares reported driving pressure with the two candidate interpretations. Counts are exact aggregates; no stay identifiers or patient rows are included.",
        "",
    ]
    for hospital, profile in payload["evidence"]["hospitals"].items():
        item = profile["counts"]
        lines.append(
            f"- `{hospital}`: rows `{item.get('row_count', 0)}`; both inputs `{item.get('both_input_count', 0)}`; subtraction <0 `{item.get('insp_minus_peep_below_candidate_range_count', 0)}`; subtraction >60 `{item.get('insp_minus_peep_above_candidate_range_count', 0)}`; reported overlap `{item.get('reported_overlap_count', 0)}`; subtraction agrees ≤0.01 `{item.get('insp_minus_peep_within_0_01_count', 0)}`; unchanged inspiratory pressure agrees ≤0.01 `{item.get('insp_unchanged_within_0_01_count', 0)}`; subtraction-only agreement `{item.get('primary_only_insp_minus_peep_matches_count', 0)}`; unchanged-only agreement `{item.get('primary_only_insp_unchanged_matches_count', 0)}`."
        )
    lines.extend(
        [
            "",
            "## Human review gate",
            "",
            "Use the hospital-level evidence to approve one interpretation per hospital, explicitly retain a hospital as unresolved, or determine that a raw-source semantic review is required. No formula is activated by this audit.",
            "",
            "## Interpretation boundary",
            "",
            "- Agreement is relationship evidence, not automatic proof of ventilator semantics.",
            "- A systematic hospital difference belongs in harmonization; isolated implausible combinations belong in cleaning.",
            "- Reported and future computed driving pressure remain separate.",
            "- No value is masked, transformed, filtered, or published by this command.",
            "",
        ]
    )
    if technical:
        lines.extend(["## Technical blockers", ""])
        lines.extend(
            f"- `{item['check']}`: {item['details']}" for item in technical
        )
        lines.append("")
    return "\n".join(lines)


def run_driving_pressure_semantics_review(
    config: DrivingPressureSemanticsConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> DrivingPressureSemanticsResult:
    if config.dataset_context != "production":
        raise HarmonizationError("Driving-pressure semantics review is production-only")
    policy = load_driving_pressure_semantics_policy(config.policy_path)
    base_policy = load_derivation_contract_review_policy(
        config.derivation_review.policy_path
    )
    if policy.cleaned_release_id != base_policy.cleaned_release_id:
        raise HarmonizationError("Driving-pressure and derivation inputs disagree")
    source = load_cleaned_release_input(config.derivation_review, base_policy)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid driving-pressure semantics run ID")
    private_dir = (
        config.reports_root
        / "private"
        / policy.private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_markdown = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_markdown)):
        raise HarmonizationError("Driving-pressure semantics review run already exists")

    evidence = scan_driving_pressure_semantics(
        source.source_files["dynamic"], policy, progress
    )
    technical: list[dict[str, str]] = []
    if evidence["row_count"] != policy.expected_dynamic_rows:
        technical.append(
            {
                "check": "cleaned_dynamic_row_count_conserved",
                "details": "Scanned rows differ from the immutable cleaned release contract.",
            }
        )
    if evidence["missing_hospital_count"] or evidence["unexpected_hospital_row_count"]:
        technical.append(
            {
                "check": "hospital_scope_complete",
                "details": "Every row must carry one approved canonical hospital ID.",
            }
        )
    hospital_row_total = sum(
        profile["counts"].get("row_count", 0)
        for profile in evidence["hospitals"].values()
    )
    if hospital_row_total + evidence["missing_hospital_count"] + evidence[
        "unexpected_hospital_row_count"
    ] != evidence["row_count"]:
        technical.append(
            {
                "check": "hospital_row_accounting_complete",
                "details": "Hospital-level row counts do not conserve all scanned rows.",
            }
        )
    overall_counts = evidence["overall"]["counts"]
    observed_prior = {
        "both_input_count": overall_counts.get("both_input_count", 0),
        "absolute_interpretation_below_zero_count": overall_counts.get(
            "insp_minus_peep_below_candidate_range_count", 0
        ),
        "absolute_interpretation_above_sixty_count": overall_counts.get(
            "insp_minus_peep_above_candidate_range_count", 0
        ),
        "reported_overlap_count": overall_counts.get("reported_overlap_count", 0),
        "absolute_interpretation_within_0_01_count": overall_counts.get(
            "insp_minus_peep_within_0_01_count", 0
        ),
    }
    if observed_prior != EXPECTED_PRIOR_EVIDENCE:
        technical.append(
            {
                "check": "prior_driving_pressure_evidence_reproduced",
                "details": "The focused audit does not exactly reproduce the immutable consolidated derivation evidence.",
            }
        )
    for profile in [evidence["overall"], *evidence["hospitals"].values()]:
        counts = profile["counts"]
        overlap = counts.get("reported_overlap_count", 0)
        primary_total = sum(
            counts.get(name, 0)
            for name in (
                "primary_both_interpretations_match_count",
                "primary_only_insp_minus_peep_matches_count",
                "primary_only_insp_unchanged_matches_count",
                "primary_neither_interpretation_matches_count",
            )
        )
        if primary_total != overlap:
            technical.append(
                {
                    "check": "interpretation_classification_conserved",
                    "details": "Primary-tolerance interpretation classes do not conserve reported-overlap rows.",
                }
            )
            break
        both_inputs = counts.get("both_input_count", 0)
        relation_total = sum(
            counts.get(name, 0)
            for name in (
                "insp_less_than_peep_count",
                "insp_equal_to_peep_count",
                "insp_greater_than_peep_count",
            )
        )
        if relation_total != both_inputs:
            technical.append(
                {
                    "check": "input_relationship_accounting_conserved",
                    "details": "Inspiratory-pressure/PEEP relation classes do not conserve rows with both inputs.",
                }
            )
            break

    human = (
        {
            "check": "hospital_scoped_driving_pressure_semantics_approved",
            "details": "Choose, defer, or reject each hospital-specific interpretation before computed driving pressure is implemented.",
        },
    )
    generated = utc_timestamp()
    overall_status = "pending_human_review" if not technical else "fail"
    payload = {
        "artifact": "asic_v3_driving_pressure_semantics_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "overall_status": overall_status,
        "technical_status": "pass" if not technical else "fail",
        "blocking_findings": list(human),
        "technical_blocking_findings": technical,
        "candidate_range_cmh2o": {
            "minimum": policy.candidate_minimum,
            "maximum": policy.candidate_maximum,
        },
        "agreement_tolerances_cmh2o": list(policy.agreement_tolerances),
        "evidence": evidence,
        "cleaned_release_modified": False,
        "clinical_data_written": False,
        "derived_formula_activated": False,
        "values_masked": False,
        "rows_or_stays_filtered": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(payload)
    private_payload = {
        **payload,
        "artifact": "asic_v3_driving_pressure_semantics_review_private",
        "artifact_version": policy.private_artifact_version,
        "input_lineage": {
            "cleaned_release_manifest_sha256": sha256_file(
                source.release_manifest_path
            ),
            "cleaned_dynamic_sha256": sha256_file(source.source_files["dynamic"]),
            "driving_pressure_review_policy_sha256": sha256_file(policy.source_path),
            "derivation_contract_review_policy_sha256": sha256_file(
                base_policy.source_path
            ),
        },
    }
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(
        private_dir / "driving_pressure_semantics_manifest.json",
        private_payload,
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, payload, 0o640)
    _write_markdown(review_markdown, _markdown(payload), 0o640)
    return DrivingPressureSemanticsResult(
        run_id=selected_run,
        overall_status=overall_status,
        blocking_findings=human,
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
