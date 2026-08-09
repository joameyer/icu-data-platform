from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe

if TYPE_CHECKING:
    from asic_pipeline.cleaning.promotion import CleanedPromotionConfig


EXPECTED_ELAPSED_RECIPE = {
    "input": "minutes_since_icu_admission",
    "output": "hours_since_icu_admission",
    "formula": "minutes_since_icu_admission_divided_by_60",
    "output_type": "float64",
    "output_unit": "h",
    "preserve_input": True,
}

EXPECTED_DRIVING_PRESSURE_RECIPE = {
    "inspiratory_pressure_input": "insp_pressure",
    "peep_input": "peep",
    "reported_comparison": "delta_p_reported",
    "output": "delta_p_computed",
    "formula": "insp_pressure_minus_peep",
    "output_type": "float64",
    "output_unit": "cmH2O",
    "candidate_valid_min": 0,
    "candidate_valid_max": 60,
    "reported_field_remains_separate": True,
    "source_inputs_remain_available": True,
}

EXPECTED_MORTALITY_RECIPE = {
    "detailed_status_input": "death_status",
    "reported_hospital_input": "hospital_mortality_reported",
    "discharge_status_audit_only_input": "discharge_status",
    "hospital_output": "hospital_mortality",
    "icu_output": "icu_mortality",
    "conflict_output": "mortality_source_conflict",
    "detailed_status_implications": {
        "discharged_alive": {"hospital_mortality": False, "icu_mortality": False},
        "died_in_icu": {"hospital_mortality": True, "icu_mortality": True},
        "died_in_hospital": {"hospital_mortality": True, "icu_mortality": False},
    },
    "detailed_missing_fallback": {
        "reported_false": {"hospital_mortality": False, "icu_mortality": False},
        "reported_true": {"hospital_mortality": True, "icu_mortality": None},
        "reported_missing": {"hospital_mortality": None, "icu_mortality": None},
    },
    "disagreement_action": {
        "hospital_mortality": "set_missing",
        "icu_mortality": "set_missing",
        "mortality_source_conflict": True,
    },
    "preserve_all_source_fields": True,
    "discharge_status_affects_output": False,
}

EXPECTED_VENTILATION_RECIPE = {
    "time_input": "minutes_since_icu_admission",
    "marker_inputs": ["fio2", "peep", "vt", "vt_per_kg_ideal_body_weight"],
    "timestamp_supported_when": "any_marker_non_missing",
    "maximum_continuity_gap_hours": 8,
    "gap_is_inclusive": True,
    "minimum_episode_duration_hours": 24,
    "duration_is_inclusive": True,
    "use_direct_cleaned_timestamps": True,
    "sort_supported_timestamps_within_stay_by_elapsed_time": True,
    "candidate_icu_episode_window": "nonnegative_minutes_since_icu_admission",
    "preserve_negative_time_source_rows": True,
    "claim_ground_truth_ventilation": False,
    "filter_stays_or_rows": False,
    "proposed_static_outputs": [
        {"name": "ventilation_supported_timestamp_count", "physical_type": "int64", "unit": "count"},
        {"name": "ventilation_episode_count", "physical_type": "int32", "unit": "count"},
        {"name": "maximum_observed_ventilation_episode_hours", "physical_type": "float64", "unit": "h"},
        {"name": "observed_mechanical_ventilation_ge_24h", "physical_type": "bool", "unit": "not_applicable"},
        {"name": "icu_recording_extent_hours", "physical_type": "float64", "unit": "h"},
    ],
}

EXPECTED_SEPARATIONS = {
    "preserve_reported_and_computed_driving_pressure_separately": True,
    "preserve_sofa_variants_separately": True,
    "preserve_isofa_variants_separately": True,
    "do_not_compute_or_coalesce_sofa_or_isofa": True,
    "mortality_sources_remain_available": True,
    "ventilation_flag_is_not_a_cohort_filter": True,
}


@dataclass(frozen=True)
class DerivationContractReviewPolicy:
    version: str
    cleaned_release_id: str
    cleaned_promotion_policy_path: Path
    cleaned_promotion_policy_sha256: str
    expected_static_rows: int
    expected_dynamic_rows: int
    rows_per_batch: int
    private_directory_name: str
    review_directory_name: str
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class DerivationContractReviewConfig:
    cleaned_promotion: CleanedPromotionConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.cleaned_promotion.dataset_context

    @property
    def data_root(self) -> Path:
        return self.cleaned_promotion.data_root

    @property
    def reports_root(self) -> Path:
        return self.cleaned_promotion.reports_root


@dataclass(frozen=True)
class DerivationContractReviewResult:
    run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    technical_blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class CleanedReleaseInput:
    release_directory: Path
    release_manifest_path: Path
    release_manifest: dict[str, Any]
    source_files: dict[str, Path]
    schemas: dict[str, pa.Schema]


@dataclass
class StayTimelineState:
    hospital: str
    row_count: int = 0
    last_time_minutes: float | None = None
    max_time_minutes: float | None = None
    supported_timestamp_count: int = 0
    negative_supported_timestamp_count: int = 0
    all_time_episode_count: int = 0
    all_time_maximum_episode_duration_hours: float = 0.0
    nonnegative_time_episode_count: int = 0
    nonnegative_time_maximum_episode_duration_hours: float = 0.0


def _episode_summary(
    times_minutes: list[float],
    maximum_gap_minutes: float,
) -> tuple[int, float]:
    if not times_minutes:
        return 0, 0.0
    ordered = sorted(times_minutes)
    episode_count = 1
    episode_start = ordered[0]
    previous = ordered[0]
    maximum_duration = 0.0
    for value in ordered[1:]:
        if value - previous > maximum_gap_minutes:
            maximum_duration = max(
                maximum_duration, (previous - episode_start) / 60.0
            )
            episode_count += 1
            episode_start = value
        previous = value
    maximum_duration = max(maximum_duration, (previous - episode_start) / 60.0)
    return episode_count, maximum_duration


def _finalize_stay_episodes(
    state: StayTimelineState,
    supported_times_minutes: list[float],
) -> None:
    state.supported_timestamp_count = len(supported_times_minutes)
    state.negative_supported_timestamp_count = sum(
        value < 0 for value in supported_times_minutes
    )
    (
        state.all_time_episode_count,
        state.all_time_maximum_episode_duration_hours,
    ) = _episode_summary(supported_times_minutes, 8.0 * 60.0)
    nonnegative = [value for value in supported_times_minutes if value >= 0]
    (
        state.nonnegative_time_episode_count,
        state.nonnegative_time_maximum_episode_duration_hours,
    ) = _episode_summary(nonnegative, 8.0 * 60.0)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def load_derivation_contract_review_config(
    path: str | Path,
) -> DerivationContractReviewConfig:
    from asic_pipeline.cleaning.promotion import load_cleaned_promotion_config

    source = Path(path).expanduser().resolve()
    cleaned_promotion = load_cleaned_promotion_config(source)
    raw = load_yaml_mapping(source, "Derivation contract review configuration")
    policy_path = resolve_path(
        required_string(raw, "derivation_contract_review_policy", "config"), source
    )
    return DerivationContractReviewConfig(
        cleaned_promotion=cleaned_promotion,
        policy_path=policy_path,
    )


def load_derivation_contract_review_policy(
    path: str | Path,
) -> DerivationContractReviewPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Derivation contract review policy")
    if raw.get("derivation_contract_review_policy_version") != "0.1" or raw.get(
        "status"
    ) != "approved_for_consolidated_read_only_derivation_review":
        raise ConfigurationError("Derivation contract review policy is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    recipes = _mapping(raw.get("candidate_recipes"), "candidate_recipes")
    if _mapping(recipes.get("exact_elapsed_hours"), "candidate_recipes.exact_elapsed_hours") != EXPECTED_ELAPSED_RECIPE:
        raise ConfigurationError("Elapsed-hours candidate recipe changed")
    if _mapping(recipes.get("computed_driving_pressure"), "candidate_recipes.computed_driving_pressure") != EXPECTED_DRIVING_PRESSURE_RECIPE:
        raise ConfigurationError("Driving-pressure candidate recipe changed")
    if _mapping(recipes.get("mortality"), "candidate_recipes.mortality") != EXPECTED_MORTALITY_RECIPE:
        raise ConfigurationError("Mortality candidate recipe changed")
    if _mapping(recipes.get("observed_ventilation_support"), "candidate_recipes.observed_ventilation_support") != EXPECTED_VENTILATION_RECIPE:
        raise ConfigurationError("Ventilation candidate recipe changed")
    if _mapping(raw.get("explicit_separations"), "explicit_separations") != EXPECTED_SEPARATIONS:
        raise ConfigurationError("Derived-stage separation boundary changed")
    deferred = _mapping(raw.get("deferred_recipes"), "deferred_recipes")
    if deferred != {
        "time_blocking": {
            "status": "deferred_requires_named_resolution_and_aggregation_contract",
            "generate_during_core_derivation": False,
        },
        "analysis_cohorts": {
            "status": "deferred_analysis_specific",
            "generate_during_core_derivation": False,
        },
    }:
        raise ConfigurationError("Deferred derivation boundary changed")
    boundary = _mapping(raw.get("boundary"), "boundary")
    if boundary != {
        "read_released_cleaned_rows": True,
        "modify_cleaned_release": False,
        "write_clinical_data": False,
        "write_reports_only": True,
        "derive_output_values_for_aggregate_evidence_only": True,
        "publish_derived_contract": False,
        "build_derived_candidate": False,
        "apply_time_blocking": False,
        "filter_rows": False,
        "filter_stays": False,
        "authorize_external_data_export": False,
    }:
        raise ConfigurationError("Derivation review boundary changed")
    promotion_path = resolve_path(
        required_string(input_raw, "cleaned_promotion_policy", "input"), source
    )
    promotion_hash = required_string(
        input_raw, "cleaned_promotion_policy_sha256", "input"
    )
    if sha256_file(promotion_path) != promotion_hash:
        raise ConfigurationError("Approved cleaned promotion policy changed")
    streaming = _mapping(raw.get("streaming"), "streaming")
    outputs = _mapping(raw.get("outputs"), "outputs")
    return DerivationContractReviewPolicy(
        version="0.1",
        cleaned_release_id=required_string(
            input_raw, "cleaned_release_id", "input"
        ),
        cleaned_promotion_policy_path=promotion_path,
        cleaned_promotion_policy_sha256=promotion_hash,
        expected_static_rows=_positive_int(
            input_raw.get("expected_static_rows"), "input.expected_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            input_raw.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
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


def load_cleaned_release_input(
    config: DerivationContractReviewConfig,
    policy: DerivationContractReviewPolicy,
) -> CleanedReleaseInput:
    from asic_pipeline.cleaning.pipeline import (
        _cleaned_schema,
        _read_json,
        _schema_digest,
        load_cleaning_policy,
        load_harmonized_release_input,
    )
    from asic_pipeline.cleaning.promotion import (
        CleanedPromotionPolicy,
        load_cleaned_promotion_policy,
    )

    if (
        config.cleaned_promotion.policy_path.resolve()
        != policy.cleaned_promotion_policy_path.resolve()
    ):
        raise HarmonizationError("Dataset and derivation promotion policies disagree")
    promotion: CleanedPromotionPolicy = load_cleaned_promotion_policy(
        policy.cleaned_promotion_policy_path
    )
    if promotion.release_id != policy.cleaned_release_id:
        raise HarmonizationError("Derivation cleaned release ID changed")
    cleaning_policy = load_cleaning_policy(config.cleaned_promotion.cleaning.policy_path)
    harmonized = load_harmonized_release_input(
        config.cleaned_promotion.cleaning, cleaning_policy
    )
    schemas = {
        table: _cleaned_schema(
            harmonized.frozen_schemas[table],
            cleaning_policy.harmonized_contract_version,
        )
        for table in ("static", "dynamic")
    }
    pointer_path = config.data_root / promotion.current_release_pointer
    pointer = _read_json(pointer_path, "Current cleaned release pointer")
    release_dir = config.data_root / promotion.releases_directory / policy.cleaned_release_id
    manifest_path = release_dir / "release_manifest.json"
    if not isinstance(pointer, dict) or (
        pointer.get("artifact") != "asic_v3_current_cleaned_release"
        or pointer.get("release_id") != policy.cleaned_release_id
        or pointer.get("cleaned_layer_ready") is not True
        or pointer.get("derived_input_approved") is not True
        or pointer.get("publication_ready") is not True
        or pointer.get("external_data_export_authorized") is not False
        or Path(str(pointer.get("release_manifest", ""))).resolve()
        != manifest_path.resolve()
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
    ):
        raise HarmonizationError("Current cleaned release pointer changed")
    manifest = _read_json(manifest_path, "Cleaned release manifest")
    if not isinstance(manifest, dict) or (
        manifest.get("artifact") != "asic_v3_cleaned_release"
        or manifest.get("release_id") != policy.cleaned_release_id
        or manifest.get("status") != "released_cleaned_layer"
        or manifest.get("payload_byte_identity_preserved") is not True
        or manifest.get("cleaned_layer_ready") is not True
        or manifest.get("derived_input_approved") is not True
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not True
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved cleaned release changed")
    summaries = manifest.get("files")
    if not isinstance(summaries, dict):
        raise HarmonizationError("Cleaned release files are unavailable")
    expected_rows = {
        "static": policy.expected_static_rows,
        "dynamic": policy.expected_dynamic_rows,
    }
    source_files: dict[str, Path] = {}
    for table in ("static", "dynamic"):
        path = release_dir / f"{table}.parquet"
        summary = summaries.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Cleaned release table is unavailable")
        parquet = pq.ParquetFile(path)
        if (
            summary.get("sha256") != sha256_file(path)
            or summary.get("row_count") != expected_rows[table]
            or parquet.metadata.num_rows != expected_rows[table]
            or summary.get("schema_sha256") != _schema_digest(schemas[table])
            or not _parquet_schema_matches(parquet.schema_arrow, schemas[table])
        ):
            raise HarmonizationError("Cleaned release table changed")
        source_files[table] = path
    return CleanedReleaseInput(
        release_directory=release_dir,
        release_manifest_path=manifest_path,
        release_manifest=manifest,
        source_files=source_files,
        schemas=schemas,
    )


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def candidate_mortality(
    death_status: str | None,
    reported: bool | None,
) -> tuple[bool | None, bool | None, bool]:
    detailed = {
        "discharged_alive": (False, False),
        "died_in_icu": (True, True),
        "died_in_hospital": (True, False),
    }.get(death_status)
    if detailed is not None:
        hospital, icu = detailed
        if reported is not None and reported != hospital:
            return None, None, True
        return hospital, icu, False
    if reported is False:
        return False, False, False
    if reported is True:
        return True, None, False
    return None, None, False


def _bool_label(value: bool | None) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "missing"


def _scan_static(path: Path, rows_per_batch: int) -> dict[str, Any]:
    columns = [
        "stay_id_global",
        "hospital_id",
        "death_status",
        "hospital_mortality_reported",
        "discharge_status",
    ]
    rows = 0
    stay_hospitals: dict[str, str] = {}
    duplicate_stays = 0
    missing_identifiers = 0
    hospital_conflicts = 0
    death_domain: Counter[str] = Counter()
    reported_domain: Counter[str] = Counter()
    discharge_domain: Counter[str] = Counter()
    mortality_outputs: Counter[str] = Counter()
    evidence_patterns: Counter[str] = Counter()
    unknown_death_status_count = 0
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=rows_per_batch, columns=columns):
        values = batch.to_pydict()
        for stay, hospital, death, reported, discharge in zip(
            *(values[column] for column in columns), strict=True
        ):
            rows += 1
            if stay is None or hospital is None:
                missing_identifiers += 1
            else:
                previous = stay_hospitals.get(stay)
                if previous is not None:
                    duplicate_stays += 1
                    if previous != hospital:
                        hospital_conflicts += 1
                else:
                    stay_hospitals[stay] = hospital
            death_label = "missing" if death is None else str(death)
            reported_label = _bool_label(reported)
            discharge_label = "missing" if discharge is None else str(discharge)
            death_domain[death_label] += 1
            reported_domain[reported_label] += 1
            discharge_domain[discharge_label] += 1
            if death is not None and death not in {
                "discharged_alive",
                "died_in_icu",
                "died_in_hospital",
            }:
                unknown_death_status_count += 1
            hospital_mortality, icu_mortality, conflict = candidate_mortality(
                death, reported
            )
            mortality_outputs[f"hospital_mortality_{_bool_label(hospital_mortality)}"] += 1
            mortality_outputs[f"icu_mortality_{_bool_label(icu_mortality)}"] += 1
            mortality_outputs[f"mortality_source_conflict_{str(conflict).lower()}"] += 1
            evidence_patterns[
                f"death={death_label}|reported={reported_label}"
            ] += 1
    return {
        "row_count": rows,
        "unique_stay_count": len(stay_hospitals),
        "stay_hospitals": stay_hospitals,
        "missing_identifier_count": missing_identifiers,
        "duplicate_stay_count": duplicate_stays,
        "stay_hospital_conflict_count": hospital_conflicts,
        "unknown_death_status_count": unknown_death_status_count,
        "death_status_domain": dict(sorted(death_domain.items())),
        "hospital_mortality_reported_domain": dict(sorted(reported_domain.items())),
        "discharge_status_domain": dict(sorted(discharge_domain.items())),
        "mortality_candidate_outputs": dict(sorted(mortality_outputs.items())),
        "mortality_evidence_patterns": dict(sorted(evidence_patterns.items())),
    }


def _scan_dynamic(
    path: Path,
    rows_per_batch: int,
    progress: Callable[[str], None] | None,
) -> dict[str, Any]:
    columns = [
        "stay_id_global",
        "hospital_id",
        "minutes_since_icu_admission",
        "insp_pressure",
        "peep",
        "delta_p_reported",
        "fio2",
        "vt",
        "vt_per_kg_ideal_body_weight",
    ]
    marker_columns = ("fio2", "peep", "vt", "vt_per_kg_ideal_body_weight")
    states: dict[str, StayTimelineState] = {}
    marker_counts: Counter[str] = Counter()
    hospital_rows: Counter[str] = Counter()
    rows = 0
    missing_identifier_count = 0
    stay_hospital_conflict_count = 0
    nonfinite_time_count = 0
    missing_time_count = 0
    negative_time_count = 0
    duplicate_time_count = 0
    time_regression_count = 0
    supported_missing_time_count = 0
    min_time: float | None = None
    max_time: float | None = None
    delta_counts: Counter[str] = Counter()
    delta_max_absolute_difference: float | None = None
    current_stay: str | None = None
    current_supported_times: list[float] = []
    closed_stays: set[str] = set()
    reappeared_stays: set[str] = set()
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=rows_per_batch, columns=columns):
        values = batch.to_pydict()
        for row in zip(*(values[column] for column in columns), strict=True):
            (
                stay,
                hospital,
                time_value,
                insp_value,
                peep_value,
                reported_value,
                fio2_value,
                vt_value,
                vt_per_kg_value,
            ) = row
            rows += 1
            hospital_key = "missing" if hospital is None else str(hospital)
            hospital_rows[hospital_key] += 1
            if stay is None or hospital is None:
                missing_identifier_count += 1
                continue
            if stay != current_stay:
                if current_stay is not None:
                    _finalize_stay_episodes(
                        states[current_stay], current_supported_times
                    )
                    closed_stays.add(current_stay)
                if stay in closed_stays:
                    reappeared_stays.add(stay)
                current_stay = stay
                current_supported_times = []
            state = states.get(stay)
            if state is None:
                state = StayTimelineState(hospital=str(hospital))
                states[stay] = state
            elif state.hospital != hospital:
                stay_hospital_conflict_count += 1
            state.row_count += 1
            time_numeric = None
            if time_value is None:
                missing_time_count += 1
            else:
                candidate = float(time_value)
                if not math.isfinite(candidate):
                    nonfinite_time_count += 1
                else:
                    time_numeric = candidate
                    min_time = candidate if min_time is None else min(min_time, candidate)
                    max_time = candidate if max_time is None else max(max_time, candidate)
                    if candidate < 0:
                        negative_time_count += 1
                    if state.last_time_minutes is not None:
                        if candidate < state.last_time_minutes:
                            time_regression_count += 1
                        elif candidate == state.last_time_minutes:
                            duplicate_time_count += 1
                    state.last_time_minutes = candidate
                    state.max_time_minutes = (
                        candidate
                        if state.max_time_minutes is None
                        else max(state.max_time_minutes, candidate)
                    )
            markers = {
                "fio2": fio2_value,
                "peep": peep_value,
                "vt": vt_value,
                "vt_per_kg_ideal_body_weight": vt_per_kg_value,
            }
            for marker, value in markers.items():
                if value is not None:
                    marker_counts[marker] += 1
            supported = any(value is not None for value in markers.values())
            if supported:
                marker_counts["any_marker_supported_timestamp"] += 1
                if time_numeric is None:
                    supported_missing_time_count += 1
                else:
                    current_supported_times.append(time_numeric)

            insp = _finite(insp_value)
            peep = _finite(peep_value)
            reported = _finite(reported_value)
            if insp is not None and peep is not None:
                computed = insp - peep
                delta_counts["computable_row_count"] += 1
                if 0 <= computed <= 60:
                    delta_counts["computed_in_candidate_range_count"] += 1
                elif computed < 0:
                    delta_counts["computed_below_candidate_range_count"] += 1
                else:
                    delta_counts["computed_above_candidate_range_count"] += 1
                if reported is not None:
                    difference = abs(computed - reported)
                    delta_counts["computed_reported_overlap_count"] += 1
                    if difference <= 0.001:
                        delta_counts["computed_reported_within_0_001_count"] += 1
                    if difference <= 0.01:
                        delta_counts["computed_reported_within_0_01_count"] += 1
                    delta_max_absolute_difference = (
                        difference
                        if delta_max_absolute_difference is None
                        else max(delta_max_absolute_difference, difference)
                    )
            elif reported is not None:
                delta_counts["reported_present_computed_unavailable_count"] += 1
        if progress is not None and rows % (rows_per_batch * 20) == 0:
            progress(f"derivation_contract_review_progress dynamic_rows={rows}")
    if current_stay is not None:
        _finalize_stay_episodes(states[current_stay], current_supported_times)
    hospital_summary: dict[str, Counter[str]] = {}
    for state in states.values():
        counts = hospital_summary.setdefault(state.hospital, Counter())
        counts["stay_count"] += 1
        counts["dynamic_row_count"] += state.row_count
        counts["supported_timestamp_count"] += state.supported_timestamp_count
        counts["negative_supported_timestamp_count"] += (
            state.negative_supported_timestamp_count
        )
        counts["all_time_episode_count"] += state.all_time_episode_count
        counts["nonnegative_time_episode_count"] += (
            state.nonnegative_time_episode_count
        )
        if state.supported_timestamp_count:
            counts["stays_with_observed_support"] += 1
        else:
            counts["stays_without_observed_support"] += 1
        if state.all_time_maximum_episode_duration_hours >= 24:
            counts["stays_ge_24h_including_negative_time"] += 1
        if state.nonnegative_time_maximum_episode_duration_hours >= 24:
            counts["stays_ge_24h_nonnegative_icu_time"] += 1
        if (
            state.all_time_maximum_episode_duration_hours >= 24
        ) != (
            state.nonnegative_time_maximum_episode_duration_hours >= 24
        ):
            counts["stays_whose_ge_24h_flag_changes"] += 1
    ventilation = {
        "marker_non_missing_counts": dict(sorted(marker_counts.items())),
        "supported_timestamp_missing_time_count": supported_missing_time_count,
        "stay_count": len(states),
        "stays_with_observed_support": sum(
            state.supported_timestamp_count > 0 for state in states.values()
        ),
        "stays_without_observed_support": sum(
            state.supported_timestamp_count == 0 for state in states.values()
        ),
        "negative_supported_timestamp_count": sum(
            state.negative_supported_timestamp_count for state in states.values()
        ),
        "all_time_episode_count": sum(
            state.all_time_episode_count for state in states.values()
        ),
        "nonnegative_time_episode_count": sum(
            state.nonnegative_time_episode_count for state in states.values()
        ),
        "stays_ge_24h_including_negative_time": sum(
            state.all_time_maximum_episode_duration_hours >= 24
            for state in states.values()
        ),
        "stays_ge_24h_nonnegative_icu_time": sum(
            state.nonnegative_time_maximum_episode_duration_hours >= 24
            for state in states.values()
        ),
        "stays_whose_ge_24h_flag_changes_when_negative_time_excluded": sum(
            (state.all_time_maximum_episode_duration_hours >= 24)
            != (state.nonnegative_time_maximum_episode_duration_hours >= 24)
            for state in states.values()
        ),
        "all_time_maximum_observed_episode_duration_hours": max(
            (
                state.all_time_maximum_episode_duration_hours
                for state in states.values()
            ),
            default=0.0,
        ),
        "nonnegative_time_maximum_observed_episode_duration_hours": max(
            (
                state.nonnegative_time_maximum_episode_duration_hours
                for state in states.values()
            ),
            default=0.0,
        ),
        "hospital_summaries": {
            hospital: dict(sorted(counts.items()))
            for hospital, counts in sorted(hospital_summary.items())
        },
    }
    return {
        "row_count": rows,
        "unique_stay_count": len(states),
        "stay_hospitals": {stay: state.hospital for stay, state in states.items()},
        "missing_identifier_count": missing_identifier_count,
        "stay_hospital_conflict_count": stay_hospital_conflict_count,
        "stay_reappearance_count": len(reappeared_stays),
        "hospital_row_counts": dict(sorted(hospital_rows.items())),
        "time_evidence": {
            "finite_time_count": rows - missing_time_count - nonfinite_time_count,
            "missing_time_count": missing_time_count,
            "nonfinite_time_count": nonfinite_time_count,
            "negative_time_count": negative_time_count,
            "duplicate_time_within_stay_count": duplicate_time_count,
            "time_regression_within_stay_count": time_regression_count,
            "minimum_minutes": min_time,
            "maximum_minutes": max_time,
            "exact_hours_derivable_count": rows - missing_time_count - nonfinite_time_count,
        },
        "driving_pressure_evidence": {
            **dict(sorted(delta_counts.items())),
            "maximum_computed_reported_absolute_difference": delta_max_absolute_difference,
        },
        "ventilation_evidence": ventilation,
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
    metrics = payload["metrics"]
    mortality = payload["mortality_evidence"]
    time = payload["time_evidence"]
    delta = payload["driving_pressure_evidence"]
    ventilation = payload["ventilation_evidence"]
    technical = payload["technical_blocking_findings"]
    status = "PASS — PENDING HUMAN REVIEW" if not technical else "FAIL"
    lines = [
        "# ASIC v3 consolidated derivation-contract review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Input cleaned release: `{payload['cleaned_release_id']}`",
        f"- Technical status: **{'PASS' if not technical else 'FAIL'}**",
        f"- Overall status: **{status}**",
        f"- Technical blocking findings: `{len(technical)}`",
        "- Human blocking findings: `1`",
        f"- Static rows scanned: `{metrics['static_row_count']}`",
        f"- Dynamic rows scanned: `{metrics['dynamic_row_count']}`",
        f"- Static stays: `{metrics['static_stay_count']}`",
        f"- Dynamic stays: `{metrics['dynamic_stay_count']}`",
        "- Clinical data written: `false`",
        "- Cleaned release modified: `false`",
        "- Publication ready: `false`",
        "",
        "## Proposed core derived contract",
        "",
        "1. `hours_since_icu_admission = minutes_since_icu_admission / 60`; retain the minute field.",
        "2. `delta_p_computed = insp_pressure - peep`; retain `delta_p_reported` separately and review how computed values outside 0–60 cmH2O should be represented.",
        "3. Derive nullable hospital and ICU mortality from the reviewed detailed/reported evidence; disagreements set both outputs missing and raise `mortality_source_conflict`.",
        "4. Build an explicitly observed-support ventilation episode summary from non-missing FiO2, PEEP, VT, or VT/ideal-body-weight markers. Sort timestamps within each stay, connect gaps up to 8 hours, and count duration at least 24 hours inclusively. The proposed ICU flag excludes negative pre-admission time while preserving every source row.",
        "5. Do not merge or recompute SOFA/iSOFA, filter a cohort, or create time blocks in the core derived release.",
        "",
        "## Exact elapsed-time evidence",
        "",
        f"- Exact hours derivable: `{time['exact_hours_derivable_count']}`",
        f"- Missing time: `{time['missing_time_count']}`",
        f"- Non-finite time: `{time['nonfinite_time_count']}`",
        f"- Negative time: `{time['negative_time_count']}`",
        f"- Within-stay time regressions in source order (handled by sorting): `{time['time_regression_within_stay_count']}`",
        f"- Duplicate timestamps within stays: `{time['duplicate_time_within_stay_count']}`",
        "",
        "## Driving-pressure evidence",
        "",
        f"- Rows with both inputs: `{delta.get('computable_row_count', 0)}`",
        f"- Computed values within 0–60: `{delta.get('computed_in_candidate_range_count', 0)}`",
        f"- Computed values below 0: `{delta.get('computed_below_candidate_range_count', 0)}`",
        f"- Computed values above 60: `{delta.get('computed_above_candidate_range_count', 0)}`",
        f"- Computed/reported overlap: `{delta.get('computed_reported_overlap_count', 0)}`",
        f"- Agreement within 0.01: `{delta.get('computed_reported_within_0_01_count', 0)}`",
        "",
        "## Mortality evidence",
        "",
        f"- Detailed status domain: `{json.dumps(mortality['death_status_domain'], sort_keys=True)}`",
        f"- Reported hospital mortality domain: `{json.dumps(mortality['hospital_mortality_reported_domain'], sort_keys=True)}`",
        f"- Discharge status domain (audit only): `{json.dumps(mortality['discharge_status_domain'], sort_keys=True)}`",
        f"- Candidate output counts: `{json.dumps(mortality['mortality_candidate_outputs'], sort_keys=True)}`",
        "",
        "## Observed ventilation-support evidence",
        "",
        f"- Supported timestamps: `{ventilation['marker_non_missing_counts'].get('any_marker_supported_timestamp', 0)}`",
        f"- Supported timestamps missing usable time: `{ventilation['supported_timestamp_missing_time_count']}`",
        f"- Stays with observed support: `{ventilation['stays_with_observed_support']}`",
        f"- Stays without observed support: `{ventilation['stays_without_observed_support']}`",
        f"- Supported timestamps before ICU admission: `{ventilation['negative_supported_timestamp_count']}`",
        f"- Episodes using all elapsed times: `{ventilation['all_time_episode_count']}`",
        f"- Episodes using nonnegative ICU time: `{ventilation['nonnegative_time_episode_count']}`",
        f"- Stays ≥24 hours when negative time is included: `{ventilation['stays_ge_24h_including_negative_time']}`",
        f"- Stays ≥24 hours using nonnegative ICU time: `{ventilation['stays_ge_24h_nonnegative_icu_time']}`",
        f"- Stay flags changed by excluding negative time: `{ventilation['stays_whose_ge_24h_flag_changes_when_negative_time_excluded']}`",
        f"- Stay IDs reappearing after another stay in source order: `{metrics['dynamic_stay_reappearance_count']}`",
        "",
        "## Human review gate",
        "",
        "Approve or revise the four formulas and output semantics together. In particular, confirm masking computed driving pressure outside 0–60 cmH2O while exposing its flag, the mortality disagreement policy, and the proposed nonnegative-time observed-support ventilation proxy. It remains neither a cohort filter nor a ground-truth ventilation label.",
        "",
        "## Deferred by design",
        "",
        "Time blocking requires a separate named-resolution aggregation contract. Analysis-specific cohorts remain downstream recipes. Neither is generated by this review.",
        "",
    ]
    if technical:
        lines.extend(("## Technical blockers", ""))
        lines.extend(
            f"- `{item['check']}`: {item['details']}" for item in technical
        )
        lines.append("")
    return "\n".join(lines)


def run_derivation_contract_review(
    config: DerivationContractReviewConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> DerivationContractReviewResult:
    policy = load_derivation_contract_review_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("The approved derivation review is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid derivation contract review run ID")
    source = load_cleaned_release_input(config, policy)
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
        raise HarmonizationError("Derivation contract review run already exists")

    static = _scan_static(source.source_files["static"], policy.rows_per_batch)
    if progress is not None:
        progress("derivation_contract_review_table_complete=static")
    dynamic = _scan_dynamic(
        source.source_files["dynamic"], policy.rows_per_batch, progress
    )
    if progress is not None:
        progress("derivation_contract_review_table_complete=dynamic")
    static_stays = static.pop("stay_hospitals")
    dynamic_stays = dynamic.pop("stay_hospitals")
    static_ids = set(static_stays)
    dynamic_ids = set(dynamic_stays)
    static_only_count = len(static_ids - dynamic_ids)
    dynamic_only_count = len(dynamic_ids - static_ids)
    cross_table_hospital_conflicts = sum(
        static_stays[stay] != dynamic_stays[stay]
        for stay in static_ids & dynamic_ids
    )
    technical: list[dict[str, str]] = []
    if (
        static["row_count"] != policy.expected_static_rows
        or dynamic["row_count"] != policy.expected_dynamic_rows
    ):
        technical.append(
            {
                "check": "released_row_counts_conserved",
                "details": "Scanned cleaned-release rows do not match the approved release contract.",
            }
        )
    if (
        static["missing_identifier_count"]
        or static["duplicate_stay_count"]
        or static["stay_hospital_conflict_count"]
        or dynamic["missing_identifier_count"]
        or dynamic["stay_hospital_conflict_count"]
        or dynamic_only_count
        or cross_table_hospital_conflicts
    ):
        technical.append(
            {
                "check": "derived_identifier_contract_valid",
                "details": "Derived inputs require valid one-hospital stay identifiers and no dynamic-only stays.",
            }
        )
    if static["unknown_death_status_count"]:
        technical.append(
            {
                "check": "mortality_domain_complete",
                "details": "An unreviewed non-missing detailed mortality status was observed.",
            }
        )
    if dynamic["time_evidence"]["nonfinite_time_count"]:
        technical.append(
            {
                "check": "elapsed_time_finite_or_missing",
                "details": "Cleaned elapsed time contains a non-finite value.",
            }
        )
    if (
        dynamic["stay_reappearance_count"]
        or dynamic["ventilation_evidence"][
            "supported_timestamp_missing_time_count"
        ]
    ):
        technical.append(
            {
                "check": "ventilation_episode_inputs_orderable",
                "details": "Every stay must be contiguous and every observed-support timestamp must have usable elapsed time before within-stay sorting and episode construction.",
            }
        )
    human = (
        {
            "check": "core_derived_contract_approved",
            "details": "The four candidate recipes and their output/null/conflict semantics require one explicit human approval before a derived candidate is built.",
        },
    )
    metrics = {
        "static_row_count": static["row_count"],
        "dynamic_row_count": dynamic["row_count"],
        "static_stay_count": static["unique_stay_count"],
        "dynamic_stay_count": dynamic["unique_stay_count"],
        "static_stays_without_dynamic_rows": static_only_count,
        "dynamic_stays_without_static_row": dynamic_only_count,
        "cross_table_hospital_conflict_count": cross_table_hospital_conflicts,
        "dynamic_stay_reappearance_count": dynamic["stay_reappearance_count"],
    }
    generated = utc_timestamp()
    overall = "pending_human_review" if not technical else "fail"
    review_payload = {
        "artifact": "asic_v3_derivation_contract_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "overall_status": overall,
        "technical_status": "pass" if not technical else "fail",
        "blocking_findings": list(human),
        "technical_blocking_findings": technical,
        "metrics": metrics,
        "time_evidence": dynamic["time_evidence"],
        "driving_pressure_evidence": dynamic["driving_pressure_evidence"],
        "mortality_evidence": {
            key: static[key]
            for key in (
                "death_status_domain",
                "hospital_mortality_reported_domain",
                "discharge_status_domain",
                "mortality_candidate_outputs",
                "mortality_evidence_patterns",
            )
        },
        "ventilation_evidence": dynamic["ventilation_evidence"],
        "candidate_output_schema": {
            "dynamic_append": [
                {"name": "hours_since_icu_admission", "physical_type": "float64", "unit": "h"},
                {"name": "delta_p_computed", "physical_type": "float64", "unit": "cmH2O"},
                {"name": "delta_p_computed_out_of_range", "physical_type": "bool", "unit": "not_applicable"},
            ],
            "static_append": [
                {"name": "hospital_mortality", "physical_type": "bool", "unit": "not_applicable"},
                {"name": "icu_mortality", "physical_type": "bool", "unit": "not_applicable"},
                {"name": "mortality_source_conflict", "physical_type": "bool", "unit": "not_applicable"},
                *EXPECTED_VENTILATION_RECIPE["proposed_static_outputs"],
            ],
        },
        "sofa_isofa_preserved_separately": True,
        "cohort_filtering_applied": False,
        "time_blocking_applied": False,
        "cleaned_release_modified": False,
        "clinical_data_written": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    private_payload = {
        **review_payload,
        "artifact": "asic_v3_derivation_contract_review_private",
        "artifact_version": policy.private_artifact_version,
        "input_lineage": {
            "cleaned_release_manifest_sha256": sha256_file(
                source.release_manifest_path
            ),
            "derivation_review_policy_sha256": sha256_file(policy.source_path),
            "static_sha256": sha256_file(source.source_files["static"]),
            "dynamic_sha256": sha256_file(source.source_files["dynamic"]),
        },
    }
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(
        private_dir / "derivation_contract_review_manifest.json",
        private_payload,
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_markdown(review_markdown, _markdown(review_payload), 0o640)
    return DerivationContractReviewResult(
        run_id=selected_run,
        overall_status=overall,
        blocking_findings=human,
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
