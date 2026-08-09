from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

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
from asic_pipeline.harmonization.harmonized_build_audit import (
    _parquet_schema_matches,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe

if TYPE_CHECKING:
    from asic_pipeline.cleaning.cleaning_0_2_promotion import (
        Cleaned02PromotionConfig,
    )


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

EXPECTED_ELAPSED_RECIPE = {
    "input": "minutes_since_icu_admission",
    "output": "hours_since_icu_admission",
    "formula": "minutes_since_icu_admission_divided_by_60",
    "output_type": "float64",
    "output_unit": "h",
    "preserve_negative_values": True,
    "preserve_input": True,
}
EXPECTED_DRIVING_RECIPE = {
    "inspiratory_pressure_input": "insp_pressure",
    "peep_input": "peep",
    "reported_comparison": "delta_p_reported",
    "output": "delta_p_computed",
    "validity_flag": "delta_p_computed_out_of_range",
    "formula": "end_inspiratory_pressure_minus_peep",
    "source_definition": "ASIC_study_protocol",
    "applies_to_all_hospitals": True,
    "output_type": "float64",
    "output_unit": "cmH2O",
    "valid_minimum": 0,
    "valid_maximum": 60,
    "missing_input_action": "output_and_flag_missing",
    "out_of_range_action": "output_missing_flag_true",
    "in_range_action": "retain_output_flag_false",
    "preserve_reported_and_inputs": True,
}
EXPECTED_MORTALITY_RECIPE = {
    "detailed_status_input": "death_status",
    "reported_hospital_input": "hospital_mortality_reported",
    "discharge_status_input": "discharge_status",
    "hospital_output": "hospital_mortality",
    "icu_output": "icu_mortality",
    "hospital_conflict_output": "hospital_mortality_source_conflict",
    "icu_conflict_output": "icu_mortality_source_conflict",
    "detailed_status_implications": {
        "discharged_alive": {"hospital_mortality": False, "icu_mortality": False},
        "died_in_icu": {"hospital_mortality": True, "icu_mortality": True},
        "died_in_hospital": {"hospital_mortality": True, "icu_mortality": False},
    },
    "reported_hospital_implications": {
        "false": {"hospital_mortality": False, "icu_mortality": False},
        "true": {"hospital_mortality": True, "icu_mortality": None},
    },
    "discharge_status_implications": {
        "transferred": {"hospital_mortality": False, "icu_mortality": False},
        "died": {"hospital_mortality": True, "icu_mortality": None},
    },
    "no_evidence_action": "output_missing_conflict_false",
    "agreeing_evidence_action": "output_agreed_value_conflict_false",
    "conflicting_evidence_action": "output_missing_conflict_true",
    "preserve_all_source_fields": True,
}
EXPECTED_VENTILATION_RECIPE = {
    "time_input": "minutes_since_icu_admission",
    "marker_inputs": ["fio2", "peep", "vt", "vt_per_kg_ideal_body_weight"],
    "timestamp_supported_when": "any_marker_non_missing",
    "maximum_continuity_gap_hours": 8,
    "gap_is_inclusive": True,
    "minimum_episode_duration_hours": 24,
    "duration_is_inclusive": True,
    "sort_supported_timestamps_within_stay": True,
    "episode_window": "nonnegative_minutes_since_icu_admission",
    "supported_timestamp_count_window": "all_elapsed_times",
    "preserve_negative_time_source_rows": True,
    "claim_ground_truth_ventilation": False,
    "filter_stays_or_rows": False,
    "static_outputs": [
        {"name": "ventilation_supported_timestamp_count", "physical_type": "int64", "unit": "count"},
        {"name": "ventilation_episode_count", "physical_type": "int32", "unit": "count"},
        {"name": "maximum_observed_ventilation_episode_hours", "physical_type": "float64", "unit": "h"},
        {"name": "observed_mechanical_ventilation_ge_24h", "physical_type": "bool", "unit": "not_applicable"},
        {"name": "icu_recording_extent_hours", "physical_type": "float64", "unit": "h"},
    ],
}
EXPECTED_SEPARATIONS = {
    "preserve_reported_and_computed_driving_pressure_separately": True,
    "preserve_sofa_and_isofa_variants_without_recomputation_or_coalescence": True,
    "ventilation_proxy_is_not_a_cohort_filter_or_ground_truth_label": True,
    "defer_time_blocking": True,
    "defer_analysis_specific_cohorts": True,
}
EXPECTED_BOUNDARY = {
    "read_released_cleaned_data": True,
    "modify_cleaned_release": False,
    "overwrite_outputs": False,
    "preserve_all_input_rows_and_columns": True,
    "filter_rows_or_stays": False,
    "calculate_or_coalesce_sofa_or_isofa": False,
    "create_time_blocks": False,
    "create_analysis_cohort": False,
    "publish_derived_release": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class CoreDerivedPolicy:
    version: str
    cleaned_release_id: str
    expected_static_rows: int
    expected_dynamic_rows: int
    core_review_generated_at_utc: str
    driving_pressure_review_run_id: str
    expected_core_evidence: dict[str, int]
    expected_driving_evidence: dict[str, int]
    expected_output_evidence: dict[str, Any]
    rows_per_batch: int
    compression: str
    candidate_directory_name: str
    candidate_artifact_version: str
    build_review_directory_name: str
    audit_private_directory_name: str
    audit_review_directory_name: str
    audit_private_artifact_version: str
    audit_review_artifact_version: str
    source_path: Path
    expected_static_input_columns: int | None = None
    expected_dynamic_input_columns: int | None = None
    review_evidence_cleaned_release_id: str | None = None
    previous_contract_path: Path | None = None
    previous_contract_sha256: str | None = None
    cleaned_promotion_policy_path: Path | None = None
    cleaned_promotion_policy_sha256: str | None = None


@dataclass(frozen=True)
class CoreDerivedConfig:
    derivation_review: DerivationContractReviewConfig
    policy_path: Path
    cleaned_0_2_promotion: Cleaned02PromotionConfig | None = None

    @property
    def dataset_context(self) -> str:
        return self.derivation_review.dataset_context

    @property
    def data_root(self) -> Path:
        return self.derivation_review.data_root

    @property
    def reports_root(self) -> Path:
        return self.derivation_review.reports_root


@dataclass(frozen=True)
class CoreDerivedBuildResult:
    run_id: str
    candidate_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class ReviewedEvidence:
    core_review_path: Path
    core_review_payload: dict[str, Any]
    driving_review_path: Path
    driving_review_payload: dict[str, Any]


@dataclass(frozen=True)
class VentilationStaySummary:
    hospital: str
    supported_timestamp_count: int
    episode_count: int
    maximum_episode_hours: float
    ge_24h: bool
    recording_extent_hours: float | None


class VentilationTracker:
    def __init__(self) -> None:
        self.summaries: dict[str, VentilationStaySummary] = {}
        self.current_stay: str | None = None
        self.current_hospital: str | None = None
        self.current_supported_count = 0
        self.current_supported_nonnegative_times: list[float] = []
        self.current_maximum_time: float | None = None
        self.closed_stays: set[str] = set()
        self.missing_identifier_count = 0
        self.hospital_conflict_count = 0
        self.supported_missing_time_count = 0
        self.nonfinite_time_count = 0

    def _finalize_current(self) -> None:
        if self.current_stay is None or self.current_hospital is None:
            return
        episode_count, maximum_hours = episode_summary(
            self.current_supported_nonnegative_times, 480.0
        )
        self.summaries[self.current_stay] = VentilationStaySummary(
            hospital=self.current_hospital,
            supported_timestamp_count=self.current_supported_count,
            episode_count=episode_count,
            maximum_episode_hours=maximum_hours,
            ge_24h=maximum_hours >= 24.0,
            recording_extent_hours=(
                self.current_maximum_time / 60.0
                if self.current_maximum_time is not None
                else None
            ),
        )
        self.closed_stays.add(self.current_stay)

    def update(
        self,
        stay: Any,
        hospital: Any,
        time_value: Any,
        marker_values: tuple[Any, ...],
    ) -> None:
        if stay is None or hospital is None:
            self.missing_identifier_count += 1
            return
        stay_key = str(stay)
        hospital_key = str(hospital)
        if hospital_key not in EXPECTED_HOSPITALS:
            raise HarmonizationError("An unapproved hospital reached derivation")
        if stay_key != self.current_stay:
            self._finalize_current()
            if stay_key in self.closed_stays:
                raise HarmonizationError("A dynamic stay reappeared after another stay")
            self.current_stay = stay_key
            self.current_hospital = hospital_key
            self.current_supported_count = 0
            self.current_supported_nonnegative_times = []
            self.current_maximum_time = None
        elif self.current_hospital != hospital_key:
            self.hospital_conflict_count += 1
        time_numeric: float | None = None
        if time_value is not None:
            candidate = float(time_value)
            if math.isfinite(candidate):
                time_numeric = candidate
                self.current_maximum_time = (
                    candidate
                    if self.current_maximum_time is None
                    else max(self.current_maximum_time, candidate)
                )
            else:
                self.nonfinite_time_count += 1
        supported = any(value is not None for value in marker_values)
        if not supported:
            return
        self.current_supported_count += 1
        if time_numeric is None:
            self.supported_missing_time_count += 1
        elif time_numeric >= 0:
            self.current_supported_nonnegative_times.append(time_numeric)

    def finish(self) -> dict[str, VentilationStaySummary]:
        self._finalize_current()
        self.current_stay = None
        self.current_hospital = None
        return self.summaries


def episode_summary(
    times_minutes: list[float], maximum_gap_minutes: float
) -> tuple[int, float]:
    if not times_minutes:
        return 0, 0.0
    ordered = sorted(times_minutes)
    episodes = 1
    start = ordered[0]
    previous = ordered[0]
    maximum_duration = 0.0
    for value in ordered[1:]:
        if value - previous > maximum_gap_minutes:
            maximum_duration = max(maximum_duration, (previous - start) / 60.0)
            episodes += 1
            start = value
        previous = value
    maximum_duration = max(maximum_duration, (previous - start) / 60.0)
    return episodes, maximum_duration


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def load_core_derived_config(path: str | Path) -> CoreDerivedConfig:
    source = Path(path).expanduser().resolve()
    derivation_review = load_derivation_contract_review_config(source)
    raw = load_yaml_mapping(source, "Core-derived configuration")
    policy_path = resolve_path(
        required_string(raw, "core_derived_contract_policy", "config"), source
    )
    return CoreDerivedConfig(
        derivation_review=derivation_review,
        policy_path=policy_path,
    )


def load_core_derived_0_2_config(path: str | Path) -> CoreDerivedConfig:
    from asic_pipeline.cleaning.cleaning_0_2_promotion import (
        load_cleaned_0_2_promotion_config,
    )

    source = Path(path).expanduser().resolve()
    derivation_review = load_derivation_contract_review_config(source)
    cleaned_0_2_promotion = load_cleaned_0_2_promotion_config(source)
    raw = load_yaml_mapping(source, "Core-derived 0.2 configuration")
    policy_path = resolve_path(
        required_string(raw, "core_derived_0_2_contract_policy", "config"),
        source,
    )
    return CoreDerivedConfig(
        derivation_review=derivation_review,
        policy_path=policy_path,
        cleaned_0_2_promotion=cleaned_0_2_promotion,
    )


def load_core_derived_policy(path: str | Path) -> CoreDerivedPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Reviewed core-derived contract")
    version = raw.get("core_derived_contract_version")
    if version not in {"0.1", "0.2"} or raw.get("status") != (
        "approved_for_nonpublishable_derived_candidate_and_complete_audit"
    ):
        raise ConfigurationError("Reviewed core-derived contract is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    if input_raw.get("expected_hospitals") != list(EXPECTED_HOSPITALS):
        raise ConfigurationError("Core-derived hospital scope changed")
    reviews = _mapping(raw.get("review_evidence"), "review_evidence")
    core = _mapping(
        reviews.get("consolidated_derivation_review"),
        "review_evidence.consolidated_derivation_review",
    )
    driving = _mapping(
        reviews.get("driving_pressure_semantics_review"),
        "review_evidence.driving_pressure_semantics_review",
    )
    recipes = _mapping(raw.get("recipes"), "recipes")
    expected_recipes = {
        "exact_elapsed_hours": EXPECTED_ELAPSED_RECIPE,
        "computed_driving_pressure": EXPECTED_DRIVING_RECIPE,
        "mortality": EXPECTED_MORTALITY_RECIPE,
        "observed_ventilation_support": EXPECTED_VENTILATION_RECIPE,
    }
    if recipes != expected_recipes:
        raise ConfigurationError("Reviewed core-derived recipes changed")
    if _mapping(raw.get("separations"), "separations") != EXPECTED_SEPARATIONS:
        raise ConfigurationError("Core-derived stage separations changed")
    if _mapping(raw.get("boundary"), "boundary") != EXPECTED_BOUNDARY:
        raise ConfigurationError("Core-derived stage boundary changed")
    expected_output = _mapping(
        raw.get("expected_output_evidence"), "expected_output_evidence"
    )
    if expected_output != {
        "hours_non_missing_count": 24_069_379,
        "hours_negative_count": 247_977,
        "driving_pressure_in_range_count": 6_383_119,
        "driving_pressure_out_of_range_count": 3_636,
        "hospital_mortality": {
            "false": 9_757,
            "true": 4_726,
            "missing": 1_571,
            "conflicts": 0,
        },
        "icu_mortality": {
            "false": 10_457,
            "true": 2_146,
            "missing": 3_451,
            "conflicts": 0,
        },
        "ventilation_supported_timestamp_count_total": 8_715_616,
        "ventilation_episode_count_total": 53_535,
        "observed_mechanical_ventilation_ge_24h_true": 12_775,
    }:
        raise ConfigurationError("Reviewed expected derived evidence changed")
    streaming = _mapping(raw.get("streaming"), "streaming")
    if streaming.get("require_contiguous_dynamic_stays") is not True:
        raise ConfigurationError("Dynamic stay-order requirement changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    expected_core = {
        key: _positive_int(core.get(key), f"core evidence.{key}")
        for key in (
            "expected_static_rows",
            "expected_dynamic_rows",
            "expected_both_driving_pressure_inputs",
            "expected_supported_timestamps",
            "expected_nonnegative_episode_count",
            "expected_nonnegative_ge_24h_stays",
        )
    }
    expected_driving = {
        key: _positive_int(driving.get(key), f"driving evidence.{key}")
        for key in (
            "expected_both_input_count",
            "expected_below_zero_count",
            "expected_above_sixty_count",
            "expected_reported_overlap_count",
        )
    }
    expected_static_input_columns: int | None = None
    expected_dynamic_input_columns: int | None = None
    review_evidence_cleaned_release_id: str | None = None
    previous_contract_path: Path | None = None
    previous_contract_sha256: str | None = None
    cleaned_promotion_policy_path: Path | None = None
    cleaned_promotion_policy_sha256: str | None = None
    if version == "0.2":
        if raw.get("implementation_authorization_recorded_utc") != "2026-08-08":
            raise ConfigurationError("Core-derived 0.2 authorization changed")
        expected_static_input_columns = _positive_int(
            input_raw.get("expected_static_input_columns"),
            "input.expected_static_input_columns",
        )
        expected_dynamic_input_columns = _positive_int(
            input_raw.get("expected_dynamic_input_columns"),
            "input.expected_dynamic_input_columns",
        )
        cleaned_promotion_policy_path = resolve_path(
            required_string(
                input_raw, "cleaned_promotion_policy", "input"
            ),
            source,
        )
        cleaned_promotion_policy_sha256 = required_string(
            input_raw, "cleaned_promotion_policy_sha256", "input"
        )
        inheritance = _mapping(
            raw.get("contract_inheritance"), "contract_inheritance"
        )
        previous_contract_path = resolve_path(
            required_string(
                inheritance, "previous_contract", "contract_inheritance"
            ),
            source,
        )
        previous_contract_sha256 = required_string(
            inheritance,
            "previous_contract_sha256",
            "contract_inheritance",
        )
        if (
            inheritance.get("previous_core_derived_contract_version") != "0.1"
            or inheritance.get("clinical_formula_changes") != 0
            or inheritance.get("output_semantic_changes") != 0
            or inheritance.get("derived_output_additions") != 0
            or inheritance.get("derived_output_removals") != 0
            or inheritance.get("implementation_instruction") != "proceed"
            or sha256_file(previous_contract_path) != previous_contract_sha256
            or sha256_file(cleaned_promotion_policy_path)
            != cleaned_promotion_policy_sha256
        ):
            raise ConfigurationError("Core-derived 0.2 inheritance changed")
        review_evidence_cleaned_release_id = required_string(
            reviews,
            "evidence_cleaned_release_id",
            "review_evidence",
        )
        if (
            required_string(input_raw, "cleaned_release_id", "input")
            != "20260807T154100Z"
            or review_evidence_cleaned_release_id != "20260806T114234Z"
            or expected_static_input_columns != 28
            or expected_dynamic_input_columns != 144
            or outputs.get("candidate_directory_name")
            != "derived_0_2_candidates"
            or outputs.get("candidate_artifact_version") != "0.2"
            or outputs.get("audit_private_artifact_version") != "0.2"
            or outputs.get("audit_review_artifact_version") != "0.2"
        ):
            raise ConfigurationError("Core-derived 0.2 scope changed")
    return CoreDerivedPolicy(
        version=str(version),
        cleaned_release_id=required_string(input_raw, "cleaned_release_id", "input"),
        expected_static_rows=_positive_int(
            input_raw.get("expected_static_rows"), "input.expected_static_rows"
        ),
        expected_dynamic_rows=_positive_int(
            input_raw.get("expected_dynamic_rows"), "input.expected_dynamic_rows"
        ),
        core_review_generated_at_utc=required_string(
            core, "generated_at_utc", "core evidence"
        ),
        driving_pressure_review_run_id=required_string(
            driving, "run_id", "driving evidence"
        ),
        expected_core_evidence=expected_core,
        expected_driving_evidence=expected_driving,
        expected_output_evidence=expected_output,
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        compression=required_string(streaming, "parquet_compression", "streaming"),
        candidate_directory_name=required_string(
            outputs, "candidate_directory_name", "outputs"
        ),
        candidate_artifact_version=required_string(
            outputs, "candidate_artifact_version", "outputs"
        ),
        build_review_directory_name=required_string(
            outputs, "build_review_directory_name", "outputs"
        ),
        audit_private_directory_name=required_string(
            outputs, "audit_private_directory_name", "outputs"
        ),
        audit_review_directory_name=required_string(
            outputs, "audit_review_directory_name", "outputs"
        ),
        audit_private_artifact_version=required_string(
            outputs, "audit_private_artifact_version", "outputs"
        ),
        audit_review_artifact_version=required_string(
            outputs, "audit_review_artifact_version", "outputs"
        ),
        source_path=source,
        expected_static_input_columns=expected_static_input_columns,
        expected_dynamic_input_columns=expected_dynamic_input_columns,
        review_evidence_cleaned_release_id=review_evidence_cleaned_release_id,
        previous_contract_path=previous_contract_path,
        previous_contract_sha256=previous_contract_sha256,
        cleaned_promotion_policy_path=cleaned_promotion_policy_path,
        cleaned_promotion_policy_sha256=cleaned_promotion_policy_sha256,
    )


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} is unreadable") from exc


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


def _write_markdown(path: Path, text: str, mode: int) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(text)
        temporary.replace(path)
        path.chmod(mode)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _schema_digest(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def load_cleaned_0_2_release_input(
    config: CoreDerivedConfig,
    policy: CoreDerivedPolicy,
) -> CleanedReleaseInput:
    from asic_pipeline.cleaning.cleaning_0_2 import (
        _cleaned_schema as _cleaned_0_2_schema,
        _load_input as _load_harmonized_0_2_input,
        load_cleaning_0_2_policy,
    )
    from asic_pipeline.cleaning.cleaning_0_2_promotion import (
        load_cleaned_0_2_promotion_policy,
    )

    promotion_config = config.cleaned_0_2_promotion
    if promotion_config is None or policy.version != "0.2":
        raise HarmonizationError("Core-derived 0.2 input configuration is missing")
    promotion = load_cleaned_0_2_promotion_policy(
        promotion_config.policy_path
    )
    if (
        policy.cleaned_promotion_policy_path is None
        or promotion_config.policy_path.resolve()
        != policy.cleaned_promotion_policy_path.resolve()
        or promotion.release_id != policy.cleaned_release_id
        or promotion.cleaning_policy_version != "0.2"
        or promotion.harmonized_contract_version != "0.2"
        or sha256_file(promotion_config.policy_path)
        != policy.cleaned_promotion_policy_sha256
    ):
        raise HarmonizationError("Core-derived 0.2 release lineage changed")
    cleaning_policy = load_cleaning_0_2_policy(
        promotion.cleaning_policy_path
    )
    harmonized = _load_harmonized_0_2_input(
        promotion_config.audit.build, cleaning_policy
    )
    schemas = {
        table: _cleaned_0_2_schema(
            harmonized.source_schemas[table],
            "0.2",
        )
        for table in ("static", "dynamic")
    }
    expected_columns = {
        "static": policy.expected_static_input_columns,
        "dynamic": policy.expected_dynamic_input_columns,
    }
    if any(
        expected_columns[table] is None
        or len(schemas[table]) != expected_columns[table]
        for table in ("static", "dynamic")
    ):
        raise HarmonizationError("Cleaned 0.2 input schema width changed")

    pointer_path = config.data_root / promotion.current_release_pointer
    pointer = _read_json(pointer_path, "Current cleaned 0.2 release pointer")
    release_dir = (
        config.data_root / promotion.releases_directory / policy.cleaned_release_id
    )
    manifest_path = release_dir / "release_manifest.json"
    if not isinstance(pointer, dict) or (
        pointer.get("artifact") != "asic_v3_current_cleaned_release"
        or pointer.get("release_id") != policy.cleaned_release_id
        or pointer.get("cleaning_policy_version") != "0.2"
        or pointer.get("cleaned_layer_ready") is not True
        or pointer.get("cleaned_dictionary_ready") is not True
        or pointer.get("derived_input_approved") is not True
        or pointer.get("publication_ready") is not True
        or pointer.get("external_data_export_authorized") is not False
        or Path(str(pointer.get("release_manifest", ""))).resolve()
        != manifest_path.resolve()
        or pointer.get("release_manifest_sha256") != sha256_file(manifest_path)
    ):
        raise HarmonizationError("Current cleaned 0.2 pointer changed")
    manifest = _read_json(manifest_path, "Cleaned 0.2 release manifest")
    if not isinstance(manifest, dict) or (
        manifest.get("artifact") != "asic_v3_cleaned_release"
        or manifest.get("artifact_version") != "0.2"
        or manifest.get("dataset_context") != config.dataset_context
        or manifest.get("release_id") != policy.cleaned_release_id
        or manifest.get("status") != "released_cleaned_layer"
        or manifest.get("harmonized_release_id")
        != promotion.harmonized_release_id
        or manifest.get("harmonized_contract_version") != "0.2"
        or manifest.get("cleaning_policy_version") != "0.2"
        or manifest.get("payload_byte_identity_preserved") is not True
        or manifest.get("dictionary_bytes_preserved") is not True
        or manifest.get("cleaned_layer_ready") is not True
        or manifest.get("cleaned_dictionary_ready") is not True
        or manifest.get("derived_input_approved") is not True
        or manifest.get("cleaning_rerun_during_promotion") is not False
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not True
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Approved cleaned 0.2 release changed")
    summaries = manifest.get("files")
    if not isinstance(summaries, dict):
        raise HarmonizationError("Cleaned 0.2 release files are unavailable")
    expected_rows = {
        "static": policy.expected_static_rows,
        "dynamic": policy.expected_dynamic_rows,
    }
    source_files: dict[str, Path] = {}
    for table in ("static", "dynamic"):
        path = release_dir / f"{table}.parquet"
        summary = summaries.get(table)
        if not isinstance(summary, dict) or not path.is_file():
            raise HarmonizationError("Cleaned 0.2 release table is unavailable")
        parquet = pq.ParquetFile(path)
        checks = {
            "sha256": summary.get("sha256") == sha256_file(path),
            "manifest_row_count": (
                summary.get("row_count") == expected_rows[table]
            ),
            "parquet_row_count": (
                parquet.metadata.num_rows == expected_rows[table]
            ),
            "contract_schema_digest": (
                summary.get("schema_sha256")
                == _schema_digest(schemas[table])
            ),
            "parquet_contract_schema": _parquet_schema_matches(
                parquet.schema_arrow, schemas[table]
            ),
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise HarmonizationError(
                f"Cleaned 0.2 release {table} validation failed: "
                + ", ".join(failed)
            )
        source_files[table] = path
    return CleanedReleaseInput(
        release_directory=release_dir,
        release_manifest_path=manifest_path,
        release_manifest=manifest,
        source_files=source_files,
        schemas=schemas,
    )


def _field(
    name: str,
    data_type: pa.DataType,
    unit: str,
    definition: str,
    contract_version: str = "0.1",
) -> pa.Field:
    return pa.field(
        name,
        data_type,
        nullable=True,
        metadata={
            b"asic_v3_unit": unit.encode(),
            b"asic_v3_definition": definition.encode(),
            b"asic_v3_stage": b"derived",
            b"asic_v3_contract_version": contract_version.encode(),
        },
    )


def derived_schema(
    input_schema: pa.Schema,
    table: str,
    contract_version: str = "0.1",
) -> pa.Schema:
    if table == "dynamic":
        additions = [
            _field(
                "hours_since_icu_admission",
                pa.float64(),
                "h",
                "Exact minutes_since_icu_admission divided by 60; negative values retained.",
            ),
            _field(
                "delta_p_computed",
                pa.float64(),
                "cmH2O",
                "ASIC-defined end-inspiratory pressure minus PEEP, retained only within 0-60 cmH2O.",
            ),
            _field(
                "delta_p_computed_out_of_range",
                pa.bool_(),
                "not_applicable",
                "True when both driving-pressure inputs exist but their difference is outside 0-60 cmH2O; null when either input is missing.",
            ),
        ]
    elif table == "static":
        additions = [
            _field("hospital_mortality", pa.bool_(), "not_applicable", "Conflict-aware hospital mortality derived from reviewed source implications."),
            _field("icu_mortality", pa.bool_(), "not_applicable", "Conflict-aware ICU mortality derived from reviewed source implications."),
            _field("hospital_mortality_source_conflict", pa.bool_(), "not_applicable", "True when available hospital-mortality implications disagree."),
            _field("icu_mortality_source_conflict", pa.bool_(), "not_applicable", "True when available ICU-mortality implications disagree."),
            _field("ventilation_supported_timestamp_count", pa.int64(), "count", "Count of timestamps at any elapsed time with at least one reviewed ventilation-support marker."),
            _field("ventilation_episode_count", pa.int32(), "count", "Count of nonnegative-time observed-support episodes joined across gaps no greater than 8 hours."),
            _field("maximum_observed_ventilation_episode_hours", pa.float64(), "h", "Maximum nonnegative-time observed-support episode duration."),
            _field("observed_mechanical_ventilation_ge_24h", pa.bool_(), "not_applicable", "Observed-support proxy indicating a nonnegative-time episode lasting at least 24 hours; not ground truth or a cohort filter."),
            _field("icu_recording_extent_hours", pa.float64(), "h", "Maximum observed minutes_since_icu_admission divided by 60 for the stay."),
        ]
    else:
        raise HarmonizationError(f"Unsupported derived table: {table}")
    if contract_version != "0.1":
        additions = [
            field.with_metadata(
                {
                    **dict(field.metadata or {}),
                    b"asic_v3_contract_version": contract_version.encode(),
                }
            )
            for field in additions
        ]
    existing = set(input_schema.names)
    if any(field.name in existing for field in additions):
        raise HarmonizationError("A reviewed derived output already exists in input")
    metadata = dict(input_schema.metadata or {})
    metadata.update(
        {
            b"asic_v3_stage": b"derived_candidate",
            b"asic_v3_core_derived_contract_version": contract_version.encode(),
            b"publication_ready": b"false",
            b"cleaning_applied": b"true",
            b"derivation_applied": b"true",
            b"cohort_filtering_applied": b"false",
            b"time_blocking_applied": b"false",
        }
    )
    return pa.schema([*input_schema, *additions], metadata=metadata)


def load_reviewed_evidence(
    config: CoreDerivedConfig, policy: CoreDerivedPolicy
) -> ReviewedEvidence:
    core_dir = config.reports_root / "review" / "derivation_contract_review"
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(core_dir.glob("*.json")):
        payload = _read_json(path, "Consolidated derivation review")
        if isinstance(payload, dict) and payload.get("generated_at_utc") == policy.core_review_generated_at_utc:
            candidates.append((path, payload))
    if len(candidates) != 1:
        raise HarmonizationError("Unique approved consolidated derivation review is unavailable")
    core_path, core_payload = candidates[0]
    metrics = core_payload.get("metrics", {})
    driving = core_payload.get("driving_pressure_evidence", {})
    ventilation = core_payload.get("ventilation_evidence", {})
    expected_core = policy.expected_core_evidence
    evidence_cleaned_release_id = (
        policy.review_evidence_cleaned_release_id
        or policy.cleaned_release_id
    )
    if (
        core_payload.get("artifact") != "asic_v3_derivation_contract_review"
        or core_payload.get("cleaned_release_id")
        != evidence_cleaned_release_id
        or core_payload.get("technical_status") != "pass"
        or core_payload.get("overall_status") != "pending_human_review"
        or core_payload.get("technical_blocking_findings") != []
        or metrics.get("static_row_count") != expected_core["expected_static_rows"]
        or metrics.get("dynamic_row_count") != expected_core["expected_dynamic_rows"]
        or driving.get("computable_row_count")
        != expected_core["expected_both_driving_pressure_inputs"]
        or ventilation.get("marker_non_missing_counts", {}).get(
            "any_marker_supported_timestamp"
        )
        != expected_core["expected_supported_timestamps"]
        or ventilation.get("nonnegative_time_episode_count")
        != expected_core["expected_nonnegative_episode_count"]
        or ventilation.get("stays_ge_24h_nonnegative_icu_time")
        != expected_core["expected_nonnegative_ge_24h_stays"]
    ):
        raise HarmonizationError("Consolidated derivation review evidence changed")

    driving_path = (
        config.reports_root
        / "review"
        / "driving_pressure_semantics"
        / f"{policy.driving_pressure_review_run_id}.json"
    )
    driving_payload = _read_json(driving_path, "Driving-pressure semantics review")
    overall_counts = driving_payload.get("evidence", {}).get("overall", {}).get("counts", {})
    expected_driving = policy.expected_driving_evidence
    if (
        driving_payload.get("artifact")
        != "asic_v3_driving_pressure_semantics_review"
        or driving_payload.get("cleaned_release_id")
        != evidence_cleaned_release_id
        or driving_payload.get("technical_status") != "pass"
        or driving_payload.get("overall_status") != "pending_human_review"
        or driving_payload.get("technical_blocking_findings") != []
        or overall_counts.get("both_input_count")
        != expected_driving["expected_both_input_count"]
        or overall_counts.get("insp_minus_peep_below_candidate_range_count")
        != expected_driving["expected_below_zero_count"]
        or overall_counts.get("insp_minus_peep_above_candidate_range_count")
        != expected_driving["expected_above_sixty_count"]
        or overall_counts.get("reported_overlap_count")
        != expected_driving["expected_reported_overlap_count"]
    ):
        raise HarmonizationError("Driving-pressure semantics review evidence changed")
    return ReviewedEvidence(
        core_review_path=core_path,
        core_review_payload=core_payload,
        driving_review_path=driving_path,
        driving_review_payload=driving_payload,
    )


def _bool_count(mask: pa.Array) -> int:
    value = pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py()
    return int(value or 0)


def transform_dynamic_batch(
    batch: pa.RecordBatch,
    output_schema: pa.Schema,
    counts: Counter[str],
) -> pa.RecordBatch:
    required = {
        name: batch.schema.get_field_index(name)
        for name in ("minutes_since_icu_admission", "insp_pressure", "peep")
    }
    if any(index < 0 for index in required.values()):
        raise HarmonizationError("Derived dynamic inputs are missing")
    minutes = pc.cast(batch.column(required["minutes_since_icu_admission"]), pa.float64())
    hours = pc.divide(minutes, pa.scalar(60.0, pa.float64()))
    insp = pc.cast(batch.column(required["insp_pressure"]), pa.float64())
    peep = pc.cast(batch.column(required["peep"]), pa.float64())
    both = pc.and_(
        pc.fill_null(pc.is_finite(insp), False),
        pc.fill_null(pc.is_finite(peep), False),
    )
    difference = pc.subtract(insp, peep)
    in_range = pc.and_(
        both,
        pc.and_(pc.greater_equal(difference, 0.0), pc.less_equal(difference, 60.0)),
    )
    out_of_range = pc.and_(both, pc.invert(in_range))
    computed = pc.if_else(
        in_range,
        difference,
        pa.scalar(None, pa.float64()),
    )
    validity_flag = pc.if_else(
        both,
        out_of_range,
        pa.scalar(None, pa.bool_()),
    )
    counts["row_count"] += batch.num_rows
    counts["hours_non_missing_count"] += len(hours) - hours.null_count
    counts["hours_negative_count"] += _bool_count(pc.less(hours, 0.0))
    counts["driving_pressure_computable_count"] += _bool_count(both)
    counts["driving_pressure_in_range_count"] += _bool_count(in_range)
    counts["driving_pressure_out_of_range_count"] += _bool_count(out_of_range)
    arrays = [*batch.columns, hours, computed, validity_flag]
    return pa.RecordBatch.from_arrays(arrays, schema=output_schema)


def resolve_mortality(
    death_status: str | None,
    reported_hospital: bool | None,
    discharge_status: str | None,
) -> tuple[bool | None, bool | None, bool, bool]:
    if death_status not in {
        None,
        "discharged_alive",
        "died_in_icu",
        "died_in_hospital",
    }:
        raise HarmonizationError("An unreviewed detailed mortality value was observed")
    if reported_hospital not in {None, False, True}:
        raise HarmonizationError("An unreviewed reported mortality value was observed")
    if discharge_status not in {None, "transferred", "died"}:
        raise HarmonizationError("An unreviewed discharge value was observed")
    hospital_evidence: list[bool] = []
    icu_evidence: list[bool] = []
    detailed = {
        "discharged_alive": (False, False),
        "died_in_icu": (True, True),
        "died_in_hospital": (True, False),
    }.get(death_status)
    if detailed is not None:
        hospital_evidence.append(detailed[0])
        icu_evidence.append(detailed[1])
    if reported_hospital is False:
        hospital_evidence.append(False)
        icu_evidence.append(False)
    elif reported_hospital is True:
        hospital_evidence.append(True)
    discharge = {
        "transferred": (False, False),
        "died": (True, None),
    }.get(discharge_status)
    if discharge is not None:
        hospital_evidence.append(discharge[0])
        if discharge[1] is not None:
            icu_evidence.append(discharge[1])

    def combine(values: list[bool]) -> tuple[bool | None, bool]:
        unique = set(values)
        if not unique:
            return None, False
        if len(unique) == 1:
            return next(iter(unique)), False
        return None, True

    hospital, hospital_conflict = combine(hospital_evidence)
    icu, icu_conflict = combine(icu_evidence)
    return hospital, icu, hospital_conflict, icu_conflict


def transform_static_batch(
    batch: pa.RecordBatch,
    output_schema: pa.Schema,
    summaries: dict[str, VentilationStaySummary],
    counts: Counter[str],
    seen_stays: set[str],
) -> pa.RecordBatch:
    required_names = (
        "stay_id_global",
        "hospital_id",
        "death_status",
        "hospital_mortality_reported",
        "discharge_status",
    )
    indices = {name: batch.schema.get_field_index(name) for name in required_names}
    if any(index < 0 for index in indices.values()):
        raise HarmonizationError("Derived static inputs are missing")
    values = {
        name: batch.column(index).to_pylist() for name, index in indices.items()
    }
    hospital_outputs: list[bool | None] = []
    icu_outputs: list[bool | None] = []
    hospital_conflicts: list[bool] = []
    icu_conflicts: list[bool] = []
    supported_counts: list[int] = []
    episode_counts: list[int] = []
    maximum_hours: list[float] = []
    ge_24h: list[bool] = []
    extents: list[float | None] = []
    for stay, hospital, death, reported, discharge in zip(
        *(values[name] for name in required_names), strict=True
    ):
        if stay is None or hospital is None:
            raise HarmonizationError("Static derived identifier is missing")
        stay_key = str(stay)
        if str(hospital) not in EXPECTED_HOSPITALS:
            raise HarmonizationError("An unapproved hospital reached static derivation")
        if stay_key in seen_stays:
            raise HarmonizationError("Static stay identifier is duplicated")
        seen_stays.add(stay_key)
        summary = summaries.get(stay_key)
        if summary is None or summary.hospital != str(hospital):
            raise HarmonizationError("Static/dynamic stay or hospital contract changed")
        outcome = resolve_mortality(death, reported, discharge)
        hospital_outputs.append(outcome[0])
        icu_outputs.append(outcome[1])
        hospital_conflicts.append(outcome[2])
        icu_conflicts.append(outcome[3])
        supported_counts.append(summary.supported_timestamp_count)
        episode_counts.append(summary.episode_count)
        maximum_hours.append(summary.maximum_episode_hours)
        ge_24h.append(summary.ge_24h)
        extents.append(summary.recording_extent_hours)
        counts[f"hospital_mortality_{_nullable_bool_label(outcome[0])}"] += 1
        counts[f"icu_mortality_{_nullable_bool_label(outcome[1])}"] += 1
        counts["hospital_mortality_conflict_count"] += int(outcome[2])
        counts["icu_mortality_conflict_count"] += int(outcome[3])
        counts["ventilation_supported_timestamp_count_total"] += summary.supported_timestamp_count
        counts["ventilation_episode_count_total"] += summary.episode_count
        counts["observed_mechanical_ventilation_ge_24h_true"] += int(summary.ge_24h)
    counts["row_count"] += batch.num_rows
    arrays = [
        *batch.columns,
        pa.array(hospital_outputs, type=pa.bool_()),
        pa.array(icu_outputs, type=pa.bool_()),
        pa.array(hospital_conflicts, type=pa.bool_()),
        pa.array(icu_conflicts, type=pa.bool_()),
        pa.array(supported_counts, type=pa.int64()),
        pa.array(episode_counts, type=pa.int32()),
        pa.array(maximum_hours, type=pa.float64()),
        pa.array(ge_24h, type=pa.bool_()),
        pa.array(extents, type=pa.float64()),
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=output_schema)


def _nullable_bool_label(value: bool | None) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    return "missing"


def update_ventilation_tracker(batch: pa.RecordBatch, tracker: VentilationTracker) -> None:
    names = (
        "stay_id_global",
        "hospital_id",
        "minutes_since_icu_admission",
        "fio2",
        "peep",
        "vt",
        "vt_per_kg_ideal_body_weight",
    )
    indices = {name: batch.schema.get_field_index(name) for name in names}
    if any(index < 0 for index in indices.values()):
        raise HarmonizationError("Ventilation episode input is missing")
    values = {
        name: batch.column(index).to_pylist() for name, index in indices.items()
    }
    for row in zip(*(values[name] for name in names), strict=True):
        tracker.update(row[0], row[1], row[2], tuple(row[3:]))


def _expected_accounting(policy: CoreDerivedPolicy) -> dict[str, int]:
    output = policy.expected_output_evidence
    return {
        "dynamic_row_count": policy.expected_dynamic_rows,
        "hours_non_missing_count": output["hours_non_missing_count"],
        "hours_negative_count": output["hours_negative_count"],
        "driving_pressure_computable_count": (
            output["driving_pressure_in_range_count"]
            + output["driving_pressure_out_of_range_count"]
        ),
        "driving_pressure_in_range_count": output[
            "driving_pressure_in_range_count"
        ],
        "driving_pressure_out_of_range_count": output[
            "driving_pressure_out_of_range_count"
        ],
        "static_row_count": policy.expected_static_rows,
        "hospital_mortality_false": output["hospital_mortality"]["false"],
        "hospital_mortality_true": output["hospital_mortality"]["true"],
        "hospital_mortality_missing": output["hospital_mortality"]["missing"],
        "hospital_mortality_conflict_count": output["hospital_mortality"][
            "conflicts"
        ],
        "icu_mortality_false": output["icu_mortality"]["false"],
        "icu_mortality_true": output["icu_mortality"]["true"],
        "icu_mortality_missing": output["icu_mortality"]["missing"],
        "icu_mortality_conflict_count": output["icu_mortality"]["conflicts"],
        "ventilation_supported_timestamp_count_total": output[
            "ventilation_supported_timestamp_count_total"
        ],
        "ventilation_episode_count_total": output[
            "ventilation_episode_count_total"
        ],
        "observed_mechanical_ventilation_ge_24h_true": output[
            "observed_mechanical_ventilation_ge_24h_true"
        ],
    }


def _build_markdown(payload: dict[str, Any]) -> str:
    counts = payload["derivation_accounting"]
    return "\n".join(
        (
            "# ASIC v3 derived candidate build review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Derived candidate: `{payload['run_id']}`",
            f"- Input cleaned release: `{payload['cleaned_release_id']}`",
            f"- Core-derived contract: `{payload['core_derived_contract_version']}`",
            "- Overall status: **PASS**",
            "- Blocking findings: `0`",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{counts['static_row_count']}`",
            f"- Dynamic rows: `{counts['dynamic_row_count']}`",
            f"- Driving pressure retained in range: `{counts['driving_pressure_in_range_count']}`",
            f"- Driving pressure masked and flagged out of range: `{counts['driving_pressure_out_of_range_count']}`",
            f"- Hospital mortality true/false/missing: `{counts['hospital_mortality_true']}` / `{counts['hospital_mortality_false']}` / `{counts['hospital_mortality_missing']}`",
            f"- ICU mortality true/false/missing: `{counts['icu_mortality_true']}` / `{counts['icu_mortality_false']}` / `{counts['icu_mortality_missing']}`",
            f"- Observed-support stays ≥24 hours: `{counts['observed_mechanical_ventilation_ge_24h_true']}`",
            "- Rows or stays filtered: `false`",
            "- Source columns changed or dropped: `false`",
            "- Cohort generated: `false`",
            "- Time blocking applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Human review gate",
            "",
            "Run the independent complete derived audit before considering promotion. This build report alone does not authorize a derived release.",
            "",
        )
    )


def _load_core_derived_source(
    config: CoreDerivedConfig,
    policy: CoreDerivedPolicy,
) -> CleanedReleaseInput:
    if policy.version == "0.1":
        base_policy = load_derivation_contract_review_policy(
            config.derivation_review.policy_path
        )
        if base_policy.cleaned_release_id != policy.cleaned_release_id:
            raise HarmonizationError(
                "Core-derived and cleaned-release inputs disagree"
            )
        return load_cleaned_release_input(config.derivation_review, base_policy)
    if policy.version == "0.2":
        return load_cleaned_0_2_release_input(config, policy)
    raise HarmonizationError("Unsupported core-derived contract version")


def _candidate_lineage(
    source: CleanedReleaseInput,
    evidence: ReviewedEvidence,
    policy: CoreDerivedPolicy,
) -> dict[str, Any]:
    lineage: dict[str, Any] = {
        "cleaned_release_id": policy.cleaned_release_id,
        "cleaned_release_manifest_sha256": sha256_file(
            source.release_manifest_path
        ),
        "core_derived_contract_sha256": sha256_file(policy.source_path),
        "consolidated_derivation_review_run_id": evidence.core_review_payload[
            "run_id"
        ],
        "consolidated_derivation_review_sha256": sha256_file(
            evidence.core_review_path
        ),
        "driving_pressure_semantics_review_run_id": (
            policy.driving_pressure_review_run_id
        ),
        "driving_pressure_semantics_review_sha256": sha256_file(
            evidence.driving_review_path
        ),
    }
    if policy.version == "0.2":
        if (
            policy.previous_contract_path is None
            or policy.previous_contract_sha256 is None
            or policy.cleaned_promotion_policy_path is None
            or policy.cleaned_promotion_policy_sha256 is None
        ):
            raise HarmonizationError("Core-derived 0.2 lineage is incomplete")
        lineage.update(
            {
                "previous_core_derived_contract_version": "0.1",
                "previous_core_derived_contract_sha256": (
                    policy.previous_contract_sha256
                ),
                "cleaned_0_2_promotion_policy_sha256": (
                    policy.cleaned_promotion_policy_sha256
                ),
                "clinical_formula_changes_from_0_1": 0,
                "output_semantic_changes_from_0_1": 0,
            }
        )
    return lineage


def run_core_derived_candidate_build(
    config: CoreDerivedConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> CoreDerivedBuildResult:
    if config.dataset_context != "production":
        raise HarmonizationError("The approved core-derived build is production-only")
    policy = load_core_derived_policy(config.policy_path)
    source = _load_core_derived_source(config, policy)
    evidence = load_reviewed_evidence(config, policy)
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid core-derived candidate run ID")
    output_parent = config.data_root / policy.candidate_directory_name
    final_output = output_parent / selected_run
    staging_output = output_parent / f".{selected_run}.incomplete"
    review_dir = config.reports_root / "review" / policy.build_review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_markdown = review_dir / f"{selected_run}.md"
    if any(
        path.exists()
        for path in (final_output, staging_output, review_json, review_markdown)
    ):
        raise HarmonizationError("Core-derived candidate run already exists")
    staging_output.mkdir(parents=True, mode=0o700)
    staging_output.chmod(0o700)
    output_schemas = {
        table: derived_schema(source.schemas[table], table, policy.version)
        for table in ("static", "dynamic")
    }
    dynamic_counts: Counter[str] = Counter()
    tracker = VentilationTracker()
    output_files: dict[str, dict[str, Any]] = {}

    dynamic_input = source.source_files["dynamic"]
    dynamic_output = staging_output / "dynamic.parquet"
    with pq.ParquetWriter(
        dynamic_output,
        output_schemas["dynamic"],
        compression=policy.compression,
    ) as writer:
        for batch in pq.ParquetFile(dynamic_input).iter_batches(
            batch_size=policy.rows_per_batch, use_threads=True
        ):
            update_ventilation_tracker(batch, tracker)
            transformed = transform_dynamic_batch(
                batch, output_schemas["dynamic"], dynamic_counts
            )
            writer.write_batch(transformed)
            if progress is not None and dynamic_counts["row_count"] % (
                policy.rows_per_batch * 20
            ) == 0:
                progress(
                    f"derived_build_progress table=dynamic rows={dynamic_counts['row_count']}"
                )
    dynamic_output.chmod(0o600)
    summaries = tracker.finish()
    if (
        dynamic_counts["row_count"] != policy.expected_dynamic_rows
        or tracker.missing_identifier_count
        or tracker.hospital_conflict_count
        or tracker.supported_missing_time_count
        or tracker.nonfinite_time_count
    ):
        raise HarmonizationError("Dynamic derived input contract changed")
    output_files["dynamic"] = {
        "path": str(final_output / "dynamic.parquet"),
        "sha256": sha256_file(dynamic_output),
        "row_count": dynamic_counts["row_count"],
        "schema_sha256": _schema_digest(output_schemas["dynamic"]),
    }
    if progress is not None:
        progress("derived_build_table_complete=dynamic")

    static_counts: Counter[str] = Counter()
    seen_stays: set[str] = set()
    static_input = source.source_files["static"]
    static_output = staging_output / "static.parquet"
    with pq.ParquetWriter(
        static_output,
        output_schemas["static"],
        compression=policy.compression,
    ) as writer:
        for batch in pq.ParquetFile(static_input).iter_batches(
            batch_size=policy.rows_per_batch, use_threads=True
        ):
            transformed = transform_static_batch(
                batch,
                output_schemas["static"],
                summaries,
                static_counts,
                seen_stays,
            )
            writer.write_batch(transformed)
    static_output.chmod(0o600)
    if (
        static_counts["row_count"] != policy.expected_static_rows
        or seen_stays != set(summaries)
    ):
        raise HarmonizationError("Static/dynamic derived stay contract changed")
    output_files["static"] = {
        "path": str(final_output / "static.parquet"),
        "sha256": sha256_file(static_output),
        "row_count": static_counts["row_count"],
        "schema_sha256": _schema_digest(output_schemas["static"]),
    }
    if progress is not None:
        progress("derived_build_table_complete=static")

    accounting = {
        "dynamic_row_count": dynamic_counts["row_count"],
        "hours_non_missing_count": dynamic_counts["hours_non_missing_count"],
        "hours_negative_count": dynamic_counts["hours_negative_count"],
        "driving_pressure_computable_count": dynamic_counts[
            "driving_pressure_computable_count"
        ],
        "driving_pressure_in_range_count": dynamic_counts[
            "driving_pressure_in_range_count"
        ],
        "driving_pressure_out_of_range_count": dynamic_counts[
            "driving_pressure_out_of_range_count"
        ],
        "static_row_count": static_counts["row_count"],
        **{
            key: static_counts[key]
            for key in (
                "hospital_mortality_false",
                "hospital_mortality_true",
                "hospital_mortality_missing",
                "hospital_mortality_conflict_count",
                "icu_mortality_false",
                "icu_mortality_true",
                "icu_mortality_missing",
                "icu_mortality_conflict_count",
                "ventilation_supported_timestamp_count_total",
                "ventilation_episode_count_total",
                "observed_mechanical_ventilation_ge_24h_true",
            )
        },
    }
    if accounting != _expected_accounting(policy):
        raise HarmonizationError("Derived accounting differs from approved evidence")
    if (
        sha256_file(dynamic_input)
        != source.release_manifest["files"]["dynamic"]["sha256"]
        or sha256_file(static_input)
        != source.release_manifest["files"]["static"]["sha256"]
    ):
        raise HarmonizationError("Cleaned release changed during derived build")

    generated = utc_timestamp()
    manifest_path = staging_output / "derived_manifest.json"
    manifest = {
        "artifact": "asic_v3_core_derived_candidate",
        "artifact_version": policy.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "status": "nonpublishable_requires_complete_derived_audit",
        "core_derived_contract_version": policy.version,
        "lineage": _candidate_lineage(source, evidence, policy),
        "outputs": output_files,
        "derivation_accounting": accounting,
        "cleaning_applied": True,
        "derivation_applied": True,
        "rows_or_stays_filtered": False,
        "source_columns_changed_or_dropped": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging_output.replace(final_output)
    manifest_path = final_output / "derived_manifest.json"
    review_payload = {
        "artifact": "asic_v3_core_derived_build_review",
        "artifact_version": policy.candidate_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "cleaned_release_id": policy.cleaned_release_id,
        "core_derived_contract_version": policy.version,
        "overall_status": "pass",
        "blocking_findings": [],
        "technical_blocking_findings": [],
        "derivation_accounting": accounting,
        "cleaned_release_modified": False,
        "rows_or_stays_filtered": False,
        "source_columns_changed_or_dropped": False,
        "cohort_generated": False,
        "time_blocking_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review_payload)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    _write_markdown(review_markdown, _build_markdown(review_payload), 0o640)
    return CoreDerivedBuildResult(
        run_id=selected_run,
        candidate_directory=final_output,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_markdown,
    )
