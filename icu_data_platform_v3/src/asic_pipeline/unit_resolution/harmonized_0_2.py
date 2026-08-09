from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
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
from asic_pipeline.harmonization.harmonized_build import (
    _arrow_type,
    _read_json,
    _schema_digest,
)
from asic_pipeline.harmonization.harmonized_build_audit import (
    _next_slice,
    _parquet_schema_matches,
    _require_output_exhausted,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import RUN_ID_PATTERN, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe
from asic_pipeline.unit_resolution.reviewed_decisions import (
    ReviewedUnitDecisions,
    load_reviewed_unit_decisions,
)
from asic_pipeline.unit_resolution.reviewed_medication_semantics import (
    load_reviewed_medication_semantics,
)
from asic_pipeline.unit_resolution.schema_freeze import (
    UnitSchemaDictionaryFreezeConfig,
    load_unit_schema_dictionary_freeze_config,
    load_unit_schema_dictionary_freeze_policy,
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

EXPECTED_EXECUTION = {
    "source_hospital_field": "hospital_id",
    "source_stay_field": "stay_id_global",
    "static_weight_field": "weight_kg",
    "preserve_source_row_order": True,
    "preserve_every_input_column": True,
    "route_split_source_unchanged_to_parallel_target": True,
    "set_split_canonical_missing_for_source_hospital": True,
    "set_parallel_target_missing_for_other_hospitals": True,
    "preserve_zero_and_null_independently": True,
    "preserve_negative_medication_values": True,
    "require_complete_nonzero_weight_linkage": True,
    "allow_cleaning": False,
    "allow_derivation": False,
}

EXPECTED_BOUNDARY = {
    "read_released_harmonized_0_1": True,
    "read_cleaned_0_1_static_weight_reference": True,
    "read_cleaned_0_1_dynamic": False,
    "read_frozen_contract_0_2": True,
    "modify_harmonized_0_1_release": False,
    "modify_cleaned_0_1_release": False,
    "modify_derived_0_1_release": False,
    "overwrite_outputs": False,
    "filter_rows": False,
    "filter_stays": False,
    "drop_input_columns": False,
    "apply_cleaning": False,
    "apply_derivation": False,
    "update_release_pointer": False,
    "publish": False,
    "authorize_external_data_export": False,
}

EXPECTED_AUDIT = {
    "rows_per_batch": 50000,
    "verify_source_release_hashes_before_and_after": True,
    "verify_candidate_hashes": True,
    "verify_frozen_schema_exactly": True,
    "compare_every_output_cell": True,
    "recompute_all_nine_unit_conversions": True,
    "recompute_all_nine_semantic_splits": True,
    "verify_uk00_static_weight_linkage": True,
    "verify_cleaned_static_weight_reference_hash_before_and_after": True,
    "verify_zero_null_and_negative_conservation": True,
    "compare_all_five_provenance_fields": True,
    "verify_all_rows_and_source_order": True,
    "verify_rule_level_counts": True,
}

EXPECTED_AUDIT_BOUNDARY = {
    "read_released_harmonized_0_1": True,
    "read_cleaned_0_1_static_weight_reference": True,
    "read_cleaned_0_1_dynamic": False,
    "read_harmonized_0_2_candidate": True,
    "modify_clinical_data": False,
    "write_reports_only": True,
    "apply_cleaning": False,
    "apply_derivation": False,
    "approve_release_automatically": False,
    "update_release_pointer": False,
    "publish": False,
    "authorize_external_data_export": False,
}


@dataclass(frozen=True)
class Harmonized02BuildPolicy:
    version: str
    source_release_id: str
    source_contract_version: str
    releases_directory: Path
    weight_release_id: str
    cleaned_releases_directory: Path
    weight_reference_reason: str
    target_contract_version: str
    target_contract_directory: Path
    freeze_policy_path: Path
    freeze_policy_sha256: str
    reviewed_decisions_path: Path
    reviewed_decisions_sha256: str
    medication_semantics_path: Path
    medication_semantics_sha256: str
    expected: dict[str, int]
    rows_per_batch: int
    compression: str
    candidate_directory_name: str
    review_directory_name: str
    artifact_version: str
    output_status: str
    source_path: Path


@dataclass(frozen=True)
class Harmonized02AuditPolicy:
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
class Harmonized02BuildConfig:
    freeze: UnitSchemaDictionaryFreezeConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.freeze.dataset_context

    @property
    def data_root(self) -> Path:
        return self.freeze.data_root

    @property
    def reports_root(self) -> Path:
        return self.freeze.reports_root


@dataclass(frozen=True)
class Harmonized02AuditConfig:
    build: Harmonized02BuildConfig
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
class Harmonized02BuildResult:
    run_id: str
    candidate_directory: Path
    manifest_path: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class Harmonized02AuditResult:
    run_id: str
    build_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, str], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path


@dataclass(frozen=True)
class Harmonized02Inputs:
    source_manifest_path: Path
    source_manifest: dict[str, Any]
    source_files: dict[str, Path]
    weight_manifest_path: Path
    weight_static_path: Path
    target_manifest_path: Path
    target_manifest: dict[str, Any]
    target_schemas: dict[str, pa.Schema]
    weights: dict[str, float]
    weight_accounting: dict[str, int]


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(value: Any, location: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location} must be a positive integer")
    return value


def _contained(value: str, location: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ConfigurationError(f"{location} must be a contained relative path")
    return path


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


def load_harmonized_0_2_build_config(
    path: str | Path,
) -> Harmonized02BuildConfig:
    source = Path(path).expanduser().resolve()
    freeze = load_unit_schema_dictionary_freeze_config(source)
    raw = load_yaml_mapping(source, "Harmonized 0.2 build configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_0_2_build_policy", "config"), source
    )
    return Harmonized02BuildConfig(freeze=freeze, policy_path=policy_path)


def load_harmonized_0_2_audit_config(
    path: str | Path,
) -> Harmonized02AuditConfig:
    source = Path(path).expanduser().resolve()
    build = load_harmonized_0_2_build_config(source)
    raw = load_yaml_mapping(source, "Harmonized 0.2 audit configuration")
    policy_path = resolve_path(
        required_string(raw, "harmonized_0_2_audit_policy", "config"), source
    )
    return Harmonized02AuditConfig(build=build, policy_path=policy_path)


def load_harmonized_0_2_build_policy(
    path: str | Path,
) -> Harmonized02BuildPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized 0.2 build policy")
    if (
        raw.get("harmonized_0_2_build_policy_version") != "0.1"
        or raw.get("status")
        != "approved_after_frozen_contract_for_nonpublishable_candidate"
    ):
        raise ConfigurationError("Harmonized 0.2 build policy is invalid")
    input_raw = _mapping(raw.get("input"), "input")
    target = _mapping(raw.get("target_contract"), "target_contract")
    reviewed = _mapping(raw.get("reviewed_decisions"), "reviewed_decisions")
    expected = _mapping(raw.get("expected"), "expected")
    execution = _mapping(raw.get("execution"), "execution")
    streaming = _mapping(raw.get("streaming"), "streaming")
    outputs = _mapping(raw.get("outputs"), "outputs")
    if execution != EXPECTED_EXECUTION or _mapping(
        raw.get("boundary"), "boundary"
    ) != EXPECTED_BOUNDARY:
        raise ConfigurationError("Harmonized 0.2 execution boundary changed")
    expected_exact = {
        "static_rows": 16054,
        "dynamic_rows": 24069379,
        "static_clinical_variables": 23,
        "dynamic_clinical_variables": 139,
        "provenance_fields_per_table": 5,
        "unit_conversion_rules": 9,
        "semantic_split_rules": 9,
        "medication_or_therapy_variables": 34,
        "current_negative_medication_values_preserved": 201,
    }
    if expected != expected_exact:
        raise ConfigurationError("Harmonized 0.2 expected accounting changed")

    def immutable_path(
        group: dict[str, Any], path_key: str, hash_key: str, label: str
    ) -> tuple[Path, str]:
        resolved = resolve_path(required_string(group, path_key, label), source)
        expected_hash = required_string(group, hash_key, label)
        if sha256_file(resolved) != expected_hash:
            raise ConfigurationError(f"Immutable {label} changed")
        return resolved, expected_hash

    freeze_path, freeze_hash = immutable_path(
        target, "freeze_policy", "freeze_policy_sha256", "target_contract"
    )
    decisions_path, decisions_hash = immutable_path(
        reviewed,
        "unit_and_semantic_splits",
        "unit_and_semantic_splits_sha256",
        "reviewed_decisions",
    )
    medication_path, medication_hash = immutable_path(
        reviewed,
        "medication_value_semantics",
        "medication_value_semantics_sha256",
        "reviewed_decisions",
    )
    reason = required_string(
        input_raw, "static_weight_reference_reason", "input"
    )
    if "no cleaned dynamic value is used" not in reason:
        raise ConfigurationError("Static-weight reference boundary changed")
    policy = Harmonized02BuildPolicy(
        version="0.1",
        source_release_id=required_string(
            input_raw, "harmonized_release_id", "input"
        ),
        source_contract_version=required_string(
            input_raw, "harmonized_contract_version", "input"
        ),
        releases_directory=_contained(
            required_string(input_raw, "releases_directory", "input"),
            "input.releases_directory",
        ),
        weight_release_id=required_string(
            input_raw, "static_weight_reference_cleaned_release_id", "input"
        ),
        cleaned_releases_directory=_contained(
            required_string(input_raw, "cleaned_releases_directory", "input"),
            "input.cleaned_releases_directory",
        ),
        weight_reference_reason=reason,
        target_contract_version=required_string(target, "version", "target_contract"),
        target_contract_directory=_contained(
            required_string(target, "directory", "target_contract"),
            "target_contract.directory",
        ),
        freeze_policy_path=freeze_path,
        freeze_policy_sha256=freeze_hash,
        reviewed_decisions_path=decisions_path,
        reviewed_decisions_sha256=decisions_hash,
        medication_semantics_path=medication_path,
        medication_semantics_sha256=medication_hash,
        expected={key: int(value) for key, value in expected.items()},
        rows_per_batch=_positive_int(
            streaming.get("rows_per_batch"), "streaming.rows_per_batch"
        ),
        compression=required_string(
            streaming, "parquet_compression", "streaming"
        ),
        candidate_directory_name=required_string(
            outputs, "candidate_directory_name", "outputs"
        ),
        review_directory_name=required_string(
            outputs, "build_review_directory_name", "outputs"
        ),
        artifact_version=required_string(
            outputs, "candidate_artifact_version", "outputs"
        ),
        output_status=required_string(outputs, "status", "outputs"),
        source_path=source,
    )
    if (
        policy.source_release_id != "20260806T111156Z"
        or policy.source_contract_version != "0.1"
        or policy.weight_release_id != "20260806T114234Z"
        or policy.target_contract_version != "0.2"
        or policy.rows_per_batch != 50000
        or policy.compression != "zstd"
        or policy.artifact_version != "0.1"
        or policy.candidate_directory_name != "harmonized_0_2_candidates"
        or policy.review_directory_name != "harmonized_0_2_build"
        or policy.output_status
        != "frozen_contract_0_2_nonpublishable_requires_complete_audit"
    ):
        raise ConfigurationError("Harmonized 0.2 build scope changed")
    return policy


def load_harmonized_0_2_audit_policy(
    path: str | Path,
) -> Harmonized02AuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Harmonized 0.2 audit policy")
    if (
        raw.get("harmonized_0_2_audit_policy_version") != "0.1"
        or raw.get("status")
        != "approved_for_complete_independent_read_only_audit"
    ):
        raise ConfigurationError("Harmonized 0.2 audit policy is invalid")
    build_path = resolve_path(
        required_string(raw, "build_policy", "audit policy"), source
    )
    build_hash = required_string(raw, "build_policy_sha256", "audit policy")
    if sha256_file(build_path) != build_hash:
        raise ConfigurationError("Approved harmonized 0.2 build policy changed")
    audit = _mapping(raw.get("audit"), "audit")
    if audit != EXPECTED_AUDIT or _mapping(
        raw.get("boundary"), "boundary"
    ) != EXPECTED_AUDIT_BOUNDARY:
        raise ConfigurationError("Complete harmonized 0.2 audit scope changed")
    outputs = _mapping(raw.get("outputs"), "outputs")
    policy = Harmonized02AuditPolicy(
        version="0.1",
        build_policy_path=build_path,
        build_policy_sha256=build_hash,
        rows_per_batch=50000,
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
    if (
        policy.private_directory_name != "harmonized_0_2_audit"
        or policy.review_directory_name != "harmonized_0_2_audit"
        or policy.private_artifact_version != "0.1"
        or policy.review_artifact_version != "0.1"
    ):
        raise ConfigurationError("Harmonized 0.2 audit outputs changed")
    return policy


def _target_schemas(
    config: Harmonized02BuildConfig,
    policy: Harmonized02BuildPolicy,
) -> tuple[dict[str, pa.Schema], dict[str, Any]]:
    freeze_policy = load_unit_schema_dictionary_freeze_policy(
        policy.freeze_policy_path
    )
    if (
        config.freeze.policy_path.resolve() != policy.freeze_policy_path.resolve()
        or freeze_policy.contract_version != policy.target_contract_version
    ):
        raise HarmonizationError("Frozen 0.2 policy lineage changed")
    contract = config.data_root / policy.target_contract_directory
    manifest_path = contract / "freeze_manifest.json"
    manifest = _read_json(manifest_path, "Frozen contract 0.2 manifest")
    if not isinstance(manifest, dict):
        raise HarmonizationError("Frozen contract 0.2 manifest is invalid")
    files = manifest.get("files")
    lineage = manifest.get("lineage")
    if (
        manifest.get("artifact")
        != "asic_v3_frozen_harmonized_schema_dictionary"
        or manifest.get("contract_version") != "0.2"
        or manifest.get("schema_frozen") is not True
        or manifest.get("dictionary_frozen") is not True
        or manifest.get("harmonized_build_0_2_implementation_authorized") is not True
        or manifest.get("unit_conversions_activated") is not False
        or manifest.get("semantic_splits_activated") is not False
        or manifest.get("cleaning_rule_activated") is not False
        or manifest.get("publication_ready") is not False
        or not isinstance(files, dict)
        or not isinstance(lineage, dict)
        or lineage.get("freeze_policy_sha256") != policy.freeze_policy_sha256
        or lineage.get("reviewed_unit_decisions_sha256")
        != policy.reviewed_decisions_sha256
        or lineage.get("reviewed_medication_value_semantics_sha256")
        != policy.medication_semantics_sha256
    ):
        raise HarmonizationError("Frozen contract 0.2 lineage changed")
    for name, expected_hash in files.items():
        candidate = contract / str(name)
        if not candidate.is_file() or sha256_file(candidate) != expected_hash:
            raise HarmonizationError("A frozen contract 0.2 file changed")
    schemas: dict[str, pa.Schema] = {}
    expected_counts = {
        "static": policy.expected["static_clinical_variables"],
        "dynamic": policy.expected["dynamic_clinical_variables"],
    }
    for table in ("static", "dynamic"):
        rows = _read_json(contract / f"{table}_schema.json", f"Frozen {table} schema")
        if not isinstance(rows, list) or len(rows) != expected_counts[table]:
            raise HarmonizationError("Frozen 0.2 schema count changed")
        fields = [
            pa.field(
                str(row["variable"]),
                _arrow_type(str(row["physical_type"])),
                nullable=True,
                metadata={
                    b"asic_v3_unit": str(row["unit"]).encode(),
                    b"asic_v3_analysis_eligibility": str(
                        row["analysis_eligibility"]
                    ).encode(),
                },
            )
            for row in rows
        ]
        fields.extend(
            pa.field(name, _arrow_type(physical_type), nullable=False)
            for name, physical_type in freeze_policy.provenance_fields
        )
        schemas[table] = pa.schema(
            fields,
            metadata={
                b"asic_v3_stage": b"harmonized_0_2_candidate",
                b"asic_v3_contract_version": b"0.2",
                b"publication_ready": b"false",
                b"cleaning_applied": b"false",
                b"derivation_applied": b"false",
            },
        )
    return schemas, manifest


def _validate_release_file(
    release_dir: Path,
    manifest: dict[str, Any],
    table: str,
    expected_rows: int,
) -> Path:
    summaries = manifest.get("files")
    path = release_dir / f"{table}.parquet"
    summary = summaries.get(table) if isinstance(summaries, dict) else None
    if not isinstance(summary, dict) or not path.is_file():
        raise HarmonizationError(f"Released {table} input is unavailable")
    parquet = pq.ParquetFile(path)
    if (
        summary.get("sha256") != sha256_file(path)
        or summary.get("row_count") != expected_rows
        or parquet.metadata.num_rows != expected_rows
    ):
        raise HarmonizationError(f"Released {table} input changed")
    return path


def _load_weights(path: Path, rows_per_batch: int) -> tuple[dict[str, float], dict[str, int]]:
    parquet = pq.ParquetFile(path)
    required = {"stay_id_global", "hospital_id", "weight_kg"}
    if not required.issubset(parquet.schema_arrow.names):
        raise HarmonizationError("Cleaned static weight reference is incomplete")
    weights: dict[str, float] = {}
    seen_uk00_stays: set[str] = set()
    rows = 0
    uk00_rows = 0
    null_weights = 0
    invalid_weights = 0
    duplicate_stays = 0
    missing_stays = 0
    for batch in parquet.iter_batches(
        batch_size=rows_per_batch,
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
            if hospital != "asic_UK00":
                continue
            uk00_rows += 1
            key = str(stay)
            if key in seen_uk00_stays:
                duplicate_stays += 1
                continue
            seen_uk00_stays.add(key)
            if weight is None:
                null_weights += 1
                continue
            numeric = float(weight)
            if not math.isfinite(numeric) or numeric <= 0:
                invalid_weights += 1
                continue
            weights[key] = numeric
    if missing_stays or duplicate_stays:
        raise HarmonizationError("Cleaned static weight identifiers are invalid")
    return weights, {
        "static_rows_scanned": rows,
        "uk00_static_rows": uk00_rows,
        "uk00_valid_weight_count": len(weights),
        "uk00_null_weight_count": null_weights,
        "uk00_invalid_weight_count": invalid_weights,
        "duplicate_static_stay_count": duplicate_stays,
        "missing_static_stay_count": missing_stays,
    }


def _load_inputs(
    config: Harmonized02BuildConfig,
    policy: Harmonized02BuildPolicy,
) -> Harmonized02Inputs:
    target_schemas, target_manifest = _target_schemas(config, policy)
    source_dir = (
        config.data_root / policy.releases_directory / policy.source_release_id
    )
    source_manifest_path = source_dir / "release_manifest.json"
    source_manifest = _read_json(
        source_manifest_path, "Released harmonized 0.1 manifest"
    )
    if not isinstance(source_manifest, dict) or (
        source_manifest.get("artifact") != "asic_v3_harmonized_release"
        or source_manifest.get("release_id") != policy.source_release_id
        or source_manifest.get("status") != "released_harmonized_layer"
        or source_manifest.get("frozen_contract_version")
        != policy.source_contract_version
        or source_manifest.get("harmonized_layer_ready") is not True
        or source_manifest.get("cleaning_applied") is not False
        or source_manifest.get("derivation_applied") is not False
        or source_manifest.get("publication_ready") is not True
    ):
        raise HarmonizationError("Released harmonized 0.1 input changed")
    source_files = {
        table: _validate_release_file(
            source_dir,
            source_manifest,
            table,
            policy.expected[f"{table}_rows"],
        )
        for table in ("static", "dynamic")
    }
    decisions = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    split_targets = [str(row["target_variable"]) for row in decisions.semantic_splits]
    provenance_count = policy.expected["provenance_fields_per_table"]
    for table in ("static", "dynamic"):
        source_schema = pq.ParquetFile(source_files[table]).schema_arrow
        target_schema = target_schemas[table]
        target_clinical_count = policy.expected[f"{table}_clinical_variables"]
        source_clinical_count = target_clinical_count - (
            len(split_targets) if table == "dynamic" else 0
        )
        expected_source_names = (
            target_schema.names[:source_clinical_count]
            + target_schema.names[target_clinical_count:]
        )
        if (
            source_schema.names != expected_source_names
            or len(source_schema) != source_clinical_count + provenance_count
        ):
            raise HarmonizationError("Released 0.1 schema is not the 0.2 base")
        for name in expected_source_names:
            if source_schema.field(name).type != target_schema.field(name).type:
                raise HarmonizationError("A source physical type changed")
    if target_schemas["dynamic"].names[130:139] != split_targets:
        raise HarmonizationError("Frozen semantic-split order changed")

    weight_dir = (
        config.data_root
        / policy.cleaned_releases_directory
        / policy.weight_release_id
    )
    weight_manifest_path = weight_dir / "release_manifest.json"
    weight_manifest = _read_json(
        weight_manifest_path, "Released cleaned static-weight manifest"
    )
    if not isinstance(weight_manifest, dict) or (
        weight_manifest.get("artifact") != "asic_v3_cleaned_release"
        or weight_manifest.get("release_id") != policy.weight_release_id
        or weight_manifest.get("status") != "released_cleaned_layer"
        or weight_manifest.get("harmonized_release_id") != policy.source_release_id
        or weight_manifest.get("harmonized_contract_version")
        != policy.source_contract_version
        or weight_manifest.get("cleaned_layer_ready") is not True
        or weight_manifest.get("publication_ready") is not True
    ):
        raise HarmonizationError("Cleaned static-weight reference changed")
    weight_static = _validate_release_file(
        weight_dir,
        weight_manifest,
        "static",
        policy.expected["static_rows"],
    )
    weights, weight_accounting = _load_weights(
        weight_static, policy.rows_per_batch
    )
    if weight_accounting["static_rows_scanned"] != policy.expected["static_rows"]:
        raise HarmonizationError("Static weight-reference row count changed")
    return Harmonized02Inputs(
        source_manifest_path=source_manifest_path,
        source_manifest=source_manifest,
        source_files=source_files,
        weight_manifest_path=weight_manifest_path,
        weight_static_path=weight_static,
        target_manifest_path=(
            config.data_root / policy.target_contract_directory / "freeze_manifest.json"
        ),
        target_manifest=target_manifest,
        target_schemas=target_schemas,
        weights=weights,
        weight_accounting=weight_accounting,
    )


def _true_count(value: pa.Array | pa.ChunkedArray) -> int:
    scalar = pc.sum(pc.cast(pc.fill_null(value, False), pa.int64()))
    return int(scalar.as_py() or 0)


def _numeric_counts(values: pa.Array) -> Counter[str]:
    valid = pc.is_valid(values)
    finite = pc.and_(valid, pc.fill_null(pc.is_finite(values), False))
    return Counter(
        {
            "null": values.null_count,
            "non_null": len(values) - values.null_count,
            "finite_zero": _true_count(pc.and_(finite, pc.equal(values, 0.0))),
            "finite_positive": _true_count(pc.and_(finite, pc.greater(values, 0.0))),
            "finite_negative": _true_count(pc.and_(finite, pc.less(values, 0.0))),
            "nonfinite": _true_count(
                pc.and_(
                    valid,
                    pc.invert(pc.fill_null(pc.is_finite(values), False)),
                )
            ),
        }
    )


def _add_prefixed(
    target: Counter[str], prefix: str, values: pa.Array
) -> None:
    for key, count in _numeric_counts(values).items():
        target[f"{prefix}_{key}"] += count


def _simple_conversion(values: pa.Array, action: dict[str, Any]) -> pa.Array:
    factor = float(action["factor"])
    if not math.isfinite(factor) or factor <= 0:
        raise HarmonizationError("Approved conversion factor is invalid")
    if action["action"] == "multiply":
        return pc.multiply(values, pa.scalar(factor, pa.float64()))
    if action["action"] == "divide":
        return pc.divide(values, pa.scalar(factor, pa.float64()))
    raise HarmonizationError("Approved conversion operation changed")


def _weight_conversion(
    source: pa.Array,
    current: pa.Array,
    hospitals: pa.Array,
    stays: pa.Array,
    hospital: str,
    weights: dict[str, float],
    counters: Counter[str],
) -> pa.Array:
    output: list[float | None] = []
    for value, current_value, observed_hospital, stay in zip(
        source.to_pylist(),
        current.to_pylist(),
        hospitals.to_pylist(),
        stays.to_pylist(),
        strict=True,
    ):
        if observed_hospital != hospital:
            output.append(current_value)
            continue
        if value is None:
            output.append(None)
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            raise HarmonizationError("Weight conversion input is non-finite")
        if numeric == 0.0:
            counters["zero_preserved_without_weight_requirement"] += 1
            output.append(0.0)
            continue
        if stay is None:
            counters["missing_stay_id"] += 1
            output.append(None)
            continue
        weight = weights.get(str(stay))
        if weight is None:
            counters["missing_or_invalid_weight"] += 1
            output.append(None)
            continue
        counters["converted_with_weight"] += 1
        output.append(numeric * weight)
    return pa.array(output, type=pa.float64())


def _transform_batch(
    batch: pa.RecordBatch,
    table: str,
    schema: pa.Schema,
    policy: Harmonized02BuildPolicy,
    decisions: ReviewedUnitDecisions,
    weights: dict[str, float],
    conversion_counts: dict[str, Counter[str]],
    split_counts: dict[str, Counter[str]],
    medication_counts: Counter[str],
) -> pa.RecordBatch:
    hospital_index = batch.schema.get_field_index("hospital_id")
    if hospital_index < 0:
        raise HarmonizationError("Harmonized input has no hospital identifier")
    hospitals = batch.column(hospital_index)
    stay_index = batch.schema.get_field_index("stay_id_global")
    stays = batch.column(stay_index) if stay_index >= 0 else None
    actions: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decisions.base_candidate.hospital_harmonization_actions:
        if str(row["decision_id"]) in decisions.approved_harmonization_action_ids:
            actions[str(row["variable"])].append(row)
    splits_by_source = {
        str(row["source_variable"]): row for row in decisions.semantic_splits
    }
    splits_by_target = {
        str(row["target_variable"]): row for row in decisions.semantic_splits
    }
    medication_sources = set(decisions.medication_variables)
    medication_outputs = medication_sources | set(splits_by_target)
    arrays: list[pa.Array] = []
    for field in schema:
        name = field.name
        if name in splits_by_target:
            split = splits_by_target[name]
            source_index = batch.schema.get_field_index(
                str(split["source_variable"])
            )
            if source_index < 0:
                raise HarmonizationError("Semantic-split source is absent")
            source = pc.cast(batch.column(source_index), field.type, safe=True)
            mask = pc.fill_null(
                pc.equal(hospitals, str(split["hospital"])), False
            )
            scoped = pc.filter(source, mask)
            transformed = pc.if_else(mask, source, pa.nulls(len(source), field.type))
            counts = split_counts[str(split["decision_id"])]
            counts["scope_rows"] += len(scoped)
            counts["other_hospital_rows"] += len(source) - len(scoped)
            _add_prefixed(counts, "source", scoped)
            _add_prefixed(counts, "parallel_target", pc.filter(transformed, mask))
        else:
            source_index = batch.schema.get_field_index(name)
            if source_index < 0:
                raise HarmonizationError(f"Input field is unavailable: {table}.{name}")
            source = batch.column(source_index)
            transformed = pc.cast(source, field.type, safe=True)
            split = splits_by_source.get(name) if table == "dynamic" else None
            if split is not None:
                mask = pc.fill_null(
                    pc.equal(hospitals, str(split["hospital"])), False
                )
                scoped = pc.filter(transformed, mask)
                transformed = pc.if_else(
                    mask, pa.nulls(len(transformed), field.type), transformed
                )
                counts = split_counts[str(split["decision_id"])]
                counts["canonical_values_masked"] += len(scoped) - scoped.null_count
                counts["canonical_nulls_retained"] += scoped.null_count
            variable_actions = actions.get(name, ()) if table == "dynamic" else ()
            for action in variable_actions:
                mask = pc.fill_null(
                    pc.equal(hospitals, str(action["hospital"])), False
                )
                scoped = pc.filter(source, mask)
                counts = conversion_counts[str(action["decision_id"])]
                counts["scope_rows"] += len(scoped)
                _add_prefixed(counts, "input", pc.cast(scoped, pa.float64()))
                if action["action"] == "multiply_by_static_weight_kg":
                    if stays is None:
                        raise HarmonizationError("Weight conversion stay ID is absent")
                    transformed = _weight_conversion(
                        pc.cast(source, pa.float64()),
                        pc.cast(transformed, pa.float64()),
                        hospitals,
                        stays,
                        str(action["hospital"]),
                        weights,
                        counts,
                    )
                else:
                    converted_all = _simple_conversion(
                        pc.cast(source, pa.float64()), action
                    )
                    transformed = pc.if_else(mask, converted_all, transformed)
                _add_prefixed(counts, "output", pc.filter(transformed, mask))
        if transformed.type != field.type:
            transformed = pc.cast(transformed, field.type, safe=True)
        if table == "dynamic" and name in medication_sources:
            _add_prefixed(
                medication_counts,
                "input",
                pc.cast(batch.column(batch.schema.get_field_index(name)), pa.float64()),
            )
        if table == "dynamic" and name in medication_outputs:
            _add_prefixed(
                medication_counts, "output", pc.cast(transformed, pa.float64())
            )
        arrays.append(transformed)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _audit_weight_conversion(
    source: pa.Array,
    current: pa.Array,
    hospitals: pa.Array,
    stays: pa.Array,
    hospital: str,
    weights: dict[str, float],
    counters: Counter[str],
) -> pa.Array:
    values = source.to_pylist()
    current_values = current.to_pylist()
    hospital_values = hospitals.to_pylist()
    stay_values = stays.to_pylist()
    output: list[float | None] = []
    for index in range(len(source)):
        if hospital_values[index] != hospital:
            output.append(current_values[index])
            continue
        value = values[index]
        if value is None:
            output.append(None)
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            raise HarmonizationError("Audited weight conversion input is non-finite")
        if numeric == 0.0:
            counters["zero_preserved_without_weight_requirement"] += 1
            output.append(0.0)
            continue
        stay = stay_values[index]
        if stay is None:
            counters["missing_stay_id"] += 1
            output.append(None)
            continue
        weight = weights.get(str(stay))
        if weight is None:
            counters["missing_or_invalid_weight"] += 1
            output.append(None)
            continue
        counters["converted_with_weight"] += 1
        output.append(numeric * weight)
    return pa.array(output, type=pa.float64())


def _audit_expected_batch(
    batch: pa.RecordBatch,
    table: str,
    schema: pa.Schema,
    decisions: ReviewedUnitDecisions,
    weights: dict[str, float],
    conversion_counts: dict[str, Counter[str]],
    split_counts: dict[str, Counter[str]],
    medication_counts: Counter[str],
) -> pa.RecordBatch:
    hospital_index = batch.schema.get_field_index("hospital_id")
    if hospital_index < 0:
        raise HarmonizationError("Audited input has no hospital identifier")
    hospitals = batch.column(hospital_index)
    stay_index = batch.schema.get_field_index("stay_id_global")
    stays = batch.column(stay_index) if stay_index >= 0 else None
    approved_ids = set(decisions.approved_harmonization_action_ids)
    approved_actions = tuple(
        row
        for row in decisions.base_candidate.hospital_harmonization_actions
        if str(row["decision_id"]) in approved_ids
    )
    medication_sources = set(decisions.medication_variables)
    split_targets = {
        str(row["target_variable"]) for row in decisions.semantic_splits
    }
    medication_outputs = medication_sources | split_targets
    arrays: list[pa.Array] = []
    for field in schema:
        name = field.name
        target_split = next(
            (
                row
                for row in decisions.semantic_splits
                if str(row["target_variable"]) == name
            ),
            None,
        )
        if target_split is not None:
            source_index = batch.schema.get_field_index(
                str(target_split["source_variable"])
            )
            if source_index < 0:
                raise HarmonizationError("Audited semantic-split source is absent")
            source = pc.cast(batch.column(source_index), field.type, safe=True)
            mask = pc.fill_null(
                pc.equal(hospitals, str(target_split["hospital"])), False
            )
            transformed = pc.if_else(
                mask,
                source,
                pa.nulls(batch.num_rows, field.type),
            )
            scoped_source = pc.filter(source, mask)
            counts = split_counts[str(target_split["decision_id"])]
            counts["scope_rows"] += len(scoped_source)
            counts["other_hospital_rows"] += batch.num_rows - len(scoped_source)
            _add_prefixed(counts, "source", scoped_source)
            _add_prefixed(
                counts,
                "parallel_target",
                pc.filter(transformed, mask),
            )
        else:
            source_index = batch.schema.get_field_index(name)
            if source_index < 0:
                raise HarmonizationError(
                    f"Audited input field is unavailable: {table}.{name}"
                )
            source = batch.column(source_index)
            transformed = pc.cast(source, field.type, safe=True)
            source_split = next(
                (
                    row
                    for row in decisions.semantic_splits
                    if str(row["source_variable"]) == name
                ),
                None,
            ) if table == "dynamic" else None
            if source_split is not None:
                split_mask = pc.fill_null(
                    pc.equal(hospitals, str(source_split["hospital"])), False
                )
                scoped_source = pc.filter(transformed, split_mask)
                transformed = pc.if_else(
                    split_mask,
                    pa.nulls(batch.num_rows, field.type),
                    transformed,
                )
                counts = split_counts[str(source_split["decision_id"])]
                counts["canonical_values_masked"] += (
                    len(scoped_source) - scoped_source.null_count
                )
                counts["canonical_nulls_retained"] += scoped_source.null_count
            if table == "dynamic":
                for action in approved_actions:
                    if str(action["variable"]) != name:
                        continue
                    action_mask = pc.fill_null(
                        pc.equal(hospitals, str(action["hospital"])), False
                    )
                    scoped_input = pc.filter(source, action_mask)
                    counts = conversion_counts[str(action["decision_id"])]
                    counts["scope_rows"] += len(scoped_input)
                    _add_prefixed(
                        counts,
                        "input",
                        pc.cast(scoped_input, pa.float64()),
                    )
                    raw_numeric = pc.cast(source, pa.float64())
                    if action["action"] == "multiply_by_static_weight_kg":
                        if stays is None:
                            raise HarmonizationError(
                                "Audited weight-conversion stay ID is absent"
                            )
                        transformed = _audit_weight_conversion(
                            raw_numeric,
                            pc.cast(transformed, pa.float64()),
                            hospitals,
                            stays,
                            str(action["hospital"]),
                            weights,
                            counts,
                        )
                    else:
                        factor = float(action["factor"])
                        if not math.isfinite(factor) or factor <= 0:
                            raise HarmonizationError(
                                "Audited conversion factor is invalid"
                            )
                        if action["action"] == "multiply":
                            converted = pc.multiply(raw_numeric, factor)
                        elif action["action"] == "divide":
                            converted = pc.divide(raw_numeric, factor)
                        else:
                            raise HarmonizationError(
                                "Audited conversion operation changed"
                            )
                        transformed = pc.if_else(
                            action_mask,
                            converted,
                            transformed,
                        )
                    _add_prefixed(
                        counts,
                        "output",
                        pc.filter(transformed, action_mask),
                    )
        if transformed.type != field.type:
            transformed = pc.cast(transformed, field.type, safe=True)
        if table == "dynamic" and name in medication_sources:
            input_index = batch.schema.get_field_index(name)
            _add_prefixed(
                medication_counts,
                "input",
                pc.cast(batch.column(input_index), pa.float64()),
            )
        if table == "dynamic" and name in medication_outputs:
            _add_prefixed(
                medication_counts,
                "output",
                pc.cast(transformed, pa.float64()),
            )
        arrays.append(transformed)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _accounting(
    conversion_counts: dict[str, Counter[str]],
    split_counts: dict[str, Counter[str]],
    medication_counts: Counter[str],
    weight_accounting: dict[str, int],
) -> dict[str, Any]:
    return {
        "unit_conversions": {
            key: dict(sorted(value.items()))
            for key, value in sorted(conversion_counts.items())
        },
        "semantic_splits": {
            key: dict(sorted(value.items()))
            for key, value in sorted(split_counts.items())
        },
        "medication_value_semantics": dict(sorted(medication_counts.items())),
        "static_weight_reference": dict(sorted(weight_accounting.items())),
    }


def _sanitized_rule_summaries(
    decisions: ReviewedUnitDecisions,
    accounting: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    action_by_id = {
        str(row["decision_id"]): row
        for row in decisions.base_candidate.hospital_harmonization_actions
        if str(row["decision_id"]) in decisions.approved_harmonization_action_ids
    }
    conversion_summaries = []
    for decision_id in decisions.approved_harmonization_action_ids:
        action = action_by_id[decision_id]
        counts = accounting["unit_conversions"][decision_id]
        conversion_summaries.append(
            {
                "decision_id": decision_id,
                "hospital": action["hospital"],
                "variable": action["variable"],
                "operation": action["action"],
                "factor": action.get("factor"),
                "scope_rows": counts.get("scope_rows", 0),
                "input_non_null": counts.get("input_non_null", 0),
                "output_non_null": counts.get("output_non_null", 0),
                "finite_zero_preserved": counts.get("output_finite_zero", 0),
                "missing_or_invalid_weight": counts.get(
                    "missing_or_invalid_weight", 0
                ),
            }
        )
    split_summaries = []
    for split in decisions.semantic_splits:
        decision_id = str(split["decision_id"])
        counts = accounting["semantic_splits"][decision_id]
        split_summaries.append(
            {
                "decision_id": decision_id,
                "hospital": split["hospital"],
                "source_variable": split["source_variable"],
                "target_variable": split["target_variable"],
                "scope_rows": counts.get("scope_rows", 0),
                "source_non_null": counts.get("source_non_null", 0),
                "canonical_values_masked": counts.get(
                    "canonical_values_masked", 0
                ),
                "parallel_target_non_null": counts.get(
                    "parallel_target_non_null", 0
                ),
                "finite_zero_preserved": counts.get(
                    "parallel_target_finite_zero", 0
                ),
            }
        )
    return conversion_summaries, split_summaries


def _validate_accounting(
    policy: Harmonized02BuildPolicy,
    accounting: dict[str, Any],
) -> None:
    conversions = accounting["unit_conversions"]
    splits = accounting["semantic_splits"]
    medication = accounting["medication_value_semantics"]
    if (
        len(conversions) != policy.expected["unit_conversion_rules"]
        or len(splits) != policy.expected["semantic_split_rules"]
        or any(row.get("scope_rows", 0) <= 0 for row in conversions.values())
        or any(row.get("scope_rows", 0) <= 0 for row in splits.values())
    ):
        raise HarmonizationError("Harmonized 0.2 rule coverage changed")
    for row in conversions.values():
        if (
            row.get("input_null", 0) != row.get("output_null", 0)
            or row.get("input_non_null", 0) != row.get("output_non_null", 0)
            or row.get("input_finite_zero", 0)
            != row.get("output_finite_zero", 0)
            or row.get("input_finite_negative", 0)
            != row.get("output_finite_negative", 0)
            or row.get("input_nonfinite", 0)
            != row.get("output_nonfinite", 0)
            or row.get("missing_stay_id", 0)
            or row.get("missing_or_invalid_weight", 0)
        ):
            raise HarmonizationError("A unit conversion violated value conservation")
    for row in splits.values():
        for suffix in (
            "null",
            "non_null",
            "finite_zero",
            "finite_positive",
            "finite_negative",
            "nonfinite",
        ):
            if row.get(f"source_{suffix}", 0) != row.get(
                f"parallel_target_{suffix}", 0
            ):
                raise HarmonizationError("A semantic split changed source values")
        if row.get("canonical_values_masked", 0) != row.get(
            "source_non_null", 0
        ):
            raise HarmonizationError("A semantic split did not clear its canonical scope")
    expected_negative = policy.expected[
        "current_negative_medication_values_preserved"
    ]
    for suffix in ("finite_zero", "finite_positive", "finite_negative", "nonfinite"):
        if medication.get(f"input_{suffix}", 0) != medication.get(
            f"output_{suffix}", 0
        ):
            raise HarmonizationError("Medication zero/sign conservation changed")
    if (
        medication.get("input_finite_negative", 0) != expected_negative
        or medication.get("output_finite_negative", 0) != expected_negative
    ):
        raise HarmonizationError("Expected negative medication evidence changed")


def _build_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
            "# ASIC v3 harmonized 0.2 candidate build review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Build run: `{payload['run_id']}`",
            "- Frozen target contract: `0.2`",
            "- Overall status: **PASS**",
            "- Technical blocking findings: `0`",
            f"- Static rows: `{metrics['static_rows']}`",
            f"- Dynamic rows: `{metrics['dynamic_rows']}`",
            f"- Unit conversion rules applied: `{metrics['unit_conversion_rules']}`",
            f"- Semantic splits applied: `{metrics['semantic_split_rules']}`",
            f"- Negative medication values preserved: `{metrics['negative_medication_values_preserved']}`",
            "- Rows or stays filtered: `false`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Next gate",
            "",
            "Run the independent complete cell-level audit. This build alone does not authorize release promotion.",
            "",
    ]
    lines.extend(("## Unit conversion accounting", ""))
    for row in payload["unit_conversion_summaries"]:
        lines.append(
            f"- `{row['decision_id']}`: scope rows `{row['scope_rows']}`; "
            f"non-missing `{row['input_non_null']}` → `{row['output_non_null']}`; "
            f"zeros preserved `{row['finite_zero_preserved']}`."
        )
    lines.extend(("", "## Semantic-split accounting", ""))
    for row in payload["semantic_split_summaries"]:
        lines.append(
            f"- `{row['decision_id']}`: scope rows `{row['scope_rows']}`; "
            f"source non-missing `{row['source_non_null']}`; canonical values "
            f"routed `{row['canonical_values_masked']}`; target non-missing "
            f"`{row['parallel_target_non_null']}`."
        )
    lines.append("")
    return "\n".join(lines)


def run_harmonized_0_2_build(
    config: Harmonized02BuildConfig,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> Harmonized02BuildResult:
    policy = load_harmonized_0_2_build_policy(config.policy_path)
    if config.dataset_context != "production":
        raise HarmonizationError("Harmonized 0.2 build is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid harmonized 0.2 build run ID")
    decisions = load_reviewed_unit_decisions(policy.reviewed_decisions_path)
    medication = load_reviewed_medication_semantics(
        policy.medication_semantics_path
    )
    if (
        len(decisions.approved_harmonization_action_ids)
        != policy.expected["unit_conversion_rules"]
        or len(decisions.semantic_splits)
        != policy.expected["semantic_split_rules"]
        or medication.evidence["negative_value_count"]
        != policy.expected["current_negative_medication_values_preserved"]
    ):
        raise HarmonizationError("Reviewed 0.2 decisions changed")
    inputs = _load_inputs(config, policy)
    output_parent = config.data_root / policy.candidate_directory_name
    final = output_parent / selected_run
    staging = output_parent / f".{selected_run}.incomplete"
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (final, staging, review_json, review_md)):
        raise HarmonizationError("Harmonized 0.2 build run already exists")
    staging.mkdir(parents=True, mode=0o700)
    staging.chmod(0o700)
    conversion_counts = {
        identifier: Counter()
        for identifier in decisions.approved_harmonization_action_ids
    }
    split_counts = {
        str(row["decision_id"]): Counter() for row in decisions.semantic_splits
    }
    medication_counts: Counter[str] = Counter()
    row_counts: Counter[str] = Counter()
    output_files: dict[str, dict[str, Any]] = {}
    next_progress = {"static": 1_000_000, "dynamic": 1_000_000}
    source_hashes = {
        table: sha256_file(inputs.source_files[table])
        for table in ("static", "dynamic")
    }
    weight_hash = sha256_file(inputs.weight_static_path)
    for table in ("static", "dynamic"):
        source = pq.ParquetFile(inputs.source_files[table])
        target_path = staging / f"{table}.parquet"
        schema = inputs.target_schemas[table]
        with pq.ParquetWriter(
            target_path, schema, compression=policy.compression
        ) as writer:
            for batch in source.iter_batches(
                batch_size=policy.rows_per_batch, use_threads=True
            ):
                transformed = _transform_batch(
                    batch,
                    table,
                    schema,
                    policy,
                    decisions,
                    inputs.weights,
                    conversion_counts,
                    split_counts,
                    medication_counts,
                )
                writer.write_batch(transformed)
                row_counts[table] += batch.num_rows
                if progress is not None and row_counts[table] >= next_progress[table]:
                    progress(
                        f"harmonized_0_2_build_progress table={table} "
                        f"rows={row_counts[table]}"
                    )
                    next_progress[table] = (
                        (row_counts[table] // 1_000_000) + 1
                    ) * 1_000_000
        target_path.chmod(0o600)
        if row_counts[table] != policy.expected[f"{table}_rows"]:
            raise HarmonizationError("Harmonized 0.2 row conservation failed")
        if sha256_file(inputs.source_files[table]) != source_hashes[table]:
            raise HarmonizationError("Harmonized 0.1 input changed during build")
        output_files[table] = {
            "path": str(final / target_path.name),
            "sha256": sha256_file(target_path),
            "row_count": row_counts[table],
            "schema_sha256": _schema_digest(schema),
        }
        if progress is not None:
            progress(f"harmonized_0_2_build_table_complete={table}")
    if sha256_file(inputs.weight_static_path) != weight_hash:
        raise HarmonizationError("Cleaned static weight reference changed during build")
    accounting = _accounting(
        conversion_counts,
        split_counts,
        medication_counts,
        inputs.weight_accounting,
    )
    _validate_accounting(policy, accounting)
    conversion_summaries, split_summaries = _sanitized_rule_summaries(
        decisions, accounting
    )
    generated = utc_timestamp()
    manifest_path = staging / "harmonized_0_2_build_manifest.json"
    manifest = {
        "artifact": "asic_v3_harmonized_0_2_candidate",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "status": policy.output_status,
        "lineage": {
            "source_harmonized_release_id": policy.source_release_id,
            "source_harmonized_manifest_sha256": sha256_file(
                inputs.source_manifest_path
            ),
            "static_weight_reference_cleaned_release_id": policy.weight_release_id,
            "static_weight_reference_manifest_sha256": sha256_file(
                inputs.weight_manifest_path
            ),
            "target_contract_version": policy.target_contract_version,
            "target_contract_manifest_sha256": sha256_file(
                inputs.target_manifest_path
            ),
            "build_policy_sha256": sha256_file(policy.source_path),
            "reviewed_unit_decisions_sha256": policy.reviewed_decisions_sha256,
            "reviewed_medication_semantics_sha256": policy.medication_semantics_sha256,
        },
        "source_file_sha256": source_hashes,
        "static_weight_reference_sha256": weight_hash,
        "outputs": output_files,
        "row_counts": dict(row_counts),
        "transformation_accounting": accounting,
        "row_filtering_applied": False,
        "stay_filtering_applied": False,
        "input_columns_dropped": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    _write_json(manifest_path, manifest, 0o600)
    staging.replace(final)
    final.chmod(0o700)
    manifest_path = final / manifest_path.name
    metrics = {
        "static_rows": row_counts["static"],
        "dynamic_rows": row_counts["dynamic"],
        "unit_conversion_rules": len(conversion_counts),
        "semantic_split_rules": len(split_counts),
        "negative_medication_values_preserved": medication_counts[
            "output_finite_negative"
        ],
    }
    review = {
        "artifact": "asic_v3_harmonized_0_2_build_review",
        "artifact_version": policy.artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "overall_status": "pass",
        "technical_blocking_findings": [],
        "metrics": metrics,
        "unit_conversion_summaries": conversion_summaries,
        "semantic_split_summaries": split_summaries,
        "candidate_artifacts_generated": True,
        "source_releases_modified": False,
        "rows_or_stays_filtered": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
    }
    assert_review_payload_is_safe(review)
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review, 0o640)
    _write_text(review_md, _build_markdown(review), 0o640)
    return Harmonized02BuildResult(
        run_id=selected_run,
        candidate_directory=final,
        manifest_path=manifest_path,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )


def _audit_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    lines = [
            "# ASIC v3 harmonized 0.2 candidate audit review",
            "",
            f"- Generated (UTC): `{payload['generated_at_utc']}`",
            f"- Dataset context: `{payload['dataset_context']}`",
            f"- Candidate: `{payload['build_run_id']}`",
            "- Technical audit status: **PASS**",
            "- Overall status: **PENDING HUMAN REVIEW**",
            "- Technical blocking findings: `0`",
            "- Human blocking findings: `1`",
            f"- Compared static rows: `{metrics['compared_static_rows']}`",
            f"- Compared dynamic rows: `{metrics['compared_dynamic_rows']}`",
            f"- Compared output cells: `{metrics['compared_output_cells']}`",
            f"- Unit conversions recomputed: `{metrics['unit_conversion_rules']}`",
            f"- Semantic splits recomputed: `{metrics['semantic_split_rules']}`",
            f"- Negative medication values preserved: `{metrics['negative_medication_values_preserved']}`",
            f"- Missing/invalid UK00 weight links: `{metrics['missing_or_invalid_weight_links']}`",
            "- Every input column preserved: `true`",
            "- Rows or stays filtered: `false`",
            "- Cleaning applied: `false`",
            "- Derivation applied: `false`",
            "- Publication ready: `false`",
            "",
            "## Human review gate",
            "",
            "Review the complete aggregate conversion, split, medication, weight-linkage, schema, and cell-level accounting before approving immutable harmonized 0.2 release promotion.",
            "",
    ]
    lines.extend(("## Independently recomputed unit conversions", ""))
    for row in payload["unit_conversion_summaries"]:
        lines.append(
            f"- `{row['decision_id']}`: scope rows `{row['scope_rows']}`; "
            f"non-missing `{row['input_non_null']}` → `{row['output_non_null']}`; "
            f"zeros preserved `{row['finite_zero_preserved']}`; missing/invalid "
            f"weight links `{row['missing_or_invalid_weight']}`."
        )
    lines.extend(("", "## Independently recomputed semantic splits", ""))
    for row in payload["semantic_split_summaries"]:
        lines.append(
            f"- `{row['decision_id']}`: source non-missing "
            f"`{row['source_non_null']}`; canonical values routed "
            f"`{row['canonical_values_masked']}`; target non-missing "
            f"`{row['parallel_target_non_null']}`; zeros preserved "
            f"`{row['finite_zero_preserved']}`."
        )
    lines.append("")
    return "\n".join(lines)


def run_harmonized_0_2_audit(
    config: Harmonized02AuditConfig,
    build_run_id: str,
    run_id: str | None = None,
    progress: Callable[[str], None] | None = None,
) -> Harmonized02AuditResult:
    audit_policy = load_harmonized_0_2_audit_policy(config.policy_path)
    build_policy = load_harmonized_0_2_build_policy(
        audit_policy.build_policy_path
    )
    if config.build.policy_path.resolve() != audit_policy.build_policy_path.resolve():
        raise HarmonizationError("Harmonized 0.2 build/audit policies disagree")
    if config.dataset_context != "production":
        raise HarmonizationError("Harmonized 0.2 audit is production-only")
    selected_run = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected_run) or not RUN_ID_PATTERN.fullmatch(
        build_run_id
    ):
        raise HarmonizationError("Invalid harmonized 0.2 audit run ID")
    decisions = load_reviewed_unit_decisions(build_policy.reviewed_decisions_path)
    inputs = _load_inputs(config.build, build_policy)
    candidate = (
        config.data_root / build_policy.candidate_directory_name / build_run_id
    )
    manifest_path = candidate / "harmonized_0_2_build_manifest.json"
    manifest = _read_json(manifest_path, "Harmonized 0.2 candidate manifest")
    if not isinstance(manifest, dict) or (
        manifest.get("artifact") != "asic_v3_harmonized_0_2_candidate"
        or manifest.get("artifact_version") != build_policy.artifact_version
        or manifest.get("run_id") != build_run_id
        or manifest.get("status") != build_policy.output_status
    ):
        raise HarmonizationError("Harmonized 0.2 candidate manifest changed")
    lineage = manifest.get("lineage")
    if not isinstance(lineage, dict) or (
        lineage.get("source_harmonized_release_id")
        != build_policy.source_release_id
        or
        lineage.get("source_harmonized_manifest_sha256")
        != sha256_file(inputs.source_manifest_path)
        or lineage.get("static_weight_reference_cleaned_release_id")
        != build_policy.weight_release_id
        or lineage.get("static_weight_reference_manifest_sha256")
        != sha256_file(inputs.weight_manifest_path)
        or lineage.get("target_contract_version")
        != build_policy.target_contract_version
        or lineage.get("target_contract_manifest_sha256")
        != sha256_file(inputs.target_manifest_path)
        or lineage.get("build_policy_sha256") != sha256_file(build_policy.source_path)
        or lineage.get("reviewed_unit_decisions_sha256")
        != build_policy.reviewed_decisions_sha256
        or lineage.get("reviewed_medication_semantics_sha256")
        != build_policy.medication_semantics_sha256
        or manifest.get("row_filtering_applied") is not False
        or manifest.get("stay_filtering_applied") is not False
        or manifest.get("input_columns_dropped") is not False
        or manifest.get("cleaning_applied") is not False
        or manifest.get("derivation_applied") is not False
        or manifest.get("publication_ready") is not False
        or manifest.get("external_data_export_authorized") is not False
    ):
        raise HarmonizationError("Harmonized 0.2 candidate lineage changed")
    private_dir = (
        config.reports_root
        / "private"
        / audit_policy.private_directory_name
        / selected_run
    )
    review_dir = config.reports_root / "review" / audit_policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if any(path.exists() for path in (private_dir, review_json, review_md)):
        raise HarmonizationError("Harmonized 0.2 audit run already exists")
    conversion_counts = {
        identifier: Counter()
        for identifier in decisions.approved_harmonization_action_ids
    }
    split_counts = {
        str(row["decision_id"]): Counter() for row in decisions.semantic_splits
    }
    medication_counts: Counter[str] = Counter()
    compared_rows: Counter[str] = Counter()
    compared_cells = 0
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise HarmonizationError("Harmonized 0.2 candidate outputs are absent")
    source_hashes = {
        table: sha256_file(inputs.source_files[table])
        for table in ("static", "dynamic")
    }
    weight_hash = sha256_file(inputs.weight_static_path)
    if (
        manifest.get("source_file_sha256") != source_hashes
        or manifest.get("static_weight_reference_sha256") != weight_hash
    ):
        raise HarmonizationError("Harmonized 0.2 input hashes changed")
    for table in ("static", "dynamic"):
        output_path = candidate / f"{table}.parquet"
        summary = outputs.get(table)
        output_parquet = pq.ParquetFile(output_path)
        if not isinstance(summary, dict) or (
            summary.get("sha256") != sha256_file(output_path)
            or summary.get("row_count") != build_policy.expected[f"{table}_rows"]
            or summary.get("schema_sha256")
            != _schema_digest(inputs.target_schemas[table])
            or not _parquet_schema_matches(
                output_parquet.schema_arrow, inputs.target_schemas[table]
            )
        ):
            raise HarmonizationError("Harmonized 0.2 candidate output changed")
        input_parquet = pq.ParquetFile(inputs.source_files[table])
        output_iterator = output_parquet.iter_batches(
            batch_size=audit_policy.rows_per_batch, use_threads=True
        )
        output_state: list[Any] = [None, 0]
        for input_batch in input_parquet.iter_batches(
            batch_size=audit_policy.rows_per_batch, use_threads=True
        ):
            observed = _next_slice(
                output_iterator, output_state, input_batch.num_rows
            )
            expected = _audit_expected_batch(
                input_batch,
                table,
                inputs.target_schemas[table],
                decisions,
                inputs.weights,
                conversion_counts,
                split_counts,
                medication_counts,
            )
            for field in inputs.target_schemas[table]:
                index = observed.schema.get_field_index(field.name)
                if not expected.column(index).equals(observed.column(index)):
                    raise HarmonizationError(
                        f"Harmonized 0.2 value mismatch: {table}.{field.name}"
                    )
                compared_cells += input_batch.num_rows
            compared_rows[table] += input_batch.num_rows
            if (
                progress is not None
                and compared_rows[table] % 1_000_000 < input_batch.num_rows
            ):
                progress(
                    f"harmonized_0_2_audit_progress table={table} "
                    f"rows={compared_rows[table]}"
                )
        _require_output_exhausted(output_iterator, output_state)
        if compared_rows[table] != build_policy.expected[f"{table}_rows"]:
            raise HarmonizationError("Harmonized 0.2 audit row count changed")
        if sha256_file(inputs.source_files[table]) != source_hashes[table]:
            raise HarmonizationError("Source release changed during 0.2 audit")
        if progress is not None:
            progress(f"harmonized_0_2_audit_table_complete={table}")
    if sha256_file(inputs.weight_static_path) != weight_hash:
        raise HarmonizationError("Static weight reference changed during audit")
    accounting = _accounting(
        conversion_counts,
        split_counts,
        medication_counts,
        inputs.weight_accounting,
    )
    _validate_accounting(build_policy, accounting)
    if manifest.get("transformation_accounting") != accounting:
        raise HarmonizationError("Build and independent audit accounting differ")
    conversion_summaries, split_summaries = _sanitized_rule_summaries(
        decisions, accounting
    )
    missing_weight = sum(
        row.get("missing_stay_id", 0) + row.get("missing_or_invalid_weight", 0)
        for row in accounting["unit_conversions"].values()
    )
    metrics = {
        "compared_static_rows": compared_rows["static"],
        "compared_dynamic_rows": compared_rows["dynamic"],
        "compared_output_cells": compared_cells,
        "unit_conversion_rules": len(conversion_counts),
        "semantic_split_rules": len(split_counts),
        "negative_medication_values_preserved": medication_counts[
            "output_finite_negative"
        ],
        "missing_or_invalid_weight_links": missing_weight,
        "provenance_fields_per_table": build_policy.expected[
            "provenance_fields_per_table"
        ],
    }
    generated = utc_timestamp()
    blockers = (
        {
            "check": "harmonized_0_2_candidate_release_approved",
            "details": "The completely audited harmonized 0.2 candidate requires explicit data-owner approval before immutable release promotion.",
        },
    )
    private_payload = {
        "artifact": "asic_v3_harmonized_0_2_audit_private",
        "artifact_version": audit_policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "build_run_id": build_run_id,
        "build_manifest_sha256": sha256_file(manifest_path),
        "metrics": metrics,
        "unit_conversion_summaries": conversion_summaries,
        "semantic_split_summaries": split_summaries,
        "transformation_accounting": accounting,
        "source_data_modified": False,
        "candidate_data_modified": False,
        "publication_ready": False,
    }
    review = {
        "artifact": "asic_v3_harmonized_0_2_audit_review",
        "artifact_version": audit_policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "build_run_id": build_run_id,
        "overall_status": "pending_human_review",
        "technical_status": "pass",
        "blocking_findings": list(blockers),
        "technical_blocking_findings": [],
        "metrics": metrics,
        "unit_conversion_summaries": conversion_summaries,
        "semantic_split_summaries": split_summaries,
        "every_input_column_preserved": True,
        "every_output_cell_recomputed": True,
        "source_data_modified": False,
        "candidate_data_modified": False,
        "rows_or_stays_filtered": False,
        "cleaning_applied": False,
        "derivation_applied": False,
        "publication_ready": False,
        "external_data_export_authorized": False,
    }
    assert_review_payload_is_safe(review)
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    _write_json(
        private_dir / "harmonized_0_2_audit_manifest.json",
        private_payload,
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review, 0o640)
    _write_text(review_md, _audit_markdown(review), 0o640)
    return Harmonized02AuditResult(
        run_id=selected_run,
        build_run_id=build_run_id,
        overall_status="pending_human_review",
        blocking_findings=blockers,
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
