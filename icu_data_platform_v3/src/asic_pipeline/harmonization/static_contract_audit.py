from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.audit.report import CheckResult, overall_status, utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.parsing import parse_direct_numeric
from asic_pipeline.harmonization.review_policy import (
    HarmonizationReviewConfig,
    load_harmonization_review_config,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


ALLOWED_EVIDENCE_MODES = frozenset({"identifier", "numeric", "binary", "free_text"})
APPROVED_STATUSES = frozenset({"human_approved_raw_v3", "not_applicable"})
TECHNICAL_CHECKS = frozenset(
    {
        "registry_and_prior_audit_evidence_valid",
        "selected_static_batch_complete",
        "ingested_static_hashes_match_manifests",
        "selected_source_bindings_valid",
        "static_scan_accounting_conserved",
        "prior_identifier_audit_passed",
        "no_production_data_artifacts_generated",
    }
)
CHECK_DETAILS = {
    "registry_and_prior_audit_evidence_valid": (
        "The immutable registry review and reviewed composite audit must match the approved evidence basis."
    ),
    "selected_static_batch_complete": (
        "Every selected canonical static variable must occur exactly once in the review batch."
    ),
    "ingested_static_hashes_match_manifests": (
        "Every scanned static Parquet file must match its immutable ingestion-manifest hash."
    ),
    "selected_source_bindings_valid": (
        "Each reviewed source occurrence must match an ingested physical field and its raw-column metadata."
    ),
    "static_scan_accounting_conserved": (
        "Null, literal-empty, and non-empty scan counts must conserve every row and reproduce schema/token evidence."
    ),
    "prior_identifier_audit_passed": (
        "The immutable post-ingestion audit must have passed its identifier-agreement check."
    ),
    "selected_static_mappings_approved": (
        "Every selected raw occurrence-to-canonical mapping requires explicit human approval."
    ),
    "selected_static_value_types_approved": (
        "Every selected canonical physical value type requires explicit human approval."
    ),
    "selected_static_units_approved": (
        "Every applicable selected canonical unit requires explicit human approval."
    ),
    "selected_static_parsing_and_missing_policies_approved": (
        "Numeric grammars, scoped missing sentinels, binary domains, and unresolved non-empty tokens require explicit review."
    ),
    "selected_static_identifier_contract_approved": (
        "The stay identifier construction and cross-hospital uniqueness policy require explicit human approval."
    ),
    "selected_static_free_text_policy_approved": (
        "The ICD-10 source representation and preservation policy require explicit human approval."
    ),
    "no_production_data_artifacts_generated": (
        "This audit may write review reports only and must not create harmonized data."
    ),
}


@dataclass(frozen=True)
class StaticVariableCandidate:
    target: str
    evidence_mode: str
    value_type: str
    unit: str


@dataclass(frozen=True)
class StaticContractAuditPolicy:
    version: str
    registry_review_basis_run_id: str
    composite_audit_basis_run_id: str
    required_registry_review_artifact_version: str
    required_ingestion_manifest_version: str
    required_ingestion_audit_artifact_version: str
    required_composite_audit_artifact_version: str
    variables: tuple[StaticVariableCandidate, ...]
    rows_per_batch: int
    maximum_private_token_examples_per_occurrence: int
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class StaticContractAuditConfig:
    harmonization_review: HarmonizationReviewConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.harmonization_review.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.harmonization_review.reports_root

    @property
    def ingested_root(self) -> Path:
        return self.harmonization_review.schema_tokens.ingestion.output_root


@dataclass(frozen=True)
class StaticContractAuditResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_occurrences: tuple[dict[str, Any], ...]
    private_variables: tuple[dict[str, Any], ...]
    private_token_examples: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


@dataclass
class _OccurrenceScan:
    row_count: int = 0
    source_null_count: int = 0
    literal_empty_count: int = 0
    nonempty_count: int = 0
    direct_numeric_count: int = 0
    approved_missing_sentinel_count: int = 0
    binary_false_count: int = 0
    binary_true_count: int = 0
    unresolved_nonempty_count: int = 0
    duplicate_identifier_row_count: int = 0
    numeric_minimum: float | None = None
    numeric_maximum: float | None = None
    distinct_nonempty_tokens: set[str] | None = None
    private_examples: Counter[str] | None = None

    @classmethod
    def create(cls, mode: str) -> _OccurrenceScan:
        return cls(
            distinct_nonempty_tokens=set(),
            private_examples=Counter() if mode != "identifier" else None,
        )

    def add_numeric(self, value: float) -> None:
        self.numeric_minimum = (
            value if self.numeric_minimum is None else min(self.numeric_minimum, value)
        )
        self.numeric_maximum = (
            value if self.numeric_maximum is None else max(self.numeric_maximum, value)
        )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _positive_int(mapping: dict[str, Any], key: str, location: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location}.{key} must be a positive integer")
    return value


def load_static_contract_audit_config(path: str | Path) -> StaticContractAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "Static-contract audit configuration")
    policy_path = resolve_path(
        required_string(raw, "static_contract_audit_policy", "config"), source
    )
    return StaticContractAuditConfig(harmonization, policy_path)


def load_static_contract_audit_policy(path: str | Path) -> StaticContractAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Static-contract audit policy")
    if raw.get("static_contract_audit_policy_version") != "0.1":
        raise ConfigurationError("Static-contract audit policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_static_contract_evidence":
        raise ConfigurationError("Static-contract audit policy is not approved")
    registry_run = required_string(raw, "registry_review_basis_run_id", "static audit")
    composite_run = required_string(raw, "composite_audit_basis_run_id", "static audit")
    if registry_run != "20260805T104417Z" or composite_run != "20260805T111026Z":
        raise ConfigurationError("Static-contract evidence basis changed unexpectedly")
    batch = raw.get("review_batch")
    if not isinstance(batch, list) or len(batch) != 8:
        raise ConfigurationError("Static-contract review batch must contain eight variables")
    variables: list[StaticVariableCandidate] = []
    for index, value in enumerate(batch):
        location = f"static audit.review_batch[{index}]"
        item = _mapping(value, location)
        mode = required_string(item, "evidence_mode", location)
        if mode not in ALLOWED_EVIDENCE_MODES:
            raise ConfigurationError(f"{location}.evidence_mode is invalid")
        variables.append(
            StaticVariableCandidate(
                target=required_string(item, "candidate_target", location),
                evidence_mode=mode,
                value_type=required_string(item, "candidate_value_type", location),
                unit=required_string(item, "candidate_unit", location),
            )
        )
    targets = [item.target for item in variables]
    if len(targets) != len(set(targets)):
        raise ConfigurationError("Static-contract review targets must be unique")
    expected_targets = {
        "stay_id_global",
        "weight_kg",
        "hosp_los",
        "icu_los",
        "dialysis_free_days",
        "vent_free_days",
        "hospital_mortality_reported",
        "icd10_codes",
    }
    if set(targets) != expected_targets:
        raise ConfigurationError("Static-contract review batch changed unexpectedly")
    scope = _mapping(raw.get("scope"), "static audit.scope")
    expected_scope = {
        "allow_registry_private_evidence_reads": True,
        "allow_ingested_static_column_reads": True,
        "allow_report_writes": True,
        "allow_ingested_data_mutation": False,
        "allow_harmonized_artifact_writes": False,
        "allow_hospital_concatenation": False,
        "allow_registry_approval": False,
        "allow_union_schema_approval": False,
    }
    if scope != expected_scope:
        raise ConfigurationError("Static-contract audit scope must remain fail-closed")
    scan = _mapping(raw.get("scan"), "static audit.scan")
    reporting = _mapping(raw.get("reporting"), "static audit.reporting")
    return StaticContractAuditPolicy(
        version="0.1",
        registry_review_basis_run_id=registry_run,
        composite_audit_basis_run_id=composite_run,
        required_registry_review_artifact_version=required_string(
            raw, "required_registry_review_artifact_version", "static audit"
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "static audit"
        ),
        required_ingestion_audit_artifact_version=required_string(
            raw, "required_ingestion_audit_artifact_version", "static audit"
        ),
        required_composite_audit_artifact_version=required_string(
            raw, "required_composite_audit_artifact_version", "static audit"
        ),
        variables=tuple(variables),
        rows_per_batch=_positive_int(scan, "rows_per_batch", "static audit.scan"),
        maximum_private_token_examples_per_occurrence=_positive_int(
            scan,
            "maximum_private_token_examples_per_occurrence",
            "static audit.scan",
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "static audit.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "static audit.reporting"
        ),
        source_path=source,
    )


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarmonizationError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise HarmonizationError(f"{label} must be a JSON object")
    return value


def _string_list(value: object, label: str) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise HarmonizationError(f"{label} contains invalid JSON") from exc
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise HarmonizationError(f"{label} must be a string list")
    return tuple(value)


def _consume_value(
    stats: _OccurrenceScan,
    raw: object,
    mode: str,
    approved_sentinels: frozenset[str],
) -> None:
    stats.row_count += 1
    if raw is None:
        stats.source_null_count += 1
        return
    if not isinstance(raw, str):
        stats.nonempty_count += 1
        stats.unresolved_nonempty_count += 1
        if stats.private_examples is not None:
            stats.private_examples[repr(raw)] += 1
        return
    if raw == "":
        stats.literal_empty_count += 1
        return
    stats.nonempty_count += 1
    assert stats.distinct_nonempty_tokens is not None
    already_seen = raw in stats.distinct_nonempty_tokens
    stats.distinct_nonempty_tokens.add(raw)
    if mode == "identifier":
        stats.duplicate_identifier_row_count += int(already_seen)
        return
    assert stats.private_examples is not None
    if mode == "free_text":
        stats.private_examples[raw] += 1
        return
    if raw in approved_sentinels:
        stats.approved_missing_sentinel_count += 1
        return
    numeric = parse_direct_numeric(raw)
    if mode == "binary":
        stats.private_examples[raw] += 1
        if numeric == 0.0:
            stats.binary_false_count += 1
        elif numeric == 1.0:
            stats.binary_true_count += 1
        else:
            stats.unresolved_nonempty_count += 1
        return
    if numeric is None:
        stats.unresolved_nonempty_count += 1
        stats.private_examples[raw] += 1
        return
    stats.direct_numeric_count += 1
    stats.add_numeric(numeric)


def _check_pass(payload: dict[str, Any], name: str) -> bool:
    checks = payload.get("checks")
    if not isinstance(checks, list):
        return False
    return any(
        isinstance(item, dict)
        and item.get("name") == name
        and item.get("status") == "pass"
        for item in checks
    )


def build_static_contract_audit(
    config: StaticContractAuditConfig,
    registry_review_run_id: str,
    composite_audit_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> StaticContractAuditResult:
    policy = load_static_contract_audit_policy(config.policy_path)
    if (
        registry_review_run_id != policy.registry_review_basis_run_id
        or composite_audit_run_id != policy.composite_audit_basis_run_id
        or not RUN_ID_PATTERN.fullmatch(registry_review_run_id)
        or not RUN_ID_PATTERN.fullmatch(composite_audit_run_id)
    ):
        raise HarmonizationError("Static-contract audit requires the approved review runs")

    registry_private = (
        config.reports_root
        / "private"
        / "harmonization_registry_review"
        / registry_review_run_id
    )
    registry_manifest_path = (
        registry_private / "harmonization_registry_review_manifest.json"
    )
    occurrences_path = registry_private / "occurrences.parquet"
    variables_path = registry_private / "variables.parquet"
    registry_review_path = (
        config.reports_root
        / "review"
        / "harmonization_registry_review"
        / f"{registry_review_run_id}.json"
    )
    composite_review_path = (
        config.reports_root
        / "review"
        / "composite_source_audit"
        / f"{composite_audit_run_id}.json"
    )
    required_paths = (
        registry_manifest_path,
        occurrences_path,
        variables_path,
        registry_review_path,
        composite_review_path,
    )
    if any(not path.is_file() for path in required_paths):
        raise HarmonizationError("Static-contract input evidence is incomplete")
    registry_manifest = _read_json(registry_manifest_path, "Registry manifest")
    registry_review = _read_json(registry_review_path, "Registry review")
    composite_review = _read_json(composite_review_path, "Composite audit review")
    if (
        registry_manifest.get("artifact")
        != "asic_v3_harmonization_registry_review_private"
        or registry_manifest.get("artifact_version")
        != policy.required_registry_review_artifact_version
        or registry_review.get("artifact")
        != "asic_v3_harmonization_registry_review"
        or registry_review.get("artifact_version")
        != policy.required_registry_review_artifact_version
        or registry_review.get("dataset_context") != config.dataset_context
        or registry_review.get("metrics", {}).get("technical_blocking_finding_count")
        != 0
    ):
        raise HarmonizationError("Registry-review evidence is incompatible")
    if (
        composite_review.get("artifact") != "asic_v3_composite_source_audit_review"
        or composite_review.get("artifact_version")
        != policy.required_composite_audit_artifact_version
        or composite_review.get("dataset_context") != config.dataset_context
        or composite_review.get("overall_status") != "pass"
        or composite_review.get("inputs", {}).get("registry_review_run_id")
        != registry_review_run_id
    ):
        raise HarmonizationError("Composite-source audit evidence is incompatible")
    occurrences = pq.read_table(occurrences_path).to_pylist()
    variables = pq.read_table(variables_path).to_pylist()
    if (
        len(occurrences) != registry_manifest.get("occurrence_review_row_count")
        or len(variables) != registry_manifest.get("variable_review_row_count")
    ):
        raise HarmonizationError("Registry-review row counts differ from its manifest")

    registry_inputs = registry_manifest.get("inputs")
    if not isinstance(registry_inputs, dict):
        raise HarmonizationError("Registry input provenance is invalid")
    schema_dir_raw = registry_inputs.get("schema_token_private_directory")
    if not isinstance(schema_dir_raw, str):
        raise HarmonizationError("Schema/token evidence path is unavailable")
    schema_dir = Path(schema_dir_raw).resolve()
    allowed_schema_root = (
        config.reports_root / "private" / "schema_token_inventory"
    ).resolve()
    if not schema_dir.is_relative_to(allowed_schema_root):
        raise HarmonizationError("Schema/token evidence escaped the v3 report root")
    schema_manifest_path = schema_dir / "schema_token_inventory_manifest.json"
    if not schema_manifest_path.is_file():
        raise HarmonizationError("Schema/token manifest is unavailable")
    schema_manifest = _read_json(schema_manifest_path, "Schema/token manifest")
    input_hashes = registry_inputs.get("hashes")
    if (
        not isinstance(input_hashes, dict)
        or input_hashes.get("schema_token_manifest_sha256")
        != sha256_file(schema_manifest_path)
    ):
        raise HarmonizationError("Schema/token manifest hash differs from registry evidence")
    schema_inputs = schema_manifest.get("inputs")
    if not isinstance(schema_inputs, dict):
        raise HarmonizationError("Schema/token input provenance is invalid")
    ingestion_audit_run_id = schema_inputs.get("ingestion_audit_run_id")
    if not isinstance(ingestion_audit_run_id, str) or not RUN_ID_PATTERN.fullmatch(
        ingestion_audit_run_id
    ):
        raise HarmonizationError("Ingestion-audit run ID is invalid")
    ingestion_audit_path = (
        config.reports_root
        / "review"
        / "ingestion_audit"
        / f"{ingestion_audit_run_id}.json"
    )
    if not ingestion_audit_path.is_file():
        raise HarmonizationError("Ingestion-audit review is unavailable")
    ingestion_audit = _read_json(ingestion_audit_path, "Ingestion audit review")
    identifier_audit_passed = bool(
        ingestion_audit.get("artifact") == "asic_v3_ingestion_audit_review"
        and ingestion_audit.get("artifact_version")
        == policy.required_ingestion_audit_artifact_version
        and ingestion_audit.get("dataset_context") == config.dataset_context
        and _check_pass(ingestion_audit, "identifier_agreement")
    )

    candidates = {item.target: item for item in policy.variables}
    selected_variables = [
        row
        for row in variables
        if row.get("table") == "static"
        and row.get("candidate_target") in candidates
    ]
    variable_by_target = {row.get("candidate_target"): row for row in selected_variables}
    selected_batch_complete = (
        len(selected_variables) == len(candidates)
        and set(variable_by_target) == set(candidates)
    )
    if not selected_batch_complete:
        raise HarmonizationError("Selected static review variables are incomplete")
    for target, candidate in candidates.items():
        row = variable_by_target[target]
        if (
            row.get("candidate_value_type") != candidate.value_type
            or row.get("candidate_unit") != candidate.unit
        ):
            raise HarmonizationError(
                f"Candidate representation changed for selected target {target}"
            )
    selected_occurrences = [
        row
        for row in occurrences
        if row.get("table") == "static"
        and row.get("candidate_target") in candidates
    ]
    if not selected_occurrences:
        raise HarmonizationError("Selected static occurrences are unavailable")
    by_hospital: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in selected_occurrences:
        hospital = row.get("hospital")
        if not isinstance(hospital, str):
            raise HarmonizationError("Selected occurrence hospital is invalid")
        by_hospital[hospital].append(row)

    scans: dict[str, _OccurrenceScan] = {}
    static_hashes_match = True
    bindings_valid = True
    accounting_valid = True
    identifier_hospitals_by_token: dict[str, set[str]] = defaultdict(set)
    input_static_hashes: dict[str, dict[str, str]] = {}
    hospital_row_counts: dict[str, int] = {}
    for hospital, rows in sorted(by_hospital.items()):
        hospital_dir = config.ingested_root / hospital
        static_path = hospital_dir / "static.parquet"
        ingestion_manifest_path = hospital_dir / "ingestion_manifest.json"
        if not static_path.is_file() or not ingestion_manifest_path.is_file():
            raise HarmonizationError(f"Ingested static evidence is unavailable for {hospital}")
        ingestion_manifest = _read_json(
            ingestion_manifest_path, f"Ingestion manifest for {hospital}"
        )
        expected_hash = (
            ingestion_manifest.get("outputs", {}).get("static", {}).get("sha256")
        )
        if (
            ingestion_manifest.get("artifact")
            != "asic_v3_lossless_hospital_ingestion"
            or ingestion_manifest.get("artifact_version")
            != policy.required_ingestion_manifest_version
            or ingestion_manifest.get("hospital", {}).get("canonical_hospital_id")
            != hospital
            or not isinstance(expected_hash, str)
        ):
            raise HarmonizationError(f"Ingestion manifest is incompatible for {hospital}")
        observed_hash = sha256_file(static_path)
        static_hashes_match &= observed_hash == expected_hash
        input_static_hashes[hospital] = {
            "ingestion_manifest_sha256": sha256_file(ingestion_manifest_path),
            "static_parquet_sha256": observed_hash,
        }
        parquet = pq.ParquetFile(static_path)
        hospital_row_counts[hospital] = parquet.metadata.num_rows
        physical_names: list[str] = []
        for row in rows:
            review_id = row.get("review_item_id")
            physical_name = row.get("physical_name")
            raw_name = row.get("raw_name")
            raw_occurrence = row.get("raw_occurrence")
            if (
                not isinstance(review_id, str)
                or not isinstance(physical_name, str)
                or not isinstance(raw_name, str)
                or not isinstance(raw_occurrence, int)
            ):
                raise HarmonizationError("Selected source occurrence identity is invalid")
            field_index = parquet.schema_arrow.get_field_index(physical_name)
            if field_index < 0:
                bindings_valid = False
                continue
            field = parquet.schema_arrow.field(field_index)
            metadata = field.metadata or {}
            binding = bool(
                (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
                and metadata.get(b"raw_name", b"").decode("utf-8") == raw_name
                and int(metadata.get(b"raw_occurrence", b"0")) == raw_occurrence
            )
            bindings_valid &= binding
            physical_names.append(physical_name)
            scans[review_id] = _OccurrenceScan.create(
                candidates[str(row["candidate_target"])].evidence_mode
            )
        if not bindings_valid:
            raise HarmonizationError("Selected source binding differs from review evidence")
        if len(physical_names) != len(set(physical_names)):
            raise HarmonizationError("Selected static physical fields are not unique")
        rows_by_physical = {str(row["physical_name"]): row for row in rows}
        for batch in parquet.iter_batches(
            batch_size=policy.rows_per_batch,
            columns=physical_names,
            use_threads=False,
        ):
            for index, physical_name in enumerate(physical_names):
                row = rows_by_physical[physical_name]
                review_id = str(row["review_item_id"])
                candidate = candidates[str(row["candidate_target"])]
                sentinels = frozenset(
                    _string_list(
                        row.get("approved_missing_sentinel_tokens"),
                        f"{review_id} approved missing sentinels",
                    )
                )
                stats = scans[review_id]
                for raw in batch.column(index).to_pylist():
                    _consume_value(stats, raw, candidate.evidence_mode, sentinels)
        for row in rows:
            review_id = str(row["review_item_id"])
            stats = scans[review_id]
            conserved = (
                stats.row_count
                == stats.source_null_count
                + stats.literal_empty_count
                + stats.nonempty_count
                and stats.row_count == row.get("expected_row_count")
                and stats.literal_empty_count == row.get("literal_empty_count")
                and stats.nonempty_count == row.get("raw_nonempty_count")
            )
            accounting_valid &= conserved
            if candidates[str(row["candidate_target"])].evidence_mode == "identifier":
                assert stats.distinct_nonempty_tokens is not None
                for token in stats.distinct_nonempty_tokens:
                    identifier_hospitals_by_token[token].add(hospital)
        if progress is not None:
            progress(
                f"static_contract_hospital_scanned={hospital} rows={parquet.metadata.num_rows}"
            )

    cross_hospital_identifier_overlap_count = sum(
        len(hospitals) > 1 for hospitals in identifier_hospitals_by_token.values()
    )
    private_occurrences: list[dict[str, Any]] = []
    private_token_examples: list[dict[str, Any]] = []
    for row in selected_occurrences:
        review_id = str(row["review_item_id"])
        stats = scans[review_id]
        candidate = candidates[str(row["candidate_target"])]
        examples = stats.private_examples or Counter()
        selected_examples = examples.most_common(
            policy.maximum_private_token_examples_per_occurrence
        )
        for raw_token, count in selected_examples:
            private_token_examples.append(
                {
                    "review_item_id": review_id,
                    "hospital": row["hospital"],
                    "candidate_target": candidate.target,
                    "evidence_mode": candidate.evidence_mode,
                    "raw_token": raw_token,
                    "count": count,
                }
            )
        private_occurrences.append(
            {
                **row,
                "static_contract_evidence_mode": candidate.evidence_mode,
                "scan_row_count": stats.row_count,
                "scan_source_null_count": stats.source_null_count,
                "scan_literal_empty_count": stats.literal_empty_count,
                "scan_nonempty_count": stats.nonempty_count,
                "scan_direct_numeric_count": stats.direct_numeric_count,
                "scan_approved_missing_sentinel_count": (
                    stats.approved_missing_sentinel_count
                ),
                "scan_binary_false_count": stats.binary_false_count,
                "scan_binary_true_count": stats.binary_true_count,
                "scan_unresolved_nonempty_count": stats.unresolved_nonempty_count,
                "scan_duplicate_identifier_row_count": (
                    stats.duplicate_identifier_row_count
                ),
                "scan_distinct_nonempty_token_count": len(
                    stats.distinct_nonempty_tokens or ()
                ),
                "scan_numeric_minimum": stats.numeric_minimum,
                "scan_numeric_maximum": stats.numeric_maximum,
                "private_token_example_count": len(selected_examples),
                "private_token_examples_truncated": len(examples) > len(selected_examples),
            }
        )

    private_variables: list[dict[str, Any]] = []
    sanitized_variables: list[dict[str, Any]] = []
    mapping_pending = 0
    type_pending = 0
    unit_pending = 0
    parser_pending = 0
    for batch_index, candidate in enumerate(policy.variables, start=1):
        source = variable_by_target[candidate.target]
        target_rows = [
            row for row in private_occurrences if row["candidate_target"] == candidate.target
        ]
        pending_mapping_count = sum(
            not str(row.get("mapping_review_status", "")).startswith(
                "human_approved"
            )
            for row in target_rows
        )
        value_statuses = _string_list(
            source.get("value_type_review_statuses"),
            f"{candidate.target} value type statuses",
        )
        unit_statuses = _string_list(
            source.get("unit_review_statuses"), f"{candidate.target} unit statuses"
        )
        pending_type = not set(value_statuses).issubset(APPROVED_STATUSES)
        pending_unit = not set(unit_statuses).issubset(APPROVED_STATUSES)
        pending_parser_count = sum(
            row.get("parser_review_status") != "human_approved_raw_v3"
            and candidate.evidence_mode in {"numeric", "binary"}
            for row in target_rows
        )
        mapping_pending += pending_mapping_count
        type_pending += int(pending_type)
        unit_pending += int(pending_unit)
        parser_pending += pending_parser_count
        summary = {
            "batch_item_id": f"S{batch_index:02d}",
            "variable_review_id": source.get("variable_review_id"),
            "candidate_target": candidate.target,
            "evidence_mode": candidate.evidence_mode,
            "candidate_value_type": candidate.value_type,
            "value_type_review_statuses": list(value_statuses),
            "candidate_unit": candidate.unit,
            "unit_review_statuses": list(unit_statuses),
            "hospital_count": len({row["hospital"] for row in target_rows}),
            "raw_occurrence_count": len(target_rows),
            "pending_mapping_occurrence_count": pending_mapping_count,
            "pending_parser_occurrence_count": pending_parser_count,
            "scan_row_count": sum(row["scan_row_count"] for row in target_rows),
            "source_null_count": sum(
                row["scan_source_null_count"] for row in target_rows
            ),
            "literal_empty_count": sum(
                row["scan_literal_empty_count"] for row in target_rows
            ),
            "nonempty_count": sum(row["scan_nonempty_count"] for row in target_rows),
            "direct_numeric_count": sum(
                row["scan_direct_numeric_count"] for row in target_rows
            ),
            "approved_missing_sentinel_count": sum(
                row["scan_approved_missing_sentinel_count"] for row in target_rows
            ),
            "binary_false_count": sum(
                row["scan_binary_false_count"] for row in target_rows
            ),
            "binary_true_count": sum(
                row["scan_binary_true_count"] for row in target_rows
            ),
            "unresolved_nonempty_count": sum(
                row["scan_unresolved_nonempty_count"] for row in target_rows
            ),
            "duplicate_identifier_row_count": sum(
                row["scan_duplicate_identifier_row_count"] for row in target_rows
            ),
            "private_token_examples_truncated_occurrence_count": sum(
                row["private_token_examples_truncated"] for row in target_rows
            ),
        }
        private_variables.append({**source, **summary})
        sanitized_variables.append(summary)

    failure_counts = {
        "registry_and_prior_audit_evidence_valid": 0,
        "selected_static_batch_complete": 0,
        "ingested_static_hashes_match_manifests": int(not static_hashes_match),
        "selected_source_bindings_valid": int(not bindings_valid),
        "static_scan_accounting_conserved": int(not accounting_valid),
        "prior_identifier_audit_passed": int(not identifier_audit_passed),
        "selected_static_mappings_approved": mapping_pending,
        "selected_static_value_types_approved": type_pending,
        "selected_static_units_approved": unit_pending,
        "selected_static_parsing_and_missing_policies_approved": parser_pending,
        "selected_static_identifier_contract_approved": 1,
        "selected_static_free_text_policy_approved": 1,
        "no_production_data_artifacts_generated": 0,
    }
    checks = [
        CheckResult(
            name=name,
            status="pass" if count == 0 else "fail",
            severity="blocking",
            observed=count,
            expected=0,
            details=CHECK_DETAILS[name],
        )
        for name, count in failure_counts.items()
    ]
    status = overall_status(checks)
    blockers = tuple(
        {"check": item.name, "details": item.details}
        for item in checks
        if item.status != "pass"
    )
    technical = tuple(
        item for item in blockers if item["check"] in TECHNICAL_CHECKS
    )
    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_static_contract_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "composite_audit_run_id": composite_audit_run_id,
            "ingestion_audit_run_id": ingestion_audit_run_id,
        },
        "metrics": {
            "selected_variable_count": len(policy.variables),
            "selected_occurrence_count": len(selected_occurrences),
            "scanned_hospital_count": len(by_hospital),
            "scanned_static_row_count": sum(hospital_row_counts.values()),
            "scanned_selected_cell_count": sum(
                value["scan_row_count"] for value in sanitized_variables
            ),
            "pending_mapping_occurrence_count": mapping_pending,
            "pending_value_type_variable_count": type_pending,
            "pending_unit_variable_count": unit_pending,
            "pending_parser_occurrence_count": parser_pending,
            "unresolved_nonempty_token_count": sum(
                value["unresolved_nonempty_count"] for value in sanitized_variables
            ),
            "cross_hospital_identifier_overlap_count": (
                cross_hospital_identifier_overlap_count
            ),
            "technical_blocking_finding_count": len(technical),
        },
        "variables": sanitized_variables,
        "checks": [asdict(item) for item in checks],
        "blocking_findings": list(blockers),
        "privacy": {
            "contains_source_filenames": False,
            "contains_stay_identifiers": False,
            "contains_raw_tokens": False,
            "contains_exact_raw_column_names": False,
        },
        "publication": {
            "harmonized_artifacts_generated": False,
            "cleaned_artifacts_generated": False,
            "source_data_modified": False,
            "registry_approved": False,
        },
        "limitations": [
            "The audit classifies selected static evidence but does not approve mappings, types, units, parsers, identifier construction, or free-text handling.",
            "Exact raw headers and bounded non-identifier token examples remain owner-only on the authorized cluster.",
            "Identifier values are never written to the audit bundle; only uniqueness and cross-hospital overlap counts are reported.",
            "No clinical value is transformed and no harmonized, cleaned, derived, concatenated, or pooled data artifact is generated.",
        ],
    }
    assert_review_payload_is_safe(review_payload)
    private_manifest = {
        "artifact": "asic_v3_static_contract_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "registry_review_manifest_sha256": sha256_file(registry_manifest_path),
            "registry_occurrences_sha256": sha256_file(occurrences_path),
            "registry_variables_sha256": sha256_file(variables_path),
            "registry_review_sha256": sha256_file(registry_review_path),
            "composite_audit_run_id": composite_audit_run_id,
            "composite_review_sha256": sha256_file(composite_review_path),
            "ingestion_audit_run_id": ingestion_audit_run_id,
            "ingestion_audit_review_sha256": sha256_file(ingestion_audit_path),
            "static_inputs": input_static_hashes,
        },
        "policy": {"version": policy.version, "path": str(policy.source_path)},
        "checks": [asdict(item) for item in checks],
        "selected_variable_count": len(private_variables),
        "selected_occurrence_count": len(private_occurrences),
        "private_token_example_count": len(private_token_examples),
        "identifier_values_persisted": False,
        "production_data_artifacts_generated": False,
    }
    return StaticContractAuditResult(
        overall_status=status,
        blocking_findings=blockers,
        technical_blocking_findings=technical,
        private_manifest=private_manifest,
        private_occurrences=tuple(private_occurrences),
        private_variables=tuple(private_variables),
        private_token_examples=tuple(private_token_examples),
        review_payload=review_payload,
    )


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


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ASIC v3 static-variable contract evidence review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input registry-review run: `{payload['inputs']['registry_review_run_id']}`",
        f"- Input composite-audit run: `{payload['inputs']['composite_audit_run_id']}`",
        "- Protected filenames, identifiers, raw tokens, and exact raw headers: excluded",
        "",
        "## Blocking findings",
        "",
    ]
    if payload["blocking_findings"]:
        lines.extend(
            f"- `{item['check']}`: {item['details']}"
            for item in payload["blocking_findings"]
        )
    else:
        lines.append("- None")
    lines.extend(["", "## Audit totals", ""])
    lines.extend(
        f"- {key}: `{value}`" for key, value in payload["metrics"].items()
    )
    lines.extend(["", "## Selected variables", ""])
    for item in payload["variables"]:
        lines.append(
            f"- `{item['batch_item_id']}` `{item['candidate_target']}`: "
            f"type `{item['candidate_value_type']}`; unit `{item['candidate_unit']}`; "
            f"hospitals `{item['hospital_count']}`; non-empty `{item['nonempty_count']}`; "
            f"unresolved `{item['unresolved_nonempty_count']}`"
        )
    lines.extend(["", "## Human review gate", ""])
    lines.append(
        "Review the owner-only occurrence, variable, and token-example workbooks before approving this static batch."
    )
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_static_contract_audit_bundle(
    result: StaticContractAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> StaticContractAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid static-contract audit run ID")
    private_dir = reports_root / "private" / "static_contract_audit" / selected
    review_dir = reports_root / "review" / "static_contract_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError(
            "Static-contract audit run already exists and will not be overwritten"
        )
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    review_dir.chmod(0o750)
    _write_json(
        private_dir / "static_contract_audit_manifest.json",
        result.private_manifest,
        0o600,
    )
    _write_private_parquet(private_dir / "occurrences.parquet", result.private_occurrences)
    _write_private_parquet(private_dir / "variables.parquet", result.private_variables)
    _write_private_parquet(
        private_dir / "token_examples.parquet", result.private_token_examples
    )
    readme = private_dir / "README.md"
    descriptor = os.open(readme, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(
            "# Owner-only static-variable contract evidence\n\n"
            "Keep this directory on the authorized cluster. Exact raw headers and "
            "bounded non-identifier token examples are private. Identifier values "
            "are not persisted. This bundle contains evidence, not approvals, and "
            "does not create harmonized data.\n"
        )
    readme.chmod(0o600)
    _write_json(review_json, result.review_payload, 0o640)
    temporary = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_markdown(result.review_payload))
        temporary.replace(review_md)
        review_md.chmod(0o640)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return StaticContractAuditResult(
        **{
            **asdict(result),
            "private_report_directory": private_dir,
            "review_json_path": review_json,
            "review_markdown_path": review_md,
        }
    )
