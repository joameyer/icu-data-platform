from __future__ import annotations

from collections import Counter
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
from asic_pipeline.inventory.report import RUN_ID_PATTERN, _write_private_parquet, default_run_id
from asic_pipeline.privacy import assert_review_payload_is_safe


PAIR_LABELS = tuple(
    f"{left}_{right}"
    for left in ("null", "false", "true")
    for right in ("null", "false", "true")
)
CHECK_DETAILS = {
    "registry_review_evidence_valid": "The immutable registry-review bundle must match the approved review basis.",
    "reviewed_retirements_all_missing": "Each approved retirement must retain zero non-empty values and a populated sibling target.",
    "ingested_dynamic_hash_matches_manifest": "The streamed UK00 dynamic Parquet must match its immutable ingestion manifest hash.",
    "composite_source_binding_valid": "The two reviewed source occurrences must match the ingested schema in physical source order.",
    "composite_binary_token_domain": "Every non-empty source token must be a finite numeric value exactly equal to zero or one.",
    "pair_pattern_accounting_conserved": "Every dynamic row must be counted once in one approved pair pattern or as unresolved.",
    "fixed_size_list_contract_valid": "Every resolved conceptual output must contain exactly two nullable Boolean positions without reduction.",
    "no_harmonized_artifacts_generated": "The audit may write reports only and must not write harmonized data.",
}


@dataclass(frozen=True)
class RetirementDecision:
    decision_id: str
    review_item_id: str
    retained_review_item_id: str
    hospital: str
    table: str
    target: str


@dataclass(frozen=True)
class CompositeListDecision:
    decision_id: str
    hospital: str
    table: str
    target: str
    source_review_item_ids: tuple[str, str]
    output_value_type: str
    output_list_size: int


@dataclass(frozen=True)
class CompositeSourceAuditPolicy:
    version: str
    review_basis_run_id: str
    required_registry_review_artifact_version: str
    required_ingestion_manifest_version: str
    retirements: tuple[RetirementDecision, ...]
    composite: CompositeListDecision
    rows_per_batch: int
    maximum_private_unresolved_examples: int
    private_artifact_version: str
    review_artifact_version: str
    source_path: Path


@dataclass(frozen=True)
class CompositeSourceAuditConfig:
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
class CompositeSourceAuditResult:
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_manifest: dict[str, Any]
    private_unresolved_tokens: tuple[dict[str, Any], ...]
    review_payload: dict[str, Any]
    private_report_directory: Path | None = None
    review_json_path: Path | None = None
    review_markdown_path: Path | None = None

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _required_positive_int(mapping: dict[str, Any], key: str, location: str) -> int:
    value = mapping.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{location}.{key} must be a positive integer")
    return value


def load_composite_source_audit_config(path: str | Path) -> CompositeSourceAuditConfig:
    source = Path(path).expanduser().resolve()
    harmonization = load_harmonization_review_config(source)
    raw = load_yaml_mapping(source, "Composite-source audit configuration")
    policy_path = resolve_path(
        required_string(raw, "composite_source_audit_policy", "config"), source
    )
    return CompositeSourceAuditConfig(harmonization, policy_path)


def load_composite_source_audit_policy(path: str | Path) -> CompositeSourceAuditPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Composite-source audit policy")
    if raw.get("composite_source_audit_policy_version") != "0.1":
        raise ConfigurationError("Composite-source audit policy version must be 0.1")
    if raw.get("status") != "approved_for_read_only_reviewed_composite_validation":
        raise ConfigurationError("Composite-source audit policy is not approved")
    review_basis = required_string(raw, "review_basis_run_id", "composite audit")
    if review_basis != "20260805T104417Z":
        raise ConfigurationError("Composite-source review basis changed unexpectedly")
    retirements_raw = raw.get("reviewed_all_missing_retirements")
    if not isinstance(retirements_raw, list) or len(retirements_raw) != 2:
        raise ConfigurationError("Exactly two reviewed retirements are required")
    retirements: list[RetirementDecision] = []
    for index, value in enumerate(retirements_raw):
        location = f"composite audit.reviewed_all_missing_retirements[{index}]"
        item = _mapping(value, location)
        if item.get("action") != "drop_after_all_missing" or item.get(
            "expected_raw_nonempty_count"
        ) != 0:
            raise ConfigurationError(f"{location} is not an all-missing retirement")
        retirements.append(
            RetirementDecision(
                decision_id=required_string(item, "decision_id", location),
                review_item_id=required_string(item, "review_item_id", location),
                retained_review_item_id=required_string(
                    item, "retained_review_item_id", location
                ),
                hospital=required_string(item, "hospital", location),
                table=required_string(item, "table", location),
                target=required_string(item, "candidate_target", location),
            )
        )
    composites = raw.get("reviewed_fixed_source_lists")
    if not isinstance(composites, list) or len(composites) != 1:
        raise ConfigurationError("Exactly one reviewed fixed-source list is required")
    item = _mapping(composites[0], "composite audit.reviewed_fixed_source_lists[0]")
    source_ids = item.get("source_review_item_ids")
    nulls = _mapping(item.get("source_null_handling"), "composite audit null handling")
    if (
        source_ids != ["R0144", "R0145"]
        or item.get("source_position_basis")
        != "lossless_ingestion_physical_column_order"
        or item.get("allowed_numeric_values") != [0, 1]
        or nulls != {"parquet_null": "null_element", "literal_empty": "null_element"}
        or item.get("preserve_conflicting_pairs") is not True
        or item.get("reduction_policy") != "none"
    ):
        raise ConfigurationError("Reviewed fixed-source list policy changed unexpectedly")
    composite = CompositeListDecision(
        decision_id=required_string(item, "decision_id", "composite list"),
        hospital=required_string(item, "hospital", "composite list"),
        table=required_string(item, "table", "composite list"),
        target=required_string(item, "candidate_target", "composite list"),
        source_review_item_ids=(source_ids[0], source_ids[1]),
        output_value_type=required_string(item, "output_value_type", "composite list"),
        output_list_size=_required_positive_int(item, "output_list_size", "composite list"),
    )
    if composite.output_value_type != "fixed_size_list_bool_2" or composite.output_list_size != 2:
        raise ConfigurationError("Composite output contract must remain a two-position Boolean list")
    scope = _mapping(raw.get("scope"), "composite audit.scope")
    if scope != {
        "allow_ingested_column_reads": True,
        "allow_report_writes": True,
        "allow_harmonized_artifact_writes": False,
        "allow_source_mutation": False,
        "allow_value_reduction": False,
        "allow_hospital_concatenation": False,
    }:
        raise ConfigurationError("Composite-source audit scope must remain read-only")
    scan = _mapping(raw.get("scan"), "composite audit.scan")
    reporting = _mapping(raw.get("reporting"), "composite audit.reporting")
    return CompositeSourceAuditPolicy(
        version="0.1",
        review_basis_run_id=review_basis,
        required_registry_review_artifact_version=required_string(
            raw, "required_registry_review_artifact_version", "composite audit"
        ),
        required_ingestion_manifest_version=required_string(
            raw, "required_ingestion_manifest_version", "composite audit"
        ),
        retirements=tuple(retirements),
        composite=composite,
        rows_per_batch=_required_positive_int(scan, "rows_per_batch", "composite audit.scan"),
        maximum_private_unresolved_examples=_required_positive_int(
            scan, "maximum_private_unresolved_token_examples", "composite audit.scan"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "composite audit.reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "composite audit.reporting"
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


def _parse_binary(raw: object) -> tuple[bool | None, str]:
    if raw is None:
        return None, "source_schema_absent"
    if not isinstance(raw, str):
        return None, "unresolved_non_string"
    if raw == "":
        return None, "literal_empty"
    numeric = parse_direct_numeric(raw)
    if numeric == 0.0:
        return False, "false"
    if numeric == 1.0:
        return True, "true"
    return None, "unresolved"


def _value_label(value: bool | None) -> str:
    if value is None:
        return "null"
    return "true" if value else "false"


def build_composite_source_audit(
    config: CompositeSourceAuditConfig,
    registry_review_run_id: str,
    progress: Callable[[str], None] | None = None,
) -> CompositeSourceAuditResult:
    policy = load_composite_source_audit_policy(config.policy_path)
    if registry_review_run_id != policy.review_basis_run_id or not RUN_ID_PATTERN.fullmatch(
        registry_review_run_id
    ):
        raise HarmonizationError("Composite audit requires the approved registry-review run")
    private_review = (
        config.reports_root
        / "private"
        / "harmonization_registry_review"
        / registry_review_run_id
    )
    review_manifest_path = private_review / "harmonization_registry_review_manifest.json"
    occurrences_path = private_review / "occurrences.parquet"
    review_json_path = (
        config.reports_root
        / "review"
        / "harmonization_registry_review"
        / f"{registry_review_run_id}.json"
    )
    if not all(path.is_file() for path in (review_manifest_path, occurrences_path, review_json_path)):
        raise HarmonizationError("Immutable registry-review evidence is incomplete")
    review_manifest = _read_json(review_manifest_path, "Registry-review manifest")
    review_payload = _read_json(review_json_path, "Registry-review report")
    if (
        review_manifest.get("artifact_version") != policy.required_registry_review_artifact_version
        or review_payload.get("artifact_version") != policy.required_registry_review_artifact_version
        or review_payload.get("metrics", {}).get("technical_blocking_finding_count") != 0
    ):
        raise HarmonizationError("Registry-review evidence is incompatible")
    occurrences = pq.read_table(occurrences_path).to_pylist()
    by_id = {row.get("review_item_id"): row for row in occurrences}
    retirement_failures = 0
    private_retirements: list[dict[str, Any]] = []
    for decision in policy.retirements:
        dropped = by_id.get(decision.review_item_id)
        retained = by_id.get(decision.retained_review_item_id)
        valid = bool(
            dropped
            and retained
            and dropped.get("hospital") == decision.hospital
            and dropped.get("table") == decision.table
            and dropped.get("candidate_target") == decision.target
            and dropped.get("raw_nonempty_count") == 0
            and dropped.get("all_missing_or_empty") is True
            and retained.get("hospital") == decision.hospital
            and retained.get("table") == decision.table
            and retained.get("candidate_target") == decision.target
            and isinstance(retained.get("raw_nonempty_count"), int)
            and retained["raw_nonempty_count"] > 0
        )
        retirement_failures += int(not valid)
        private_retirements.append(
            {
                "decision_id": decision.decision_id,
                "review_item_id": decision.review_item_id,
                "retained_review_item_id": decision.retained_review_item_id,
                "evidence_valid": valid,
            }
        )
    source_rows = [by_id.get(item) for item in policy.composite.source_review_item_ids]
    if any(row is None for row in source_rows):
        raise HarmonizationError("Reviewed composite source occurrence is unavailable")
    typed_source_rows = [row for row in source_rows if isinstance(row, dict)]
    if any(
        row.get("hospital") != policy.composite.hospital
        or row.get("table") != policy.composite.table
        or row.get("candidate_target") != policy.composite.target
        for row in typed_source_rows
    ):
        raise HarmonizationError("Reviewed composite source scope differs")
    ingested_dir = config.ingested_root / policy.composite.hospital
    dynamic_path = ingested_dir / "dynamic.parquet"
    ingestion_manifest_path = ingested_dir / "ingestion_manifest.json"
    if not dynamic_path.is_file() or not ingestion_manifest_path.is_file():
        raise HarmonizationError("Reviewed ingested dynamic artifact is unavailable")
    ingestion_manifest = _read_json(ingestion_manifest_path, "Ingestion manifest")
    expected_hash = ingestion_manifest.get("outputs", {}).get("dynamic", {}).get("sha256")
    if (
        ingestion_manifest.get("artifact") != "asic_v3_lossless_hospital_ingestion"
        or ingestion_manifest.get("artifact_version") != policy.required_ingestion_manifest_version
        or ingestion_manifest.get("hospital", {}).get("canonical_hospital_id")
        != policy.composite.hospital
        or not isinstance(expected_hash, str)
    ):
        raise HarmonizationError("Ingestion manifest is incompatible")
    observed_hash = sha256_file(dynamic_path)
    hash_matches = observed_hash == expected_hash
    parquet = pq.ParquetFile(dynamic_path)
    physical_names = [str(row["physical_name"]) for row in typed_source_rows]
    indices = [parquet.schema_arrow.get_field_index(name) for name in physical_names]
    binding_valid = (
        len(set(physical_names)) == 2
        and all(index >= 0 for index in indices)
        and indices == sorted(indices)
    )
    if not binding_valid:
        raise HarmonizationError("Composite sources do not match physical source order")
    for row, name in zip(typed_source_rows, physical_names, strict=True):
        field = parquet.schema_arrow.field(name)
        metadata = field.metadata or {}
        if (
            not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
            or metadata.get(b"raw_name", b"").decode("utf-8") != row["raw_name"]
            or int(metadata.get(b"raw_occurrence", b"0")) != row["raw_occurrence"]
        ):
            raise HarmonizationError("Composite source field metadata differs from review evidence")
    source_counts = [Counter(), Counter()]
    pair_counts: Counter[str] = Counter()
    unresolved_rows = 0
    unresolved_tokens: Counter[tuple[int, str]] = Counter()
    scanned_rows = 0
    for batch in parquet.iter_batches(
        batch_size=policy.rows_per_batch,
        columns=physical_names,
        use_threads=False,
    ):
        columns = [batch.column(index).to_pylist() for index in range(2)]
        for raw_left, raw_right in zip(columns[0], columns[1], strict=True):
            values: list[bool | None] = []
            statuses: list[str] = []
            for position, raw_value in enumerate((raw_left, raw_right)):
                parsed, parse_status = _parse_binary(raw_value)
                values.append(parsed)
                statuses.append(parse_status)
                source_counts[position][parse_status] += 1
                if parse_status.startswith("unresolved"):
                    unresolved_tokens[(position, str(raw_value))] += 1
            if any(status.startswith("unresolved") for status in statuses):
                unresolved_rows += 1
            else:
                pair_counts[
                    f"{_value_label(values[0])}_{_value_label(values[1])}"
                ] += 1
            scanned_rows += 1
        if progress is not None:
            progress(f"composite_source_rows_scanned={scanned_rows}")
    expected_rows = parquet.metadata.num_rows
    pattern_total = sum(pair_counts.values())
    accounting_valid = scanned_rows == expected_rows == pattern_total + unresolved_rows
    domain_valid = unresolved_rows == 0
    checks = [
        CheckResult("registry_review_evidence_valid", "pass", "blocking", 0, 0, CHECK_DETAILS["registry_review_evidence_valid"]),
        CheckResult("reviewed_retirements_all_missing", "pass" if retirement_failures == 0 else "fail", "blocking", retirement_failures, 0, CHECK_DETAILS["reviewed_retirements_all_missing"]),
        CheckResult("ingested_dynamic_hash_matches_manifest", "pass" if hash_matches else "fail", "blocking", hash_matches, True, CHECK_DETAILS["ingested_dynamic_hash_matches_manifest"]),
        CheckResult("composite_source_binding_valid", "pass" if binding_valid else "fail", "blocking", binding_valid, True, CHECK_DETAILS["composite_source_binding_valid"]),
        CheckResult("composite_binary_token_domain", "pass" if domain_valid else "fail", "blocking", unresolved_rows, 0, CHECK_DETAILS["composite_binary_token_domain"]),
        CheckResult("pair_pattern_accounting_conserved", "pass" if accounting_valid else "fail", "blocking", scanned_rows, expected_rows, CHECK_DETAILS["pair_pattern_accounting_conserved"]),
        CheckResult("fixed_size_list_contract_valid", "pass" if domain_valid and accounting_valid else "fail", "blocking", policy.composite.output_list_size, 2, CHECK_DETAILS["fixed_size_list_contract_valid"]),
        CheckResult("no_harmonized_artifacts_generated", "pass", "blocking", False, False, CHECK_DETAILS["no_harmonized_artifacts_generated"]),
    ]
    status = overall_status(checks)
    blockers = tuple(
        {"check": check.name, "details": check.details}
        for check in checks
        if check.status != "pass"
    )
    private_examples = tuple(
        {
            "source_position": position,
            "raw_token": token,
            "count": count,
        }
        for (position, token), count in unresolved_tokens.most_common(
            policy.maximum_private_unresolved_examples
        )
    )
    generated = utc_timestamp()
    safe_pair_counts = {label: pair_counts.get(label, 0) for label in PAIR_LABELS}
    payload = {
        "artifact": "asic_v3_composite_source_audit_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {"registry_review_run_id": registry_review_run_id},
        "metrics": {
            "reviewed_retirement_count": len(policy.retirements),
            "validated_retirement_count": len(policy.retirements) - retirement_failures,
            "composite_source_count": 2,
            "scanned_row_count": scanned_rows,
            "unresolved_row_count": unresolved_rows,
            "pair_pattern_counts": safe_pair_counts,
            "pair_pattern_count_total": pattern_total,
            "output_list_size": 2,
            "output_value_type": policy.composite.output_value_type,
        },
        "checks": [asdict(check) for check in checks],
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
        },
        "limitations": [
            "The audit validates a reviewed fixed-position representation but does not create it.",
            "Pair patterns preserve both source values and do not imply an OR, consensus, or preferred source.",
            "Exact source headers and unresolved tokens remain owner-only on the authorized cluster.",
        ],
    }
    assert_review_payload_is_safe(payload)
    private_manifest = {
        "artifact": "asic_v3_composite_source_audit_private",
        "artifact_version": policy.private_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "overall_status": status,
        "inputs": {
            "registry_review_run_id": registry_review_run_id,
            "registry_review_manifest_sha256": sha256_file(review_manifest_path),
            "registry_review_occurrences_sha256": sha256_file(occurrences_path),
            "ingestion_manifest_sha256": sha256_file(ingestion_manifest_path),
            "ingested_dynamic_sha256": observed_hash,
        },
        "policy": {
            "version": policy.version,
            "path": str(policy.source_path),
        },
        "reviewed_retirements": private_retirements,
        "composite": {
            "decision_id": policy.composite.decision_id,
            "target": policy.composite.target,
            "source_review_item_ids": list(policy.composite.source_review_item_ids),
            "source_physical_names": physical_names,
            "source_raw_names": [row["raw_name"] for row in typed_source_rows],
            "source_status_counts": [dict(sorted(count.items())) for count in source_counts],
            "pair_pattern_counts": safe_pair_counts,
            "unresolved_row_count": unresolved_rows,
            "output_value_type": policy.composite.output_value_type,
            "output_list_size": 2,
            "reduction_applied": False,
        },
        "checks": [asdict(check) for check in checks],
        "unresolved_token_example_count": len(private_examples),
        "production_data_artifacts_generated": False,
    }
    return CompositeSourceAuditResult(
        overall_status=status,
        blocking_findings=blockers,
        technical_blocking_findings=blockers,
        private_manifest=private_manifest,
        private_unresolved_tokens=private_examples,
        review_payload=payload,
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
        "# ASIC v3 reviewed composite-source audit",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input registry-review run: `{payload['inputs']['registry_review_run_id']}`",
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
    for key, value in payload["metrics"].items():
        if key != "pair_pattern_counts":
            lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Pair patterns", ""])
    for label, count in payload["metrics"]["pair_pattern_counts"].items():
        lines.append(f"- `{label}`: `{count}`")
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in payload["limitations"])
    return "\n".join(lines) + "\n"


def write_composite_source_audit_bundle(
    result: CompositeSourceAuditResult,
    reports_root: Path,
    run_id: str | None = None,
) -> CompositeSourceAuditResult:
    selected = run_id or default_run_id()
    if not RUN_ID_PATTERN.fullmatch(selected):
        raise HarmonizationError("Invalid composite-source audit run ID")
    private_dir = reports_root / "private" / "composite_source_audit" / selected
    review_dir = reports_root / "review" / "composite_source_audit"
    review_json = review_dir / f"{selected}.json"
    review_md = review_dir / f"{selected}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Composite-source audit run already exists and will not be overwritten")
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    review_dir.mkdir(parents=True, exist_ok=True, mode=0o750)
    _write_json(private_dir / "composite_source_audit_manifest.json", result.private_manifest, 0o600)
    _write_private_parquet(private_dir / "unresolved_tokens.parquet", result.private_unresolved_tokens)
    readme = private_dir / "README.md"
    _write_json(
        private_dir / "decision_snapshot.json",
        {
            "reviewed_retirements": result.private_manifest["reviewed_retirements"],
            "composite": result.private_manifest["composite"],
        },
        0o600,
    )
    descriptor = os.open(readme, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write("# Owner-only composite-source audit\n\nKeep this evidence on the authorized cluster. No harmonized data is generated.\n")
    _write_json(review_json, result.review_payload, 0o640)
    temporary = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(result.review_payload))
    temporary.replace(review_md)
    review_md.chmod(0o640)
    return CompositeSourceAuditResult(
        **{
            **asdict(result),
            "private_report_directory": private_dir,
            "review_json_path": review_json,
            "review_markdown_path": review_md,
        }
    )
