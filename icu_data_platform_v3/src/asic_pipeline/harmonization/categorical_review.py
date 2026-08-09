from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.audit.report import utc_timestamp
from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError, HarmonizationError
from asic_pipeline.harmonization.consolidated_audit import (
    ConsolidatedAuditConfig,
    load_consolidated_audit_config,
)
from asic_pipeline.harmonization.consolidated_decisions import (
    load_reviewed_consolidated_decisions,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import (
    RUN_ID_PATTERN,
    _write_private_parquet,
    default_run_id,
)
from asic_pipeline.privacy import assert_review_payload_is_safe


@dataclass(frozen=True)
class CategoricalReviewPolicy:
    version: str
    reviewed_decisions_path: Path
    reviewed_decisions_sha256: str
    consolidated_private_artifact_version: str
    consolidated_review_artifact_version: str
    consolidated_private_directory_name: str
    consolidated_review_directory_name: str
    prior_reviewed_scalar: tuple[str, ...]
    prior_reviewed_composite: tuple[str, ...]
    priority_pending_variables: tuple[str, ...]
    private_artifact_version: str
    review_artifact_version: str
    private_directory_name: str
    review_directory_name: str
    source_path: Path


@dataclass(frozen=True)
class CategoricalReviewConfig:
    consolidated: ConsolidatedAuditConfig
    policy_path: Path

    @property
    def dataset_context(self) -> str:
        return self.consolidated.dataset_context

    @property
    def reports_root(self) -> Path:
        return self.consolidated.reports_root


@dataclass(frozen=True)
class CategoricalReviewResult:
    run_id: str
    consolidated_audit_run_id: str
    overall_status: str
    blocking_findings: tuple[dict[str, Any], ...]
    technical_blocking_findings: tuple[dict[str, Any], ...]
    private_report_directory: Path
    review_json_path: Path
    review_markdown_path: Path

    @property
    def has_technical_failure(self) -> bool:
        return bool(self.technical_blocking_findings)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a YAML mapping")
    return dict(value)


def _string_tuple(value: Any, location: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError(f"{location} must be a non-empty list")
    result = tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise ConfigurationError(f"{location} entries must be non-empty strings")
    if len(result) != len(set(result)):
        raise ConfigurationError(f"{location} contains duplicate entries")
    return result


def load_categorical_review_config(path: str | Path) -> CategoricalReviewConfig:
    source = Path(path).expanduser().resolve()
    consolidated = load_consolidated_audit_config(source)
    raw = load_yaml_mapping(source, "Categorical contract review configuration")
    policy_path = resolve_path(
        required_string(
            raw, "categorical_contract_review_policy", "config"
        ),
        source,
    )
    return CategoricalReviewConfig(consolidated=consolidated, policy_path=policy_path)


def load_categorical_review_policy(path: str | Path) -> CategoricalReviewPolicy:
    source = Path(path).expanduser().resolve()
    raw = load_yaml_mapping(source, "Categorical contract review policy")
    if raw.get("categorical_contract_review_policy_version") != "0.1":
        raise ConfigurationError("Categorical review policy version must be 0.1")
    if raw.get("status") != (
        "approved_for_read_only_review_from_immutable_consolidated_evidence"
    ):
        raise ConfigurationError("Categorical review policy is not approved")
    decisions = _mapping(raw.get("reviewed_decisions"), "reviewed_decisions")
    consolidated = _mapping(
        raw.get("required_consolidated_audit"), "required_consolidated_audit"
    )
    if consolidated.get("technical_blocking_finding_count") != 0:
        raise ConfigurationError("Categorical review requires a technical pass")
    prior = _mapping(raw.get("prior_reviewed_variables"), "prior_reviewed_variables")
    write_boundary = _mapping(raw.get("write_boundary"), "write_boundary")
    if write_boundary != {
        "read_candidate_data": False,
        "rescan_clinical_rows": False,
        "modify_candidate_data": False,
        "write_harmonized_data": False,
        "publish_data": False,
    }:
        raise ConfigurationError("Categorical review write boundary changed")
    reporting = _mapping(raw.get("reporting"), "reporting")
    return CategoricalReviewPolicy(
        version="0.1",
        reviewed_decisions_path=resolve_path(
            required_string(decisions, "path", "reviewed_decisions"), source
        ),
        reviewed_decisions_sha256=required_string(
            decisions, "sha256", "reviewed_decisions"
        ),
        consolidated_private_artifact_version=required_string(
            consolidated, "private_artifact_version", "required_consolidated_audit"
        ),
        consolidated_review_artifact_version=required_string(
            consolidated, "review_artifact_version", "required_consolidated_audit"
        ),
        consolidated_private_directory_name=required_string(
            consolidated, "private_directory_name", "required_consolidated_audit"
        ),
        consolidated_review_directory_name=required_string(
            consolidated, "review_directory_name", "required_consolidated_audit"
        ),
        prior_reviewed_scalar=_string_tuple(
            prior.get("scalar"), "prior_reviewed_variables.scalar"
        ),
        prior_reviewed_composite=_string_tuple(
            prior.get("fixed_position_composite"),
            "prior_reviewed_variables.fixed_position_composite",
        ),
        priority_pending_variables=_string_tuple(
            raw.get("priority_pending_variables"), "priority_pending_variables"
        ),
        private_artifact_version=required_string(
            reporting, "private_artifact_version", "reporting"
        ),
        review_artifact_version=required_string(
            reporting, "review_artifact_version", "reporting"
        ),
        private_directory_name=required_string(
            reporting, "private_directory_name", "reporting"
        ),
        review_directory_name=required_string(
            reporting, "review_directory_name", "reporting"
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
        "# ASIC v3 categorical-contract evidence review",
        "",
        f"- Generated (UTC): `{payload['generated_at_utc']}`",
        f"- Dataset context: `{payload['dataset_context']}`",
        f"- Overall status: **{payload['overall_status'].upper()}**",
        f"- Input consolidated audit: `{payload['consolidated_audit_run_id']}`",
        "- Candidate data were not read or modified",
        "- Exact categorical values remain in the owner-only private workbook",
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
    lines.extend(("", "## Review totals", ""))
    lines.extend(f"- {key}: `{value}`" for key, value in payload["metrics"].items())
    lines.extend(("", "## Variables pending categorical approval", ""))
    lines.extend(
        f"- `{item['table']}.{item['variable']}`: domain size `{item['domain_value_count']}`; hospitals `{item['hospital_count']}`"
        for item in payload["pending_variables"]
    )
    lines.extend(
        (
            "",
            "## Human review gate",
            "",
            "Review the owner-only exact domains and approve or revise each pending mapping. This checkpoint does not freeze the ordered schema or variable dictionary.",
            "",
            "## Limitations",
            "",
            "- This command reuses immutable consolidated-audit evidence and does not rescan clinical rows.",
            "- A complete observed domain is evidence, not automatic clinical-semantic approval.",
            "- The fixed-position therapy confirmation composite retains its separately approved contract.",
            "- No harmonized, cleaned, derived, or published artifact is generated.",
            "",
        )
    )
    return "\n".join(lines)


def run_categorical_contract_review(
    config: CategoricalReviewConfig,
    consolidated_audit_run_id: str,
    run_id: str | None = None,
) -> CategoricalReviewResult:
    if not RUN_ID_PATTERN.fullmatch(consolidated_audit_run_id):
        raise HarmonizationError("Invalid consolidated-audit run ID")
    selected_run = default_run_id() if run_id is None else run_id
    if not RUN_ID_PATTERN.fullmatch(selected_run):
        raise HarmonizationError("Invalid categorical-review run ID")
    policy = load_categorical_review_policy(config.policy_path)
    if sha256_file(policy.reviewed_decisions_path) != policy.reviewed_decisions_sha256:
        raise HarmonizationError("Reviewed consolidated decisions changed")
    decisions = load_reviewed_consolidated_decisions(policy.reviewed_decisions_path)
    if consolidated_audit_run_id != decisions.consolidated_audit_run_id:
        raise HarmonizationError(
            "Categorical review must use the consolidated audit bound to the approved decisions"
        )

    source_private = (
        config.reports_root
        / "private"
        / policy.consolidated_private_directory_name
        / consolidated_audit_run_id
    )
    source_review = (
        config.reports_root
        / "review"
        / policy.consolidated_review_directory_name
        / f"{consolidated_audit_run_id}.json"
    )
    source_manifest_path = source_private / "consolidated_audit_manifest.json"
    source_manifest = _read_json(source_manifest_path, "Consolidated private manifest")
    source_review_payload = _read_json(source_review, "Consolidated review")
    if (
        source_manifest.get("artifact")
        != "asic_v3_consolidated_harmonization_audit_private"
        or source_manifest.get("artifact_version")
        != policy.consolidated_private_artifact_version
        or source_manifest.get("run_id") != consolidated_audit_run_id
        or source_manifest.get("candidate_run_id") != decisions.candidate_run_id
        or source_review_payload.get("artifact_version")
        != policy.consolidated_review_artifact_version
        or source_review_payload.get("run_id") != consolidated_audit_run_id
        or source_review_payload.get("metrics", {}).get(
            "technical_blocking_finding_count"
        )
        != 0
    ):
        raise HarmonizationError("Consolidated categorical evidence is not eligible")

    required_tables = (
        "categorical_domains.parquet",
        "list_profiles.parquet",
        "candidate_variable_dictionary.parquet",
    )
    hashes = source_manifest.get("table_sha256")
    if not isinstance(hashes, dict):
        raise HarmonizationError("Consolidated table hashes are missing")
    for name in required_tables:
        table_path = source_private / name
        if not table_path.is_file() or hashes.get(name) != sha256_file(table_path):
            raise HarmonizationError(f"Immutable categorical evidence changed: {name}")

    domain_rows = pq.read_table(source_private / required_tables[0]).to_pylist()
    list_rows = pq.read_table(source_private / required_tables[1]).to_pylist()
    dictionary_rows = pq.read_table(source_private / required_tables[2]).to_pylist()
    dictionary_keys = {
        (str(row.get("table")), str(row.get("variable"))) for row in dictionary_rows
    }
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in domain_rows:
        key = (str(row.get("table")), str(row.get("variable")))
        grouped.setdefault(key, []).append(dict(row))
    if set(grouped) - dictionary_keys:
        raise HarmonizationError("Categorical evidence is absent from the dictionary")

    prior_scalar = set(policy.prior_reviewed_scalar)
    summaries: list[dict[str, Any]] = []
    private_rows: list[dict[str, Any]] = []
    for (table, variable), rows in sorted(grouped.items()):
        status = (
            "prior_raw_v3_decision_retained"
            if variable in prior_scalar
            else "pending_human_domain_and_mapping_review"
        )
        summaries.append(
            {
                "table": table,
                "variable": variable,
                "hospital_count": len({str(row.get("hospital")) for row in rows}),
                "domain_value_count": len(
                    {
                        str(row.get("value_repr"))
                        for row in rows
                        if not bool(row.get("value_is_null"))
                    }
                ),
                "non_null_cell_count": sum(
                    int(row.get("count", 0))
                    for row in rows
                    if not bool(row.get("value_is_null"))
                ),
                "null_cell_count": sum(
                    int(row.get("count", 0))
                    for row in rows
                    if bool(row.get("value_is_null"))
                ),
                "domain_truncated": any(
                    bool(row.get("domain_truncated")) for row in rows
                ),
                "review_status": status,
                "priority_review": variable in policy.priority_pending_variables,
            }
        )
        private_rows.extend({**row, "review_status": status} for row in rows)

    composite_variables = set(policy.prior_reviewed_composite)
    observed_composites = {
        str(row.get("variable"))
        for row in list_rows
        if str(row.get("variable")) in composite_variables
    }
    missing_prior_scalar = sorted(prior_scalar - {item[1] for item in grouped})
    missing_composites = sorted(composite_variables - observed_composites)
    truncated_variables = sorted(
        row["variable"] for row in summaries if row["domain_truncated"]
    )
    technical_count = (
        len(missing_prior_scalar) + len(missing_composites) + len(truncated_variables)
    )
    pending = [
        row
        for row in summaries
        if row["review_status"] == "pending_human_domain_and_mapping_review"
    ]
    blockers: list[dict[str, Any]] = []
    technical: list[dict[str, Any]] = []
    if technical_count:
        finding = {
            "check": "categorical_evidence_complete",
            "details": "Every categorical domain must be complete and every prior reviewed scalar/composite must remain present.",
        }
        blockers.append(finding)
        technical.append(finding)
    if pending:
        blockers.append(
            {
                "check": "remaining_categorical_domains_approved",
                "details": "Every remaining exact domain and value mapping requires explicit human approval or revision.",
            }
        )

    generated = utc_timestamp()
    review_payload = {
        "artifact": "asic_v3_categorical_contract_review",
        "artifact_version": policy.review_artifact_version,
        "dataset_context": config.dataset_context,
        "generated_at_utc": generated,
        "run_id": selected_run,
        "consolidated_audit_run_id": consolidated_audit_run_id,
        "candidate_run_id": decisions.candidate_run_id,
        "overall_status": "fail" if blockers else "pass",
        "blocking_findings": blockers,
        "metrics": {
            "scalar_categorical_variable_count": len(summaries),
            "prior_reviewed_scalar_variable_count": len(summaries) - len(pending),
            "pending_scalar_variable_count": len(pending),
            "fixed_position_composite_variable_count": len(observed_composites),
            "domain_row_count": len(domain_rows),
            "domain_truncated_variable_count": len(truncated_variables),
            "technical_blocking_finding_count": len(technical),
            "candidate_rows_rescanned": 0,
        },
        "pending_variables": [
            {
                "table": row["table"],
                "variable": row["variable"],
                "domain_value_count": row["domain_value_count"],
                "hospital_count": row["hospital_count"],
                "priority_review": row["priority_review"],
            }
            for row in pending
        ],
        "publication": {
            "candidate_mutated": False,
            "harmonized_artifact_generated": False,
            "publication_ready": False,
        },
    }
    assert_review_payload_is_safe(review_payload)

    private_dir = (
        config.reports_root / "private" / policy.private_directory_name / selected_run
    )
    review_dir = config.reports_root / "review" / policy.review_directory_name
    review_json = review_dir / f"{selected_run}.json"
    review_md = review_dir / f"{selected_run}.md"
    if private_dir.exists() or review_json.exists() or review_md.exists():
        raise HarmonizationError("Categorical review run already exists")
    private_dir.mkdir(parents=True, mode=0o700)
    private_dir.chmod(0o700)
    private_table = private_dir / "categorical_domain_review.parquet"
    _write_private_parquet(private_table, tuple(private_rows))
    private_table.chmod(0o600)
    _write_json(
        private_dir / "categorical_review_manifest.json",
        {
            "artifact": "asic_v3_categorical_contract_review_private",
            "artifact_version": policy.private_artifact_version,
            "dataset_context": config.dataset_context,
            "generated_at_utc": generated,
            "run_id": selected_run,
            "consolidated_audit_run_id": consolidated_audit_run_id,
            "candidate_run_id": decisions.candidate_run_id,
            "source_consolidated_manifest_sha256": sha256_file(source_manifest_path),
            "source_table_sha256": {name: hashes[name] for name in required_tables},
            "reviewed_decisions_sha256": policy.reviewed_decisions_sha256,
            "categorical_domain_review_sha256": sha256_file(private_table),
            "publication_ready": False,
        },
        0o600,
    )
    review_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    review_dir.chmod(0o750)
    _write_json(review_json, review_payload, 0o640)
    temporary_md = review_md.with_suffix(".md.tmp")
    descriptor = os.open(temporary_md, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_markdown(review_payload))
    temporary_md.replace(review_md)
    review_md.chmod(0o640)
    return CategoricalReviewResult(
        run_id=selected_run,
        consolidated_audit_run_id=consolidated_audit_run_id,
        overall_status=review_payload["overall_status"],
        blocking_findings=tuple(blockers),
        technical_blocking_findings=tuple(technical),
        private_report_directory=private_dir,
        review_json_path=review_json,
        review_markdown_path=review_md,
    )
