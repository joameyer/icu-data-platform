from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    StaticContractAuditConfig,
    build_static_contract_audit,
    load_static_contract_audit_config,
    load_static_contract_audit_policy,
    write_static_contract_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


REGISTRY_RUN = "20260805T104417Z"
COMPOSITE_RUN = "20260805T111026Z"
INGESTION_AUDIT_RUN = "20260805T060046Z"


VARIABLES = (
    ("stay_id_global", "identifier", "large_string", "not_applicable"),
    ("weight_kg", "numeric", "float64", "kg"),
    ("hosp_los", "numeric", "float64", "day"),
    ("icu_los", "numeric", "float64", "day"),
    ("dialysis_free_days", "numeric", "float64", "day"),
    ("vent_free_days", "numeric", "float64", "day"),
    ("hospital_mortality_reported", "binary", "bool", "not_applicable"),
    ("icd10_codes", "free_text", "large_string", "not_applicable"),
)


def _config(tmp_path: Path, project_root: Path) -> StaticContractAuditConfig:
    reports = tmp_path / "reports" / "demo"
    inventory = InventoryConfig(
        dataset_context="demo",
        policy_path=project_root / "asic/config/inventory/policy.yaml",
        comparison_reference_path=None,
        paths=InventoryPaths(
            raw_root=tmp_path / "raw",
            reports=reports,
            runs=tmp_path / "runs" / "demo",
        ),
        source_path=project_root / "asic/config/datasets/demo.yaml",
    )
    ingestion = IngestionConfig(
        inventory=inventory,
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=tmp_path / "data" / "demo",
        rows_per_batch=10,
    )
    schema = SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )
    review = HarmonizationReviewConfig(
        schema_tokens=schema,
        policy_path=project_root / "asic/config/harmonization/review_policy.yaml",
    )
    return StaticContractAuditConfig(
        review,
        project_root / "asic/config/harmonization/static_contract_audit.yaml",
    )


def _write_static(path: Path) -> None:
    values = {
        "pseudo_id": ["patient_alpha", "patient_beta", "patient_gamma"],
        "weight": ["70", "80.5", ""],
        "hosp": ["5", "10", ""],
        "icu": ["2", "4", ""],
        "dfd": ["20", "18", ""],
        "vfd": ["19", "17", ""],
        "mortality": ["0", "1.0", ""],
        "icd": ["A10", "B20", ""],
    }
    fields = []
    arrays = []
    for physical_name, column in values.items():
        field = pa.field(
            physical_name,
            pa.large_string(),
            metadata={
                b"raw_name": f"raw_{physical_name}".encode("utf-8"),
                b"raw_occurrence": b"1",
            },
        )
        fields.append(field)
        arrays.append(pa.array(column, type=pa.large_string()))
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), path)


def _write_evidence(config: StaticContractAuditConfig) -> None:
    reports = config.reports_root
    schema_dir = reports / "private" / "schema_token_inventory" / "schema_input"
    schema_dir.mkdir(parents=True)
    schema_manifest = {
        "artifact": "asic_v3_schema_token_inventory_private",
        "artifact_version": "0.3",
        "dataset_context": "demo",
        "inputs": {"ingestion_audit_run_id": INGESTION_AUDIT_RUN},
    }
    schema_manifest_path = schema_dir / "schema_token_inventory_manifest.json"
    schema_manifest_path.write_text(json.dumps(schema_manifest), encoding="utf-8")

    private = (
        reports / "private" / "harmonization_registry_review" / REGISTRY_RUN
    )
    private.mkdir(parents=True)
    review_dir = reports / "review" / "harmonization_registry_review"
    review_dir.mkdir(parents=True)
    occurrences = []
    variables = []
    physical_names = tuple(
        "pseudo_id weight hosp icu dfd vfd mortality icd".split()
    )
    for index, ((target, mode, value_type, unit), physical_name) in enumerate(
        zip(VARIABLES, physical_names, strict=True), start=1
    ):
        nonempty = 3 if mode == "identifier" else 2
        literal_empty = 0 if mode == "identifier" else 1
        kind = {
            "identifier": "identifier",
            "numeric": "numeric",
            "binary": "numeric",
            "free_text": "free_text",
        }[mode]
        occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": "asic_UK00",
                "table": "static",
                "physical_name": physical_name,
                "raw_name": f"raw_{physical_name}",
                "raw_occurrence": 1,
                "candidate_target": target,
                "candidate_kind": kind,
                "mapping_review_status": "requires_human_review",
                "candidate_value_type": value_type,
                "value_type_review_status": "candidate_requires_raw_v3_review",
                "candidate_unit": unit,
                "unit_review_status": (
                    "not_applicable"
                    if unit == "not_applicable"
                    else "candidate_requires_raw_v3_review"
                ),
                "parser_review_status": "requires_human_review",
                "approved_missing_sentinel_tokens": [],
                "expected_row_count": 3,
                "literal_empty_count": literal_empty,
                "raw_nonempty_count": nonempty,
            }
        )
        variables.append(
            {
                "variable_review_id": f"V{index:04d}",
                "table": "static",
                "candidate_target": target,
                "candidate_kind": kind,
                "candidate_value_type": value_type,
                "value_type_review_statuses": [
                    "candidate_requires_raw_v3_review"
                ],
                "candidate_unit": unit,
                "unit_review_statuses": [
                    "not_applicable"
                    if unit == "not_applicable"
                    else "candidate_requires_raw_v3_review"
                ],
            }
        )
    _write_private_parquet(private / "occurrences.parquet", occurrences)
    _write_private_parquet(private / "variables.parquet", variables)
    registry_manifest = {
        "artifact": "asic_v3_harmonization_registry_review_private",
        "artifact_version": "0.3",
        "dataset_context": "demo",
        "inputs": {
            "schema_token_private_directory": str(schema_dir),
            "hashes": {
                "schema_token_manifest_sha256": sha256_file(schema_manifest_path)
            },
        },
        "occurrence_review_row_count": len(occurrences),
        "variable_review_row_count": len(variables),
    }
    (private / "harmonization_registry_review_manifest.json").write_text(
        json.dumps(registry_manifest), encoding="utf-8"
    )
    (review_dir / f"{REGISTRY_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_harmonization_registry_review",
                "artifact_version": "0.3",
                "dataset_context": "demo",
                "metrics": {"technical_blocking_finding_count": 0},
            }
        ),
        encoding="utf-8",
    )

    composite_dir = reports / "review" / "composite_source_audit"
    composite_dir.mkdir(parents=True)
    (composite_dir / f"{COMPOSITE_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_composite_source_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "overall_status": "pass",
                "inputs": {"registry_review_run_id": REGISTRY_RUN},
            }
        ),
        encoding="utf-8",
    )
    ingestion_audit_dir = reports / "review" / "ingestion_audit"
    ingestion_audit_dir.mkdir(parents=True)
    (ingestion_audit_dir / f"{INGESTION_AUDIT_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_ingestion_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "checks": [{"name": "identifier_agreement", "status": "pass"}],
            }
        ),
        encoding="utf-8",
    )

    hospital_dir = config.ingested_root / "asic_UK00"
    static_path = hospital_dir / "static.parquet"
    _write_static(static_path)
    (hospital_dir / "ingestion_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_lossless_hospital_ingestion",
                "artifact_version": "0.2",
                "hospital": {"canonical_hospital_id": "asic_UK00"},
                "outputs": {"static": {"sha256": sha256_file(static_path)}},
            }
        ),
        encoding="utf-8",
    )


def test_static_contract_audit_is_read_only_private_and_review_gated(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_evidence(config)

    result = build_static_contract_audit(config, REGISTRY_RUN, COMPOSITE_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    assert result.review_payload["metrics"]["selected_variable_count"] == 8
    assert result.review_payload["metrics"]["scanned_static_row_count"] == 3
    assert result.review_payload["metrics"]["scanned_selected_cell_count"] == 24
    assert result.review_payload["metrics"]["unresolved_nonempty_token_count"] == 0
    mortality = next(
        item
        for item in result.review_payload["variables"]
        if item["candidate_target"] == "hospital_mortality_reported"
    )
    assert mortality["binary_false_count"] == 1
    assert mortality["binary_true_count"] == 1
    assert "patient_alpha" not in json.dumps(result.private_manifest)
    assert "patient_alpha" not in json.dumps(result.private_occurrences)
    assert "patient_alpha" not in json.dumps(result.private_token_examples)
    assert "A10" in json.dumps(result.private_token_examples)
    assert "A10" not in json.dumps(result.review_payload)

    written = write_static_contract_audit_bundle(
        result, config.reports_root, "static_contract_output"
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_static_contract_audit_bundle(
            result, config.reports_root, "static_contract_output"
        )


def test_static_contract_policy_and_configs_are_fail_closed(project_root: Path) -> None:
    for context in ("demo", "production"):
        config = load_static_contract_audit_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_static_contract_audit_policy(config.policy_path)
        assert policy.version == "0.1"
        assert len(policy.variables) == 8
        assert policy.registry_review_basis_run_id == REGISTRY_RUN
        assert policy.composite_audit_basis_run_id == COMPOSITE_RUN


def test_static_contract_audit_surfaces_changed_ingested_hash(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_evidence(config)
    static_path = config.ingested_root / "asic_UK00" / "static.parquet"
    table = pq.read_table(static_path)
    pq.write_table(table.replace_schema_metadata({b"changed": b"true"}), static_path)

    result = build_static_contract_audit(config, REGISTRY_RUN, COMPOSITE_RUN)

    assert result.has_technical_failure is True
    assert "ingested_static_hashes_match_manifests" in {
        item["check"] for item in result.technical_blocking_findings
    }
