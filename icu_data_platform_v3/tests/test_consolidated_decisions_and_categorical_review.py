from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.harmonization import (
    CategoricalReviewConfig,
    load_categorical_review_policy,
    load_reviewed_consolidated_decisions,
    run_categorical_contract_review,
)
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.harmonization.consolidated_audit import _decision_level_evidence


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_shipped_partial_consolidated_decisions_are_fail_closed(
    project_root: Path,
) -> None:
    path = (
        project_root
        / "asic/config/harmonization/reviewed_consolidated_decisions_0_1.yaml"
    )
    decisions = load_reviewed_consolidated_decisions(path)

    assert decisions.candidate_run_id == "20260806T074852Z"
    assert decisions.consolidated_audit_run_id == "20260806T092818Z"
    assert len(decisions.approved_units) == 55
    assert {
        (item.hospital, item.variable, item.operation, item.factor)
        for item in decisions.conversions
    } == {
        ("asic_UK04", "etco2", "divide", 7.50062),
        ("asic_UK03", "fio2", "multiply", 100.0),
        ("asic_UK03", "hematocrit", "multiply", 100.0),
        ("asic_UK00", "lymph_pct", "multiply", 100.0),
    }
    assert decisions.all_missing_variables == (
        "feo2",
        "severity_read_confirmation",
        "sofa_score_without_gcs",
        "stroke_volume_bolus",
    )

    categorical_policy = load_categorical_review_policy(
        project_root / "asic/config/harmonization/categorical_contract_review.yaml"
    )
    assert sha256_file(path) == categorical_policy.reviewed_decisions_sha256
    assert categorical_policy.priority_pending_variables == (
        "ards_diagnosis_app",
        "ecmo",
        "position_therapy",
    )


def test_targeted_quality_rules_have_one_explicit_table_scope(
    project_root: Path,
) -> None:
    import yaml

    quality = yaml.safe_load(
        (
            project_root
            / "asic/config/harmonization/cross_hospital_quality_audit.yaml"
        ).read_text(encoding="utf-8")
    )
    assert {item["table"] for item in quality["targeted_scale_audits"]} == {
        "dynamic"
    }
    assert {item["table"] for item in quality["targeted_bucket_audits"]} == {
        "dynamic"
    }


def test_review_counts_are_decision_level_not_hospital_row_level() -> None:
    relationships, invalid, row_scale = _decision_level_evidence(
        [
            {"audit_id": "same_rule", "table": "dynamic", "hospital": "asic_UK00"},
            {"audit_id": "same_rule", "table": "dynamic", "hospital": "asic_UK01"},
        ],
        [
            {"table": "dynamic", "variable": "ph_art", "hospital": "asic_UK00", "invalid_count": 1},
            {"table": "dynamic", "variable": "ph_art", "hospital": "asic_UK01", "invalid_count": 2},
        ],
        [
            {"audit_id": "ph_scale", "candidate_recovery_count": 1},
        ],
    )
    assert relationships == [
        {
            "audit_id": "same_rule",
            "table": "dynamic",
            "variable": None,
            "hospital": None,
            "evidence_row_count": 2,
        }
    ]
    assert invalid == {("dynamic", "ph_art")}
    assert row_scale == {"ph_scale"}


def test_categorical_review_reuses_immutable_evidence_and_keeps_values_private(
    tmp_path: Path,
    project_root: Path,
) -> None:
    reports = tmp_path / "reports"
    source_private = (
        reports
        / "private/consolidated_harmonization_audit/20260806T092818Z"
    )
    domain_path = source_private / "categorical_domains.parquet"
    list_path = source_private / "list_profiles.parquet"
    dictionary_path = source_private / "candidate_variable_dictionary.parquet"
    protected = "PROTECTED_RAW_CATEGORY_TOKEN"
    domain_rows = [
        {
            "table": "static",
            "hospital": "asic_UK00",
            "variable": variable,
            "value_repr": "true",
            "value_is_null": False,
            "count": 1,
            "domain_truncated": False,
        }
        for variable in (
            "hospital_mortality_reported",
            "icu_readmit",
            "study_implementation_phase",
        )
    ]
    domain_rows.append(
        {
            "table": "dynamic",
            "hospital": "asic_UK00",
            "variable": "ards_diagnosis_app",
            "value_repr": protected,
            "value_is_null": False,
            "count": 10,
            "domain_truncated": False,
        }
    )
    _write_rows(domain_path, domain_rows)
    _write_rows(
        list_path,
        [
            {
                "table": "dynamic",
                "hospital": "asic_UK00",
                "variable": "therapy_read_confirmation_utc",
                "arrow_type": "fixed_size_list<element: bool>[2]",
                "row_count": 10,
            }
        ],
    )
    _write_rows(
        dictionary_path,
        [
            {"table": row["table"], "variable": row["variable"]}
            for row in domain_rows
        ]
        + [
            {
                "table": "dynamic",
                "variable": "therapy_read_confirmation_utc",
            }
        ],
    )
    manifest_path = source_private / "consolidated_audit_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "artifact": "asic_v3_consolidated_harmonization_audit_private",
                "artifact_version": "0.1",
                "run_id": "20260806T092818Z",
                "candidate_run_id": "20260806T074852Z",
                "table_sha256": {
                    path.name: sha256_file(path)
                    for path in (domain_path, list_path, dictionary_path)
                },
            }
        ),
        encoding="utf-8",
    )
    source_review = (
        reports
        / "review/consolidated_harmonization_audit/20260806T092818Z.json"
    )
    source_review.parent.mkdir(parents=True, exist_ok=True)
    source_review.write_text(
        json.dumps(
            {
                "artifact_version": "0.1",
                "run_id": "20260806T092818Z",
                "metrics": {"technical_blocking_finding_count": 0},
            }
        ),
        encoding="utf-8",
    )
    config = CategoricalReviewConfig(
        consolidated=SimpleNamespace(
            dataset_context="production",
            reports_root=reports,
        ),
        policy_path=(
            project_root
            / "asic/config/harmonization/categorical_contract_review.yaml"
        ),
    )

    result = run_categorical_contract_review(
        config,
        "20260806T092818Z",
        "categorical001",
    )

    assert result.overall_status == "fail"
    assert result.has_technical_failure is False
    review_text = result.review_json_path.read_text(encoding="utf-8")
    markdown_text = result.review_markdown_path.read_text(encoding="utf-8")
    assert protected not in review_text
    assert protected not in markdown_text
    review = json.loads(review_text)
    assert review["metrics"]["candidate_rows_rescanned"] == 0
    assert [row["variable"] for row in review["pending_variables"]] == [
        "ards_diagnosis_app"
    ]
    private_values = pq.read_table(
        result.private_report_directory / "categorical_domain_review.parquet"
    )["value_repr"].to_pylist()
    assert protected in private_values
