from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.harmonization import (
    load_composite_source_audit_policy,
    load_consolidated_audit_config,
    load_consolidated_audit_policy,
    load_harmonization_dry_run_config,
    load_harmonization_dry_run_policy,
    load_harmonization_review_policy,
    load_reviewed_icd10_contract,
    load_reviewed_static_decision_registry,
)
from asic_pipeline.harmonization.icd10_parser import (
    ReviewedICD10PrivateEvidence,
)
from asic_pipeline.harmonization.parsing import (
    parse_explicit_percentage_fraction,
    parse_ratio_numeric,
    parse_threshold_boundary,
)
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.schema_tokens.policy import load_schema_token_policy


dry_run = importlib.import_module("asic_pipeline.harmonization.dry_run")
consolidated_audit = importlib.import_module(
    "asic_pipeline.harmonization.consolidated_audit"
)


def test_candidate_notation_parsers_are_bounded() -> None:
    assert parse_ratio_numeric("1:2") == 0.5
    assert parse_ratio_numeric("1/2") == 0.5
    assert parse_ratio_numeric("1:0") is None
    assert parse_ratio_numeric("1:2:3") is None
    assert parse_explicit_percentage_fraction("50%") == 0.5
    assert parse_explicit_percentage_fraction("0,5%") == 0.005
    assert parse_explicit_percentage_fraction("50") is None
    assert parse_threshold_boundary("< 10") == 10.0
    assert parse_threshold_boundary(">=0,25") == 0.25
    assert parse_threshold_boundary("approximately 10") is None


def test_dynamic_identifier_evidence_uses_filename_and_optional_corroboration() -> None:
    filename = pa.array(["42", "42"], type=pa.string())
    unavailable_in_file = pa.array([None, ""], type=pa.string())
    counts = dry_run._validate_dynamic_identifier_evidence(
        filename,
        unavailable_in_file,
        None,
    )
    assert counts == {
        "filename_nonempty_row_count": 2,
        "in_file_nonempty_row_count": 0,
        "raw_corroborating_nonempty_row_count": 0,
    }

    in_file = pa.array(["42", "42"], type=pa.string())
    raw = pa.array(["42", "42"], type=pa.large_string())
    counts = dry_run._validate_dynamic_identifier_evidence(
        filename,
        in_file,
        raw,
    )
    assert counts["in_file_nonempty_row_count"] == 2
    assert counts["raw_corroborating_nonempty_row_count"] == 2


@pytest.mark.parametrize(
    ("filename", "in_file", "raw", "message"),
    (
        ([None], [None], None, "filename-derived"),
        (["42"], ["43"], None, "in-file and filename-derived"),
        (["42"], ["42"], ["43"], "raw and ingested in-file"),
    ),
)
def test_dynamic_identifier_evidence_blocks_missing_or_disagreement(
    filename: list[str | None],
    in_file: list[str | None],
    raw: list[str | None] | None,
    message: str,
) -> None:
    with pytest.raises(HarmonizationError, match=message):
        dry_run._validate_dynamic_identifier_evidence(
            pa.array(filename, type=pa.string()),
            pa.array(in_file, type=pa.string()),
            pa.array(raw, type=pa.string()) if raw is not None else None,
        )


def test_dynamic_identifier_membership_is_required_without_exposing_value() -> None:
    dry_run._require_dynamic_identifiers_in_static(
        pa.array(["42", "42"], type=pa.large_string()),
        {"42"},
    )
    with pytest.raises(HarmonizationError, match="resolve to static stays") as error:
        dry_run._require_dynamic_identifiers_in_static(
            pa.array(["protected-stay"], type=pa.large_string()),
            {"42"},
        )
    assert "protected-stay" not in str(error.value)


def test_shipped_dry_run_policy_is_fail_closed(project_root: Path) -> None:
    config = load_harmonization_dry_run_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_harmonization_dry_run_policy(config.policy_path)

    assert policy.approved_hospitals == (
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    )
    assert policy.output_directory_name == "harmonization_candidates"
    assert policy.output_status == (
        "candidate_nonpublishable_requires_consolidated_review"
    )
    assert policy.evidence_run_ids["registry_review_run_id"] == (
        "20260805T104417Z"
    )
    phase = dry_run.load_reviewed_phase_decision(
        project_root
        / "asic/config/harmonization/reviewed_phase_decisions_0_1.yaml"
    )
    assert phase.categorical.values == {
        "K": "calibration",
        "RI": "roll_in",
        "QS": "app_implementation",
        "NAN": None,
    }
    assert [
        phase.categorical.values[
            dry_run._candidate_normalize(
                raw, phase.categorical.normalization
            )
        ]
        for raw in ("K", "RI", "QS", "nan")
    ] == ["calibration", "roll_in", "app_implementation", None]


def test_shipped_consolidated_audit_policy_is_complete_and_read_only(
    project_root: Path,
) -> None:
    config = load_consolidated_audit_config(
        project_root / "asic/config/datasets/production.yaml"
    )
    policy = load_consolidated_audit_policy(config.policy_path)

    assert dict(policy.hospital_suffixes) == {
        "asic_UK00": "0",
        "asic_UK01": "1",
        "asic_UK02": "2",
        "asic_UK03": "3",
        "asic_UK04": "4",
        "asic_UK06": "6",
        "asic_UK07": "7",
        "asic_UK08": "8",
    }
    assert policy.general_scale_factors == (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0)
    assert policy.private_directory_name == "consolidated_harmonization_audit"
    assert policy.comparison_reference_path is None
    assert policy.quality_policy_path.is_relative_to(project_root)
    assert policy.quality_policy_path.name == "cross_hospital_quality_audit.yaml"
    assert sha256_file(policy.quality_policy_path) == policy.quality_policy_sha256
    quality_rules = consolidated_audit._quality_rules(
        consolidated_audit.load_yaml_mapping(
            policy.quality_policy_path,
            "V3 cross-hospital quality policy",
        )
    )
    assert len(quality_rules["legacy_invalid_value_rules"]) == 44
    wrapper = (
        project_root
        / "asic/slurm/run_consolidated_harmonization_audit_production.sh"
    ).read_text(encoding="utf-8")
    assert "V2_ROOT" not in wrapper
    assert "/icu_data_platform/asic/" not in wrapper


def _source_field(name: str, raw_name: str) -> pa.Field:
    return pa.field(
        name,
        pa.large_string(),
        metadata={
            b"asic_v3_role": b"raw_clinical_string",
            b"raw_name": raw_name.encode("utf-8"),
            b"raw_occurrence": b"1",
        },
    )


def _write_ingested_table(
    path: Path,
    source_values: list[tuple[str, str, list[str | None]]],
    row_count: int,
    *,
    filename_stay_ids: list[str | None],
    in_file_stay_ids: list[str | None],
) -> None:
    assert len(filename_stay_ids) == row_count
    assert len(in_file_stay_ids) == row_count
    fields = [_source_field(name, raw_name) for name, raw_name, _ in source_values]
    fields.extend(
        (
            pa.field("__v3_source_file_id", pa.string()),
            pa.field("__v3_source_file_order", pa.int32()),
            pa.field("__v3_source_row_number", pa.int64()),
            pa.field("__v3_source_order", pa.int64()),
            pa.field("__v3_source_schema_variant_id", pa.string()),
            pa.field("__v3_filename_stay_id", pa.string()),
            pa.field("__v3_in_file_stay_id", pa.string()),
        )
    )
    arrays = [
        pa.array(values, type=pa.large_string())
        for _, _, values in source_values
    ]
    arrays.extend(
        (
            pa.array(["file"] * row_count, type=pa.string()),
            pa.array([0] * row_count, type=pa.int32()),
            pa.array(range(1, row_count + 1), type=pa.int64()),
            pa.array(range(row_count), type=pa.int64()),
            pa.array(["schema"] * row_count, type=pa.string()),
            pa.array(filename_stay_ids, type=pa.string()),
            pa.array(in_file_stay_ids, type=pa.string()),
        )
    )
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), path)


def _occurrence(
    review_item_id: str,
    hospital: str,
    table: str,
    physical_name: str,
    raw_name: str,
    target: str,
    kind: str,
    *,
    approved: bool = False,
) -> dict[str, object]:
    return {
        "review_item_id": review_item_id,
        "hospital": hospital,
        "table": table,
        "physical_name": physical_name,
        "raw_name": raw_name,
        "raw_occurrence": 1,
        "candidate_target": target,
        "candidate_kind": kind,
        "mapping_review_status": (
            "human_approved_raw_v3" if approved else "requires_human_review"
        ),
        "parser_review_status": (
            "human_approved_raw_v3" if approved else "requires_human_review"
        ),
        "proposed_action": "map_to_candidate_target",
    }


def test_streaming_dry_run_writes_isolated_common_schema_candidates(
    tmp_path: Path,
    project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hospitals = (
        "asic_UK00",
        "asic_UK01",
        "asic_UK02",
        "asic_UK03",
        "asic_UK04",
        "asic_UK06",
        "asic_UK07",
        "asic_UK08",
    )
    suffixes = ("00", "01", "02", "03", "04", "06", "07", "08")
    cluster_ids = ("C1", "C5", "C9", "C6", "C10", "C8", "C11", "C12")
    data_root = tmp_path / "asic/data/demo"
    ingested_root = data_root / "ingested"
    reports_root = tmp_path / "asic/reports/demo"
    occurrences: list[dict[str, object]] = []
    next_id = 1000

    for hospital, cluster_id in zip(hospitals, cluster_ids, strict=True):
        hospital_root = ingested_root / hospital
        hospital_root.mkdir(parents=True)
        static_sources = [
            ("Pseudo-ID", "Pseudo-ID", ["42"]),
            ("ICD-10_Codes", "ICD-10_Codes", ["A01, A01,B02"]),
            ("Cluster-ID", "Cluster-ID", [cluster_id]),
        ]
        dynamic_sources = [
            ("Herzfrequenz", "Herzfrequenz", ["80", "81"]),
        ]
        occurrences.extend(
            (
                _occurrence(
                    f"T{next_id}", hospital, "static", "Pseudo-ID", "Pseudo-ID",
                    "stay_id_global", "identifier", approved=True,
                ),
                _occurrence(
                    f"T{next_id + 1}", hospital, "static", "ICD-10_Codes",
                    "ICD-10_Codes", "icd10_codes", "free_text", approved=True,
                ),
                _occurrence(
                    f"T{next_id + 4}", hospital, "static", "Cluster-ID",
                    "Cluster-ID", "cluster_id", "categorical", approved=True,
                ),
                _occurrence(
                    f"T{next_id + 3}", hospital, "dynamic", "Herzfrequenz",
                    "Herzfrequenz", "heart_rate", "numeric",
                ),
            )
        )
        next_id += 10
        if hospital == "asic_UK00":
            static_sources.append(
                ("heightcm", "heightcm", ["[170.0, nan, 175.0]"])
            )
            occurrences.append(
                _occurrence(
                    "R-HEIGHT-UK00",
                    hospital,
                    "static",
                    "heightcm",
                    "heightcm",
                    "height_measurements_cm",
                    "numeric",
                    approved=True,
                )
            )
            dynamic_sources.extend(
                (
                    ("ph_retained", "pH", ["7.4", "7.3"]),
                    ("ph_empty", "pH", ["", ""]),
                    ("therapy_left", "LesebestaetigungTherapie_utc", ["1", None]),
                    ("therapy_right", "LesebestaetigugnTherapie_utc", ["0", "1"]),
                    ("vt_total", "VT_kgKG", ["450", "500"]),
                )
            )
            semantic_split = _occurrence(
                "T0999",
                hospital,
                "dynamic",
                "vt_total",
                "VT_kgKG",
                "vt_per_kg_ideal_body_weight",
                "numeric",
            )
            semantic_split.update(
                {
                    "semantic_split_candidate_id": "CANDIDATE-UK00-VT-PBW-TOTAL-001",
                    "semantic_split_proposed_value_type": "float64",
                    "semantic_split_proposed_unit": "mL",
                    "semantic_split_review_status": "requires_human_review",
                }
            )
            occurrences.extend(
                (
                    _occurrence("R0080", hospital, "dynamic", "ph_retained", "pH", "ph_art", "numeric"),
                    _occurrence("R0129", hospital, "dynamic", "ph_empty", "pH", "ph_art", "numeric", approved=True),
                    _occurrence("R0144", hospital, "dynamic", "therapy_left", "LesebestaetigungTherapie_utc", "therapy_read_confirmation_utc", "categorical", approved=True),
                    _occurrence("R0145", hospital, "dynamic", "therapy_right", "LesebestaetigugnTherapie_utc", "therapy_read_confirmation_utc", "categorical", approved=True),
                    semantic_split,
                )
            )
        if hospital == "asic_UK03":
            static_sources.append(("Phase", "Phase", ["K"]))
            occurrences.append(
                _occurrence(
                    "R-PHASE-UK03",
                    hospital,
                    "static",
                    "Phase",
                    "Phase",
                    "study_implementation_phase",
                    "categorical",
                    approved=True,
                )
            )
        if hospital == "asic_UK04":
            dynamic_sources.insert(0, ("Pseudo-ID", "Pseudo-ID", ["42", "42"]))
            occurrences.append(
                _occurrence(
                    f"T{next_id + 2}",
                    hospital,
                    "dynamic",
                    "Pseudo-ID",
                    "Pseudo-ID",
                    "stay_id_global",
                    "identifier",
                    approved=True,
                )
            )
        if hospital == "asic_UK02":
            dynamic_sources.extend(
                (
                    ("ntprob_empty", "NTproBNP", ["", ""]),
                    ("ntprob_retained", "NTproBNP", ["5", "6"]),
                )
            )
            occurrences.extend(
                (
                    _occurrence("R0326", hospital, "dynamic", "ntprob_empty", "NTproBNP", "ntprobnp", "numeric", approved=True),
                    _occurrence("R0344", hospital, "dynamic", "ntprob_retained", "NTproBNP", "ntprobnp", "numeric"),
                )
            )
        _write_ingested_table(
            hospital_root / "static.parquet",
            static_sources,
            1,
            filename_stay_ids=[None],
            in_file_stay_ids=["42"],
        )
        _write_ingested_table(
            hospital_root / "dynamic.parquet",
            dynamic_sources,
            2,
            filename_stay_ids=["42", "42"],
            in_file_stay_ids=(
                ["42", "42"] if hospital == "asic_UK04" else [None, None]
            ),
        )
        manifest = {
            "artifact": "asic_v3_lossless_hospital_ingestion",
            "artifact_version": "0.2",
            "hospital": {"canonical_hospital_id": hospital},
            "tables": {
                "static": {"output_row_count": 1},
                "dynamic": {"output_row_count": 2},
            },
            "outputs": {
                "static": {"sha256": sha256_file(hospital_root / "static.parquet")},
                "dynamic": {"sha256": sha256_file(hospital_root / "dynamic.parquet")},
            },
        }
        (hospital_root / "ingestion_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )

    candidate_contract = load_harmonization_review_policy(
        project_root / "asic/config/harmonization/review_policy.yaml"
    ).candidate_contract
    composite = load_composite_source_audit_policy(
        project_root / "asic/config/harmonization/composite_source_audit.yaml"
    )
    static_registry = load_reviewed_static_decision_registry(
        project_root / "asic/config/harmonization/reviewed_static_decisions_0_3.yaml"
    )
    icd10 = load_reviewed_icd10_contract(
        project_root / "asic/config/harmonization/reviewed_icd10_contract_0_2.yaml"
    )
    schema_policy = load_schema_token_policy(
        project_root / "asic/config/schema_tokens/policy.yaml"
    )
    inventory = SimpleNamespace(
        hospital_mappings=tuple(
            SimpleNamespace(
                canonical_hospital_id=hospital,
                source_folder=suffix,
                cohort_action="include",
            )
            for hospital, suffix in zip(hospitals, suffixes, strict=True)
        )
    )
    policy = dry_run.HarmonizationDryRunPolicy(
        version="0.3",
        evidence_run_ids={
            "inventory_run_id": "inventory",
            "ingestion_audit_run_id": "audit",
            "schema_token_run_id": "tokens",
            "registry_review_run_id": "registry",
            "composite_audit_run_id": "composite",
        },
        decision_paths={},
        decision_hashes={},
        approved_hospitals=hospitals,
        rows_per_batch=1,
        compression="zstd",
        maximum_token_examples=100,
        maximum_categorical_values=100,
        output_directory_name="harmonization_candidates",
        artifact_version="0.3",
        private_artifact_version="0.3",
        review_artifact_version="0.3",
        output_status="candidate_nonpublishable_requires_consolidated_review",
        source_path=tmp_path / "policy.yaml",
    )
    config = SimpleNamespace(
        policy_path=tmp_path / "policy.yaml",
        dataset_context="demo",
        data_root=data_root,
        ingested_root=ingested_root,
        reports_root=reports_root,
    )
    private_icd = ReviewedICD10PrivateEvidence(
        audit_run_id="20260805T135859Z",
        incomplete_components=(("asic_UK08", "D"),),
        source_path=tmp_path / "examples.parquet",
    )
    phase_decision = dry_run.load_reviewed_phase_decision(
        project_root
        / "asic/config/harmonization/reviewed_phase_decisions_0_1.yaml"
    )
    monkeypatch.setattr(dry_run, "load_harmonization_dry_run_policy", lambda _: policy)
    monkeypatch.setattr(
        dry_run,
        "_load_and_validate_evidence",
        lambda *_: (
            occurrences,
            inventory,
            SimpleNamespace(
                provenance_columns={
                    "filename_stay_id": "__v3_filename_stay_id",
                    "in_file_stay_id": "__v3_in_file_stay_id",
                }
            ),
            schema_policy,
            candidate_contract,
            composite,
            static_registry,
            icd10,
            private_icd,
            phase_decision,
            {},
        ),
    )

    result = dry_run.run_harmonization_dry_run(
        config, "registry", "audit", run_id="candidate001"
    )

    assert result.overall_status == "fail"
    assert result.has_technical_failure is False
    assert result.candidate_directory.name == "candidate001"
    static = pq.read_table(result.candidate_directory / "asic_UK00/static.parquet")
    dynamic = pq.read_table(result.candidate_directory / "asic_UK00/dynamic.parquet")
    assert static["stay_id_local"].to_pylist() == ["42"]
    assert static["stay_id_global"].to_pylist() == ["42:0"]
    assert static["hospital_id"].to_pylist() == ["asic_UK00"]
    assert static["cluster_id"].to_pylist() == ["C1"]
    assert static["height_measurements_cm"].to_pylist() == [[170.0, 175.0]]
    assert static["icd10_codes_source_text"].to_pylist() == ["A01, A01,B02"]
    assert static["icd10_codes"].to_pylist() == [["A01", "B02"]]
    assert dynamic["therapy_read_confirmation_utc"].to_pylist() == [
        [True, False],
        [None, True],
    ]
    assert dynamic["stay_id_local"].to_pylist() == ["42", "42"]
    assert dynamic["stay_id_global"].to_pylist() == ["42:0", "42:0"]
    assert dynamic["ph_art"].to_pylist() == [7.4, 7.3]
    assert dynamic["vt_per_ideal_bw_total"].to_pylist() == [450.0, 500.0]
    assert (
        dynamic.schema.field("vt_per_ideal_bw_total").metadata[b"candidate_unit"]
        == b"mL"
    )
    assert dynamic["ntprobnp"].null_count == 2
    assert dynamic["__v3_source_row_number"].to_pylist() == [1, 2]
    expected_dynamic_schema = pq.read_schema(
        result.candidate_directory / "asic_UK00/dynamic.parquet"
    )
    for hospital in hospitals[1:]:
        assert pq.read_schema(
            result.candidate_directory / f"{hospital}/dynamic.parquet"
        ) == expected_dynamic_schema
    uk02 = pq.read_table(
        result.candidate_directory / "asic_UK02/dynamic.parquet"
    )
    assert uk02["ntprobnp"].to_pylist() == [5.0, 6.0]
    uk03_static = pq.read_table(
        result.candidate_directory / "asic_UK03/static.parquet"
    )
    assert uk03_static["study_implementation_phase"].to_pylist() == [
        "calibration"
    ]
    run_manifest = json.loads(
        (result.candidate_directory / "candidate_run_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_manifest["publication_ready"] is False
    assert run_manifest["cleaning_allowed"] is False
    assert run_manifest["derivation_allowed"] is False
    dictionary = pq.read_table(
        result.private_report_directory / "candidate_variable_dictionary.parquet"
    )
    assert "heart_rate" in dictionary["variable"].to_pylist()
    assert "vt_per_ideal_bw_total" in dictionary["variable"].to_pylist()
    assert not (data_root / "cleaned").exists()
    assert not (data_root / "derived").exists()

    dry_policy_file = tmp_path / "dry_run.yaml"
    quality_file = tmp_path / "quality.yaml"
    consolidated_policy_file = tmp_path / "consolidated.yaml"
    dry_policy_file.write_text("synthetic: true\n", encoding="utf-8")
    consolidated_policy_file.write_text("synthetic: true\n", encoding="utf-8")
    quality_file.write_text(
        "legacy_static_minus_one_sentinels: []\n"
        "candidate_context_audit:\n"
        "  windows_hours: [1]\n"
        "  fields: []\n",
        encoding="utf-8",
    )
    consolidated_policy = consolidated_audit.ConsolidatedAuditPolicy(
        version="0.2",
        dry_run_policy_path=dry_policy_file,
        dry_run_policy_sha256=sha256_file(dry_policy_file),
        candidate_artifact_version="0.3",
        candidate_private_artifact_version="0.3",
        candidate_review_artifact_version="0.3",
        candidate_status="candidate_nonpublishable_requires_consolidated_review",
        hospital_count=8,
        hospital_suffixes=tuple(
            (hospital, str(int(hospital[-2:]))) for hospital in hospitals
        ),
        comparison_reference_path=None,
        comparison_reference_sha256=None,
        quality_policy_path=quality_file,
        quality_policy_sha256=sha256_file(quality_file),
        rows_per_batch=1,
        sample_capacity=100,
        sample_rows_per_batch=1,
        maximum_examples=100,
        categorical_domain_cap=100,
        minimum_distribution_count=1,
        minimum_distribution_hospitals=2,
        iqr_fence_factor=1.5,
        general_scale_factors=(0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        peer_minimum_improvement_factor=5.0,
        peer_maximum_residual_factor=2.0,
        row_scale_factors=(0.001, 0.01, 0.1, 10.0, 100.0, 1000.0),
        private_artifact_version="0.1",
        review_artifact_version="0.1",
        private_directory_name="consolidated_harmonization_audit",
        review_directory_name="consolidated_harmonization_audit",
        source_path=consolidated_policy_file,
    )
    reference_quality = {
        "legacy_invalid_value_rules": [
            {
                "legacy_name": "delta_p",
                "columns": ["delta_p_computed"],
                "hard_min": 0,
                "hard_max": 60,
            }
        ],
        "targeted_scale_audits": [],
        "row_level_scale_entry_audits": [],
        "targeted_comparison_audits": [],
        "targeted_bucket_audits": [],
        "relationship_audits": [],
        "availability_audits": [],
        "known_legacy_semantic_findings": [],
        "predicted_body_weight_tidal_volume_audit": None,
        "candidate_context_audit": {"windows_hours": [1], "fields": []},
        "legacy_static_minus_one_sentinels": [],
    }
    monkeypatch.setattr(
        consolidated_audit,
        "load_consolidated_audit_policy",
        lambda _: consolidated_policy,
    )
    monkeypatch.setattr(
        consolidated_audit,
        "load_harmonization_dry_run_policy",
        lambda _: policy,
    )
    monkeypatch.setattr(
        consolidated_audit,
        "_quality_rules",
        lambda _: reference_quality,
    )
    consolidated_config = SimpleNamespace(
        policy_path=consolidated_policy_file,
        dataset_context="demo",
        data_root=data_root,
        ingested_root=ingested_root,
        reports_root=reports_root,
    )
    consolidated_result = (
        consolidated_audit.run_consolidated_harmonization_audit(
            consolidated_config,
            "candidate001",
            run_id="audit001",
        )
    )

    consolidated_review = json.loads(
        consolidated_result.review_json_path.read_text(encoding="utf-8")
    )
    assert consolidated_result.overall_status == "fail"
    assert consolidated_result.has_technical_failure is False, (
        consolidated_result.technical_blocking_findings,
        consolidated_review["metrics"],
    )
    assert (
        consolidated_review["unit_policy_summary"][
            "unit_policy_rows_checked"
        ]
        == consolidated_review["unit_policy_summary"]["unit_policy_variable_count"]
    )
    assert (
        consolidated_review["metrics"]["technical_blocking_finding_count"]
        == 0
    )
    assert consolidated_review["metrics"]["decision_register_row_count"] > 0
    assert consolidated_review["optional_old_version_comparison"] == {
        "status": "skipped_not_configured",
        "authoritative_for_v3": False,
        "required_for_v3": False,
    }
    assert consolidated_review["publication"]["candidate_mutated"] is False
    private_files = {
        path.name for path in consolidated_result.private_report_directory.iterdir()
    }
    assert "unit_review.parquet" in private_files
    assert "decision_register.parquet" in private_files
    assert "legacy_invalid_and_scale_entry.parquet" in private_files
    assert "row_scale_entry_audits.parquet" in private_files
    assert "old_version_reference_summaries.parquet" in private_files
    invalid_rules = pq.read_table(
        consolidated_result.private_report_directory
        / "legacy_invalid_and_scale_entry.parquet"
    ).to_pylist()
    assert [
        row["variable"]
        for row in invalid_rules
        if row.get("status")
        == "candidate_variable_unavailable_in_v3_union_schema"
    ] == ["delta_p_computed"]
    decisions = pq.read_table(
        consolidated_result.private_report_directory / "decision_register.parquet"
    ).to_pylist()
    decision_ids = {row["decision_id"] for row in decisions}
    assert "FREEZE-ORDERED-UNION-SCHEMAS" in decision_ids
    assert "FREEZE-VARIABLE-DICTIONARY" in decision_ids
    assert any(value.startswith("CANDIDATE-") for value in decision_ids)
    assert not (data_root / "harmonized").exists()
    assert not (data_root / "cleaned").exists()
    assert not (data_root / "derived").exists()
