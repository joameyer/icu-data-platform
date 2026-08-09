from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from interoperability_catalog.contract import (
    CatalogPaths,
    canonicalize_numeric_source,
    load_conversion_registry,
    validate_catalog,
    validate_no_analysis_runtime_dependency,
)
from interoperability_catalog.render import render_alignment_table


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_complete_core_derived_inventory_and_contract(project_root: Path) -> None:
    metrics = validate_catalog(CatalogPaths(project_root))
    assert metrics == {
        "concept_count": 184,
        "mapping_count": 73,
        "conversion_count": 34,
        "static_field_count": 37,
        "dynamic_field_count": 147,
    }
    concepts = _rows(project_root / "interoperability/config/clinical_concepts_0_1.csv")
    assert all(row["mimic_overlap_disposition"] for row in concepts)
    assert {row["source_core_derived_release"] for row in concepts} == {"20260808T074305Z"}


def test_unknown_units_fail_closed_and_source_evidence_is_preserved(project_root: Path) -> None:
    mappings = _rows(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    albumin = next(row for row in mappings if row["mapping_id"].startswith("map.dynamic_albumin"))
    conversions = load_conversion_registry(project_root / "interoperability/config/unit_conversions_0_1.yaml")
    result = canonicalize_numeric_source(
        source_value=4.0,
        source_unit="unknown",
        mapping=albumin,
        conversions=conversions,
        minimum=200,
        maximum=500,
    )
    assert result["source_value"] == 4.0
    assert result["source_unit"] == "unknown"
    assert result["canonical_value"] is None
    assert result["conversion_status"] == "unknown_or_unaccepted_source_unit"
    assert result["plausibility_status"] == "not_checked"
    assert result["eligibility_status"] == "ineligible"


def test_conversion_precedes_canonical_plausibility(project_root: Path) -> None:
    mappings = _rows(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    albumin = next(row for row in mappings if row["mapping_id"].startswith("map.dynamic_albumin"))
    conversions = load_conversion_registry(project_root / "interoperability/config/unit_conversions_0_1.yaml")
    result = canonicalize_numeric_source(
        source_value=4.0,
        source_unit="g/dL",
        mapping=albumin,
        conversions=conversions,
        minimum=200,
        maximum=500,
    )
    assert result["canonical_value"] == 400.0
    assert result["conversion_status"] == "converted"
    assert result["plausibility_status"] == "accepted"
    assert result["eligibility_status"] == "eligible"


def test_affine_scale_and_offset_and_round_trip_are_exercised(project_root: Path) -> None:
    mappings = _rows(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    fahrenheit = next(row for row in mappings if row["conversion_id"] == "fahrenheit_to_celsius")
    conversions = load_conversion_registry(project_root / "interoperability/config/unit_conversions_0_1.yaml")
    result = canonicalize_numeric_source(
        source_value=98.6,
        source_unit="degF",
        mapping=fahrenheit,
        conversions=conversions,
        minimum=25,
        maximum=45,
    )
    assert result["canonical_value"] == pytest.approx(37.0)
    conversion = conversions["fahrenheit_to_celsius"]
    assert conversion["source_to_canonical_scale"] == pytest.approx(5 / 9)
    assert conversion["source_to_canonical_offset"] == pytest.approx(-160 / 9)


def test_arterial_mappings_remain_human_gated(project_root: Path) -> None:
    mappings = _rows(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    arterial = [
        row for row in mappings
        if "same-specimen" in row["measurement_method_or_specimen"].lower()
    ]
    assert arterial
    assert all(row["eligibility"] != "enabled" for row in arterial)
    assert all("specimen" in row["semantic_caveat"].lower() for row in arterial)


def test_independent_draft_precedes_legacy_comparison(project_root: Path) -> None:
    independent = json.loads(
        (project_root / "interoperability/config/independent_draft_manifest_0_1.json").read_text(encoding="utf-8")
    )
    comparison = json.loads(
        (project_root / "interoperability/config/legacy_comparison_manifest_0_1.json").read_text(encoding="utf-8")
    )
    assert independent["legacy_input_read"] is False
    assert comparison["independent_draft_combined_sha256"] == independent["combined_sha256"]
    assert comparison["legacy_sha256"] == "a116d58c09299e27670afd33b3989c4c5f7253987bc3ab44a9383b98d018d3e7"
    assert comparison["automatic_catalog_changes_from_legacy"] == 0


def test_generated_review_table_has_no_patient_examples(project_root: Path) -> None:
    rendered = render_alignment_table(project_root)
    assert "Generated ASIC--MIMIC alignment table" in rendered
    assert "subject_id=" not in rendered
    assert "hadm_id=" not in rendered
    assert "stay_id=" not in rendered
    assert "/hpcwork/" not in rendered


def test_no_analysis_repository_runtime_dependency(project_root: Path) -> None:
    validate_no_analysis_runtime_dependency(project_root)
