from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import yaml


LEGACY_TO_FIELD = {
    "hr": "dynamic.heart_rate",
    "map": "dynamic.map",
    "rr": "dynamic.resp_rate",
    "sao": "dynamic.sao2",
    "tem": "dynamic.core_temp",
    "flb": "dynamic.fluid_balance_24h",
    "alb": "dynamic.albumin",
    "ast": "dynamic.ast",
    "bil": "dynamic.bilirubin_total",
    "ck": "dynamic.ck",
    "cre": "dynamic.creatinine",
    "hem": "dynamic.hemoglobin",
    "hto": "dynamic.hematocrit",
    "inr": "dynamic.inr",
    "lac": "dynamic.lactate_art",
    "ldh": "dynamic.ldh",
    "lip": "dynamic.lipase",
    "oxy": "dynamic.pao2",
    "pco": "dynamic.paco2",
    "pha": "dynamic.ph_art",
    "pla": "dynamic.platelets",
    "ure": "dynamic.urea",
    "wbc": "dynamic.wbc",
    "fio": "dynamic.fio2_set",
    "peep": "dynamic.peep_set",
    "vt": "dynamic.vt",
    "gender": "static.sex",
    "age": "static.age_group",
    "weight": "static.weight_kg",
    "height": "static.height_measurements_cm",
    "bmi": "static.bmi_group",
    "icd_codes": "static.icd10_codes",
    "readmission": "static.icu_readmit",
    "in_hospital_mortality": "static.hospital_mortality",
}

SPECIAL_COMPARISONS = {
    "sao": (
        "disagreement_semantic_conflation",
        "Legacy calls one field arterial oxygen saturation and SpO2. The new catalog keeps pulse-oximetry SpO2, arterial SaO2, and ScvO2 distinct.",
    ),
    "flb": (
        "unresolved_not_ground_truth",
        "Legacy labels a 24-hour balance but does not establish rolling-window boundaries, included inputs/outputs, or missing-side behavior.",
    ),
    "inr": (
        "legacy_incomplete_unit",
        "Legacy units are blank. The new catalog requires an explicit MIMIC source-unit spelling and dimensionless-ratio target.",
    ),
    "ure": (
        "unresolved_quantity_conversion",
        "Legacy treats urea/BUN as interchangeable and rounds the inverse factor to 2.8; the new draft uses 2.801 but keeps the BUN-to-urea quantity change human-gated.",
    ),
    "fio": (
        "agreement_with_stricter_context_gate",
        "Both identify a ventilator setting, but MIMIC values can use fraction or percent representations and require the reviewed contextual rule.",
    ),
    "peep": (
        "agreement_with_naming_correction",
        "Legacy identifies a setting; the independent catalog maps the current items to peep_set rather than silently treating them as an unspecified PEEP measurement.",
    ),
    "vt": (
        "disagreement_requires_human_definition",
        "Legacy says tidal-volume setting, while the frozen ASIC dictionary does not resolve vt semantics and the independent draft prefers observed VT for item 224685; item 224684 remains separately ineligible setting evidence.",
    ),
    "bmi": (
        "related_representation_not_equivalent",
        "Legacy expects BMI while ASIC core-derived 0.2 contains bmi_group, not a continuous BMI field.",
    ),
    "icd_codes": (
        "disagreement_scope",
        "Legacy groups ICD-9 and ICD-10; the ASIC field is explicitly ICD-10 and the catalog prohibits silent ICD-9 translation.",
    ),
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _inverse_scale_agrees(legacy_factor: str, conversion: dict[str, Any]) -> bool:
    try:
        factor = float(legacy_factor)
        scale = float(conversion["source_to_canonical_scale"])
    except (TypeError, ValueError, KeyError):
        return False
    return scale != 0 and math.isclose(factor, 1.0 / scale, rel_tol=0.006, abs_tol=1e-12)


def compare_legacy(*, project_root: Path, legacy_csv: Path) -> list[dict[str, str]]:
    manifest_path = project_root / "interoperability/config/independent_draft_manifest_0_1.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("legacy_input_read") is not False or not manifest.get("combined_sha256"):
        raise ValueError("Independent pre-legacy draft manifest is absent or invalid")
    concepts = _read_csv(project_root / "interoperability/config/clinical_concepts_0_1.csv")
    mappings = _read_csv(project_root / "interoperability/config/mimic_source_mappings_0_1.csv")
    conversions = yaml.safe_load(
        (project_root / "interoperability/config/unit_conversions_0_1.yaml").read_text(encoding="utf-8")
    )["conversions"]
    concept_by_field = {row["asic_field_id"]: row for row in concepts}
    mappings_by_concept: dict[str, list[dict[str, str]]] = {}
    for row in mappings:
        mappings_by_concept.setdefault(row["canonical_concept_id"], []).append(row)

    output: list[dict[str, str]] = []
    for legacy in _read_csv(legacy_csv):
        legacy_name = legacy["variable"].strip()
        field_id = LEGACY_TO_FIELD.get(legacy_name, "not_mapped")
        concept = concept_by_field.get(field_id)
        if concept is None:
            outcome, note = "legacy_only", "No ASIC core-derived 0.2 occurrence was independently matched."
            mapping_rows: list[dict[str, str]] = []
        else:
            mapping_rows = mappings_by_concept.get(concept["canonical_concept_id"], [])
            if legacy_name in SPECIAL_COMPARISONS:
                outcome, note = SPECIAL_COMPARISONS[legacy_name]
            elif not legacy.get("unit_mimic") and not legacy.get("unit_asic"):
                outcome, note = "semantic_agreement_units_not_compared", "Legacy did not provide physical units for this field."
            elif any(_inverse_scale_agrees(legacy.get("factor_asic_to_mimic", ""), conversions.get(row["conversion_id"], {})) for row in mapping_rows):
                outcome, note = "agreement_direction_reversed", "Legacy ASIC-to-MIMIC factor agrees within tolerance with the inverse of a new MIMIC-to-canonical conversion."
            elif legacy.get("factor_asic_to_mimic") == "1.0" and any(
                conversions.get(row["conversion_id"], {}).get("source_to_canonical_scale") == 1.0
                for row in mapping_rows
            ):
                outcome, note = "agreement_identity", "Identity conversion agrees; new catalog additionally binds item, source unit, method, and eligibility."
            else:
                outcome, note = "not_comparable_or_requires_review", "The legacy row lacks enough authoritative item/unit semantics for automatic reconciliation."
        output.append({
            "legacy_variable": legacy_name,
            "legacy_description": legacy.get("description", "").strip(),
            "new_asic_field_id": field_id,
            "new_canonical_concept_id": concept["canonical_concept_id"] if concept else "not_applicable",
            "new_overlap_disposition": concept["mimic_overlap_disposition"] if concept else "not_applicable",
            "legacy_mimic_unit": legacy.get("unit_mimic", ""),
            "legacy_asic_unit": legacy.get("unit_asic", ""),
            "new_shared_canonical_unit": concept["preferred_shared_canonical_unit"] if concept else "not_applicable",
            "legacy_asic_to_mimic_factor": legacy.get("factor_asic_to_mimic", ""),
            "new_mimic_to_canonical_conversion_ids": ";".join(sorted({row["conversion_id"] for row in mapping_rows})) or "none",
            "comparison_outcome": outcome,
            "resolution_note": note,
            "legacy_locf_window_not_adopted": legacy.get("locf_window", ""),
        })
    return output


def write_comparison(*, project_root: Path, legacy_csv: Path) -> Path:
    rows = compare_legacy(project_root=project_root, legacy_csv=legacy_csv)
    output = project_root / "interoperability/config/legacy_variable_configuration_comparison_0_1.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    evidence = {
        "artifact": "legacy_alignment_regression_comparison",
        "artifact_version": "0.1",
        "legacy_sha256": hashlib.sha256(legacy_csv.read_bytes()).hexdigest(),
        "independent_draft_combined_sha256": json.loads(
            (project_root / "interoperability/config/independent_draft_manifest_0_1.json").read_text(encoding="utf-8")
        )["combined_sha256"],
        "comparison_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "comparison_row_count": len(rows),
        "automatic_catalog_changes_from_legacy": 0,
        "legacy_runtime_dependency": False,
    }
    evidence_path = project_root / "interoperability/config/legacy_comparison_manifest_0_1.json"
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare a frozen independent alignment draft with legacy regression evidence")
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--legacy-csv", type=Path, required=True)
    args = parser.parse_args()
    print(write_comparison(project_root=args.project_root.resolve(), legacy_csv=args.legacy_csv.resolve()))


if __name__ == "__main__":
    main()
