from __future__ import annotations

import ast
import csv
from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

import yaml


DISPOSITIONS = {
    "aligned",
    "conditionally_aligned",
    "unresolved_blocking",
    "related_but_not_equivalent",
    "no_mimic_equivalent",
    "operational_or_nonclinical_not_in_scope",
}

CONCEPT_COLUMNS = {
    "asic_field_id",
    "canonical_concept_id",
    "asic_core_derived_variable",
    "asic_table",
    "core_derived_position",
    "clinical_family",
    "clinical_definition",
    "asic_physical_type",
    "asic_unit",
    "asic_analysis_eligibility",
    "asic_definition_status",
    "asic_caveats",
    "asic_evidence_confidence",
    "asic_unit_evidence_basis",
    "mimic_overlap_disposition",
    "preferred_shared_canonical_unit",
    "remaining_semantic_gate",
    "source_dictionary_contract",
    "source_core_derived_contract",
    "source_core_derived_release",
    "source_metadata_sha256",
    "catalog_review_status",
}

MAPPING_COLUMNS = {
    "mapping_id",
    "canonical_concept_id",
    "mimic_source_table",
    "mimic_source_field",
    "dictionary_table",
    "item_id",
    "dictionary_label",
    "category",
    "linksto",
    "measurement_method_or_specimen",
    "accepted_source_unit",
    "source_value_type",
    "shared_canonical_unit",
    "conversion_id",
    "source_precedence",
    "duplicate_resolution_policy",
    "plausibility_policy_reference",
    "current_profile_membership",
    "current_profile_variable",
    "eligibility",
    "semantic_caveat",
    "evidence_source",
    "review_status",
}


class AlignmentContractError(ValueError):
    """Raised when the catalog violates a fail-closed contract."""


@dataclass(frozen=True)
class CatalogPaths:
    root: Path

    @property
    def concepts(self) -> Path:
        return self.root / "interoperability/config/clinical_concepts_0_1.csv"

    @property
    def mappings(self) -> Path:
        return self.root / "interoperability/config/mimic_source_mappings_0_1.csv"

    @property
    def conversions(self) -> Path:
        return self.root / "interoperability/config/unit_conversions_0_1.yaml"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AlignmentContractError(f"YAML root must be a mapping: {path}")
    return value


def load_conversion_registry(path: Path) -> dict[str, dict[str, Any]]:
    raw = _read_yaml(path)
    conversions = raw.get("conversions")
    if not isinstance(conversions, dict) or not conversions:
        raise AlignmentContractError("Conversion registry must contain conversions")
    return conversions


def _assert_columns(rows: list[dict[str, str]], required: set[str], label: str) -> None:
    if not rows:
        raise AlignmentContractError(f"{label} is empty")
    missing = required.difference(rows[0])
    if missing:
        raise AlignmentContractError(f"{label} missing columns: {sorted(missing)}")


def _numeric_conversion(conversion: dict[str, Any], value: float) -> float:
    kind = conversion.get("conversion_kind")
    if kind not in {"identity", "scale", "affine", "molar"}:
        raise AlignmentContractError(
            f"Conversion {conversion.get('conversion_id')} is not executable without context"
        )
    scale = conversion.get("source_to_canonical_scale")
    offset = conversion.get("source_to_canonical_offset")
    if not isinstance(scale, (int, float)) or not isinstance(offset, (int, float)):
        raise AlignmentContractError("Executable conversion lacks numeric scale/offset")
    converted = float(value) * float(scale) + float(offset)
    if not math.isfinite(converted):
        raise AlignmentContractError("Conversion produced a non-finite result")
    return converted


def canonicalize_numeric_source(
    *,
    source_value: object,
    source_unit: str | None,
    mapping: dict[str, str],
    conversions: dict[str, dict[str, Any]],
    minimum: float | None = None,
    maximum: float | None = None,
) -> dict[str, Any]:
    """Preserve source evidence and convert before checking canonical bounds.

    Unknown or non-exact unit spellings never fall back to a label, item ID, or
    data type. Contextual and unavailable conversions remain ineligible.
    """

    output: dict[str, Any] = {
        "source_value": source_value,
        "source_unit": source_unit,
        "canonical_value": None,
        "shared_canonical_unit": mapping.get("shared_canonical_unit"),
        "conversion_id": mapping.get("conversion_id"),
        "conversion_status": "not_attempted",
        "plausibility_status": "not_checked",
        "eligibility_status": "ineligible",
    }
    accepted = mapping.get("accepted_source_unit")
    if not source_unit or source_unit != accepted:
        output["conversion_status"] = "unknown_or_unaccepted_source_unit"
        return output
    conversion_id = mapping.get("conversion_id", "")
    conversion = conversions.get(conversion_id)
    if conversion is None:
        output["conversion_status"] = "unregistered_conversion"
        return output
    if conversion.get("accepted_source_unit") != accepted:
        output["conversion_status"] = "conversion_unit_contract_mismatch"
        return output
    try:
        numeric = float(source_value)
    except (TypeError, ValueError):
        output["conversion_status"] = "non_numeric_source_value"
        return output
    if not math.isfinite(numeric):
        output["conversion_status"] = "nonfinite_source_value"
        return output
    try:
        canonical = _numeric_conversion(conversion, numeric)
    except AlignmentContractError:
        output["conversion_status"] = "context_required_or_unavailable"
        return output
    output["conversion_status"] = "converted"
    output["canonical_value"] = canonical
    plausible = (minimum is None or canonical >= minimum) and (
        maximum is None or canonical <= maximum
    )
    output["plausibility_status"] = "accepted" if plausible else "rejected"
    if plausible and mapping.get("eligibility") == "enabled":
        output["eligibility_status"] = "eligible"
    return output


def inverse_numeric_value(conversion: dict[str, Any], canonical_value: float) -> float:
    if not conversion.get("round_trip_support"):
        raise AlignmentContractError("Conversion does not support an inverse")
    scale = conversion.get("source_to_canonical_scale")
    offset = conversion.get("source_to_canonical_offset")
    if not isinstance(scale, (int, float)) or float(scale) == 0:
        raise AlignmentContractError("Conversion has no invertible numeric scale")
    if not isinstance(offset, (int, float)):
        raise AlignmentContractError("Conversion has no numeric offset")
    return (float(canonical_value) - float(offset)) / float(scale)


def validate_catalog(paths: CatalogPaths) -> dict[str, int]:
    concepts = _read_csv(paths.concepts)
    mappings = _read_csv(paths.mappings)
    conversions = load_conversion_registry(paths.conversions)
    _assert_columns(concepts, CONCEPT_COLUMNS, "concept registry")
    _assert_columns(mappings, MAPPING_COLUMNS, "mapping registry")

    field_ids = [row["asic_field_id"] for row in concepts]
    if len(field_ids) != len(set(field_ids)):
        raise AlignmentContractError("ASIC field IDs are not unique")
    if len(concepts) != 184:
        raise AlignmentContractError(f"Expected 184 core-derived fields, found {len(concepts)}")
    table_counts = {
        table: sum(row["asic_table"] == table for row in concepts)
        for table in ("static", "dynamic")
    }
    if table_counts != {"static": 37, "dynamic": 147}:
        raise AlignmentContractError(f"Core-derived table coverage changed: {table_counts}")
    invalid = {
        row["mimic_overlap_disposition"]
        for row in concepts
        if row["mimic_overlap_disposition"] not in DISPOSITIONS
    }
    if invalid:
        raise AlignmentContractError(f"Invalid dispositions: {sorted(invalid)}")
    for row in concepts:
        if not row["mimic_overlap_disposition"]:
            raise AlignmentContractError(f"Missing disposition: {row['asic_field_id']}")
        if not row["source_metadata_sha256"]:
            raise AlignmentContractError(f"Missing source hash: {row['asic_field_id']}")

    concept_units = {
        row["canonical_concept_id"]: row["preferred_shared_canonical_unit"]
        for row in concepts
    }
    if len(concept_units) != len(concepts):
        raise AlignmentContractError("Canonical concept IDs must be unique per ASIC occurrence")
    mapping_ids = [row["mapping_id"] for row in mappings]
    if len(mapping_ids) != len(set(mapping_ids)):
        raise AlignmentContractError("Mapping IDs are not unique")
    mapping_keys = [
        (
            row["canonical_concept_id"], row["mimic_source_table"],
            row["mimic_source_field"], row["item_id"],
            row["accepted_source_unit"],
        )
        for row in mappings
    ]
    if len(mapping_keys) != len(set(mapping_keys)):
        raise AlignmentContractError("Duplicate concept/table/item/unit mapping")

    for row in mappings:
        concept_id = row["canonical_concept_id"]
        if concept_id not in concept_units:
            raise AlignmentContractError(f"Unknown concept in mapping: {concept_id}")
        if not row["accepted_source_unit"]:
            raise AlignmentContractError(f"Accepted unit is not explicit: {row['mapping_id']}")
        if row["shared_canonical_unit"] != concept_units[concept_id]:
            raise AlignmentContractError(f"Canonical unit mismatch: {row['mapping_id']}")
        conversion_id = row["conversion_id"]
        conversion = conversions.get(conversion_id)
        if conversion is None:
            raise AlignmentContractError(f"Unregistered conversion: {conversion_id}")
        if conversion.get("accepted_source_unit") != row["accepted_source_unit"]:
            raise AlignmentContractError(f"Source-unit conversion mismatch: {row['mapping_id']}")
        if conversion.get("shared_canonical_unit") != row["shared_canonical_unit"]:
            raise AlignmentContractError(f"Target-unit conversion mismatch: {row['mapping_id']}")
        table = row["mimic_source_table"]
        dictionary = row["dictionary_table"]
        if table == "labevents" and dictionary != "d_labitems":
            raise AlignmentContractError(f"Lab mapping lacks d_labitems: {row['mapping_id']}")
        if table in {"chartevents", "inputevents", "outputevents", "procedureevents"} and dictionary != "d_items":
            raise AlignmentContractError(f"ICU event mapping lacks d_items: {row['mapping_id']}")
        if table in {"patients", "admissions", "icustays", "services", "diagnoses_icd"}:
            if dictionary != "not_applicable" or row["item_id"] != "not_applicable":
                raise AlignmentContractError(f"Direct mapping incorrectly uses dictionary: {row['mapping_id']}")
        if "arterial" in row["measurement_method_or_specimen"].lower():
            if row["review_status"] == "reviewed_enabled":
                raise AlignmentContractError(
                    f"Arterial mapping cannot be enabled before specimen review: {row['mapping_id']}"
                )

    for conversion_id, conversion in conversions.items():
        if conversion.get("conversion_id") != conversion_id:
            raise AlignmentContractError(f"Conversion ID/key mismatch: {conversion_id}")
        examples = conversion.get("test_examples", [])
        if conversion.get("conversion_kind") in {"identity", "scale", "affine", "molar"}:
            if not examples:
                raise AlignmentContractError(f"Executable conversion has no tests: {conversion_id}")
            for example in examples:
                observed = _numeric_conversion(conversion, float(example["source_value"]))
                expected = float(example["canonical_value"])
                tolerance = float(example.get("absolute_tolerance", 1e-9))
                if not math.isclose(observed, expected, rel_tol=0.0, abs_tol=tolerance):
                    raise AlignmentContractError(f"Conversion test failed: {conversion_id}")
                if conversion.get("round_trip_support"):
                    recovered = inverse_numeric_value(conversion, observed)
                    if not math.isclose(
                        recovered, float(example["source_value"]), rel_tol=0.0,
                        abs_tol=max(tolerance, 1e-9),
                    ):
                        raise AlignmentContractError(f"Round trip failed: {conversion_id}")

    return {
        "concept_count": len(concepts),
        "mapping_count": len(mappings),
        "conversion_count": len(conversions),
        "static_field_count": table_counts["static"],
        "dynamic_field_count": table_counts["dynamic"],
    }


def validate_no_analysis_runtime_dependency(project_root: Path) -> None:
    """Reject Python imports of analysis repositories/packages."""

    source_root = project_root / "src/interoperability_catalog"
    forbidden = {"phase_aware_icu_mortality_prediction", "icu_mortality_last72h"}
    for path in source_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            if any(name.split(".")[0] in forbidden for name in names):
                raise AlignmentContractError(f"Analysis runtime dependency in {path}")
