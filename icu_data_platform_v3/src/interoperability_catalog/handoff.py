from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import yaml

from interoperability_catalog.contract import AlignmentContractError, CatalogPaths, validate_catalog


RUN_ID = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
ANALYSIS_EXCLUDED_NONPREDICTORS = {
    "fio2",
    "fio2_set",
    "peep",
    "peep_set",
    "vt",
    "vt_spontaneous",
    "vt_per_ideal_bw_total",
    "vt_per_kg_ideal_body_weight",
}


@dataclass(frozen=True)
class HandoffExportResult:
    output_directory: Path
    handoff_path: Path
    export_audit_path: Path
    review_status: str
    handoff_sha256: str
    unresolved_analysis_concepts: tuple[str, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AlignmentContractError(f"YAML must contain a mapping: {path}")
    return value


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AlignmentContractError(f"Unreadable {label}") from exc
    if not isinstance(value, dict):
        raise AlignmentContractError(f"{label} must contain a JSON object")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _split(value: str) -> list[str]:
    return sorted({part for part in value.split(";") if part})


def _validate_projection(
    *,
    project_root: Path,
    contract: dict[str, Any],
    projection: list[dict[str, str]],
    concepts: list[dict[str, str]],
    renal: dict[str, Any],
) -> None:
    expected = contract["platform_inputs"]
    if len(concepts) != expected["expected_catalog_concept_count"]:
        raise AlignmentContractError("Full platform concept inventory changed")
    if len(projection) != expected["expected_projection_concept_count"]:
        raise AlignmentContractError("Analysis projection must contain exactly 35 concepts")
    sources = [row["analysis_source_variable"] for row in projection]
    if len(sources) != len(set(sources)):
        raise AlignmentContractError("Analysis projection source variables are not unique")
    if ANALYSIS_EXCLUDED_NONPREDICTORS.intersection(sources):
        raise AlignmentContractError("Nonpredictor ventilation concepts entered the analysis projection")
    concept_by_id = {row["canonical_concept_id"]: row for row in concepts}
    field_by_id = {row["asic_field_id"]: row for row in concepts}
    for row in projection:
        concept = concept_by_id.get(row["platform_canonical_concept_id"])
        if concept is None or field_by_id.get(row["platform_asic_field_id"]) != concept:
            raise AlignmentContractError(
                f"Projection does not resolve to one platform concept: {row['analysis_source_variable']}"
            )
        if row["platform_shared_canonical_unit"] != concept["preferred_shared_canonical_unit"]:
            raise AlignmentContractError(
                f"Projection platform-unit binding changed: {row['analysis_source_variable']}"
            )
        if not _split(row["supported_operations"]):
            raise AlignmentContractError(
                f"Projection has no supported operation: {row['analysis_source_variable']}"
            )
    if renal.get("analysis_source_variable") != "crf":
        raise AlignmentContractError("Narrow renal registry no longer binds crf")
    if renal.get("intended_definition") != "chronic_dialysis_dependent_renal_failure":
        raise AlignmentContractError("Narrow renal definition changed")
    current_chronic = _read_yaml(
        project_root / "mimic/config/registries/chronic_comorbidity_icd10_0_1.yaml"
    )
    if current_chronic.get("phenotypes", {}).get("crf") != ["N18", "N19"]:
        raise AlignmentContractError("Current broad MIMIC CRF evidence changed; re-review required")
    if (
        renal.get("current_platform_conflict", {}).get("disposition_for_analysis_projection")
        != "ineligible_broad_definition"
    ):
        raise AlignmentContractError("Broad CRF registry is not explicitly ineligible")


def _validate_manifest(
    *, path: Path | None, expected: dict[str, Any], label: str
) -> tuple[str | None, list[str], dict[str, Any] | None]:
    if path is None:
        return None, [f"{label}_missing"], None
    value = _read_json(path, label)
    gates: list[str] = []
    for key in ("artifact", "artifact_version"):
        if value.get(key) != expected[key]:
            gates.append(f"{label}_{key}_mismatch")
    if label == "asic_release_manifest":
        for key in ("release_id", "core_derived_contract_version"):
            if value.get(key) != expected[key]:
                gates.append(f"{label}_{key}_mismatch")
    else:
        declared = value.get("source_release_evidence", {}).get("declared_release")
        if declared != expected["declared_release"]:
            gates.append(f"{label}_declared_release_mismatch")
    return _sha256(path), gates, value


def _stage_a_evidence(
    *, path: Path | None, run_id: str | None, expected: dict[str, Any]
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str]]:
    if path is None:
        return None, {"provided": False}, ["stage_a_cluster_evidence_missing"]
    value = _read_json(path, "Stage-A evidence")
    gates: list[str] = []
    for key in ("artifact", "artifact_version", "privacy"):
        if value.get(key) != expected[key]:
            gates.append(f"stage_a_{key}_mismatch")
    if run_id is None or RUN_ID.fullmatch(run_id) is None:
        gates.append("stage_a_run_id_missing_or_invalid")
    findings = value.get("findings")
    if not isinstance(findings, list):
        gates.append("stage_a_findings_schema_invalid")
        findings = []
    if findings:
        gates.append("stage_a_has_findings")
    lineage = {
        "provided": True,
        "run_id": run_id,
        "sha256": _sha256(path),
        "artifact": value.get("artifact"),
        "artifact_version": value.get("artifact_version"),
        "finding_count": len(findings),
        "evidence_status": "pass" if not gates else "incomplete",
    }
    return value, lineage, gates


def _stage_unit_evidence(
    *,
    platform_concept_id: str,
    mappings_by_concept: dict[str, list[dict[str, str]]],
    stage_a: dict[str, Any] | None,
) -> list[str]:
    mappings = [
        row
        for row in mappings_by_concept.get(platform_concept_id, [])
        if row["mimic_source_table"] in {"chartevents", "labevents"}
        and row["eligibility"] != "ineligible"
    ]
    if not mappings:
        return []
    if stage_a is None:
        return ["stage_a_item_unit_evidence_missing"]
    metadata = stage_a.get("dictionary_metadata")
    counts = stage_a.get("observed_item_unit_counts")
    if not isinstance(metadata, dict) or not isinstance(counts, dict):
        return ["stage_a_item_unit_evidence_schema_invalid"]
    dictionary_confirmed = False
    accepted_observation_count = 0
    for mapping in mappings:
        dictionary_rows = metadata.get(mapping["dictionary_table"], {})
        if isinstance(dictionary_rows, dict) and mapping["item_id"] in dictionary_rows:
            dictionary_confirmed = True
        table_counts = counts.get(mapping["mimic_source_table"], [])
        if not isinstance(table_counts, list):
            continue
        for evidence in table_counts:
            if not isinstance(evidence, dict):
                continue
            if (
                str(evidence.get("item_id")) == mapping["item_id"]
                and evidence.get("source_unit") == mapping["accepted_source_unit"]
            ):
                accepted_observation_count += int(evidence.get("observation_count", 0))
    gates: list[str] = []
    if not dictionary_confirmed:
        gates.append("mimic_dictionary_item_not_confirmed")
    if accepted_observation_count <= 0:
        gates.append("mimic_source_unit_not_confirmed")
    return gates


def _create_alignment_binding(
    project_root: Path, output_directory: Path, contract: dict[str, Any]
) -> tuple[Path, str]:
    inputs = contract["platform_inputs"]
    config_root = project_root / "interoperability/config"
    components = {
        "analysis_projection_registry": config_root / inputs["projection_registry"],
        "clinical_concept_registry": config_root / inputs["clinical_concept_registry"],
        "mimic_source_mapping_registry": config_root / inputs["mimic_source_mapping_registry"],
        "renal_definition_registry": config_root / inputs["renal_definition_registry"],
        "mimic_chronic_registry": project_root / "mimic/config/registries/chronic_comorbidity_icd10_0_1.yaml",
    }
    binding = {
        "artifact": "asic_mimic_analysis_alignment_registry_binding",
        "artifact_version": "0.1",
        "component_sha256": {name: _sha256(path) for name, path in sorted(components.items())},
        "catalog_concept_count": inputs["expected_catalog_concept_count"],
        "analysis_projection_concept_count": inputs["expected_projection_concept_count"],
        "analysis_repository_runtime_dependency": False,
    }
    output = output_directory / "alignment_registry_binding_manifest.json"
    _write_json(output, binding)
    return output, _sha256(output)


def _create_mimic_dictionary_binding(
    *, output_directory: Path, d_items: Path | None, d_labitems: Path | None
) -> tuple[Path | None, str | None, list[str]]:
    if d_items is None or d_labitems is None:
        return None, None, ["mimic_dictionary_inputs_missing"]
    binding = {
        "artifact": "mimic_iv_3_1_dictionary_hash_binding",
        "artifact_version": "0.1",
        "mimic_version": "3.1",
        "dictionary_sha256": {
            "d_items": _sha256(d_items),
            "d_labitems": _sha256(d_labitems),
        },
        "dictionary_rows_exported": False,
        "source_paths_or_filenames_included": False,
    }
    output = output_directory / "mimic_dictionary_binding_manifest.json"
    _write_json(output, binding)
    return output, _sha256(output), []


def export_analysis_alignment_handoff(
    *,
    project_root: Path,
    output_directory: Path,
    asic_release_manifest: Path | None = None,
    mimic_input_manifest: Path | None = None,
    mimic_d_items: Path | None = None,
    mimic_d_labitems: Path | None = None,
    stage_a_evidence: Path | None = None,
    stage_a_run_id: str | None = None,
) -> HandoffExportResult:
    """Export metadata only; no analysis repository or clinical row is read."""

    project_root = project_root.resolve()
    validate_catalog(CatalogPaths(project_root))
    config_root = project_root / "interoperability/config"
    contract = _read_yaml(config_root / "analysis_alignment_handoff_0_1.yaml")
    projection = _read_csv(config_root / "analysis_alignment_projection_0_1.csv")
    concepts = _read_csv(config_root / "clinical_concepts_0_1.csv")
    mappings = _read_csv(config_root / "mimic_source_mappings_0_1.csv")
    renal = _read_yaml(config_root / "chronic_dialysis_dependent_renal_failure_0_1.yaml")
    _validate_projection(
        project_root=project_root,
        contract=contract,
        projection=projection,
        concepts=concepts,
        renal=renal,
    )
    if output_directory.exists():
        raise AlignmentContractError("Handoff output directory already exists")
    output_directory.mkdir(parents=True, mode=0o700)

    alignment_binding_path, alignment_binding_sha = _create_alignment_binding(
        project_root, output_directory, contract
    )
    _, mimic_dictionary_sha, global_gates = _create_mimic_dictionary_binding(
        output_directory=output_directory,
        d_items=mimic_d_items,
        d_labitems=mimic_d_labitems,
    )
    external = contract["external_manifest_validation"]
    asic_manifest_sha, gates, _ = _validate_manifest(
        path=asic_release_manifest,
        expected=external["asic_release_manifest"],
        label="asic_release_manifest",
    )
    global_gates.extend(gates)
    mimic_manifest_sha, gates, _ = _validate_manifest(
        path=mimic_input_manifest,
        expected=external["mimic_input_manifest"],
        label="mimic_input_manifest",
    )
    global_gates.extend(gates)
    stage_a, stage_lineage, stage_gates = _stage_a_evidence(
        path=stage_a_evidence,
        run_id=stage_a_run_id,
        expected=external["stage_a_evidence"],
    )
    global_gates.extend(stage_gates)

    concepts_by_id = {row["canonical_concept_id"]: row for row in concepts}
    mappings_by_concept: dict[str, list[dict[str, str]]] = {}
    for row in mappings:
        mappings_by_concept.setdefault(row["canonical_concept_id"], []).append(row)
    output_concepts: list[dict[str, Any]] = []
    unresolved_analysis: list[dict[str, Any]] = []
    for row in projection:
        platform = concepts_by_id[row["platform_canonical_concept_id"]]
        unresolved = _split(row["base_unresolved_gates"])
        unresolved.extend(
            _stage_unit_evidence(
                platform_concept_id=row["platform_canonical_concept_id"],
                mappings_by_concept=mappings_by_concept,
                stage_a=stage_a,
            )
        )
        unresolved = sorted(set(unresolved))
        required_gates = _split(row["required_analysis_gates"])
        addressed = required_gates if not unresolved else []
        concept = {
            "analysis_source_variable": row["analysis_source_variable"],
            "datasets": contract["required_handoff_identity"]["datasets"],
            "physical_type": row["analysis_physical_type"],
            "canonical_unit": row["analysis_interface_unit"],
            "supported_operations": _split(row["supported_operations"]),
            "alignment_status": "blocked" if unresolved or global_gates else "review_ready",
            "unresolved_gates": unresolved,
            "analysis_gates_addressed": addressed,
            "canonicalization_owned_by_platform": True,
            "analysis_conversion_required": False,
            "platform_source_kind": row["platform_source_kind"],
            "platform_asic_field_id": row["platform_asic_field_id"],
            "platform_canonical_concept_id": row["platform_canonical_concept_id"],
            "platform_shared_canonical_unit": row["platform_shared_canonical_unit"],
            "analysis_unit_relation": row["unit_relation"],
            "platform_overlap_disposition": platform["mimic_overlap_disposition"],
            "semantic_caveat": row["semantic_caveat"],
        }
        output_concepts.append(concept)
        if unresolved:
            unresolved_analysis.append(
                {
                    "analysis_source_variable": row["analysis_source_variable"],
                    "unresolved_gates": unresolved,
                }
            )

    all_concept_gates_closed = not unresolved_analysis
    all_global_gates_closed = not global_gates
    review_ready = all_concept_gates_closed and all_global_gates_closed
    review_status = "pending_human_review" if review_ready else "incomplete"
    concept_status = "review_ready" if review_ready else "blocked"
    for concept in output_concepts:
        concept["alignment_status"] = concept_status

    registry_hashes: dict[str, str | None] = {
        "alignment_registry": alignment_binding_sha,
        "asic_variable_dictionary": concepts[0]["source_metadata_sha256"],
        "mimic_variable_dictionary": mimic_dictionary_sha,
        "unit_conversion_registry": _sha256(config_root / "unit_conversions_0_1.yaml"),
        "asic_release_manifest": asic_manifest_sha,
        "mimic_input_manifest": mimic_manifest_sha,
    }
    renal_candidates = renal["direct_dependency_status_code_candidates_evaluated"]
    renal_summary = {
        "analysis_source_variable": "crf",
        "intended_definition": "chronic_dialysis_dependent_renal_failure",
        "definition_decision_status": "human_approved",
        "mapping_review_status": "pending_human_review",
        "direct_dependency_status_code_candidates_evaluated": {
            source: details["codes"] for source, details in renal_candidates.items()
        },
        "broad_ckd_or_esrd_codes_used_without_dependency_evidence": False,
        "acute_dialysis_or_rrt_used_as_chronic_baseline_evidence": False,
        "post_admission_or_future_evidence_used_for_baseline_predictor": False,
        "current_broad_mimic_crf_prefixes": ["N18", "N19"],
        "current_broad_mimic_crf_prefixes_eligible_for_analysis_crf": False,
        "unresolved_gates": ["chronic_dialysis_dependency_source_mapping"],
    }
    identity = contract["required_handoff_identity"]
    handoff = {
        "artifact": identity["artifact"],
        "artifact_version": identity["artifact_version"],
        "review_status": review_status,
        "source_lineage": identity["source_lineage"],
        "registry_hashes": registry_hashes,
        "evidence_lineage": {"stage_a_cluster_evidence": stage_lineage},
        "catalog_projection": {
            "full_platform_concept_count": len(concepts),
            "analysis_source_concept_count": len(output_concepts),
            "general_catalog_preserved": True,
            "analysis_projection_expands_mimic_profile": False,
        },
        "concepts": output_concepts,
        "unresolved_handoff_gates": sorted(set(global_gates)),
        "unresolved_analysis_concepts": unresolved_analysis,
        "chronic_dialysis_dependent_renal_failure": renal_summary,
        "nonpredictor_catalog_scope": {
            "fio2_tidal_volume_medications_and_advanced_hemodynamics_retained_in_general_catalog": True,
            "included_in_35_concept_analysis_projection": False,
            "block_analysis_projection": False,
        },
        "consumer_interface_compatibility": {
            "consumer_requirement_version": "0.1",
            "compatible": review_status != "incomplete",
            "mismatch_id": contract["interface_mismatch"]["mismatch_id"] if review_status == "incomplete" else None,
            "smallest_consumer_adjustment": contract["interface_mismatch"]["smallest_consumer_adjustment"] if review_status == "incomplete" else None,
        },
        "privacy": contract["privacy"],
        "authorization": {
            "human_approved": False,
            "production_analysis_candidate": False,
            "model_training": False,
            "release_promotion": False,
            "pointer_modification": False,
            "external_export": False,
        },
    }
    handoff_path = output_directory / "analysis_alignment_handoff.json"
    _write_json(handoff_path, handoff)
    handoff_sha = _sha256(handoff_path)
    audit = {
        "artifact": "platform_analysis_alignment_handoff_export_audit",
        "artifact_version": "0.1",
        "status": review_status,
        "handoff_sha256": handoff_sha,
        "alignment_registry_binding_sha256": _sha256(alignment_binding_path),
        "full_platform_concept_count": len(concepts),
        "analysis_source_concept_count": len(output_concepts),
        "unresolved_handoff_gate_count": len(set(global_gates)),
        "unresolved_analysis_concept_count": len(unresolved_analysis),
        "human_approved": False,
        "clinical_data_accessed": False,
        "identifiers_or_patient_rows_exported": False,
        "analysis_repository_runtime_dependency": False,
        "mimic_profile_expanded": False,
        "release_created": False,
        "pointer_modified": False,
    }
    export_audit_path = output_directory / "export_audit.json"
    _write_json(export_audit_path, audit)
    return HandoffExportResult(
        output_directory=output_directory,
        handoff_path=handoff_path,
        export_audit_path=export_audit_path,
        review_status=review_status,
        handoff_sha256=handoff_sha,
        unresolved_analysis_concepts=tuple(
            row["analysis_source_variable"] for row in unresolved_analysis
        ),
    )
