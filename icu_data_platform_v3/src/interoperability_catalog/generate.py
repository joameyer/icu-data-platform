from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import pyarrow.parquet as pq
import yaml


CORE_RELEASE = "20260808T074305Z"
DICTIONARY_HASH = "98c61eaccf2f7669fc7c59f0315f65584d1dd6686e096a6938d495a17c99bdff"
CORE_CONTRACT_HASH = "bfc7599a5eebce483b03ef73d5dd013c41bc9d0dc17aca899d173a43555ef3a6"
OFFICIAL_MIMIC_CODE = "https://github.com/MIT-LCP/mimic-code/blob/main/mimic-iv/concepts"

OPERATIONAL_FIELDS = (
    ("__v3_source_file_id", "string"),
    ("__v3_source_file_order", "int32"),
    ("__v3_source_row_number", "int64"),
    ("__v3_source_order", "int64"),
    ("__v3_source_schema_variant_id", "string"),
)

DERIVED_FIELDS: dict[str, tuple[tuple[str, str, str, str], ...]] = {
    "static": (
        ("hospital_mortality", "bool", "not_applicable", "Hospital mortality reconciled from the preserved ASIC mortality evidence."),
        ("icu_mortality", "bool", "not_applicable", "ICU mortality reconciled from the preserved ASIC mortality evidence."),
        ("hospital_mortality_source_conflict", "bool", "not_applicable", "Whether ASIC hospital-mortality sources conflict; this is a derivation QC flag."),
        ("icu_mortality_source_conflict", "bool", "not_applicable", "Whether ASIC ICU-mortality sources conflict; this is a derivation QC flag."),
        ("ventilation_supported_timestamp_count", "int64", "count", "Count of ASIC timestamps supported by any reviewed ventilation proxy marker."),
        ("ventilation_episode_count", "int32", "count", "Count of ASIC proxy episodes using the reviewed eight-hour maximum continuity gap."),
        ("maximum_observed_ventilation_episode_hours", "float64", "h", "Longest observed ASIC ventilation-proxy episode duration."),
        ("observed_mechanical_ventilation_ge_24h", "bool", "not_applicable", "Whether an ASIC ventilation-proxy episode lasted at least 24 hours; not ground truth ventilation."),
        ("icu_recording_extent_hours", "float64", "h", "Extent from the minimum to maximum nonnegative ASIC dynamic observation time."),
    ),
    "dynamic": (
        ("hours_since_icu_admission", "float64", "h", "Exact minutes since ICU admission divided by 60, preserving negative values."),
        ("delta_p_computed", "float64", "cmH2O", "ASIC computed driving pressure: end-inspiratory pressure minus PEEP when in the reviewed range."),
        ("delta_p_computed_out_of_range", "bool", "not_applicable", "Whether computable ASIC driving pressure falls outside 0 to 60 cmH2O."),
    ),
}

IDENTIFIERS = {"stay_id_global", "stay_id_local", "hospital_id"}
STATIC_BODY = {"age_group", "bmi_group", "height_group", "height_measurements_cm", "weight_group", "weight_kg"}
STATIC_OUTCOME = {
    "death_status", "discharge_status", "hospital_mortality_reported", "hospital_mortality",
    "icu_mortality", "hospital_mortality_source_conflict", "icu_mortality_source_conflict",
}
STATIC_ADMIN = {"hosp_los", "icu_los", "icu_readmit"}
STATIC_STUDY = {"cluster_id", "study_implementation_phase", "time_since_study_start"}
STATIC_VENT = {
    "vent_free_days", "ventilation_supported_timestamp_count", "ventilation_episode_count",
    "maximum_observed_ventilation_episode_hours", "observed_mechanical_ventilation_ge_24h",
    "icu_recording_extent_hours",
}
VITALS = {"core_temp", "heart_rate", "map", "dbp", "sbp", "resp_rate", "spo2"}
BLOOD_GAS = {"base_excess_art", "bicarbonate_art", "lactate_art", "paco2", "pao2", "ph_art", "sao2", "scvo2"}
CHEMISTRY = {
    "albumin", "alt", "amylase", "ast", "bilirubin_total", "ck", "ck_mb", "creatinine",
    "crp", "d_dimer", "il6", "inr", "ldh", "lipase", "ntprobnp", "bnp", "pct",
    "troponin", "urea",
}
HEMATOLOGY = {"hematocrit", "hemoglobin", "lymph_abs", "lymph_pct", "platelets", "ptt", "wbc"}
HEMODYNAMICS = {
    "cardiac_index_bolus", "cardiac_index_cont", "cardiac_output_bolus", "cardiac_output_cont", "cvp",
    "dpap", "evlwi", "gedvi", "mpap", "pcwp", "pvri", "spap", "stroke_index_bolus",
    "stroke_index_cont", "stroke_volume_bolus", "stroke_volume_cont", "svri",
}
RESPIRATORY = {
    "compliance", "delta_p_reported", "feo2", "fio2", "fio2_set", "ie_ratio", "ie_ratio_set",
    "insp_pressure", "peep", "peep_set", "pf_ratio", "spont_resp_rate", "vt",
    "vt_per_ideal_bw_total", "vt_per_kg_ideal_body_weight", "vt_spontaneous", "delta_p_computed",
    "delta_p_computed_out_of_range",
}
EXTRACORPOREAL = {"ecmo", "ecmo_o2", "extracorp_blood_flow", "extracorp_o2_flow"}
SCORES = {
    "isofa_cardiovascular", "isofa_cns", "isofa_liver", "isofa_renal", "isofa_respiratory",
    "isofa_thrombocyte", "isofa_total_score", "sofa_blood", "sofa_cns", "sofa_liver",
    "sofa_renal", "sofa_respiratory", "sofa_respiratory_calculated", "sofa_score_unspecified",
    "sofa_score_without_gcs", "sofa_total_score",
}
MEDICATIONS = {
    "clonidine_iv_cont", "dexamethasone_iv_bolus", "dexmedetomidine_iv_cont", "dobutamine_iv_cont",
    "epinephrine_iv_cont", "fentanyl_iv_cont", "fludrocortisone_po_bolus", "furosemide_iv_cont",
    "hydrocortisone_iv_bolus", "inhaled_iloprost", "inhaled_no", "isoflurane_inh",
    "ketanest_iv_cont", "levosimendan_iv_cont", "midazolam_iv_cont", "milrinone_iv_cont",
    "morphine_iv_cont", "norepinephrine_iv_cont", "prednisolone_iv_bolus", "propofol_iv_cont",
    "rocuronium_iv_bolus", "sevoflurane_inh", "sufentanil_iv_cont", "terlipressin_iv_bolus",
    "vasopressin_iv_cont", "clonidine_iv_cont_weight_normalized", "epinephrine_iv_cont_absolute",
    "hydrocortisone_iv_bolus_source_uk08", "ketanest_iv_cont_weight_normalized",
    "morphine_iv_cont_weight_normalized", "norepinephrine_iv_cont_absolute",
    "prednisolone_iv_bolus_source_uk02", "propofol_iv_cont_weight_normalized",
    "sufentanil_iv_cont_source_uk00",
}

ALIGNED = {
    "static.hosp_los", "static.hospital_mortality_reported", "static.icu_los", "static.sex", "static.weight_kg",
    "static.hospital_mortality", "dynamic.albumin", "dynamic.alt", "dynamic.amylase", "dynamic.ast",
    "dynamic.bilirubin_total", "dynamic.ck", "dynamic.core_temp", "dynamic.creatinine", "dynamic.heart_rate",
    "dynamic.hematocrit", "dynamic.hemoglobin", "dynamic.inr", "dynamic.ldh", "dynamic.lipase",
    "dynamic.lymph_abs", "dynamic.lymph_pct", "dynamic.map", "dynamic.ntprobnp", "dynamic.platelets",
    "dynamic.ptt", "dynamic.resp_rate", "dynamic.sbp", "dynamic.dbp", "dynamic.spo2",
    "dynamic.spont_resp_rate", "dynamic.vt_spontaneous", "dynamic.wbc", "dynamic.minutes_since_icu_admission",
    "dynamic.hours_since_icu_admission",
}
CONDITIONAL = {
    "static.age_group", "static.bmi_group", "static.death_status", "static.discharge_status", "static.height_group",
    "static.height_measurements_cm", "static.icd10_codes", "static.icu_readmit", "static.weight_group",
    "static.icu_mortality", "dynamic.base_excess_art", "dynamic.bicarbonate_art", "dynamic.bnp",
    "dynamic.fio2", "dynamic.fio2_set", "dynamic.lactate_art", "dynamic.paco2", "dynamic.pao2",
    "dynamic.ph_art", "dynamic.sao2", "dynamic.scvo2", "dynamic.troponin", "dynamic.pct",
    "dynamic.il6", "dynamic.peep", "dynamic.peep_set", "dynamic.vt", "dynamic.pf_ratio",
    "dynamic.ecmo", "dynamic.etco2", "dynamic.compliance", "dynamic.insp_pressure",
    "dynamic.vt_per_ideal_bw_total", "dynamic.vt_per_kg_ideal_body_weight",
}
UNRESOLVED = {
    "static.dialysis_free_days", "dynamic.ck_mb", "dynamic.crp", "dynamic.d_dimer",
    "dynamic.fluid_balance_24h", "dynamic.urea", "dynamic.position_therapy",
    *{f"dynamic.{name}" for name in HEMODYNAMICS},
    *{f"dynamic.{name}" for name in MEDICATIONS if "source_uk" not in name},
    "dynamic.ecmo_o2", "dynamic.extracorp_blood_flow", "dynamic.extracorp_o2_flow",
    "dynamic.ie_ratio", "dynamic.ie_ratio_set",
}
RELATED = {
    "static.death_status", "static.dialysis_free_days", "static.discharge_status", "static.vent_free_days",
    "static.ventilation_supported_timestamp_count", "static.ventilation_episode_count",
    "static.maximum_observed_ventilation_episode_hours", "static.observed_mechanical_ventilation_ge_24h",
    "static.icu_recording_extent_hours", "dynamic.ards_diagnosis_app", "dynamic.delta_p_reported",
    "dynamic.delta_p_computed", *{f"dynamic.{name}" for name in SCORES},
    *{f"dynamic.{name}" for name in MEDICATIONS if "source_uk" in name},
}
NO_EQUIVALENT = {
    "static.cluster_id", "static.study_implementation_phase", "static.time_since_study_start",
    "dynamic.feo2", "dynamic.severity_read_confirmation",
}
OPERATIONAL = {
    "dynamic.anchored_time_since_icu_admission", "dynamic.therapy_read_confirmation_utc",
    "static.hospital_mortality_source_conflict", "static.icu_mortality_source_conflict",
    "dynamic.delta_p_computed_out_of_range", "static.icd10_codes_source_text",
}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def family_for(table: str, variable: str) -> str:
    if variable in IDENTIFIERS:
        return "identifier"
    if variable.startswith("__v3_"):
        return "operational_provenance"
    if table == "static":
        if variable in STATIC_BODY:
            return "demographics_and_body_size"
        if variable in STATIC_OUTCOME:
            return "outcome"
        if variable in STATIC_ADMIN:
            return "stay_and_administration"
        if variable in STATIC_STUDY:
            return "study_metadata"
        if variable in STATIC_VENT:
            return "ventilation_summary"
        if variable in {"icd10_codes", "icd10_codes_source_text"}:
            return "diagnosis"
        if variable == "dialysis_free_days":
            return "renal_replacement_summary"
    if variable in VITALS:
        return "vital_sign"
    if variable in BLOOD_GAS:
        return "blood_gas"
    if variable in CHEMISTRY:
        return "laboratory_chemistry"
    if variable in HEMATOLOGY:
        return "hematology_and_coagulation"
    if variable in HEMODYNAMICS:
        return "advanced_hemodynamics"
    if variable in RESPIRATORY:
        return "respiratory_measurement_or_setting"
    if variable in EXTRACORPOREAL:
        return "extracorporeal_support"
    if variable in SCORES:
        return "clinical_score"
    if variable in MEDICATIONS:
        return "medication_or_therapy"
    if variable in {"minutes_since_icu_admission", "hours_since_icu_admission", "anchored_time_since_icu_admission"}:
        return "time"
    if variable == "fluid_balance_24h":
        return "fluid_balance"
    if variable == "position_therapy":
        return "position_therapy"
    if variable == "ards_diagnosis_app":
        return "diagnosis_or_score"
    if variable in {"severity_read_confirmation", "therapy_read_confirmation_utc"}:
        return "operational_confirmation"
    return "other_clinical"


def concept_id(table: str, variable: str) -> str:
    family = family_for(table, variable)
    if family in {"identifier", "operational_provenance"}:
        return f"operational.{table}.{_slug(variable)}"
    return f"clinical.{family}.{_slug(variable)}"


def disposition_for(table: str, variable: str) -> tuple[str, str]:
    field_id = f"{table}.{variable}"
    if variable in IDENTIFIERS or variable.startswith("__v3_") or field_id in OPERATIONAL:
        return "operational_or_nonclinical_not_in_scope", "Retain for lineage/QC only; never treat as a clinical overlap."
    if field_id in ALIGNED:
        return "aligned", "No remaining concept gate; exact source-unit spellings and item dictionaries still fail closed."
    if field_id in CONDITIONAL:
        gate = "Requires the concept-specific method, specimen, representation, or derivation review recorded in the mapping catalog."
        return "conditionally_aligned", gate
    if field_id in UNRESOLVED:
        return "unresolved_blocking", "Requires reviewed item/unit/context semantics before any MIMIC value is eligible."
    if field_id in RELATED:
        return "related_but_not_equivalent", "Related MIMIC evidence must remain separately named; automatic coalescence is prohibited."
    if field_id in NO_EQUIVALENT:
        return "no_mimic_equivalent", "No equivalent original MIMIC-IV 3.1 field is currently supported."
    return "no_mimic_equivalent", "No sufficiently defined MIMIC-IV equivalent was identified in the independent draft."


def build_concepts(root: Path) -> list[dict[str, str]]:
    metadata_root = root / "asic/data/local_metadata/harmonized_schema_dictionary/0.2"
    dictionary = pq.read_table(metadata_root / "variable_dictionary.parquet").to_pylist()
    by_key = {(str(row["table"]), str(row["variable"])): row for row in dictionary}
    schemas = {
        table: json.loads((metadata_root / f"{table}_schema.json").read_text(encoding="utf-8"))
        for table in ("static", "dynamic")
    }
    rows: list[dict[str, str]] = []
    for table in ("static", "dynamic"):
        fields: list[dict[str, Any]] = list(schemas[table])
        next_position = len(fields)
        for offset, (name, physical_type) in enumerate(OPERATIONAL_FIELDS, start=1):
            fields.append({
                "ordered_position": next_position + offset,
                "variable": name,
                "physical_type": physical_type,
                "unit": "not_applicable",
                "analysis_eligibility": "operational_only",
            })
        next_position = len(fields)
        for offset, (name, physical_type, unit, definition) in enumerate(DERIVED_FIELDS[table], start=1):
            fields.append({
                "ordered_position": next_position + offset,
                "variable": name,
                "physical_type": physical_type,
                "unit": unit,
                "analysis_eligibility": "derived_contract_eligible",
                "definition": definition,
            })
        for field in fields:
            variable = str(field["variable"])
            source = by_key.get((table, variable), {})
            definition = str(source.get("definition") or field.get("definition") or "Operational provenance field.")
            caveats = str(source.get("analysis_caveat") or "")
            if variable.startswith("__v3_"):
                caveats = "Private row/file provenance; prohibited from public review outputs."
            disp, gate = disposition_for(table, variable)
            unit = str(field.get("unit") or source.get("unit") or "not_applicable")
            rows.append({
                "asic_field_id": f"{table}.{variable}",
                "canonical_concept_id": concept_id(table, variable),
                "asic_core_derived_variable": variable,
                "asic_table": table,
                "core_derived_position": str(field["ordered_position"]),
                "clinical_family": family_for(table, variable),
                "clinical_definition": definition,
                "asic_physical_type": str(field.get("physical_type") or source.get("physical_type") or ""),
                "asic_unit": unit,
                "asic_analysis_eligibility": str(field.get("analysis_eligibility") or source.get("analysis_eligibility") or ""),
                "asic_definition_status": str(source.get("definition_status") or ("reviewed_core_derived_0_2" if variable in {x[0] for x in DERIVED_FIELDS[table]} else "operational")),
                "asic_caveats": caveats,
                "asic_evidence_confidence": str(source.get("unit_confidence") or ("not_applicable" if unit == "not_applicable" else "reviewed_formula")),
                "asic_unit_evidence_basis": str(source.get("unit_evidence_basis") or ("reviewed_core_derived_contract_0_2" if variable in {x[0] for x in DERIVED_FIELDS[table]} else "frozen_schema_manifest")),
                "mimic_overlap_disposition": disp,
                "preferred_shared_canonical_unit": unit,
                "remaining_semantic_gate": gate,
                "source_dictionary_contract": "ASIC harmonized schema/dictionary 0.2; review run 20260807T101321Z",
                "source_core_derived_contract": "0.2",
                "source_core_derived_release": CORE_RELEASE,
                "source_metadata_sha256": DICTIONARY_HASH if source else CORE_CONTRACT_HASH,
                "catalog_review_status": "independent_draft_pending_human_review",
            })
    return rows


def build_mappings(concepts: list[dict[str, str]]) -> list[dict[str, str]]:
    by_field = {row["asic_field_id"]: row for row in concepts}
    rows: list[dict[str, str]] = []

    current_items: dict[tuple[str, int], tuple[str, str]] = {}
    for variable, ids, role in (
        ("heart_rate", [220045], "predictor"), ("map", [220052, 220181, 225312], "predictor"),
        ("resp_rate", [220210, 224690], "predictor"), ("spo2", [220277], "predictor"),
        ("core_temp", [223762, 223761], "predictor"), ("height_cm", [226730, 226707], "static_input"),
        ("weight_kg", [226512, 224639], "static_input"), ("fio2", [223835], "audit_marker"),
        ("peep", [220339, 224700], "audit_marker"), ("vt", [224684, 224685, 224686], "audit_marker"),
    ):
        for item in ids:
            current_items[("chartevents", item)] = (variable, role)
    for variable, ids, role in (
        ("albumin", [50862], "predictor"), ("ast", [50878], "predictor"),
        ("bilirubin_total", [50885], "predictor"), ("ck", [50910], "predictor"),
        ("creatinine", [50912], "predictor"), ("hemoglobin", [51222, 50811], "predictor"),
        ("hematocrit", [51221, 50810], "predictor"), ("inr", [51237], "predictor"),
        ("lactate_art", [50813], "predictor_human_gate"), ("ldh", [50954], "predictor"),
        ("lipase", [50956], "predictor"), ("pao2", [50821], "predictor_human_gate"),
        ("paco2", [50818], "predictor_human_gate"), ("ph_art", [50820], "predictor_human_gate"),
        ("platelets", [51265], "predictor"), ("urea", [51006], "predictor_human_gate"),
        ("wbc", [51300, 51301, 51755], "predictor"),
    ):
        for item in ids:
            current_items[("labevents", item)] = (variable, role)

    def add(
        field_id: str, source_table: str, source_field: str, item_id: int | str,
        label: str, category: str, method: str, source_unit: str, value_type: str,
        conversion: str, *, precedence: int = 100, eligibility: str | None = None,
        caveat: str = "", evidence: str = "", review_status: str = "proposed_pending_dictionary_audit",
    ) -> None:
        concept = by_field[field_id]
        dictionary = "d_labitems" if source_table == "labevents" else (
            "d_items" if source_table in {"chartevents", "inputevents", "outputevents", "procedureevents"}
            else "not_applicable"
        )
        item_text = str(item_id) if dictionary != "not_applicable" else "not_applicable"
        current = current_items.get((source_table, int(item_id))) if isinstance(item_id, int) else None
        disposition = concept["mimic_overlap_disposition"]
        resolved_eligibility = eligibility or ("enabled" if disposition == "aligned" else "human_gate")
        rows.append({
            "mapping_id": f"map.{_slug(field_id)}.{_slug(source_table)}.{_slug(source_field)}.{_slug(item_text)}.{_slug(source_unit)}",
            "canonical_concept_id": concept["canonical_concept_id"],
            "mimic_source_table": source_table,
            "mimic_source_field": source_field,
            "dictionary_table": dictionary,
            "item_id": item_text,
            "dictionary_label": label,
            "category": category,
            "linksto": source_table,
            "measurement_method_or_specimen": method,
            "accepted_source_unit": source_unit,
            "source_value_type": value_type,
            "shared_canonical_unit": concept["preferred_shared_canonical_unit"],
            "conversion_id": conversion,
            "source_precedence": f"{precedence:03d}; lower_rank_wins_only_for_equal_event_time",
            "duplicate_resolution_policy": "retain_all_source_rows; exact_duplicates_use_source_row_order; blocking_selects_deterministic_last_non_missing",
            "plausibility_policy_reference": f"ASIC cleaned 0.2 canonical-unit policy for {concept['asic_core_derived_variable']}",
            "current_profile_membership": "yes" if current else "no",
            "current_profile_variable": current[0] if current else "not_applicable",
            "eligibility": resolved_eligibility,
            "semantic_caveat": caveat,
            "evidence_source": evidence or "MIMIC-IV 3.1 dictionary aggregate audit required",
            "review_status": review_status,
        })

    # Direct MIMIC tables: no item dictionary and no invented item identifiers.
    add("static.age_group", "patients", "anchor_age", "not_applicable", "anchor_age", "demographics", "reported administrative age", "not_applicable", "integer", "identity_not_applicable", caveat="Exact ASIC age-bin boundaries and MIMIC age reconstruction must be reviewed.")
    add("static.death_status", "admissions", "hospital_expire_flag", "not_applicable", "hospital_expire_flag", "outcome", "reported hospital mortality flag", "not_applicable", "boolean", "identity_not_applicable", eligibility="ineligible", caveat="Binary hospital death is related but cannot reproduce ASIC multi-category death_status.")
    add("static.discharge_status", "admissions", "discharge_location", "not_applicable", "discharge_location", "administrative", "reported discharge destination", "not_applicable", "categorical", "identity_not_applicable", eligibility="ineligible", caveat="Vocabulary is not equivalent to the ASIC discharge-status categories.")
    add("static.hosp_los", "admissions", "admittime+dischtime", "not_applicable", "admittime and dischtime", "administrative", "elapsed timestamp pair", "timestamp_pair", "composite", "timestamp_pair_to_days", review_status="reviewed_formula")
    add("static.hospital_mortality_reported", "admissions", "hospital_expire_flag", "not_applicable", "hospital_expire_flag", "outcome", "reported binary hospital outcome", "not_applicable", "boolean", "identity_not_applicable", review_status="reviewed_semantic_mapping")
    add("static.hospital_mortality", "admissions", "hospital_expire_flag", "not_applicable", "hospital_expire_flag", "outcome", "reported binary hospital outcome", "not_applicable", "boolean", "identity_not_applicable", review_status="reviewed_semantic_mapping")
    add("static.icd10_codes", "diagnoses_icd", "icd_code where icd_version=10", "not_applicable", "icd_code", "diagnosis", "reported ICD-10-CM code", "not_applicable", "categorical", "identity_not_applicable", caveat="ICD-9 rows remain separate and cannot be silently translated.", review_status="reviewed_source_pending_phenotype_contract")
    add("static.icu_los", "icustays", "intime+outtime", "not_applicable", "official ICU intime and outtime", "administrative", "elapsed timestamp pair using official outtime", "timestamp_pair", "composite", "timestamp_pair_to_days", review_status="reviewed_formula")
    add("static.icu_readmit", "icustays", "subject_id+hadm_id+intime", "not_applicable", "ordered ICU stays", "administrative", "contextual repeat-stay derivation", "not_applicable", "composite", "identity_not_applicable", caveat="Readmission horizon and within-hospital-versus-ever semantics require review.")
    add("static.sex", "patients", "gender", "not_applicable", "gender", "demographics", "reported administrative sex field", "not_applicable", "categorical", "identity_not_applicable", review_status="reviewed_semantic_mapping")
    add("static.icu_mortality", "patients", "dod with icustays.outtime", "not_applicable", "date of death and ICU outtime", "outcome", "contextual death-within-ICU derivation", "not_applicable", "composite", "identity_not_applicable", caveat="Date precision and definition of ICU death require a reviewed outcome contract.")

    # Bedside measurements and settings.
    chart_rows = [
        ("static.height_measurements_cm", 226730, "Height (cm)", "routine vital signs", "bedside charted measurement", "cm", "identity_cm", 10),
        ("static.height_measurements_cm", 226707, "Height", "routine vital signs", "bedside charted measurement", "inch", "inch_to_cm", 20),
        ("static.weight_kg", 226512, "Admission Weight (Kg)", "routine vital signs", "bedside charted measurement", "kg", "identity_kg", 10),
        ("static.weight_kg", 224639, "Daily Weight", "routine vital signs", "bedside charted measurement", "kg", "identity_kg", 20),
        ("dynamic.heart_rate", 220045, "Heart Rate", "routine vital signs", "bedside charted measurement", "bpm", "identity_bpm", 10),
        ("dynamic.sbp", 225309, "ART BP Systolic", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 10),
        ("dynamic.sbp", 220050, "Arterial Blood Pressure systolic", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 20),
        ("dynamic.sbp", 220179, "Non Invasive Blood Pressure systolic", "routine vital signs", "non-invasive cuff charted measurement", "mmHg", "identity_mmhg", 30),
        ("dynamic.dbp", 225310, "ART BP Diastolic", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 10),
        ("dynamic.dbp", 220051, "Arterial Blood Pressure diastolic", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 20),
        ("dynamic.dbp", 220180, "Non Invasive Blood Pressure diastolic", "routine vital signs", "non-invasive cuff charted measurement", "mmHg", "identity_mmhg", 30),
        ("dynamic.map", 225312, "ART BP Mean", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 10),
        ("dynamic.map", 220052, "Arterial Blood Pressure mean", "routine vital signs", "arterial catheter charted measurement", "mmHg", "identity_mmhg", 20),
        ("dynamic.map", 220181, "Non Invasive Blood Pressure mean", "routine vital signs", "non-invasive cuff charted measurement", "mmHg", "identity_mmhg", 30),
        ("dynamic.resp_rate", 220210, "Respiratory Rate", "routine vital signs", "bedside charted measurement", "insp/min", "identity_insp_per_min", 10),
        ("dynamic.resp_rate", 224690, "Respiratory Rate (Total)", "respiratory", "ventilator total measured rate", "insp/min", "identity_insp_per_min", 20),
        ("dynamic.spont_resp_rate", 224689, "Respiratory Rate (spontaneous)", "respiratory", "ventilator spontaneous measured rate", "insp/min", "identity_insp_per_min", 10),
        ("dynamic.spo2", 220277, "SpO2, peripheral", "routine vital signs", "pulse oximetry; not arterial SaO2 or central-venous ScvO2", "%", "identity_percent", 10),
        ("dynamic.core_temp", 223762, "Temperature Celsius", "routine vital signs", "charted temperature; site field is separate", "degC", "degc_to_deg_c", 10),
        ("dynamic.core_temp", 223761, "Temperature Fahrenheit", "routine vital signs", "charted temperature; site field is separate", "degF", "fahrenheit_to_celsius", 20),
        ("dynamic.fio2_set", 223835, "Inspired O2 Fraction", "respiratory", "ventilator setting with mixed fraction/percent representation", "%", "fraction_or_percent_to_percent", 10),
        ("dynamic.peep_set", 220339, "PEEP set", "respiratory", "ventilator setting", "cmH2O", "identity_cmh2o", 10),
        ("dynamic.peep_set", 224700, "PEEP", "respiratory", "ventilator setting in official ventilator concept", "cmH2O", "identity_cmh2o", 20),
        ("dynamic.vt", 224685, "Tidal Volume (observed)", "respiratory", "ventilator measured total tidal volume", "mL", "identity_ml", 10),
        ("dynamic.vt", 224684, "Tidal Volume (set)", "respiratory", "ventilator setting; not equivalent to measured tidal volume", "mL", "identity_ml", 20),
        ("dynamic.vt_spontaneous", 224686, "Tidal Volume (spontaneous)", "respiratory", "ventilator measured spontaneous tidal volume", "mL", "identity_ml", 10),
    ]
    for field_id, item, label, category, method, unit, conversion, precedence in chart_rows:
        disposition = by_field[field_id]["mimic_overlap_disposition"]
        eligibility = "enabled" if disposition == "aligned" else "human_gate"
        caveat = ""
        if item == 224684:
            eligibility = "ineligible"
            caveat = "Setting is retained as related evidence and cannot populate a measured VT concept."
        if field_id in {"dynamic.fio2_set", "dynamic.peep_set"}:
            caveat = "Current MIMIC profile uses a less-specific variable name; migration requires a reviewed canonical schema contract, not profile expansion."
        add(field_id, "chartevents", "valuenum/valueuom", item, label, category, method, unit, "numeric", conversion, precedence=precedence, eligibility=eligibility, caveat=caveat, evidence=f"{OFFICIAL_MIMIC_CODE}/measurement/vitalsign.sql" if field_id.split('.')[1] in VITALS else f"{OFFICIAL_MIMIC_CODE}/measurement/ventilator_setting.sql", review_status="reviewed_source_mapping" if eligibility == "enabled" else "proposed_semantic_gate")

    # Laboratory measurements. Arterial concepts remain gated by item 52033 specimen evidence.
    lab_rows = [
        ("dynamic.albumin", 50862, "Albumin", "Chemistry", "serum/plasma laboratory measurement", "g/dL", "g_per_dl_to_dg_per_l", 10),
        ("dynamic.alt", 50861, "Alanine Aminotransferase (ALT)", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.amylase", 50867, "Amylase", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.ast", 50878, "Asparate Aminotransferase (AST)", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.base_excess_art", 50802, "Base Excess", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial", "mmol/L", "identity_mmol_per_l", 10),
        ("dynamic.bicarbonate_art", 50803, "Calculated Bicarbonate, Whole Blood", "Blood Gas", "calculated blood-gas value; arterial only with reviewed same-specimen evidence", "mmol/L", "identity_mmol_per_l", 10),
        ("dynamic.bilirubin_total", 50885, "Bilirubin, Total", "Chemistry", "laboratory measurement", "mg/dL", "bilirubin_mg_per_dl_to_umol_per_l", 10),
        ("dynamic.ck", 50910, "Creatine Kinase (CK)", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.ck_mb", 50911, "Creatine Kinase, MB Isoenzyme", "Chemistry", "mass concentration; not activity", "ng/mL", "ckmb_mass_to_activity_unavailable", 10),
        ("dynamic.creatinine", 50912, "Creatinine", "Chemistry", "laboratory measurement", "mg/dL", "creatinine_mg_per_dl_to_umol_per_l", 10),
        ("dynamic.crp", 50889, "C-Reactive Protein", "Chemistry", "laboratory mass concentration", "mg/L", "crp_mg_per_l_to_nmol_per_l_unavailable", 10),
        ("dynamic.d_dimer", 51196, "D-Dimer", "Hematology", "assay basis FEU/DDU unspecified", "ng/mL", "d_dimer_assay_conversion_unavailable", 10),
        ("dynamic.hematocrit", 51221, "Hematocrit", "Hematology", "central laboratory measurement", "%", "identity_percent", 10),
        ("dynamic.hematocrit", 50810, "Hematocrit, Calculated", "Blood Gas", "calculated blood-gas value; specimen unspecified", "%", "identity_percent", 20),
        ("dynamic.hemoglobin", 51222, "Hemoglobin", "Hematology", "central laboratory measurement", "g/dL", "hemoglobin_g_per_dl_to_mmol_per_l", 10),
        ("dynamic.hemoglobin", 50811, "Hemoglobin", "Blood Gas", "blood-gas measurement; specimen unspecified", "g/dL", "hemoglobin_g_per_dl_to_mmol_per_l", 20),
        ("dynamic.inr", 51237, "INR(PT)", "Hematology", "reported coagulation ratio", "ratio", "ratio_to_dimensionless", 10),
        ("dynamic.lactate_art", 50813, "Lactate", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial", "mmol/L", "identity_mmol_per_l", 10),
        ("dynamic.ldh", 50954, "Lactate Dehydrogenase (LD)", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.lipase", 50956, "Lipase", "Chemistry", "laboratory enzyme activity", "IU/L", "iu_per_l_to_u_per_l", 10),
        ("dynamic.lymph_abs", 51133, "Absolute Lymphocyte Count", "Hematology", "reported absolute count", "K/uL", "k_per_ul_to_10e9_per_l", 10),
        ("dynamic.lymph_abs", 52769, "Absolute Lymphocyte Count", "Hematology", "reported absolute count", "#/uL", "number_per_ul_to_10e9_per_l", 20),
        ("dynamic.lymph_pct", 51244, "Lymphocytes", "Hematology", "reported differential percentage", "%", "identity_percent", 10),
        ("dynamic.lymph_pct", 51245, "Lymphocytes, Percent", "Hematology", "reported differential percentage", "%", "identity_percent", 20),
        ("dynamic.ntprobnp", 50963, "NT-proBNP", "Chemistry", "N-terminal pro-BNP laboratory measurement", "pg/mL", "identity_pg_per_ml", 10),
        ("dynamic.paco2", 50818, "pCO2", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial", "mmHg", "identity_mmhg", 10),
        ("dynamic.pao2", 50821, "pO2", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial", "mmHg", "identity_mmhg", 10),
        ("dynamic.ph_art", 50820, "pH", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial", "units", "identity_ph", 10),
        ("dynamic.platelets", 51265, "Platelet Count", "Hematology", "central laboratory measurement", "K/uL", "k_per_ul_to_10e9_per_l", 10),
        ("dynamic.ptt", 51275, "PTT", "Hematology", "reported activated partial thromboplastin time", "sec", "identity_seconds", 10),
        ("dynamic.sao2", 50817, "Oxygen Saturation", "Blood Gas", "arterial only when same-specimen item 52033 is reviewed arterial; not SpO2 or ScvO2", "%", "identity_percent", 10),
        ("dynamic.troponin", 51003, "Troponin T", "Chemistry", "troponin T assay; ASIC subtype unspecified", "ng/mL", "identity_ng_per_ml", 10),
        ("dynamic.urea", 51006, "Urea Nitrogen", "Chemistry", "BUN quantity converted to urea quantity", "mg/dL", "bun_mg_per_dl_to_urea_mmol_per_l", 10),
        ("dynamic.wbc", 51301, "White Blood Cells", "Hematology", "central laboratory measurement", "K/uL", "k_per_ul_to_10e9_per_l", 10),
        ("dynamic.wbc", 51300, "WBC Count", "Hematology", "central laboratory measurement", "K/uL", "k_per_ul_to_10e9_per_l", 20),
        ("dynamic.wbc", 51755, "White Blood Cells", "Hematology", "central laboratory measurement", "K/uL", "k_per_ul_to_10e9_per_l", 30),
    ]
    arterial_fields = {"dynamic.base_excess_art", "dynamic.bicarbonate_art", "dynamic.lactate_art", "dynamic.paco2", "dynamic.pao2", "dynamic.ph_art", "dynamic.sao2"}
    unavailable = {"dynamic.ck_mb", "dynamic.crp", "dynamic.d_dimer"}
    evidence_files = {
        "dynamic.alt": "enzyme.sql", "dynamic.amylase": "enzyme.sql", "dynamic.ast": "enzyme.sql",
        "dynamic.bilirubin_total": "enzyme.sql", "dynamic.ck": "enzyme.sql", "dynamic.ck_mb": "cardiac_marker.sql",
        "dynamic.ntprobnp": "cardiac_marker.sql", "dynamic.troponin": "cardiac_marker.sql",
        "dynamic.hematocrit": "complete_blood_count.sql", "dynamic.hemoglobin": "complete_blood_count.sql",
        "dynamic.platelets": "complete_blood_count.sql", "dynamic.wbc": "complete_blood_count.sql",
        "dynamic.lymph_abs": "blood_differential.sql", "dynamic.lymph_pct": "blood_differential.sql",
        "dynamic.inr": "coagulation.sql", "dynamic.ptt": "coagulation.sql", "dynamic.d_dimer": "coagulation.sql",
    }
    for field_id, item, label, category, method, unit, conversion, precedence in lab_rows:
        disposition = by_field[field_id]["mimic_overlap_disposition"]
        eligibility = "enabled" if disposition == "aligned" else "human_gate"
        review = "reviewed_source_mapping"
        caveat = ""
        if field_id in arterial_fields:
            eligibility = "human_gate"
            review = "pending_reviewed_same_specimen_arterial_semantics"
            caveat = "Must link by specimen_id to item 52033 and accept only a reviewed arterial value; venous, capillary, and unspecified remain ineligible."
        if field_id in unavailable:
            eligibility = "ineligible"
            review = "rejected_or_unresolved_quantity_alignment"
            caveat = "The MIMIC quantity or assay basis cannot be converted safely to the ASIC quantity."
        if field_id == "dynamic.urea":
            eligibility = "human_gate"
            review = "pending_BUN_to_urea_human_approval"
            caveat = "This changes quantity from blood urea nitrogen to urea; do not enable merely as a unit spelling conversion."
        if field_id == "dynamic.troponin":
            caveat = "MIMIC item is troponin T while the ASIC variable has no assay subtype; retain subtype provenance."
        filename = evidence_files.get(field_id, "bg.sql" if field_id in arterial_fields else "chemistry.sql")
        add(field_id, "labevents", "valuenum/valueuom", item, label, category, method, unit, "numeric", conversion, precedence=precedence, eligibility=eligibility, caveat=caveat, evidence=f"{OFFICIAL_MIMIC_CODE}/measurement/{filename}", review_status=review)

    return sorted(rows, key=lambda row: row["mapping_id"])


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_independent_draft(root: Path) -> dict[str, str]:
    concepts = build_concepts(root)
    mappings = build_mappings(concepts)
    config = root / "interoperability/config"
    concept_path = config / "clinical_concepts_0_1.csv"
    mapping_path = config / "mimic_source_mappings_0_1.csv"
    _write_csv(concept_path, concepts)
    _write_csv(mapping_path, mappings)
    files = [concept_path, mapping_path, config / "unit_conversions_0_1.yaml"]
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    combined = hashlib.sha256(
        "".join(f"{name}:{value}\n" for name, value in sorted(hashes.items())).encode("utf-8")
    ).hexdigest()
    manifest = {
        "artifact": "asic_mimic_alignment_independent_draft_manifest",
        "artifact_version": "0.1",
        "chronology": "created_before_legacy_variable_configuration_csv_was_opened",
        "authoritative_inputs": [
            "ASIC frozen harmonized schema/dictionary 0.2",
            "ASIC reviewed core-derived contract 0.2 and release 20260808T074305Z",
            "ASIC reviewed unit provenance 0.1/0.2",
            "MIMIC-IV 3.1 original table/dictionary contracts",
            "MIT-LCP official MIMIC code concepts",
            "current icu_data_platform_v3 MIMIC contracts and implementation",
        ],
        "legacy_input_read": False,
        "files": hashes,
        "combined_sha256": combined,
        "concept_count": len(concepts),
        "mapping_count": len(mappings),
    }
    manifest_path = config / "independent_draft_manifest_0_1.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {**hashes, "combined_sha256": combined}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the independent ASIC--MIMIC alignment draft")
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args()
    hashes = write_independent_draft(args.project_root.resolve())
    print(yaml.safe_dump(hashes, sort_keys=True).strip())


if __name__ == "__main__":
    main()
