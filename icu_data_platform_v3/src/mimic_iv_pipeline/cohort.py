from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

from mimic_iv_pipeline.errors import MIMICPipelineError


def _normalized(value: object) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().upper().split())
    return text or None


def _binary(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in (0, 1):
        return value
    return None


def _age(patient: dict[str, Any], intime: datetime) -> int | None:
    anchor_age = patient.get("anchor_age")
    anchor_year = patient.get("anchor_year")
    if not isinstance(anchor_age, (int, float)) or not isinstance(anchor_year, int):
        return None
    return int(anchor_age + intime.year - anchor_year)


def select_cohort(
    icustays: Iterable[dict[str, Any]],
    patients: Iterable[dict[str, Any]],
    admissions: Iterable[dict[str, Any]],
    services: Iterable[dict[str, Any]],
    *,
    included_services: tuple[str, ...] = ("MED", "CMED", "OMED", "NMED"),
    excluded_discharge_locations: tuple[str, ...] = (
        "HOSPICE",
        "AGAINST ADVICE",
        "OTHER FACILITY",
    ),
) -> list[dict[str, Any]]:
    """Annotate every ICU stay and deterministically select the supervised cohort."""

    patient_by_subject = {row.get("subject_id"): row for row in patients}
    admission_by_hadm = {row.get("hadm_id"): row for row in admissions}
    services_by_hadm: dict[object, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for source_order, row in enumerate(services):
        exact_order = row.get("__source_row_number", source_order)
        services_by_hadm[row.get("hadm_id")].append((int(exact_order), row))

    rows = [dict(row) for row in icustays]
    first_stay: dict[object, object] = {}
    grouped: dict[object, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("subject_id")].append(row)
    for subject_id, candidates in grouped.items():
        ordered = sorted(
            candidates,
            key=lambda row: (
                row.get("intime") is None,
                row.get("intime") or datetime.max,
                str(row.get("stay_id")),
            ),
        )
        first_stay[subject_id] = ordered[0].get("stay_id")

    output: list[dict[str, Any]] = []
    for stay in sorted(rows, key=lambda row: (str(row.get("subject_id")), str(row.get("stay_id")))):
        subject_id, hadm_id = stay.get("subject_id"), stay.get("hadm_id")
        intime, outtime = stay.get("intime"), stay.get("outtime")
        valid_time = (
            isinstance(intime, datetime)
            and isinstance(outtime, datetime)
            and outtime >= intime
        )
        admission = admission_by_hadm.get(hadm_id, {})
        patient = patient_by_subject.get(subject_id, {})
        age_years = _age(patient, intime) if isinstance(intime, datetime) else None

        service = None
        service_time = None
        service_source_order = None
        if isinstance(intime, datetime):
            candidates = []
            for source_order, service_row in services_by_hadm.get(hadm_id, []):
                transfer = service_row.get("transfertime")
                if isinstance(transfer, datetime) and transfer <= intime:
                    candidates.append((transfer, source_order, service_row))
            if candidates:
                service_time, service_source_order, service_row = max(
                    candidates, key=lambda item: (item[0], item[1])
                )
                service = _normalized(service_row.get("curr_service"))

        discharge = _normalized(admission.get("discharge_location"))
        mortality = _binary(admission.get("hospital_expire_flag"))
        reasons: list[str] = []
        if not valid_time:
            reasons.append("invalid_or_missing_official_icu_time")
        if age_years is None or age_years < 18:
            reasons.append("age_below_18_or_unresolved")
        if service not in included_services:
            reasons.append("service_not_included_or_unresolved")
        if discharge in excluded_discharge_locations:
            reasons.append("excluded_discharge_location")
        if stay.get("stay_id") != first_stay.get(subject_id):
            reasons.append("not_first_icu_stay_per_subject")
        if mortality is None:
            reasons.append("nonbinary_or_missing_hospital_mortality")

        output.append(
            {
                **stay,
                "age_years": age_years,
                "service_at_icu_intime": service,
                "service_attribution_time": service_time,
                "service_source_order": service_source_order,
                "discharge_location_normalized": discharge,
                "hospital_mortality": mortality,
                "official_outtime": outtime,
                "patient_source_row_number": patient.get("__source_row_number"),
                "admission_source_row_number": admission.get("__source_row_number"),
                "icustay_source_row_number": stay.get("__source_row_number"),
                "supervised_eligible": not reasons,
                "exclusion_reasons": reasons,
            }
        )
    return output
