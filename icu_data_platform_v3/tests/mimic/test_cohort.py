from datetime import datetime, timedelta

from mimic_iv_pipeline.cohort import select_cohort


def test_cohort_uses_official_times_latest_nonfuture_service_and_first_stay() -> None:
    t0 = datetime(2200, 1, 1, 12)
    stays = [
        {"subject_id": 1, "hadm_id": 10, "stay_id": 101, "intime": t0, "outtime": t0 + timedelta(hours=2)},
        {"subject_id": 1, "hadm_id": 11, "stay_id": 102, "intime": t0 + timedelta(days=1), "outtime": t0 + timedelta(days=1, hours=1)},
        {"subject_id": 2, "hadm_id": 20, "stay_id": 201, "intime": t0, "outtime": t0 + timedelta(minutes=1)},
    ]
    patients = [
        {"subject_id": 1, "anchor_age": 40, "anchor_year": 2200},
        {"subject_id": 2, "anchor_age": 17, "anchor_year": 2200},
    ]
    admissions = [
        {"hadm_id": 10, "hospital_expire_flag": 1, "discharge_location": "HOME"},
        {"hadm_id": 11, "hospital_expire_flag": 0, "discharge_location": "HOME"},
        {"hadm_id": 20, "hospital_expire_flag": 0, "discharge_location": "HOME"},
    ]
    services = [
        {"hadm_id": 10, "transfertime": t0 - timedelta(hours=2), "curr_service": "SURG"},
        {"hadm_id": 10, "transfertime": t0 - timedelta(minutes=1), "curr_service": "MED"},
        {"hadm_id": 10, "transfertime": t0 + timedelta(minutes=1), "curr_service": "OMED"},
        {"hadm_id": 11, "transfertime": t0, "curr_service": "MED"},
        {"hadm_id": 20, "transfertime": t0, "curr_service": "MED"},
    ]
    rows = select_cohort(stays, patients, admissions, services)
    by_stay = {row["stay_id"]: row for row in rows}
    assert by_stay[101]["supervised_eligible"] is True
    assert by_stay[101]["service_at_icu_intime"] == "MED"
    assert by_stay[101]["service_attribution_time"] == t0 - timedelta(minutes=1)
    assert by_stay[101]["official_outtime"] == t0 + timedelta(hours=2)
    assert "not_first_icu_stay_per_subject" in by_stay[102]["exclusion_reasons"]
    assert "age_below_18_or_unresolved" in by_stay[201]["exclusion_reasons"]


def test_no_minimum_los_or_ventilation_requirement() -> None:
    t0 = datetime(2200, 1, 1)
    rows = select_cohort(
        [{"subject_id": 1, "hadm_id": 10, "stay_id": 100, "intime": t0, "outtime": t0}],
        [{"subject_id": 1, "anchor_age": 18, "anchor_year": 2200}],
        [{"hadm_id": 10, "hospital_expire_flag": 0, "discharge_location": None}],
        [{"hadm_id": 10, "transfertime": t0, "curr_service": "NMED"}],
    )
    assert rows[0]["supervised_eligible"] is True
