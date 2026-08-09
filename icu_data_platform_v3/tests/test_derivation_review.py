from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from asic_pipeline.derivation import review as review_module
from asic_pipeline.derivation.review import (
    CleanedReleaseInput,
    DerivationContractReviewConfig,
    candidate_mortality,
    run_derivation_contract_review,
)
from asic_pipeline.inventory.hashing import sha256_file


def test_candidate_mortality_exposes_conflicts_and_nullable_fallbacks() -> None:
    assert candidate_mortality("died_in_icu", True) == (True, True, False)
    assert candidate_mortality("died_in_hospital", True) == (True, False, False)
    assert candidate_mortality("discharged_alive", True) == (None, None, True)
    assert candidate_mortality(None, False) == (False, False, False)
    assert candidate_mortality(None, True) == (True, None, False)
    assert candidate_mortality(None, None) == (None, None, False)


def test_consolidated_derivation_review_scans_all_candidate_recipes(
    tmp_path: Path,
    project_root: Path,
    monkeypatch,
) -> None:
    static_path = tmp_path / "input/static.parquet"
    dynamic_path = tmp_path / "input/dynamic.parquet"
    static_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "stay_id_global": ["A", "B", "C"],
                "hospital_id": ["asic_UK00"] * 3,
                "death_status": ["died_in_icu", None, "discharged_alive"],
                "hospital_mortality_reported": [True, False, True],
                "discharge_status": ["transferred", None, "transferred"],
            }
        ),
        static_path,
    )
    pq.write_table(
        pa.table(
            {
                "stay_id_global": ["A", "A", "A", "A", "B", "B", "C"],
                "hospital_id": ["asic_UK00"] * 7,
                # A deliberate source-order regression verifies that episode
                # construction sorts within each contiguous stay.
                "minutes_since_icu_admission": [0.0, 960.0, 480.0, 1440.0, 0.0, 540.0, 0.0],
                "insp_pressure": [20.0, 20.0, 20.0, 20.0, None, None, None],
                "peep": [5.0, 5.0, 5.0, 5.0, 5.0, 5.0, None],
                "delta_p_reported": [15.0, 15.0, 15.0, 15.0, None, None, None],
                "fio2": [40.0, 40.0, 40.0, 40.0, None, None, None],
                "vt": [None] * 7,
                "vt_per_kg_ideal_body_weight": [None] * 7,
            }
        ),
        dynamic_path,
    )
    manifest_path = tmp_path / "input/release_manifest.json"
    manifest_path.write_text("{}\n", encoding="utf-8")
    source = CleanedReleaseInput(
        release_directory=static_path.parent,
        release_manifest_path=manifest_path,
        release_manifest={},
        source_files={"static": static_path, "dynamic": dynamic_path},
        schemas={},
    )
    monkeypatch.setattr(review_module, "load_cleaned_release_input", lambda *_: source)

    raw = yaml.safe_load(
        (project_root / "asic/config/derivation/contract_review.yaml").read_text(
            encoding="utf-8"
        )
    )
    promotion_path = (
        project_root
        / "asic/config/cleaning/reviewed_cleaned_promotion_20260806T114234Z.yaml"
    )
    raw["input"]["cleaned_promotion_policy"] = str(promotion_path)
    raw["input"]["cleaned_promotion_policy_sha256"] = sha256_file(promotion_path)
    raw["input"]["expected_static_rows"] = 3
    raw["input"]["expected_dynamic_rows"] = 7
    policy_path = tmp_path / "contract_review.yaml"
    policy_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    config = DerivationContractReviewConfig(
        cleaned_promotion=SimpleNamespace(
            dataset_context="production",
            data_root=tmp_path / "data",
            reports_root=tmp_path / "reports",
        ),
        policy_path=policy_path,
    )

    result = run_derivation_contract_review(config, "review001")

    assert result.overall_status == "pending_human_review"
    assert result.technical_blocking_findings == ()
    payload = yaml.safe_load(result.review_json_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["static_row_count"] == 3
    assert payload["metrics"]["dynamic_row_count"] == 7
    assert payload["time_evidence"]["exact_hours_derivable_count"] == 7
    assert payload["time_evidence"]["time_regression_within_stay_count"] == 1
    assert payload["driving_pressure_evidence"]["computable_row_count"] == 4
    assert payload["mortality_evidence"]["mortality_candidate_outputs"] == {
        "hospital_mortality_false": 1,
        "hospital_mortality_missing": 1,
        "hospital_mortality_true": 1,
        "icu_mortality_false": 1,
        "icu_mortality_missing": 1,
        "icu_mortality_true": 1,
        "mortality_source_conflict_false": 2,
        "mortality_source_conflict_true": 1,
    }
    ventilation = payload["ventilation_evidence"]
    assert ventilation["stay_count"] == 3
    assert ventilation["all_time_episode_count"] == 3
    assert ventilation["nonnegative_time_episode_count"] == 3
    assert ventilation["stays_ge_24h_nonnegative_icu_time"] == 1
    assert payload["clinical_data_written"] is False
    assert "Proposed core derived contract" in result.review_markdown_path.read_text(
        encoding="utf-8"
    )
