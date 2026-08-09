from pathlib import Path

from mimic_iv_pipeline.contracts import load_profile


def test_profile_has_complete_selected_source_coverage(project_root: Path) -> None:
    loaded = load_profile(
        project_root / "mimic/config/profiles/phase_aware_mortality_0_1.yaml"
    )
    assert loaded.profile["analysis_handoff"]["required_resolution"] == "blocked_8h"
    assert loaded.contracts["blocked_8h"]["source_artifact"] == "canonical_selected_events_0_1"
    assert loaded.variables["fluid_balance_24h"].eligibility == "human_gate"
    assert loaded.variables["fio2"].role == "ventilation_audit"
    assert loaded.ranges["heart_rate"].contains(250)
    assert not loaded.ranges["heart_rate"].contains(0)
