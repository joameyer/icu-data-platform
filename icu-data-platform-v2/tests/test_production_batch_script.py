from __future__ import annotations

import os
from pathlib import Path
import subprocess


PROJECT_ROOT = Path(__file__).parents[1]
SCRIPT_PATH = PROJECT_ROOT / "asic" / "run_pooled_to_translated_production.sh"
TRANSLATED_AUDIT_SCRIPT_PATH = (
    PROJECT_ROOT / "asic" / "run_translated_input_audit_production.sh"
)
CROSS_HOSPITAL_AUDIT_SCRIPT_PATH = (
    PROJECT_ROOT / "asic" / "run_cross_hospital_audit_production.sh"
)


def test_production_batch_script_has_valid_bash_syntax_and_is_executable() -> None:
    assert os.access(SCRIPT_PATH, os.X_OK)
    subprocess.run(["bash", "-n", str(SCRIPT_PATH)], check=True)


def test_production_batch_script_enforces_reviewed_execution_order() -> None:
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "asic/config/datasets/production.yaml" in script
    assert "${project_root}/.venv/bin/python" in script
    assert "--overwrite" not in script
    assert "#SBATCH --output=asic/runs/production/" in script
    assert "#SBATCH --error=asic/runs/production/" in script

    commands = [
        "-m asic_pipeline validate-translation-policy",
        "-m asic_pipeline audit-pooled",
        "-m asic_pipeline inventory-pooled-categories",
        "-m asic_pipeline pooled-to-translated",
    ]
    positions = [script.index(command) for command in commands]
    assert positions == sorted(positions)


def test_translated_audit_batch_script_is_read_only_and_executable() -> None:
    assert os.access(TRANSLATED_AUDIT_SCRIPT_PATH, os.X_OK)
    subprocess.run(
        ["bash", "-n", str(TRANSLATED_AUDIT_SCRIPT_PATH)],
        check=True,
    )
    script = TRANSLATED_AUDIT_SCRIPT_PATH.read_text(encoding="utf-8")
    assert "asic/config/datasets/production.yaml" in script
    assert "-m asic_pipeline audit-translated" in script
    assert "pooled-to-translated" not in script
    assert "translated-to-cleaned" not in script
    assert "build-time-blocks" not in script
    assert "--overwrite" not in script


def test_cross_hospital_batch_script_is_read_only_and_orders_prerequisite() -> None:
    assert os.access(CROSS_HOSPITAL_AUDIT_SCRIPT_PATH, os.X_OK)
    subprocess.run(
        ["bash", "-n", str(CROSS_HOSPITAL_AUDIT_SCRIPT_PATH)],
        check=True,
    )
    script = CROSS_HOSPITAL_AUDIT_SCRIPT_PATH.read_text(encoding="utf-8")
    assert "asic/config/datasets/production.yaml" in script
    commands = [
        "-m asic_pipeline audit-translated",
        "-m asic_pipeline audit-cross-hospital",
    ]
    positions = [script.index(command) for command in commands]
    assert positions == sorted(positions)
    assert "pooled-to-translated" not in script
    assert "translated-to-cleaned" not in script
    assert "build-time-blocks" not in script
    assert "--overwrite" not in script
