from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from asic_pipeline.config import load_config
from asic_pipeline.categorical_inventory import load_categorical_inventory_config
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.data_quality import load_cross_hospital_audit_config
from asic_pipeline.translations import load_translation_config
from asic_pipeline.translated_input import load_translated_input_config


PROJECT_ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("dataset_context", ["demo", "mock", "production"])
def test_one_committed_profile_configures_all_context_operations(
    dataset_context: str,
) -> None:
    path = (
        PROJECT_ROOT
        / "asic"
        / "config"
        / "datasets"
        / f"{dataset_context}.yaml"
    )

    audit = load_config(path)
    translation = load_translation_config(path)
    inventory = load_categorical_inventory_config(path)
    translated = load_translated_input_config(path)
    cross_hospital = load_cross_hospital_audit_config(path)

    assert audit.dataset_context == dataset_context
    assert translation.dataset_context == dataset_context
    assert inventory.dataset_context == dataset_context
    assert translated.dataset_context == dataset_context
    assert audit.contract_path == translation.source_contract_path
    assert audit.contract_path == inventory.source_contract_path
    assert audit.paths.pooled == translation.source_pooled_dir
    assert audit.paths.pooled == inventory.source_pooled_dir
    assert translated.translated_dir == translation.output_translated_dir
    assert translated.cleaned_dir.name == "cleaned"
    assert translated.contract_path.name == "contract.yaml"
    assert cross_hospital.dataset_context == dataset_context
    assert cross_hospital.translated_contract_path == translated.contract_path
    assert cross_hospital.translated_dir == translated.translated_dir
    assert cross_hospital.policy_path.name == "data_quality_audit.yaml"


def test_mock_config_cannot_write_reports_to_production_tree(tmp_path: Path) -> None:
    config_path = tmp_path / "asic" / "config" / "datasets" / "mock.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "dataset_context": "mock",
                "contract": "contract.yaml",
                "paths": {
                    "pooled": str(tmp_path / "data" / "mock" / "pooled"),
                    "reports": str(tmp_path / "reports" / "production"),
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="production"):
        load_config(config_path)


def test_demo_config_cannot_write_reports_to_mock_tree(tmp_path: Path) -> None:
    config_path = tmp_path / "asic" / "config" / "datasets" / "demo.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "dataset_context": "demo",
                "contract": "contract.yaml",
                "paths": {
                    "pooled": str(tmp_path / "data" / "demo" / "pooled"),
                    "reports": str(tmp_path / "reports" / "mock"),
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="mock"):
        load_config(config_path)
