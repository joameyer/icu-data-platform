import pytest

from mimic_iv_pipeline.errors import MIMICPipelineError
from mimic_iv_pipeline.phenotypes import phenotype_icd10
from mimic_iv_pipeline.privacy import validate_public_aggregate_rows


def test_icd10_prefix_and_icd9_uncertainty() -> None:
    prefixes = {"cirrhosis": ["K74"], "aids": ["B20"]}
    result = phenotype_icd10(
        [
            {"icd_version": 10, "icd_code": "k74.60"},
            {"icd_version": 9, "icd_code": "042"},
        ],
        prefixes,
    )
    assert result == {"cirrhosis": True, "aids": None}


def test_public_aggregate_rejects_identifiers_and_small_cells() -> None:
    validate_public_aggregate_rows([{"eligible_count": 10}])
    with pytest.raises(MIMICPipelineError):
        validate_public_aggregate_rows([{"stay_id": 1, "eligible_count": 10}])
    with pytest.raises(MIMICPipelineError):
        validate_public_aggregate_rows([{"eligible_count": 9}])
