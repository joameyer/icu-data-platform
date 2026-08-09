from __future__ import annotations

import pyarrow as pa
import pytest

from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    harmonize_reviewed_column_batch,
    load_harmonization_bootstrap_registry,
)


def test_bootstrap_registry_is_reviewed_but_fail_closed(project_root) -> None:
    registry = load_harmonization_bootstrap_registry(
        project_root / "asic/config/harmonization/bootstrap.yaml"
    )

    assert registry.bootstrap_policy_version == "0.1"
    assert len(registry.rules) == 18
    assert registry.production_execution_enabled is False
    assert registry.allow_production_reads is False
    assert registry.allow_artifact_writes is False
    assert registry.allow_hospital_concatenation is False
    assert registry.rule_for("asic_UK01", "dynamic", "I:E") is None
    with pytest.raises(HarmonizationError, match="production execution is blocked"):
        registry.require_production_ready()


def test_height_harmonization_preserves_list_shape_and_missing_elements(
    project_root,
) -> None:
    registry = load_harmonization_bootstrap_registry(
        project_root / "asic/config/harmonization/bootstrap.yaml"
    )
    rule = registry.rule_for("asic_UK00", "static", "heightcm")
    assert rule is not None

    result = harmonize_reviewed_column_batch(
        pa.array(
            [
                "[170.0]",
                "[190.0, 180.0]",
                "[8.0, nan, 175.0]",
                "",
                None,
            ]
        ),
        rule,
    )

    assert result.values.type == pa.list_(pa.float64())
    assert result.values.to_pylist() == [
        [170.0],
        [190.0, 180.0],
        [8.0, None, 175.0],
        None,
        None,
    ]
    assert result.parse_status.to_pylist() == [
        "numeric_list_parsed",
        "numeric_list_parsed",
        "numeric_list_parsed_with_missing_elements",
        "literal_empty_missing",
        "source_schema_absent",
    ]
    assert result.metrics.input_cell_count == 5
    assert result.metrics.custom_parsed_count == 3
    assert result.metrics.numeric_list_cell_count == 3
    assert result.metrics.numeric_list_element_count == 6
    assert result.metrics.numeric_list_numeric_element_count == 5
    assert result.metrics.numeric_list_approved_missing_element_count == 1
    assert result.metrics.numeric_list_unresolved_element_count == 0
    assert result.metrics.cell_accounting_is_conserved is True
    assert result.metrics.list_element_accounting_is_conserved is True
    result.require_resolved()


def test_height_unknown_element_blocks_without_exposing_token(project_root) -> None:
    registry = load_harmonization_bootstrap_registry(
        project_root / "asic/config/harmonization/bootstrap.yaml"
    )
    rule = registry.rule_for("asic_UK00", "static", "heightcm")
    assert rule is not None
    result = harmonize_reviewed_column_batch(
        pa.array(["[170.0, unknown]"]),
        rule,
    )

    assert result.values.to_pylist() == [None]
    assert result.metrics.unresolved_count == 1
    assert result.metrics.numeric_list_unresolved_element_count == 1
    with pytest.raises(HarmonizationError) as captured:
        result.require_resolved()
    assert "unknown" not in str(captured.value)

    nonfinite = harmonize_reviewed_column_batch(
        pa.array(["[1e999]"]),
        rule,
    )
    assert nonfinite.publication_blocked is True
    assert nonfinite.metrics.numeric_list_unresolved_element_count == 1


def test_reviewed_categorical_decimal_sentinel_and_alias_rules(project_root) -> None:
    registry = load_harmonization_bootstrap_registry(
        project_root / "asic/config/harmonization/bootstrap.yaml"
    )

    cluster = registry.rule_for("asic_UK00", "static", "Cluster-ID")
    assert cluster is not None
    cluster_result = harmonize_reviewed_column_batch(
        pa.array(["C1", "C3", "C5"]), cluster
    )
    assert cluster_result.values.to_pylist() == ["C1", "C3", None]
    assert cluster_result.metrics.approved_categorical_count == 2
    assert cluster_result.metrics.unresolved_count == 1

    peep = registry.rule_for("asic_UK01", "dynamic", "PEEP")
    assert peep is not None
    peep_result = harmonize_reviewed_column_batch(
        pa.array(["1,5", "2.5", "unresolved"]), peep
    )
    assert peep_result.values.to_pylist() == [1.5, 2.5, None]
    assert peep_result.parse_status.to_pylist() == [
        "decimal_comma_parsed",
        "direct_numeric",
        "unresolved",
    ]

    hosp_los = registry.rule_for("asic_UK08", "static", "Liegedauer_KH")
    assert hosp_los is not None
    los_result = harmonize_reviewed_column_batch(
        pa.array(["-1", "-1.0", "12"]), hosp_los
    )
    assert los_result.values.to_pylist() == [None, None, 12.0]
    assert los_result.metrics.approved_missing_sentinel_count == 2
    assert los_result.metrics.direct_numeric_count == 1
    los_result.require_resolved()

    nbsp_alias = registry.rule_for(
        "asic_UK01", "static", "Dialyse_(dialysefreie_Tage)\u00a0"
    )
    assert nbsp_alias is not None
    alias_result = harmonize_reviewed_column_batch(
        pa.array(["", None]), nbsp_alias
    )
    assert alias_result.values.to_pylist() == [None, None]
    assert alias_result.parse_status.to_pylist() == [
        "literal_empty_missing",
        "source_schema_absent",
    ]
    alias_result.require_resolved()
