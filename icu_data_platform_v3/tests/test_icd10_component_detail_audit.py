from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from asic_pipeline.config import InventoryConfig, InventoryPaths
from asic_pipeline.errors import HarmonizationError
from asic_pipeline.harmonization import (
    HarmonizationReviewConfig,
    ICD10ComponentDetailAuditConfig,
    build_icd10_component_detail_audit,
    load_icd10_component_detail_audit_config,
    load_icd10_component_detail_audit_policy,
    write_icd10_component_detail_audit_bundle,
)
from asic_pipeline.ingestion.config import IngestionConfig
from asic_pipeline.inventory.hashing import sha256_file
from asic_pipeline.inventory.report import _write_private_parquet
from asic_pipeline.schema_tokens.config import SchemaTokenConfig


COMPONENT_RUN = "20260805T134245Z"
STATIC_RUN = "20260805T112746Z"
HOSPITAL_SPECS = {
    "asic_UK00": {"rows": 3676, "parsed": 3676, "components": 93976, "duplicate_cells": 0, "duplicates": 0, "missing": 0, "empty_source": 0, "empty_components": (0, 0, 0)},
    "asic_UK01": {"rows": 1571, "parsed": 1570, "components": 27593, "duplicate_cells": 586, "duplicates": 6000, "missing": 0, "empty_source": 1, "empty_components": (0, 0, 0)},
    "asic_UK02": {"rows": 902, "parsed": 902, "components": 17748, "duplicate_cells": 72, "duplicates": 1000, "missing": 0, "empty_source": 0, "empty_components": (0, 0, 0)},
    "asic_UK03": {"rows": 1360, "parsed": 1336, "components": 47160, "duplicate_cells": 1309, "duplicates": 20000, "missing": 24, "empty_source": 0, "empty_components": (10, 5, 5)},
    "asic_UK04": {"rows": 486, "parsed": 486, "components": 7236, "duplicate_cells": 0, "duplicates": 0, "missing": 0, "empty_source": 0, "empty_components": (0, 0, 0)},
    "asic_UK06": {"rows": 678, "parsed": 678, "components": 16777, "duplicate_cells": 0, "duplicates": 0, "missing": 0, "empty_source": 0, "empty_components": (0, 0, 0)},
    "asic_UK07": {"rows": 2217, "parsed": 2217, "components": 64590, "duplicate_cells": 1888, "duplicates": 20000, "missing": 0, "empty_source": 0, "empty_components": (0, 0, 0)},
    "asic_UK08": {"rows": 5164, "parsed": 5164, "components": 190922, "duplicate_cells": 5157, "duplicates": 49505, "missing": 0, "empty_source": 0, "empty_components": (2, 2, 2)},
}


def _config(tmp_path: Path, project_root: Path) -> ICD10ComponentDetailAuditConfig:
    reports = tmp_path / "reports" / "demo"
    inventory = InventoryConfig(
        dataset_context="demo",
        policy_path=project_root / "asic/config/inventory/policy.yaml",
        comparison_reference_path=None,
        paths=InventoryPaths(
            raw_root=tmp_path / "raw",
            reports=reports,
            runs=tmp_path / "runs" / "demo",
        ),
        source_path=project_root / "asic/config/datasets/demo.yaml",
    )
    ingestion = IngestionConfig(
        inventory=inventory,
        policy_path=project_root / "asic/config/ingestion/policy.yaml",
        data_root=tmp_path / "data" / "demo",
        rows_per_batch=10,
    )
    schema = SchemaTokenConfig(
        ingestion=ingestion,
        policy_path=project_root / "asic/config/schema_tokens/policy.yaml",
    )
    review = HarmonizationReviewConfig(
        schema_tokens=schema,
        policy_path=project_root / "asic/config/harmonization/review_policy.yaml",
    )
    return ICD10ComponentDetailAuditConfig(
        review,
        project_root
        / "asic/config/harmonization/icd10_component_detail_audit.yaml",
    )


def _code(number: int) -> str:
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    first = chr(ord("A") + (number % 26))
    stem = (number // 26) % 100
    suffix_number = number // 2600
    suffix = ""
    if suffix_number:
        digits = []
        while suffix_number:
            digits.append(alphabet[suffix_number % len(alphabet)])
            suffix_number //= len(alphabet)
        suffix = "." + "".join(reversed(digits))[-4:]
    return f"{first}{stem:02d}{suffix}"


def _hospital_values(
    hospital: str, spec: dict[str, int | tuple[int, int, int]]
) -> list[str]:
    parsed = int(spec["parsed"])
    target_components = int(spec["components"])
    duplicate_cells = int(spec["duplicate_cells"])
    duplicate_target = int(spec["duplicates"])
    leading, trailing, interior = spec["empty_components"]
    empty_target = leading + trailing + interior
    unexpected_target = 1 if hospital == "asic_UK08" else 0

    cells = [[_code(index)] for index in range(parsed)]
    for index in range(duplicate_cells):
        cells[index].append(cells[index][0])
    additional_duplicates = duplicate_target - duplicate_cells
    if additional_duplicates:
        cells[0].extend([cells[0][0]] * additional_duplicates)

    current_components = parsed + duplicate_target + empty_target + unexpected_target
    unique_to_add = target_components - current_components
    assert unique_to_add >= 0
    next_code = parsed
    for offset in range(unique_to_add):
        cell_index = offset % parsed
        candidate = _code(next_code)
        next_code += 1
        while candidate in cells[cell_index]:
            candidate = _code(next_code)
            next_code += 1
        cells[cell_index].append(candidate)
    if unexpected_target:
        cells[-1].append("protected-incomplete")

    cursor = 0
    for _ in range(leading):
        cells[cursor].insert(0, "")
        cursor += 1
    for _ in range(trailing):
        cells[cursor].append("")
        cursor += 1
    for _ in range(interior):
        cells[cursor].insert(1, "")
        cursor += 1

    values = [",".join(cell) for cell in cells]
    values.extend(["nan"] * int(spec["missing"]))
    values.extend([""] * int(spec["empty_source"]))
    assert len(values) == int(spec["rows"])
    return values


def _write_input_evidence(config: ICD10ComponentDetailAuditConfig) -> None:
    static_private = (
        config.reports_root / "private" / "static_contract_audit" / STATIC_RUN
    )
    static_private.mkdir(parents=True)
    occurrences = []
    prior_hospitals = []
    for index, (hospital, spec) in enumerate(HOSPITAL_SPECS.items(), start=1):
        values = _hospital_values(hospital, spec)
        occurrences.append(
            {
                "review_item_id": f"R{index:04d}",
                "hospital": hospital,
                "table": "static",
                "physical_name": "icd_source",
                "raw_name": "ICD-10_Codes",
                "raw_occurrence": 1,
                "candidate_target": "icd10_codes",
            }
        )
        field = pa.field(
            "icd_source",
            pa.large_string(),
            metadata={b"raw_name": b"ICD-10_Codes", b"raw_occurrence": b"1"},
        )
        table = pa.Table.from_arrays(
            [pa.array(values, type=pa.large_string())], schema=pa.schema([field])
        )
        hospital_dir = config.ingested_root / hospital
        hospital_dir.mkdir(parents=True)
        static_path = hospital_dir / "static.parquet"
        pq.write_table(table, static_path)
        (hospital_dir / "ingestion_manifest.json").write_text(
            json.dumps(
                {
                    "artifact": "asic_v3_lossless_hospital_ingestion",
                    "artifact_version": "0.2",
                    "hospital": {"canonical_hospital_id": hospital},
                    "outputs": {"static": {"sha256": sha256_file(static_path)}},
                }
            ),
            encoding="utf-8",
        )
        leading, trailing, interior = spec["empty_components"]
        empty_count = leading + trailing + interior
        prior_hospitals.append(
            {
                "hospital": hospital,
                "row_count": int(spec["rows"]),
                "parsed_cell_count": int(spec["parsed"]),
                "component_count": int(spec["components"]),
                "code_candidate_component_count": int(spec["components"])
                - empty_count
                - (1 if hospital == "asic_UK08" else 0),
                "unexpected_component_count": 1 if hospital == "asic_UK08" else 0,
                "empty_component_count": empty_count,
                "cells_with_duplicate_components": int(spec["duplicate_cells"]),
                "duplicate_component_count": int(spec["duplicates"]),
            }
        )
    _write_private_parquet(static_private / "occurrences.parquet", tuple(occurrences))
    (static_private / "static_contract_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_static_contract_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
            }
        ),
        encoding="utf-8",
    )

    component_private = (
        config.reports_root / "private" / "icd10_component_audit" / COMPONENT_RUN
    )
    component_private.mkdir(parents=True)
    (component_private / "icd10_component_audit_manifest.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_component_audit_private",
                "artifact_version": "0.1",
                "dataset_context": "demo",
            }
        ),
        encoding="utf-8",
    )
    component_review = config.reports_root / "review" / "icd10_component_audit"
    component_review.mkdir(parents=True)
    (component_review / f"{COMPONENT_RUN}.json").write_text(
        json.dumps(
            {
                "artifact": "asic_v3_icd10_component_audit_review",
                "artifact_version": "0.1",
                "dataset_context": "demo",
                "metrics": {
                    "scanned_row_count": 16054,
                    "parsed_cell_count": 16029,
                    "component_count": 466002,
                    "code_candidate_component_count": 465975,
                    "unexpected_component_count": 1,
                    "empty_component_count": 26,
                    "cells_with_empty_components": 26,
                    "cells_with_duplicate_components": 9012,
                    "duplicate_component_count": 96505,
                    "nonstring_source_cell_count": 0,
                    "technical_blocking_finding_count": 0,
                },
                "hospital_summaries": prior_hospitals,
            }
        ),
        encoding="utf-8",
    )


def test_icd10_component_detail_audit_accounts_for_all_decision_evidence(
    tmp_path: Path,
    project_root: Path,
) -> None:
    config = _config(tmp_path, project_root)
    _write_input_evidence(config)

    result = build_icd10_component_detail_audit(config, COMPONENT_RUN)

    assert result.overall_status == "fail"
    assert result.technical_blocking_findings == ()
    metrics = result.review_payload["metrics"]
    assert metrics["scanned_row_count"] == 16054
    assert metrics["component_count"] == 466002
    assert metrics["incomplete_component_candidate_count"] == 1
    assert metrics["empty_component_count"] == 26
    assert metrics["leading_empty_component_count"] == 12
    assert metrics["trailing_empty_component_count"] == 7
    assert metrics["interior_empty_component_count"] == 7
    assert metrics["cells_empty_after_empty_removal"] == 0
    assert metrics["duplicate_component_count"] == 96505
    assert metrics["adjacent_duplicate_component_count"] == 96505
    assert metrics["separated_duplicate_component_count"] == 0
    assert metrics["component_count_after_empty_removal"] == 465976
    assert metrics["component_count_after_ordered_deduplication"] == 369471
    assert metrics["cells_changed_by_ordered_deduplication"] == 9012
    assert metrics["technical_blocking_finding_count"] == 0
    assert {item["check"] for item in result.blocking_findings} == {
        "icd10_empty_component_policy_approved",
        "icd10_duplicate_policy_approved",
        "icd10_incomplete_component_policy_approved",
        "icd10_component_detail_evidence_approved",
        "icd10_canonical_list_contract_activated",
    }
    serialized = json.dumps(result.review_payload)
    assert "ICD-10_Codes" not in serialized
    assert "protected-incomplete" not in serialized
    assert any(
        row.get("detail_class") == "incomplete_component_candidate"
        and row.get("raw_component") == "protected-incomplete"
        for row in result.private_examples
    )
    assert any("raw_source_text" in row for row in result.private_examples)

    written = write_icd10_component_detail_audit_bundle(
        result, config.reports_root, "detail_output"
    )
    assert written.private_report_directory is not None
    assert written.private_report_directory.stat().st_mode & 0o777 == 0o700
    for path in written.private_report_directory.iterdir():
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(HarmonizationError, match="will not be overwritten"):
        write_icd10_component_detail_audit_bundle(
            result, config.reports_root, "detail_output"
        )


def test_icd10_component_detail_policy_and_configs_are_fail_closed(
    project_root: Path,
) -> None:
    for context in ("demo", "production"):
        config = load_icd10_component_detail_audit_config(
            project_root / f"asic/config/datasets/{context}.yaml"
        )
        policy = load_icd10_component_detail_audit_policy(config.policy_path)
        assert policy.version == "0.1"
        assert policy.component_audit_basis_run_id == COMPONENT_RUN
        assert policy.rows_per_batch == 10000
        assert policy.reviewed_contract_path.name == (
            "reviewed_icd10_contract_0_1.yaml"
        )
