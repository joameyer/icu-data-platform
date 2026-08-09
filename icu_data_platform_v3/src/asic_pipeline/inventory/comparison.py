from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from asic_pipeline.config import load_yaml_mapping, required_string, resolve_path
from asic_pipeline.errors import ConfigurationError


@dataclass(frozen=True)
class PooledReferenceConfig:
    pooled_static: Path
    stay_id_column: str
    hospital_code_column: str
    status: str


@dataclass(frozen=True)
class FolderPooledComparison:
    status: str
    raw_unique_stays: int
    pooled_unique_stays: int | None
    exact_identifier_matches: int | None
    prefix_identifier_matches: int | None
    prefix_matches_by_pooled_hospital_code: dict[str, int]
    limitation: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_pooled_reference_config(path: Path) -> PooledReferenceConfig:
    raw = load_yaml_mapping(path, "Pooled comparison reference")
    status = required_string(raw, "status", "comparison")
    if status != "untrusted_read_only_comparison":
        raise ConfigurationError(
            "Comparison reference must be marked untrusted_read_only_comparison"
        )
    paths = raw.get("paths")
    identifiers = raw.get("identifiers")
    if not isinstance(paths, dict) or not isinstance(identifiers, dict):
        raise ConfigurationError(
            "Comparison reference requires paths and identifiers mappings"
        )
    return PooledReferenceConfig(
        pooled_static=resolve_path(
            required_string(paths, "pooled_static", "comparison.paths"), path
        ),
        stay_id_column=required_string(
            identifiers, "stay_id_column", "comparison.identifiers"
        ),
        hospital_code_column=required_string(
            identifiers, "hospital_code_column", "comparison.identifiers"
        ),
        status=status,
    )


def compare_folder_stays_to_pooled(
    raw_stay_ids: set[str],
    reference: PooledReferenceConfig,
) -> FolderPooledComparison:
    if not reference.pooled_static.is_file():
        return FolderPooledComparison(
            status="reference_unavailable",
            raw_unique_stays=len(raw_stay_ids),
            pooled_unique_stays=None,
            exact_identifier_matches=None,
            prefix_identifier_matches=None,
            prefix_matches_by_pooled_hospital_code={},
            limitation="The configured untrusted pooled static artifact was not accessible.",
        )
    parquet = pq.ParquetFile(reference.pooled_static)
    required = {reference.stay_id_column, reference.hospital_code_column}
    missing = sorted(required - set(parquet.schema_arrow.names))
    if missing:
        return FolderPooledComparison(
            status="reference_schema_unusable",
            raw_unique_stays=len(raw_stay_ids),
            pooled_unique_stays=None,
            exact_identifier_matches=None,
            prefix_identifier_matches=None,
            prefix_matches_by_pooled_hospital_code={},
            limitation=f"Required comparison columns are absent: {missing}",
        )

    pooled_ids: set[str] = set()
    exact_ids: set[str] = set()
    prefix_ids: set[str] = set()
    prefix_ids_by_hospital: dict[str, set[str]] = {}
    for batch in parquet.iter_batches(
        columns=[reference.stay_id_column, reference.hospital_code_column],
        batch_size=65_536,
    ):
        ids = batch.column(0).to_pylist()
        hospital_codes = batch.column(1).to_pylist()
        for pooled_id, hospital_code in zip(ids, hospital_codes, strict=True):
            if not isinstance(pooled_id, str):
                continue
            pooled_ids.add(pooled_id)
            if pooled_id in raw_stay_ids:
                exact_ids.add(pooled_id)
            raw_prefix = pooled_id.rsplit(":", 1)[0] if ":" in pooled_id else None
            if raw_prefix in raw_stay_ids:
                prefix_ids.add(raw_prefix)
                prefix_ids_by_hospital.setdefault(str(hospital_code), set()).add(
                    raw_prefix
                )
    return FolderPooledComparison(
        status="comparison_complete",
        raw_unique_stays=len(raw_stay_ids),
        pooled_unique_stays=len(pooled_ids),
        exact_identifier_matches=len(exact_ids),
        prefix_identifier_matches=len(prefix_ids),
        prefix_matches_by_pooled_hospital_code={
            hospital: len(values)
            for hospital, values in sorted(prefix_ids_by_hospital.items())
        },
        limitation=(
            "The old pooled artifact is lossy and is used only for stay-set evidence; "
            "it cannot establish raw token preservation or hospital identity."
        ),
    )
