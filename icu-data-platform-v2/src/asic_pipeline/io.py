from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from asic_pipeline.contracts import InputContract
from asic_pipeline.errors import InputReadError


@dataclass(frozen=True)
class PooledTable:
    name: str
    path: Path
    parquet: pq.ParquetFile

    @property
    def row_count(self) -> int:
        return self.parquet.metadata.num_rows

    @property
    def row_group_count(self) -> int:
        return self.parquet.metadata.num_row_groups

    @property
    def schema(self) -> pa.Schema:
        return self.parquet.schema_arrow


@dataclass(frozen=True)
class PooledInput:
    static: PooledTable
    dynamic: PooledTable


def _open_table(name: str, path: Path) -> PooledTable:
    if not path.is_file():
        raise InputReadError(f"Pooled {name} file does not exist: {path}")
    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:
        raise InputReadError(f"Could not open pooled {name} Parquet file {path}: {exc}") from exc
    return PooledTable(name=name, path=path, parquet=parquet)


def open_pooled_input(pooled_dir: Path, contract: InputContract) -> PooledInput:
    return PooledInput(
        static=_open_table("static", pooled_dir / contract.static.filename),
        dynamic=_open_table("dynamic", pooled_dir / contract.dynamic.filename),
    )

