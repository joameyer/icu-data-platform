from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal


CheckStatus = Literal["pass", "fail", "skipped"]
CheckSeverity = Literal["blocking", "advisory"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: CheckStatus
    severity: CheckSeverity
    observed: Any
    expected: Any
    details: str


@dataclass(frozen=True)
class AuditReport:
    artifact: str
    artifact_version: str
    dataset_context: str
    generated_at_utc: str
    overall_status: Literal["pass", "warning", "fail"]
    inputs: dict[str, Any]
    metrics: dict[str, Any]
    checks: tuple[CheckResult, ...]
    limitations: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def overall_status(checks: list[CheckResult]) -> Literal["pass", "warning", "fail"]:
    if any(
        check.status != "pass" and check.severity == "blocking"
        for check in checks
    ):
        return "fail"
    if any(check.status == "fail" for check in checks):
        return "warning"
    return "pass"


def write_json_private(path: Path, value: Any) -> Path:
    """Write JSON atomically with owner-only permissions."""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False, allow_nan=False)
            stream.write("\n")
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    temporary_path.chmod(0o600)
    temporary_path.replace(path)
    path.chmod(0o600)
    return path

