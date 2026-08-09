from __future__ import annotations

import json
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
    contract_version: str
    dataset: str
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


def write_audit_report(report: AuditReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    with temporary_path.open("w", encoding="utf-8") as stream:
        json.dump(report.to_dict(), stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    temporary_path.replace(path)
    return path
