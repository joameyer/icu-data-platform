from __future__ import annotations

from typing import Iterable


def normalize_icd_code(value: object) -> str:
    return "".join(str(value).upper().replace(".", "").split())


def phenotype_icd10(
    diagnoses: Iterable[dict[str, object]],
    prefixes: dict[str, list[str]],
) -> dict[str, bool | None]:
    """Prefix-match ICD-10 only; any ICD-9 input leaves every phenotype gated."""

    rows = list(diagnoses)
    has_icd9 = any(row.get("icd_version") == 9 for row in rows)
    normalized = [
        normalize_icd_code(row.get("icd_code"))
        for row in rows
        if row.get("icd_version") == 10 and row.get("icd_code") is not None
    ]
    output: dict[str, bool | None] = {}
    for name, values in prefixes.items():
        matched = any(code.startswith(tuple(values)) for code in normalized)
        output[name] = True if matched else (None if has_icd9 else False)
    return output
