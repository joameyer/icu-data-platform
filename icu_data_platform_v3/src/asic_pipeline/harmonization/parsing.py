from __future__ import annotations

from dataclasses import dataclass
import math
import re


DIRECT_NUMERIC_PATTERN = re.compile(
    r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?$"
)
DECIMAL_COMMA_PATTERN = re.compile(
    r"^[+-]?(?:(?:\d+(?:,\d*)?)|(?:,\d+))(?:[eE][+-]?\d+)?$"
)
RATIO_PATTERN = re.compile(
    r"^([+-]?(?:(?:\d+(?:[.,]\d*)?)|(?:[.,]\d+)))\s*[:/]\s*"
    r"([+-]?(?:(?:\d+(?:[.,]\d*)?)|(?:[.,]\d+)))$"
)
PERCENTAGE_PATTERN = re.compile(
    r"^([+-]?(?:(?:\d+(?:[.,]\d*)?)|(?:[.,]\d+)))\s*%$"
)
THRESHOLD_PATTERN = re.compile(
    r"^(?:<=|>=|<|>)\s*([+-]?(?:(?:\d+(?:[.,]\d*)?)|(?:[.,]\d+)))$"
)


@dataclass(frozen=True)
class NumericListParseResult:
    recognized_list: bool
    values: tuple[float | None, ...]
    numeric_element_count: int
    approved_missing_element_count: int
    unresolved_element_count: int

    @property
    def element_count(self) -> int:
        return len(self.values)

    @property
    def resolved(self) -> bool:
        return self.recognized_list and self.unresolved_element_count == 0


def parse_direct_numeric(token: str) -> float | None:
    value = token.strip()
    if not DIRECT_NUMERIC_PATTERN.fullmatch(value):
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def parse_decimal_comma_numeric(token: str) -> float | None:
    value = token.strip()
    if "." in value or not DECIMAL_COMMA_PATTERN.fullmatch(value):
        return None
    try:
        parsed = float(value.replace(",", "."))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_candidate_number(token: str) -> float | None:
    direct = parse_direct_numeric(token)
    if direct is not None:
        return direct
    return parse_decimal_comma_numeric(token)


def parse_ratio_numeric(token: str) -> float | None:
    match = RATIO_PATTERN.fullmatch(token.strip())
    if match is None:
        return None
    numerator = _parse_candidate_number(match.group(1))
    denominator = _parse_candidate_number(match.group(2))
    if numerator is None or denominator in {None, 0.0}:
        return None
    result = numerator / denominator
    return result if math.isfinite(result) else None


def parse_explicit_percentage_fraction(token: str) -> float | None:
    match = PERCENTAGE_PATTERN.fullmatch(token.strip())
    if match is None:
        return None
    value = _parse_candidate_number(match.group(1))
    if value is None:
        return None
    result = value / 100.0
    return result if math.isfinite(result) else None


def parse_threshold_boundary(token: str) -> float | None:
    match = THRESHOLD_PATTERN.fullmatch(token.strip())
    if match is None:
        return None
    return _parse_candidate_number(match.group(1))


def parse_numeric_list(
    token: str,
    approved_missing_tokens: tuple[str, ...],
) -> NumericListParseResult:
    value = token.strip()
    if not (value.startswith("[") and value.endswith("]")):
        return NumericListParseResult(False, (), 0, 0, 0)
    content = value[1:-1].strip()
    if content == "":
        return NumericListParseResult(True, (), 0, 0, 0)
    approved_missing = {
        item.strip().casefold() for item in approved_missing_tokens
    }
    parsed: list[float | None] = []
    numeric_count = 0
    missing_count = 0
    unresolved_count = 0
    for raw_element in content.split(","):
        element = raw_element.strip()
        if not element:
            parsed.append(None)
            unresolved_count += 1
            continue
        if element.casefold() in approved_missing:
            parsed.append(None)
            missing_count += 1
            continue
        numeric = parse_direct_numeric(element)
        if numeric is None:
            parsed.append(None)
            unresolved_count += 1
            continue
        parsed.append(numeric)
        numeric_count += 1
    return NumericListParseResult(
        True,
        tuple(parsed),
        numeric_count,
        missing_count,
        unresolved_count,
    )
