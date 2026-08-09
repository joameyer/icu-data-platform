from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from asic_pipeline.errors import InventoryError


# These fields may contain protected source filenames, stay identifiers, or
# row-level identifier evidence. They are allowed only in the private bundle.
PROTECTED_FIELD_NAMES = frozenset(
    {
        "absolute_path",
        "relative_path",
        "source_filename",
        "filename_stay_id",
        "identifier_values",
        "stay_ids",
        "stay_set_digest",
        "source_files",
    }
)


def _walk(value: Any, location: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            if key_text in PROTECTED_FIELD_NAMES:
                raise InventoryError(
                    f"Protected field {key_text!r} reached review payload at {location}"
                )
            _walk(child, f"{location}.{key_text}")
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for index, child in enumerate(value):
            _walk(child, f"{location}[{index}]")


def assert_review_payload_is_safe(value: Any) -> None:
    """Fail closed if a review report contains a protected field."""

    _walk(value, "review")
