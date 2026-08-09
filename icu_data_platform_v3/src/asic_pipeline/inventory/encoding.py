from __future__ import annotations

import codecs
from dataclasses import dataclass
from pathlib import Path


UTF8_BOM = codecs.BOM_UTF8


@dataclass(frozen=True)
class EncodingAttempt:
    encoding: str
    status: str
    error: str | None


@dataclass(frozen=True)
class EncodingDetection:
    status: str
    selected_encoding: str | None
    bom: str | None
    attempts: tuple[EncodingAttempt, ...]


def _strict_decode_succeeds(path: Path, encoding: str) -> tuple[bool, str | None]:
    decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                decoder.decode(chunk)
        decoder.decode(b"", final=True)
    except (UnicodeDecodeError, LookupError) as exc:
        return False, str(exc)
    return True, None


def detect_encoding(path: Path, candidates: tuple[str, ...]) -> EncodingDetection:
    with path.open("rb") as stream:
        prefix = stream.read(4)
    has_utf8_bom = prefix.startswith(UTF8_BOM)
    attempts: list[EncodingAttempt] = []
    successes: list[str] = []
    for encoding in candidates:
        succeeded, error = _strict_decode_succeeds(path, encoding)
        attempts.append(
            EncodingAttempt(
                encoding=encoding,
                status="success" if succeeded else "decode_error",
                error=error,
            )
        )
        if succeeded:
            successes.append(encoding)

    if has_utf8_bom and "utf-8-sig" in successes:
        selected = "utf-8-sig"
    elif "utf-8" in successes:
        selected = "utf-8"
    else:
        non_utf8_successes = [
            value for value in successes if value not in {"utf-8", "utf-8-sig"}
        ]
        selected = non_utf8_successes[0] if len(non_utf8_successes) == 1 else None

    if selected is not None:
        status = "selected"
    elif successes:
        status = "ambiguous"
    else:
        status = "unreadable"
    return EncodingDetection(
        status=status,
        selected_encoding=selected,
        bom="utf-8" if has_utf8_bom else None,
        attempts=tuple(attempts),
    )

