from __future__ import annotations

from typing import Any

from .contracts import RawEnvelope, SourceRecord


def project_raw_record(record: SourceRecord) -> RawEnvelope:
    payload = record.payload
    return RawEnvelope(
        source_line=record.source_line,
        source_byte_offset=record.source_byte_offset,
        word=_optional_text(payload.get("word")),
        lang=_optional_text(payload.get("lang")),
        lang_code=_optional_text(payload.get("lang_code")),
        pos=_optional_text(payload.get("pos")),
        payload_json=record.json_text,
    )


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None
