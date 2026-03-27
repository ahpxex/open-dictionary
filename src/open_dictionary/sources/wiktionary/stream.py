from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterator

from .contracts import SnapshotArtifact, SourceAnomaly, SourceRecord


UTF8_BOM = b"\xef\xbb\xbf"


def iter_source_items(
    artifact: SnapshotArtifact,
) -> Iterator[SourceRecord | SourceAnomaly]:
    archive_path = Path(artifact.archive_path)
    if not archive_path.is_file():
        raise FileNotFoundError(f"Archive {archive_path} does not exist")

    with archive_path.open("rb") as raw_handle:
        payload_handle = _open_payload_stream(raw_handle, artifact.compression)

        for line_number, raw_line in enumerate(payload_handle, start=1):
            if not raw_line.strip():
                continue

            if line_number == 1 and raw_line.startswith(UTF8_BOM):
                raw_line = raw_line[len(UTF8_BOM) :]

            json_bytes = raw_line.rstrip(b"\r\n")
            if not json_bytes:
                continue

            byte_offset = raw_handle.tell()

            try:
                json_text = json_bytes.decode("utf-8")
            except UnicodeDecodeError as exc:
                yield SourceAnomaly(
                    source_line=line_number,
                    source_byte_offset=byte_offset,
                    anomaly_type="invalid_utf8",
                    detail=str(exc),
                    json_text=None,
                )
                continue

            try:
                payload = json.loads(json_text)
            except json.JSONDecodeError as exc:
                yield SourceAnomaly(
                    source_line=line_number,
                    source_byte_offset=byte_offset,
                    anomaly_type="invalid_json",
                    detail=f"{exc.msg} (column {exc.colno})",
                    json_text=json_text,
                )
                continue

            if not isinstance(payload, dict):
                yield SourceAnomaly(
                    source_line=line_number,
                    source_byte_offset=byte_offset,
                    anomaly_type="non_object_json",
                    detail=f"Expected object, got {type(payload).__name__}",
                    json_text=json_text,
                )
                continue

            yield SourceRecord(
                source_line=line_number,
                source_byte_offset=byte_offset,
                json_text=json_text,
                payload=payload,
            )


def _open_payload_stream(raw_handle, compression: str):
    if compression == "gzip":
        return gzip.GzipFile(fileobj=raw_handle)
    return raw_handle
