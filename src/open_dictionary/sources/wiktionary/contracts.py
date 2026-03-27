from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


AcquisitionMode = Literal["download", "register_local"]
CompressionKind = Literal["gzip", "plain"]
SourceItemType = Literal["record", "anomaly"]


@dataclass(frozen=True)
class SnapshotRequest:
    workdir: Path
    source_url: str | None = None
    archive_path: Path | None = None
    overwrite_download: bool = False
    source_name: str = "wiktionary"


@dataclass(frozen=True)
class SnapshotArtifact:
    source_name: str
    source_url: str | None
    archive_path: Path
    workdir: Path
    acquisition_mode: AcquisitionMode
    compression: CompressionKind
    archive_sha256: str
    archive_size_bytes: int


@dataclass(frozen=True)
class SourceRecord:
    source_line: int
    source_byte_offset: int
    json_text: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class SourceAnomaly:
    source_line: int
    source_byte_offset: int
    anomaly_type: str
    detail: str
    json_text: str | None


@dataclass(frozen=True)
class RawEnvelope:
    source_line: int
    source_byte_offset: int
    word: str | None
    lang: str | None
    lang_code: str | None
    pos: str | None
    payload_json: str
