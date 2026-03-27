from __future__ import annotations

import json
from pathlib import Path

from open_dictionary.sources.wiktionary.contracts import SnapshotArtifact, SourceAnomaly, SourceRecord
from open_dictionary.sources.wiktionary.project import project_raw_record
from open_dictionary.sources.wiktionary.stream import iter_source_items


def make_artifact(path: Path, compression: str) -> SnapshotArtifact:
    return SnapshotArtifact(
        source_name="wiktionary",
        source_url=None,
        archive_path=path,
        workdir=path.parent,
        acquisition_mode="register_local",
        compression=compression,  # type: ignore[arg-type]
        archive_sha256="dummy",
        archive_size_bytes=path.stat().st_size,
    )


def test_iter_source_items_reads_valid_plain_records(plain_jsonl_path: Path) -> None:
    # This case verifies the happy path for a plain JSONL archive.
    items = list(iter_source_items(make_artifact(plain_jsonl_path, "plain")))

    assert len(items) == 2
    assert all(isinstance(item, SourceRecord) for item in items)
    assert items[0].source_line == 1
    assert items[1].source_line == 2


def test_iter_source_items_reads_valid_gzip_records(gzip_jsonl_path: Path) -> None:
    # This case verifies the gzip stream path used by the real full-size snapshots.
    items = list(iter_source_items(make_artifact(gzip_jsonl_path, "gzip")))

    assert len(items) == 2
    assert all(isinstance(item, SourceRecord) for item in items)
    assert items[0].payload["word"] == "cat"
    assert items[1].payload["word"] == "run"


def test_iter_source_items_emits_anomalies_for_invalid_records(anomaly_jsonl_path: Path) -> None:
    # This case proves that malformed source lines do not crash the stream;
    # they are converted into explicit anomalies instead.
    items = list(iter_source_items(make_artifact(anomaly_jsonl_path, "plain")))

    assert len(items) == 4
    assert isinstance(items[0], SourceRecord)
    assert isinstance(items[1], SourceAnomaly)
    assert isinstance(items[2], SourceAnomaly)
    assert isinstance(items[3], SourceRecord)
    assert items[1].anomaly_type == "invalid_json"
    assert items[2].anomaly_type == "non_object_json"


def test_project_raw_record_extracts_core_raw_fields(sample_record_lines: list[str]) -> None:
    # This case verifies the raw projection contract that all later curation
    # work will depend on.
    payload = json.loads(sample_record_lines[0])
    record = SourceRecord(
        source_line=42,
        source_byte_offset=512,
        json_text=sample_record_lines[0],
        payload=payload,
    )

    envelope = project_raw_record(record)

    assert envelope.source_line == 42
    assert envelope.source_byte_offset == 512
    assert envelope.word == "cat"
    assert envelope.lang == "English"
    assert envelope.lang_code == "en"
    assert envelope.pos == "noun"
    assert envelope.payload_json == sample_record_lines[0]
