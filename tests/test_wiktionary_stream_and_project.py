from __future__ import annotations

import json
from pathlib import Path

import pytest

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


def test_iter_source_items_raises_when_archive_is_missing(tmp_path: Path) -> None:
    # This case exercises the direct stream guard instead of only the higher
    # level acquire_snapshot validation path.
    missing = tmp_path / "missing.jsonl"

    with pytest.raises(FileNotFoundError, match=str(missing)):
        list(iter_source_items(make_artifact(missing, "plain")))


def test_iter_source_items_skips_blank_lines_and_strips_utf8_bom(tmp_path: Path) -> None:
    # This case covers two common source oddities: a UTF-8 BOM on the first
    # line and blank lines mixed into the archive.
    source_path = tmp_path / "bom-and-blank.jsonl"
    source_path.write_bytes(
        b'\xef\xbb\xbf{"word":"cat","lang":"English","lang_code":"en","pos":"noun"}\n'
        b"\n"
        b'{"word":"run","lang":"English","lang_code":"en","pos":"verb"}\n'
    )

    items = list(iter_source_items(make_artifact(source_path, "plain")))

    assert len(items) == 2
    assert all(isinstance(item, SourceRecord) for item in items)
    assert items[0].payload["word"] == "cat"
    assert items[0].source_line == 1
    assert items[1].source_line == 3


def test_iter_source_items_emits_invalid_utf8_anomalies(tmp_path: Path) -> None:
    # This case verifies that undecodable source bytes are preserved as
    # anomalies instead of crashing the stream reader.
    source_path = tmp_path / "invalid-utf8.jsonl"
    source_path.write_bytes(
        b'{"word":"cat","lang":"English","lang_code":"en","pos":"noun"}\n'
        b"\xff\xfe\xfa\n"
    )

    items = list(iter_source_items(make_artifact(source_path, "plain")))

    assert len(items) == 2
    assert isinstance(items[0], SourceRecord)
    assert isinstance(items[1], SourceAnomaly)
    assert items[1].anomaly_type == "invalid_utf8"
    assert items[1].json_text is None


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


def test_project_raw_record_normalizes_blank_and_non_text_fields() -> None:
    # This case covers the optional-text normalization rules that protect the
    # raw layer contract from whitespace-only and non-string source values.
    record = SourceRecord(
        source_line=7,
        source_byte_offset=99,
        json_text='{"word":"   ","lang":123,"lang_code":null,"pos":" verb "}',
        payload={
            "word": "   ",
            "lang": 123,
            "lang_code": None,
            "pos": " verb ",
        },
    )

    envelope = project_raw_record(record)

    assert envelope.word is None
    assert envelope.lang is None
    assert envelope.lang_code is None
    assert envelope.pos == "verb"
