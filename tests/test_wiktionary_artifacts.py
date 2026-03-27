from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from open_dictionary.sources.wiktionary.artifacts import (
    copy_file_atomic,
    detect_compression,
    prepare_atomic_path,
    resolve_archive_path,
    sha256_file,
)


def test_resolve_archive_path_uses_url_filename(tmp_path: Path) -> None:
    # This case ensures that source URLs deterministically map to local artifact names.
    path = resolve_archive_path(
        workdir=tmp_path,
        source_url="https://example.com/dumps/raw-wiktextract-data.jsonl.gz",
        archive_path=None,
    )

    assert path == tmp_path / "raw-wiktextract-data.jsonl.gz"


def test_resolve_archive_path_prefers_explicit_archive_path(tmp_path: Path) -> None:
    # This case protects the local-archive workflow used when full snapshots are
    # already present on disk and should not be renamed by URL heuristics.
    explicit = tmp_path / "already-downloaded.jsonl.gz"

    path = resolve_archive_path(
        workdir=tmp_path,
        source_url="https://example.com/ignored.jsonl.gz",
        archive_path=explicit,
    )

    assert path == explicit


def test_prepare_atomic_path_appends_part_suffix(tmp_path: Path) -> None:
    # This case verifies that artifact writes go through a temporary file path
    # instead of writing directly into the final filename.
    final_path = tmp_path / "sample.jsonl.gz"

    temp_path = prepare_atomic_path(final_path)

    assert temp_path.name == "sample.jsonl.gz.part"


def test_detect_compression_distinguishes_gzip_and_plain(tmp_path: Path) -> None:
    # This case keeps the stage honest about whether it should stream through
    # gzip decoding or read a plain JSONL file directly.
    assert detect_compression(tmp_path / "sample.jsonl.gz") == "gzip"
    assert detect_compression(tmp_path / "sample.jsonl") == "plain"


def test_sha256_file_matches_expected_digest(tmp_path: Path) -> None:
    # This case verifies the dedupe key for snapshots, since the whole raw
    # ingest idempotency story depends on a stable archive hash.
    path = tmp_path / "payload.bin"
    payload = b"open-dictionary"
    path.write_bytes(payload)

    digest = sha256_file(path)

    assert digest == hashlib.sha256(payload).hexdigest()


def test_copy_file_atomic_copies_source_when_destination_is_missing(tmp_path: Path) -> None:
    # This case verifies the basic local-archive acquisition path.
    source = tmp_path / "source.jsonl.gz"
    source.write_bytes(b"payload")
    destination = tmp_path / "nested" / "destination.jsonl.gz"

    copied = copy_file_atomic(
        source_path=source,
        destination_path=destination,
        overwrite=False,
    )

    assert copied == destination
    assert destination.read_bytes() == b"payload"
    assert not (destination.parent / "destination.jsonl.gz.part").exists()


def test_copy_file_atomic_reuses_destination_when_overwrite_is_disabled(tmp_path: Path) -> None:
    # This case ensures we do not silently replace an existing artifact unless
    # the caller explicitly requested overwrite behavior.
    source = tmp_path / "source.jsonl.gz"
    source.write_bytes(b"new")
    destination = tmp_path / "destination.jsonl.gz"
    destination.write_bytes(b"old")

    copied = copy_file_atomic(
        source_path=source,
        destination_path=destination,
        overwrite=False,
    )

    assert copied == destination
    assert destination.read_bytes() == b"old"


def test_copy_file_atomic_raises_for_directory_destination(tmp_path: Path) -> None:
    # This case protects the artifact layer from clobbering directories when
    # users pass the wrong path.
    source = tmp_path / "source.jsonl.gz"
    source.write_bytes(b"payload")
    destination = tmp_path / "directory-target"
    destination.mkdir()

    with pytest.raises(IsADirectoryError):
        copy_file_atomic(
            source_path=source,
            destination_path=destination,
            overwrite=True,
        )
