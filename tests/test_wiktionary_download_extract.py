from __future__ import annotations

import gzip
from pathlib import Path
import urllib.error

import pytest

from open_dictionary.sources.wiktionary.download import download_wiktionary_dump
from open_dictionary.sources.wiktionary.extract import extract_wiktionary_dump


class FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._offset = 0
        self.headers = {"Content-Length": str(len(payload))}

    def read(self, chunk_size: int) -> bytes:
        chunk = self._payload[self._offset : self._offset + chunk_size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_download_wiktionary_dump_streams_response_to_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This case protects the basic archive acquisition path without requiring
    # a real network call in the test suite.
    payload = b"fixture-bytes"
    monkeypatch.setattr(
        "open_dictionary.sources.wiktionary.download.urllib.request.urlopen",
        lambda request, timeout=60: FakeResponse(payload),
    )

    destination = tmp_path / "download" / "raw.jsonl.gz"

    result = download_wiktionary_dump(destination, url="https://example.com/raw.jsonl.gz")

    assert result == destination
    assert destination.read_bytes() == payload


def test_download_wiktionary_dump_raises_runtime_error_on_network_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This case ensures callers get a domain-level RuntimeError instead of
    # leaking raw urllib exceptions from the download helper.
    monkeypatch.setattr(
        "open_dictionary.sources.wiktionary.download.urllib.request.urlopen",
        lambda request, timeout=60: (_ for _ in ()).throw(urllib.error.URLError("offline")),
    )

    with pytest.raises(RuntimeError, match="Failed to download Wiktionary dump"):
        download_wiktionary_dump(tmp_path / "raw.jsonl.gz", url="https://example.com/raw.jsonl.gz")


def test_extract_wiktionary_dump_expands_gzip_archive(tmp_path: Path) -> None:
    # This case verifies that the extraction helper correctly expands a gzip
    # snapshot into a plain JSONL file for local inspection workflows.
    source = tmp_path / "raw.jsonl.gz"
    destination = tmp_path / "raw.jsonl"
    expected = b'{"word":"cat"}\n{"word":"run"}\n'
    with gzip.open(source, "wb") as handle:
        handle.write(expected)

    result = extract_wiktionary_dump(source, destination)

    assert result == destination
    assert destination.read_bytes() == expected


def test_extract_wiktionary_dump_reuses_existing_output_when_overwrite_is_disabled(
    tmp_path: Path,
) -> None:
    # This case protects developers from accidentally blowing away an extracted
    # local JSONL file when they are iterating on downstream logic.
    source = tmp_path / "raw.jsonl.gz"
    destination = tmp_path / "raw.jsonl"
    with gzip.open(source, "wb") as handle:
        handle.write(b'{"word":"cat"}\n')
    destination.write_text("existing\n", encoding="utf-8")

    result = extract_wiktionary_dump(source, destination, overwrite=False)

    assert result == destination
    assert destination.read_text(encoding="utf-8") == "existing\n"
