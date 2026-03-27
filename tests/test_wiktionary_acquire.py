from __future__ import annotations

from pathlib import Path

from open_dictionary.sources.wiktionary.acquire import acquire_snapshot
from open_dictionary.sources.wiktionary.contracts import SnapshotRequest


def test_acquire_snapshot_registers_existing_local_archive(gzip_jsonl_path: Path, tmp_path: Path) -> None:
    # This case covers the most important low-bandwidth workflow:
    # ingesting from a local archive that already exists on disk.
    artifact = acquire_snapshot(
        SnapshotRequest(
            workdir=tmp_path / "workdir",
            archive_path=gzip_jsonl_path,
        )
    )

    assert artifact.acquisition_mode == "register_local"
    assert artifact.compression == "gzip"
    assert artifact.archive_path == gzip_jsonl_path
    assert artifact.archive_size_bytes > 0
    assert artifact.archive_sha256


def test_acquire_snapshot_keeps_explicit_local_archive_path(
    gzip_jsonl_path: Path,
    tmp_path: Path,
) -> None:
    # This case locks in the current contract: an explicit local archive path is
    # treated as the canonical artifact location and is not silently copied.

    artifact = acquire_snapshot(
        SnapshotRequest(
            workdir=tmp_path / "workdir",
            source_url="https://example.com/snapshot.jsonl.gz",
            archive_path=gzip_jsonl_path,
            overwrite_download=True,
        )
    )

    assert artifact.archive_path == gzip_jsonl_path
    assert artifact.source_url == "https://example.com/snapshot.jsonl.gz"


def test_acquire_snapshot_raises_when_local_archive_is_missing(tmp_path: Path) -> None:
    # This case prevents the stage from pretending it registered a local source
    # when the caller actually pointed at a nonexistent file.
    missing = tmp_path / "missing.jsonl.gz"

    try:
        acquire_snapshot(
            SnapshotRequest(
                workdir=tmp_path / "workdir",
                archive_path=missing,
            )
        )
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("Expected FileNotFoundError for missing local archive")
