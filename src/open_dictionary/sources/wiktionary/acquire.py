from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

from open_dictionary.wikitionary.progress import ByteProgressPrinter

from .artifacts import (
    copy_file_atomic,
    detect_compression,
    install_atomic_file,
    prepare_atomic_path,
    resolve_archive_path,
    sha256_file,
)
from .contracts import SnapshotArtifact, SnapshotRequest


DEFAULT_WIKTIONARY_SOURCE_URL = "https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz"


def acquire_snapshot(request: SnapshotRequest) -> SnapshotArtifact:
    archive_path = resolve_archive_path(
        workdir=request.workdir,
        source_url=request.source_url,
        archive_path=request.archive_path,
    )

    if request.archive_path is not None:
        source_path = Path(request.archive_path)
        if not source_path.is_file():
            raise FileNotFoundError(f"Local archive {source_path} does not exist")
        mode = "register_local"
        if source_path.resolve() != archive_path.resolve():
            archive_path = copy_file_atomic(
                source_path=source_path,
                destination_path=archive_path,
                overwrite=request.overwrite_download,
            )
    else:
        if not request.source_url:
            raise ValueError("Either source_url or archive_path must be provided")
        archive_path = _download_snapshot(
            destination_path=archive_path,
            source_url=request.source_url,
            overwrite=request.overwrite_download,
        )
        mode = "download"

    compression = detect_compression(archive_path)
    archive_sha256 = sha256_file(archive_path)
    archive_size_bytes = archive_path.stat().st_size

    return SnapshotArtifact(
        source_name=request.source_name,
        source_url=request.source_url,
        archive_path=archive_path,
        workdir=Path(request.workdir),
        acquisition_mode=mode,
        compression=compression,  # type: ignore[arg-type]
        archive_sha256=archive_sha256,
        archive_size_bytes=archive_size_bytes,
    )


def _download_snapshot(
    *,
    destination_path: Path,
    source_url: str,
    overwrite: bool,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    if destination_path.exists():
        if destination_path.is_dir():
            raise IsADirectoryError(f"Destination {destination_path} is a directory")
        if not overwrite:
            print(f"Download skipped; {destination_path} already exists.", file=sys.stderr)
            return destination_path

    temp_path = prepare_atomic_path(destination_path)
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    request = urllib.request.Request(
        source_url,
        headers={"User-Agent": "open-dictionary/raw-ingest"},
    )

    downloaded = 0
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            total_size = int(response.headers.get("Content-Length", "0") or 0)
            progress = ByteProgressPrinter("Downloading", total_size)

            with temp_path.open("wb") as out_handle:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_handle.write(chunk)
                    downloaded += len(chunk)
                    progress.report(downloaded)

            progress.finalize(downloaded)
    except urllib.error.URLError as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to download Wiktionary snapshot: {exc}") from exc
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise

    install_atomic_file(temp_path, destination_path)
    return destination_path
