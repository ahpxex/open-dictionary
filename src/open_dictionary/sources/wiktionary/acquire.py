from __future__ import annotations

from pathlib import Path

from .artifacts import (
    copy_file_atomic,
    detect_compression,
    resolve_archive_path,
    sha256_file,
)
from .contracts import SnapshotArtifact, SnapshotRequest
from .download import download_wiktionary_dump


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
        archive_path = download_wiktionary_dump(
            archive_path,
            url=request.source_url,
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
