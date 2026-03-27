from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
import urllib.parse


def resolve_archive_path(
    *,
    workdir: Path,
    source_url: str | None,
    archive_path: Path | None,
) -> Path:
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    if archive_path is not None:
        return Path(archive_path)

    if source_url:
        parsed = urllib.parse.urlparse(source_url)
        archive_name = Path(parsed.path or "wiktextract.jsonl.gz").name
        return workdir / archive_name

    return workdir / "wiktextract.jsonl.gz"


def detect_compression(path: Path) -> str:
    if path.suffix == ".gz":
        return "gzip"
    return "plain"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def prepare_atomic_path(path: Path) -> Path:
    suffix = "".join(path.suffixes)
    if suffix:
        return path.with_name(path.name + ".part")
    return path.with_suffix(".part")


def install_atomic_file(temp_path: Path, final_path: Path) -> None:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.replace(final_path)


def copy_file_atomic(
    *,
    source_path: Path,
    destination_path: Path,
    overwrite: bool,
) -> Path:
    if destination_path.exists():
        if destination_path.is_dir():
            raise IsADirectoryError(f"Destination {destination_path} is a directory")
        if not overwrite:
            return destination_path

    temp_path = prepare_atomic_path(destination_path)
    temp_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, temp_path)
    install_atomic_file(temp_path, destination_path)
    return destination_path
