"""Extraction helpers for local Wiktionary snapshot archives."""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

from .artifacts import install_atomic_file, prepare_atomic_path
from .progress import ByteProgressPrinter


def extract_wiktionary_dump(
    source: Path,
    destination: Path,
    *,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Extract a ``.jsonl.gz`` archive to a plain JSONL file."""

    source_path = Path(source)
    if not source_path.is_file():
        raise FileNotFoundError(f"Source archive {source_path} does not exist")

    destination_path = Path(destination)
    if destination_path.exists():
        if destination_path.is_dir():
            raise IsADirectoryError(f"Destination {destination_path} is a directory")
        if not overwrite:
            print(f"Extraction skipped; {destination_path} already exists.", file=sys.stderr)
            return destination_path

    temp_path = prepare_atomic_path(destination_path)
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    total_size = source_path.stat().st_size
    progress = ByteProgressPrinter("Extracting", total_size)

    try:
        with source_path.open("rb") as raw_handle:
            with gzip.GzipFile(fileobj=raw_handle) as gz_handle:
                with temp_path.open("wb") as out_handle:
                    while True:
                        chunk = gz_handle.read(chunk_size)
                        if not chunk:
                            break
                        out_handle.write(chunk)
                        progress.report(raw_handle.tell())
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise

    progress.finalize(total_size)
    install_atomic_file(temp_path, destination_path)
    return destination_path


__all__ = ["extract_wiktionary_dump"]
