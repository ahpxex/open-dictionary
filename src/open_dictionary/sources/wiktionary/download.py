"""Download helpers for Wiktionary / Wiktextract snapshot archives."""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

from .artifacts import install_atomic_file, prepare_atomic_path
from .progress import ByteProgressPrinter


DEFAULT_WIKTIONARY_SOURCE_URL = "https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz"


def download_wiktionary_dump(
    destination: Path,
    *,
    url: str = DEFAULT_WIKTIONARY_SOURCE_URL,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Download a snapshot archive to ``destination`` with atomic writes."""

    destination_path = Path(destination)
    if destination_path.exists():
        if destination_path.is_dir():
            raise IsADirectoryError(f"Destination {destination_path} is a directory")
        if not overwrite:
            print(f"Download skipped; {destination_path} already exists.", file=sys.stderr)
            return destination_path

    temp_path = prepare_atomic_path(destination_path)
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "opend/ingest-snapshot"},
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
    except urllib.error.URLError as exc:  # pragma: no cover - network failure guard
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to download Wiktionary dump: {exc}") from exc
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise

    install_atomic_file(temp_path, destination_path)
    return destination_path


__all__ = ["DEFAULT_WIKTIONARY_SOURCE_URL", "download_wiktionary_dump"]
