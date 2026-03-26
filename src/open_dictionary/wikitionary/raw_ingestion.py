from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any
import urllib.parse
from uuid import UUID, uuid4

from psycopg import sql

from open_dictionary.config import RuntimeSettings
from open_dictionary.db.connection import get_connection
from open_dictionary.pipeline import complete_run, fail_run, start_run

from .downloader import DEFAULT_WIKTIONARY_URL, download_wiktionary_dump
from .extract import extract_wiktionary_dump
from .transform import JsonlProcessingError, iter_json_lines


RAW_INGEST_STAGE = "wiktionary.raw_ingest"
DEFAULT_RAW_TABLE = "raw.wiktionary_entries"


@dataclass(frozen=True)
class RawIngestionResult:
    run_id: UUID
    snapshot_id: UUID
    archive_path: Path
    extracted_path: Path
    rows_loaded: int
    archive_sha256: str


def run_raw_ingestion(
    *,
    settings: RuntimeSettings,
    workdir: Path,
    url: str = DEFAULT_WIKTIONARY_URL,
    target_table: str = DEFAULT_RAW_TABLE,
    overwrite_download: bool = False,
    overwrite_extract: bool = False,
    skip_download: bool = False,
    skip_extract: bool = False,
) -> RawIngestionResult:
    workdir = Path(workdir)
    archive_path, extracted_path = _resolve_workdir_paths(workdir, url)

    run_config = {
        "source_name": "wiktionary",
        "source_url": url,
        "target_table": target_table,
        "workdir": str(workdir),
        "archive_path": str(archive_path),
        "extracted_path": str(extracted_path),
        "overwrite_download": overwrite_download,
        "overwrite_extract": overwrite_extract,
        "skip_download": skip_download,
        "skip_extract": skip_extract,
    }

    with get_connection(settings) as conn:
        run_id = start_run(conn, stage=RAW_INGEST_STAGE, config=run_config)

    try:
        if skip_download:
            if not archive_path.is_file():
                raise FileNotFoundError(
                    f"Expected archive {archive_path} when --skip-download is used"
                )
        else:
            download_wiktionary_dump(
                archive_path,
                url=url,
                overwrite=overwrite_download,
            )

        if skip_extract:
            if not extracted_path.is_file():
                raise FileNotFoundError(
                    f"Expected JSONL file {extracted_path} when --skip-extract is used"
                )
        else:
            extract_wiktionary_dump(
                archive_path,
                extracted_path,
                overwrite=overwrite_extract,
            )

        archive_sha256 = _sha256_file(archive_path)
        archive_size_bytes = archive_path.stat().st_size

        with get_connection(settings) as conn:
            snapshot_id = _register_snapshot(
                conn,
                run_id=run_id,
                source_url=url,
                archive_path=archive_path,
                extracted_path=extracted_path,
                archive_sha256=archive_sha256,
                archive_size_bytes=archive_size_bytes,
            )
            rows_loaded = _load_raw_entries(
                conn,
                run_id=run_id,
                snapshot_id=snapshot_id,
                jsonl_path=extracted_path,
                target_table=target_table,
            )
            complete_run(
                conn,
                run_id=run_id,
                stats={
                    "snapshot_id": str(snapshot_id),
                    "rows_loaded": rows_loaded,
                    "archive_sha256": archive_sha256,
                    "archive_size_bytes": archive_size_bytes,
                },
            )

        return RawIngestionResult(
            run_id=run_id,
            snapshot_id=snapshot_id,
            archive_path=archive_path,
            extracted_path=extracted_path,
            rows_loaded=rows_loaded,
            archive_sha256=archive_sha256,
        )
    except Exception as exc:
        with get_connection(settings) as conn:
            fail_run(conn, run_id=run_id, error=str(exc))
        raise


def _resolve_workdir_paths(workdir: Path, url: str) -> tuple[Path, Path]:
    workdir.mkdir(parents=True, exist_ok=True)
    parsed = urllib.parse.urlparse(url)
    archive_name = Path(parsed.path or "wiktextract.jsonl.gz").name
    archive_path = workdir / archive_name
    extracted_path = archive_path.with_suffix("")
    return archive_path, extracted_path


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _register_snapshot(
    conn,
    *,
    run_id: UUID,
    source_url: str,
    archive_path: Path,
    extracted_path: Path,
    archive_sha256: str,
    archive_size_bytes: int,
) -> UUID:
    snapshot_id = uuid4()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO meta.source_snapshots (
                snapshot_id,
                run_id,
                source_name,
                source_url,
                archive_path,
                extracted_path,
                archive_sha256,
                archive_size_bytes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_id,
                run_id,
                "wiktionary",
                source_url,
                str(archive_path),
                str(extracted_path),
                archive_sha256,
                archive_size_bytes,
            ),
        )
    conn.commit()
    return snapshot_id


def _load_raw_entries(
    conn,
    *,
    run_id: UUID,
    snapshot_id: UUID,
    jsonl_path: Path,
    target_table: str,
) -> int:
    table_identifier = _identifier_from_dotted(target_table)
    rows_loaded = 0

    copy_sql = sql.SQL(
        """
        COPY {} (
            run_id,
            snapshot_id,
            source_line,
            source_byte_offset,
            word,
            lang_code,
            pos,
            payload
        ) FROM STDIN
        """
    ).format(table_identifier)

    with conn.cursor() as cursor:
        with cursor.copy(copy_sql.as_string(conn)) as copy:
            for line_number, (json_text, byte_offset) in enumerate(
                iter_json_lines(jsonl_path),
                start=1,
            ):
                payload = _decode_payload(json_text, line_number)
                copy.write_row(
                    (
                        str(run_id),
                        str(snapshot_id),
                        line_number,
                        byte_offset,
                        _optional_text(payload.get("word")),
                        _optional_text(payload.get("lang_code")),
                        _optional_text(payload.get("pos")),
                        json_text,
                    )
                )
                rows_loaded += 1

    conn.commit()
    return rows_loaded


def _decode_payload(json_text: str, line_number: int) -> dict[str, Any]:
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise JsonlProcessingError(
            f"Invalid JSON on line {line_number}: {exc.msg} (column {exc.colno})"
        ) from exc

    if not isinstance(payload, dict):
        raise JsonlProcessingError(
            f"Expected a JSON object on line {line_number}, got {type(payload).__name__}"
        )

    return payload


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
