from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from open_dictionary.config import RuntimeSettings
from open_dictionary.db.connection import get_connection
from open_dictionary.pipeline import ProgressCallback, complete_run, emit_progress, fail_run, start_run
from open_dictionary.sources.wiktionary import (
    DEFAULT_WIKTIONARY_SOURCE_URL,
    SnapshotRequest,
    acquire_snapshot,
)

from .loader import load_snapshot


RAW_INGEST_STAGE = "source.ingest"
DEFAULT_RAW_TABLE = "raw.wiktionary_entries"


@dataclass(frozen=True)
class RawIngestResult:
    run_id: UUID
    snapshot_id: UUID
    archive_path: Path
    rows_loaded: int
    anomalies_logged: int
    archive_sha256: str
    snapshot_preexisting: bool


def run_raw_ingest_stage(
    *,
    settings: RuntimeSettings,
    workdir: Path,
    source_url: str | None = DEFAULT_WIKTIONARY_SOURCE_URL,
    archive_path: Path | None = None,
    target_table: str = DEFAULT_RAW_TABLE,
    overwrite_download: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> RawIngestResult:
    emit_progress(
        progress_callback,
        stage=RAW_INGEST_STAGE,
        event="acquire_start",
        workdir=str(workdir),
        source_url=source_url,
        archive_path=str(archive_path) if archive_path is not None else None,
    )
    artifact = acquire_snapshot(
        SnapshotRequest(
            workdir=Path(workdir),
            source_url=source_url,
            archive_path=archive_path,
            overwrite_download=overwrite_download,
        )
    )
    emit_progress(
        progress_callback,
        stage=RAW_INGEST_STAGE,
        event="acquire_complete",
        archive_path=str(artifact.archive_path),
        archive_size_bytes=artifact.archive_size_bytes,
        archive_sha256=artifact.archive_sha256,
        acquisition_mode=artifact.acquisition_mode,
        compression=artifact.compression,
    )

    with get_connection(settings) as conn:
        snapshot_id = get_or_create_snapshot(conn, artifact=artifact)
        run_id = start_run(
            conn,
            stage=RAW_INGEST_STAGE,
            config={
                "target_table": target_table,
                "source_url": artifact.source_url,
                "archive_path": str(artifact.archive_path),
                "workdir": str(artifact.workdir),
                "source_name": artifact.source_name,
                "archive_sha256": artifact.archive_sha256,
                "archive_size_bytes": artifact.archive_size_bytes,
                "compression": artifact.compression,
                "acquisition_mode": artifact.acquisition_mode,
                "snapshot_id": str(snapshot_id),
            },
        )

    try:
        with get_connection(settings) as conn:
            stats = load_snapshot(
                conn,
                run_id=run_id,
                snapshot_id=snapshot_id,
                artifact=artifact,
                target_table=target_table,
                stage_name=RAW_INGEST_STAGE,
                progress_callback=progress_callback,
            )
            complete_run(
                conn,
                run_id=run_id,
                stats={
                    "snapshot_id": str(snapshot_id),
                    "rows_loaded": stats.rows_loaded,
                    "anomalies_logged": stats.anomalies_logged,
                    "last_source_line": stats.last_source_line,
                    "last_source_byte_offset": stats.last_source_byte_offset,
                    "archive_sha256": artifact.archive_sha256,
                    "archive_size_bytes": artifact.archive_size_bytes,
                    "snapshot_preexisting": stats.snapshot_preexisting,
                },
            )

        return RawIngestResult(
            run_id=run_id,
            snapshot_id=snapshot_id,
            archive_path=artifact.archive_path,
            rows_loaded=stats.rows_loaded,
            anomalies_logged=stats.anomalies_logged,
            archive_sha256=artifact.archive_sha256,
            snapshot_preexisting=stats.snapshot_preexisting,
        )
    except Exception as exc:
        with get_connection(settings) as conn:
            fail_run(conn, run_id=run_id, error=str(exc))
        raise


def get_or_create_snapshot(conn, *, artifact) -> UUID:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT snapshot_id
            FROM meta.source_snapshots
            WHERE source_name = %s AND archive_sha256 = %s
            LIMIT 1
            """,
            (artifact.source_name, artifact.archive_sha256),
        )
        row = cursor.fetchone()
        if row:
            return row[0]

        snapshot_id = uuid4()
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
                archive_size_bytes,
                acquisition_mode,
                compression,
                metadata
            ) VALUES (%s, NULL, %s, %s, %s, NULL, %s, %s, %s, %s, %s)
            """,
            (
                snapshot_id,
                artifact.source_name,
                artifact.source_url,
                str(artifact.archive_path),
                artifact.archive_sha256,
                artifact.archive_size_bytes,
                artifact.acquisition_mode,
                artifact.compression,
                Jsonb({"workdir": str(artifact.workdir)}),
            ),
        )
    conn.commit()
    return snapshot_id
