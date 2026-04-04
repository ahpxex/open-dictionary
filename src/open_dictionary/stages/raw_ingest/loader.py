from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence
from uuid import UUID

from psycopg import sql

from open_dictionary.pipeline import ProgressCallback, ThrottledProgressReporter, emit_progress
from open_dictionary.sources.wiktionary import SnapshotArtifact, SourceAnomaly, SourceRecord
from open_dictionary.sources.wiktionary.project import project_raw_record
from open_dictionary.sources.wiktionary.stream import iter_source_items

from .anomalies import flush_anomalies
from .checkpoints import save_checkpoint


@dataclass(frozen=True)
class LoadStats:
    rows_loaded: int
    anomalies_logged: int
    last_source_line: int
    last_source_byte_offset: int
    snapshot_preexisting: bool


def load_snapshot(
    conn,
    *,
    run_id: UUID,
    snapshot_id: UUID,
    artifact: SnapshotArtifact,
    target_table: str,
    stage_name: str,
    chunk_size: int = 5000,
    progress_callback: ProgressCallback | None = None,
) -> LoadStats:
    reporter = ThrottledProgressReporter(progress_callback, stage=stage_name)
    if _snapshot_is_preexisting(
        conn,
        snapshot_id=snapshot_id,
        target_table=target_table,
        stage_name=stage_name,
    ):
        stats = _existing_snapshot_stats(conn, snapshot_id=snapshot_id, target_table=target_table)
        save_checkpoint(
            conn,
            run_id=run_id,
            stage_name=stage_name,
            payload={
                "rows_loaded": stats.rows_loaded,
                "anomalies_logged": stats.anomalies_logged,
                "last_source_line": stats.last_source_line,
                "last_source_byte_offset": stats.last_source_byte_offset,
                "snapshot_preexisting": True,
            },
        )
        emit_progress(
            progress_callback,
            stage=stage_name,
            event="snapshot_preexisting",
            rows_loaded=stats.rows_loaded,
            anomalies_logged=stats.anomalies_logged,
            snapshot_preexisting=True,
        )
        conn.commit()
        return stats

    rows_loaded = 0
    anomalies_logged = 0
    last_source_line = 0
    last_source_byte_offset = 0
    row_chunk = []
    anomaly_chunk: list[SourceAnomaly] = []

    for item in iter_source_items(artifact):
        if isinstance(item, SourceAnomaly):
            anomaly_chunk.append(item)
            last_source_line = item.source_line
            last_source_byte_offset = item.source_byte_offset
        else:
            row_chunk.append(project_raw_record(item))
            last_source_line = item.source_line
            last_source_byte_offset = item.source_byte_offset

        if len(row_chunk) >= chunk_size:
            rows_loaded += flush_rows(
                conn,
                snapshot_id=snapshot_id,
                run_id=run_id,
                target_table=target_table,
                rows=row_chunk,
            )
            row_chunk.clear()

        if len(anomaly_chunk) >= chunk_size:
            anomalies_logged += flush_anomalies(
                conn,
                run_id=run_id,
                snapshot_id=snapshot_id,
                anomalies=anomaly_chunk,
            )
            anomaly_chunk.clear()

        if not row_chunk and not anomaly_chunk:
            save_checkpoint(
                conn,
                run_id=run_id,
                stage_name=stage_name,
                payload={
                    "rows_loaded": rows_loaded,
                    "anomalies_logged": anomalies_logged,
                    "last_source_line": last_source_line,
                    "last_source_byte_offset": last_source_byte_offset,
                    "snapshot_preexisting": False,
                },
            )
            conn.commit()
            reporter.report(
                event="load_progress",
                rows_loaded=rows_loaded,
                anomalies_logged=anomalies_logged,
                last_source_line=last_source_line,
                last_source_byte_offset=last_source_byte_offset,
            )

    if row_chunk:
        rows_loaded += flush_rows(
            conn,
            snapshot_id=snapshot_id,
            run_id=run_id,
            target_table=target_table,
            rows=row_chunk,
        )
    if anomaly_chunk:
        anomalies_logged += flush_anomalies(
            conn,
            run_id=run_id,
            snapshot_id=snapshot_id,
            anomalies=anomaly_chunk,
        )

    save_checkpoint(
        conn,
        run_id=run_id,
        stage_name=stage_name,
        payload={
            "rows_loaded": rows_loaded,
            "anomalies_logged": anomalies_logged,
            "last_source_line": last_source_line,
            "last_source_byte_offset": last_source_byte_offset,
            "snapshot_preexisting": False,
        },
    )
    conn.commit()
    reporter.report(
        event="load_complete",
        force=True,
        rows_loaded=rows_loaded,
        anomalies_logged=anomalies_logged,
        last_source_line=last_source_line,
        last_source_byte_offset=last_source_byte_offset,
        snapshot_preexisting=False,
    )

    return LoadStats(
        rows_loaded=rows_loaded,
        anomalies_logged=anomalies_logged,
        last_source_line=last_source_line,
        last_source_byte_offset=last_source_byte_offset,
        snapshot_preexisting=False,
    )


def flush_rows(
    conn,
    *,
    snapshot_id: UUID,
    run_id: UUID,
    target_table: str,
    rows: Sequence,
) -> int:
    if not rows:
        return 0

    table_identifier = _identifier_from_dotted(target_table)
    copy_sql = sql.SQL(
        """
        COPY {} (
            run_id,
            snapshot_id,
            source_line,
            source_byte_offset,
            word,
            lang,
            lang_code,
            pos,
            payload
        ) FROM STDIN
        """
    ).format(table_identifier)

    with conn.cursor() as cursor:
        with cursor.copy(copy_sql.as_string(conn)) as copy:
            for row in rows:
                copy.write_row(
                    (
                        str(run_id),
                        str(snapshot_id),
                        row.source_line,
                        row.source_byte_offset,
                        row.word,
                        row.lang,
                        row.lang_code,
                        row.pos,
                        row.payload_json,
                    )
                )

    return len(rows)


def _snapshot_is_preexisting(
    conn,
    *,
    snapshot_id: UUID,
    target_table: str,
    stage_name: str,
) -> bool:
    if _snapshot_has_rows(conn, snapshot_id=snapshot_id, target_table=target_table):
        return True

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM raw.wiktionary_ingest_anomalies WHERE snapshot_id = %s LIMIT 1",
            (snapshot_id,),
        )
        if cursor.fetchone() is not None:
            return True

        cursor.execute(
            """
            SELECT 1
            FROM meta.pipeline_runs
            WHERE stage = %s
              AND status = 'succeeded'
              AND COALESCE(stats->>'snapshot_id', config->>'snapshot_id') = %s
            LIMIT 1
            """,
            (stage_name, str(snapshot_id)),
        )
        return cursor.fetchone() is not None


def _snapshot_has_rows(conn, *, snapshot_id: UUID, target_table: str) -> bool:
    table_identifier = _identifier_from_dotted(target_table)
    query = sql.SQL("SELECT 1 FROM {} WHERE snapshot_id = %s LIMIT 1").format(table_identifier)
    with conn.cursor() as cursor:
        cursor.execute(query, (snapshot_id,))
        return cursor.fetchone() is not None


def _existing_snapshot_stats(conn, *, snapshot_id: UUID, target_table: str) -> LoadStats:
    table_identifier = _identifier_from_dotted(target_table)
    anomaly_query = "SELECT COUNT(*) FROM raw.wiktionary_ingest_anomalies WHERE snapshot_id = %s"
    row_query = sql.SQL(
        """
        SELECT COUNT(*), COALESCE(MAX(source_line), 0), COALESCE(MAX(source_byte_offset), 0)
        FROM {}
        WHERE snapshot_id = %s
        """
    ).format(table_identifier)

    with conn.cursor() as cursor:
        cursor.execute(row_query, (snapshot_id,))
        count, max_line, max_offset = cursor.fetchone()
        cursor.execute(anomaly_query, (snapshot_id,))
        anomaly_count = cursor.fetchone()[0]

    return LoadStats(
        rows_loaded=int(count),
        anomalies_logged=int(anomaly_count),
        last_source_line=int(max_line),
        last_source_byte_offset=int(max_offset),
        snapshot_preexisting=True,
    )


def _identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
