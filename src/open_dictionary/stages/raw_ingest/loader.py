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
    resumed_from_run_id: UUID | None = None


@dataclass(frozen=True)
class ResumeCheckpoint:
    run_id: UUID
    rows_loaded: int
    anomalies_logged: int
    last_source_line: int
    last_source_byte_offset: int


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
    if _snapshot_has_succeeded_run(
        conn,
        snapshot_id=snapshot_id,
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
                "resumed_from_run_id": None,
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

    resume_checkpoint = _load_resume_checkpoint(
        conn,
        snapshot_id=snapshot_id,
        stage_name=stage_name,
    )
    if resume_checkpoint is not None:
        _validate_resume_checkpoint_state(
            conn,
            snapshot_id=snapshot_id,
            target_table=target_table,
            resume_checkpoint=resume_checkpoint,
        )
    if resume_checkpoint is None and _snapshot_has_partial_data(conn, snapshot_id=snapshot_id, target_table=target_table):
        raise RuntimeError(
            "Found partial snapshot ingest data without a durable checkpoint. "
            f"Cannot safely resume snapshot {snapshot_id}."
        )

    rows_loaded = resume_checkpoint.rows_loaded if resume_checkpoint is not None else 0
    anomalies_logged = resume_checkpoint.anomalies_logged if resume_checkpoint is not None else 0
    last_source_line = resume_checkpoint.last_source_line if resume_checkpoint is not None else 0
    last_source_byte_offset = (
        resume_checkpoint.last_source_byte_offset if resume_checkpoint is not None else 0
    )
    if resume_checkpoint is not None:
        emit_progress(
            progress_callback,
            stage=stage_name,
            event="resume_start",
            resumed_from_run_id=str(resume_checkpoint.run_id),
            rows_loaded=rows_loaded,
            anomalies_logged=anomalies_logged,
            last_source_line=last_source_line,
            last_source_byte_offset=last_source_byte_offset,
        )

    row_chunk = []
    anomaly_chunk: list[SourceAnomaly] = []
    buffered_items = 0

    for item in iter_source_items(artifact):
        if item.source_line <= last_source_line:
            continue

        if isinstance(item, SourceAnomaly):
            anomaly_chunk.append(item)
        else:
            row_chunk.append(project_raw_record(item))
        buffered_items += 1
        batch_last_source_line = item.source_line
        batch_last_source_byte_offset = item.source_byte_offset

        if buffered_items < chunk_size:
            continue

        rows_loaded, anomalies_logged, last_source_line, last_source_byte_offset = _flush_batch(
            conn,
            run_id=run_id,
            snapshot_id=snapshot_id,
            target_table=target_table,
            stage_name=stage_name,
            row_chunk=row_chunk,
            anomaly_chunk=anomaly_chunk,
            rows_loaded=rows_loaded,
            anomalies_logged=anomalies_logged,
            last_source_line=batch_last_source_line,
            last_source_byte_offset=batch_last_source_byte_offset,
            resumed_from_run_id=resume_checkpoint.run_id if resume_checkpoint is not None else None,
        )
        row_chunk.clear()
        anomaly_chunk.clear()
        buffered_items = 0
        reporter.report(
            event="load_progress",
            rows_loaded=rows_loaded,
            anomalies_logged=anomalies_logged,
            last_source_line=last_source_line,
            last_source_byte_offset=last_source_byte_offset,
        )

    if buffered_items:
        rows_loaded, anomalies_logged, last_source_line, last_source_byte_offset = _flush_batch(
            conn,
            run_id=run_id,
            snapshot_id=snapshot_id,
            target_table=target_table,
            stage_name=stage_name,
            row_chunk=row_chunk,
            anomaly_chunk=anomaly_chunk,
            rows_loaded=rows_loaded,
            anomalies_logged=anomalies_logged,
            last_source_line=batch_last_source_line,
            last_source_byte_offset=batch_last_source_byte_offset,
            resumed_from_run_id=resume_checkpoint.run_id if resume_checkpoint is not None else None,
        )
    else:
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
                "resumed_from_run_id": (
                    str(resume_checkpoint.run_id) if resume_checkpoint is not None else None
                ),
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
        resumed_from_run_id=resume_checkpoint.run_id if resume_checkpoint is not None else None,
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


def _snapshot_has_succeeded_run(
    conn,
    *,
    snapshot_id: UUID,
    stage_name: str,
) -> bool:
    with conn.cursor() as cursor:
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


def _snapshot_has_partial_data(
    conn,
    *,
    snapshot_id: UUID,
    target_table: str,
) -> bool:
    return _snapshot_has_rows(conn, snapshot_id=snapshot_id, target_table=target_table) or _snapshot_has_anomalies(
        conn,
        snapshot_id=snapshot_id,
    )


def _snapshot_has_anomalies(conn, *, snapshot_id: UUID) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM raw.wiktionary_ingest_anomalies WHERE snapshot_id = %s LIMIT 1",
            (snapshot_id,),
        )
        return cursor.fetchone() is not None


def _load_resume_checkpoint(
    conn,
    *,
    snapshot_id: UUID,
    stage_name: str,
) -> ResumeCheckpoint | None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT pr.run_id, cp.payload
            FROM meta.stage_checkpoints AS cp
            JOIN meta.pipeline_runs AS pr
              ON pr.run_id = cp.run_id
            WHERE cp.stage_name = %s
              AND pr.stage = %s
              AND pr.status IN ('running', 'failed', 'cancelled')
              AND COALESCE(pr.stats->>'snapshot_id', pr.config->>'snapshot_id') = %s
            ORDER BY cp.updated_at DESC, pr.started_at DESC
            LIMIT 1
            """,
            (stage_name, stage_name, str(snapshot_id)),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    prior_run_id, payload = row
    payload = payload or {}
    return ResumeCheckpoint(
        run_id=prior_run_id,
        rows_loaded=int(payload.get("rows_loaded") or 0),
        anomalies_logged=int(payload.get("anomalies_logged") or 0),
        last_source_line=int(payload.get("last_source_line") or 0),
        last_source_byte_offset=int(payload.get("last_source_byte_offset") or 0),
    )


def _validate_resume_checkpoint_state(
    conn,
    *,
    snapshot_id: UUID,
    target_table: str,
    resume_checkpoint: ResumeCheckpoint,
) -> None:
    snapshot_stats = _existing_snapshot_stats(conn, snapshot_id=snapshot_id, target_table=target_table)
    if (
        snapshot_stats.rows_loaded != resume_checkpoint.rows_loaded
        or snapshot_stats.anomalies_logged != resume_checkpoint.anomalies_logged
        or snapshot_stats.last_source_line != resume_checkpoint.last_source_line
        or snapshot_stats.last_source_byte_offset != resume_checkpoint.last_source_byte_offset
    ):
        raise RuntimeError(
            "Found partial snapshot ingest data whose persisted rows do not match the latest durable checkpoint. "
            f"Cannot safely resume snapshot {snapshot_id}."
        )


def _flush_batch(
    conn,
    *,
    run_id: UUID,
    snapshot_id: UUID,
    target_table: str,
    stage_name: str,
    row_chunk: Sequence,
    anomaly_chunk: Sequence[SourceAnomaly],
    rows_loaded: int,
    anomalies_logged: int,
    last_source_line: int,
    last_source_byte_offset: int,
    resumed_from_run_id: UUID | None,
) -> tuple[int, int, int, int]:
    rows_loaded += flush_rows(
        conn,
        snapshot_id=snapshot_id,
        run_id=run_id,
        target_table=target_table,
        rows=row_chunk,
    )
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
            "resumed_from_run_id": str(resumed_from_run_id) if resumed_from_run_id is not None else None,
        },
    )
    conn.commit()
    return rows_loaded, anomalies_logged, last_source_line, last_source_byte_offset


def _snapshot_has_rows(conn, *, snapshot_id: UUID, target_table: str) -> bool:
    table_identifier = _identifier_from_dotted(target_table)
    query = sql.SQL("SELECT 1 FROM {} WHERE snapshot_id = %s LIMIT 1").format(table_identifier)
    with conn.cursor() as cursor:
        cursor.execute(query, (snapshot_id,))
        return cursor.fetchone() is not None


def _existing_snapshot_stats(conn, *, snapshot_id: UUID, target_table: str) -> LoadStats:
    table_identifier = _identifier_from_dotted(target_table)
    anomaly_query = """
        SELECT COUNT(*), COALESCE(MAX(source_line), 0), COALESCE(MAX(source_byte_offset), 0)
        FROM raw.wiktionary_ingest_anomalies
        WHERE snapshot_id = %s
    """
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
        anomaly_count, anomaly_max_line, anomaly_max_offset = cursor.fetchone()
    latest_line, latest_offset = max(
        (int(max_line), int(max_offset)),
        (int(anomaly_max_line), int(anomaly_max_offset)),
    )

    return LoadStats(
        rows_loaded=int(count),
        anomalies_logged=int(anomaly_count),
        last_source_line=latest_line,
        last_source_byte_offset=latest_offset,
        snapshot_preexisting=True,
        resumed_from_run_id=None,
    )


def _identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
