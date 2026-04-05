from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb


def start_run(
    conn: psycopg.Connection,
    *,
    stage: str,
    config: dict[str, Any] | None = None,
    parent_run_id: UUID | None = None,
) -> UUID:
    run_id = uuid4()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO meta.pipeline_runs (
                run_id,
                stage,
                status,
                parent_run_id,
                config
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_id,
                stage,
                "running",
                parent_run_id,
                Jsonb(config or {}),
            ),
        )
    conn.commit()
    return run_id


def complete_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    stats: dict[str, Any] | None = None,
) -> None:
    _finish_run(
        conn,
        run_id=run_id,
        status="succeeded",
        stats=stats or {},
        error=None,
    )


def update_run_config(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    config_updates: dict[str, Any],
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE meta.pipeline_runs
            SET config = config || %s
            WHERE run_id = %s
            """,
            (
                Jsonb(config_updates),
                run_id,
            ),
        )
    conn.commit()


def fail_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    error: str,
    stats: dict[str, Any] | None = None,
) -> None:
    _finish_run(
        conn,
        run_id=run_id,
        status="failed",
        stats=stats or {},
        error=error,
    )


def _finish_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    status: str,
    stats: dict[str, Any],
    error: str | None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE meta.pipeline_runs
            SET status = %s,
                stats = %s,
                error = %s,
                finished_at = NOW()
            WHERE run_id = %s
            """,
            (
                status,
                Jsonb(stats),
                error,
                run_id,
            ),
        )
    conn.commit()
