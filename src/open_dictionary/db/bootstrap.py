from __future__ import annotations

from typing import Final

import psycopg


FOUNDATION_VERSION: Final[str] = "20260327_foundation_v1"


FOUNDATION_STATEMENTS: Final[tuple[str, ...]] = (
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS curated",
    "CREATE SCHEMA IF NOT EXISTS llm",
    "CREATE SCHEMA IF NOT EXISTS export",
    """
    CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
        run_id UUID PRIMARY KEY,
        stage TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
        parent_run_id UUID REFERENCES meta.pipeline_runs(run_id),
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        stats JSONB NOT NULL DEFAULT '{}'::jsonb,
        error TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta.source_snapshots (
        snapshot_id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES meta.pipeline_runs(run_id),
        source_name TEXT NOT NULL,
        source_url TEXT NOT NULL,
        archive_path TEXT NOT NULL,
        extracted_path TEXT,
        archive_sha256 TEXT NOT NULL,
        archive_size_bytes BIGINT NOT NULL,
        downloaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.wiktionary_entries (
        id BIGSERIAL PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES meta.pipeline_runs(run_id),
        snapshot_id UUID NOT NULL REFERENCES meta.source_snapshots(snapshot_id),
        source_line BIGINT NOT NULL,
        source_byte_offset BIGINT NOT NULL,
        word TEXT,
        lang_code TEXT,
        pos TEXT,
        payload JSONB NOT NULL,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_id, source_line)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS raw_wiktionary_entries_snapshot_id_idx
    ON raw.wiktionary_entries (snapshot_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS raw_wiktionary_entries_word_idx
    ON raw.wiktionary_entries (word)
    """,
    """
    CREATE INDEX IF NOT EXISTS raw_wiktionary_entries_lang_code_idx
    ON raw.wiktionary_entries (lang_code)
    """,
    """
    CREATE INDEX IF NOT EXISTS raw_wiktionary_entries_pos_idx
    ON raw.wiktionary_entries (pos)
    """,
)


def apply_foundation(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            "SELECT 1 FROM meta.schema_migrations WHERE version = %s",
            (FOUNDATION_VERSION,),
        )
        if cursor.fetchone():
            conn.commit()
            return False

        for statement in FOUNDATION_STATEMENTS:
            cursor.execute(statement)

        cursor.execute(
            "INSERT INTO meta.schema_migrations (version) VALUES (%s)",
            (FOUNDATION_VERSION,),
        )

    conn.commit()
    return True
