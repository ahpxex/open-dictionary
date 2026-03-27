from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import psycopg


@dataclass(frozen=True)
class Migration:
    version: str
    statements: tuple[str, ...]


MIGRATIONS: Final[tuple[Migration, ...]] = (
    Migration(
        version="20260327_foundation_v1",
        statements=(
            "CREATE SCHEMA IF NOT EXISTS meta",
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
                run_id UUID,
                source_name TEXT NOT NULL,
                source_url TEXT,
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
                lang TEXT,
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
        ),
    ),
    Migration(
        version="20260327_raw_ingest_rewrite_v2",
        statements=(
            """
            ALTER TABLE meta.source_snapshots
            ALTER COLUMN run_id DROP NOT NULL
            """,
            """
            ALTER TABLE meta.source_snapshots
            ALTER COLUMN source_url DROP NOT NULL
            """,
            """
            ALTER TABLE meta.source_snapshots
            ADD COLUMN IF NOT EXISTS acquisition_mode TEXT
            """,
            """
            ALTER TABLE meta.source_snapshots
            ADD COLUMN IF NOT EXISTS compression TEXT
            """,
            """
            ALTER TABLE meta.source_snapshots
            ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb
            """,
            """
            ALTER TABLE raw.wiktionary_entries
            ADD COLUMN IF NOT EXISTS lang TEXT
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS meta_source_snapshots_source_name_sha256_uidx
            ON meta.source_snapshots (source_name, archive_sha256)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS raw_wiktionary_entries_snapshot_line_uidx
            ON raw.wiktionary_entries (snapshot_id, source_line)
            """,
            """
            CREATE TABLE IF NOT EXISTS meta.stage_checkpoints (
                run_id UUID NOT NULL REFERENCES meta.pipeline_runs(run_id),
                stage_name TEXT NOT NULL,
                checkpoint_key TEXT NOT NULL,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (run_id, stage_name, checkpoint_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS raw.wiktionary_ingest_anomalies (
                anomaly_id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL REFERENCES meta.pipeline_runs(run_id),
                snapshot_id UUID NOT NULL REFERENCES meta.source_snapshots(snapshot_id),
                source_line BIGINT NOT NULL,
                source_byte_offset BIGINT NOT NULL,
                anomaly_type TEXT NOT NULL,
                detail TEXT NOT NULL,
                raw_payload TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS raw_wiktionary_ingest_anomalies_snapshot_id_idx
            ON raw.wiktionary_ingest_anomalies (snapshot_id)
            """,
        ),
    ),
)


LATEST_FOUNDATION_VERSION: Final[str] = MIGRATIONS[-1].version


def apply_foundation(conn: psycopg.Connection) -> list[str]:
    applied_versions: list[str] = []

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

    conn.commit()

    for migration in MIGRATIONS:
        if _is_applied(conn, migration.version):
            continue

        with conn.cursor() as cursor:
            for statement in migration.statements:
                cursor.execute(statement)
            cursor.execute(
                "INSERT INTO meta.schema_migrations (version) VALUES (%s)",
                (migration.version,),
            )
        conn.commit()
        applied_versions.append(migration.version)

    return applied_versions


def _is_applied(conn: psycopg.Connection, version: str) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM meta.schema_migrations WHERE version = %s",
            (version,),
        )
        return cursor.fetchone() is not None
