from __future__ import annotations

from pathlib import Path

import psycopg

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.db.bootstrap import LATEST_FOUNDATION_VERSION, apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.stages.raw_ingest.anomalies import flush_anomalies
from open_dictionary.stages.raw_ingest.checkpoints import save_checkpoint
from open_dictionary.stages.raw_ingest.stage import get_or_create_snapshot, run_raw_ingest_stage
from open_dictionary.sources.wiktionary.acquire import acquire_snapshot
from open_dictionary.sources.wiktionary.contracts import SnapshotRequest, SourceAnomaly


def test_apply_foundation_is_idempotent(temp_database_url: str) -> None:
    # This case ensures migrations can be safely rerun, which is essential for
    # local development and repeated CI setup.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        applied_first = apply_foundation(conn)
        applied_second = apply_foundation(conn)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select table_schema || '.' || table_name
                from information_schema.tables
                where table_schema in ('meta', 'raw', 'curated', 'llm', 'export')
                order by 1
                """
            )
            tables = [row[0] for row in cursor.fetchall()]

    assert applied_first
    assert applied_second == []
    assert "meta.pipeline_runs" in tables
    assert "meta.stage_checkpoints" in tables
    assert "raw.wiktionary_entries" in tables
    assert "raw.wiktionary_ingest_anomalies" in tables


def test_save_checkpoint_upserts_payload(temp_database_url: str) -> None:
    # This case protects resumability by proving that checkpoint writes replace
    # earlier payloads for the same run and stage.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        run_id = psycopg.connect(temp_database_url).execute("select gen_random_uuid()").fetchone()[0]

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                insert into meta.pipeline_runs (run_id, stage, status, config)
                values (%s, %s, %s, '{}'::jsonb)
                """,
                (run_id, "test.stage", "running"),
            )
        conn.commit()
        save_checkpoint(
            conn,
            run_id=run_id,
            stage_name="test.stage",
            payload={"rows_loaded": 1},
        )
        save_checkpoint(
            conn,
            run_id=run_id,
            stage_name="test.stage",
            payload={"rows_loaded": 2},
        )
        conn.commit()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select payload->>'rows_loaded'
                from meta.stage_checkpoints
                where run_id = %s and stage_name = %s
                """,
                (run_id, "test.stage"),
            )
            stored = cursor.fetchone()[0]

    assert stored == "2"


def test_flush_anomalies_persists_record_level_exceptions(temp_database_url: str) -> None:
    # This case verifies that malformed raw records are retained for agent-side
    # triage instead of disappearing silently.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                insert into meta.pipeline_runs (run_id, stage, status, config)
                values (gen_random_uuid(), 'test.stage', 'running', '{}'::jsonb)
                returning run_id
                """
            )
            run_id = cursor.fetchone()[0]
            cursor.execute(
                """
                insert into meta.source_snapshots (
                    snapshot_id, run_id, source_name, source_url, archive_path,
                    archive_sha256, archive_size_bytes, acquisition_mode, compression, metadata
                ) values (
                    gen_random_uuid(), null, 'wiktionary', null, '/tmp/archive.jsonl.gz',
                    'abc', 123, 'register_local', 'gzip', '{}'::jsonb
                )
                returning snapshot_id
                """
            )
            snapshot_id = cursor.fetchone()[0]
        conn.commit()

        count = flush_anomalies(
            conn,
            run_id=run_id,
            snapshot_id=snapshot_id,
            anomalies=[
                SourceAnomaly(
                    source_line=2,
                    source_byte_offset=128,
                    anomaly_type="invalid_json",
                    detail="broken json",
                    json_text='{"word": "broken"',
                )
            ],
        )
        conn.commit()
        with conn.cursor() as cursor:
            cursor.execute("select count(*), max(anomaly_type) from raw.wiktionary_ingest_anomalies")
            stored_count, stored_type = cursor.fetchone()

    assert count == 1
    assert stored_count == 1
    assert stored_type == "invalid_json"


def test_get_or_create_snapshot_reuses_existing_snapshot(gzip_jsonl_path: Path, temp_database_url: str) -> None:
    # This case verifies snapshot-level idempotency, which is the main reason
    # for introducing archive hashing in the rewritten raw ingest stage.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    artifact = acquire_snapshot(
        SnapshotRequest(
            workdir=gzip_jsonl_path.parent,
            archive_path=gzip_jsonl_path,
        )
    )

    with get_connection(settings) as conn:
        first = get_or_create_snapshot(conn, artifact=artifact)
        second = get_or_create_snapshot(conn, artifact=artifact)

    assert first == second


def test_run_raw_ingest_stage_loads_rows_and_reuses_existing_snapshot(
    gzip_jsonl_path: Path,
    temp_database_url: str,
) -> None:
    # This case covers the main local-archive ingest path end to end and then
    # proves that repeating the same ingest does not duplicate raw rows.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    first = run_raw_ingest_stage(
        settings=settings,
        workdir=gzip_jsonl_path.parent,
        archive_path=gzip_jsonl_path,
    )
    second = run_raw_ingest_stage(
        settings=settings,
        workdir=gzip_jsonl_path.parent,
        archive_path=gzip_jsonl_path,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from raw.wiktionary_entries")
            row_count = cursor.fetchone()[0]
            cursor.execute(
                "select count(*) from meta.pipeline_runs where stage = 'source.ingest'"
            )
            run_count = cursor.fetchone()[0]

    assert first.rows_loaded == 2
    assert first.snapshot_preexisting is False
    assert second.rows_loaded == 2
    assert second.snapshot_preexisting is True
    assert first.snapshot_id == second.snapshot_id
    assert row_count == 2
    assert run_count == 2


def test_run_raw_ingest_stage_logs_anomalies_for_bad_source_lines(
    anomaly_jsonl_path: Path,
    temp_database_url: str,
) -> None:
    # This case verifies that the stage keeps loading valid rows even when the
    # source file contains malformed lines and non-object JSON values.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    result = run_raw_ingest_stage(
        settings=settings,
        workdir=anomaly_jsonl_path.parent,
        archive_path=anomaly_jsonl_path,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from raw.wiktionary_entries")
            row_count = cursor.fetchone()[0]
            cursor.execute("select count(*) from raw.wiktionary_ingest_anomalies")
            anomaly_count = cursor.fetchone()[0]

    assert result.rows_loaded == 2
    assert result.anomalies_logged == 2
    assert row_count == 2
    assert anomaly_count == 2
