from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.db.bootstrap import apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.stages.raw_ingest import loader as loader_module
from open_dictionary.stages.raw_ingest import stage as stage_module
from open_dictionary.stages.raw_ingest.anomalies import flush_anomalies
from open_dictionary.stages.raw_ingest.loader import _identifier_from_dotted, flush_rows
from open_dictionary.stages.raw_ingest.stage import RAW_INGEST_STAGE, get_or_create_snapshot
from open_dictionary.sources.wiktionary.acquire import acquire_snapshot
from open_dictionary.sources.wiktionary.contracts import SnapshotRequest


def test_load_snapshot_flushes_small_chunks_and_records_progress(
    anomaly_jsonl_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    temp_database_url: str,
) -> None:
    # This case forces the chunked loader path so intermediate flushes and
    # checkpoint updates are exercised instead of only the final drain.
    settings = RuntimeSettings(database_url=temp_database_url)
    artifact = acquire_snapshot(
        SnapshotRequest(
            workdir=anomaly_jsonl_path.parent,
            archive_path=anomaly_jsonl_path,
        )
    )
    payloads: list[dict[str, int | bool]] = []
    original_save_checkpoint = loader_module.save_checkpoint

    def tracking_save_checkpoint(conn, *, run_id, stage_name, payload, checkpoint_key="main"):
        payloads.append(dict(payload))
        return original_save_checkpoint(
            conn,
            run_id=run_id,
            stage_name=stage_name,
            payload=payload,
            checkpoint_key=checkpoint_key,
        )

    monkeypatch.setattr(loader_module, "save_checkpoint", tracking_save_checkpoint)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    run_id = uuid4()

    with get_connection(settings) as conn:
        snapshot_id = get_or_create_snapshot(conn, artifact=artifact)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                insert into meta.pipeline_runs (run_id, stage, status, config)
                values (%s, %s, %s, '{}'::jsonb)
                """,
                (run_id, "test.stage", "running"),
            )
        conn.commit()

        stats = loader_module.load_snapshot(
            conn,
            run_id=run_id,
            snapshot_id=snapshot_id,
            artifact=artifact,
            target_table="raw.wiktionary_entries",
            stage_name="test.stage",
            chunk_size=1,
        )

    assert stats.rows_loaded == 2
    assert stats.anomalies_logged == 2
    assert stats.last_source_line == 4
    assert len(payloads) == 5
    assert [payload["rows_loaded"] for payload in payloads] == [1, 1, 1, 2, 2]
    assert [payload["anomalies_logged"] for payload in payloads] == [0, 1, 2, 2, 2]
    assert payloads[-1]["snapshot_preexisting"] is False


def test_run_raw_ingest_stage_marks_run_failed_when_loader_raises(
    gzip_jsonl_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    temp_database_url: str,
) -> None:
    # This case verifies that failed ingest attempts are still durably recorded
    # in pipeline_runs instead of disappearing as bare Python exceptions.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    def boom(*args, **kwargs):
        raise RuntimeError("forced loader failure")

    monkeypatch.setattr(stage_module, "load_snapshot", boom)

    with pytest.raises(RuntimeError, match="forced loader failure"):
        stage_module.run_raw_ingest_stage(
            settings=settings,
            workdir=gzip_jsonl_path.parent,
            archive_path=gzip_jsonl_path,
        )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select status, error
                from meta.pipeline_runs
                where stage = %s
                order by started_at desc
                limit 1
                """,
                (RAW_INGEST_STAGE,),
            )
            status, error = cursor.fetchone()

    assert status == "failed"
    assert "forced loader failure" in error


def test_run_raw_ingest_stage_does_not_duplicate_all_anomaly_snapshots(
    tmp_path: Path,
    temp_database_url: str,
) -> None:
    # This case locks in snapshot-level idempotency even when the archive has
    # zero valid rows and only anomaly output.
    source_path = tmp_path / "all-bad.jsonl"
    source_path.write_text('{"word": "broken"\n["not", "an", "object"]\n', encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    first = stage_module.run_raw_ingest_stage(
        settings=settings,
        workdir=source_path.parent,
        archive_path=source_path,
    )
    second = stage_module.run_raw_ingest_stage(
        settings=settings,
        workdir=source_path.parent,
        archive_path=source_path,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from raw.wiktionary_entries")
            row_count = cursor.fetchone()[0]
            cursor.execute("select count(*) from raw.wiktionary_ingest_anomalies")
            anomaly_count = cursor.fetchone()[0]

    assert first.rows_loaded == 0
    assert first.anomalies_logged == 2
    assert first.snapshot_preexisting is False
    assert second.rows_loaded == 0
    assert second.anomalies_logged == 2
    assert second.snapshot_preexisting is True
    assert row_count == 0
    assert anomaly_count == 2


def test_run_raw_ingest_stage_reuses_completed_empty_snapshot(
    tmp_path: Path,
    temp_database_url: str,
) -> None:
    # This case covers the degenerate but valid empty-archive path and proves
    # a completed zero-output ingest is still treated as preexisting.
    source_path = tmp_path / "empty.jsonl"
    source_path.write_text("", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    first = stage_module.run_raw_ingest_stage(
        settings=settings,
        workdir=source_path.parent,
        archive_path=source_path,
    )
    second = stage_module.run_raw_ingest_stage(
        settings=settings,
        workdir=source_path.parent,
        archive_path=source_path,
    )

    assert first.rows_loaded == 0
    assert first.anomalies_logged == 0
    assert first.snapshot_preexisting is False
    assert second.rows_loaded == 0
    assert second.anomalies_logged == 0
    assert second.snapshot_preexisting is True


def test_run_raw_ingest_stage_resumes_failed_snapshot_from_checkpoint(
    anomaly_jsonl_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    temp_database_url: str,
) -> None:
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    original_iter_source_items = loader_module.iter_source_items
    original_load_snapshot = loader_module.load_snapshot
    fail_state = {"should_fail": True, "seen": 0}

    def flaky_iter_source_items(artifact):
        for item in original_iter_source_items(artifact):
            fail_state["seen"] += 1
            if fail_state["should_fail"] and fail_state["seen"] > 2:
                fail_state["should_fail"] = False
                raise RuntimeError("forced mid-stream failure")
            yield item

    def chunked_load_snapshot(conn, **kwargs):
        return original_load_snapshot(conn, chunk_size=2, **kwargs)

    monkeypatch.setattr(loader_module, "iter_source_items", flaky_iter_source_items)
    monkeypatch.setattr(stage_module, "load_snapshot", chunked_load_snapshot)

    with pytest.raises(RuntimeError, match="forced mid-stream failure"):
        stage_module.run_raw_ingest_stage(
            settings=settings,
            workdir=anomaly_jsonl_path.parent,
            archive_path=anomaly_jsonl_path,
        )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select run_id
                from meta.pipeline_runs
                where stage = %s and status = 'failed'
                order by started_at desc
                limit 1
                """,
                (RAW_INGEST_STAGE,),
            )
            failed_run_id = cursor.fetchone()[0]

    result = stage_module.run_raw_ingest_stage(
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
    assert result.snapshot_preexisting is False
    assert result.resumed_from_run_id == failed_run_id
    assert row_count == 2
    assert anomaly_count == 2


def test_flush_rows_returns_zero_for_empty_batch() -> None:
    # This case locks in the no-op guard so callers can safely flush at stage
    # boundaries without pre-checking the batch themselves.
    assert (
        flush_rows(
            None,
            snapshot_id=uuid4(),
            run_id=uuid4(),
            target_table="raw.wiktionary_entries",
            rows=[],
        )
        == 0
    )


def test_flush_anomalies_returns_zero_for_empty_batch() -> None:
    # This case mirrors the row flush guard for anomaly batches.
    assert flush_anomalies(None, run_id=uuid4(), snapshot_id=uuid4(), anomalies=[]) == 0


def test_identifier_from_dotted_rejects_blank_names() -> None:
    # This case prevents unsafe SQL identifiers from quietly becoming malformed
    # COPY / SELECT statements later in the loader.
    with pytest.raises(ValueError, match="Identifier name cannot be empty"):
        _identifier_from_dotted(" . ")
