from __future__ import annotations

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.db.bootstrap import apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.pipeline.runs import complete_run, fail_run, start_run


def test_start_run_creates_running_pipeline_row(temp_database_url: str) -> None:
    # This case verifies the initial run contract: every stage execution must
    # begin with a durable pipeline run row in `running` state.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        run_id = start_run(
            conn,
            stage="test.stage",
            config={"source": "fixture"},
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select stage, status, config->>'source'
                from meta.pipeline_runs
                where run_id = %s
                """,
                (run_id,),
            )
            stage, status, source = cursor.fetchone()

    assert stage == "test.stage"
    assert status == "running"
    assert source == "fixture"


def test_complete_run_marks_run_succeeded_with_stats(temp_database_url: str) -> None:
    # This case proves that successful stage completion persists summarized
    # statistics for later inspection and reproducibility checks.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        run_id = start_run(conn, stage="test.stage")
        complete_run(conn, run_id=run_id, stats={"rows_loaded": 123})
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select status, stats->>'rows_loaded', finished_at is not null
                from meta.pipeline_runs
                where run_id = %s
                """,
                (run_id,),
            )
            status, rows_loaded, has_finished_at = cursor.fetchone()

    assert status == "succeeded"
    assert rows_loaded == "123"
    assert has_finished_at is True


def test_fail_run_marks_run_failed_with_error(temp_database_url: str) -> None:
    # This case ensures that stage crashes leave an explicit failure row instead
    # of an ambiguous forever-running pipeline record.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        run_id = start_run(conn, stage="test.stage")
        fail_run(conn, run_id=run_id, error="boom", stats={"rows_loaded": 7})
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select status, error, stats->>'rows_loaded', finished_at is not null
                from meta.pipeline_runs
                where run_id = %s
                """,
                (run_id,),
            )
            status, error, rows_loaded, has_finished_at = cursor.fetchone()

    assert status == "failed"
    assert error == "boom"
    assert rows_loaded == "7"
    assert has_finished_at is True
