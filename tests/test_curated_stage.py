from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.db.bootstrap import apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.stages.curated_build.stage import run_curated_build_stage
from open_dictionary.stages.raw_ingest.stage import run_raw_ingest_stage


def insert_raw_row(
    conn,
    *,
    word: str,
    lang: str,
    lang_code: str,
    pos: str,
    source_line: int,
    gloss: str,
) -> None:
    run_id = uuid4()
    snapshot_id = uuid4()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            insert into meta.pipeline_runs (run_id, stage, status, config)
            values (%s, %s, %s, '{}'::jsonb)
            """,
            (run_id, "test.raw_seed", "succeeded"),
        )
        cursor.execute(
            """
            insert into meta.source_snapshots (
                snapshot_id, run_id, source_name, source_url, archive_path,
                archive_sha256, archive_size_bytes, acquisition_mode, compression, metadata
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
            """,
            (
                snapshot_id,
                run_id,
                "wiktionary",
                None,
                f"/tmp/{word}.jsonl",
                f"sha-{word}-{source_line}",
                1,
                "register_local",
                "plain",
            ),
        )
        cursor.execute(
            """
            insert into raw.wiktionary_entries (
                run_id, snapshot_id, source_line, source_byte_offset,
                word, lang, lang_code, pos, payload
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                run_id,
                snapshot_id,
                source_line,
                source_line,
                word,
                lang,
                lang_code,
                pos,
                (
                    '{"word":"%s","lang":"%s","lang_code":"%s","pos":"%s",'
                    '"senses":[{"glosses":["%s"]}]}'
                    % (word, lang, lang_code, pos, gloss)
                ),
            ),
        )


def test_curated_build_stage_creates_entries_relations_and_triage(
    anomaly_jsonl_path: Path,
    temp_database_url: str,
) -> None:
    # This case runs the first two stages together and verifies that curated
    # entries, relation rows, and triage items all land in the database.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)

    run_raw_ingest_stage(
        settings=settings,
        workdir=anomaly_jsonl_path.parent,
        archive_path=anomaly_jsonl_path,
    )
    with get_connection(settings) as conn:
        insert_raw_row(
            conn,
            word="倦",
            lang="Japanese",
            lang_code="ja",
            pos="character",
            source_line=99,
            gloss="in fatigue",
        )
        conn.commit()
    result = run_curated_build_stage(settings=settings)

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from curated.entries")
            entries = cursor.fetchone()[0]
            cursor.execute("select count(*) from curated.entries where run_id is not null")
            entries_with_run_id = cursor.fetchone()[0]
            cursor.execute("select count(*) from curated.entry_relations")
            relations = cursor.fetchone()[0]
            cursor.execute("select count(*) from curated.entry_relations where run_id is not null")
            relations_with_run_id = cursor.fetchone()[0]
            cursor.execute("select count(*) from curated.triage_queue")
            triage = cursor.fetchone()[0]
            cursor.execute("select count(*) from curated.triage_queue where run_id is not null")
            triage_with_run_id = cursor.fetchone()[0]

    assert result.entries_written == entries
    assert result.relations_written == relations
    assert result.triage_written == triage
    assert entries >= 1
    assert triage >= 1
    assert entries_with_run_id == entries
    assert relations_with_run_id == relations
    assert triage_with_run_id == triage


def test_curated_build_stage_filters_by_lang_codes(
    gzip_jsonl_path: Path,
    temp_database_url: str,
) -> None:
    # This case protects targeted local rebuilds where only one language slice
    # should be materialized.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        insert_raw_row(conn, word="cat", lang="English", lang_code="en", pos="noun", source_line=1, gloss="cat")
        insert_raw_row(conn, word="chien", lang="French", lang_code="fr", pos="noun", source_line=2, gloss="dog")
        conn.commit()

    result = run_curated_build_stage(settings=settings, lang_codes=["fr"])

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select lang_code, word from curated.entries order by word")
            rows = cursor.fetchall()

    assert result.entries_written == 1
    assert rows == [("fr", "chien")]


def test_curated_build_stage_limit_groups_stops_after_requested_count(
    temp_database_url: str,
) -> None:
    # This case supports debugger-friendly partial builds over a larger raw table.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        insert_raw_row(conn, word="alpha", lang="English", lang_code="en", pos="noun", source_line=1, gloss="alpha")
        insert_raw_row(conn, word="beta", lang="English", lang_code="en", pos="noun", source_line=2, gloss="beta")
        conn.commit()

    result = run_curated_build_stage(settings=settings, limit_groups=1)

    assert result.groups_processed == 1


def test_curated_build_stage_replace_existing_resets_outputs(
    temp_database_url: str,
) -> None:
    # This case ensures rebuild mode behaves deterministically instead of appending
    # duplicate output rows across repeated curated runs.
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        insert_raw_row(conn, word="alpha", lang="English", lang_code="en", pos="noun", source_line=1, gloss="alpha")
        conn.commit()

    run_curated_build_stage(settings=settings)
    run_curated_build_stage(settings=settings, replace_existing=True)

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from curated.entries")
            entries = cursor.fetchone()[0]

    assert entries == 1
