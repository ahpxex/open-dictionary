from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg import sql
from psycopg.types.json import Jsonb

from open_dictionary.config import RuntimeSettings
from open_dictionary.db.connection import get_connection
from open_dictionary.pipeline import complete_run, fail_run, start_run

from .transform import CuratedBuildOutput, TriageItem, build_curated_entry


CURATED_BUILD_STAGE = "curated.build"
DEFAULT_RAW_SOURCE_TABLE = "raw.wiktionary_entries"
DEFAULT_CURATED_TABLE = "curated.entries"
DEFAULT_RELATIONS_TABLE = "curated.entry_relations"
DEFAULT_TRIAGE_TABLE = "curated.triage_queue"


@dataclass(frozen=True)
class CuratedBuildResult:
    run_id: UUID
    groups_processed: int
    entries_written: int
    relations_written: int
    triage_written: int


def run_curated_build_stage(
    *,
    settings: RuntimeSettings,
    source_table: str = DEFAULT_RAW_SOURCE_TABLE,
    target_table: str = DEFAULT_CURATED_TABLE,
    relations_table: str = DEFAULT_RELATIONS_TABLE,
    triage_table: str = DEFAULT_TRIAGE_TABLE,
    lang_codes: list[str] | None = None,
    limit_groups: int | None = None,
    replace_existing: bool = False,
) -> CuratedBuildResult:
    with get_connection(settings) as conn:
        run_id = start_run(
            conn,
            stage=CURATED_BUILD_STAGE,
            config={
                "source_table": source_table,
                "target_table": target_table,
                "relations_table": relations_table,
                "triage_table": triage_table,
                "lang_codes": lang_codes or [],
                "limit_groups": limit_groups,
                "replace_existing": replace_existing,
            },
        )

    groups_processed = 0
    entries_written = 0
    relations_written = 0
    triage_written = 0

    try:
        with get_connection(settings) as conn:
            if replace_existing:
                _reset_outputs(conn, target_table=target_table, relations_table=relations_table, triage_table=triage_table)

            current_rows: list[dict[str, Any]] = []
            current_key: tuple[str, str] | None = None

            for raw_row in iter_groupable_rows(conn, source_table=source_table, lang_codes=lang_codes):
                next_key = (
                    str(raw_row.get("lang_code") or "_"),
                    str(raw_row.get("normalized_word") or "_"),
                )
                if current_key is None:
                    current_key = next_key
                if next_key != current_key:
                    groups_processed += 1
                    output = build_curated_entry(current_rows)
                    entries_written += persist_curated_output(
                        conn,
                        output=output,
                        target_table=target_table,
                        relations_table=relations_table,
                        triage_table=triage_table,
                    )
                    relations_written += len(output.relations)
                    triage_written += len(output.triage_items)
                    if limit_groups is not None and groups_processed >= limit_groups:
                        current_rows = []
                        current_key = next_key
                        break
                    current_rows = [raw_row]
                    current_key = next_key
                else:
                    current_rows.append(raw_row)

            if current_rows and (limit_groups is None or groups_processed < limit_groups):
                groups_processed += 1
                output = build_curated_entry(current_rows)
                entries_written += persist_curated_output(
                    conn,
                    output=output,
                    target_table=target_table,
                    relations_table=relations_table,
                    triage_table=triage_table,
                )
                relations_written += len(output.relations)
                triage_written += len(output.triage_items)

            complete_run(
                conn,
                run_id=run_id,
                stats={
                    "groups_processed": groups_processed,
                    "entries_written": entries_written,
                    "relations_written": relations_written,
                    "triage_written": triage_written,
                },
            )

        return CuratedBuildResult(
            run_id=run_id,
            groups_processed=groups_processed,
            entries_written=entries_written,
            relations_written=relations_written,
            triage_written=triage_written,
        )
    except Exception as exc:
        with get_connection(settings) as conn:
            fail_run(conn, run_id=run_id, error=str(exc))
        raise


def iter_groupable_rows(conn, *, source_table: str, lang_codes: list[str] | None):
    table_identifier = _identifier_from_dotted(source_table)
    query = sql.SQL(
        """
        SELECT
            id,
            snapshot_id,
            run_id,
            source_line,
            word,
            lang,
            lang_code,
            pos,
            payload,
            lower(coalesce(word, payload->>'word', '')) AS normalized_word
        FROM {}
        """
    ).format(table_identifier)

    params: list[Any] = []
    if lang_codes:
        query += sql.SQL(" WHERE lang_code = ANY(%s)")
        params.append(lang_codes)

    query += sql.SQL(" ORDER BY lang_code, normalized_word, id")

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        columns = [desc.name for desc in cursor.description]
        for row in cursor:
            yield dict(zip(columns, row, strict=False))


def persist_curated_output(
    conn,
    *,
    output: CuratedBuildOutput,
    target_table: str,
    relations_table: str,
    triage_table: str,
) -> int:
    if output.entry is not None:
        upsert_entry(conn, target_table=target_table, entry=output.entry)
        replace_relations(conn, relations_table=relations_table, entry_id=output.entry["entry_id"], relations=output.relations)
    for triage_item in output.triage_items:
        insert_triage_item(conn, triage_table=triage_table, item=triage_item)
    conn.commit()
    return 1 if output.entry is not None else 0


def upsert_entry(conn, *, target_table: str, entry: dict[str, Any]) -> None:
    table_identifier = _identifier_from_dotted(target_table)
    query = sql.SQL(
        """
        INSERT INTO {} (
            entry_id,
            lang_code,
            normalized_word,
            word,
            payload,
            entry_flags,
            source_summary
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (entry_id)
        DO UPDATE SET
            lang_code = EXCLUDED.lang_code,
            normalized_word = EXCLUDED.normalized_word,
            word = EXCLUDED.word,
            payload = EXCLUDED.payload,
            entry_flags = EXCLUDED.entry_flags,
            source_summary = EXCLUDED.source_summary,
            updated_at = NOW()
        """
    ).format(table_identifier)

    with conn.cursor() as cursor:
        cursor.execute(
            query,
            (
                entry["entry_id"],
                entry["lang_code"],
                entry["normalized_word"],
                entry["word"],
                Jsonb(entry),
                entry["entry_flags"],
                Jsonb(entry["source_summary"]),
            ),
        )


def replace_relations(conn, *, relations_table: str, entry_id: str, relations: list[dict[str, Any]]) -> None:
    table_identifier = _identifier_from_dotted(relations_table)
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("DELETE FROM {} WHERE entry_id = %s").format(table_identifier), (entry_id,))
        if not relations:
            return
        values_sql = sql.SQL(", ").join(
            sql.SQL("(%s::uuid, %s::text, %s::text, %s::text, %s::text, %s::jsonb)")
            for _ in relations
        )
        query = sql.SQL(
            """
            INSERT INTO {} (
                entry_id,
                relation_type,
                target_word,
                target_lang_code,
                source_scope,
                payload
            ) VALUES {}
            """
        ).format(table_identifier, values_sql)
        params: list[Any] = []
        for relation in relations:
            params.extend(
                (
                    entry_id,
                    relation["relation_type"],
                    relation["target_word"],
                    relation.get("target_lang_code"),
                    relation["source_scope"],
                    Jsonb(relation["payload"]),
                )
            )
        cursor.execute(query, params)


def insert_triage_item(conn, *, triage_table: str, item: TriageItem) -> None:
    table_identifier = _identifier_from_dotted(triage_table)
    query = sql.SQL(
        """
        INSERT INTO {} (
            lang_code,
            word,
            reason_code,
            severity,
            suggested_action,
            raw_record_refs,
            payload
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    ).format(table_identifier)
    with conn.cursor() as cursor:
        cursor.execute(
            query,
            (
                item.lang_code,
                item.word,
                item.reason_code,
                item.severity,
                item.suggested_action,
                Jsonb(item.raw_record_refs),
                Jsonb(item.payload),
            ),
        )


def _reset_outputs(conn, *, target_table: str, relations_table: str, triage_table: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(_identifier_from_dotted(target_table)))
        cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(_identifier_from_dotted(relations_table)))
        cursor.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(_identifier_from_dotted(triage_table)))
    conn.commit()


def _identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
