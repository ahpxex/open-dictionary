from __future__ import annotations

import json
import time
from typing import Any, Sequence

from psycopg import sql
from psycopg.cursor import Cursor

from open_dictionary.db.access import DatabaseAccess

try:
    from toon import encode as toon_encode
except ImportError:
    toon_encode = None

FETCH_BATCH_SIZE = 5000
UPDATE_BATCH_SIZE = 5000
PROGRESS_EVERY_ROWS = 20_000
PROGRESS_EVERY_SECONDS = 30.0

_ALLOWED_TOP_LEVEL_KEYS = (
    "pos",
    "word",
    "forms",
    "derived",
    "etymology_text",
)
_SENSE_KEYS = ("glosses", "raw_glosses")


def preprocess_entries(
    *,
    table_name: str,
    source_column: str = "data",
    target_column: str = "process",
    fetch_batch_size: int = FETCH_BATCH_SIZE,
    update_batch_size: int = UPDATE_BATCH_SIZE,
    progress_every_rows: int = PROGRESS_EVERY_ROWS,
    progress_every_seconds: float = PROGRESS_EVERY_SECONDS,
    recompute_existing: bool = False,
    use_toon: bool = False,
) -> None:
    """Normalize Wiktionary payloads into a slimmer JSONB column."""

    if fetch_batch_size <= 0:
        raise ValueError("fetch_batch_size must be positive")
    if update_batch_size <= 0:
        raise ValueError("update_batch_size must be positive")
    if use_toon and toon_encode is None:
        raise ValueError("TOON format requested but 'toon' package is not installed")

    data_access = DatabaseAccess()
    _ensure_target_column(data_access, table_name, target_column, use_toon)

    where_clause = None
    if not recompute_existing:
        where_clause = sql.SQL("{column} IS NULL").format(
            column=sql.Identifier(target_column)
        )

    print(
        "[pre-process] starting "
        f"table={table_name} source={source_column} target={target_column} "
        f"fetch_batch={fetch_batch_size} update_batch={update_batch_size} "
        f"progress_rows={progress_every_rows} progress_seconds={progress_every_seconds} "
        f"recompute_existing={recompute_existing} use_toon={use_toon}",
        flush=True,
    )

    processed = 0
    updated = 0
    skipped = 0
    start_time = time.monotonic()
    last_log_time = start_time
    pending_updates: list[tuple[int, str]] = []

    with data_access.get_connection() as update_conn:
        with update_conn.cursor() as cursor:
            row_stream = data_access.iterate_table(
                table_name,
                batch_size=fetch_batch_size,
                columns=(
                    "id",
                    source_column,
                    target_column,
                ),
                where=where_clause,
                order_by=("id",),
            )

            for row in row_stream:
                row_id = row.get("id")
                if row_id is None:
                    skipped += 1
                    continue

                payload = _load_payload(row.get(source_column))
                if payload is None:
                    skipped += 1
                    continue

                processed_payload = _preprocess_payload(payload)

                if use_toon:
                    payload_str = convert_to_toon(processed_payload)
                else:
                    payload_str = json.dumps(
                        processed_payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )

                pending_updates.append((int(row_id), payload_str))

                if len(pending_updates) >= update_batch_size:
                    batch_count = _flush_updates(
                        cursor,
                        table_name,
                        target_column,
                        pending_updates,
                        use_toon,
                    )
                    update_conn.commit()
                    updated += batch_count
                    pending_updates.clear()

                processed += 1

                emit_progress = False
                now = time.monotonic()
                if processed == 1:
                    emit_progress = True
                elif progress_every_rows and processed % progress_every_rows == 0:
                    emit_progress = True
                elif progress_every_seconds and (now - last_log_time) >= progress_every_seconds:
                    emit_progress = True

                if emit_progress:
                    _report_progress(processed, updated, skipped, start_time)
                    last_log_time = now

            if pending_updates:
                batch_count = _flush_updates(
                    cursor,
                    table_name,
                    target_column,
                    pending_updates,
                    use_toon,
                )
                update_conn.commit()
                updated += batch_count
                pending_updates.clear()

    _report_completion(processed, updated, skipped, start_time)


def _ensure_target_column(
    data_access: DatabaseAccess,
    table_name: str,
    target_column: str,
    use_toon: bool = False,
) -> None:
    column_type = "TEXT" if use_toon else "JSONB"
    with data_access.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS {column} {type}
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    column=sql.Identifier(target_column),
                    type=sql.SQL(column_type),
                )
            )
        conn.commit()


def _flush_updates(
    cursor: Cursor[Any],
    table_name: str,
    target_column: str,
    payloads: Sequence[tuple[int, str]],
    use_toon: bool = False,
) -> int:
    if not payloads:
        return 0

    values_sql = sql.SQL(", ").join(
        sql.SQL("(%s::bigint, %s::text)") for _ in payloads
    )

    # When using TOON, store as TEXT; otherwise cast to JSONB
    cast_type = "text" if use_toon else "jsonb"

    update_sql = sql.SQL(
        """
        UPDATE {table} AS t
        SET {column} = v.payload::{cast_type}
        FROM (VALUES {values}) AS v(id, payload)
        WHERE t.id = v.id
        """
    ).format(
        table=sql.Identifier(table_name),
        column=sql.Identifier(target_column),
        cast_type=sql.SQL(cast_type),
        values=values_sql,
    )

    params: list[Any] = []
    for row_id, payload_json in payloads:
        params.extend((row_id, payload_json))

    cursor.execute(update_sql, params)
    return cursor.rowcount


def _preprocess_payload(payload: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key in _ALLOWED_TOP_LEVEL_KEYS:
        if key in payload:
            value = payload[key]
            if value is not None:
                result[key] = value

    senses = _extract_senses(payload.get("senses"))
    if senses is not None:
        result["senses"] = senses

    sounds = _extract_sounds(payload.get("sounds"))
    if sounds is not None:
        result["sounds"] = sounds

    related = _extract_related(payload.get("related"))
    if related is not None:
        result["related"] = related

    return result


def convert_to_toon(payload: dict[str, Any]) -> str:
    """Convert a preprocessed dictionary payload to TOON format.

    Args:
        payload: The preprocessed dictionary entry (output of _preprocess_payload).

    Returns:
        A string representation in TOON format.

    Raises:
        ValueError: If the toon package is not available.
    """
    if toon_encode is None:
        raise ValueError("TOON format requested but 'toon' package is not installed")

    return toon_encode(payload)


def _extract_senses(value: Any) -> list[dict[str, list[str]]] | None:
    if not isinstance(value, list):
        return None

    senses: list[dict[str, list[str]]] = []
    for item in value:
        if not isinstance(item, dict):
            continue

        sense: dict[str, list[str]] = {}
        for key in _SENSE_KEYS:
            normalized = _ensure_string_list(item.get(key))
            if normalized is not None:
                sense[key] = normalized

        if sense:
            senses.append(sense)

    if not senses:
        return None
    return senses


def _extract_sounds(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None

    urls: list[str] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, dict):
            candidate = item.get("ogg_url")
        else:
            candidate = None

        if not isinstance(candidate, str):
            continue

        trimmed = candidate.strip()
        if not trimmed or trimmed in seen:
            continue

        urls.append(trimmed)
        seen.add(trimmed)

    if not urls:
        return None
    return urls


def _extract_related(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None

    items: list[str] = []
    seen: set[str] = set()

    for entry in value:
        candidate: Any
        if isinstance(entry, dict):
            candidate = entry.get("word")
        elif isinstance(entry, (list, tuple)) and entry:
            candidate = entry[0]
        else:
            candidate = entry

        if not isinstance(candidate, str):
            continue

        word = candidate.strip()
        if not word or word in seen:
            continue

        items.append(word)
        seen.add(word)

    if not items:
        return None
    return items


def _ensure_string_list(value: Any) -> list[str] | None:
    if value is None:
        return None

    items: list[str] = []

    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            items.append(trimmed)
    elif isinstance(value, (list, tuple)):
        for entry in value:
            if not isinstance(entry, str):
                continue
            trimmed = entry.strip()
            if trimmed:
                items.append(trimmed)
    else:
        return None

    if not items:
        return None
    return items


def _load_payload(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
        return _load_payload(decoded)
    if isinstance(value, memoryview):
        return _load_payload(value.tobytes())
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(decoded, dict):
            return decoded
        return None
    return None


def _report_progress(
    processed: int,
    updated: int,
    skipped: int,
    start_time: float,
) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    processed_rate = processed / elapsed
    print(
        f"[pre-process] progress processed={processed:,} "
        f"updated={updated:,} skipped={skipped:,} "
        f"elapsed={elapsed:,.1f}s rate={processed_rate:,.0f} rows/s",
        flush=True,
    )


def _report_completion(
    processed: int,
    updated: int,
    skipped: int,
    start_time: float,
) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    processed_rate = processed / elapsed if processed else 0.0
    print(
        f"[pre-process] completed processed={processed:,} "
        f"updated={updated:,} skipped={skipped:,} "
        f"elapsed={elapsed:,.1f}s avg_rate={processed_rate:,.0f} rows/s",
        flush=True,
    )


__all__ = [
    "FETCH_BATCH_SIZE",
    "UPDATE_BATCH_SIZE",
    "PROGRESS_EVERY_ROWS",
    "PROGRESS_EVERY_SECONDS",
    "preprocess_entries",
    "convert_to_toon",
]

