from __future__ import annotations

import concurrent.futures
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from psycopg import sql
from psycopg.cursor import Cursor

from open_dictionary.db.access import DatabaseAccess
from open_dictionary.llm.define import Definition, define

DEFAULT_TABLE_NAME = "dictionary_filtered_en"
DEFAULT_SOURCE_COLUMN = "data"
DEFAULT_TARGET_COLUMN = "new_speak"
DEFAULT_FETCH_BATCH_SIZE = 400
DEFAULT_LLM_BATCH_SIZE = 40
DEFAULT_MAX_WORKERS = DEFAULT_LLM_BATCH_SIZE
DEFAULT_MAX_RETRIES = 5
DEFAULT_INITIAL_BACKOFF_SECONDS = 5.0
DEFAULT_MAX_BACKOFF_SECONDS = 60.0
DEFAULT_PROGRESS_EVERY_ROWS = 120
DEFAULT_PROGRESS_EVERY_SECONDS = 30.0


@dataclass(frozen=True)
class RowPayload:
    row_id: int
    payload: str


def enrich_definitions(
    *,
    table_name: str = DEFAULT_TABLE_NAME,
    source_column: str = DEFAULT_SOURCE_COLUMN,
    target_column: str = DEFAULT_TARGET_COLUMN,
    fetch_batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
    llm_batch_size: int = DEFAULT_LLM_BATCH_SIZE,
    max_workers: int | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS,
    max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    progress_every_rows: int = DEFAULT_PROGRESS_EVERY_ROWS,
    progress_every_seconds: float = DEFAULT_PROGRESS_EVERY_SECONDS,
    recompute_existing: bool = False,
) -> None:
    """Generate LLM-enriched dictionary entries and store them in a JSONB column."""

    if llm_batch_size <= 0:
        raise ValueError("llm_batch_size must be positive")
    if fetch_batch_size <= 0:
        raise ValueError("fetch_batch_size must be positive")
    if max_workers is not None and max_workers <= 0:
        raise ValueError("max_workers must be positive when provided")

    data_access = DatabaseAccess()
    _ensure_target_column(data_access, table_name, target_column)

    where_clause = None
    if not recompute_existing:
        where_clause = sql.SQL("{column} IS NULL").format(
            column=sql.Identifier(target_column)
        )

    max_workers = max_workers or llm_batch_size

    print(
        "[llm-define] starting "
        f"table={table_name} source={source_column} target={target_column} "
        f"fetch_batch={fetch_batch_size} llm_batch={llm_batch_size} "
        f"max_workers={max_workers} retries={max_retries} "
        f"backoff_start={initial_backoff_seconds}s backoff_max={max_backoff_seconds}s "
        f"recompute_existing={recompute_existing}",
        flush=True,
    )

    processed = 0
    succeeded = 0
    failed = 0
    start_time = time.monotonic()
    last_log_time = start_time
    last_log_count = 0
    pending_rows: list[RowPayload] = []

    def emit_progress(force: bool = False) -> None:
        nonlocal last_log_time, last_log_count
        now = time.monotonic()
        should_emit = force
        if not should_emit:
            if progress_every_rows and processed - last_log_count >= progress_every_rows:
                should_emit = True
            if progress_every_seconds and (now - last_log_time) >= progress_every_seconds:
                should_emit = True
        if should_emit:
            _report_progress(processed, succeeded, failed, start_time)
            last_log_time = now
            last_log_count = processed

    def record_result(is_success: bool) -> None:
        nonlocal processed, succeeded, failed
        processed += 1
        if is_success:
            succeeded += 1
        else:
            failed += 1
        emit_progress(force=True)

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
                order_by=("common_score",),
            )

            for row in row_stream:
                row_id = row.get("id")
                if row_id is None:
                    failed += 1
                    processed += 1
                    print("[llm-define] skipped row without id", flush=True)
                    emit_progress(force=True)
                    continue

                payload = _load_payload(row.get(source_column))
                if payload is None:
                    failed += 1
                    processed += 1
                    print(
                        f"[llm-define] row_id={row_id} missing or invalid {source_column}",
                        flush=True,
                    )
                    emit_progress(force=True)
                    continue

                sanitized_payload = _sanitize_payload(payload)

                pending_rows.append(RowPayload(int(row_id), sanitized_payload))

                if len(pending_rows) >= llm_batch_size:
                    _process_batch(
                        cursor,
                        table_name,
                        target_column,
                        pending_rows,
                        max_workers,
                        max_retries,
                        initial_backoff_seconds,
                        max_backoff_seconds,
                        record_result,
                    )
                    pending_rows.clear()
                    update_conn.commit()

            if pending_rows:
                _process_batch(
                    cursor,
                    table_name,
                    target_column,
                    pending_rows,
                    max_workers,
                    max_retries,
                    initial_backoff_seconds,
                    max_backoff_seconds,
                    record_result,
                )
                pending_rows.clear()
                update_conn.commit()

    _report_completion(processed, succeeded, failed, start_time)


def _process_batch(
    cursor: Cursor[Any],
    table_name: str,
    target_column: str,
    rows: Sequence[RowPayload],
    max_workers: int,
    max_retries: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    record_result: Callable[[bool], None],
) -> None:
    successes = _run_llm_batch(
        rows,
        max_workers,
        max_retries,
        initial_backoff_seconds,
        max_backoff_seconds,
        record_result,
    )

    _apply_updates(cursor, table_name, target_column, successes)


def _run_llm_batch(
    rows: Sequence[RowPayload],
    max_workers: int,
    max_retries: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    record_result: Callable[[bool], None],
) -> list[tuple[int, str]]:
    successes: list[tuple[int, str]] = []

    worker_count = min(max(len(rows), 1), max_workers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_row = {
            executor.submit(
                _define_with_retry,
                row.payload,
                max_retries,
                initial_backoff_seconds,
                max_backoff_seconds,
            ): row
            for row in rows
        }

        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future]
            try:
                definition = future.result()
            except Exception as exc:  # pragma: no cover - network/runtime failures
                print(
                    f"[llm-define] row_id={row.row_id} failed: {exc}",
                    flush=True,
                )
                _log_error(row.row_id, row.payload, exc)
                record_result(False)
            else:
                payload_json = json.dumps(
                    definition.model_dump(mode="json"),
                    ensure_ascii=False,
                )
                successes.append((row.row_id, payload_json))
                record_result(True)

    return successes


def _define_with_retry(
    payload: str,
    max_retries: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
) -> Definition:
    attempt = 0
    while True:
        try:
            return define(payload)
        except Exception as exc:  # pragma: no cover - passthrough for runtime errors
            attempt += 1
            if attempt >= max_retries:
                raise exc

            backoff = min(
                max_backoff_seconds,
                initial_backoff_seconds * (2 ** (attempt - 1)),
            )
            jitter = random.uniform(0.0, initial_backoff_seconds)
            sleep_seconds = max(backoff + jitter, 0.0)
            time.sleep(sleep_seconds)


def _apply_updates(
    cursor: Cursor[Any],
    table_name: str,
    target_column: str,
    payloads: Sequence[tuple[int, str]],
) -> None:
    if not payloads:
        return

    values_sql = sql.SQL(", ").join(
        sql.SQL("(%s::bigint, %s::text)") for _ in payloads
    )

    update_sql = sql.SQL(
        """
        UPDATE {table} AS t
        SET {column} = v.payload::jsonb
        FROM (VALUES {values}) AS v(id, payload)
        WHERE t.id = v.id
        """
    ).format(
        table=sql.Identifier(table_name),
        column=sql.Identifier(target_column),
        values=values_sql,
    )

    params: list[Any] = []
    for row_id, payload_json in payloads:
        params.extend((row_id, payload_json))

    cursor.execute(update_sql, params)


def _ensure_target_column(
    data_access: DatabaseAccess,
    table_name: str,
    target_column: str,
) -> None:
    with data_access.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS {column} JSONB
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    column=sql.Identifier(target_column),
                )
            )
        conn.commit()


def _load_payload(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
        return decoded
    if isinstance(value, memoryview):
        return _load_payload(value.tobytes())
    return None


def _sanitize_payload(payload: str, max_length: int = 1000) -> str:
    """Trim overly large payloads by dropping noisy fields."""
    if len(payload) <= max_length:
        return payload

    try:
        payload_obj = json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return payload

    if isinstance(payload_obj, dict):
        for key in ("derived", "forms", "glosses"):
            payload_obj.pop(key, None)

        senses = payload_obj.get("senses")
        if isinstance(senses, list):
            for sense in senses:
                if isinstance(sense, dict):
                    sense.pop("glosses", None)

        return json.dumps(payload_obj, ensure_ascii=False)

    return payload


def _report_progress(
    processed: int,
    succeeded: int,
    failed: int,
    start_time: float,
) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    rate = processed / elapsed
    print(
        f"[llm-define] progress processed={processed:,} "
        f"succeeded={succeeded:,} failed={failed:,} "
        f"elapsed={elapsed:,.1f}s rate={rate:,.0f} rows/s",
        flush=True,
    )


def _report_completion(
    processed: int,
    succeeded: int,
    failed: int,
    start_time: float,
) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    rate = processed / elapsed if processed else 0.0
    print(
        f"[llm-define] completed processed={processed:,} "
        f"succeeded={succeeded:,} failed={failed:,} "
        f"elapsed={elapsed:,.1f}s avg_rate={rate:,.0f} rows/s",
        flush=True,
    )


def _log_error(
    row_id: int,
    payload: str,
    error: Exception,
    log_file: str = "data/llm_define_errors.log",
) -> None:
    """Write error details to a log file."""
    import os
    from datetime import datetime

    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            timestamp = datetime.now().isoformat()
            f.write(f"\n{'='*80}\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Row ID: {row_id}\n")
            f.write(f"Error: {type(error).__name__}: {error}\n")
            f.write(f"\nPayload:\n{payload}\n")
            
            # Log the LLM response if it's attached to the exception
            if hasattr(error, 'llm_response'):
                f.write(f"\nLLM Response:\n{error.llm_response}\n") # type: ignore
            
            f.write(f"{'='*80}\n")
    except Exception as log_exc:  # pragma: no cover
        print(
            f"[llm-define] failed to write error log: {log_exc}",
            flush=True,
        )


__all__ = [
    "DEFAULT_TABLE_NAME",
    "DEFAULT_SOURCE_COLUMN",
    "DEFAULT_TARGET_COLUMN",
    "DEFAULT_FETCH_BATCH_SIZE",
    "DEFAULT_LLM_BATCH_SIZE",
    "DEFAULT_MAX_WORKERS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_INITIAL_BACKOFF_SECONDS",
    "DEFAULT_MAX_BACKOFF_SECONDS",
    "DEFAULT_PROGRESS_EVERY_ROWS",
    "DEFAULT_PROGRESS_EVERY_SECONDS",
    "enrich_definitions",
]
