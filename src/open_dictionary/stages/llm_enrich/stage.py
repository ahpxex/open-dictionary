from __future__ import annotations

import concurrent.futures
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg import sql
from psycopg.types.json import Jsonb

from open_dictionary.config import RuntimeSettings
from open_dictionary.contracts import DEFAULT_DEFINITION_LANGUAGE, LanguageSpec, normalize_language_spec
from open_dictionary.db.connection import get_connection
from open_dictionary.llm.client import LLMClientError, OpenAICompatLLMClient
from open_dictionary.llm.config import load_llm_settings
from open_dictionary.llm.prompt import COMPACT_RETRY_MAX_TOKENS, DEFAULT_MAX_TOKENS, PROMPT_VERSION, PromptBundle, build_generation_source_payload, build_prompt_bundle, build_user_prompt
from open_dictionary.pipeline import ProgressCallback, ThrottledProgressReporter, complete_run, emit_progress, fail_run, start_run

from .schema import build_expected_generation_targets, validate_enrichment_payload


LLM_ENRICH_STAGE = "definitions.generate"
PERSIST_COMMIT_INTERVAL = 25


@dataclass(frozen=True)
class LLMEnrichResult:
    run_id: UUID
    processed: int
    succeeded: int
    failed: int


def run_llm_enrich_stage(
    *,
    settings: RuntimeSettings,
    env_file: str = ".env",
    source_table: str = "curated.entries",
    target_table: str = "llm.entry_enrichments",
    prompt_version: str = PROMPT_VERSION,
    definition_language: LanguageSpec | dict[str, Any] = DEFAULT_DEFINITION_LANGUAGE,
    limit_entries: int | None = None,
    max_workers: int = 4,
    max_retries: int = 3,
    recompute_existing: bool = False,
    client: OpenAICompatLLMClient | None = None,
    progress_callback: ProgressCallback | None = None,
) -> LLMEnrichResult:
    llm_settings = load_llm_settings(env_file=env_file)
    llm_client = client or OpenAICompatLLMClient(llm_settings)
    language = normalize_language_spec(definition_language)
    prompt_bundle = build_prompt_bundle(
        prompt_version=prompt_version,
        definition_language=language,
    )

    with get_connection(settings) as conn:
        run_id = start_run(
            conn,
            stage=LLM_ENRICH_STAGE,
            config={
                "source_table": source_table,
                "target_table": target_table,
                "prompt_template_version": prompt_bundle.template_version,
                "prompt_version": prompt_bundle.resolved_prompt_version,
                "definition_language": language.as_dict(),
                "model": llm_settings.model,
                "limit_entries": limit_entries,
                "max_workers": max_workers,
                "max_retries": max_retries,
                "recompute_existing": recompute_existing,
            },
        )
        ensure_prompt_version(conn, prompt_bundle=prompt_bundle)

    processed = 0
    succeeded = 0
    failed = 0

    try:
        items = list(
            iter_curated_entries(
                settings,
                source_table=source_table,
                target_table=target_table,
                prompt_bundle=prompt_bundle,
                model=llm_settings.model,
                recompute_existing=recompute_existing,
                limit_entries=limit_entries,
            )
        )
        emit_progress(
            progress_callback,
            stage=LLM_ENRICH_STAGE,
            event="generate_start",
            queued_entries=len(items),
            max_workers=max_workers,
            max_retries=max_retries,
            prompt_version=prompt_bundle.resolved_prompt_version,
            prompt_template_version=prompt_bundle.template_version,
            definition_language_code=language.code,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    enrich_one_entry,
                    entry=item,
                    llm_client=llm_client,
                    prompt_bundle=prompt_bundle,
                    model=llm_settings.model,
                    max_retries=max_retries,
                ): item
                for item in items
            }

            with get_connection(settings) as conn:
                reporter = ThrottledProgressReporter(progress_callback, stage=LLM_ENRICH_STAGE)
                pending_writes = 0
                for future in concurrent.futures.as_completed(future_map):
                    processed += 1
                    try:
                        record = future.result()
                    except Exception as exc:
                        failed += 1
                        persist_enrichment_failure(
                            conn,
                            target_table=target_table,
                            run_id=run_id,
                            entry_id=future_map[future]["entry_id"],
                            model=llm_settings.model,
                            prompt_version=prompt_bundle.resolved_prompt_version,
                            definition_language=language,
                            input_hash=future_map[future]["input_hash"],
                            request_payload=future_map[future]["request_payload"],
                            retries=max_retries,
                            error=str(exc),
                        )
                    else:
                        succeeded += 1
                        persist_enrichment_success(
                            conn,
                            target_table=target_table,
                            run_id=run_id,
                            record=record,
                        )
                    pending_writes += 1
                    if pending_writes >= PERSIST_COMMIT_INTERVAL:
                        conn.commit()
                        pending_writes = 0
                    reporter.report(
                        event="generate_progress",
                        processed=processed,
                        queued_entries=len(items),
                        succeeded=succeeded,
                        failed=failed,
                    )

                if pending_writes:
                    conn.commit()

                complete_run(
                    conn,
                    run_id=run_id,
                    stats={
                        "processed": processed,
                        "succeeded": succeeded,
                        "failed": failed,
                        "model": llm_settings.model,
                        "prompt_template_version": prompt_bundle.template_version,
                        "prompt_version": prompt_bundle.resolved_prompt_version,
                        "definition_language": language.as_dict(),
                    },
                )
                emit_progress(
                    progress_callback,
                    stage=LLM_ENRICH_STAGE,
                    event="generate_complete",
                    processed=processed,
                    queued_entries=len(items),
                    succeeded=succeeded,
                    failed=failed,
                    prompt_version=prompt_bundle.resolved_prompt_version,
                    prompt_template_version=prompt_bundle.template_version,
                    definition_language_code=language.code,
                )

        return LLMEnrichResult(run_id=run_id, processed=processed, succeeded=succeeded, failed=failed)
    except Exception as exc:
        with get_connection(settings) as conn:
            fail_run(conn, run_id=run_id, error=str(exc))
        raise


def ensure_prompt_version(conn, *, prompt_bundle: PromptBundle) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO llm.prompt_versions (
                prompt_version,
                prompt_text,
                output_contract,
                definition_language_code,
                definition_language_name,
                prompt_bundle
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (prompt_version) DO NOTHING
            """,
            (
                prompt_bundle.resolved_prompt_version,
                prompt_bundle.system_prompt,
                Jsonb(prompt_bundle.output_contract),
                prompt_bundle.definition_language.code,
                prompt_bundle.definition_language.name,
                Jsonb(prompt_bundle.as_metadata()),
            ),
        )
    conn.commit()


def iter_curated_entries(
    settings: RuntimeSettings,
    *,
    source_table: str,
    target_table: str,
    prompt_bundle: PromptBundle,
    model: str,
    recompute_existing: bool,
    limit_entries: int | None,
):
    source_identifier = identifier_from_dotted(source_table)
    target_identifier = identifier_from_dotted(target_table)
    query = sql.SQL(
        """
        SELECT e.entry_id, e.payload
        FROM {} AS e
        """
    ).format(source_identifier)
    params: list[Any] = []
    if not recompute_existing:
        query += sql.SQL(
            """
            WHERE NOT EXISTS (
                SELECT 1
                FROM {} AS x
                WHERE x.entry_id = e.entry_id
                  AND x.model = %s
                  AND x.prompt_version = %s
                  AND x.definition_language_code = %s
                  AND x.status = 'succeeded'
            )
            """
        ).format(target_identifier)
        params.extend(
            (
                model,
                prompt_bundle.resolved_prompt_version,
                prompt_bundle.definition_language.code,
            )
        )
    query += sql.SQL(" ORDER BY e.lang_code, e.normalized_word")
    if limit_entries is not None:
        query += sql.SQL(" LIMIT %s")
        params.append(limit_entries)

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            for entry_id, payload in cursor.fetchall():
                request_payload = {
                    "entry": build_generation_source_payload(
                        payload,
                        definition_language=prompt_bundle.definition_language,
                    ),
                    "prompt_template_version": prompt_bundle.template_version,
                    "prompt_version": prompt_bundle.resolved_prompt_version,
                    "definition_language": prompt_bundle.definition_language.as_dict(),
                }
                yield {
                    "entry_id": entry_id,
                    "payload": payload,
                    "request_payload": request_payload,
                    "input_hash": compute_input_hash(request_payload),
                }


def enrich_one_entry(
    *,
    entry: dict[str, Any],
    llm_client: OpenAICompatLLMClient,
    prompt_bundle: PromptBundle,
    model: str,
    max_retries: int,
) -> dict[str, Any]:
    request_payload = entry["request_payload"]
    input_hash = entry["input_hash"]
    generation_source_payload = request_payload["entry"]
    expected_pos_targets = build_expected_generation_targets(generation_source_payload)
    user_prompt = build_user_prompt(generation_source_payload)
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        use_compact_retry_prompt = attempt > 1
        system_prompt = (
            prompt_bundle.compact_retry_system_prompt
            if use_compact_retry_prompt
            else prompt_bundle.system_prompt
        )
        max_tokens = COMPACT_RETRY_MAX_TOKENS if use_compact_retry_prompt else DEFAULT_MAX_TOKENS
        try:
            raw_response = llm_client.generate_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.0,
                max_tokens=max_tokens,
            )
            response_payload = json.loads(raw_response)
            validated = validate_enrichment_payload(
                response_payload,
                expected_pos_targets=expected_pos_targets,
            )
            return {
                "entry_id": entry["entry_id"],
                "model": model,
                "prompt_version": prompt_bundle.resolved_prompt_version,
                "prompt_template_version": prompt_bundle.template_version,
                "definition_language": prompt_bundle.definition_language,
                "input_hash": input_hash,
                "request_payload": request_payload,
                "response_payload": validated,
                "raw_response": raw_response,
                "retries": attempt - 1,
            }
        except (json.JSONDecodeError, ValueError, LLMClientError) as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(min(0.5 * attempt, 2.0))

    assert last_error is not None
    raise last_error


def persist_enrichment_success(conn, *, target_table: str, run_id: UUID, record: dict[str, Any]) -> None:
    target_identifier = identifier_from_dotted(target_table)
    definition_language = normalize_language_spec(record["definition_language"])
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    run_id,
                    entry_id,
                    model,
                    prompt_version,
                    definition_language_code,
                    definition_language_name,
                    input_hash,
                    status,
                    request_payload,
                    response_payload,
                    raw_response,
                    retries
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'succeeded', %s, %s, %s, %s)
                """
            ).format(target_identifier),
            (
                run_id,
                record["entry_id"],
                record["model"],
                record["prompt_version"],
                definition_language.code,
                definition_language.name,
                record["input_hash"],
                Jsonb(record["request_payload"]),
                Jsonb(record["response_payload"]),
                record["raw_response"],
                record["retries"],
            ),
        )


def persist_enrichment_failure(
    conn,
    *,
    target_table: str,
    run_id: UUID,
    entry_id: str,
    model: str,
    prompt_version: str,
    definition_language: LanguageSpec | dict[str, Any],
    input_hash: str,
    request_payload: dict[str, Any],
    retries: int,
    error: str,
) -> None:
    target_identifier = identifier_from_dotted(target_table)
    language = normalize_language_spec(definition_language)
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    run_id,
                    entry_id,
                    model,
                    prompt_version,
                    definition_language_code,
                    definition_language_name,
                    input_hash,
                    status,
                    request_payload,
                    error,
                    retries
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'failed', %s, %s, %s)
                """
            ).format(target_identifier),
            (
                run_id,
                entry_id,
                model,
                prompt_version,
                language.code,
                language.name,
                input_hash,
                Jsonb(request_payload),
                error,
                retries,
            ),
        )


def compute_input_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
