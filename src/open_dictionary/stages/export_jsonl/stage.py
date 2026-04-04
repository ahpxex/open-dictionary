from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg import sql
from psycopg.types.json import Jsonb

from open_dictionary.config import RuntimeSettings
from open_dictionary.db.connection import get_connection
from open_dictionary.pipeline import complete_run, fail_run, start_run


EXPORT_AUDIT_JSONL_STAGE = "export.audit_jsonl"
# Backward-compatible alias while the CLI migrates away from the ambiguous
# export-jsonl naming.
EXPORT_JSONL_STAGE = EXPORT_AUDIT_JSONL_STAGE


@dataclass(frozen=True)
class ExportJSONLResult:
    run_id: UUID
    output_path: Path
    entry_count: int
    output_sha256: str


def run_export_jsonl_stage(
    *,
    settings: RuntimeSettings,
    output_path: Path,
    curated_table: str = "curated.entries",
    llm_table: str = "llm.entry_enrichments",
    artifact_table: str = "export.artifacts",
    model: str | None = None,
    prompt_version: str | None = None,
    include_unenriched: bool = True,
) -> ExportJSONLResult:
    with get_connection(settings) as conn:
        run_id = start_run(
            conn,
            stage=EXPORT_AUDIT_JSONL_STAGE,
            config={
                "output_path": str(output_path),
                "curated_table": curated_table,
                "llm_table": llm_table,
                "artifact_table": artifact_table,
                "model": model,
                "prompt_version": prompt_version,
                "include_unenriched": include_unenriched,
                "artifact_role": "audit",
            },
        )

    try:
        records = list(
            iter_export_records(
                settings=settings,
                curated_table=curated_table,
                llm_table=llm_table,
                model=model,
                prompt_version=prompt_version,
                include_unenriched=include_unenriched,
            )
        )
        documents = [record["document"] for record in records]
        curated_run_ids = sorted({record["curated_run_id"] for record in records if record["curated_run_id"]})
        llm_run_ids = sorted({record["llm_run_id"] for record in records if record["llm_run_id"]})
        output_sha256 = write_jsonl_atomic(output_path, documents)
        with get_connection(settings) as conn:
            record_export_artifact(
                conn,
                artifact_table=artifact_table,
                run_id=run_id,
                artifact_type="audit_jsonl",
                output_path=output_path,
                output_sha256=output_sha256,
                entry_count=len(documents),
                metadata={
                    "curated_table": curated_table,
                    "llm_table": llm_table,
                    "model": model,
                    "prompt_version": prompt_version,
                    "include_unenriched": include_unenriched,
                    "artifact_role": "audit",
                    "curated_run_ids": curated_run_ids,
                    "llm_run_ids": llm_run_ids,
                },
            )
            complete_run(
                conn,
                run_id=run_id,
                stats={
                    "output_path": str(output_path),
                    "entry_count": len(documents),
                    "output_sha256": output_sha256,
                    "curated_run_ids": curated_run_ids,
                    "llm_run_ids": llm_run_ids,
                },
            )
        return ExportJSONLResult(
            run_id=run_id,
            output_path=Path(output_path),
            entry_count=len(documents),
            output_sha256=output_sha256,
        )
    except Exception as exc:
        with get_connection(settings) as conn:
            fail_run(conn, run_id=run_id, error=str(exc))
        raise


def iter_export_records(
    *,
    settings: RuntimeSettings,
    curated_table: str,
    llm_table: str,
    model: str | None,
    prompt_version: str | None,
    include_unenriched: bool,
):
    curated_identifier = identifier_from_dotted(curated_table)
    llm_identifier = identifier_from_dotted(llm_table)

    latest_enrichment_sql = sql.SQL(
        """
        SELECT DISTINCT ON (entry_id)
            run_id,
            entry_id,
            model,
            prompt_version,
            response_payload
        FROM {}
        WHERE status = 'succeeded'
        """
    ).format(llm_identifier)

    params: list[Any] = []
    if model is not None:
        latest_enrichment_sql += sql.SQL(" AND model = %s")
        params.append(model)
    if prompt_version is not None:
        latest_enrichment_sql += sql.SQL(" AND prompt_version = %s")
        params.append(prompt_version)
    latest_enrichment_sql += sql.SQL(" ORDER BY entry_id, created_at DESC")

    join_type = sql.SQL("LEFT JOIN") if include_unenriched else sql.SQL("JOIN")
    query = sql.SQL(
        """
        WITH latest_enrichment AS (
            {latest_enrichment_sql}
        )
        SELECT
            e.run_id,
            e.entry_id,
            e.lang_code,
            e.normalized_word,
            e.word,
            e.payload,
            l.run_id,
            l.model,
            l.prompt_version,
            l.response_payload
        FROM {curated_table} AS e
        {join_type} latest_enrichment AS l
          ON l.entry_id = e.entry_id
        ORDER BY e.lang_code, e.normalized_word
        """
    ).format(
        latest_enrichment_sql=latest_enrichment_sql,
        curated_table=curated_identifier,
        join_type=join_type,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            for curated_run_id, entry_id, lang_code, normalized_word, word, payload, llm_run_id, enrich_model, enrich_prompt_version, response_payload in cursor.fetchall():
                yield {
                    "curated_run_id": str(curated_run_id) if curated_run_id is not None else None,
                    "llm_run_id": str(llm_run_id) if llm_run_id is not None else None,
                    "document": {
                        "entry_id": str(entry_id),
                        "lang_code": lang_code,
                        "normalized_word": normalized_word,
                        "word": word,
                        "curated": payload,
                        "llm": (
                            {
                                "model": enrich_model,
                                "prompt_version": enrich_prompt_version,
                                "payload": response_payload,
                            }
                            if response_payload is not None
                            else None
                        ),
                    },
                }


def iter_export_documents(
    *,
    settings: RuntimeSettings,
    curated_table: str,
    llm_table: str,
    model: str | None,
    prompt_version: str | None,
    include_unenriched: bool,
):
    for record in iter_export_records(
        settings=settings,
        curated_table=curated_table,
        llm_table=llm_table,
        model=model,
        prompt_version=prompt_version,
        include_unenriched=include_unenriched,
    ):
        yield record["document"]


def write_jsonl_atomic(output_path: Path, documents: list[dict[str, Any]]) -> str:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(output_path.name + ".part")

    digest = hashlib.sha256()
    with temp_path.open("w", encoding="utf-8") as handle:
        for document in documents:
            line = json.dumps(document, ensure_ascii=False, sort_keys=True)
            handle.write(line)
            handle.write("\n")
            digest.update(line.encode("utf-8"))
            digest.update(b"\n")

    temp_path.replace(output_path)
    return digest.hexdigest()


def record_export_artifact(
    conn,
    *,
    artifact_table: str,
    run_id: UUID,
    artifact_type: str,
    output_path: Path,
    output_sha256: str,
    entry_count: int,
    metadata: dict[str, Any],
) -> None:
    artifact_identifier = identifier_from_dotted(artifact_table)
    with conn.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    run_id,
                    artifact_type,
                    output_path,
                    output_sha256,
                    entry_count,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """
            ).format(artifact_identifier),
            (
                run_id,
                artifact_type,
                str(output_path),
                output_sha256,
                entry_count,
                Jsonb(metadata),
            ),
        )
    conn.commit()


def run_export_audit_jsonl_stage(**kwargs) -> ExportJSONLResult:
    return run_export_jsonl_stage(**kwargs)


def identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)
