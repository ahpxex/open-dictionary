from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg import sql

from open_dictionary.config import RuntimeSettings
from open_dictionary.contracts import DEFAULT_DEFINITION_LANGUAGE, LanguageSpec, normalize_language_spec
from open_dictionary.db.connection import get_connection
from open_dictionary.llm.prompt import PROMPT_VERSION, build_pos_group_id, build_prompt_bundle
from open_dictionary.pipeline import ProgressCallback, ThrottledProgressReporter, complete_run, emit_progress, fail_run, start_run, update_run_config
from open_dictionary.stages.export_distribution_jsonl.schema import DISTRIBUTION_SCHEMA_VERSION, validate_distribution_document
from open_dictionary.stages.export_jsonl.stage import ExportJSONLResult, identifier_from_dotted, record_export_artifact, write_jsonl_atomic


EXPORT_DISTRIBUTION_JSONL_STAGE = "distribution.export"


def run_export_distribution_jsonl_stage(
    *,
    settings: RuntimeSettings,
    output_path: Path,
    curated_table: str = "curated.entries",
    llm_table: str = "llm.entry_enrichments",
    artifact_table: str = "export.artifacts",
    model: str | None = None,
    prompt_version: str = PROMPT_VERSION,
    definition_language: LanguageSpec | dict[str, Any] = DEFAULT_DEFINITION_LANGUAGE,
    parent_run_id: UUID | None = None,
    progress_callback: ProgressCallback | None = None,
) -> ExportJSONLResult:
    language = normalize_language_spec(definition_language)
    prompt_bundle = build_prompt_bundle(
        prompt_version=prompt_version,
        definition_language=language,
    )

    with get_connection(settings) as conn:
        run_id = start_run(
            conn,
            stage=EXPORT_DISTRIBUTION_JSONL_STAGE,
            config={
                "output_path": str(output_path),
                "curated_table": curated_table,
                "definitions_table": llm_table,
                "artifact_table": artifact_table,
                "model": model,
                "prompt_template_version": prompt_bundle.template_version,
                "prompt_version": prompt_bundle.resolved_prompt_version,
                "schema_version": DISTRIBUTION_SCHEMA_VERSION,
                "artifact_role": "distribution",
                "definition_language": language.as_dict(),
            },
            parent_run_id=parent_run_id,
        )

    try:
        emit_progress(
            progress_callback,
            stage=EXPORT_DISTRIBUTION_JSONL_STAGE,
            event="export_start",
            model=model,
            prompt_version=prompt_bundle.resolved_prompt_version,
            prompt_template_version=prompt_bundle.template_version,
            definition_language_code=language.code,
        )
        records = list(
            iter_distribution_records(
                settings=settings,
                curated_table=curated_table,
                llm_table=llm_table,
                model=model,
                prompt_bundle=prompt_bundle,
                progress_callback=progress_callback,
            )
        )
        documents = [record["document"] for record in records if record["document"] is not None]
        skipped_entries_without_meanings = sum(1 for record in records if record["document"] is None)
        output_sha256 = write_jsonl_atomic(output_path, documents)
        emit_progress(
            progress_callback,
            stage=EXPORT_DISTRIBUTION_JSONL_STAGE,
            event="export_complete",
            entry_count=len(documents),
            skipped_entries_without_meanings=skipped_entries_without_meanings,
            output_path=str(output_path),
            output_sha256=output_sha256,
        )
        curated_run_ids = sorted({record["curated_run_id"] for record in records if record["curated_run_id"]})
        llm_run_ids = sorted({record["llm_run_id"] for record in records if record["llm_run_id"]})

        with get_connection(settings) as conn:
            update_run_config(
                conn,
                run_id=run_id,
                config_updates={
                    "curated_run_ids": curated_run_ids,
                    "definition_run_ids": llm_run_ids,
                },
            )
            record_export_artifact(
                conn,
                artifact_table=artifact_table,
                run_id=run_id,
                artifact_type="distribution_jsonl",
                output_path=output_path,
                output_sha256=output_sha256,
                entry_count=len(documents),
                metadata={
                    "curated_table": curated_table,
                    "definitions_table": llm_table,
                    "model": model,
                    "prompt_template_version": prompt_bundle.template_version,
                    "prompt_version": prompt_bundle.resolved_prompt_version,
                    "schema_version": DISTRIBUTION_SCHEMA_VERSION,
                    "artifact_role": "distribution",
                    "definition_language": language.as_dict(),
                    "curated_run_ids": curated_run_ids,
                    "definition_run_ids": llm_run_ids,
                    "skipped_entries_without_meanings": skipped_entries_without_meanings,
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
                    "definition_run_ids": llm_run_ids,
                    "schema_version": DISTRIBUTION_SCHEMA_VERSION,
                    "definition_language": language.as_dict(),
                    "skipped_entries_without_meanings": skipped_entries_without_meanings,
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


def iter_distribution_records(
    *,
    settings: RuntimeSettings,
    curated_table: str,
    llm_table: str,
    model: str | None,
    prompt_bundle,
    progress_callback: ProgressCallback | None = None,
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
            definition_language_code,
            definition_language_name,
            response_payload
        FROM {}
        WHERE status = 'succeeded'
          AND prompt_version = %s
          AND definition_language_code = %s
        """
    ).format(llm_identifier)

    params: list[Any] = [
        prompt_bundle.resolved_prompt_version,
        prompt_bundle.definition_language.code,
    ]
    if model is not None:
        latest_enrichment_sql += sql.SQL(" AND model = %s")
        params.append(model)
    latest_enrichment_sql += sql.SQL(" ORDER BY entry_id, created_at DESC")

    query = sql.SQL(
        """
        WITH latest_enrichment AS (
            {latest_enrichment_sql}
        )
        SELECT
            e.run_id,
            l.run_id,
            e.entry_id,
            e.lang_code,
            e.normalized_word,
            e.word,
            e.payload,
            l.model,
            l.prompt_version,
            l.definition_language_code,
            l.definition_language_name,
            l.response_payload
        FROM {curated_table} AS e
        JOIN latest_enrichment AS l
          ON l.entry_id = e.entry_id
        ORDER BY e.lang_code, e.normalized_word
        """
    ).format(
        latest_enrichment_sql=latest_enrichment_sql,
        curated_table=curated_identifier,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            reporter = ThrottledProgressReporter(progress_callback, stage=EXPORT_DISTRIBUTION_JSONL_STAGE)
            processed = 0
            exported = 0
            cursor.execute(query, params)
            for curated_run_id, llm_run_id, entry_id, _lang_code, _normalized_word, _word, curated_payload, llm_model, llm_prompt_version, llm_definition_language_code, llm_definition_language_name, response_payload in cursor.fetchall():
                if (
                    llm_definition_language_code != prompt_bundle.definition_language.code
                    or (llm_definition_language_name or "").strip() != prompt_bundle.definition_language.name
                ):
                    raise ValueError(
                        "Selected enrichment does not match the requested definition language contract: "
                        f"expected {prompt_bundle.definition_language.as_dict()}, "
                        f"got {{'code': {llm_definition_language_code!r}, 'name': {llm_definition_language_name!r}}}"
                    )
                document = build_distribution_document(
                    curated_payload=curated_payload,
                    llm_payload=response_payload,
                    definition_language=prompt_bundle.definition_language,
                )
                if document is not None:
                    validate_distribution_document(document)
                    exported += 1
                processed += 1
                reporter.report(
                    event="export_progress",
                    processed_entries=processed,
                    exported_entries=exported,
                )
                yield {
                    "curated_run_id": str(curated_run_id) if curated_run_id is not None else None,
                    "llm_run_id": str(llm_run_id) if llm_run_id is not None else None,
                    "document": document,
                }
            reporter.report(
                event="export_progress",
                force=True,
                processed_entries=processed,
                exported_entries=exported,
            )


def build_distribution_document(
    *,
    curated_payload: dict[str, Any],
    llm_payload: dict[str, Any],
    definition_language: LanguageSpec | dict[str, Any],
) -> dict[str, Any] | None:
    language = normalize_language_spec(definition_language)
    if not isinstance(curated_payload, dict):
        raise ValueError("Curated payload must be a JSON object")
    if not isinstance(llm_payload, dict):
        raise ValueError("LLM payload must be a JSON object")

    llm_group_lookup = {
        str(group["pos_group_id"]): group
        for group in llm_payload.get("pos_groups", [])
        if isinstance(group, dict) and group.get("pos_group_id")
    }

    distribution_pos_groups = []
    for curated_group in curated_payload.get("pos_groups", []):
        pos_group_id = build_pos_group_id(
            pos=curated_group.get("pos"),
            etymology_id=curated_group.get("etymology_id"),
        )
        llm_group = llm_group_lookup.get(pos_group_id)
        if llm_group is None:
            raise ValueError(f"LLM payload is missing generated fields for pos_group_id {pos_group_id}")
        distribution_pos_group = build_distribution_pos_group(
            curated_group=curated_group,
            llm_group=llm_group,
            pos_group_id=pos_group_id,
        )
        if distribution_pos_group is not None:
            distribution_pos_groups.append(distribution_pos_group)

    if not distribution_pos_groups:
        return None

    return {
        "schema_version": DISTRIBUTION_SCHEMA_VERSION,
        "entry_id": curated_payload["entry_id"],
        "headword": curated_payload["word"],
        "normalized_headword": curated_payload["normalized_word"],
        "headword_language": {
            "code": curated_payload["lang_code"],
            "name": curated_payload["lang"],
        },
        "definition_language": language.as_dict(),
        "entry_type": derive_entry_type(curated_payload.get("entry_flags") or []),
        "headword_summary": llm_payload["headword_summary"],
        "study_notes": llm_payload["study_notes"],
        "etymology_note": llm_payload["etymology_note"],
        "etymologies": [
            {
                "etymology_id": group.get("etymology_id"),
                "text": group.get("etymology_text"),
                "pos_members": group.get("member_pos") or [],
            }
            for group in curated_payload.get("etymology_groups", [])
        ],
        "pos_groups": distribution_pos_groups,
    }


def build_distribution_pos_group(
    *,
    curated_group: dict[str, Any],
    llm_group: dict[str, Any],
    pos_group_id: str,
) -> dict[str, Any] | None:
    llm_meaning_lookup = {
        str(meaning["sense_id"]): meaning
        for meaning in llm_group.get("meanings", [])
        if isinstance(meaning, dict) and meaning.get("sense_id")
    }

    meanings = []
    for curated_meaning in curated_group.get("senses", []):
        sense_id = str(curated_meaning.get("sense_id"))
        llm_meaning = llm_meaning_lookup.get(sense_id)
        if llm_meaning is None:
            raise ValueError(
                f"LLM payload is missing generated meaning fields for pos_group_id {pos_group_id} sense_id {sense_id}"
            )
        meanings.append(
            {
                "meaning_id": sense_id,
                "short_gloss": llm_meaning.get("short_gloss"),
                "learner_explanation": llm_meaning.get("learner_explanation"),
                "usage_note": llm_meaning.get("usage_note"),
                "labels": curated_meaning.get("tags") or [],
                "topics": curated_meaning.get("topics") or [],
                "examples": [
                    {
                        "source_text": example.get("text"),
                        "translation": example.get("translation"),
                        "note": None,
                        "ref": example.get("ref"),
                        "type": example.get("type"),
                    }
                    for example in curated_meaning.get("examples", [])
                ],
                "relations": [
                    {
                        "type": relation.get("relation_type"),
                        "word": relation.get("target_word"),
                        "lang_code": relation.get("target_lang_code"),
                    }
                    for relation in curated_meaning.get("relations", [])
                    if relation.get("relation_type") in {"form_of", "alternative_of", "compound_of"}
                ],
            }
        )

    if not meanings:
        return None

    return {
        "pos_group_id": pos_group_id,
        "pos": curated_group.get("pos"),
        "etymology_id": curated_group.get("etymology_id"),
        "summary": llm_group.get("summary"),
        "usage_notes": llm_group.get("usage_notes"),
        "forms": [
            {
                "text": form.get("form"),
                "tags": form.get("tags") or [],
                "roman": form.get("roman"),
            }
            for form in curated_group.get("forms", [])
        ],
        "pronunciations": [
            {
                "ipa": pronunciation.get("ipa"),
                "text": pronunciation.get("pronunciation_text"),
                "audio_url": pronunciation.get("audio_url"),
                "tags": pronunciation.get("tags") or [],
            }
            for pronunciation in curated_group.get("pronunciations", [])
        ],
        "meanings": meanings,
        "relations": [
            {
                "type": relation.get("relation_type"),
                "word": relation.get("target_word"),
                "lang_code": relation.get("target_lang_code"),
            }
            for relation in curated_group.get("relations", [])
            if relation.get("relation_type") in {"derived_term", "related_term", "synonym", "antonym", "descendant"}
        ],
    }


def derive_entry_type(entry_flags: list[str]) -> str:
    flag_set = set(entry_flags)
    if "entry_type:proverb" in flag_set:
        return "proverb"
    if "entry_type:affix" in flag_set:
        return "affix"
    return "standard"
