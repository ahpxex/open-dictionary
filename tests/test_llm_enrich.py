from __future__ import annotations

import json
from pathlib import Path
import urllib.request
from uuid import uuid4

import pytest

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.contracts import DEFAULT_DEFINITION_LANGUAGE
from open_dictionary.db.bootstrap import apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.llm.client import LLMClientError, OpenAICompatLLMClient
from open_dictionary.llm.config import LLMSettings
from open_dictionary.llm.config import load_llm_settings
from open_dictionary.llm.prompt import (
    PROMPT_VERSION,
    build_prompt_bundle,
    build_generation_source_payload,
    build_pos_group_id,
    build_user_prompt,
)
from open_dictionary.pipeline.runs import start_run
from open_dictionary.stages.llm_enrich import stage as llm_stage
from open_dictionary.stages.llm_enrich.schema import validate_enrichment_payload


class FakeLLMClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.max_tokens_seen: list[int | None] = []

    def generate_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        self.calls += 1
        self.max_tokens_seen.append(max_tokens)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


ENGLISH_DEFINITION_LANGUAGE = {
    "code": "en",
    "name": "English",
}


def valid_payload(
    pos: str = "noun",
    sense_ids: list[str] | None = None,
    etymology_id: str | None = None,
) -> dict:
    sense_ids = ["s1"] if sense_ids is None else sense_ids
    return {
        "headword_summary": "一个对中文学习者友好的整体说明。",
        "study_notes": ["Note one", "Note two"],
        "etymology_note": "一个简短的词源说明。",
        "pos_groups": [
            {
                "pos_group_id": build_pos_group_id(pos=pos, etymology_id=etymology_id),
                "pos": pos,
                "summary": "这个词性的整体说明。",
                "usage_notes": "Usage note.",
                "meanings": [
                    {
                        "sense_id": sense_id,
                        "short_gloss": f"{sense_id} short gloss",
                        "learner_explanation": f"{sense_id} 的详细自然语言解释。",
                        "usage_note": f"{sense_id} usage note.",
                    }
                    for sense_id in sense_ids
                ],
            }
        ],
    }


def seed_curated_entry(conn, *, word: str = "cat", lang_code: str = "en", pos: str = "noun") -> str:
    entry_id = str(uuid4())
    run_id = start_run(conn, stage="entries.assemble")
    payload = {
        "entry_id": entry_id,
        "word": word,
        "normalized_word": word,
        "lang": "English",
        "lang_code": lang_code,
        "entry_flags": [],
        "source_summary": {
            "raw_record_count": 1,
            "raw_snapshot_ids": ["snapshot-1"],
            "raw_run_ids": ["run-1"],
            "raw_record_refs": [{"snapshot_id": "snapshot-1", "run_id": "run-1", "raw_record_id": 1, "source_line": 1, "pos": pos}],
        },
        "etymology_groups": [],
        "pos_groups": [
            {
                "pos": pos,
                "pos_flags": [],
                "etymology_id": None,
                "senses": [
                    {
                        "sense_id": "s1",
                        "gloss": f"{word} gloss",
                        "raw_gloss": None,
                        "tags": [],
                        "qualifier": None,
                        "topics": [],
                        "examples": [],
                        "relations": [],
                        "sense_flags": [],
                    }
                ],
                "forms": [],
                "pronunciations": [],
                "relations": [],
            }
        ],
    }
    with conn.cursor() as cursor:
        cursor.execute(
            """
            insert into curated.entries (
                run_id, entry_id, lang_code, normalized_word, word, payload, entry_flags, source_summary
            ) values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                entry_id,
                lang_code,
                word,
                word,
                json.dumps(payload),
                [],
                json.dumps(payload["source_summary"]),
            ),
        )
    return entry_id


def test_load_llm_settings_reads_values_from_env_file(tmp_path: Path) -> None:
    # This case verifies that the LLM stage can bootstrap itself from the repository .env.
    env_file = tmp_path / ".env"
    env_file.write_text(
        "LLM_API=http://127.0.0.1:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n",
        encoding="utf-8",
    )

    settings = load_llm_settings(env_file=env_file)

    assert settings.api_base == "http://127.0.0.1:3888/v1"
    assert settings.api_key == "EMPTY"
    assert settings.model == "test-model"


def test_load_llm_settings_raises_when_api_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # This case protects the stage from starting with half-configured model credentials.
    monkeypatch.delenv("LLM_API", raising=False)
    monkeypatch.delenv("LLM_KEY", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="LLM_API"):
        load_llm_settings(env_file=env_file)


def test_build_user_prompt_embeds_curated_payload() -> None:
    # This case verifies that prompt rendering keeps the full curated structure available to the model.
    prompt = build_user_prompt(
        build_generation_source_payload(
            {"word": "cat", "lang": "English", "lang_code": "en", "pos_groups": []},
            definition_language=ENGLISH_DEFINITION_LANGUAGE,
        )
    )

    assert "Generated-field source payload" in prompt
    assert '"headword": "cat"' in prompt
    assert '"definition_language": {' in prompt
    assert '"code": "en"' in prompt


def test_build_prompt_bundle_resolves_language_specific_prompt_version() -> None:
    bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=ENGLISH_DEFINITION_LANGUAGE,
    )

    assert bundle.template_version == PROMPT_VERSION
    assert bundle.resolved_prompt_version.endswith("__deflang__en")
    assert "required definition language for this run is English (en)" in bundle.system_prompt


def test_validate_enrichment_payload_accepts_valid_shape() -> None:
    # This case locks in the baseline enrichment contract.
    payload = valid_payload("noun")

    validated = validate_enrichment_payload(
        payload,
        expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
    )

    assert validated["headword_summary"] == payload["headword_summary"]


def test_validate_enrichment_payload_rejects_missing_keys() -> None:
    # This case prevents silently accepting incomplete LLM output.
    with pytest.raises(ValueError, match="missing required keys"):
        validate_enrichment_payload(
            {"headword_summary": "x"},
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_rejects_empty_headword_summary() -> None:
    # This case forces the model output to contain meaningful top-level text.
    payload = valid_payload()
    payload["headword_summary"] = "   "

    with pytest.raises(ValueError, match="headword_summary"):
        validate_enrichment_payload(
            payload,
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_rejects_invalid_study_notes() -> None:
    # This case guards against malformed list fields in the generated JSON.
    payload = valid_payload()
    payload["study_notes"] = ["good", 123]

    with pytest.raises(ValueError, match="study_notes"):
        validate_enrichment_payload(
            payload,
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_rejects_unexpected_pos() -> None:
    # This case prevents the model from inventing part-of-speech groups that do not exist in curated data.
    payload = valid_payload("verb")

    with pytest.raises(ValueError, match="unexpected pos_group_id"):
        validate_enrichment_payload(
            payload,
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_rejects_unexpected_sense_id() -> None:
    # This case prevents the model from inventing meanings outside the curated entry skeleton.
    payload = valid_payload("noun", sense_ids=["s2"])

    with pytest.raises(ValueError, match="unexpected sense_id"):
        validate_enrichment_payload(
            payload,
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_rejects_missing_sense_id() -> None:
    # This case ensures every curated sense receives a generated explanation.
    payload = valid_payload("noun", sense_ids=[])

    with pytest.raises(ValueError, match="missing sense_ids"):
        validate_enrichment_payload(
            payload,
            expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
        )


def test_validate_enrichment_payload_coerces_string_study_notes() -> None:
    # This case captures a common model drift where a one-item list becomes a bare string.
    payload = valid_payload()
    payload["study_notes"] = "Single note"

    validated = validate_enrichment_payload(
        payload,
        expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
    )

    assert validated["study_notes"] == ["Single note"]


def test_validate_enrichment_payload_coerces_null_study_notes_to_empty_list() -> None:
    # This case handles compact fallback generations that choose null instead of [] for optional notes.
    payload = valid_payload()
    payload["study_notes"] = None

    validated = validate_enrichment_payload(
        payload,
        expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
    )

    assert validated["study_notes"] == []


def test_validate_enrichment_payload_coerces_usage_note_lists() -> None:
    # This case captures the exact schema drift seen during a real model smoke run.
    payload = valid_payload()
    payload["pos_groups"][0]["usage_notes"] = ["First note.", "Second note."]
    payload["pos_groups"][0]["meanings"][0]["usage_note"] = ["Meaning note one.", "Meaning note two."]

    validated = validate_enrichment_payload(
        payload,
        expected_pos_targets=[{"pos_group_id": build_pos_group_id(pos="noun", etymology_id=None), "pos": "noun", "sense_ids": ["s1"]}],
    )

    assert validated["pos_groups"][0]["usage_notes"] == "First note. Second note."
    assert validated["pos_groups"][0]["meanings"][0]["usage_note"] == "Meaning note one. Meaning note two."


def test_compute_input_hash_is_stable() -> None:
    # This case ensures repeated enrichment over the same request payload gets the same hash.
    payload = {"entry": {"word": "cat"}, "prompt_version": "v1"}

    first = llm_stage.compute_input_hash(payload)
    second = llm_stage.compute_input_hash({"prompt_version": "v1", "entry": {"word": "cat"}})

    assert first == second


def test_enrich_one_entry_succeeds_with_fake_client() -> None:
    # This case covers the happy-path single-entry enrichment flow.
    client = FakeLLMClient([json.dumps(valid_payload("noun"))])
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    request_entry = build_generation_source_payload(
        {
            "entry_id": "entry-1",
            "word": "cat",
            "normalized_word": "cat",
            "lang": "English",
            "lang_code": "en",
            "entry_flags": [],
            "etymology_groups": [],
            "pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}],
        }
    )
    entry = {
        "entry_id": "entry-1",
        "payload": {"pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}]},
        "request_payload": {"entry": request_entry},
        "input_hash": "hash",
    }

    record = llm_stage.enrich_one_entry(
        entry=entry,
        llm_client=client,
        prompt_bundle=prompt_bundle,
        model="test-model",
        max_retries=2,
    )

    assert record["entry_id"] == "entry-1"
    assert record["retries"] == 0
    assert client.calls == 1


def test_enrich_one_entry_retries_before_success() -> None:
    # This case verifies that transient model failures are retried instead of immediately failing the stage.
    client = FakeLLMClient([ValueError("bad"), json.dumps(valid_payload("noun"))])
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    request_entry = build_generation_source_payload(
        {
            "entry_id": "entry-1",
            "word": "cat",
            "normalized_word": "cat",
            "lang": "English",
            "lang_code": "en",
            "entry_flags": [],
            "etymology_groups": [],
            "pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}],
        }
    )
    entry = {
        "entry_id": "entry-1",
        "payload": {"pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}]},
        "request_payload": {"entry": request_entry},
        "input_hash": "hash",
    }

    record = llm_stage.enrich_one_entry(
        entry=entry,
        llm_client=client,
        prompt_bundle=prompt_bundle,
        model="test-model",
        max_retries=2,
    )

    assert record["retries"] == 1
    assert client.calls == 2
    assert client.max_tokens_seen == [llm_stage.DEFAULT_MAX_TOKENS, llm_stage.COMPACT_RETRY_MAX_TOKENS]


def test_enrich_one_entry_raises_after_max_retries() -> None:
    # This case ensures persistent invalid outputs bubble up as failures.
    client = FakeLLMClient([ValueError("bad"), ValueError("still bad")])
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    request_entry = build_generation_source_payload(
        {
            "entry_id": "entry-1",
            "word": "cat",
            "normalized_word": "cat",
            "lang": "English",
            "lang_code": "en",
            "entry_flags": [],
            "etymology_groups": [],
            "pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}],
        }
    )
    entry = {
        "entry_id": "entry-1",
        "payload": {"pos_groups": [{"pos": "noun", "etymology_id": None, "senses": [{"sense_id": "s1"}]}]},
        "request_payload": {"entry": request_entry},
        "input_hash": "hash",
    }

    with pytest.raises(ValueError, match="still bad"):
        llm_stage.enrich_one_entry(
            entry=entry,
            llm_client=client,
            prompt_bundle=prompt_bundle,
            model="test-model",
            max_retries=2,
        )


def test_openai_client_wraps_timeout_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # This case ensures transport-level timeouts enter the stage retry path instead of escaping as raw exceptions.
    client = OpenAICompatLLMClient(
        LLMSettings(
            api_base="http://127.0.0.1:3888/v1",
            api_key="EMPTY",
            model="test-model",
        )
    )

    def raise_timeout(*args, **kwargs):
        raise TimeoutError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", raise_timeout)

    with pytest.raises(LLMClientError, match="timed out"):
        client.generate_json(system_prompt="system", user_prompt="user", max_tokens=100)


def test_ensure_prompt_version_is_idempotent(temp_database_url: str) -> None:
    # This case prevents duplicate prompt metadata rows for the same version string.
    settings = RuntimeSettings(database_url=temp_database_url)
    prompt_bundle = build_prompt_bundle(
        prompt_version="v-test",
        definition_language=ENGLISH_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        apply_foundation(conn)
        llm_stage.ensure_prompt_version(conn, prompt_bundle=prompt_bundle)
        llm_stage.ensure_prompt_version(conn, prompt_bundle=prompt_bundle)
        with conn.cursor() as cursor:
            cursor.execute(
                "select count(*) from llm.prompt_versions where prompt_version = %s",
                (prompt_bundle.resolved_prompt_version,),
            )
            count = cursor.fetchone()[0]

    assert count == 1


def test_iter_curated_entries_skips_existing_successes(temp_database_url: str) -> None:
    # This case verifies that reruns do not keep sending already-enriched entries back to the model.
    settings = RuntimeSettings(database_url=temp_database_url)
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        apply_foundation(conn)
        entry_id = seed_curated_entry(conn)
        llm_stage.ensure_prompt_version(conn, prompt_bundle=prompt_bundle)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                insert into meta.pipeline_runs (run_id, stage, status, config)
                values (gen_random_uuid(), 'definitions.generate', 'succeeded', '{}'::jsonb)
                returning run_id
                """
            )
            run_id = cursor.fetchone()[0]
        llm_stage.persist_enrichment_success(
            conn,
            target_table="llm.entry_enrichments",
            run_id=run_id,
            record={
                "entry_id": entry_id,
                "model": "test-model",
                "prompt_version": prompt_bundle.resolved_prompt_version,
                "definition_language": DEFAULT_DEFINITION_LANGUAGE,
                "input_hash": "hash",
                "request_payload": {"entry": "payload"},
                "response_payload": valid_payload(),
                "raw_response": json.dumps(valid_payload()),
                "retries": 0,
            },
        )

    items = list(
        llm_stage.iter_curated_entries(
            settings,
            source_table="curated.entries",
            target_table="llm.entry_enrichments",
            prompt_bundle=prompt_bundle,
            model="test-model",
            recompute_existing=False,
            limit_entries=None,
        )
    )

    assert items == []


def test_iter_curated_entries_recompute_existing_returns_entries(temp_database_url: str) -> None:
    # This case covers the explicit rebuild path for LLM output regeneration.
    settings = RuntimeSettings(database_url=temp_database_url)
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn)

    items = list(
        llm_stage.iter_curated_entries(
            settings,
            source_table="curated.entries",
            target_table="llm.entry_enrichments",
            prompt_bundle=prompt_bundle,
            model="test-model",
            recompute_existing=True,
            limit_entries=None,
        )
    )

    assert len(items) == 1


def test_persist_enrichment_success_stores_success_row(temp_database_url: str) -> None:
    # This case ensures successful generations are durably persisted for export.
    settings = RuntimeSettings(database_url=temp_database_url)
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        apply_foundation(conn)
        entry_id = seed_curated_entry(conn)
        llm_stage.ensure_prompt_version(conn, prompt_bundle=prompt_bundle)
        run_id = start_run(conn, stage="definitions.generate")
        llm_stage.persist_enrichment_success(
            conn,
            target_table="llm.entry_enrichments",
            run_id=run_id,
            record={
                "entry_id": entry_id,
                "model": "test-model",
                "prompt_version": prompt_bundle.resolved_prompt_version,
                "definition_language": DEFAULT_DEFINITION_LANGUAGE,
                "input_hash": "hash",
                "request_payload": {"entry": "payload"},
                "response_payload": valid_payload(),
                "raw_response": json.dumps(valid_payload()),
                "retries": 0,
            },
        )
        with conn.cursor() as cursor:
            cursor.execute("select status, model from llm.entry_enrichments")
            status, model = cursor.fetchone()

    assert status == "succeeded"
    assert model == "test-model"


def test_persist_enrichment_failure_stores_error_row(temp_database_url: str) -> None:
    # This case ensures failed generations are visible for debugging and retries.
    settings = RuntimeSettings(database_url=temp_database_url)
    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=DEFAULT_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        apply_foundation(conn)
        entry_id = seed_curated_entry(conn)
        llm_stage.ensure_prompt_version(conn, prompt_bundle=prompt_bundle)
        run_id = start_run(conn, stage="definitions.generate")
        llm_stage.persist_enrichment_failure(
            conn,
            target_table="llm.entry_enrichments",
            run_id=run_id,
            entry_id=entry_id,
            model="test-model",
            prompt_version=prompt_bundle.resolved_prompt_version,
            definition_language=DEFAULT_DEFINITION_LANGUAGE,
            input_hash="hash",
            request_payload={"entry": "payload"},
            retries=3,
            error="boom",
        )
        with conn.cursor() as cursor:
            cursor.execute("select status, error from llm.entry_enrichments")
            status, error = cursor.fetchone()

    assert status == "failed"
    assert error == "boom"


def test_run_llm_enrich_stage_processes_curated_entries_with_fake_client(tmp_path: Path, temp_database_url: str) -> None:
    # This case drives the entire stage end to end without a live model service.
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)
    client = FakeLLMClient([json.dumps(valid_payload())])

    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn)

    result = llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=client,
        max_workers=1,
    )

    assert result.processed == 1
    assert result.succeeded == 1
    assert result.failed == 0


def test_run_llm_enrich_stage_supports_non_default_definition_language(
    tmp_path: Path,
    temp_database_url: str,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)
    client = FakeLLMClient(
        [
            json.dumps(
                {
                    "headword_summary": "Overall learner-facing summary.",
                    "study_notes": ["Keep register in mind."],
                    "etymology_note": "Short etymology note.",
                    "pos_groups": [
                        {
                            "pos_group_id": build_pos_group_id(pos="noun", etymology_id=None),
                            "pos": "noun",
                            "summary": "Noun summary.",
                            "usage_notes": None,
                            "meanings": [
                                {
                                    "sense_id": "s1",
                                    "short_gloss": "cat",
                                    "learner_explanation": "A domestic feline animal.",
                                    "usage_note": None,
                                }
                            ],
                        }
                    ],
                }
            )
        ]
    )

    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn)

    result = llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=client,
        definition_language=ENGLISH_DEFINITION_LANGUAGE,
        max_workers=1,
    )

    prompt_bundle = build_prompt_bundle(
        prompt_version=PROMPT_VERSION,
        definition_language=ENGLISH_DEFINITION_LANGUAGE,
    )
    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                select definition_language_code, definition_language_name, prompt_version
                from llm.entry_enrichments
                """
            )
            stored_language_code, stored_language_name, stored_prompt_version = cursor.fetchone()

    assert result.succeeded == 1
    assert stored_language_code == "en"
    assert stored_language_name == "English"
    assert stored_prompt_version == prompt_bundle.resolved_prompt_version


def test_run_llm_enrich_stage_records_failed_entries(tmp_path: Path, temp_database_url: str) -> None:
    # This case verifies the stage keeps going and stores failures when the model output never validates.
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)
    client = FakeLLMClient([ValueError("bad output")])

    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn)

    result = llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=client,
        max_workers=1,
        max_retries=1,
    )

    with get_connection(settings) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) from llm.entry_enrichments where status = 'failed'")
            failed_count = cursor.fetchone()[0]

    assert result.failed == 1
    assert failed_count == 1


def test_run_llm_enrich_stage_respects_limit_entries(tmp_path: Path, temp_database_url: str) -> None:
    # This case supports debugger-friendly partial LLM runs over larger curated tables.
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)
    client = FakeLLMClient([json.dumps(valid_payload()), json.dumps(valid_payload())])

    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn, word="cat")
        seed_curated_entry(conn, word="dog")

    result = llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=client,
        max_workers=1,
        limit_entries=1,
    )

    assert result.processed == 1
    assert client.calls == 1


def test_run_llm_enrich_stage_recompute_existing_enriches_again(tmp_path: Path, temp_database_url: str) -> None:
    # This case verifies the explicit rebuild mode for enrichment rows.
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n", encoding="utf-8")
    settings = RuntimeSettings(database_url=temp_database_url)

    with get_connection(settings) as conn:
        apply_foundation(conn)
        seed_curated_entry(conn, word="cat")

    first_client = FakeLLMClient([json.dumps(valid_payload())])
    second_client = FakeLLMClient([json.dumps(valid_payload())])

    llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=first_client,
        max_workers=1,
    )
    result = llm_stage.run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=second_client,
        max_workers=1,
        recompute_existing=True,
    )

    assert result.processed == 1
    assert second_client.calls == 1
