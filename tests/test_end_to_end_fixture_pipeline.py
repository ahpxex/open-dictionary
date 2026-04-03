from __future__ import annotations

import json
from pathlib import Path

from open_dictionary.config.settings import RuntimeSettings
from open_dictionary.db.bootstrap import apply_foundation
from open_dictionary.db.connection import get_connection
from open_dictionary.stages.curated_build.stage import run_curated_build_stage
from open_dictionary.stages.export_jsonl.stage import run_export_jsonl_stage
from open_dictionary.stages.llm_enrich.stage import run_llm_enrich_stage
from open_dictionary.stages.raw_ingest.stage import run_raw_ingest_stage


class FixtureAwareFakeLLMClient:
    def __init__(self) -> None:
        self.calls = 0

    def generate_json(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> str:
        self.calls += 1
        marker = "Generated-field source payload:\n"
        payload = json.loads(user_prompt.split(marker, 1)[1])
        pos_groups = []
        for group in payload.get("pos_groups", []):
            pos_groups.append(
                {
                    "pos": group["pos"],
                    "summary": f"{group['pos']} 的整体说明。",
                    "usage_notes": None,
                    "meanings": [
                        {
                            "sense_id": meaning["sense_id"],
                            "short_gloss": f"{meaning['sense_id']} short gloss",
                            "learner_explanation": f"{meaning['sense_id']} 的详细中文解释。",
                            "usage_note": None,
                        }
                        for meaning in group.get("meanings", [])
                    ],
                }
            )
        response = {
            "headword_summary": f"{payload['headword']} 的整体中文说明。",
            "etymology_note": None,
            "study_notes": [f"学习 {payload['headword']} 时要注意语境。"],
            "pos_groups": pos_groups,
        }
        return json.dumps(response, ensure_ascii=False)


def test_fixture_pipeline_runs_end_to_end_with_fake_llm(tmp_path: Path, temp_database_url: str) -> None:
    # This case drives the full rewritten pipeline over the repository fixture:
    # raw ingest -> curated build -> llm enrich -> audit jsonl export.
    env_file = tmp_path / ".env"
    env_file.write_text(
        "LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n",
        encoding="utf-8",
    )
    settings = RuntimeSettings(database_url=temp_database_url)
    fixture_path = Path("fixtures/wiktionary/raw.jsonl")
    output_path = tmp_path / "audit.jsonl"
    fake_client = FixtureAwareFakeLLMClient()

    with get_connection(settings) as conn:
        apply_foundation(conn)

    raw_result = run_raw_ingest_stage(
        settings=settings,
        workdir=tmp_path / "raw-workdir",
        archive_path=fixture_path,
    )
    curated_result = run_curated_build_stage(settings=settings)
    llm_result = run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=fake_client,
        max_workers=4,
    )
    export_result = run_export_jsonl_stage(
        settings=settings,
        output_path=output_path,
        include_unenriched=False,
    )

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert raw_result.rows_loaded == 1000
    assert curated_result.entries_written > 0
    assert llm_result.succeeded == curated_result.entries_written
    assert export_result.entry_count == curated_result.entries_written
    assert len(lines) == export_result.entry_count
    assert fake_client.calls == llm_result.processed


def test_fixture_pipeline_export_rows_contain_curated_and_llm_sections(
    tmp_path: Path,
    temp_database_url: str,
) -> None:
    # This case verifies the shape of the merged audit documents produced by the
    # rewritten end-to-end fixture pipeline.
    env_file = tmp_path / ".env"
    env_file.write_text(
        "LLM_API=http://localhost:3888/v1\nLLM_KEY=EMPTY\nLLM_MODEL=test-model\n",
        encoding="utf-8",
    )
    settings = RuntimeSettings(database_url=temp_database_url)
    fixture_path = Path("fixtures/wiktionary/raw.jsonl")
    output_path = tmp_path / "audit.jsonl"

    with get_connection(settings) as conn:
        apply_foundation(conn)

    run_raw_ingest_stage(
        settings=settings,
        workdir=tmp_path / "raw-workdir",
        archive_path=fixture_path,
    )
    run_curated_build_stage(settings=settings, limit_groups=25)
    run_llm_enrich_stage(
        settings=settings,
        env_file=str(env_file),
        client=FixtureAwareFakeLLMClient(),
        max_workers=2,
    )
    run_export_jsonl_stage(
        settings=settings,
        output_path=output_path,
        include_unenriched=False,
    )

    first_doc = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])

    assert "curated" in first_doc
    assert "llm" in first_doc
    assert "pos_groups" in first_doc["curated"]
    assert "headword_summary" in first_doc["llm"]["payload"]
