from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from open_dictionary import cli
from open_dictionary.config.settings import RuntimeSettings


@pytest.mark.parametrize(
    ("argv", "patches", "expected_command", "expected_checks"),
    [
        (
            ["db-init"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "get_connection": None,
                "apply_foundation": lambda conn: ["20260403_curated_lineage_v2"],
            },
            "db-init",
            {"applied_versions": ["20260403_curated_lineage_v2"]},
        ),
        (
            ["download", "--output", "data/raw/sample.jsonl.gz"],
            {
                "download_wiktionary_dump": lambda output, **kwargs: Path(output),
            },
            "download",
            {"output_path": "data/raw/sample.jsonl.gz"},
        ),
        (
            ["extract", "--input", "data/raw/sample.jsonl.gz", "--output", "data/raw/sample.jsonl"],
            {
                "extract_wiktionary_dump": lambda input_path, output, **kwargs: Path(output),
            },
            "extract",
            {"output_path": "data/raw/sample.jsonl"},
        ),
        (
            ["raw-ingest", "--archive-path", "fixtures/wiktionary/raw.jsonl"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "run_raw_ingest_stage": lambda **kwargs: SimpleNamespace(
                    run_id=uuid4(),
                    snapshot_id=uuid4(),
                    archive_path=Path("fixtures/wiktionary/raw.jsonl"),
                    rows_loaded=1000,
                    anomalies_logged=0,
                    archive_sha256="sha",
                    snapshot_preexisting=False,
                ),
            },
            "raw-ingest",
            {"rows_loaded": 1000},
        ),
        (
            ["curated-build"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "run_curated_build_stage": lambda **kwargs: SimpleNamespace(
                    run_id=uuid4(),
                    groups_processed=742,
                    entries_written=742,
                    relations_written=10,
                    triage_written=1,
                ),
            },
            "curated-build",
            {"entries_written": 742},
        ),
        (
            ["llm-enrich"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "run_llm_enrich_stage": lambda **kwargs: SimpleNamespace(
                    run_id=uuid4(),
                    processed=742,
                    succeeded=742,
                    failed=0,
                ),
            },
            "llm-enrich",
            {"succeeded": 742},
        ),
        (
            ["export-audit-jsonl", "--output", "data/export/audit.jsonl"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "run_export_audit_jsonl_stage": lambda **kwargs: SimpleNamespace(
                    run_id=uuid4(),
                    entry_count=742,
                    output_path=Path("data/export/audit.jsonl"),
                    output_sha256="audit-sha",
                ),
            },
            "export-audit-jsonl",
            {"entry_count": 742},
        ),
        (
            ["export-distribution-jsonl", "--output", "data/export/distribution.jsonl"],
            {
                "load_settings": lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
                "run_export_distribution_jsonl_stage": lambda **kwargs: SimpleNamespace(
                    run_id=uuid4(),
                    entry_count=741,
                    output_path=Path("data/export/distribution.jsonl"),
                    output_sha256="dist-sha",
                ),
            },
            "export-distribution-jsonl",
            {"entry_count": 741},
        ),
    ],
)
def test_cli_commands_emit_consistent_json_results(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    patches: dict[str, object],
    expected_command: str,
    expected_checks: dict[str, object],
) -> None:
    class DummyConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    for name, value in patches.items():
        if value is None and name == "get_connection":
            monkeypatch.setattr(cli, "get_connection", lambda settings: DummyConnection())
        else:
            monkeypatch.setattr(cli, name, value)

    exit_code = cli.main(argv)

    assert exit_code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["command"] == expected_command
    assert summary["status"] == "succeeded"
    for key, value in expected_checks.items():
        assert summary[key] == value


def test_pipeline_run_executes_stages_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, object] = {}

    def fake_raw(**kwargs):
        calls["raw"] = kwargs
        kwargs["progress_callback"]({"stage": "wiktionary.raw_ingest", "event": "acquire_complete", "rows_loaded": 0})
        return SimpleNamespace(
            run_id=uuid4(),
            snapshot_id=uuid4(),
            rows_loaded=1000,
            anomalies_logged=0,
            snapshot_preexisting=False,
            archive_sha256="sha",
        )

    def fake_curated(**kwargs):
        calls["curated"] = kwargs
        kwargs["progress_callback"]({"stage": "curated.build", "event": "build_progress", "groups_processed": 10})
        return SimpleNamespace(
            run_id=uuid4(),
            groups_processed=742,
            entries_written=742,
            relations_written=10,
            triage_written=1,
        )

    def fake_llm(**kwargs):
        calls["llm"] = kwargs
        kwargs["progress_callback"]({"stage": "llm.enrich", "event": "enrich_progress", "processed": 10})
        return SimpleNamespace(
            run_id=uuid4(),
            processed=742,
            succeeded=742,
            failed=0,
        )

    def fake_distribution(**kwargs):
        calls["distribution"] = kwargs
        kwargs["progress_callback"]({"stage": "export.distribution_jsonl", "event": "export_progress", "processed_entries": 10})
        return SimpleNamespace(
            run_id=uuid4(),
            entry_count=741,
            output_path=Path("data/export/distribution.jsonl"),
            output_sha256="dist-sha",
        )

    def fake_audit(**kwargs):
        calls["audit"] = kwargs
        return SimpleNamespace(
            run_id=uuid4(),
            entry_count=742,
            output_path=Path("data/export/audit.jsonl"),
            output_sha256="audit-sha",
        )

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
    )
    monkeypatch.setattr(cli, "run_raw_ingest_stage", fake_raw)
    monkeypatch.setattr(cli, "run_curated_build_stage", fake_curated)
    monkeypatch.setattr(cli, "run_llm_enrich_stage", fake_llm)
    monkeypatch.setattr(cli, "run_export_distribution_jsonl_stage", fake_distribution)
    monkeypatch.setattr(cli, "run_export_audit_jsonl_stage", fake_audit)
    pending_counts = iter([742, 0])
    monkeypatch.setattr(cli, "_count_pending_llm_entries", lambda *args, **kwargs: next(pending_counts))
    monkeypatch.setattr(
        cli,
        "validate_distribution_jsonl_file",
        lambda path, **kwargs: SimpleNamespace(output_path=Path(path), entry_count=741),
    )

    exit_code = cli.main(
        [
            "pipeline-run",
            "--skip-db-init",
            "--archive-path",
            "fixtures/wiktionary/raw.jsonl",
            "--max-workers",
            "50",
            "--validate-distribution",
            "--distribution-output",
            "data/export/distribution.jsonl",
            "--audit-output",
            "data/export/audit.jsonl",
            "--llm-env-file",
            "/tmp/llm.env",
        ]
    )

    assert exit_code == 0
    assert calls["raw"]["archive_path"] == Path("fixtures/wiktionary/raw.jsonl")
    assert calls["llm"]["max_workers"] == 50
    assert calls["llm"]["env_file"] == "/tmp/llm.env"
    assert calls["distribution"]["output_path"] == Path("data/export/distribution.jsonl")
    assert calls["audit"]["output_path"] == Path("data/export/audit.jsonl")

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["command"] == "pipeline-run"
    assert summary["status"] == "succeeded"
    assert summary["llm"]["failed"] == 0
    assert summary["distribution_export"]["entry_count"] == 741
    assert summary["distribution_export"]["validated_entry_count"] == 741
    assert summary["audit_export"]["entry_count"] == 742
    assert "[progress] stage=wiktionary.raw_ingest event=acquire_complete" in captured.err
    assert "[progress] stage=llm.enrich event=enrich_progress" in captured.err


def test_pipeline_run_stops_when_llm_has_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
    )
    monkeypatch.setattr(
        cli,
        "run_raw_ingest_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            snapshot_id=uuid4(),
            rows_loaded=1000,
            anomalies_logged=0,
            snapshot_preexisting=False,
            archive_sha256="sha",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_curated_build_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            groups_processed=742,
            entries_written=742,
            relations_written=10,
            triage_written=1,
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_llm_enrich_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            processed=742,
            succeeded=733,
            failed=9,
        ),
    )
    pending_counts = iter([742, 9, 9, 9, 9])
    monkeypatch.setattr(cli, "_count_pending_llm_entries", lambda *args, **kwargs: next(pending_counts))
    monkeypatch.setattr(cli, "_fetch_recent_failed_enrichments", lambda *args, **kwargs: [("entry-1", "timed out")])

    with pytest.raises(SystemExit):
        cli.main(
            [
                "pipeline-run",
                "--skip-db-init",
                "--archive-path",
                "fixtures/wiktionary/raw.jsonl",
            ]
        )


def test_pipeline_run_retries_with_worker_tiers_until_pending_is_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[int] = []

    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda **kwargs: RuntimeSettings(database_url="postgresql://example/test"),
    )
    monkeypatch.setattr(
        cli,
        "run_raw_ingest_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            snapshot_id=uuid4(),
            rows_loaded=1000,
            anomalies_logged=0,
            snapshot_preexisting=False,
            archive_sha256="sha",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_curated_build_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            groups_processed=742,
            entries_written=742,
            relations_written=10,
            triage_written=1,
        ),
    )

    def fake_llm(**kwargs):
        calls.append(kwargs["max_workers"])
        if kwargs["max_workers"] == 50:
            return SimpleNamespace(run_id=uuid4(), processed=742, succeeded=731, failed=11)
        return SimpleNamespace(run_id=uuid4(), processed=11, succeeded=11, failed=0)

    monkeypatch.setattr(cli, "run_llm_enrich_stage", fake_llm)
    pending_counts = iter([742, 11, 0])
    monkeypatch.setattr(cli, "_count_pending_llm_entries", lambda *args, **kwargs: next(pending_counts))
    monkeypatch.setattr(
        cli,
        "run_export_distribution_jsonl_stage",
        lambda **kwargs: SimpleNamespace(
            run_id=uuid4(),
            entry_count=741,
            output_path=Path("data/export/distribution.jsonl"),
            output_sha256="dist-sha",
        ),
    )
    monkeypatch.setattr(
        cli,
        "validate_distribution_jsonl_file",
        lambda path, **kwargs: SimpleNamespace(output_path=Path(path), entry_count=741),
    )

    exit_code = cli.main(
        [
            "pipeline-run",
            "--skip-db-init",
            "--archive-path",
            "fixtures/wiktionary/raw.jsonl",
            "--worker-tiers",
            "50",
            "12",
            "--validate-distribution",
        ]
    )

    assert exit_code == 0
    assert calls == [50, 12]
    summary = json.loads(capsys.readouterr().out)
    assert summary["command"] == "pipeline-run"
    assert summary["status"] == "succeeded"
    assert summary["llm"]["attempts"][0]["workers"] == 50
    assert summary["llm"]["attempts"][1]["workers"] == 12
    assert summary["llm"]["attempts"][1]["remaining_entries"] == 0


def test_validate_distribution_jsonl_command_reads_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_path = tmp_path / "distribution.jsonl"
    output_path.write_text(
        json.dumps(
            {
                "schema_version": "distribution_entry_v1",
                "entry_id": "entry-1",
                "headword": "barra",
                "normalized_headword": "barra",
                "headword_language": {"code": "aa", "name": "Afar"},
                "definition_language": {"code": "zh-Hans", "name": "Chinese (Simplified)"},
                "entry_type": "standard",
                "headword_summary": "整体说明。",
                "study_notes": [],
                "etymology_note": None,
                "etymologies": [{"etymology_id": "et1", "text": None, "pos_members": ["noun"]}],
                "pos_groups": [
                    {
                        "pos_group_id": "noun|et1",
                        "pos": "noun",
                        "etymology_id": "et1",
                        "summary": "词性说明。",
                        "usage_notes": None,
                        "forms": [],
                        "pronunciations": [{"ipa": "/x/", "text": None, "audio_url": None, "tags": []}],
                        "meanings": [
                            {
                                "meaning_id": "s1",
                                "short_gloss": "女人",
                                "learner_explanation": "详细解释。",
                                "usage_note": None,
                                "labels": [],
                                "topics": [],
                                "examples": [],
                                "relations": [],
                            }
                        ],
                        "relations": [],
                    }
                ],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = cli.main(["validate-distribution-jsonl", "--input", str(output_path)])

    assert exit_code == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["command"] == "validate-distribution-jsonl"
    assert summary["status"] == "succeeded"
    assert summary["entry_count"] == 1
    assert "[progress] stage=export.distribution_jsonl.validate event=validate_start" in captured.err
    assert "[progress] stage=export.distribution_jsonl.validate event=validate_complete" in captured.err
