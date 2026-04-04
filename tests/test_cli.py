from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from open_dictionary import cli
from open_dictionary.config.settings import RuntimeSettings


def test_pipeline_run_executes_stages_and_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, object] = {}

    def fake_raw(**kwargs):
        calls["raw"] = kwargs
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
        return SimpleNamespace(
            run_id=uuid4(),
            groups_processed=742,
            entries_written=742,
            relations_written=10,
            triage_written=1,
        )

    def fake_llm(**kwargs):
        calls["llm"] = kwargs
        return SimpleNamespace(
            run_id=uuid4(),
            processed=742,
            succeeded=742,
            failed=0,
        )

    def fake_distribution(**kwargs):
        calls["distribution"] = kwargs
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

    exit_code = cli.main(
        [
            "pipeline-run",
            "--skip-db-init",
            "--archive-path",
            "fixtures/wiktionary/raw.jsonl",
            "--max-workers",
            "50",
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

    stdout = capsys.readouterr().out
    summary = json.loads(stdout)
    assert summary["llm"]["failed"] == 0
    assert summary["distribution_export"]["entry_count"] == 741
    assert summary["audit_export"]["entry_count"] == 742


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

    with pytest.raises(SystemExit):
        cli.main(
            [
                "pipeline-run",
                "--skip-db-init",
                "--archive-path",
                "fixtures/wiktionary/raw.jsonl",
            ]
        )
