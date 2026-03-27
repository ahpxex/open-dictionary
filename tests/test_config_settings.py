from __future__ import annotations

from pathlib import Path

import pytest

from open_dictionary.config.settings import load_settings


def test_load_settings_reads_database_url_from_env_file(tmp_path: Path) -> None:
    # This case proves that the runtime settings loader can bootstrap itself
    # from a repository-local .env file without requiring shell preconfiguration.
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DATABASE_URL=postgresql://tester@localhost:5432/test_db\n",
        encoding="utf-8",
    )

    settings = load_settings(env_file=env_file)

    assert settings.database_url == "postgresql://tester@localhost:5432/test_db"
    assert settings.env_file == env_file
    assert settings.database_url_var == "DATABASE_URL"


def test_load_settings_raises_when_database_url_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This case protects the rewrite against silent startup with no database DSN.
    # The test explicitly clears DATABASE_URL first because the developer shell
    # may already have a value exported from a previous run.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("LLM_MODEL=dummy\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        load_settings(env_file=env_file)
