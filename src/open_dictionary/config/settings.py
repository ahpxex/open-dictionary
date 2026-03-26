from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class RuntimeSettings:
    database_url: str
    database_url_var: str = "DATABASE_URL"
    env_file: Path | None = None
    application_name: str = "open_dictionary"


def load_settings(
    *,
    env_file: str | Path | None = ".env",
    database_url_var: str = "DATABASE_URL",
    application_name: str = "open_dictionary",
) -> RuntimeSettings:
    env_path = Path(env_file) if env_file else None
    if env_path is not None:
        load_dotenv(env_path)

    database_url = os.getenv(database_url_var)
    if not database_url:
        raise RuntimeError(
            f"Environment variable {database_url_var} is not set. "
            "Provide it via the shell environment or the configured .env file."
        )

    return RuntimeSettings(
        database_url=database_url,
        database_url_var=database_url_var,
        env_file=env_path,
        application_name=application_name,
    )
