from __future__ import annotations

import psycopg

from open_dictionary.config import RuntimeSettings


def get_connection(
    settings_or_dsn: RuntimeSettings | str,
    *,
    autocommit: bool = False,
) -> psycopg.Connection:
    if isinstance(settings_or_dsn, RuntimeSettings):
        return psycopg.connect(
            settings_or_dsn.database_url,
            autocommit=autocommit,
            application_name=settings_or_dsn.application_name,
        )

    return psycopg.connect(settings_or_dsn, autocommit=autocommit)
