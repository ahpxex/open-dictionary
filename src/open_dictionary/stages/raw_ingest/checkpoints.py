from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb


DEFAULT_CHECKPOINT_KEY = "main"


def save_checkpoint(
    conn,
    *,
    run_id: UUID,
    stage_name: str,
    payload: dict[str, Any],
    checkpoint_key: str = DEFAULT_CHECKPOINT_KEY,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO meta.stage_checkpoints (
                run_id,
                stage_name,
                checkpoint_key,
                payload
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id, stage_name, checkpoint_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            """,
            (
                run_id,
                stage_name,
                checkpoint_key,
                Jsonb(payload),
            ),
        )
