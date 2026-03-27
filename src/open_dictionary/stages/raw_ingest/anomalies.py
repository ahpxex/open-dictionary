from __future__ import annotations

from typing import Sequence
from uuid import UUID

from psycopg import sql

from open_dictionary.sources.wiktionary.contracts import SourceAnomaly


def flush_anomalies(
    conn,
    *,
    run_id: UUID,
    snapshot_id: UUID,
    anomalies: Sequence[SourceAnomaly],
) -> int:
    if not anomalies:
        return 0

    values_sql = sql.SQL(", ").join(
        sql.SQL("(%s::uuid, %s::uuid, %s::bigint, %s::bigint, %s::text, %s::text, %s::text)")
        for _ in anomalies
    )
    insert_sql = sql.SQL(
        """
        INSERT INTO raw.wiktionary_ingest_anomalies (
            run_id,
            snapshot_id,
            source_line,
            source_byte_offset,
            anomaly_type,
            detail,
            raw_payload
        ) VALUES {values}
        """
    ).format(values=values_sql)

    params: list[object] = []
    for anomaly in anomalies:
        params.extend(
            (
                run_id,
                snapshot_id,
                anomaly.source_line,
                anomaly.source_byte_offset,
                anomaly.anomaly_type,
                anomaly.detail,
                anomaly.json_text,
            )
        )

    with conn.cursor() as cursor:
        cursor.execute(insert_sql, params)
        return cursor.rowcount
