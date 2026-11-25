import logging
from datetime import date, datetime
from time import sleep

import psycopg
from psycopg.errors import TransactionRollback

from .clickhouse_queries import DailySnapshot
from .config import POSTGRES

log = logging.getLogger(__name__)

COLUMNS = ("metric_date", "metric_name", "dimension_key", "dimension_value", "metric_value", "calculated_at")
COLUMN_SQL = ", ".join(COLUMNS)
DAILY_NAMES = ("dau", "avg_watch_seconds", "started_sessions", "completed_sessions", "conversion")
UPSERT = f"""
INSERT INTO daily_metrics ({COLUMN_SQL})
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (metric_date, metric_name, dimension_key, dimension_value)
DO UPDATE SET metric_value = EXCLUDED.metric_value, calculated_at = EXCLUDED.calculated_at
"""


def metric_rows(day: date, snapshot: DailySnapshot, calculated_at: datetime) -> list[tuple]:
    values = [(name, "", "", value) for name, value in zip(DAILY_NAMES, snapshot.activity, strict=True)]
    values.extend(("top_movie_completed_views", "movie_id", movie, count) for movie, count in snapshot.top)
    if snapshot.retention:
        values.append(("retention_cohort_size", "", "", snapshot.retention[0][1]))
    for offset, _, returned, rate in snapshot.retention:
        values.append(("retention_rate", "day_number", str(offset), rate))
        values.append(("retention_returned_users", "day_number", str(offset), returned))
        if offset in (1, 7):
            values.append((f"retention_d{offset}", "", "", rate))
    return [(day, *row, calculated_at) for row in values]


def retry_postgres(operation):
    for attempt in range(3):
        try:
            return operation()
        except (psycopg.OperationalError, psycopg.InterfaceError, TransactionRollback) as error:
            if attempt == 2:
                raise
            log.warning("action=postgres_retry attempt=%s error=%s", attempt + 1, type(error).__name__)
            sleep(0.25 * 2**attempt)


def write_metrics(day: date, rows: list[tuple]):
    def write():
        with psycopg.connect(**POSTGRES) as conn, conn.cursor() as cursor:
            # Remove dimensions that disappeared before upserting the new snapshot.
            cursor.execute(
                """
                DELETE FROM daily_metrics m
                WHERE metric_date = %s AND NOT EXISTS (
                    SELECT 1 FROM unnest(%s::text[], %s::text[], %s::text[]) AS k(name, key, value)
                    WHERE (m.metric_name, m.dimension_key, m.dimension_value) = (k.name, k.key, k.value)
                )
                """,
                (day, [row[1] for row in rows], [row[2] for row in rows], [row[3] for row in rows]),
            )
            cursor.executemany(UPSERT, rows)

    retry_postgres(write)


def read_metrics(day: date) -> list[tuple]:
    def read():
        with psycopg.connect(**POSTGRES) as conn:
            return conn.execute(
                f"""
                SELECT {COLUMN_SQL} FROM daily_metrics WHERE metric_date = %s
                ORDER BY metric_name, dimension_key, dimension_value
                """,
                (day,),
            ).fetchall()

    return retry_postgres(read)
