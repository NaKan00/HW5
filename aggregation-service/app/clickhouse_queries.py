from dataclasses import dataclass
from datetime import date, datetime

DAILY = """
SELECT
    count() AS events_processed,
    uniqExact(user_id) AS dau,
    ifNotFinite(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0.) AS avg_watch_seconds,
    uniqExactIf(tuple(user_id, session_id), event_type = 'VIEW_STARTED') AS started_sessions,
    uniqExactIf(tuple(user_id, session_id), event_type = 'VIEW_FINISHED') AS completed_sessions,
    if(started_sessions = 0, 0., completed_sessions / started_sessions) AS conversion
FROM raw_movie_events FINAL
WHERE event_date = {day:Date}
"""

TOP = """
SELECT movie_id, count() AS completed_views
FROM raw_movie_events FINAL
WHERE event_date = {day:Date} AND event_type = 'VIEW_FINISHED'
GROUP BY movie_id
ORDER BY completed_views DESC, movie_id ASC
LIMIT 10
"""

RETENTION = """
WITH
    {day:Date} AS target_date,
    activity AS (
        SELECT user_id, event_date
        FROM raw_movie_events FINAL
        WHERE event_date <= addDays(toDate32(target_date), 7)
          AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
        GROUP BY user_id, event_date
    ),
    cohort AS (
        SELECT user_id
        FROM activity
        GROUP BY user_id
        HAVING min(event_date) = target_date
    ),
    returns AS (
        SELECT toUInt64(dateDiff('day', target_date, event_date)) AS day_number,
               count() AS returned_users
        FROM activity INNER JOIN cohort USING (user_id)
        WHERE event_date >= target_date
        GROUP BY day_number
    ),
    (SELECT count() FROM cohort) AS cohort_size
SELECT toUInt8(n.number), cohort_size, r.returned_users,
       if(cohort_size = 0, 0., r.returned_users / cohort_size) AS retention_rate
FROM numbers(8) AS n
LEFT JOIN returns AS r ON n.number = r.day_number
WHERE cohort_size > 0
ORDER BY n.number
"""


@dataclass(frozen=True)
class DailySnapshot:
    events_processed: int
    activity: tuple
    top: list[tuple]
    retention: list[tuple]


class ClickHouseStore:
    def __init__(self, client):
        self.client = client

    def dates(self) -> list[date]:
        return [row[0] for row in self.client.query(
            "SELECT DISTINCT event_date FROM raw_movie_events ORDER BY event_date"
        ).result_rows]

    def calculate(self, day: date) -> DailySnapshot:
        params = {"day": day}
        count, *activity = self.client.query(DAILY, parameters=params).first_row
        top = self.client.query(TOP, parameters=params).result_rows
        retention = self.client.query(RETENTION, parameters=params).result_rows
        return DailySnapshot(count, tuple(activity), top, retention)

    def write(self, day: date, result: DailySnapshot, calculated_at: datetime):
        tables = {
            "daily_activity": [(day, *result.activity, calculated_at)],
            "daily_top_movies": [
                (day, rank, movie_id, views, calculated_at)
                for rank, (movie_id, views) in enumerate(result.top, start=1)
            ],
            "daily_retention": [
                (day, *row, calculated_at) for row in result.retention
            ],
        }
        for table, rows in tables.items():
            stage = f"{table}_stage"
            if rows:
                self.client.command(f"TRUNCATE TABLE {stage}")
                self.client.insert(stage, rows)
                self.client.command(
                    f"ALTER TABLE {table} REPLACE PARTITION {{day:Date}} FROM {stage}",
                    parameters={"day": day},
                )
                self.client.command(f"TRUNCATE TABLE {stage}")
            else:
                self.client.command(
                    f"ALTER TABLE {table} DROP PARTITION {{day:Date}}",
                    parameters={"day": day},
                )
