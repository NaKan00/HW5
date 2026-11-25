from datetime import date, timedelta
from math import isclose

from common import ANALYTICS, MOVIE, clickhouse, http, metrics_in_postgres, require, wait, wait_pipeline

DEMO_DAY = date(2026, 4, 1)


def wait_for_seeded_events():
    wait_pipeline()
    result = http(f"{MOVIE}/generator/demo", data={}, expected=201, timeout=90)
    dates = [date.fromisoformat(day) for day in result["dates"]]
    require(dates == [DEMO_DAY + timedelta(days=n) for n in range(9)], f"Unexpected demo dates: {dates}")

    def ingested():
        rows = clickhouse(
            "SELECT count() AS n FROM raw_movie_events FINAL "
            "WHERE startsWith(user_id, 'demo-2026-04-01-')"
        )
        return rows[0]["n"] == result["events_published"]

    wait(f"all {result['events_published']} seeded events", ingested)
    print(f"Seeded {result['events_published']} events across {len(dates)} UTC dates.")
    return dates


def aggregate(day):
    return http(f"{ANALYTICS}/aggregate/{day}", data={}, timeout=60)


def verify_clickhouse():
    activity, = clickhouse(f"""
        SELECT dau, avg_watch_seconds, started_sessions, completed_sessions, conversion
        FROM daily_activity WHERE metric_date = '{DEMO_DAY}'
    """)
    require(activity == {
        "dau": 10, "avg_watch_seconds": 4200, "started_sessions": 10,
        "completed_sessions": 9, "conversion": 0.9,
    }, f"Unexpected daily activity: {activity}")
    top = clickhouse(f"""
        SELECT rank, movie_id, completed_views FROM daily_top_movies
        WHERE metric_date = '{DEMO_DAY}' ORDER BY rank
    """)
    require(top == [
        {"rank": 1, "movie_id": "orbit", "completed_views": 6},
        {"rank": 2, "movie_id": "harbor", "completed_views": 3},
    ], f"Unexpected top movies: {top}")
    for day, size, returns in (
        (DEMO_DAY, 10, [10, 5, 0, 0, 0, 0, 0, 3]),
        (DEMO_DAY + timedelta(days=1), 4, [4, 2, 0, 0, 0, 0, 0, 1]),
    ):
        cohort = clickhouse(f"""
            SELECT day_number, cohort_size, returned_users, retention_rate
            FROM daily_retention WHERE metric_date = '{day}' ORDER BY day_number
        """)
        expected = [
            {"day_number": offset, "cohort_size": size, "returned_users": count, "retention_rate": count / size}
            for offset, count in enumerate(returns)
        ]
        require(cohort == expected, f"Unexpected retention cohort for {day}: {cohort}")
    print(f"ClickHouse {DEMO_DAY}: DAU 10, watch time 4200 s, conversion 90%, top movie orbit (6).")
    print("Cohort retention Day 0–7: 100%, 50%, 0%, 0%, 0%, 0%, 0%, 30%.")


def verify_postgres():
    rows = metrics_in_postgres(DEMO_DAY)
    values = {(row["metric_name"], row["dimension_key"], row["dimension_value"]): row["metric_value"] for row in rows}
    expected = {
        ("dau", "", ""): 10,
        ("avg_watch_seconds", "", ""): 4200,
        ("conversion", "", ""): 0.9,
        ("started_sessions", "", ""): 10,
        ("completed_sessions", "", ""): 9,
        ("top_movie_completed_views", "movie_id", "orbit"): 6,
        ("top_movie_completed_views", "movie_id", "harbor"): 3,
        ("retention_cohort_size", "", ""): 10,
        ("retention_d1", "", ""): 0.5,
        ("retention_d7", "", ""): 0.3,
    }
    returns = [10, 5, 0, 0, 0, 0, 0, 3]
    for number, count in enumerate(returns):
        expected[("retention_rate", "day_number", str(number))] = count / 10
        expected[("retention_returned_users", "day_number", str(number))] = count
    require(len(rows) == len(expected) and values.keys() == expected.keys(), f"Unexpected PostgreSQL metric keys: {values}")
    for key, value in expected.items():
        require(isclose(values[key], value), f"PostgreSQL {key} returned {values[key]}, expected {value}")
    return values


def main():
    wait("Aggregation Service", lambda: http(f"{ANALYTICS}/health"))
    for day in wait_for_seeded_events():
        summary = aggregate(day)
        require(summary["date"] == day.isoformat() and summary["events_processed"] > 0, f"Unexpected response: {summary}")
    verify_clickhouse()
    before = verify_postgres()
    repeated = aggregate(DEMO_DAY)
    require(repeated["metrics_written"] == len(before), f"Unexpected retry row count: {repeated}")
    require(verify_postgres() == before, "Recalculation changed the PostgreSQL metric keys or values")
    verify_clickhouse()
    print(f"PostgreSQL upsert verified: {len(before)} logical metrics after recalculation.")
    print("Business metrics demonstration passed.")


if __name__ == "__main__":
    main()
