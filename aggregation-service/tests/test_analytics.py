from datetime import UTC, date, datetime
from unittest.mock import Mock

from app import postgres_writer
from app.clickhouse_queries import DAILY, RETENTION, TOP, ClickHouseStore, DailySnapshot
from app.postgres_writer import metric_rows, write_metrics

DAY = date(2026, 4, 1)
NOW = datetime(2026, 4, 9, 12, tzinfo=UTC)


def snapshot():
    return DailySnapshot(
        47, (10, 4200.0, 10, 9, 0.9), [("orbit", 6), ("harbor", 3)],
        [(offset, 10, returned, returned / 10) for offset, returned in enumerate([10, 5, 0, 0, 0, 0, 0, 3])],
    )


def test_aggregate_queries_use_the_requested_date_and_build_one_snapshot():
    client = Mock()
    expected = snapshot()
    client.query.side_effect = [
        Mock(first_row=(expected.events_processed, *expected.activity)),
        Mock(result_rows=expected.top),
        Mock(result_rows=expected.retention),
    ]
    assert ClickHouseStore(client).calculate(DAY) == expected
    assert [call.args[0] for call in client.query.call_args_list] == [DAILY, TOP, RETENTION]
    assert all(call.kwargs == {"parameters": {"day": DAY}} for call in client.query.call_args_list)


def test_metrics_are_projected_with_required_dimensions():
    rows = metric_rows(DAY, snapshot(), NOW)
    metrics = {(name, key, dimension): value for _, name, key, dimension, value, _ in rows}
    assert len(metrics) == len(rows) == 26
    assert metrics["dau", "", ""] == 10
    assert metrics["avg_watch_seconds", "", ""] == 4200
    assert metrics["conversion", "", ""] == 0.9
    assert metrics["top_movie_completed_views", "movie_id", "orbit"] == 6
    assert metrics["retention_d1", "", ""] == 0.5
    assert metrics["retention_d7", "", ""] == 0.3
    assert metrics["retention_rate", "day_number", "0"] == 1.0
    assert metrics["retention_returned_users", "day_number", "7"] == 3


def test_write_replaces_clickhouse_partitions_and_clears_empty_dimensions():
    client = Mock()
    store = ClickHouseStore(client)
    store.write(DAY, snapshot(), NOW)
    assert len(client.insert.call_args_list) == 3
    assert sum("REPLACE PARTITION" in call.args[0] for call in client.command.call_args_list) == 3
    client.reset_mock()
    store.write(DAY, DailySnapshot(0, (0, 0.0, 0, 0, 0.0), [], []), NOW)
    assert len(client.insert.call_args_list) == 1
    assert sum("DROP PARTITION" in call.args[0] for call in client.command.call_args_list) == 2


def test_postgres_recalculation_upserts_same_keys_in_one_transaction(monkeypatch):
    conn = Mock()
    cursor = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    conn.cursor.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(postgres_writer.psycopg, "connect", lambda **_: conn)
    rows = metric_rows(DAY, snapshot(), NOW)
    write_metrics(DAY, rows)
    write_metrics(DAY, rows)
    assert cursor.executemany.call_count == 2
    query, submitted = cursor.executemany.call_args.args
    assert "ON CONFLICT (metric_date, metric_name, dimension_key, dimension_value)" in query
    assert "DO UPDATE" in query
    assert submitted == rows
    assert cursor.execute.call_count == conn.__exit__.call_count == 2
