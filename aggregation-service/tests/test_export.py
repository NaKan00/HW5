import csv
import io
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app import exporter


DAY = date(2026, 4, 1)
NOW = datetime(2026, 4, 9, tzinfo=UTC)
ROWS = [(DAY, "top_movie_completed_views", "movie_id", "Harbor, II", Decimal("3"), NOW)]


def test_export_key_and_csv_columns():
    assert exporter.object_key(DAY) == "daily/2026-04-01/aggregates.csv"
    result, = csv.DictReader(io.StringIO(exporter.csv_bytes(ROWS).decode()))
    assert result == {
        "metric_date": "2026-04-01",
        "metric_name": "top_movie_completed_views",
        "dimension_key": "movie_id",
        "dimension_value": "Harbor, II",
        "metric_value": "3",
        "calculated_at": "2026-04-09 00:00:00+00:00",
    }


def test_repeated_export_replaces_object(monkeypatch):
    class Bucket:
        def __init__(self):
            self.objects = {}

        def put_object(self, *, Bucket, Key, Body, ContentType):
            self.objects[Bucket, Key] = Body

    s3 = Bucket()
    service = exporter.Exporter(s3)
    monkeypatch.setattr(exporter, "read_metrics", lambda _: ROWS)
    assert service.export(DAY)["rows_exported"] == 1
    original, = s3.objects.values()
    revised = [(DAY, "dau", "", "", Decimal("10"), NOW)]
    monkeypatch.setattr(exporter, "read_metrics", lambda _: revised)
    service.export(DAY)
    assert s3.objects == {("movie-analytics", exporter.object_key(DAY)): exporter.csv_bytes(revised)}
    assert next(iter(s3.objects.values())) != original


def test_export_without_metrics_cannot_create_an_empty_object(monkeypatch):
    monkeypatch.setattr(exporter, "read_metrics", lambda _: [])
    with pytest.raises(exporter.NoMetricsError):
        exporter.Exporter(None).export(DAY)
