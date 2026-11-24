import csv
import io
import json
from datetime import datetime
from decimal import Decimal

from common import ANALYTICS, compose, http, metrics_in_postgres, require, wait
from metrics_demo import DEMO_DAY, aggregate, wait_for_seeded_events

BUCKET = "movie-analytics"
KEY = f"daily/{DEMO_DAY}/aggregates.csv"
OBJECT = f"local/{BUCKET}/{KEY}"
FIELDS = ["metric_date", "metric_name", "dimension_key", "dimension_value", "metric_value", "calculated_at"]


def mc(*args):
    return compose("run", "--rm", "-T", "--no-deps", "minio-init", *args, timeout=45)


def export_and_read():
    report = http(f"{ANALYTICS}/export/{DEMO_DAY}", data={}, timeout=30)
    require(report["bucket"] == BUCKET and report["key"] == KEY, f"Unexpected export location: {report}")
    raw = mc("cat", OBJECT)
    reader = csv.DictReader(io.StringIO(raw))
    require(reader.fieldnames == FIELDS, f"Unexpected CSV header: {reader.fieldnames}")
    rows = list(reader)
    require(len(rows) == report["rows_exported"] > 0, f"Unexpected CSV row count: {report}")
    return raw, rows


def values(rows):
    return {
        tuple(row[field] for field in FIELDS[:4]): Decimal(str(row["metric_value"]))
        for row in rows
    }


def verify_csv(rows):
    stored = metrics_in_postgres(DEMO_DAY)
    require(values(rows) == values(stored) and len(rows) == len(stored), "CSV does not match PostgreSQL metrics")
    require(any(row["metric_name"] == "retention_d7" for row in rows), "D7 metric missing from CSV")
    for row in rows:
        parsed = datetime.fromisoformat(row["calculated_at"])
        require(parsed.utcoffset() is not None, f"Missing timezone in calculated_at: {row}")


def main():
    wait("Aggregation Service", lambda: http(f"{ANALYTICS}/health"))
    if not any(row["metric_name"] == "retention_d7" for row in metrics_in_postgres(DEMO_DAY)):
        wait_for_seeded_events()
    aggregate(DEMO_DAY)
    before, rows = export_and_read()
    verify_csv(rows)
    aggregate(DEMO_DAY)
    after, rows = export_and_read()
    verify_csv(rows)
    require(before != after, "The object content did not reflect the new calculation timestamp")
    listing = mc("ls", "--json", f"local/{BUCKET}/daily/{DEMO_DAY}/")
    keys = [json.loads(line)["key"] for line in listing.splitlines()]
    require(keys == ["aggregates.csv"], f"Expected exactly one object, got {keys}")
    print(f"Verified s3://{BUCKET}/{KEY} with {len(rows)} rows and one overwritten object key.")
    print("S3 export demonstration passed.")


if __name__ == "__main__":
    main()
