import csv
import io
import logging
import os
from datetime import date

import boto3
from botocore.config import Config

from .config import S3_BUCKET, S3_ENDPOINT
from .postgres_writer import COLUMNS, read_metrics

log = logging.getLogger(__name__)


def object_key(day: date) -> str:
    return f"daily/{day.isoformat()}/aggregates.csv"


def csv_bytes(rows: list[tuple]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(COLUMNS)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "cinema-demo"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "cinema-demo-password"),
        region_name="us-east-1",
        config=Config(
            connect_timeout=3,
            read_timeout=5,
            retries={"mode": "standard", "total_max_attempts": 3},
            s3={"addressing_style": "path"},
        ),
    )


class NoMetricsError(LookupError):
    pass


class Exporter:
    def __init__(self, client):
        self.client = client

    def export(self, day: date) -> dict:
        rows = read_metrics(day)
        if not rows:
            raise NoMetricsError(day.isoformat())
        key = object_key(day)
        self.client.put_object(
            Bucket=S3_BUCKET, Key=key, Body=csv_bytes(rows), ContentType="text/csv; charset=utf-8"
        )
        log.info("action=export_complete date=%s key=%s rows_exported=%s", day, key, len(rows))
        return {"date": day, "bucket": S3_BUCKET, "key": key, "rows_exported": len(rows)}
