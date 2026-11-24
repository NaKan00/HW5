import os

CLICKHOUSE = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "port": 8123,
    "username": "cinema",
    "password": os.getenv("CLICKHOUSE_PASSWORD", "cinema_demo"),
    "database": "cinema",
    "autogenerate_session_id": False,
    "connect_timeout": 5,
    "send_receive_timeout": 30,
}
POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": "cinema",
    "user": "cinema",
    "password": os.getenv("POSTGRES_PASSWORD", "cinema_demo"),
    "connect_timeout": 3,
    "options": "-c statement_timeout=15000 -c timezone=UTC",
}
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = "movie-analytics"
AGGREGATION_INTERVAL = float(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60"))
EXPORT_HOUR = int(os.getenv("EXPORT_HOUR_UTC", "0"))
if not 1 <= AGGREGATION_INTERVAL <= 86400 or not 0 <= EXPORT_HOUR <= 23:
    raise ValueError("Invalid aggregation interval or UTC export hour")
