import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
MOVIE = "http://127.0.0.1:8000"
ANALYTICS = "http://127.0.0.1:8001"
REGISTRY = "http://127.0.0.1:8081"
GRAFANA = "http://127.0.0.1:3000"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def http(url, *, data=None, headers=None, expected=200, timeout=10):
    payload = None if data is None else json.dumps(data).encode()
    request = Request(url, data=payload, headers={"Content-Type": "application/json", **(headers or {})})
    with urlopen(request, timeout=timeout) as response:
        require(response.status == expected, f"{url} returned {response.status}, expected {expected}")
        return json.load(response)


def compose(*args, timeout=30):
    result = subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, check=False, capture_output=True,
        encoding="utf-8", timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(f"Docker Compose command failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout.strip()


def wait(description, probe, *, timeout=90, interval=1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (URLError, TimeoutError, ConnectionError, RuntimeError, subprocess.TimeoutExpired) as error:
            last_error = error
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {description}" + (f": {last_error}" if last_error else ""))


def clickhouse(sql):
    output = compose(
        "exec", "-T", "clickhouse", "sh", "-ec",
        'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -d cinema --query "$1"',
        "query", f"{sql} SETTINGS output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow",
    )
    return [json.loads(line) for line in output.splitlines()]


def postgres(sql):
    output = compose("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "cinema", "-d", "cinema", "-Atc", sql)
    return json.loads(output)


def metrics_in_postgres(day):
    return postgres(f"""
        SELECT COALESCE(json_agg(m ORDER BY metric_name, dimension_key, dimension_value), '[]')
        FROM daily_metrics m WHERE metric_date = DATE '{day.isoformat()}'
    """)


def wait_pipeline():
    wait("Movie Service", lambda: http(f"{MOVIE}/health"))
    wait("Schema Registry", lambda: http(f"{REGISTRY}/subjects"))
    wait("ClickHouse raw table", lambda: clickhouse("SELECT count() FROM raw_movie_events"))


def new_event(day="2026-03-15", user=None):
    return {
        "event_id": str(uuid4()),
        "user_id": user or f"verification-{uuid4()}",
        "movie_id": "pipeline-verification",
        "event_type": "VIEW_FINISHED",
        "timestamp": f"{day}T12:34:56.123+03:00",
        "device_type": "TABLET",
        "session_id": f"verification-{uuid4()}",
        "progress_seconds": 3781,
    }


def publish(event, *, wait_for_recovery=False):
    url = f"{MOVIE}/events"

    def attempt():
        try:
            return http(url, data=event, expected=201, timeout=30)
        except HTTPError as error:
            if error.code != 503:
                raise AssertionError(f"{url} returned {error.code}: {error.read().decode()}") from error
            raise

    response = wait("Kafka publication", attempt) if wait_for_recovery else attempt()
    require(response == {"event_id": event["event_id"], "status": "published"}, f"Unexpected publish response {response}")
    print(f"Published event {event['event_id']}", flush=True)


def verify_ingestion(event):
    event_id = event["event_id"]
    sql = f"""
        SELECT event_id, user_id, movie_id, event_type, device_type, session_id, progress_seconds,
               toUnixTimestamp64Milli(timestamp) AS timestamp_ms, event_date
        FROM raw_movie_events FINAL WHERE event_id = toUUID('{event_id}')
    """
    rows = wait(f"ClickHouse event {event_id}", lambda: clickhouse(sql))
    timestamp = datetime.fromisoformat(event["timestamp"])
    expected = {key: value for key, value in event.items() if key != "timestamp"}
    expected["timestamp_ms"] = int(timestamp.timestamp() * 1000)

    expected["event_date"] = timestamp.astimezone(UTC).date().isoformat()
    require(rows == [expected], f"Expected {expected}, got {rows}")
    print("Kafka -> ClickHouse ingestion verified.", flush=True)


def service_healthy(service):
    container_id = compose("ps", "-q", service)
    if not container_id:
        return False
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_id],
        check=True, capture_output=True, text=True, timeout=10,
    )
    return result.stdout.strip() == "healthy"


def topic_description(broker="kafka-1"):
    description = compose(
        "exec", "-T", broker, "kafka-topics",
        "--bootstrap-server", f"{broker}:29092", "--describe", "--topic", "movie-events",
    )
    partitions = {
        int(partition): (int(leader), {int(replica) for replica in replicas.split(",") if replica})
        for partition, leader, replicas in re.findall(
            r"Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*[\d,]+\s+Isr:\s*([\d,]*)", description
        )
    }
    return description, partitions


def verify_topic_config(description):
    require(
        re.search(r"ReplicationFactor:\s*2\b", description) and
        re.search(r"min\.insync\.replicas=1\b", description),
        f"Unexpected movie-events configuration: {description}",
    )


def demo_grafana_password():
    if "GRAFANA_PASSWORD" in os.environ:
        return os.environ["GRAFANA_PASSWORD"]
    env_path = ROOT / ".env"
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            if line.startswith("GRAFANA_PASSWORD="):
                return line.split("=", 1)[1].strip().strip("\"'")
    return "cinema_demo"
