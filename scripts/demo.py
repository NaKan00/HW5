import argparse
import json

import export_demo
import grafana_demo
import integration_test
import kafka_failover_demo
import metrics_demo
from common import REGISTRY, http, require, topic_description, verify_topic_config, wait_pipeline


def verify_schema_and_topic():
    wait_pipeline()
    subject = "movie-events-value"
    require(subject in http(f"{REGISTRY}/subjects"), f"Schema subject {subject} is missing")
    latest = http(f"{REGISTRY}/subjects/{subject}/versions/latest")
    schema = json.loads(latest["schema"])
    require(schema["name"] == "MovieEvent" and schema["type"] == "record", f"Unexpected schema: {schema}")
    require([field["name"] for field in schema["fields"]] == [
        "event_id", "user_id", "movie_id", "event_type", "timestamp",
        "device_type", "session_id", "progress_seconds",
    ], f"Unexpected schema fields: {schema}")
    require(schema["fields"][4]["type"]["logicalType"] == "timestamp-millis", "Expected a millisecond timestamp")
    print(f"Schema Registry: {subject} version {latest['version']} (schema ID {latest['id']}).")

    description, partitions = topic_description()
    require(len(partitions) == 3 and all(isr == {1, 2} for _, isr in partitions.values()), description)
    verify_topic_config(description)
    print(description)


def main():
    command = argparse.ArgumentParser(description="Verify the running cinema analytics stack")
    command.add_argument("--failover", action="store_true", help="also stop and restore kafka-1 to verify failover")
    args = command.parse_args()
    verify_schema_and_topic()
    integration_test.main()
    metrics_demo.main()
    grafana_demo.main()
    export_demo.main()
    if args.failover:
        kafka_failover_demo.main()


if __name__ == "__main__":
    main()
