from common import compose, new_event, publish, require, service_healthy, topic_description, verify_ingestion, verify_topic_config, wait, wait_pipeline


def fully_replicated():
    _, partitions = topic_description()
    return len(partitions) == 3 and all(leader in (1, 2) and isr == {1, 2} for leader, isr in partitions.values())


def single_broker():
    _, partitions = topic_description("kafka-2")
    return len(partitions) == 3 and all(leader == 2 and isr == {2} for leader, isr in partitions.values())


def round_trip(user=None):
    event = new_event(day="2026-03-16", user=user)
    publish(event, wait_for_recovery=True)
    verify_ingestion(event)
    return event


def main():
    wait_pipeline()
    for service in ("kafka-1", "kafka-2", "kafka-controller"):
        wait(f"{service} healthy", lambda: service_healthy(service))
    wait("all partitions fully replicated", fully_replicated, interval=2)
    description, _ = topic_description()
    verify_topic_config(description)
    baseline = round_trip()
    print("Stopping kafka-1 for the failover check.", flush=True)
    try:
        compose("stop", "-t", "15", "kafka-1", timeout=40)
        wait("all leaders and ISR on kafka-2", single_broker, interval=2)
        require(not compose("ps", "-q", "kafka-1"), "kafka-1 is still running")
        round_trip(baseline["user_id"])
        print("Verified publication and ingestion while only kafka-2 was serving traffic.", flush=True)
    finally:
        compose("start", "kafka-1", timeout=45)
        wait("kafka-1 healthy again", lambda: service_healthy("kafka-1"), timeout=120, interval=2)
        wait("replication restored on all partitions", fully_replicated, timeout=120, interval=2)
    round_trip(baseline["user_id"])
    print("Kafka single-broker failover and recovery passed.")


if __name__ == "__main__":
    main()
