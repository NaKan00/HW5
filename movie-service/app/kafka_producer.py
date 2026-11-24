import json
import logging
from threading import Lock
from time import sleep

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import BOOTSTRAP_SERVERS, REGISTRY_URL, SCHEMA_PATH, SUBJECT, TOPIC
from .models import MovieEvent

log = logging.getLogger(__name__)


class PublicationError(RuntimeError):
    pass


class EventPublisher:
    def __init__(self):
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        registry = SchemaRegistryClient({"url": REGISTRY_URL, "timeout": 5, "max.retries": 2})
        schema_id = registry.register_schema(SUBJECT, Schema(schema, "AVRO"))
        self._serialize = AvroSerializer(registry, schema, conf={"auto.register.schemas": False})
        self._producer = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
            "linger.ms": 1,
            "delivery.timeout.ms": 8000,
            "request.timeout.ms": 3000,
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 1000,
        })
        self._lock = Lock()
        log.info(json.dumps({"action": "schema_registered", "subject": SUBJECT, "schema_id": schema_id}))

    def publish(self, event: MovieEvent) -> None:
        with self._lock:
            try:
                payload = self._serialize(event.avro_record(), SerializationContext(TOPIC, MessageField.VALUE))
            except Exception as error:
                raise PublicationError("Avro serialization failed") from error
            for attempt in range(3):
                try:
                    partition, offset = self._deliver(event.user_id, payload)
                    break
                except PublicationError:
                    if attempt == 2:
                        raise
                    sleep(0.25 * 2**attempt)

        log.info(json.dumps({
            "action": "published",
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "timestamp": event.timestamp.isoformat(),
            "partition": partition,
            "offset": offset,
        }))

    def _deliver(self, key: str, payload: bytes) -> tuple[int, int]:
        result = []
        try:
            self._producer.produce(TOPIC, key=key.encode(), value=payload, on_delivery=lambda error, msg: result.append((error, msg)))
            self._producer.flush(9)
        except (BufferError, KafkaException) as error:
            raise PublicationError("Kafka delivery failed") from error
        if not result or result[0][0] is not None:
            error_name = result[0][0].name() if result and result[0][0] else "timeout"
            raise PublicationError(f"Kafka delivery failed ({error_name})")
        message = result[0][1]
        return message.partition(), message.offset()
