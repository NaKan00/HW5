import os
from pathlib import Path

TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = Path(os.getenv("MOVIE_SCHEMA_PATH", "schemas/movie_event.avsc"))
