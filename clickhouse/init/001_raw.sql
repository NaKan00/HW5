CREATE TABLE IF NOT EXISTS cinema.raw_movie_events (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    event_date Date,
    device_type LowCardinality(String),
    session_id String,
    progress_seconds UInt32,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS cinema.movie_events_queue (
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092,kafka-2:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'cinema-clickhouse-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest_movie_events
TO cinema.raw_movie_events AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    toDate(timestamp, 'UTC') AS event_date,
    device_type,
    session_id,
    toUInt32(progress_seconds) AS progress_seconds
FROM cinema.movie_events_queue;
