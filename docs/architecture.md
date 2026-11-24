# Architecture

## Startup and storage

The broker containers and the controller-only container share a static KRaft quorum. Kafka topic initialization waits for both brokers, creates `movie-events`, and prints its partition and ISR state. Schema Registry persists schema versions to Kafka with a requested replication factor of two. Movie Service registers its schema idempotently before it starts accepting traffic. Both producer and ClickHouse consumer use both broker addresses.

The ClickHouse and PostgreSQL images run their initialization SQL on clean volumes. Grafana downloads a versioned ClickHouse plugin at image build time with a pinned archive digest, then provisions the datasource and dashboard from files. The MinIO initialization job creates the bucket if necessary, and Aggregation Service starts after the bucket and databases are ready.

Kafka has two brokers, each with both broker and controller roles, plus a third controller-only node. A single stopped broker leaves two out of three votes and one replica of every healthy partition. Unclean leader election is disabled. If a live replica is out of ISR when the other fails, that partition can become unavailable. With RF=2 and min ISR=1, `acks=all` can acknowledge a write held on just one broker during an outage. Losing that broker's disk before replication catches up can lose acknowledged writes.

## Ingestion and replay

`cinema.movie_events_queue` uses Kafka Engine with `AvroConfluent` and `format_avro_schema_registry_url` to look up the writer schema ID from each Confluent-framed payload. The schema's enums decode to strings and its logical timestamp decodes as `DateTime64(3, 'UTC')`. `cinema.ingest_movie_events` casts the UUID and progress fields and derives a UTC `event_date` into `cinema.raw_movie_events`.

The raw table is a monthly-partitioned `ReplacingMergeTree` ordered by `(event_date, user_id, timestamp, event_id)`. The month avoids a part per day, while the leading date and user columns support date pruning and user activity lookups. Application retries and consumer replay can duplicate records. Raw reads with `FINAL` coalesce unchanged replays with the same sorting key before merges finish. Clients should reuse the event ID and original payload on retry. Correcting an existing event in place or reusing an ID for different data is outside the event contract.

## Calculation and export

One worker process serves manual requests and runs the aggregation loop. It serializes aggregation and export writes. For a date, ClickHouse SQL calculates activity, top movies, and cohort retention from the raw table. Each destination table replaces that date partition from a staging table, or drops it if an optional dimension has no rows. PostgreSQL deletes stale metric keys and upserts retained keys in one transaction. Each store can serve its latest committed results. If a cross-store update is interrupted, running the same date again converges both stores, and the scheduler retries on its next pass.

The scheduled calculation revisits every stored UTC event date so late events can move a user's cohort and update historical metrics. This is intended for the demo dataset. ClickHouse retains the full raw history needed for first-activity calculations.

The export step reads PostgreSQL, serializes every row for the selected date, and sends one S3 PUT. Each completed event date is exported once per UTC day after `EXPORT_HOUR_UTC` (default 0), following a successful calculation. The in-process schedule can repeat an export after a restart. The next daily run captures late arrivals, and a manual export can refresh the object immediately after recalculation. A failed export remains eligible on subsequent scheduler passes.
