# Movie event contract

Schema `cinema.MovieEvent` lives in [movie_event.avsc](../schemas/movie_event.avsc). Subject `movie-events-value` follows Confluent's topic-name strategy. Schema Registry assigns IDs and versions when the producer registers the schema. Confluent framing carries the schema ID in each Kafka value.

| Field | Avro type | Meaning |
| --- | --- | --- |
| `event_id` | string | UUID. Movie Service generates one when omitted. Reuse it with the same event on retry. |
| `user_id` | string | Nonempty user ID and Kafka key. |
| `movie_id` | string | Nonempty movie ID. Use `__search__` when no movie has been selected. |
| `event_type` | enum | `VIEW_STARTED`, `VIEW_FINISHED`, `VIEW_PAUSED`, `VIEW_RESUMED`, `LIKED`, `SEARCHED`. |
| `timestamp` | long, `timestamp-millis` | Send ISO 8601 with an offset. The service converts to UTC and truncates to milliseconds. |
| `device_type` | enum | `MOBILE`, `DESKTOP`, `TV`, `TABLET`. |
| `session_id` | string | Nonempty session ID scoped to the user. |
| `progress_seconds` | int | Nonnegative Avro signed int. A search uses zero. For `VIEW_FINISHED` it is completed watch progress in seconds. |

All Avro fields are required. HTTP alone permits an omitted ID so records on the topic always have one. The ClickHouse `Date` range bounds the UTC event date. The API validates events independently. A generator session has a search, start, pause and resume, then a finish and optionally a like for completed sessions. Kafka retains order by partition offset. The producer routes via `user_id` for stable placement. Timestamp order reflects the client clock and can differ from offset order.

The deterministic seed covers nine consecutive UTC dates, multiple users and movies, and all event and device enums. Stable UUIDs allow the seed to be replayed without changing metrics. Logs include event ID, type, timestamp, partition, and offset.
