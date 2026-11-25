# Verification and rubric audit

Run the commands in [README](../README.md) from the repository root. The end-to-end checks use Linux containers and Docker Compose V2.40.3. The PowerShell commands target Docker Desktop in Linux-container mode. Direct execution in Windows PowerShell and Docker Desktop remains unverified.

## Automated checks

| Check | Result |
| --- | --- |
| Clean Compose startup | All ten long-running services became healthy. Topic and bucket initialization each exited with code 0. Database tables, the schema subject, and Grafana were available after startup. |
| Movie Service unit tests | 16 passed, covering HTTP validation, publication errors, UTC normalization, both generator modes, and worker shutdown. |
| Aggregation and export unit tests | 7 passed, covering query orchestration, metric dimensions, ClickHouse partition replacement, PostgreSQL upsert, CSV formatting, and overwrite semantics. |
| Continuous generator | Start, repeated start, status and stop passed on the running stack. Nine acknowledged events from two completed sessions reached ClickHouse with current UTC timestamps and ordered playback progress. |
| Exact Kafka → ClickHouse event | A fresh UUID reached the raw table, with every event field checked, including the normalized timestamp and UTC date. |
| Business metrics | 178 seed events across 9 days. April 1 DAU 10, average watch time 4,200 seconds, conversion 90%, and `orbit` first with 6 completed views. D1 50%, D7 30%, and both checked cohorts matched all 8 offsets. |
| Empty date | ClickHouse and PostgreSQL both reported zero conversion and watch time for a date without activity. |
| Recalculation | The same daily calculation yielded 26 PostgreSQL metric keys with unchanged values and replaced the ClickHouse date partitions. |
| Grafana | The API confirmed provisioning and executed all five panel queries against ClickHouse. The retention query returned the expected eight values. Chromium rendered the April 1 percentages and the Day 7 gradient. |
| S3 export | The demo downloaded and compared all 26 rows with PostgreSQL. A second export replaced the same key. A scheduled run separately wrote a CSV for an integration-test date. |
| Kafka failover | A fresh UUID reached ClickHouse while broker 1 was stopped. Broker 1 was restored, both replicas rejoined ISR on all partitions, and another event reached ClickHouse. |

## Rubric

| Points | Block | Evidence |
| ---: | --- | --- |
| 1 | Kafka and Avro | `cinema.MovieEvent`, Schema Registry subject `movie-events-value`, tested version and ID 1. Topic created at startup with three partitions. `user_id` is the Kafka key. |
| 1 | Producer | Validated FastAPI endpoint, Avro serialization, delivery acknowledgements, bounded backoff, structured publication logs, a deterministic historical seed, and a controllable background stream. |
| 1 | ClickHouse | Kafka Engine with `AvroConfluent` consumes directly. The materialized view writes to persistent `ReplacingMergeTree`. The integration check exercises decoding. |
| 1 | Integration | `py scripts/integration_test.py` isolates a UUID, posts the event, polls ClickHouse, and validates all stored fields. |
| 3 | Aggregation | Separate manual and scheduled service. ClickHouse SQL computes DAU, average watch time, conversion, top movies, D1 and D7. Date partitions are replaced, metric keys are upserted with retries, and processing statistics are logged. |
| 1 | Grafana | Provisioned ClickHouse datasource, a Day 0–7 retention table with colored rate cells, and four other panels querying aggregate tables. |
| 1 | Object storage | Initialized MinIO bucket, manual and scheduled CSV export, bounded retries, and verified replacement at `daily/YYYY-MM-DD/aggregates.csv`. |
| 1 | Kafka HA | Two health-checked brokers and a third controller-only voter. Three partitions, RF=2, min ISR=1, and schema persistence in Kafka. The failover demo published and ingested before, during, and after stopping broker 1. Both replicas returned to ISR on every partition. |
