# Metric definitions

All dates and cohort offsets use UTC. Calculations read `raw_movie_events FINAL`, which coalesces immutable replays. `events_processed` is the logical number of events for the requested date.

| Metric | Definition |
| --- | --- |
| DAU | `uniqExact(user_id)` across all valid event types on the date. |
| Average watch time | Mean `progress_seconds` over `VIEW_FINISHED` events. A date with no finishes reports zero. |
| Top movies | Top 10 by number of `VIEW_FINISHED` events for each movie and date. Movie ID sorts ties ascending. |
| View conversion | Distinct `(user_id, session_id)` finishes divided by distinct starts for the same UTC date. Zero starts reports zero. Starts and finishes are counted on the day they occur, so sessions crossing days can produce a rate above 100%. |
| D1 / D7 | Distinct cohort users viewing on exactly first viewing date + 1 / + 7, divided by that cohort's size. |

## Cohort retention

Viewing activity means `VIEW_STARTED` or `VIEW_FINISHED`. The denominator is the number of distinct users whose first observed viewing date is the selected UTC date. The numerator is the number of those users with viewing activity exactly N calendar days later. User-date pairs are deduplicated before counting. Day 0 is 100% for any nonempty cohort. Empty cohorts have no row. For an immature cohort, future days have zero observed returns and are recomputed as events arrive.

The seed's April 1 cohort has ten users. Five return on Day 1 and three on Day 7. The April 2 cohort has four new users, two returning on Day 1 and one returning on Day 7. Grafana reads all eight values from `daily_retention` and displays fractions as percentages.

## Storage

ClickHouse persists `daily_activity`, `daily_top_movies`, and `daily_retention`, partitioned by aggregate date. `daily_retention.metric_date` is the cohort date. The PostgreSQL `daily_metrics` primary key is `(metric_date, metric_name, dimension_key, dimension_value)`. Top movie counts use `movie_id` as the dimension and retention rows use `day_number`. D1 and D7 are also stored as named scalar metrics. Every row carries `calculated_at`. The exporter writes all six PostgreSQL columns to CSV. Both stores keep conversion and retention on the ratio scale.
