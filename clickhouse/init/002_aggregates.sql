CREATE TABLE IF NOT EXISTS cinema.daily_activity (
    metric_date Date,
    dau UInt64,
    avg_watch_seconds Float64,
    started_sessions UInt64,
    completed_sessions UInt64,
    conversion Float64,
    calculated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY metric_date
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS cinema.daily_top_movies (
    metric_date Date,
    rank UInt8,
    movie_id String,
    completed_views UInt64,
    calculated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY metric_date
ORDER BY (metric_date, rank);

CREATE TABLE IF NOT EXISTS cinema.daily_retention (
    metric_date Date,
    day_number UInt8,
    cohort_size UInt64,
    returned_users UInt64,
    retention_rate Float64,
    calculated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY metric_date
ORDER BY (metric_date, day_number);

CREATE TABLE IF NOT EXISTS cinema.daily_activity_stage AS cinema.daily_activity;
CREATE TABLE IF NOT EXISTS cinema.daily_top_movies_stage AS cinema.daily_top_movies;
CREATE TABLE IF NOT EXISTS cinema.daily_retention_stage AS cinema.daily_retention;
