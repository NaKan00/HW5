CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date date NOT NULL,
    metric_name text NOT NULL,
    dimension_key text NOT NULL DEFAULT '',
    dimension_value text NOT NULL DEFAULT '',
    metric_value numeric NOT NULL,
    calculated_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric_name, dimension_key, dimension_value)
);
