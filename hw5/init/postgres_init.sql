CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE IF NOT EXISTS daily_top_movies (
    metric_date DATE NOT NULL,
    movie_id TEXT NOT NULL,
    views_count INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, movie_id)
);
