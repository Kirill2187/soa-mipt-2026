CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.movie_events_kafka (
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS analytics.movie_events_raw (
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Int32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), event_type, user_id, timestamp);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.movie_events_mv
TO analytics.movie_events_raw AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds
FROM analytics.movie_events_kafka;

CREATE TABLE IF NOT EXISTS analytics.daily_metrics (
    metric_date Date,
    dau UInt64,
    avg_watch_seconds Float64,
    view_started UInt64,
    view_finished UInt64,
    view_conversion Float64,
    retention_d1 Float64,
    retention_d7 Float64,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE TABLE IF NOT EXISTS analytics.daily_top_movies (
    metric_date Date,
    movie_id String,
    views_count UInt64,
    finished_count UInt64,
    avg_watch_seconds Float64,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, movie_id);

CREATE TABLE IF NOT EXISTS analytics.retention_cohorts (
    cohort_date Date,
    day_num UInt8,
    cohort_size UInt64,
    returned_users UInt64,
    retention_pct Float64,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_num);
