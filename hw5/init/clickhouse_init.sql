CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Virtual Kafka consumer table (does NOT store rows on disk).
-- Reads Avro messages in Confluent Wire Format directly from Kafka topic 'movie-events'.
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
    kafka_broker_list = 'kafka-1:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-consumer',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1;

-- 2. Permanent analytical storage (MergeTree).
-- PARTITION BY month keeps partition count small and allows fast monthly drops.
-- ORDER BY starts with day -> event_type -> user_id -> timestamp to match
-- daily DAU, conversion, watch time, and cohort queries.
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

-- 3. Insert trigger: automatically moves batches from Kafka Engine to MergeTree
-- and commits Kafka offsets after successful writes (at-least-once).
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
