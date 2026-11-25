#!/bin/bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092}"
TOPIC="${KAFKA_TOPIC:-movie-events}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  sleep 1
done

echo "Creating topic '${TOPIC}' (partitions=${PARTITIONS}, replication=${REPLICATION})..."
kafka-topics --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}"

kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
echo "Topic '${TOPIC}' is ready."
