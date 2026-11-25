#!/bin/bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092}"
TOPIC="${KAFKA_TOPIC:-movie-events}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-2}"
MIN_ISR="${KAFKA_MIN_INSYNC_REPLICAS:-1}"

IFS=',' read -ra BROKERS <<< "${BOOTSTRAP}"
for broker in "${BROKERS[@]}"; do
  echo "Waiting for Kafka broker at ${broker}..."
  until kafka-broker-api-versions --bootstrap-server "${broker}" >/dev/null 2>&1; do
    sleep 1
  done
done

echo "Creating topic '${TOPIC}' (partitions=${PARTITIONS}, rf=${REPLICATION}, min.insync.replicas=${MIN_ISR})..."
kafka-topics --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config "min.insync.replicas=${MIN_ISR}"

kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
echo "Topic '${TOPIC}' is ready."
