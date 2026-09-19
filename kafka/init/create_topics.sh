#!/bin/bash
set -e

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"kafka:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"stock-market-data"}
KAFKA_PARTITIONS=${KAFKA_PARTITIONS:-3}
KAFKA_REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-1}

echo "Waiting for Kafka broker at ${KAFKA_BOOTSTRAP_SERVERS}..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --list > /dev/null 2>&1; do
    echo "Kafka not ready yet. Retrying in 2 seconds..."
    sleep 2
done

echo "Checking if topic '${KAFKA_TOPIC}' exists..."
if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --list | grep -w "${KAFKA_TOPIC}" > /dev/null 2>&1; then
    echo "Topic '${KAFKA_TOPIC}' already exists. Skipping creation."
else
    echo "Creating topic '${KAFKA_TOPIC}' (Partitions: ${KAFKA_PARTITIONS}, Replication Factor: ${KAFKA_REPLICATION_FACTOR})..."
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
        --create \
        --topic "${KAFKA_TOPIC}" \
        --partitions "${KAFKA_PARTITIONS}" \
        --replication-factor "${KAFKA_REPLICATION_FACTOR}" \
        --if-not-exists
fi

echo "Verifying topic '${KAFKA_TOPIC}':"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --describe --topic "${KAFKA_TOPIC}"

echo "Kafka initialization completed successfully."
