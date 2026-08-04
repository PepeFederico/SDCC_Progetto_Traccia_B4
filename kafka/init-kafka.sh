#!/bin/sh
BROKER="kafka:9096"
echo "Waiting for Kafka broker to be ready..."

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  echo "Kafka not ready yet, retrying..."
  sleep 2
done

echo "Kafka is ready. Creating topics..."

# Creazione di events-topic (1 partizioni)
echo "Creating topic: events-topic"
/opt/kafka/bin/kafka-topics.sh --create --topic "events-topic" \
  --bootstrap-server "$BROKER" \
  --partitions 1 --replication-factor 1 --if-not-exists

# Creazione dei topic dei segnali
echo "Creating topic: signals-topic"
/opt/kafka/bin/kafka-topics.sh --create --topic "signals-topic" \
  --bootstrap-server "$BROKER" \
  --partitions 1 --replication-factor 1 --if-not-exists
echo "All topics created."