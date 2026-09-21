#!/bin/sh
BROKER="kafka:9096"
echo "Waiting for Kafka broker to be ready..."

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
  echo "Kafka not ready yet, retrying..."
  sleep 2
done

echo "Kafka is ready. Creating configured topics..."

# --- 1. TOPIC SENSORI GREZZI (Ingestione rapida, retention breve) ---
# Retention: 12 ore (i dati grezzi una volta puliti non servono a lungo sul broker)
echo "Creating topics for raw sensor data..."
/opt/kafka/bin/kafka-topics.sh --create --topic "temperature-topic-sensor" \
  --bootstrap-server "$BROKER" \
  --partitions 2 --replication-factor 1 --if-not-exists \
  --config retention.ms=43200000 \
  --config segment.bytes=67108864

/opt/kafka/bin/kafka-topics.sh --create --topic "pressure-topic-sensor" \
  --bootstrap-server "$BROKER" \
  --partitions 2 --replication-factor 1 --if-not-exists \
  --config retention.ms=43200000 \
  --config segment.bytes=67108864

# --- 2. DATA CLEANED (Processing pipeline) ---
# Retention: 24 ore
echo "Creating topic: data-topic-cleaned"
/opt/kafka/bin/kafka-topics.sh --create --topic "data-topic-cleaned" \
  --bootstrap-server "$BROKER" \
  --partitions 4 --replication-factor 1 --if-not-exists \
  --config retention.ms=86400000 \
  --config segment.bytes=134217728

# --- 3. PROCESSED DATA (Risultati per il Decisore) ---
# Retention: 3 giorni (mantiene la cronologia recente per eventuali rielaborazioni)
echo "Creating topic: processed-data-topic"
/opt/kafka/bin/kafka-topics.sh --create --topic "processed-data-topic" \
  --bootstrap-server "$BROKER" \
  --partitions 2 --replication-factor 1 --if-not-exists \
  --config retention.ms=259200000 \
  --config segment.bytes=134217728

# --- 4. SIGNALS TOPIC (Allarmi e Comandi di Controllo) ---
# Cleanup: COMPACT (Mantiene l'ultimo stato noto/comando inviato a ciascun sensore)
echo "Creating topic: signals-topic"
/opt/kafka/bin/kafka-topics.sh --create --topic "signals-topic" \
  --bootstrap-server "$BROKER" \
  --partitions 1 --replication-factor 1 --if-not-exists \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=3600000

echo "All topics successfully created with production-ready settings."

