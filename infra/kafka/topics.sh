set -euo pipefail

BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  echo "  Kafka not ready, retrying in 5s..."
  sleep 5
done
echo "Kafka is ready."

create() {
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
  echo "  topic: $1 (partitions=$2)"
}

echo "Creating topics..."
create tasks             6
create results           6
create events.raw        3
create events.normalized 3
create events.failed     3   # DLQ for normalization failures — needs a replay/monitoring consumer

echo "All topics created."