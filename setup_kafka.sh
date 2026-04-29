#!/bin/bash
# ─────────────────────────────────────────────
#  Kafka Setup Script — Air Traffic Analytics
#  Run this ONCE before starting the pipeline
# ─────────────────────────────────────────────

KAFKA_HOME=${KAFKA_HOME:-"C:\kafka"}   # change if your Kafka is elsewhere
BROKER="localhost:9092"
TOPIC="air-traffic-raw"

echo "[1/3] Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
sleep 5

echo "[2/3] Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
sleep 5

echo "[3/3] Creating topic: $TOPIC"
$KAFKA_HOME/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic $TOPIC \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 1

echo ""
echo "✓ Kafka is ready. Topic '$TOPIC' created with 3 partitions."
echo ""
echo "Next steps:"
echo "  python producer/opensky_producer.py    ← real flights (needs internet)"
echo "  python producer/simulator_producer.py  ← synthetic flights (offline)"
echo "  spark-submit consumer/spark_consumer.py"
