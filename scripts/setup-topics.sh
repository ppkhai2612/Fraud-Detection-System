#!/bin/bash
set -euo pipefail

echo "Creating Kafka topics..."

declare -A TOPICS
TOPICS=(
    ["transactions"]=6
    ["enriched-transactions"]=6
    ["account-updates"]=3
    ["merchant-updates"]=3
    ["alerts-high-risk"]=3
    ["alerts-review"]=3
    ["dead-letter"]=1
)

for TOPIC in "${!TOPICS[@]}"; do # ! indicates to get only keys
    PARTITIONS=${TOPICS[$TOPIC]}
    echo "  Creating topic: $TOPIC (partitions: $PARTITIONS)"
    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --if-not-exists
done

echo
echo "Verifying topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

echo
echo "Topic creation completed."