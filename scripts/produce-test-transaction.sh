#!/bin/bash
set -euo pipefail

# Produce a test Avro transaction to Kafka

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python3"

echo "Producing test transaction to Kafka..."

cd "$PROJECT_DIR"

"$PYTHON" << 'PYTHON_EOF'
import json
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Config
schema_registry_conf = {"url": "http://localhost:8081"}
kafka_conf = {"bootstrap.servers": "localhost:9092"}

# Load schema
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
with open("schemas/transaction.avsc") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda tx, ctx: tx
)

producer = Producer(kafka_conf)

# Test transaction
test_tx = {
    "transaction_id": "test-tx-001",
    "account_id": "acct-12345",
    "merchant_id": "merch-99",
    "amount": 142.50,
    "currency": "USD",
    "transaction_type": "PURCHASE",
    "channel": "ONLINE",
    "location": {
        "latitude": 33.5186,
        "longitude": -86.8104,
        "country": "US",
        "city": "Birmingham"
    },
    "timestamp": int(time.time() * 1000),
    "metadata": {"source": "test-script"}
}

producer.produce(
    topic="transactions",
    value=avro_serializer(
        test_tx,
        SerializationContext("transactions", MessageField.VALUE)
    ),
    on_delivery=lambda err, msg: print(
        f"  Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        if err is None
        else f"  Delivery failed: {err}"
    )
)
producer.flush(10)
print("Test transaction produced.")
PYTHON_EOF