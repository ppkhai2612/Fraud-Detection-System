#!/bin/bash
set -euo pipefail

# Produce test data to Kafka for end-to-end pipeline testing.
# Includes scenarios that trigger different fraud detection rules.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="${PROJECT_DIR}/.venv/bin/python3"

echo "Producing test data for fraud detection pipeline..."

cd "$PROJECT_DIR"

"$PYTHON" << 'PYTHON_EOF'
import time
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry_conf = {"url": "http://localhost:8081"}
kafka_conf = {"bootstrap.servers": "localhost:9092"}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
producer = Producer(kafka_conf)


def make_serializer(schema_file):
    with open(f"schemas/{schema_file}") as f:
        schema_str = f.read()
    return AvroSerializer(schema_registry_client, schema_str, lambda obj, ctx: obj)


def produce(topic, serializer, value, label):
    producer.produce(
        topic=topic,
        value=serializer(value, SerializationContext(topic, MessageField.VALUE)),
        on_delivery=lambda err, msg: print(
            f"  [{label}] -> {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
            if err is None
            else f"  [{label}] FAILED: {err}"
        ),
    )
    producer.flush(10)


account_serializer = make_serializer("account.avsc")
merchant_serializer = make_serializer("merchant.avsc")
tx_serializer = make_serializer("transaction.avsc")

now_ms = int(time.time() * 1000)

# ============================================================
# Reference data
# ============================================================

print("=== Reference Data ===")

print("1. Account update (acct-12345, ACTIVE)...")
produce("account-updates", account_serializer, {
    "account_id": "acct-12345",
    "customer_id": "cust-001",
    "account_type": "CHECKING",
    "status": "ACTIVE",
    "created_at": now_ms - 86400000,
    "risk_score": 0.25,
    "country": "US",
    "updated_at": now_ms,
}, "Account")

print("2. Merchant update (merch-99, LOW risk)...")
produce("merchant-updates", merchant_serializer, {
    "merchant_id": "merch-99",
    "merchant_name": "Test Electronics",
    "category": "electronics",
    "country": "US",
    "risk_level": "LOW",
    "is_blacklisted": False,
    "updated_at": now_ms,
}, "Merchant")

print("3. Merchant update (merch-bad, BLACKLISTED)...")
produce("merchant-updates", merchant_serializer, {
    "merchant_id": "merch-bad",
    "merchant_name": "Shady Shop",
    "category": "gambling",
    "country": "XX",
    "risk_level": "HIGH",
    "is_blacklisted": True,
    "updated_at": now_ms,
}, "Blacklisted Merchant")

print("4. Waiting 3 seconds for broadcast state propagation...")
time.sleep(3)

# ============================================================
# Scenario 1: Normal transaction (should score low)
# ============================================================

print()
print("=== Scenario 1: Normal Transaction ===")
produce("transactions", tx_serializer, {
    "transaction_id": "test-normal-001",
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
        "city": "Birmingham",
    },
    "timestamp": now_ms,
    "metadata": {"source": "test-script"},
}, "Normal TX")

# ============================================================
# Scenario 2: Velocity burst (6 fast transactions)
# ============================================================

print()
print("=== Scenario 2: Velocity Burst (6 rapid transactions) ===")
for i in range(6):
    produce("transactions", tx_serializer, {
        "transaction_id": f"test-velocity-{i+1:03d}",
        "account_id": "acct-12345",
        "merchant_id": "merch-99",
        "amount": 25.00 + i,
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "channel": "ONLINE",
        "location": {
            "latitude": 33.5186,
            "longitude": -86.8104,
            "country": "US",
            "city": "Birmingham",
        },
        "timestamp": now_ms + (i * 1000),
    }, f"Velocity TX {i+1}")

# ============================================================
# Scenario 3: Blacklisted merchant
# ============================================================

print()
print("=== Scenario 3: Blacklisted Merchant ===")
produce("transactions", tx_serializer, {
    "transaction_id": "test-blacklist-001",
    "account_id": "acct-12345",
    "merchant_id": "merch-bad",
    "amount": 50.00,
    "currency": "USD",
    "transaction_type": "PURCHASE",
    "channel": "ONLINE",
    "timestamp": now_ms + 10000,
}, "Blacklist TX")

# ============================================================
# Scenario 4: Invalid transaction (dead letter)
# ============================================================

print()
print("=== Scenario 4: Invalid Transaction (dead letter) ===")
produce("transactions", tx_serializer, {
    "transaction_id": "test-invalid-001",
    "account_id": "acct-12345",
    "merchant_id": "merch-99",
    "amount": -50.00,
    "currency": "USD",
    "transaction_type": "REFUND",
    "channel": "ONLINE",
    "timestamp": now_ms + 11000,
}, "Invalid TX")

print()
print("All test data produced.")
print("Check Flink logs: docker logs flink-taskmanager 2>&1 | grep 'SCORED:'")
PYTHON_EOF