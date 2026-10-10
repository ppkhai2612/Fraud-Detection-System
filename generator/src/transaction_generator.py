"""Synthetic transaction generator for the fraud detection pipeline.

On startup, seeds account and merchant reference data to Kafka,
Then continuously produces transactions at a configured rate,
injecting fraud patterns at a configurable probability.
"""

import logging
import random
import signal
import time
from typing import Any
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from . import config
from . import account_profiles
from . import fraud_patterns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ================================
# Globals
# ================================

_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    logger.info(f"Received signal {signum}, shutting down...")
    _running = False


# ================================
# Avro helpers
# ================================

def _make_serializer(
    sr_client: SchemaRegistryClient, schema_path: str
) -> AvroSerializer:
    with open(schema_path) as f:
        schema_str = f.read()
    return AvroSerializer(sr_client, schema_str, lambda obj, ctx: obj)


def _delivery_callback(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error(f"Delivery failed: {err}")


# ================================
# Normal transaction generation
# ================================

TRANSACTION_TYPES = ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL"]
TRANSACTION_TYPE_WEIGHTS = [90, 5, 3, 2]


def generate_normal_transaction(profile: dict[str, Any]) -> dict[str, Any]:
    """Generate a single legitimate transaction based on the account's profile."""
    now_ms = int(time.time() * 1000)
    home = profile["home_location"]

    # Amount from the account's normal distribution, clamped positive
    amount = max(0.01, random.gauss(profile["avg_amount"], profile["stddev_amount"]))

    # Merchant: 80% from preferred, 20% random
    if random.random() < 0.8 and profile["preferred_merchant_ids"]:
        merchant_id = random.choice(profile["preferred_merchant_ids"])
    else:
        merchant_id = account_profiles.get_random_merchant()["merchant_id"]

    channel = random.choices(
        profile["channels"], weights=profile["channel_weights"]
    )[0]

    tx_type = random.choices(TRANSACTION_TYPES, weights=TRANSACTION_TYPE_WEIGHTS)[0]

    # Location: slight jitter from home (~0.01 degrees ~ 1km)
    location = {
        "latitude": round(home[0] + random.gauss(0, 0.01), 4),
        "longitude": round(home[1] + random.gauss(0, 0.01), 4),
        "country": home[2],
        "city": home[3],
    }

    return {
        "transaction_id": f"tx-{uuid4().hex[:12]}",
        "account_id": profile["account_id"],
        "merchant_id": merchant_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "transaction_type": tx_type,
        "channel": channel,
        "location": location,
        "timestamp": now_ms,
    }


# ================================
# Seeding
# ================================

def seed_accounts(
    producer: Producer,
    serializer: AvroSerializer,
    accounts: list[dict[str, Any]],
) -> None:
    """Produce all accounts to the account-updates topic."""
    logger.info(f"Seeding {len(accounts)} accounts to {config.TOPIC_ACCOUNT_UPDATES}...")
    for acct in accounts:
        producer.produce(
            topic=config.TOPIC_ACCOUNT_UPDATES,
            key=acct["account_id"],
            value=serializer(
                acct, SerializationContext(config.TOPIC_ACCOUNT_UPDATES, MessageField.VALUE)
            ),
            on_delivery=_delivery_callback,
        )
        producer.poll(0)
    producer.flush(30)
    logger.info(f"Seeded {len(accounts)} accounts.")
    

def seed_merchants(
    producer: Producer,
    serializer: AvroSerializer,
    merchants: list[dict[str, Any]],
) -> None:
    """Produce all merchants to the merchant-updates topic."""
    logger.info(f"Seeding {len(merchants)} merchants to {config.TOPIC_MERCHANT_UPDATES}...")
    for m in merchants:
        producer.produce(
            topic=config.TOPIC_MERCHANT_UPDATES,
            key=m["merchant_id"],
            value=serializer(
                m,
                SerializationContext(config.TOPIC_MERCHANT_UPDATES, MessageField.VALUE),
            ),
            on_delivery=_delivery_callback,
        )
        producer.poll(0)
    producer.flush(30)
    bl_count = sum(1 for m in merchants if m["is_blacklisted"])
    logger.info(f"Seeded {len(merchants)} merchants ({bl_count} blacklisted).")


# ================================
# Main loop
# ================================

def run() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("=== Transaction Generator ===")
    logger.info(f"Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    logger.info(f"Target TPS: {config.TRANSACTIONS_PER_SECOND}")
    logger.info(f"Fraud rate: {config.FRAUD_RATE * 100:.1f}%")

    # Schema Registry + serializers
    schemas_dir = config.find_schemas_dir()
    logger.info(f"Schemas dir: {schemas_dir}")

    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    tx_serializer = _make_serializer(sr_client, f"{schemas_dir}/transaction.avsc")
    account_serializer = _make_serializer(sr_client, f"{schemas_dir}/account.avsc")
    merchant_serializer = _make_serializer(sr_client, f"{schemas_dir}/merchant.avsc")

    # Kafka producer
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 100000,
    })

    # Generate and seed reference data
    merchants = account_profiles.generate_merchants(config.NUM_MERCHANTS)
    accounts = account_profiles.generate_accounts(config.NUM_ACCOUNTS)

    seed_merchants(producer, merchant_serializer, merchants)
    seed_accounts(producer, account_serializer, accounts)

    logger.info("Waiting 2 seconds for broadcast state propagation...")
    time.sleep(2)

    # Main transaction loop
    logger.info(f"Starting transaction generation at {config.TRANSACTIONS_PER_SECOND} TPS...")

    tps = config.TRANSACTIONS_PER_SECOND
    slot_interval = 1.0 / tps if tps > 0 else 1.0

    total_produced = 0
    total_fraud = 0
    stats_start = time.time()

    while _running:
        slot_start = time.time()

        # Pick a random account
        acct = account_profiles.get_random_account()
        profile = account_profiles.get_account_profile(acct["account_id"])

        if random.random() < config.FRAUD_RATE:
            # Fraud pattern
            txns = fraud_patterns.generate_fraud(profile)
            for tx in txns:
                producer.produce(
                    topic=config.TOPIC_TRANSACTIONS,
                    key=tx["account_id"],
                    value=tx_serializer(
                        tx,
                        SerializationContext(
                            config.TOPIC_TRANSACTIONS, MessageField.VALUE
                        ),
                    ),
                    on_delivery=_delivery_callback,
                )
                total_produced += 1
                total_fraud += 1
        else:
            # Normal transaction
            tx = generate_normal_transaction(profile)
            producer.produce(
                topic=config.TOPIC_TRANSACTIONS,
                key=tx["account_id"],
                value=tx_serializer(
                    tx,
                    SerializationContext(
                        config.TOPIC_TRANSACTIONS, MessageField.VALUE
                    ),
                ),
                on_delivery=_delivery_callback,
            )
            total_produced += 1
        
        producer.poll(0)
        
        # Stats logging every 10 seconds
        now = time.time()
        elapsed_stats = now - stats_start
        if elapsed_stats >= 10:
            rate = total_produced / elapsed_stats
            fraud_pct = (total_fraud / total_produced * 100) if total_produced > 0 else 0
            legit = total_produced - total_fraud
            logger.info(f"""
                Generated {total_produced} transactions ({rate:.0f}/s),
                {total_fraud} fraud ({fraud_pct:.1f}%), {legit} legitimate
            """)
            total_produced = 0
            total_fraud = 0
            stats_start = now

        # Rate limiting
        elapsed = time.time() - slot_start
        remaining = slot_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)
    
    # Shutdown
    logger.info("Flushing producer...")
    producer.flush(10)
    logger.info("Generator stopped.")


if __name__ == "__main__":
    run()