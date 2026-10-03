"""Generator configuration from environment variables."""

import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TRANSACTIONS_PER_SECOND = int(os.getenv("TRANSACTIONS_PER_SECOND", "100"))
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.02"))

# Topics
TOPIC_TRANSACTIONS = "transactions"
TOPIC_ACCOUNT_UPDATES = "account-updates"
TOPIC_MERCHANT_UPDATES = "merchant-updates"

# Seed data sizes
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", "1000"))
NUM_MERCHANTS = int(os.getenv("NUM_MERCHANTS", "200"))

# Fraud pattern probabilities (within the FRAUD_RATE allocation).
# These should roughly sum to 1.0.
FRAUD_VELOCITY_WEIGHT = 0.25
FRAUD_GEO_ANOMALY_WEIGHT = 0.20
FRAUD_LARGE_AMOUNT_WEIGHT = 0.25
FRAUD_CARD_TESTING_WEIGHT = 0.15
FRAUD_BLACKLIST_WEIGHT = 0.15


def find_schemas_dir() -> str:
	"""Locate the Avro schemas dir."""
	env = os.getenv("SCHEMAS_DIR")
	if env and os.path.isdir(env):
		return env
	# Docker: /app/schemas
	if os.path.isdir("schemas"):
		return "schemas"
	# Local from generator/: ../schemas
	if os.path.isdir("../schemas"):
		return "../schemas"
	raise RuntimeError("Cannot find schemas directory. Set SCHEMAS_DIR env var.")