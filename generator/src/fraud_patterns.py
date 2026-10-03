"""Fraud pattern generators.

Each function takes an account profile and returns a list of transaction dicts
that exhibit fraud-specific characteristics.
"""

import random
import time
from typing import Any
from uuid import uuid4

from . import account_profiles
from . import config

INTERNATIONAL_CITIES = account_profiles.INTERNATIONAL_CITIES
DOMESTIC_CITIES = account_profiles.DOMESTIC_CITIES


def _tx_id() -> str:
    return f"tx-{uuid4().hex[:12]}"


def _make_location(
    city: tuple[float, float, str, str], jitter: float = 0.0
) -> dict[str, Any]:
    """Build a Location dict from a city tuple, with optional coordinate jitter."""
    lat = city[0] + random.gauss(0, jitter) if jitter else city[0]
    lng = city[1] + random.gauss(0, jitter) if jitter else city[1]
    return {
        "latitude": round(lat, 4),
        "longitude": round(lng, 4),
        "country": city[2],
        "city": city[3],
    }


def _pick_distant_city(
    home: tuple[float, float, str, str],
) -> tuple[float, float, str, str]:
    """Pick a city far from home. International if home is domestic and vice versa."""
    if home[2] == "US":
        pool = INTERNATIONAL_CITIES
    else:
        pool = DOMESTIC_CITIES
    return random.choice(pool)


def velocity_burst(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """5-8 rapid transactions from the same account, different merchants."""
    count = random.randint(5, 8)
    now_ms = int(time.time() * 1000)
    merchants = account_profiles.get_all_merchants()
    home = profile["home_location"]
    txns = []

    for i in range(count):
        merchant = random.choice(merchants)
        amount = max(1.0, random.gauss(profile["avg_amount"], profile["stddev_amount"]))
        channel = random.choices(
            profile["channels"], weights=profile["channel_weights"]
        )[0]

        txns.append({
            "transaction_id": _tx_id(),
            "account_id": profile["account_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": round(amount, 2),
            "currency": "USD",
            "transaction_type": "PURCHASE",
            "channel": channel,
            "location": _make_location(home, jitter=0.01),
            "timestamp": now_ms + i * 500,
        })

    return txns


def geo_anomaly(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Transaction from an impossible-travel location (>5000km from home)."""
    now_ms = int(time.time() * 1000)
    home = profile["home_location"]
    far_city = _pick_distant_city(home)
    merchants = account_profiles.get_all_merchants()
    merchant = random.choice(merchants)
    amount = max(1.0, random.gauss(profile["avg_amount"], profile["stddev_amount"]))

    return [{
        "transaction_id": _tx_id(),
        "account_id": profile["account_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": round(amount, 2),
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "channel": random.choices(
            profile["channels"], weights=profile["channel_weights"]
        )[0],
        "location": _make_location(far_city),
        "timestamp": now_ms,
    }]


def large_amount(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Single transaction 10-50x the account's average amount."""
    now_ms = int(time.time() * 1000)
    multiplier = random.uniform(10, 50)
    amount = round(profile["avg_amount"] * multiplier, 2)
    merchants = account_profiles.get_all_merchants()
    merchant = random.choice(merchants)
    home = profile["home_location"]

    return [{
        "transaction_id": _tx_id(),
        "account_id": profile["account_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": amount,
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "channel": random.choices(
            profile["channels"], weights=profile["channel_weights"]
        )[0],
        "location": _make_location(home, jitter=0.01),
        "timestamp": now_ms,
    }]


def card_testing(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """3-5 micro-transactions followed by one larger purchase."""
    now_ms = int(time.time() * 1000)
    merchants = account_profiles.get_all_merchants()
    home = profile["home_location"]
    txns = []

    micro_count = random.randint(3, 5)
    for i in range(micro_count):
        merchant = random.choice(merchants)
        txns.append({
            "transaction_id": _tx_id(),
            "account_id": profile["account_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": round(random.uniform(0.01, 1.00), 2),
            "currency": "USD",
            "transaction_type": "PURCHASE",
            "channel": "ONLINE",
            "location": _make_location(home, jitter=0.01),
            "timestamp": now_ms + i * 300,
        })

    # Larger purchase after the micro-transactions
    merchant = random.choice(merchants)
    txns.append({
        "transaction_id": _tx_id(),
        "account_id": profile["account_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": round(random.uniform(200, 1000), 2),
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "channel": "ONLINE",
        "location": _make_location(home, jitter=0.01),
        "timestamp": now_ms + micro_count * 300,
    })

    return txns


def blacklisted_merchant(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Normal-looking transaction targeting a blacklisted merchant."""
    now_ms = int(time.time() * 1000)
    bl_merchants = account_profiles.get_blacklisted_merchants()
    merchant = random.choice(bl_merchants)
    amount = max(1.0, random.gauss(profile["avg_amount"], profile["stddev_amount"]))
    home = profile["home_location"]

    return [{
        "transaction_id": _tx_id(),
        "account_id": profile["account_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": round(amount, 2),
        "currency": "USD",
        "transaction_type": "PURCHASE",
        "channel": random.choices(
            profile["channels"], weights=profile["channel_weights"]
        )[0],
        "location": _make_location(home, jitter=0.01),
        "timestamp": now_ms,
    }]


# Pattern dispatch table: (function, weight)
FRAUD_PATTERNS: list[tuple[Any, float]] = [
    (velocity_burst, config.FRAUD_VELOCITY_WEIGHT),
    (geo_anomaly, config.FRAUD_GEO_ANOMALY_WEIGHT),
    (large_amount, config.FRAUD_LARGE_AMOUNT_WEIGHT),
    (card_testing, config.FRAUD_CARD_TESTING_WEIGHT),
    (blacklisted_merchant, config.FRAUD_BLACKLIST_WEIGHT),
]


def generate_fraud(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Pick a random fraud pattern and generate transactions."""
    funcs, weights = zip(*FRAUD_PATTERNS)
    chosen = random.choices(funcs, weights=weights)[0]
    return chosen(profile)