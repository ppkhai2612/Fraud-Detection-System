"""Account and merchant seed data generation with behavioral profiles."""

import random
import time
from typing import Any

from . import config

# ---------------------------------------------------------------------------
# City reference data: (lat, lng, country, city_name)
# ---------------------------------------------------------------------------

CITIES = [
    (33.52, -86.81, "US", "Birmingham"),
    (40.71, -74.01, "US", "New York"),
    (34.05, -118.24, "US", "Los Angeles"),
    (41.88, -87.63, "US", "Chicago"),
    (29.76, -95.37, "US", "Houston"),
    (33.45, -112.07, "US", "Phoenix"),
    (39.95, -75.17, "US", "Philadelphia"),
    (29.42, -98.49, "US", "San Antonio"),
    (32.72, -117.16, "US", "San Diego"),
    (32.78, -96.80, "US", "Dallas"),
    (51.51, -0.13, "UK", "London"),
    (43.65, -79.38, "CA", "Toronto"),
    (52.52, 13.41, "DE", "Berlin"),
    (48.86, 2.35, "FR", "Paris"),
]

# Per-city weights: 10 US cities share 60%, then UK=15, CA=10, DE=5, FR=5
CITY_WEIGHTS = [6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 15, 10, 5, 5]

DOMESTIC_CITIES = [c for c in CITIES if c[2] == "US"]
INTERNATIONAL_CITIES = [c for c in CITIES if c[2] != "US"]

# ---------------------------------------------------------------------------
# Merchant constants
# ---------------------------------------------------------------------------

MERCHANT_CATEGORIES = [
    "electronics", "grocery", "gas_station", "restaurant", "travel",
    "clothing", "entertainment", "pharmacy", "services", "other",
]

MERCHANT_NAME_PREFIXES = {
    "electronics": ["TechZone", "ByteShop", "CircuitWorld", "GadgetHub", "DigiMart"],
    "grocery": ["FreshMart", "GreenGrocer", "DailyBasket", "QuickStop", "FarmFresh"],
    "gas_station": ["SpeedFuel", "QuickGas", "RoadRunner", "FuelUp", "PumpCity"],
    "restaurant": ["CafeBliss", "GoldenBite", "UrbanEats", "TastySpoon", "FireGrill"],
    "travel": ["SkyWay", "GlobeTrek", "FastTravel", "JetSet", "Wanderlust"],
    "clothing": ["StyleHouse", "ThreadBarn", "UrbanWear", "FitTrend", "ClassicCut"],
    "entertainment": ["FunZone", "ScreenTime", "PlayPark", "StarVenue", "BeatBox"],
    "pharmacy": ["HealthPlus", "MediCare", "WellRx", "CureAll", "QuickMeds"],
    "services": ["FixItPro", "CleanSweep", "SwiftRepair", "PrimeCare", "TopNotch"],
    "other": ["GeneralCo", "MiscMart", "AllGoods", "ValueShop", "CatchAll"],
}

CHANNELS = ["ONLINE", "IN_STORE", "MOBILE", "ATM"]

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_accounts: list[dict[str, Any]] = []
_merchants: list[dict[str, Any]] = []
_blacklisted_merchants: list[dict[str, Any]] = []
_account_profiles: dict[str, dict[str, Any]] = {}


def generate_merchants(n: int) -> list[dict[str, Any]]:
    """Generate n merchants and populate the module-level pool."""
    global _merchants, _blacklisted_merchants
    _merchants = []
    _blacklisted_merchants = []
    now_ms = int(time.time() * 1000)

    for i in range(n):
        category = random.choice(MERCHANT_CATEGORIES)
        prefix = random.choice(MERCHANT_NAME_PREFIXES[category])
        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]

        # Risk: 85% LOW, 10% MEDIUM, 5% HIGH
        risk_level = random.choices(
            ["LOW", "MEDIUM", "HIGH"], weights=[85, 10, 5]
        )[0]

        # ~1% blacklisted
        is_blacklisted = random.random() < 0.01

        merchant = {
            "merchant_id": f"merch-{i:04d}",
            "merchant_name": f"{prefix} #{i}",
            "category": category,
            "country": city[2],
            "risk_level": risk_level,
            "is_blacklisted": is_blacklisted,
            "updated_at": now_ms,
        }
        _merchants.append(merchant)
        if is_blacklisted:
            _blacklisted_merchants.append(merchant)

    # Guarantee at least 1 blacklisted merchant for fraud patterns
    if not _blacklisted_merchants:
        idx = random.randint(0, len(_merchants) - 1)
        _merchants[idx]["is_blacklisted"] = True
        _merchants[idx]["risk_level"] = "HIGH"
        _blacklisted_merchants.append(_merchants[idx])

    return _merchants


def generate_accounts(n: int) -> list[dict[str, Any]]:
    """Generate n accounts with behavioral profiles. Call after generate_merchants."""
    global _accounts, _account_profiles
    _accounts = []
    _account_profiles = {}
    now_ms = int(time.time() * 1000)

    merchant_ids = [m["merchant_id"] for m in _merchants]

    for i in range(n):
        account_id = f"acct-{i:05d}"

        account_type = random.choices(
            ["CHECKING", "CREDIT", "SAVINGS", "PREPAID"],
            weights=[50, 25, 15, 10],
        )[0]

        status = random.choices(
            ["ACTIVE", "SUSPENDED", "CLOSED"],
            weights=[95, 3, 2],
        )[0]

        # Risk score: 90% low (0.0-0.3), 10% elevated (0.5-0.9)
        if random.random() < 0.9:
            risk_score = round(random.uniform(0.0, 0.3), 2)
        else:
            risk_score = round(random.uniform(0.5, 0.9), 2)

        city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]

        account = {
            "account_id": account_id,
            "customer_id": f"cust-{i:05d}",
            "account_type": account_type,
            "status": status,
            "created_at": now_ms - random.randint(86400000, 86400000 * 365),
            "risk_score": risk_score,
            "country": city[2],
            "updated_at": now_ms,
        }
        _accounts.append(account)

        # Behavioral profile
        avg_amount = round(random.uniform(20, 500), 2)
        num_preferred = min(random.randint(3, 10), len(merchant_ids))
        channel_weights = _random_channel_weights()

        _account_profiles[account_id] = {
            "account_id": account_id,
            "avg_amount": avg_amount,
            "stddev_amount": avg_amount * 0.3,
            "channels": CHANNELS,
            "channel_weights": channel_weights,
            "preferred_merchant_ids": random.sample(merchant_ids, k=num_preferred),
            "home_location": city,
        }

    return _accounts


def _random_channel_weights() -> list[int]:
    """Generate a plausible channel weight distribution per account."""
    primary = random.choice(range(4))
    weights = [5, 5, 5, 5]
    weights[primary] = 60
    secondary = random.choice([j for j in range(4) if j != primary])
    weights[secondary] = 20
    # Remaining two share the leftover
    remaining = 100 - weights[primary] - weights[secondary]
    others = [j for j in range(4) if j not in (primary, secondary)]
    weights[others[0]] = remaining // 2
    weights[others[1]] = remaining - remaining // 2
    return weights


def get_random_account() -> dict[str, Any]:
    """Return a random account from the pool."""
    return random.choice(_accounts)


def get_random_merchant() -> dict[str, Any]:
    """Return a random merchant from the pool."""
    return random.choice(_merchants)


def get_blacklisted_merchants() -> list[dict[str, Any]]:
    """Return all blacklisted merchants."""
    return _blacklisted_merchants


def get_account_profile(account_id: str) -> dict[str, Any]:
    """Return the behavioral profile for an account."""
    return _account_profiles[account_id]


def get_all_merchants() -> list[dict[str, Any]]:
    """Return all merchants."""
    return _merchants