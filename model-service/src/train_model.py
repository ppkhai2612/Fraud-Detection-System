"""Train a RandomForest fraud classifier on synthetic data."""

from __future__ import annotations

import math
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

SEED = 42
N_SAMPLES = 10_000
FRAUD_RATE = 0.05
MODEL_PATH = Path(__file__).parent / "model.pkl"


def generate_data(
    n_samples: int = N_SAMPLES, fraud_rate: float = FRAUD_RATE, seed: int = SEED
) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic transaction data with correlated fraud labels."""
    rng = np.random.default_rng(seed)

    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud

    # --- Legitimate transactions ---
    legit_amount = rng.lognormal(mean=3.5, sigma=1.0, size=n_legit)
    legit_tx_type = rng.integers(0, 4, size=n_legit).astype(float)
    legit_channel = rng.integers(0, 4, size=n_legit).astype(float)
    legit_acct_type = rng.integers(0, 4, size=n_legit).astype(float)
    legit_risk = rng.beta(2, 8, size=n_legit)  # skewed low
    legit_blacklist = np.zeros(n_legit)
    legit_rules = rng.beta(1, 20, size=(n_legit, 5))  # mostly near 0

    # --- Fraudulent transactions ---
    fraud_amount = rng.lognormal(mean=5.5, sigma=1.5, size=n_fraud)
    fraud_tx_type = rng.integers(0, 4, size=n_fraud).astype(float)
    fraud_channel = rng.integers(0, 4, size=n_fraud).astype(float)
    fraud_acct_type = rng.integers(0, 4, size=n_fraud).astype(float)
    fraud_risk = rng.beta(5, 3, size=n_fraud)  # skewed high
    fraud_blacklist = rng.choice([0.0, 1.0], size=n_fraud, p=[0.3, 0.7])
    fraud_rules = rng.beta(5, 2, size=(n_fraud, 5))  # mostly high

    # Combine
    amount = np.concatenate([legit_amount, fraud_amount])
    log_amount = np.array([math.log1p(a) for a in amount])
    tx_type = np.concatenate([legit_tx_type, fraud_tx_type])
    channel = np.concatenate([legit_channel, fraud_channel])
    acct_type = np.concatenate([legit_acct_type, fraud_acct_type])
    risk = np.concatenate([legit_risk, fraud_risk])
    blacklist = np.concatenate([legit_blacklist, fraud_blacklist])
    rules = np.concatenate([legit_rules, fraud_rules], axis=0)
    max_rule = rules.max(axis=1)

    features = np.column_stack(
        [log_amount, tx_type, channel, acct_type, risk, blacklist, rules, max_rule]
    )
    labels = np.concatenate([np.zeros(n_legit), np.ones(n_fraud)])

    return features, labels


def train() -> None:
    """Train model and save to disk."""
    print(f"Generating {N_SAMPLES} synthetic samples ({FRAUD_RATE:.0%} fraud rate)...")
    features, labels = generate_data()

    x_train, x_test, y_train, y_test = train_test_split(
        features, labels, test_size=0.2, random_state=SEED, stratify=labels
    )

    print(f"Training RandomForest (train={len(x_train)}, test={len(x_test)})...")
    clf = RandomForestClassifier(
        n_estimators=100, max_depth=10, random_state=SEED, n_jobs=-1
    )
    clf.fit(x_train, y_train)

    print("\nTest set performance:")
    y_pred = clf.predict(x_test)
    print(classification_report(y_test, y_pred, target_names=["legit", "fraud"]))

    joblib.dump(clf, MODEL_PATH)
    print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    train()