"""Feature extraction from prediction requests."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from .main import PredictRequest

# Ordinal encoding maps
TRANSACTION_TYPES = {"PURCHASE": 0, "WITHDRAWAL": 1, "TRANSFER": 2, "REFUND": 3}
CHANNELS = {"ONLINE": 0, "POS": 1, "ATM": 2, "MOBILE": 3}
ACCOUNT_TYPES = {"CHECKING": 0, "SAVINGS": 1, "CREDIT": 2, "PREPAID": 3}

RULE_NAMES = ["velocity", "geo_anomaly", "amount_threshold", "card_testing", "blacklist"]


def extract_features(request: PredictRequest) -> np.ndarray:
    """Convert a prediction request into a numeric feature vector.
    
    Feature order (12 features):
      0: log(amount + 1)
      1: transaction_type (ordinal)
      2: channel (ordinal)
      3: account_type (ordinal)
      4: account_risk_score
      5: is_blacklisted (0/1)
      6-10: rule scores (velocity, geo_anomaly, amount_threshold, card_testing, blacklist)
      11: max rule score
    """
    rule_scores = request.rule_scores or {}

    features = [
        math.log1p(request.amount),
        float(TRANSACTION_TYPES.get(request.transaction_type, 0)),
        float(CHANNELS.get(request.channel, 0)),
        float(ACCOUNT_TYPES.get(request.account_type or "", 0)),
        request.account_risk_score or 0.0,
        1.0 if request.is_blacklisted else 0.0,
    ]

    rule_values = [rule_scores.get(name, 0.0) for name in RULE_NAMES]
    features.extend(rule_values)
    features.append(max(rule_values) if rule_values else 0.0)

    return np.array(features, dtype=np.float64)