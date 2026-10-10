"""Fraud model wrapper for prediction."""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np


class FraudModel:
    """Loads a trained sklearn model and provides fraud probability predictions."""

    def __init__(self, model_path: str | None = None) -> None:
        if model_path is None:
            model_path = str(Path(__file__).parent / "model.pkl")
        self.model = joblib.load(model_path)

    def predict(self, features: np.ndarray) -> float:
        """Return fraud probability (0.0 to 1.0)."""
        proba = self.model.predict_proba(features.reshape(1, -1))
        return float(proba[0][1])