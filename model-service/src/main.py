"""FastAPI application for fraud detection model scoring."""

from __future__ import annotations

import time

from fastapi import FastAPI
from pydantic import BaseModel

from .features import extract_features
from .model import FraudModel

app = FastAPI(title="Fraud Detection Model Service")
model = FraudModel()


class PredictRequest(BaseModel):
    transaction_id: str
    account_id: str
    amount: float
    transaction_type: str = "PURCHASE"
    channel: str = "ONLINE"
    account_type: str | None = None
    account_status: str | None = None
    account_risk_score: float | None = None
    merchant_category: str | None = None
    merchant_risk_level: str | None = None
    is_blacklisted: bool | None = None
    rule_scores: dict[str, float] | None = None


class PredictResponse(BaseModel):
    transaction_id: str
    fraud_score: float
    model_version: str = "1.0.0"
    latency_ms: float


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    start = time.time()
    features = extract_features(request)
    score = model.predict(features)
    latency = (time.time() - start) * 1000

    return PredictResponse(
        transaction_id=request.transaction_id,
        fraud_score=round(score, 6),
        latency_ms=round(latency, 2),
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy", "model_version": "1.0.0"}