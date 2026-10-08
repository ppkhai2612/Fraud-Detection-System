# Designing a Fraud Detection System: From Requirements to Architecture

Link: https://medium.com/@blakelassiter/designing-a-fraud-detection-system-from-requirements-to-architecture-d2225218e402

## Requirements and Constraints

Fraud detection operates at two different timescales and confusing them leads to bad architecture decisions.
- The **authorization path** makes the approve/decline decision while the customer waits. A card swipe at a retail terminal or a checkout button on a website triggers an authorization request that needs a response in roughly 100ms. That budget includes network round-trips to the payment network, so the actual fraud check within the authorization path gets maybe 10–20ms. At that speed, you’re limited to pre-computed risk scores, simple threshold checks and cached blacklist lookups. These checks are fast but shallow.
- The **streaming pipeline** runs slightly behind the authorization path. It consumes transactions after they clear the initial authorization gate and performs richer analysis — computing rolling features across wider time windows, correlating with account history and merchant risk profiles, calling ML models for pattern detection. The streaming pipeline doesn’t block the customer. Its outputs update risk scores that the authorization path consumes on the next transaction, route flagged transactions to analyst review queues and trigger downstream actions like step-up authentication or card freezes.

Trying to run deep analysis in the authorization path blows your latency budget. Assuming the streaming pipeline needs sub-100ms latency adds unnecessary engineering complexity. The streaming pipeline targets seconds to low single-digit minutes of end-to-end latency — fast enough to catch ongoing fraud sessions without compromising analysis depth.

Beyond latency, three other constraints shape the design:
- **Throughput**. A mid-size payment processor handles thousands of transactions per second. The pipeline needs to sustain that volume continuously without building consumer lag. Kafka’s partitioning model provides horizontal scalability by distributing transactions across partitions keyed by `account_id`.
- **Accuracy tradeoffs**. Every fraud detection system navigates the tension between catching fraud and generating false positives. A system that flags everything catches all fraud but makes the product unusable. A system that flags nothing has no false positives but misses fraud entirely. The sweet spot depends on the business — how much fraud loss is acceptable vs. how much customer friction the product can tolerate. The architecture needs tunable thresholds, not hardcoded decisions.
- **Compliance**. Financial transaction data falls under PCI-DSS, which constrains where cardholder data can flow, how it must be encrypted in transit and at rest and who can access it. The architecture can’t treat every Kafka topic and processing stage identically. Sensitive fields need masking or tokenization before they reach analytics sinks or log aggregators.

## Data Model

Four Avro schemas form the backbone of the system. These are registered in [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) with BACKWARD compatibility mode, so producers can add optional fields without breaking running consumers (the evolution patterns)
- **Transaction** — the primary input event:

    ```avro
    {
        "type": "record",
        "name": "Transaction",
        "namespace": "com.frauddetection",
        "fields": [
            {"name": "transaction_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "merchant_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "currency", "type": "string", "default": "USD"},
            {"name": "transaction_type", "type": {"type": "enum", "name": "TransactionType",
            "symbols": ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL"]}},
            {"name": "channel", "type": {"type": "enum", "name": "Channel",
            "symbols": ["ONLINE", "IN_STORE", "ATM", "MOBILE"]}},
            {"name": "location", "type": ["null", {
            "type": "record",
            "name": "Location",
            "fields": [
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "country", "type": "string"},
                {"name": "city", "type": ["null", "string"], "default": null}
            ]
            }], "default": null},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null}
        ]
    }
    ```

    The `location` field is nullable because ATM transactions and some online transactions may not have geolocation. The `metadata` map allows attaching additional signals (device fingerprint, IP address, session ID) without a schema change - just add a new key. The `transaction_type` enum distinguishes purchase from refund because fraudsters often test with refunds before attempting large purchases.

- **Account** — reference data flowing from the operational database via CDC:

    ```avro
    {
        "type": "record",
        "name": "Account",
        "namespace": "com.frauddetection",
        "fields": [
            {"name": "account_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "account_type", "type": {"type": "enum", "name": "AccountType",
            "symbols": ["CHECKING", "SAVINGS", "CREDIT", "PREPAID"]}},
            {"name": "status", "type": {"type": "enum", "name": "AccountStatus",
            "symbols": ["ACTIVE", "SUSPENDED", "CLOSED"]}},
            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "risk_score", "type": ["null", "double"], "default": null},
            {"name": "country", "type": "string"},
            {"name": "updated_at", "type": "long", "logicalType": "timestamp-millis"}
        ]
    }
    ```

- **Merchant** — also CDC-sourced reference data:

    ```avro
    {
        "type": "record",
        "name": "Merchant",
        "namespace": "com.frauddetection",
        "fields": [
            {"name": "merchant_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "risk_tier", "type": {"type": "enum", "name": "RiskTier",
            "symbols": ["LOW", "MEDIUM", "HIGH"]}},
            {"name": "country", "type": "string"},
            {"name": "updated_at", "type": "long", "logicalType": "timestamp-millis"}
        ]
    }
    ```

- **FraudAlert** — the pipeline’s primary output:

    ```avro
    {
        "type": "record",
        "name": "FraudAlert",
        "namespace": "com.frauddetection",
        "fields": [
            {"name": "alert_id", "type": "string"},
            {"name": "transaction_id", "type": "string"},
            {"name": "account_id", "type": "string"},
            {"name": "alert_type", "type": {"type": "enum", "name": "AlertType",
            "symbols": ["VELOCITY", "GEO_ANOMALY", "AMOUNT_ANOMALY", "BLACKLIST", "MODEL_HIGH_RISK"]}},
            {"name": "risk_score", "type": "double"},
            {"name": "rule_scores", "type": {"type": "map", "values": "double"}},
            {"name": "model_score", "type": ["null", "double"], "default": null},
            {"name": "decision", "type": {"type": "enum", "name": "Decision",
            "symbols": ["APPROVE", "REVIEW", "DECLINE"]}},
            {"name": "explanation", "type": "string"},
            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"}
        ]
    }
    ```

    The `explanation` field matters for analyst workflows. A fraud analyst receiving an alert needs to know why it was flagged - "velocity: 8 transactions in 5 minutes" tells them something actionable. "risk_score: 0.87" does not. The `rule_scores` map preserves individual signal contributions so analysts and model retraining pipelines can see which signals drove the decision.

## Processing Topology

```
                ┌────────────────────────────────────────────┐
                │               KAFKA TOPICS                 │
                │                                            │
Transactions ───► │  transactions (partitioned by account_id)  │
                │  account-updates (CDC from PostgreSQL)     │
Databases ──CDC──►│  merchant-updates (CDC from PostgreSQL)    │
                │  alerts-high-risk                          │
                │  alerts-review                             │
                │  enriched-transactions                     │
                └──────────────────┬─────────────────────────┘
                                    │
                ┌──────────────────▼─────────────────────────┐
                │            FLINK PROCESSING                │
                │                                            │
                │  ┌────────────┐  ┌───────────┐             │
                │  │  Validate  │  │  Feature  │             │
                │  │  & Enrich  ├─►│Computation├─┐           │
                │  └────────────┘  └───────────┘ │           │
                │                                │           │
                │                      ┌─────────▼────────┐  │
                │                      │  Rule   │ Model  │  │
                │                      │  Engine │ Scorer │  │
                │                      │         │(Async) │  │
                │                      └────┬────┴───┬────┘  │
                │                           │        │       │
                │                      ┌────▼────────▼────┐  │
                │                      │   Score Fusion   │  │
                │                      └────────┬─────────┘  │
                └───────────────────────────────┼────────────┘
                                                │
                        ┌──────────────┬───────────▼─────┐
                        ▼              ▼                 ▼
                ┌──────────┐   ┌──────────┐   ┌──────────────┐
                │  Alert   │   │ Analytics│   │  Prometheus  │
                │  Topics  │   │   Sink   │   │  (Metrics)   │
                └──────────┘   └──────────┘   └──────────────┘
```

The pipeline has five logical stages, each a separate Flink operator or job.
- **Validate & Enrich** consumes raw transactions, runs input validation and joins with reference data. Account and merchant data arrive via CDC topics and are held in broadcast state so every parallel instance has access to the full reference dataset. Invalid transactions route to a dead letter queue — a dedicated topic for records that failed validation — while valid transactions emerge enriched with account status, risk tier and merchant category.
- **Feature Computation** takes enriched transactions keyed by `account_id` and maintains per-account state: rolling transaction counts, sum over time windows, average transaction amount, time since last transaction and last known location. These features feed both the rule engine and the ML model in parallel.
- **Rule Engine** and **Model Scorer** operate on the same enriched, feature-augmented stream simultaneously. The rule engine evaluates deterministic fraud rules. The model scorer calls an external ML service asynchronously using Flink’s async I/O.
- **Score Fusion** combines rule scores and model scores into a final decision: approve, review or decline.

Partitioning the transactions topic by `account_id` ensures all transactions for a given account land on the same Kafka partition and route to the same Flink task instance after keyBy. Velocity checks need to see every transaction for an account, not a random subset - this partitioning strategy makes that possible.

## Rule-Based Detection

Rules catch well-defined fraud patterns. Each rule produces a score between 0.0 and 1.0, where higher means more suspicious.

- **Velocity rules** flag accounts with unusually high transaction frequency. A sliding window of five minutes tracks transaction count per account. If the count exceeds the account’s typical frequency by a configurable multiple, the velocity score increases proportionally. The threshold isn’t a single number — an account that typically makes two transactions per day triggering eight in five minutes is very different from a high-volume corporate card doing the same.
- **Geographic anomaly detection** works differently. Rather than counting events in a window, it compares the current transaction’s location against the previous transaction’s location and the time between them. If the distance between two consecutive transactions is physically impossible given the elapsed time (the “impossible travel” check), the geo score spikes. This requires maintaining the last known location in keyed state per account. Location data can be null for some channels, so the rule skips the check when location is unavailable rather than producing a false signal.
- **Amount threshold rules** compare the transaction amount against the account’s historical pattern. A transaction that’s five times the account’s average over the last 30 days scores higher than one that’s within normal range. The reference point is the account’s own history, not a global threshold, because what’s normal for a corporate expense card is abnormal for a student’s debit card.
- **Card testing detection** targets a different pattern entirely: a series of small-value transactions (often under $5) in rapid succession, followed by a large purchase. Fraudsters use small transactions to verify that a stolen card number is active before attempting a significant charge. This rule uses a combination of the sliding window for frequency and amount state to detect the small-then-large sequence.
- Finally, **blacklist matching** checks the transaction’s merchant and account against known-bad lists. These lists are maintained in a database table, flow into Kafka via CDC and are held in broadcast state so every parallel instance can check against them without external lookups. When the fraud ops team adds a merchant to the blacklist in PostgreSQL, the CDC pipeline propagates that update to all Flink instances within seconds.

## ML-Based Detection

Rules handle patterns that humans can define explicitly. ML models handle patterns that are harder to articulate — subtle shifts in spending behavior, correlations across multiple features that no single rule captures, emerging fraud techniques that haven’t been codified into rules yet.

**Feature computation** for ML starts with the same per-account state that feeds the rule engine but extends further. The model might consume dozens of features: transaction count and sum across multiple time windows (1 hour, 24 hours, 7 days, 30 days), average transaction amount, standard deviation, time since last transaction, channel distribution (what percentage of this account’s transactions are online vs. in-store), merchant category distribution and various ratios comparing current transaction characteristics to historical baselines.

- **Online features** are computed in real-time from the streaming pipeline’s state — transaction velocity, recency, current session behavior. **Offline features** are computed in batch from the data warehouse — long-term spending patterns, customer segment, credit risk scores. The offline features are pre-computed and loaded as reference data, similar to how account profiles work in the enrichment stage.
- **Feature freshness matters**. A customer’s 30-day average transaction amount computed yesterday is probably still useful today. Their 1-hour transaction count computed an hour ago is stale. The architecture separates features by their update frequency — real-time features come from Flink state, while slower-changing features come from periodic batch computation and are loaded as reference data.
- This split between online and offline features creates a practical challenge: **keeping them synchronized**. The model was trained on a feature vector that combines both. If the offline features are stale (because the batch job hasn’t run) or the online features are missing (because the Flink job restarted and hasn’t rebuilt state from its latest checkpoint), the model receives inputs that don’t match its training distribution. The result is degraded prediction quality, not outright failure — which makes it harder to detect than a crash. Monitoring **feature completeness** and **freshness** alongside **model accuracy** is how you catch this drift before it becomes a problem.

## Model Serving Integration

**The ML model runs as a separate service, not embedded in Flink**. Data science teams have their own deployment cadence for model updates. Models may require GPU inference hardware that doesn’t match Flink’s cluster topology. Model serving frameworks (TensorFlow Serving, Triton or even a FastAPI + scikit-learn service) have their own scaling and versioning capabilities.

**Flink calls the model service using async I/O**. The async approach lets Flink continue processing other transactions while waiting for model responses, rather than blocking a thread per inference call.
- The **latency budget** for the model call is tight. If the overall streaming pipeline targets single-digit seconds of end-to-end latency, the model inference call gets roughly 50–200ms including network overhead. The async I/O configuration sets a timeout (say, 200ms) and a concurrency limit (say, 100 concurrent requests) to prevent the model service from becoming a bottleneck.
- A **fallback strategy** is just as important as the happy path, because model services go down — network partitions, deployment rollouts and endpoint failures all cause brief unavailability. When the model call times out or fails, the pipeline can’t stall. Transactions keep flowing. The fallback is rule-only scoring. The system still functions, just with less sensitivity to the patterns that only the model catches. This is a conscious tradeoff: slightly higher false negative rates during model outages vs. pipeline stalls that create fraud detection blind spots.

If the model timeout rate spikes, that’s an operational signal that needs attention — not something the system should silently absorb.

## Score Fusion and Decisioning

Each transaction now has a set of rule scores (velocity, geo, amount, card testing, blacklist) and possibly a model score. These need to combine into a single decision.

A weighted sum is a common approach. Velocity score times its weight, plus geo score times its weight and so on, with the model score weighted separately. The weights are configuration, not code — stored externally and loaded at startup so they can be tuned without redeploying the Flink job.

```java
double ruleScore = (velocityScore * config.getWeight("velocity"))
    + (geoScore * config.getWeight("geo"))
    + (amountScore * config.getWeight("amount"))
    + (cardTestScore * config.getWeight("card_test"))
    + (blacklistScore * config.getWeight("blacklist"));

double finalScore;
if (modelScore != null) {
    finalScore = (ruleScore * config.getWeight("rules_combined"))
        + (modelScore * config.getWeight("model"));
} else {
    finalScore = ruleScore;  // Rule-only fallback
}
```

The final score maps to a decision tier:
- **Approve** (score below lower threshold): Transaction passes. Updated features feed back into state for future scoring.
- **Review** (score between thresholds): Transaction is routed to the analyst review queue. An analyst examines the alert and makes a manual decision.
- **Decline** (score above upper threshold): Transaction is flagged for immediate action — account freeze, step-up authentication on next attempt or direct decline if the streaming system feeds back into the authorization path’s pre-computed scores.

Thresholds are also configuration, not hardcoded values. A more aggressive fraud team might lower the review threshold, catching more fraud but generating more analyst workload. A team optimizing for customer experience might raise it, accepting more risk in exchange for fewer false positives. The architecture supports either approach without code changes.

The `explanation` field on the FraudAlert schema carries a human-readable summary of what drove the decision. For a velocity-triggered alert, that might read: "8 transactions in 5 minutes (typical: 2 per day); geo anomaly: transactions in two countries within 30 minutes." Analysts, model retraining pipelines and regulators all depend on this field - without it, nobody can answer "why was this flagged?"

## Output Routing

Decisions flow to three destinations.
- **Alert topics** in Kafka carry fraud alerts downstream. High-risk alerts (decline tier) route to `alerts-high-risk` for immediate automated action. Review-tier alerts route to `alerts-review` for the analyst queue. Separating these into distinct topics lets downstream consumers subscribe only to what they need and apply different exactly-once guarantees per topic. A missed high-risk alert is worse than a missed review-queue entry, so the high-risk topic might use stricter delivery guarantees.
- **Analytics sink** captures every scored transaction (not just alerts) for offline analysis. This feeds the data warehouse where data science teams retrain models, fraud analysts run historical queries and compliance teams generate reports. The analytics sink doesn’t need real-time delivery. Batch loading into the warehouse every few minutes is fine, which relaxes the sink connector’s latency requirements.
- **Metrics emission** pushes operational metrics to Prometheus: transactions processed per second, alert rates by type, model latency percentiles, model timeout rates and consumer lag. These metrics power dashboards and alerting for the platform team operating the pipeline.

## Security Considerations

Financial transaction data carries regulatory obligations that shape infrastructure decisions.

Kafka security spans three layers. All communication uses **TLS** for encryption in transit — broker-to-broker replication, client-to-broker connections and Schema Registry API calls. Each component (the transaction producer, Flink jobs, sink connectors) authenticates via **SASL** with its own credentials, enabling per-component audit trails. And Kafka **ACLs** restrict which clients can produce to or consume from which topics. The Flink fraud detection job can read from `transactions` and `account-updates` but can't write to them. The transaction generator can produce to `transactions` but can't read `alerts-high-risk` - following the principle of least privilege at the topic level.

**PCI-DSS implications** go beyond encryption and access control. Cardholder data (full card number, CVV) should never appear in the streaming pipeline at all. The transaction schema deliberately omits these fields — by the time a transaction event reaches Kafka, the card number should already be tokenized upstream. Fields like `account_id` are tokens referencing the actual account, not the card number itself.
- For data that does flow through the pipeline, **masking** applies before it reaches the analytics sink. An analyst investigating a fraud pattern doesn’t need to see full account details in the warehouse. Partial masking or tokenization in the sink connector keeps the warehouse data useful for analysis without exposing raw PII (personally identifiable information).

**Audit logging** records who accessed what and when. Every configuration change to thresholds and weights is logged. Model deployments are logged. Alert dispositions by analysts are logged. When a regulator asks “how was this decision made and who reviewed it?” the audit trail provides the answer.
- In practice, audit logging for a streaming pipeline means capturing two categories of events. **Operational changes** — who modified the velocity threshold, when was the model endpoint updated, which analyst disposed of which alert — go to a dedicated audit topic in Kafka with long retention. **Data access patterns** — which Flink jobs read which topics, which analysts queried which accounts in the review tool — go to the organization’s central logging infrastructure. Treating audit as a first-class data stream (rather than application-level log files) ensures it’s durable, searchable and subject to the same delivery guarantees as the business data.