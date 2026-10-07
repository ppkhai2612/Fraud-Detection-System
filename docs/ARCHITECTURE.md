# Architecture

## Pipeline Overview

![](images/architecture.svg)

The fraud detection pipeline is a single Flink job that reads from three Kafka topics, processes transactions through seven operator stages and writes to four output topics. Everything runs in a single TaskManager with four task slots for local development.

## Data Flow

A transaction takes this path through the pipeline:

1. **Generator** produces an Avro-encoded `Transaction` to the `transaction` topic
2. **TransactionValidator** checks for null IDs, negative amounts and timestamp sanity. Invalid records go to the `dead-letter` topic as JSON strings via a side output
3. **AccountBroadcastFunction** enriches the transaction with account data (type, status, risk score) from broadcast state
4. **MerchantBroadcastFunction** adds merchant data (category, risk level, blacklist flag) from a second broadcast state
5. The enriched transaction is written to the `enriched-transactions` topic
6. **AccountFeatureFunction** computes per-account features: rolling average, standard deviation, velocity count, time since last transaction, distance from last location


The main pipelien class is FraudDetectionPipeline in the pipeline module. It wires all stages together:

```java
// Validation with dead-letter side output
SingleOutputStreamOperator<Transaction> validTransactions = transactions
        .process(new TransactionValidator())
        .name("Transaction Validator");

// Account enrichment via broadcast join
SingleOutputStreamOperator<EnrichedTransaction> accountEnriched = validTransactions
        .keyBy(Transaction::getAccountId)
        .connect(accountBroadcast)
        .process(new AccountBroadcastFunction())
        .name("Account Enrichment");

// Feature computation, rule engine, model scoring, alert generation
DataStream<FeatureVector> features = enrichedTransactions
        .keyBy(EnrichedTransaction::getAccountId)
        .process(new AccountFeatureFunction())
        .name("Feature Computation");
```

## Enrichment Pattern

Reference data (accounts and merchants) arrives on separate Kafka topics and gets broadcast to all parallel instances of the enrichment operators. This is Flink's broadcast state pattern - every parallel subtask holds a complete copy of the lookup table.

Two sequential broadcast joins enrich each transaction:
1. **Account enrichment** - keyed by `account_id`, broadcasts `Account` records into a `MapStateDescriptor<String, Account>`. When a transaction arrives, the function looks up the account and copies `account_type`, `account_status` and `account_risk_score` onto the output.
2. **Merchant enrichment** - re-keyed by `merchant_id`, broadcasts `Merchant` records. Adds `merchant_category`, `merchant_risk_level` and `is_blacklisted`.

The two joins are sequential rather than combined because each requires a different key. Account enrichment keys the transaction stream by `account_id` and merchant enrichment re-keys by `merchant_id`. Flink's `KeyedBroadcastProcessFunction` requires a keyed stream on the non-broadcast side.

```java
public class AccountBroadcastFunction
        extends KeyedBroadcastProcessFunction<String, Transaction, Account, EnrichedTransaction> {

    public static final MapStateDescriptor<String, Account> ACCOUNT_STATE =
            new MapStateDescriptor<>("account-state", Types.STRING, new AvroTypeInfo<>(Account.class));

    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx,
                               Collector<EnrichedTransaction> out) throws Exception {
        Account account = ctx.getBroadcastState(ACCOUNT_STATE).get(tx.getAccountId());
        EnrichedTransaction enriched = buildEnrichedTransaction(tx, account);
        out.collect(enriched);
    }

    @Override
    public void processBroadcastElement(Account account, Context ctx,
                                        Collector<EnrichedTransaction> out) throws Exception {
        ctx.getBroadcastState(ACCOUNT_STATE).put(account.getAccountId(), account);
    }
} 
```

If no account or merchant data exists for a given ID, the enrichment fields are left null. The pipeline does not block waiting for reference data - it enriches with whatever is available.

## Feature Computation

## Rule Engine

### VelocityRule

Detects high transaction frequency. If more than 5 transactions occur within the 5-minute velocity window, the score starts at 0.5 and increases by 0.1 per additional transaction, capped at 1.0.

### GeoAnomalyRule

Flags impossible travel. Uses the Haversine distance and time between consecutive transactions to estimate travel speed. Speed above 900 km/h scores 0.9 (impossible travel). Distance above 500 km within 2 hours scores 0.5 (suspicious).

### AmountThresholdRule

Detects statistical outliers. Computes the z-score of the current transaction amount against the account's rolling average and standard deviation. Z-score above 3.0 scores 0.9, above 2.0 scores 0.5, above 1.5 scores 0.2.

### CardTestingRule

Identifies card testing patterns - rapid sequences of micro-transactions used to validate stolen card numbers. Amount below $1.00 with more than 3 transactions in the window scores 0.8. Amount below $5.00 with more than 5 scores 0.6.

### BlacklistRule

Checks enrichment data for known bad actors. Blacklisted merchant scores 1.0. Suspended account scores 0.7. Merchant with HIGH risk level scores 0.4. The highest matching condition wins.

## ML Integration

## Score Fusion

## Decision Routing



## Serialization

All Kafka messages use Avro with Confluent Schema Registry. Five schemas are defined in the `schemas/` directory:

| **Schema** | **Topics** |
|-|-|
| `transaction.avsc` | `transactions` |
| `account.avsc` | `account-updates` |
| `merchant.avsc` | `merchant-updates` |
| `enriched-transaction.avsc` | `enriched-transactions` |
| `fraud-alert.avsc` | `alerts-high-risk`, `alerts-review` |

`AvroSerdeFactory` in the `common` module provides generic factory methods for creating Kafka sources and sinks with the correct serialization configuration. Subject naming follows the `{topic}-value` convention.

## Checkpointing and Fault Tolerance

