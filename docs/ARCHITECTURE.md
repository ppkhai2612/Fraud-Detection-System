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
6. 


## Enrichment Pattern

Reference data (accounts and merchants) arrives on separate Kafka topics and gets broadcast to all parallel instances of the enrichment operators. This is Flink's broadcast state pattern - every parallel subtask holds a complete copy of the lookup table.

Two sequential broadcast joins enrich each transaction:
1. **Account enrichment** - keyed by `account_id`, broadcasts `Account` records into a `MapStateDescriptor<String, Account>`. When a transaction arrives, the function looks up the account and copies `account_type`, `account_status` and `account_risk_score` onto the output.
2. **Merchant enrichment** - re-keyed by `merchant_id`, broadcasts `Merchant` records. Adds `merchant_category`, `merchant_risk_level` and `is_blacklisted`.

The two joins are sequential rather than combined because each requires a different key. Account enrichment keys the transaction stream by `account_id` and merchant enrichment re-keys by `merchant_id`. Flink's `KeyedBroadcastProcessFunction` requires a keyed stream on the non-broadcast side.

```java

```

If no account or merchant data exists for a given ID, the enrichment fields are left null. The pipeline does not block waiting for reference data - it enriches with whatever is available.

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