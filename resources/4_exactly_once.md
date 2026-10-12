# The Exactly-Once Problem in Streaming Pipelines

Link: https://medium.com/@blakelassiter/the-exactly-once-problem-in-streaming-pipelines-175344cd9628

The fraud detection pipeline looked bulletproof. Idempotent producer with `acks=all`. Flink checkpointing every 60 seconds. Alerts wrote to two destinations: Kafka for downstream consumers and Postgres for the review queue dashboard. Every configuration followed the documentation.

A downstream team flagged duplicate alerts. Same transaction flagged twice, identical timestamps, identical scores. We expected the Kafka topic to show the problem — it was the more complex sink. Instead, Kafka was clean. Postgres had two rows per alert.

The investigation traced the gap to checkpoint recovery. The Flink job crashed after a checkpoint completed but before the full commit cycle finished. On recovery, it replayed from the checkpoint and wrote to both sinks again. The Kafka sink was transactional — its writes committed atomically with the checkpoint. The JDBC sink wasn’t configured for two-phase commit. It had written immediately, before the transaction boundary closed. Those writes weren’t part of the atomic commit, so recovery wrote them again.

The fix took two lines of SQL — changing INSERT to an upsert with `alert_id` as key. But the lesson was bigger. "Exactly-once" isn't a checkbox you enable on Flink. It requires every component in the chain to participate in the protocol - or be designed to handle duplicates gracefully.

## Why This Is Hard

Recall the Two Generals problem — the theoretical reason that perfect coordination across distributed systems is impossible. The practical version is simpler: network calls can fail after the work is done but before confirmation arrives.

Three scenarios create duplicates or gaps:
- **Producer retry duplicates**. A producer sends a message. The broker receives it and writes it to disk. The acknowledgment gets lost in transit. The producer times out, retries and the broker receives the message again. The topic now has two copies.
- **Consumer gap from early commit**. A consumer reads a message, commits its offset and then crashes before finishing the downstream write. On restart, the consumer picks up from the committed offset — after the message it never finished processing. That message is gone.
- **Consumer duplicate from late commit**. A consumer reads a message, writes to a downstream database and then crashes before committing its offset. On restart, the consumer reads the same message again and writes to the database again. Duplicate.

The challenge is that achieving exactly-once requires explicit coordination across every component — producers, brokers, stream processors and sinks.

## The Delivery Guarantee Spectrum

The three delivery guarantee levels each fit different scenarios:
- **At-most-once**. Fire and forget. The producer sends without waiting for acknowledgment. If something fails, the message is lost. No retries, no duplicates, no guarantees.

    - When acceptable: metrics, logs, telemetry where occasional loss doesn’t matter. A dropped CPU reading doesn’t break monitoring. Implementation is simple — acks=0, no retry logic, minimal overhead.

- **At-least-once**. Retry until acknowledged. The producer keeps sending until the broker confirms receipt. If the acknowledgment gets lost, the retry creates a duplicate. The message is guaranteed to arrive — possibly more than once.

    - When acceptable: most workloads where downstream systems can handle duplicates. This is the default for Kafka producers with `acks=all` and retries enabled.

- **Exactly-once**. Each record affects the output exactly once. No losses, no duplicates. Required for financial transactions, billing systems, anything where duplicates have real-world consequences.

    - Exactly-once comes in two flavors: true transactional coordination across all components, or at-least-once with idempotent handling that makes duplicates invisible. Both achieve “effectively exactly-once” semantics. The implementation differs significantly.

## Kafka’s Exactly-Once Mechanisms

Kafka provides two mechanisms that work together:

- [Idempotent producers](https://kafka.apache.org/43/design/design/#message-delivery-semantics). Setting `enable.idempotence=true` makes the producer idempotent - meaning the same operation can be applied multiple times with the same result. Kafka achieves this by assigning sequence numbers to messages. The broker tracks sequences per producer session and partition. If a retry sends a message the broker has already seen, the broker discards the duplicate and returns success. The producer never knows whether the original or retry succeeded - from its perspective, the message was delivered exactly once.

    - Idempotence handles producer retry duplicates within a single partition. It doesn’t help with cross-partition coordination or failures at other points in the pipeline.

- [Transactional producers](https://kafka.apache.org/43/design/design/#the-producer). For atomic writes across multiple partitions or topics, Kafka supports transactions. A transactional producer groups writes into a unit that either commits entirely or aborts entirely.

    - The `transactional.id` is crucial. It identifies this producer across restarts. If the producer crashes mid-transaction, Kafka uses this ID to fence old transactions and prevent duplicate commits when the producer restarts.
    - **Consumer isolation levels**. Transactional writes only provide exactly-once if consumers read with `isolation.level=read_committed`. With the default `read_uncommitted`, consumers see messages from transactions that might later abort. For exactly-once, both sides must participate.
    - **The boundary of Kafka’s guarantees**. Kafka transactions guarantee exactly-once within Kafka — messages written atomically to one or more topics. External systems aren’t part of that transaction. Writing to Kafka and Postgres in the same code block doesn’t make them atomic. Coordinating writes to external systems requires something beyond Kafka’s transaction scope — which is what Flink’s checkpointing provides.

```python
from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'enable.idempotence': True,
    'transactional.id': 'fraud-alerts-producer',
    'acks': 'all'
}

producer = Producer(producer_config)
producer.init_transactions()

try:
    producer.begin_transaction()
    
    # Both writes commit atomically or neither does
    producer.produce('fraud-alerts', key=alert_id, value=alert_data)
    producer.produce('audit-log', key=alert_id, value=audit_data)
    
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    raise
```

## Flink’s Exactly-Once Mechanisms

Flink extends exactly-once semantics beyond Kafka through checkpointing and the two-phase commit protocol.

[Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/). Flink periodically snapshots the state of every operator plus the positions in source streams. On recovery, it restores state from the snapshot and replays from those source positions. Processing resumes as if the failure never happened — but only if downstream writes coordinate with checkpoints.

Checkpoint interval balances two concerns. More frequent checkpoints mean less data to replay after failure. But checkpoints have overhead — they pause processing briefly and write state to durable storage. Sixty seconds is a common starting point.

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode

env = StreamExecutionEnvironment.get_execution_environment()

# Checkpoint every 60 seconds
env.enable_checkpointing(60000)

# Exactly-once mode
env.get_checkpoint_config().set_checkpointing_mode(
    CheckpointingMode.EXACTLY_ONCE
)

# Minimum 30 seconds between checkpoints
env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)

# Checkpoint must complete within 2 minutes
env.get_checkpoint_config().set_checkpoint_timeout(120000)
```

Checkpointing snapshots Flink’s internal state, but what about data already written to external systems? That’s where two-phase commit comes in. Two-phase commit is a protocol where participants first vote “prepared” (ready to commit) and then, only after all votes succeed, actually commit. This ensures all-or-nothing behavior across multiple systems. Sinks must participate in this protocol to be part of Flink’s exactly-once guarantees.

The sequence for a transactional sink:
1. Flink processes records and writes to the sink (uncommitted)
2. Checkpoint triggers
3. Sink votes “prepared” — it can commit if asked
4. All operators complete their checkpoint snapshots
5. Checkpoint succeeds
6. Sink commits its prepared writes

If Flink crashes between steps 4 and 6, the checkpoint is incomplete. On recovery, Flink doesn’t tell the sink to commit — the prepared writes are rolled back. If Flink crashes after step 6, the checkpoint is complete and the writes are durable. Recovery starts from the next checkpoint.

**The Kafka sink is transactional by default**. Flink’s Kafka sink participates in two-phase commit automatically when exactly-once checkpointing is enabled. Writes to Kafka commit atomically with checkpoints.

**JDBC sinks require explicit configuration**. This is where the opening incident happened. The JDBC sink in Flink can support exactly-once, but it requires database support for [XA (eXtended Architecture)](https://en.wikipedia.org/wiki/X/Open_XA) transactions — the industry standard for distributed two-phase commit — and explicit configuration. Without that configuration, it writes immediately — not as part of the two-phase commit.

## End-to-End Exactly-Once

True end-to-end exactly-once requires understanding what’s inside the transaction boundary and what’s outside.

- **Inside the boundary** — components that can coordinate with Flink’s checkpoint protocol:

    - Kafka consumer offset commits (Flink commits these atomically with state)
    - Flink operator state (snapshotted during checkpoint)
    - Transactional sinks like Kafka or properly configured JDBC (they hold writes until checkpoint completes)

- **Outside the boundary** — anything that commits before the checkpoint finalizes:

    - Non-transactional sinks (they write immediately)
    - External API calls (no rollback mechanism)
    - Side effects that can’t be undone (emails sent, webhooks fired, SMS delivered)

The hard truth: full transactional coordination across all sinks is rare in practice. Not all databases support XA transactions. APIs don’t have rollback endpoints. Email can’t be unsent.

Even where it’s possible, transactional coordination has overhead:
- Latency increases from coordination round-trips
- Throughput may decrease during checkpoint alignment
- Configuration complexity grows
- More failure modes to understand and debug

For financial transactions, billing and fraud decisions, the overhead is worth it. But for analytics pipelines or logging, at-least-once with idempotent handling is often simpler and sufficient.

## Idempotency as Alternative

When two-phase commit isn’t practical or necessary, making sinks idempotent achieves “effectively exactly-once” semantics with simpler implementation.

**Pattern 1: Upsert with natural key.**

```sql
-- If the same alert_id arrives twice, the second write does nothing.
INSERT INTO fraud_alerts (alert_id, transaction_id, score, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (alert_id) DO NOTHING;
```

**Pattern 2: Deduplication with processed table.**

```sql
-- Check if already processed
SELECT 1 FROM processed_transactions WHERE transaction_id = ?;

-- If not found, insert to both tables
INSERT INTO processed_transactions (transaction_id, processed_at) 
VALUES (?, NOW());
INSERT INTO fraud_results (...)
VALUES (...);
```

- This works when the natural key isn’t available at the destination. Track what you’ve processed and skip duplicates.

**Pattern 3: Flink-side deduplication.**

```sql
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

class DeduplicatingFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        self.seen = runtime_context.get_state(
            ValueStateDescriptor('seen', Types.BOOLEAN())
        )
    
    def process_element(self, value, ctx):
        if self.seen.value():
            return  # Already processed
        
        self.seen.update(True)
        yield value
```

- Deduplicate before writing to sinks. The state tracks which keys have been processed. Duplicates from checkpoint recovery get filtered before reaching external systems.

## Resolving the Opening Incident

Two solutions:
- **Option 1: Configure two-phase commit on JDBC sink**. [Postgres supports XA transactions](https://www.postgresql.org/docs/current/two-phase.html). Flink’s JDBC sink can participate in exactly-once semantics with the right configuration. The sink holds writes until checkpoint completes, then commits.
- **Option 2: Make writes idempotent**. Change the INSERT to an upsert with `alert_id` as key. Duplicates overwrite themselves. The table always reflects current state.

We chose option 2. The review queue dashboard didn’t need transactional coordination — it just needed accurate current state. Making writes idempotent was simpler and faster than configuring XA transactions. The duplicates became harmless overwrites.

Exactly-once is achievable when you need it. Understanding when you actually need it — versus when idempotent design is sufficient — is part of the expertise.

## Applying This to Fraud Detection

The fraud detection system uses a mixed approach:
- **Idempotent Kafka producer** (`enable.idempotence=true`) prevents retry duplicates
- **Transactional Kafka writes** for multi-topic alerts (fraud-alerts and audit-log commit together)
- **Flink checkpointing** every 60 seconds with exactly-once mode
- **Transactional Kafka sink** for downstream consumers
- **Idempotent writes to Postgres** using upsert with `transaction_id` as key

This mixed approach reflects the principle from the opening incident: use transactional coordination where it’s available and valuable (Kafka-to-Kafka), and design for idempotency where it’s simpler (Postgres dashboard).

Not every sink needs transactional coordination. Analytics pipelines and logging often work well with at-least-once delivery and idempotent handling. The decision depends on what happens if a duplicate gets through — and whether the overhead of two-phase commit is worth preventing it.