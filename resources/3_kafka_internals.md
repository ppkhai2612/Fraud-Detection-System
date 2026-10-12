# Kafka Internals: What Senior Engineers Actually Need to Know

Link: https://medium.com/@blakelassiter/kafka-deep-dive-beyond-the-basics-6c822964f0ef

## How Kafka Stores Data

**Log segments**. Kafka doesn’t store a topic as a single file. Each partition is a directory containing segment files. As messages arrive, Kafka appends them to the active segment. When the segment reaches a size threshold (default 1GB) or age threshold (default 7 days), Kafka closes it and opens a new one. Old segments are what retention policies delete - Kafka removes entire segments, not individual messages.
- Each segment has an index file that maps offsets to file positions. When a consumer requests messages starting at offset 47,293, Kafka uses the index to jump directly to the right location rather than scanning from the beginning.

**Why Kafka is fast**. Kafka’s performance comes from working with the operating system rather than against it.

- Sequential I/O dominates. Appending to a log file is one of the fastest operations a disk can do. Kafka writes are sequential appends. Kafka reads are sequential scans. Random I/O - jumping around to different file positions - is slow. Kafka mostly avoids it.
- Zero-copy transfers skip unnecessary data movement. When a consumer reads messages, Kafka uses the `sendfile` system call to transfer data directly from the page cache to the network socket. The data never passes through the application layer.
- Batching amortizes overhead. Producers batch messages before sending. Brokers write batches to disk together. Consumers fetch batches of messages. The per-message overhead of network calls and disk writes gets spread across many messages.
- Page cache utilization means Kafka relies heavily on the OS file cache rather than managing memory directly. Recent writes stay in memory. Consumers reading recent data - the common case - read from cache, not disk. This is why Kafka brokers often perform better with more RAM allocated to the OS than to the JVM heap.

## Partitions: The Decisions That Matter

**Partition count is a one-way door**. You can add partitions to a topic. You cannot remove them. Adding partitions also changes which partition each key hashes to - existing data for key A stays in its original partition while new data for key A may go to a different one. For topics using key-based routing, this breaks ordering assumptions.
- The practical impact: choose partition counts carefully at topic creation. Increasing later is possible but has consequences. Decreasing requires creating a new topic and migrating data.

**Rules of thumb and when to ignore them**. A common guideline is to set partition count equal to the expected maximum number of consumers (12 partitions = 12 consumers).
- But partitions have overhead. Each partition is a directory on disk, an open file handle, memory for index structures and replication traffic. A broker handling 10,000 partitions behaves differently than one handling 100. For topics with low throughput, fewer partitions reduce operational overhead. For topics requiring high parallelism, more partitions enable more consumers.
- The target throughput matters more than the consumer count. If a single partition can handle 50 MB/s and your topic needs 200 MB/s, you need at least four partitions regardless of how many consumers you plan to run.

**Ordering is per-partition only**. Messages within a single partition maintain their order. Messages across different partitions have no ordering guarantee. If transaction A goes to partition 0 and transaction B goes to partition 1, either might be processed first even if A was produced before B.

## Consumer Groups and the Rebalancing Problem

Consumer groups are how Kafka scales consumption. Multiple consumers share the work of processing a topic, with each partition assigned to exactly one consumer in the group. Simple enough - until that assignment needs to change.

**Why rebalancing hurts**. When a consumer joins or leaves a group - or fails to send heartbeats in time - Kafka must redistribute partitions. In the classic “eager” rebalancing protocol, this meant every consumer stopped processing, gave up all its partitions and waited for new assignments. Even if only one partition needed to move, every consumer paused.
- For a group with twelve consumers processing a high-volume topic, even a brief pause during a rolling deployment caused significant lag accumulation.

**Cooperative rebalancing**. The `CooperativeStickyAssignor` (default since Kafka 3.0) improved this significantly. Instead of stopping everything, it identifies which partitions actually need to move and only revokes those. Consumers keep processing unaffected partitions during the rebalance. A twelve-consumer rolling deployment now causes twelve small partition movements rather than twelve complete redistributions.

**Static membership**. Even with cooperative rebalancing, a consumer restart looks like a new member joining - Kafka doesn’t know the restarting consumer is the same one that just left. Setting `group.instance.id` to a stable identifier (like a hostname or pod name in Kubernetes) tells Kafka to expect that consumer back. If the consumer returns before `session.timeout.ms` expires, no rebalance happens at all. The consumer picks up its previous partitions and continues.

**Kafka 4.0 and KIP-848**. The new consumer rebalance protocol in Kafka 4.0 takes this further by moving rebalancing logic from clients to the group coordinator on the broker. Consumers unaffected by a rebalance don’t pause at all - they continue processing without interruption. The protocol also allows offset commits during rebalancing, eliminating another source of processing gaps. For new deployments on Kafka 4.0+, this is the default behavior.

**Offset tracking**. Static membership controls which consumer handles a partition. Offsets control where that consumer resumes after a restart. An offset is just a number - the position of a message within a partition. When a consumer commits offset 47,293, it’s telling Kafka “I’ve processed everything up to here.” On restart, the consumer resumes from that position. The distinction between current position (where the consumer is reading) and committed offset (what’s recorded in Kafka) matters for failure recovery:
- If a consumer processes a message but crashes before committing the offset, it will reprocess that message on restart. This is at-least-once delivery - safe if processing is idempotent.
- If a consumer commits the offset but crashes before finishing processing, that message is skipped on restart. This is at-most-once delivery - potentially losing data.

Auto-commit (`enable.auto.commit=true`) commits offsets periodically in the background. It's convenient but imprecise - the commit happens on a timer, not based on actual processing completion. Manual commits after processing completes give more control but require explicit code.

**Offset reset strategies**. When a consumer group starts with no committed offsets - either a new group or after offsets expire - the `auto.offset.reset` setting determines where to start:
- `earliest`: Start from the beginning of the retained data
- `latest`: Start from new messages only, skipping existing data
- A timestamp-based reset lets you start from a specific point in time

## Replication: Surviving Failures

Replication copies data across brokers for durability. Each partition has one leader and zero or more followers. Writes go to the leader. Followers replicate from the leader. If the leader fails, a follower becomes the new leader.

**In-sync replicas**. Not all followers are equally caught up. A follower might be replicating but lagging behind due to network issues, disk slowness or high load. Kafka tracks which replicas are “in sync” - close enough to the leader to be considered current.
- The `replica.lag.time.max.ms` setting (default 30 seconds) determines how far behind a follower can fall before being removed from the in-sync replica (ISR) list. A follower that hasn't fetched new data within that window drops out of the ISR. When it catches up, it rejoins.

**The acks setting**. Replication only helps if the producer waits for it. When a producer sends a message, how many acknowledgments does it wait for? The choice depends on what you can afford to lose.
- `acks=0` is fire-and-forget - maximum throughput, no durability guarantee. Fine for metrics where occasional loss is tolerable.
- `acks=1` waits for the leader to confirm
- `acks=all` waits for every in-sync replica to confirm. The message survives any single broker failure. For data that can't be lost, this is the only setting that makes sense.

**The min.insync.replicas setting**. There’s a gap in that protection. With `acks=all`, the producer waits for all ISR members to acknowledge. But what if the ISR has shrunk to just the leader? Then `acks=all` degrades to `acks=1` - only the leader confirms.
- The `min.insync.replicas` setting (configured per topic) prevents this. If set to 2, Kafka requires at least two replicas in the ISR before accepting writes. With only the leader in sync, producers receive a "not enough replicas" error instead of silently losing durability.

**The ISR cascade**. This was broker slowness.
- During a traffic spike, follower replicas fell behind. They were running but couldn’t keep up with replication traffic. The ISR shrank from three replicas to two, then to one. With `min.insync.replicas=2`, producers started failing - not because a broker died, but because only the leader was in sync.
- The fix was adding broker capacity and tuning `replica.lag.time.max.ms` to be less aggressive about dropping followers from the ISR.

**Unclean leader election**. When a leader fails, Kafka normally promotes a follower from the ISR. But what if the ISR is empty except for the failed leader? By default, the partition becomes unavailable - no reads or writes until a replica catches up.
- The setting `unclean.leader.election.enable` allows promoting an out-of-sync replica. This restores availability but risks data loss - the new leader may be missing messages that the old leader had. For most production use cases, leaving this disabled (the default) is correct.

## Retention and Compaction: How Long Data Lives

Retention handles a different problem in Kafka: data can’t stored forever, and old data has costs - storage, compaction overhead, longer recovery times. The question is what to delete and when.

**Time and size-based retention**. The default behavior deletes old segments based on time (`retention.ms`, default 7 days) or size (`retention.bytes`, no limit by default).
- Deletion happens at the segment level. If retention.ms is 7 days and the active segment is 3 days old, messages from 8 days ago in a closed segment get deleted while 3-day-old messages in the active segment remain. This is why very short retention periods can still retain more data than expected if segment rolling is slow.

**Log compaction**. Some topics don’t need historical event sequences - they need the latest value for each key. An account status topic, for example, might receive updates whenever status changes. For querying current status, only the most recent update per account matters.
- Log compaction keeps the latest record per key and discards older versions. The topic becomes a snapshot that converges to current state. CDC streams often use compaction - downstream consumers need current row values, not the full history of every update.
- Compaction runs in the background on closed segments. Active segments aren’t compacted, so recent duplicates may exist until the segment rolls. This is usually fine - consumers catching up will see the latest value soon enough.
- Deletes require special handling. A record with a null value - a tombstone - signals that the key should be removed. Kafka retains tombstones for a configurable period (`delete.retention.ms`) so consumers see the deletion, then removes them. If a consumer is offline longer than that window, they may miss the delete entirely and retain stale data.

**Choosing between them**. The choice comes down to what consumers need.
- If they’re processing events in order - analyzing transactions, replaying logs, training models on historical data - they need the full sequence. Time-based retention with enough history for your replay and debugging scenarios.
- If consumers just need current state - what’s this account’s status, what’s this entity’s latest value - they don’t care about every historical update. Compaction keeps only the most recent value per key, which is exactly what CDC streams and KTable bootstrapping require.
- Some patterns use both: time-based retention for recent change history plus compaction to guarantee current state is always available. The topic retains 7 days of changes while also keeping the latest value per key regardless of age.

## Configuration for Production

**Producer settings**.

- For durability, `acks=all` - covered above.
- Preventing duplicates on retry is the next concern. Network errors create an awkward situation: did the broker receive the message or not? The producer retries to be safe, potentially creating duplicates. `enable.idempotence=true` lets the broker detect and discard these duplicate retries. This is required for exactly-once semantics.
- Retry limits rarely need tuning. The default retry count is astronomically high (2147483647) but bounded by `delivery.timeout.ms` (default 2 minutes). Messages that can't be delivered within that window fail. The defaults work for most cases.
- Throughput versus latency is where tuning pays off. Producers batch messages before sending, controlled by `batch.size` (default 16KB) and `linger.ms` (default 0). With `linger.ms=0`, each message ships immediately - lowest latency, but every message is its own network call. Bumping this to 5-10ms lets the producer collect more messages per batch, often improving throughput significantly with minimal latency impact.

**Consumer settings**.

- Offset commits need explicit thought. `enable.auto.commit=true` (the default) commits offsets periodically in the background - convenient, but the commit happens on a timer, not when processing actually completes. Disabling auto-commit and committing manually after successful processing gives you control over exactly-once semantics.
- Batch size affects both throughput and stability. `max.poll.records` (default 500) limits how many records a single `poll()` returns. The interaction with `max.poll.interval.ms` (default 5 minutes) is where things get tricky - more on that below.
- Failure detection has two timeouts. `session.timeout.ms` (default 45 seconds) is how long Kafka waits for heartbeats before declaring a consumer dead. `heartbeat.interval.ms` (default 3 seconds) is how often the consumer sends them. For static membership, set `session.timeout.ms` high enough to cover expected restart times - otherwise your deployment triggers the very rebalancing you're trying to avoid.

**The max.poll interaction**.

- A common misconfiguration: `max.poll.records` is high, processing is slow and `max.poll.interval.ms` is at the default. The consumer fetches 500 records, takes 6 minutes to process them and gets kicked out of the group after 5 minutes because it didn't poll in time. The consumer then rejoins, fetches another 500 records and the cycle repeats. ("consumer keeps rejoining" issues)
- The fix is either reducing `max.poll.records` to what can be processed within the interval, increasing `max.poll.interval.ms` to accommodate processing time, or making processing faster.

## Applying This to Fraud Detection

**Transaction topic configuration**.

```bash
kafka-topics.sh --create --topic transactions \
    --partitions 12 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --config min.insync.replicas=2
```

- Twelve partitions support twelve parallel consumers for scaling the fraud detection pipeline. Replication factor 3 with `min.insync.replicas=2` means writes survive any single broker failure. Seven-day retention provides enough history for debugging and replay without excessive storage costs.
- Partitioning by `account_id` keeps each customer's transactions together. The partition key is set at the producer level - every transaction for account A goes to the same partition, preserving order for that account's velocity calculations.

**Alert topic configuration**. Alert topics can use shorter retention (alerts are processed quickly) and might partition by severity or alert type rather than account. The exact configuration depends on downstream consumers — an alerting system might want all high-severity alerts in one partition for priority processing.

**Producer configuration (Python)**:

```python
producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'acks': 'all',
    'enable.idempotence': True,
    'retries': 10,
    'max.in.flight.requests.per.connection': 5,
    'linger.ms': 5
}
```
- `acks=all` with idempotence enabled provides exactly-once producer semantics. The `linger.ms` of 5 adds minimal latency while improving batching efficiency.

**Consumer configuration (Python)**:

```python
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'fraud-detection',
    'group.instance.id': f'fraud-consumer-{hostname}',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
    'max.poll.records': 100,
    'session.timeout.ms': 60000
}
```
- Static membership via `group.instance.id` prevents rebalancing storms during deployments. Manual offset commits (`enable.auto.commit=False`) allow committing after successful processing. A lower `max.poll.records` keeps processing batches manageable within the poll interval.

**Diagnostic commands**. When something goes wrong, these commands help identify the issue:

```bash
# Check consumer group lag - are consumers keeping up?
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
    --describe --group fraud-detection

# Check topic configuration - retention, replication settings
kafka-configs.sh --bootstrap-server kafka:9092 \
    --describe --topic transactions --all

# Check partition and ISR status - replication health
kafka-topics.sh --bootstrap-server kafka:9092 \
    --describe --topic transactions
```
- Consumer lag growing over time indicates consumers aren’t keeping up — either processing is too slow or there aren’t enough consumers.
- ISR counts below replication factor indicate replication lag — followers not keeping up with the leader.