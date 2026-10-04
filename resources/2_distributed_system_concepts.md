# The Distributed Systems Concepts That Actually Matter for Streaming

Link: https://medium.com/@blakelassiter/distributed-systems-foundations-for-streaming-6fcea36e17cf

Streaming systems are distributed systems. That means facing a series of engineering challenges: How do you store an unbounded stream of events? How do you scale beyond one machine? How do you survive machine failures? How do you keep copies consistent? The concepts in this article - the log, partitioning, replication and consistency - each address one of these problems.

## The Log: Storing Unbounded Event Streams

Kafka’s core abstraction is the append-only log. New records get added to the end, old records stay where they are and nothing gets modified in place.
- **Why append-only works**. Sequential writes are fast - disks are optimized for them. There’s no coordination overhead from concurrent updates to the same record. And because nothing changes after it’s written, the log becomes a reliable source of truth. If something goes wrong downstream, you can replay from any point.
- **Offsets as position**. Each record in the log has an offset - just a number indicating its position. A consumer’s progress is nothing more than “I’ve processed up to offset 47,293.” Resuming after a failure means starting from the last committed offset. Replaying historical data means resetting to an earlier offset.
- **Immutability enables debugging**. When a processing bug corrupts derived data, the source log is unaffected. Fix the bug, replay the events, rebuild the correct state. But it only works if the log still contains the data you need. For example, after discovering a bug in our IoT anomaly detection logic, we needed to reprocess three days of sensor readings to identify which alerts were false positives. Our retention was set to 48 hours. The data was already gone. Retention configuration determines whether replay is possible.
- **Log compaction**. Not all use cases need infinite history of every event. Sometimes you just need the latest state for each key - the current balance, the most recent address, the active configuration. Log compaction keeps the most recent record per key while discarding older versions. It’s useful for CDC streams and state snapshots, where historical values don’t matter but current state does.

## Partitioning: Scaling Beyond One Machine

> The append-only log handles storage elegantly. But a single log on a single machine eventually hits limits - disk capacity, write throughput, read parallelism. Scaling requires splitting the log across machines.

Partitioning splits the log into multiple independent segments. Each partition is its own ordered log and together they form the topic. Kafka scales not by making individual partitions bigger but by adding more of them.

- **Parallelism through partitions**. Each partition can be handled by a different consumer in a consumer group. Ten partitions means up to ten consumers processing in parallel. Partitions are the unit of scale in Kafka — you add partitions and consumers together to increase throughput.
- **Ordering within, not across**. Events in a single partition maintain their order. Events across different partitions have no ordering guarantee. If transaction A and transaction B go to different partitions, either could be processed first.
- **Partition key selection**. How Kafka decides which partition gets each record depends on the key. Same key always goes to same partition, assuming partition count doesn’t change.

    - For fraud detection, `account_id` as partition key means all of a customer's transactions land in the same partition. That keeps related data together for stateful processing - velocity checks, pattern detection, anything that needs transaction history - without coordinating across partitions.

- **The hot spot problem**. Even distribution assumes keys are roughly equal in volume. They often aren’t. For example, a malfunctioning sensor generated 300x normal volume. Every one of those readings hashed to the same partition. That partition’s consumer fell behind while others had spare capacity. Consumer lag grew. Alerts fired. Adding partitions wouldn’t help because the same device would still hash to one partition.

    - The options aren’t great: composite keys that add randomness but break ordering, separate handling for high-volume sources or accepting uneven load. Each has tradeoffs. The point is understanding why the problem happens so you can make informed decisions.

- **Partition count is mostly permanent**. You can add partitions to a topic but you can’t reduce them. Adding partitions changes which partition each key maps to - suddenly `device_id` A's new readings go to a different partition than its historical ones.

    - A real case is that three partitions to handle growth once. Then every consumer in the group stopped processing while Kafka redistributed assignments. At large volume, even a 30-second pause meant tens of thousands of messages queued up, and clearing that backlog while new messages kept arriving took longer than the pause itself.

## Replication: Surviving Machine Failures

> Partitioning spreads load across machines. But each partition still lives on one machine. When that machine fails, the partition is unavailable - or lost entirely if the disk fails.

Replication creates copies of each partition on multiple machines. If one fails, others have the data. It helps Kafka achieves durability.

- **Leader-follower model**. Each partition has one leader and one or more followers. All writes go to the leader. Followers replicate from the leader. When clients read, they typically read from the leader, though follower reads are now supported for some use cases.
- **In-sync replicas (ISR)**. Not all followers are equally caught up. A follower might be replicating but lag behind due to network issues or load. Kafka tracks which replicas are “in sync” - close enough to the leader to be considered current. The ISR list shrinks and grows dynamically based on replication health.
- **The acks tradeoff**. When a producer sends a message, how long does it wait for acknowledgment?

    - With `acks=0`, the producer doesn't wait at all. Fire and forget. Maximum throughput, no durability guarantee.
    - With `acks=1`, the producer waits for the leader to write it. Fast, but if the leader fails before replication, data is lost.
    - With `acks=all`, the producer waits for all in-sync replicas to confirm. Slower, but data survives any single node failure.

- **Leader election**. When a leader fails, Kafka promotes a follower from the ISR to become the new leader. Clients discover the new leader through metadata updates. This happens automatically but not instantaneously - there’s a brief period where writes to that partition fail. Understanding this helps debug the “occasional write timeout”
- **min.insync.replicas**. This setting determines how many replicas must acknowledge before `acks=all` is satisfied. With three replicas and `min.insync.replicas=2`, a write succeeds if the leader and at least one follower confirm. If only the leader is in the ISR because followers are lagging, writes fail rather than risk losing data. It's the safety net for the safety net.

## Consistency: The Tradeoffs of Multiple Copies

> Replication protects against failure by maintaining copies. But copies can diverge, even briefly. When they do, you’re facing the consistency problem.

When a producer writes to the leader, there’s a window before followers have the same data. If a client reads from a lagging follower, they see stale data. If the leader fails during that window, the latest writes might not exist on the new leader. Distributed systems force you to make tradeoffs here.

- **Strong versus eventual consistency**. Strong consistency means every read returns the most recent write. Eventual consistency means reads might be stale but will eventually catch up. Most streaming systems - including Kafka - are eventually consistent. For many use cases, this is fine. Analytics on transaction data doesn’t need to include transactions from the last 50 milliseconds. But for some use cases - like fraud decisions that depend on the most recent transaction - it matters.
- **CAP theorem in practice**. The CAP theorem says distributed systems can provide at most two of three properties - Consistency, Availability and (network) Partition tolerance.

    - Network partitions - when parts of a distributed system lose connectivity with each other - happen in real systems. A datacenter loses connectivity, a switch fails, a cable gets unplugged. You can’t choose “no network partitions.” So the real choice is whether to sacrifice consistency by allowing reads and writes that might be stale or conflicting, or sacrifice availability by refusing requests until connectivity is restored.
    - Kafka chooses availability and partition tolerance. During a network split, clients can still read and write to whichever Kafka partition leaders they can reach. When connectivity is restored, replicas sync up. This is usually the right choice for streaming - you don’t want your entire pipeline to stop because one segment of the network is flaky.

- **What “exactly-once” really means**. In streaming systems, you generally want each event processed exactly once - not skipped, not duplicated. A skipped fraud alert means missed fraud. A duplicated payment means charging a customer twice.

    - But true exactly-once processing across distributed systems is impossible. The [Two Generals problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem) demonstrates why - if two parties must agree on an action and can only communicate through unreliable channels, they can never be certain of agreement. Messages might be lost. Acknowledgments might be lost. There’s always uncertainty.
    - What streaming systems actually provide is “effectively exactly-once” - mechanisms that make duplicates either impossible or invisible.
    - **Idempotent producers** in Kafka assign sequence numbers to messages. If a retry sends the same message twice, the broker recognizes the duplicate and discards it. **Transactional writes** can span multiple partitions atomically - either all writes succeed or none do. Consumer offset commits in Flink happen together with **state checkpoints**. These periodic snapshots capture processing progress, so if a failure occurs, processing resumes from that point without reprocessing or skipping.

## State: Derived from the Log

The log stores events - what happened. But many streaming applications need derived state - aggregations, lookups, running totals. A fraud detection system needs to know how many transactions an account has made in the last hour.

This derived state is effectively a cache computed from the log.

- **State in streaming processors**. Flink maintains state as a first-class concept. The most common type is keyed state - partitioned by key so all events for `account_id` A go to the same task, which maintains that account's state. This is what fraud detection uses for velocity checks.

    - There's also operator state for things that span keys (like tracking which Kafka partitions you're reading) and broadcast state for reference data that every task needs (like the current fraud rules).

- **Incremental updates**. Unlike a batch job that recomputes everything from scratch, streaming updates state incrementally. A new transaction arrives, the running count increments by one, the rolling sum adds the transaction amount.
- **State as cache has cache problems**. State can become stale if events are delayed or lost. State can grow unbounded if you never clean up old entries. State needs to survive failures and restarts.
- **The log enables state recovery**. If state is lost, it can be rebuilt by replaying the log from the beginning or from the last checkpoint. The log is the source of truth. State is a derived, cached view. This relationship shapes how production streaming systems handle failure and recovery.