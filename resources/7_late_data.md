# Late Data Will Break Your Pipeline: Time & Windowing in Flink

Link: https://medium.com/@blakelassiter/late-data-will-break-your-pipeline-time-windowing-in-flink-a765b01ea37a

## The Time Problem

**Event time** is when something actually occurred in the real world. For a transaction, that’s the timestamp from the payment terminal or mobile app, the moment the customer swiped their card. This timestamp comes from the source system and travels with the event.

**Processing time** is when Flink processes the event, the wall clock on the TaskManager handling the record. Processing time is simple because you always know it. Just check the clock. But it conflates two unrelated things: business reality and infrastructure timing.

**Ingestion time** is when an event entered Flink specifically. It’s rarely used and mostly exists for edge cases where source systems don’t provide timestamps.

For the fraud scenario, event time is what matters. A fraudster shouldn’t be able to evade velocity checks by using a mobile device with intermittent connectivity. The transaction happened at 2:43:17. That’s when it should be counted, regardless of when our system received it.

To be clear about how this works in practice: velocity checks don’t block transactions in real-time. Authorization decisions happen in milliseconds, using simpler checks like card status and hard spending limits. Velocity checks run slightly behind and trigger alerts or downstream actions — flagging the account for review, blocking subsequent transactions, notifying the customer. By the time a velocity window fires, the fraudulent transaction has already completed. The goal is catching the pattern quickly enough to prevent the next one. That’s why getting the count right matters: missing that sixth transaction means the alert never fires and the seventh transaction goes through too.

Processing time has its place. Monitoring dashboards that show “messages processed per second right now” legitimately care about processing time. Alert systems asking “is the pipeline keeping up?” care about processing time. But any business logic that aggregates, joins or computes over events needs event time to be correct.

The `VelocityCheckFunction` failed because it used `ctx.timestamp()` to get the event's timestamp but then made window-closing decisions based on event arrival patterns. True event-time processing requires a mechanism for deciding when enough time has passed in the event stream, independent of when events physically arrive.

## Watermarks

A watermark is a timestamp that flows through the stream, asserting that no events with earlier timestamps should be expected. When a watermark with timestamp T passes through an operator, that operator can assume all events with timestamps before T have arrived, or at least have been given reasonable opportunity to arrive.

Flink generates watermarks at sources and propagates them through the topology. When a windowing operator sees a watermark pass the end time of a window, it knows the window is complete and fires the computation. The window for 2:38:00–2:43:00 fires when a watermark with timestamp 2:43:00 or later passes through.

The most common watermark strategy handles bounded out-of-orderness:

```java
WatermarkStrategy
    .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((tx, timestamp) -> tx.getTransactionTime());
```

This tells Flink: “Events might arrive up to 30 seconds after their timestamps would suggest. Wait that long before considering a time period complete.” If the most recent event timestamp seen is 2:43:47, the watermark will be at 2:43:17 (30 seconds behind). Windows ending at 2:43:17 or earlier can fire. Windows ending after 2:43:17 must wait.

The 30-second bound is a tradeoff. Longer bounds catch more late events but delay results. Shorter bounds provide faster results but drop more late arrivals. The right value depends on your data characteristics and tolerance for late data. For the mobile transaction scenario, 30 seconds would have caught the event that arrived with a 30-second delay.

For data sources where events always arrive in timestamp order, like a single-threaded producer writing synchronously, you can use monotonically increasing watermarks:

```java
WatermarkStrategy
    .<Transaction>forMonotonousTimestamps()
    .withTimestampAssigner((tx, timestamp) -> tx.getTransactionTime());
```

This assumes no out-of-orderness at all. The watermark equals the highest timestamp seen. It’s more efficient but only correct when the assumption holds.

**Idle Kafka partitions** create a subtle problem. Watermarks advance based on the minimum across all input partitions. If your topic has 12 Kafka partitions and partition 7 stops receiving events because the producer feeding it crashed, the watermark for partition 7 stalls. Since Flink takes the minimum watermark across all partitions to determine global progress, that one stalled partition holds back all windows across all accounts — not just the accounts whose data happens to land in partition 7. The fix is declaring an idleness timeout:

```bash
WatermarkStrategy
    .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withIdleness(Duration.ofMinutes(1));
```

If a partition has no events for one minute, Flink stops considering it when computing the global watermark. The idle partition doesn’t hold back progress.

## Window Types

Flink provides three main window types:
- **Tumbling windows** are fixed-size and non-overlapping. They divide time into consecutive buckets with no gaps and no overlap. Each event belongs to exactly one window.

    ```java
    transactions
    .keyBy(tx -> tx.getAccountId())
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .sum("amount");
    ```
    - This computes hourly transaction totals per account. The window from 2:00 PM to 3:00 PM contains all transactions with timestamps in that hour. When the watermark passes 3:00 PM, the window fires and emits the total. The next window (3:00 PM to 4:00 PM) is completely separate.
    - Tumbling windows work well for periodic aggregations like hourly reports, daily summaries and distinct time buckets where you want each event counted once.

- **Sliding windows** are fixed-size but overlapping. They’re defined by two parameters: the window size and the slide interval. A 5-minute window that slides every 1 minute creates overlapping periods.

    ```java
    transactions
    .keyBy(tx -> tx.getAccountId())
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .process(new VelocityWindowFunction());
    ```
    - Each event belongs to multiple windows. A transaction at 2:43:30 falls into windows ending at 2:44, 2:45, 2:46, 2:47 and 2:48. This creates a rolling view where every minute you see the last 5 minutes of activity.
    - Sliding windows are the right choice for velocity detection. The fraud rule “no more than 5 transactions in any 5-minute period” requires checking overlapping periods. If you only check discrete 5-minute buckets (tumbling windows), a fraudster could spread 10 transactions across a bucket boundary and never trigger the alert.

- **Session windows** have dynamic size based on activity gaps. Instead of fixed time boundaries, they group events that occur close together and close the window after a period of inactivity.

    ```java
    transactions
    .keyBy(tx -> tx.getAccountId())
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .process(new SessionAnalyzer());
    ```
    - If an account makes transactions at 2:00, 2:05, 2:20 and then nothing until 3:15, the session window groups the first three transactions together (gaps less than 30 minutes) and closes when 30 minutes pass with no activity. The 3:15 transaction starts a new session.
    - Session windows suit user behavior analysis. They’re good for grouping related activity into logical sessions, detecting unusual session patterns and analyzing engagement.

## Rewriting VelocityCheckFunction with Windows

The manual approach (from Part 6) had four problems. Manual timestamp filtering is prone to bugs. There’s no handling for late arrivals. Window boundaries are driven by event arrival rather than time. And state grows with transaction volume rather than time. Flink’s windowing addresses all of these.

```java
DataStream<FraudAlert> alerts = transactions
    // Problem 2 solved: Watermarks handle late arrivals - events up to 30 seconds 
    // late still land in the correct window
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withTimestampAssigner((tx, timestamp) -> tx.getTransactionTime()))
    .keyBy(tx -> tx.getAccountId())
    // Problem 1 & 3 solved: Window boundaries are time-driven, not event-driven.
    // No manual timestamp filtering needed.
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .process(new VelocityWindowFunction());
    // Problem 4 solved: Flink automatically cleans up window state after the window
    // closes and any allowed lateness expires. State scales with time, not volume.

public class VelocityWindowFunction 
    extends ProcessWindowFunction<Transaction, FraudAlert, String, TimeWindow> {
    
    private static final int MAX_TRANSACTIONS = 5;
    
    @Override
    public void process(
            String accountId,
            Context context,
            Iterable<Transaction> transactions,
            Collector<FraudAlert> out) {
        
        int count = 0;
        for (Transaction tx : transactions) {
            count++;
        }
        
        if (count > MAX_TRANSACTIONS) {
            out.collect(new FraudAlert(
                accountId,
                "VELOCITY",
                String.format("%d transactions in 5-minute window ending %s",
                    count,
                    Instant.ofEpochMilli(context.window().getEnd()))
            ));
        }
    }
}
```

The watermark strategy declares a 30-second tolerance for out-of-order events. The sliding window creates overlapping 5-minute periods that slide every minute. The ProcessWindowFunction receives all transactions in a window and can access window metadata like start and end times.

What Flink now handles automatically:
- **Window boundaries are time-driven**. The window for 2:38:00–2:43:00 exists because of clock time, not because of which events arrived when. Events get assigned to windows based on their timestamps.
- **Late events go to the correct window**. That mobile transaction from 2:43:17 arriving at 2:43:47 lands in the right window because its timestamp falls within the window’s time range. The 30-second watermark delay we configured means Flink waits until it sees events timestamped 2:43:30 or later before considering the 2:43:00 window complete. Our late event arrives while the window is still open. But what if an event arrives even later, after the watermark has advanced past the window’s end? That’s where `allowedLateness` comes in, which we'll cover shortly.
- **Window state gets cleaned up**. After a window fires and any allowed lateness expires, Flink discards the window’s state. You don’t need manual cleanup logic.
- **One tradeoff to consider**. The windowed approach emits results when windows close, not on every event. For velocity checks requiring immediate response to the very first transaction that crosses the threshold, you might combine approaches: a stateful `KeyedProcessFunction` for immediate "definitely over threshold" alerts plus windowed aggregation for accurate final counts.

## Configuring Windows: Three Decisions

With the basic windowing in place, there are three decisions to make: how to compute results within each window, what to do with late-arriving data and when windows should emit their results.

### Choosing a Window Function

Flink provides three types of window functions with different performance characteristics. The choice depends on what you need to compute and how much data you’re willing to buffer.
- **ReduceFunction** works incrementally. As events arrive, Flink combines them immediately without storing all events. For summing transaction amounts, Flink adds each new amount to a running total rather than accumulating all transactions and summing at the end.

    ```java
    .reduce((tx1, tx2) -> new Transaction(
        tx1.getAccountId(),
        tx1.getAmount() + tx2.getAmount(),
        tx2.getTransactionTime()
    ));
    ```
    - ReduceFunction is efficient because it uses constant memory regardless of window size. The limitation is that it only works for operations where you can combine partial results.

- **AggregateFunction** provides more flexibility with a typed accumulator. You can track multiple values during aggregation.

    ```java
    public class TransactionAggregator 
        implements AggregateFunction<Transaction, Accumulator, Result> {
        
        @Override
        public Accumulator createAccumulator() {
            return new Accumulator(0, 0.0); // count, sum
        }
        
        @Override
        public Accumulator add(Transaction tx, Accumulator acc) {
            return new Accumulator(acc.count + 1, acc.sum + tx.getAmount());
        }
        
        @Override
        public Result getResult(Accumulator acc) {
            return new Result(acc.count, acc.sum / acc.count); // count and average
        }
        
        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            return new Accumulator(a.count + b.count, a.sum + b.sum);
        }
    }
    ```
    - AggregateFunction is still incremental. Memory scales with accumulator size, not window contents.

- **ProcessWindowFunction** receives all elements in the window plus metadata like window boundaries. It’s the most flexible but least efficient because Flink buffers all events until the window fires.

    ```java
    public class FullWindowProcessor 
        extends ProcessWindowFunction<Transaction, Result, String, TimeWindow> {
        
        @Override
        public void process(String key, Context ctx, 
                Iterable<Transaction> elements, Collector<Result> out) {
            // Access window metadata
            long windowStart = ctx.window().getStart();
            long windowEnd = ctx.window().getEnd();
            // Can iterate elements multiple times, compute medians, etc.
        }
    }
    ```
    - The ReduceFunction runs incrementally during the window’s lifetime. The ProcessWindowFunction receives just the reduced result but can add window metadata.

- **Combining approaches** gives you both efficiency and metadata access:

    ```java
    .reduce(reduceFunction, processWindowFunction)
    ```
    - The ReduceFunction runs incrementally during the window’s lifetime. The ProcessWindowFunction receives just the reduced result but can add window metadata.

For our velocity check, ProcessWindowFunction makes sense because we need the window end time for the alert message. If we only needed the count, a simple ReduceFunction would be more efficient.

### Handling Late Data

By default, late events are dropped silently. The window has already fired and been cleaned up. This might be acceptable for monitoring dashboards but wrong for velocity checks where missing a transaction means missing fraud.

- **Allowed lateness** keeps windows open past their end time:

    ```java
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .allowedLateness(Time.minutes(10))
    ```
    - The window stays materialized for an extra 10 minutes after the watermark passes its end. Late events within this period get added to the window and trigger re-computation. The window may emit multiple results: first when the watermark passes, then again for each late arrival.
    - For downstream consumers, this means handling updates to previously-emitted results. A velocity alert might fire initially with 5 transactions, then update to 6 when a late arrival appears.

- **Side outputs** capture data that arrives after even the allowed lateness has expired:

    ```java
    OutputTag<Transaction> lateTag = new OutputTag<Transaction>("late-transactions"){};

    SingleOutputStreamOperator<FraudAlert> results = transactions
        .keyBy(tx -> tx.getAccountId())
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .allowedLateness(Time.minutes(10))
        .sideOutputLateData(lateTag)
        .process(new WindowProcessor());

    DataStream<Transaction> lateTransactions = results.getSideOutput(lateTag);
    ```
    - Events arriving after allowed lateness can be routed to a separate stream. Maybe they go to a reconciliation queue, trigger manual review or feed into an audit log. The main processing continues normally while late data gets special handling.

The tradeoffs are straightforward. Longer allowed lateness means more memory because windows stay materialized longer and each window’s state accumulates. For high-cardinality keys like account IDs, this adds up. Short allowed lateness drops more late data. The right balance depends on your late data distribution and correctness requirements.

For fraud velocity checks specifically, missing a late transaction is worse than potentially alerting twice. Configure generous lateness and handle re-emissions downstream.

### Controlling When Windows Fire

By default, event-time windows fire once when the watermark passes the window’s end time. Sometimes you need different behavior.
- **Early results** emit partial data before the window closes:

    ```java
    .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
    ```
    - A one-hour tumbling window configured this way emits intermediate results every minute. Useful for dashboards that want to show progress before final results are ready.

- **Count-based triggers** fire based on element count rather than time:

    ```bash
    .trigger(CountTrigger.of(100))
    ```
    - Fire whenever 100 elements accumulate, regardless of time. Rarely the right choice for event-time windows but useful in specific scenarios.

- **Purging versus accumulating** determines what happens to window contents after firing. Accumulating mode (the default) keeps all data, so subsequent firings include everything. Purging mode clears the window after each firing.

For most fraud detection use cases, the default trigger behavior works fine. Windows fire when watermarks indicate completeness. Custom triggers add complexity that’s usually unnecessary unless you have specific latency requirements for partial results.

## Practical Patterns

Here’s how these concepts combine for common use cases.
- **Daily transaction totals** use tumbling windows with allowed lateness for overnight batch uploads:

    ```java
    transactions
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withIdleness(Duration.ofMinutes(10)))
    .keyBy(tx -> tx.getAccountId())
    .window(TumblingEventTimeWindows.of(Time.days(1)))
    .allowedLateness(Time.hours(2))
    .sum("amount");
    ```
    - The 2-hour allowed lateness handles transactions that arrive after midnight but belong to the previous day. The 5-minute bounded out-of-orderness handles normal network variance. The idleness timeout prevents stalled partitions from blocking progress.

- **Velocity detection** uses sliding windows to catch fraud spanning bucket boundaries:

    ```java
    transactions
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(30)))
    .keyBy(tx -> tx.getAccountId())
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .allowedLateness(Time.minutes(2))
    .sideOutputLateData(lateTag)
    .process(new VelocityWindowFunction());
    ```
    - Thirty-second tolerance for mobile transaction delays. Two-minute allowed lateness for stragglers. Late data captured separately for audit.

- **Session analysis** detects unusual activity patterns:

    ```java
    transactions
    .keyBy(tx -> tx.getAccountId())
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .process(new SessionAnalyzer());
    ```
    - Accounts that normally have single-transaction sessions suddenly showing 50-transaction sessions might indicate compromised credentials.