# Flink from the Ground Up: The Mental Model You Need

Link: https://medium.com/@blakelassiter/flink-from-the-ground-up-the-mental-model-you-need-1f2f5bfb614e

## Where Flink Fits: The 30,000-Foot View

Kafka’s role as transport. Kafka receives events, stores them durably, makes them available to consumers. It moves data between systems but doesn’t analyze that data. Getting events from point A to point B is the job.

Flink does the analysis. It reads from sources like Kafka, transforms what it reads, maintains state across events, writes results wherever they need to go. Kafka handles “how do I deliver these events reliably?” - Flink handles “what should I actually do with them?”

You could process Kafka events with simple consumers. For stateless transformations -- filtering, mapping, format conversion - that works fine. But fraud detection, for example, needs context: the number of recent transactions for an account, the last known location, whether an amount deviates from typical patterns. That kind of stateful processing, at scale, with fault tolerance, is what Flink provides.

## Flink Architecture: JobManager and TaskManagers

Flink uses a leader-worker architecture.
- **JobManager** is the coordinator. When you submit a job, the JobManager parses it into a graph of operations, decides how to distribute work across available workers, tracks progress, handles failures and coordinates checkpoints (snapshots of job state). It doesn’t process your data. It orchestrates - a conductor coordinating the orchestra without playing an instrument.
- **TaskManagers** are the workers. Each one runs on a separate machine or container, executes the actual data processing, maintains local state for its assigned work and reports status back to the JobManager.

    - Each TaskManager has a fixed number of **task slots** - essentially CPU/memory allocations for running parallel work. A TaskManager with 4 slots can run 4 parallel tasks. The total slots across all TaskManagers determines your cluster’s processing capacity.

**Why serialization matters**: When you submit a job, the JobManager needs to send your processing logic to TaskManagers. Your functions - the lambdas, the map operations, the filters - have to be converted to bytes and shipped over the network. TaskManagers reconstruct them on the other side. Any object your function references needs to survive that process. If it can’t be serialized, the job fails at deployment time in cluster mode (not at runtime) with a stack trace pointing at some inner class you didn’t realize was involved.

## A Note on Java vs. Scala

Flink 2.0 removed the native Scala DataStream API entirely ([FLIP-265](https://cwiki.apache.org/confluence/spaces/FLINK/pages/231115782/FLIP-265+Deprecate+and+remove+Scala+API+support)). The Scala API had become a maintenance burden. Every Flink release meant testing against multiple Scala versions, keeping feature parity with Java, dealing with binary compatibility issues. The Flink team decided maintaining two full APIs wasn’t worth it.

Most enterprise Flink deployments use Java. The official documentation is Java-first.

## Sources, Operators, and Sinks

> Note: the code examples use Java 

A Flink job is a pipeline with three types of components: Sources read data into the pipeline - Kafka topics, files, databases. Operators transform data as it flows through. Sinks write results out.

Every Flink job starts by creating an [execution environment](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/#anatomy-of-a-flink-program) - the context that manages the job:

```java
// detects whether you’re running locally or on a cluster
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

### Reading from Kafka (Source)

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("transactions")
    .setGroupId("fraud-detection")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

// fromSource() connects the Kafka source to your pipeline and returns a DataStream - Flink's core
// abstraction for a stream of events.
DataStream<String> rawTransactions = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "Kafka Source"
);
```

### Writing to Kafka (Sink)

```java
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
            .setTopic("fraud-alerts")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
    )
    .build();

alertStream.sinkTo(sink);
```

### Transformations (Operators)

Between source and sink, operators transform the data. Flink provides two categories: stateless operations that process each event independently, and stateful operations that remember information across events.

- **Stateless** transformations include:

    ```java
    // map: transform each event (one-to-one)
    DataStream<Transaction> transactions = rawTransactions
        .map(json -> parseTransaction(json));

    // filter: keep only events matching a condition
    DataStream<Transaction> highValue = transactions
        .filter(tx -> tx.getAmount() > 10000);

    // flatMap: transform each event into zero or more events (one-to-many)
    DataStream<String> keywords = descriptions
        .flatMap((String description, Collector<String> collector) -> {
            for (String word : description.split(" ")) {
                collector.collect(word);
            }
        })
        .returns(Types.STRING);  // Required — Java type erasure hides Collector's generic type
    ```

- **Stateful** transformations start with [keyBy()](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/#keyby):

    ```java
    // Group transactions by account - all events for the same account
    // go to the same parallel subtask
    KeyedStream<Transaction, String> byAccount = transactions
        .keyBy(tx -> tx.getAccountId());

    // Now we can apply stateful operations
    DataStream<FraudAlert> alerts = byAccount
        .process(new FraudDetectionFunction());  // Can maintain per-account state
    ```

### Combining Multiple Streams

Sometimes you need to process two related streams together. In fraud detection, you might have:
- **Transaction stream**: Continuous flow of payment events
- **Account update stream**: Occasional updates like “this account was reported stolen”

When processing a transaction, you want access to the latest account status. Flink’s `connect()` lets you join two streams:

```bash
// Two separate streams
DataStream<Transaction> transactions = /* ... */;
DataStream<AccountUpdate> accountUpdates = /* ... */;

// Connect them for joint processing
ConnectedStreams<Transaction, AccountUpdate> connected = 
    transactions.connect(accountUpdates);

// Process both streams together, maintaining shared state
DataStream<EnrichedTransaction> enriched = connected
    .keyBy(tx -> tx.getAccountId(), update -> update.getAccountId())
    .process(new AccountEnrichmentFunction());
```

The `process()` function receives events from both streams and can maintain state that both streams access. When an account update arrives, you update your state. When a transaction arrives, you can check that state.

This pattern appears frequently in streaming: enriching events with reference data, joining streams from different sources, combining real-time data with slower-updating dimensions.

## Parallelism: Scaling Processing

Flink scales by distributing work across machines. Parallelism is the number of parallel instances of an operator. If you set parallelism to 4 for a map operation, Flink runs 4 copies of that map, each processing a portion of the data.

You can set parallelism at multiple levels:

```java
// Environment level - default for all operators
env.setParallelism(4);

// Operator level - override for specific operations
DataStream<Transaction> parsed = rawTransactions
    .map(json -> parseTransaction(json))
    .setParallelism(8);  // This specific map runs with 8 parallel instances
```

### Parallelism and Kafka Partitions

When reading from Kafka, parallelism interacts with partition count. Flink assigns Kafka partitions to parallel source instances:
- **Parallelism < partitions**: Some instances read multiple partitions. Works fine, each instance handles more data.
- **Parallelism = partitions**: One-to-one mapping. Each instance reads one partition. Often the sweet spot.
- **Parallelism > partitions**: Extra instances have nothing to read and sit idle. Wasteful.

A practical approach: set source parallelism equal to your Kafka partition count, then adjust downstream operator parallelism based on processing needs. Heavy transformations might warrant higher parallelism; simple filters might need less.

### Task Chaining

Flink optimizes by [chaining](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/#task-chaining-and-resource-groups) consecutive operators into single tasks when possible. If a map is followed by a filter, and both have the same parallelism, Flink can run them together without network transfer between them.

```java
// These three operations can be chained into one task
stream
    .map(x -> transform(x))      // ->
    .filter(x -> x.isValid())    // -> Potentially chained
    .map(x -> format(x))         // ->
    .keyBy(x -> x.getKey())      // <- Forces chain break (shuffle)
    .process(new StatefulProcessor());
```

Chaining reduces overhead — data stays in memory rather than being serialized and sent over the network. The `keyBy()` operation breaks chains because it redistributes data across workers (events with the same key must go to the same worker).

You generally don’t need to manage chaining manually. Flink handles it automatically. But understanding it helps when reading the Flink UI — you’ll see “chained” operators grouped together in the job graph.

## Execution Modes: Local vs. Cluster

Flink runs in two primary modes:
- **Local mode** runs everything in a single JVM. When you run a Flink job from your IDE or with flink run against a local mini-cluster, all operators run in one process. No network transfer between operators, no serialization of functions - they're just objects in memory. Fast iteration for development.
- **Cluster mode** distributes work across machines. JobManager and TaskManagers run in separate JVMs, potentially on different servers. Functions are serialized and shipped to TaskManagers. Data is serialized when transferred between operators on different machines. This is production deployment with fault tolerance.

The serialization boundary is the key difference. Code that works locally may fail in cluster mode if it captures non-serializable references. Our config object bug was invisible locally because nothing needed to serialize.

```java
// BAD: lambda captures 'this', which includes non-serializable config
class FraudProcessor {
    private Config config;  // Config doesn't implement Serializable
    
    public DataStream<Alert> process(DataStream<Transaction> txs) {
        return txs.filter(tx -> tx.getAmount() > config.getThreshold());
        // config.getThreshold() references 'this.config' — serializes the whole FraudProcessor
    }
}

// GOOD: extract the value before the lambda
class FraudProcessor {
    private Config config;
    
    public DataStream<Alert> process(DataStream<Transaction> txs) {
        double threshold = config.getThreshold();  // Primitive, always serializable
        return txs.filter(tx -> tx.getAmount() > threshold);  // Captures only the double
    }
}
```

The fix is straightforward: extract the values you need into local variables before defining the lambda. Primitives and standard Java types are serializable. Complex objects often aren’t.

## Job Lifecycle: From Submission to Recovery

**Submission flow**:
1. You submit a job JAR to the JobManager
2. JobManager parses the code into a JobGraph (the execution plan)
3. JobManager requests slots from TaskManagers
4. Tasks are deployed to TaskManagers
5. Tasks begin processing

Jobs move through states: CREATED (parsed but not running), RUNNING (processing), FAILING (shutting down after error), FAILED (terminated), CANCELLING (you requested stop), CANCELED (stopped), FINISHED (completed — only for bounded/batch jobs). For streaming, RUNNING is the steady state. The job runs until something breaks or you cancel it.

### Checkpoints vs. Savepoints

Both are snapshots of job state, but they serve different purposes:
- [Checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/) are automatic. Flink takes periodic snapshots based on your configuration and stores them in a distributed filesystem. If a failure occurs, the job restarts from the last checkpoint. You configure them; Flink manages them.

    ```java
    // Enable checkpointing every 60 seconds
    env.enableCheckpointing(60000);

    // Configure checkpoint storage
    env.getCheckpointConfig().setCheckpointStorage("s3://bucket/checkpoints");
    ```

- **Savepoints** are manual. You trigger them explicitly — before an upgrade, before maintenance, before anything where you want a known-good recovery point. They’re portable across job versions (with care) and useful for migrations or A/B testing.

    ```java
    # Trigger a savepoint
    flink savepoint <jobId> s3://bucket/savepoints/

    # Restart from savepoint
    flink run -s s3://bucket/savepoints/savepoint-abc123 job.jar
    ```

### Debugging Flink Jobs

When things go wrong, the Flink Web UI is your primary diagnostic tool.
- The Job Overview shows your pipeline as a graph — operators, their parallelism, connections between them. When something fails, that operator turns red.
- Task Metrics shows throughput and backpressure (upstream slowdown when downstream can’t keep up); if one operator shows high backpressure, you’ve found your bottleneck.
- The Exceptions tab has stack traces from failed tasks — this is where you’d see the serialization exception from our opening bug.
- Checkpoint history shows duration and size over time; growing checkpoint times usually mean state is bloating.

### Common Issues

- “Could not allocate slots” means you’re asking for more parallelism than your cluster can provide. Either reduce parallelism or add TaskManagers.
- Serialization exceptions mean a function captured something that can’t be converted to bytes — usually a reference to a non-serializable object. Extract primitive values before defining the lambda.
- OutOfMemoryError usually means state or buffers exceeded available memory. Options: more TaskManager memory, RocksDB state backend for large state, or lower parallelism.
- Checkpoint timeouts happen when checkpoints take too long, often due to backpressure or large state. Find the bottleneck operator first.

## Putting It Together: A Simple Pipeline

```java
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing for fault tolerance
        env.enableCheckpointing(60000);
        env.setParallelism(4);  // Match Kafka partition count
        
        // Source: read transactions from Kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("transactions")
            .setGroupId("fraud-detection")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> rawTransactions = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        
        // Parse JSON to Transaction objects
        DataStream<Transaction> transactions = rawTransactions
            .map(json -> Transaction.fromJson(json))
            .name("Parse Transactions");
        
        // Filter to high-value transactions (stateless)
        DataStream<Transaction> highValue = transactions
            .filter(tx -> tx.getAmount() > 1000)
            .name("High Value Filter");
        
        // Key by account for stateful processing
        DataStream<FraudAlert> alerts = highValue
            .keyBy(tx -> tx.getAccountId())
            .process(new VelocityCheckFunction())  // Stateful: checks tx rate per account
            .name("Velocity Check");
        
        // Sink: write alerts to Kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("fraud-alerts")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .build();
        
        alerts
            .map(alert -> alert.toJson())
            .sinkTo(sink);
        
        // Execute the job
        env.execute("Fraud Detection Pipeline");
    }
}
```