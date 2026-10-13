# Your Streaming Data Is Missing Context: Enrichment Patterns That Scale

Link: https://medium.com/@blakelassiter/your-streaming-data-is-missing-context-enrichment-patterns-that-scale-d5d006c7246d

## The Enrichment Problem

Consider what a raw transaction looks like:

```json
{
    "transaction_id": "tx-123",
    "account_id": "acct-456",
    "merchant_id": "merch-789",
    "amount": 5000.00,
    "timestamp": "2024-01-15T14:30:00Z"
}
```

This tells you what happened but not whether it should concern you. Now consider the same transaction with enrichment:

```json
{
    "transaction_id": "tx-123",
    "account_id": "acct-456",
    "account_age_days": 3650,
    "account_risk_score": 12,
    "typical_transaction_amount": 2500.00,
    "merchant_id": "merch-789",
    "merchant_category": "electronics",
    "merchant_risk_rating": "low",
    "merchant_country": "US",
    "amount": 5000.00,
    "amount_vs_typical": 2.0,
    "timestamp": "2024-01-15T14:30:00Z"
}
```

Now the fraud detection logic has something to work with. This is twice the typical amount but the account is ten years old with a low risk score, buying from a low-risk domestic merchant. That context changes the decision from “suspicious” to “probably fine.”

The challenge is that reference data lives in databases, not streams. Account profiles sit in a customer database. Merchant risk ratings come from a compliance system. Blacklists update through batch processes or real-time feeds. Bringing this data into the streaming pipeline creates three problems:

- **Throughput**: Querying a database for every transaction kills performance. At 10,000 transactions per second, synchronous database lookups would require 10,000 queries per second — more than most databases can handle while maintaining acceptable latency.
- **Freshness**: Reference data changes. An account’s risk score might update when suspicious activity is detected. A merchant might get added to a blacklist. Using stale data means making fraud decisions on outdated information.
- **Memory**: Loading all reference data into memory provides fast lookups but requires significant resources. A million account profiles at 1KB each needs a gigabyte of memory per parallel task instance.

## Stream-Stream Joins: Correlating Two Event Streams

Sometimes the enrichment data you need comes from another stream rather than a static table. Correlating a login stream with a transaction stream can reveal suspicious patterns — did someone log in from a new device immediately before making a large purchase?

Flink provides two approaches for stream-stream joins: windowed joins and interval joins.

- **Windowed joins** match events that fall into the same time window:

    ```java
    transactionStream
        .join(loginStream)
        .where(tx -> tx.getAccountId())
        .equalTo(login -> login.getAccountId())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .apply(new TransactionLoginJoiner());
    ```
    - Windowed joins use fixed boundaries — with 5-minute tumbling windows, the boundaries are [10:00–10:05), [10:05–10:10), and so on. Events match only if they fall in the same window. This creates an awkward edge case: a login at 10:04 and a transaction at 10:06 won’t match even though they’re only two minutes apart, because they fall in different windows.
    - For fraud detection, this boundary problem makes windowed joins a poor fit. What you typically want is “find logins within X minutes before this transaction” — a relative time range, not fixed calendar boundaries.

- **Interval joins** solve this by defining a time range relative to each event:

    ```java
    transactionStream
        .keyBy(tx -> tx.getAccountId())
        .intervalJoin(loginStream.keyBy(login -> login.getAccountId()))
        .between(Time.minutes(-10), Time.minutes(0))
        .process(new TransactionLoginProcessor());
    ```
    - For each transaction, this finds login events from the previous ten minutes. A transaction at 10:15 matches logins between 10:05 and 10:15 — regardless of window boundaries. This is almost always what you want for correlating events in fraud detection.

Both approaches buffer events in state until the join window or interval passes. Memory usage scales with event rate multiplied by the time range. For an interval join looking back 10 minutes, if the login stream runs at 1,000 events per second, you’re buffering roughly 600,000 login events in state — per key partition.

## Stream-Table Joins: The Fundamental Pattern

Most enrichment involves looking up relatively static reference data. Conceptually it’s simple:

```txt
For each transaction:
    Look up account profile by account_id
    Look up merchant data by merchant_id
    Check blacklists
    Combine into enriched transaction
```

The challenge is implementation. How you perform these lookups determines whether your pipeline handles 100 events per second or 100,000.

A naive approach queries the database for each event. At low volumes this works fine. At high volumes it creates two problems: the database can’t handle the query load, and the synchronous queries add latency that slows the entire pipeline. A 10ms database query per event means 10 seconds to process 1,000 events — serial execution turns your stream processor into a batch system.

The next section covers three strategies that avoid these problems, each with different tradeoffs for data size, update frequency and latency requirements.

## Choosing an Enrichment Strategy

The right enrichment strategy depends on three factors: how large is your reference data, how often does it change and what latency can you tolerate?

![](images/enrichment_strategy.png)

### When Reference Data Fits in Memory: Broadcast State

For small, slowly-changing reference data, the simplest approach is loading everything into memory on every parallel task instance. [Flink’s broadcast state](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/broadcast_state/) does exactly this — it takes a stream of reference data and makes it available as in-memory state that all parallel instances can read.

Broadcast state works well when:
- **Reference data is small enough to fit in memory** (a useful rule of thumb: under 1GB)
- **Data changes infrequently** — daily updates rather than per-second changes
- You need **sub-millisecond lookup latency**

The implementation involves broadcasting reference data to all task instances and then connecting that broadcast stream with your main data stream:

```java
// Define the broadcast state descriptor
MapStateDescriptor<String, AccountProfile> profileDescriptor = 
    new MapStateDescriptor<>(
        "account-profiles",
        String.class,
        AccountProfile.class
    );

// Broadcast the account profiles to all parallel instances
BroadcastStream<AccountProfile> profileBroadcast = 
    accountProfileStream.broadcast(profileDescriptor);

// Connect transaction stream with broadcast profiles
transactionStream
    .keyBy(tx -> tx.getAccountId())
    .connect(profileBroadcast)
    .process(new AccountEnrichmentFunction());
```

The `AccountEnrichmentFunction` extends `KeyedBroadcastProcessFunction` and implements two methods - one for processing transactions (the keyed stream) and one for processing profile updates (the broadcast stream):

```java
public class AccountEnrichmentFunction 
    extends KeyedBroadcastProcessFunction<String, Transaction, AccountProfile, EnrichedTransaction> {
    
    private final MapStateDescriptor<String, AccountProfile> profileDescriptor;
    
    public AccountEnrichmentFunction(MapStateDescriptor<String, AccountProfile> descriptor) {
        this.profileDescriptor = descriptor;
    }
    
    @Override
    public void processElement(
            Transaction tx, 
            ReadOnlyContext ctx, 
            Collector<EnrichedTransaction> out) throws Exception {
        
        ReadOnlyBroadcastState<String, AccountProfile> profiles = 
            ctx.getBroadcastState(profileDescriptor);
        
        AccountProfile profile = profiles.get(tx.getAccountId());
        out.collect(enrich(tx, profile));
    }
    
    @Override
    public void processBroadcastElement(
            AccountProfile profile, 
            Context ctx, 
            Collector<EnrichedTransaction> out) throws Exception {
        
        ctx.getBroadcastState(profileDescriptor)
           .put(profile.getAccountId(), profile);
    }
}
```

The `processElement` method handles incoming transactions by looking up the account profile from broadcast state. Since broadcast state is just an in-memory hash map, lookup latency is sub-millisecond. The processBroadcastElement method handles profile updates by storing them in the broadcast state.

The tradeoff is memory. Each parallel task instance holds a complete copy of the reference data. With 10 task instances and 500MB of reference data, you’re using 5GB total memory just for broadcast state.

For fraud detection, broadcast state fits well for:
- **Account profiles** when account counts are manageable (millions of accounts at ~1KB each fits in ~1GB)
- **Merchant blacklists** which typically contain thousands of entries, not millions
- **Merchant category codes** which are relatively small lookup tables

One operational note: broadcast state can catch you off guard when reference data grows. We started with 50,000 merchants in our blacklist — maybe 5MB of data. Over two years it grew to 500,000 entries. So, monitor your broadcast state size and set alerts before it closes to memory limits.

### When Data is Too Large: Async I/O

When reference data exceeds what fits in memory, you need to query an external system. The key is doing it asynchronously so queries for different events can overlap.

Flink’s [async I/O](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/asyncio/) lets you issue multiple concurrent requests to an external system. While one event’s lookup is in flight, other events can start their lookups. This improves throughput compared to synchronous queries.

```java
AsyncDataStream.orderedWait(
    transactionStream,
    new AsyncAccountLookup(),
    30, TimeUnit.SECONDS,  // timeout
    100  // max concurrent requests
);
```

The async function extends `RichAsyncFunction` and uses the callback pattern to handle results:

```java
public class AsyncAccountLookup 
    extends RichAsyncFunction<Transaction, EnrichedTransaction> {
    
    private transient RedisAsyncCommands<String, String> redisCommands;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        RedisClient client = RedisClient.create("redis://localhost:6379");
        StatefulRedisConnection<String, String> connection = client.connect();
        redisCommands = connection.async();
    }
    
    @Override
    public void asyncInvoke(
            Transaction tx, 
            ResultFuture<EnrichedTransaction> resultFuture) {
        
        RedisFuture<String> profileFuture = 
            redisCommands.get("profile:" + tx.getAccountId());
        
        profileFuture.thenAccept(profileJson -> {
            AccountProfile profile = deserialize(profileJson);
            resultFuture.complete(
                Collections.singleton(enrich(tx, profile))
            );
        });
    }
}
```

The `orderedWait` variant preserves event order - results are emitted in the same order events arrived, even if lookups complete out of order. This matters for fraud detection where velocity checks depend on processing transactions in sequence. The alternative `unorderedWait` emits results as lookups complete, which provides lower latency but changes event order.

The tradeoffs with async I/O:
- **Latency**: Adds milliseconds per event. Redis typically responds in 1–5ms; direct database queries take 5–50ms.
- **Throughput**: Limited by the external system’s capacity and your configured max concurrent requests. Setting this too low creates backpressure; too high can overwhelm the external system.
- **Failure handling**: Timeouts and retries need careful configuration. The 30-second timeout above is generous — in practice you might use 5–10 seconds and implement retry logic.

Async I/O works well for:
- **Large reference datasets** that don’t fit in memory
- **Lookup patterns with good locality** where caching can reduce external calls
- **Systems with an async-capable client** (Redis, many databases, HTTP APIs)

### When You Need Real-Time Updates: CDC-Backed Enrichment

Both broadcast state and async I/O have freshness tradeoffs. Broadcast state updates only when new data flows through the broadcast stream. Async I/O reads from an external system that might be slightly stale or require cache invalidation.

When reference data changes frequently and you need those changes reflected within seconds, CDC (Change Data Capture) provides another option. CDC streams database changes — inserts, updates, deletes — into Kafka topics. Your Flink job consumes both the transaction stream and the CDC stream, maintaining reference data in keyed state that updates in real-time.

```java
// CDC stream of account profile changes from database
DataStream<AccountProfile> profileChanges = 
    env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "profile-cdc");

// Connect transactions with CDC stream, keyed by account
DataStream<EnrichedTransaction> enriched = transactionStream
    .keyBy(tx -> tx.getAccountId())
    .connect(profileChanges.keyBy(p -> p.getAccountId()))
    .process(new CDCEnrichmentFunction());
```

The `CDCEnrichmentFunction` uses `CoProcessFunction` to handle both streams:

```java
public class CDCEnrichmentFunction 
    extends CoProcessFunction<Transaction, AccountProfile, EnrichedTransaction> {
    
    private ValueState<AccountProfile> profileState;
    
    @Override
    public void open(OpenContext openContext) throws Exception {
        profileState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("profile", AccountProfile.class)
        );
    }
    
    @Override
    public void processElement1(
            Transaction tx, 
            Context ctx, 
            Collector<EnrichedTransaction> out) throws Exception {
        
        AccountProfile profile = profileState.value();
        out.collect(enrich(tx, profile));
    }
    
    @Override
    public void processElement2(
            AccountProfile profile, 
            Context ctx, 
            Collector<EnrichedTransaction> out) throws Exception {
        
        profileState.update(profile);
    }
}
```
- When a profile changes in the database, CDC captures that change and streams it to Flink. The job updates its keyed state immediately. The next transaction for that account sees the updated profile.
- Unlike broadcast state where every task holds all profiles, CDC-backed enrichment partitions state by key. Each task instance only holds profiles for the accounts it’s responsible for. This scales better for large datasets.

The tradeoffs:
- **Freshness**: Changes reflected within seconds of database commit
- **Memory**: Partitioned by key, so scales better than broadcast for large datasets
- **Complexity**: Requires CDC infrastructure
- **Ordering**: Transactions might arrive before their account’s profile. You need to handle the cold-start case where `profileState.value()` returns null.

CDC-backed enrichment works well for:
- **Frequently-updated reference data** like account risk scores that change based on recent activity
- **Large datasets** where broadcast state’s memory multiplication is prohibitive
- **Systems already using CDC** for other purposes

## Caching Strategies for Async I/O

Async I/O to a database handles large datasets but adds latency and puts load on the external system. Caching reduces both problems by storing frequently-accessed data closer to the processing.

The cache-aside pattern checks the cache first and falls back to the database on misses:

```java
@Override
public void asyncInvoke(
        Transaction tx, 
        ResultFuture<EnrichedTransaction> resultFuture) {
    
    String accountId = tx.getAccountId();
    
    // Check cache first
    RedisFuture<String> cacheFuture = cache.get("profile:" + accountId);
    
    cacheFuture.thenCompose(cached -> {
        if (cached != null) {
            return CompletableFuture.completedFuture(deserialize(cached));
        }
        
        // Cache miss - query database and populate cache
        return database.queryAsync(accountId)
            .thenApply(profile -> {
                cache.setex("profile:" + accountId, TTL_SECONDS, serialize(profile));
                return profile;
            });
    }).thenAccept(profile -> {
        resultFuture.complete(Collections.singleton(enrich(tx, profile)));
    });
}
```

Cache TTL (time-to-live) requires balancing freshness against hit rate:
- **Too short**: High cache miss rate means most requests still hit the database
- **Too long**: Stale data leads to incorrect fraud decisions

For fraud detection, 5–15 minutes often works well for account profiles. A risk score that’s five minutes old is still useful. Static data like merchant categories can have longer TTLs — hours or even days.

One gotcha: cache TTLs should account for the business impact of staleness. For blacklists, where a five-minute delay means approving transactions from a known fraudulent account, shorter TTLs or CDC-backed enrichment is safer.

**Cache warming** helps avoid cold-start problems. When a Flink job starts or restarts, the cache is empty. A traffic spike with an empty cache creates a “thundering herd” that overwhelms the database. Pre-populating the cache with frequently-accessed accounts before the job starts prevents this.

Another lesson: watch your async I/O max concurrency setting. The right number depends on your external system’s capacity, not what Flink can generate.

## Practical Patterns for Fraud Detection

Bringing the strategies together, here’s how enrichment typically works in a fraud detection pipeline:
- **Account profiles** (Broadcast or CDC): Risk scores, account age, typical transaction amounts. The data is relatively small (one record per account, not one per transaction). For systems with a manageable account count, broadcast state works well. For systems where account risk scores update frequently based on recent activity, CDC-backed enrichment ensures decisions use current scores.
- **Merchant categorization** (Broadcast): Merchant category codes, risk ratings and country information. This data changes slowly — merchants don’t change categories often. Broadcast state is the obvious choice.
- **Blacklist checking** (Broadcast with frequent refresh): Known fraudulent cards, devices, accounts and merchants. The dataset is small but updates matter urgently — when fraud is discovered, you want to block it immediately. Broadcast state with a frequently-refreshed source stream, or CDC-backed if you need sub-second propagation.
- **Geographic enrichment** (Async I/O with cache): IP-to-location lookups, postal code data, distance calculations. These datasets are large but change rarely. Async I/O with aggressive caching keeps lookup latency low while handling datasets that don’t fit in memory.

A pipeline combining these patterns might look like:

```java
// Start with raw transactions
DataStream<Transaction> transactions = ...;

// Broadcast small, slowly-changing reference data
BroadcastStream<MerchantInfo> merchantBroadcast = 
    merchantStream.broadcast(merchantDescriptor);
BroadcastStream<Blacklist> blacklistBroadcast = 
    blacklistStream.broadcast(blacklistDescriptor);

// Enrich with merchant data
DataStream<Transaction> withMerchant = transactions
    .keyBy(tx -> tx.getMerchantId())
    .connect(merchantBroadcast)
    .process(new MerchantEnrichment());

// Check blacklists
DataStream<Transaction> withBlacklist = withMerchant
    .keyBy(tx -> tx.getAccountId())
    .connect(blacklistBroadcast)
    .process(new BlacklistCheck());

// Async I/O for geographic lookup (large dataset, high cache hit rate)
DataStream<EnrichedTransaction> fullyEnriched = 
    AsyncDataStream.orderedWait(
        withBlacklist,
        new AsyncGeoLookup(),
        10, TimeUnit.SECONDS,
        100
    );
```

The ordering matters. Broadcast enrichments are fast and can filter out obvious fraud (blacklisted accounts) before more expensive async lookups. Geographic enrichment — the slowest step — happens last, and only for transactions that pass earlier checks.

## Handling Enrichment Failures

Enrichment can fail. The account profile might not exist for a new account. The merchant lookup might timeout. The CDC stream might be delayed. What happens to the transaction?

There are three options:
- **Pass through with defaults**: Enrich what you can and use sensible defaults for missing data. A missing account profile might default to “high risk” — treat unknown accounts conservatively. This keeps transactions flowing but might increase false positives.
- **Buffer and retry**: Hold the transaction and retry the lookup. This works for transient failures but risks building up a backlog if failures persist.
- **Route to manual review**: Send transactions with missing enrichment data to a separate queue for human review. This ensures no fraud decisions are made with incomplete data but requires operational support.

The right choice depends on your tolerance for false positives versus false negatives. Most systems use a combination.