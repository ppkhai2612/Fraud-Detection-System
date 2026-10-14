# Bad Data Will Find You: Validation Patterns for Streaming Pipelines

Link: https://medium.com/@blakelassiter/bad-data-will-find-you-validation-patterns-for-streaming-pipelines-24bf292d75f3

## Why Streaming Data Quality Is Different

In batch processing, data quality has a natural safety net. A job reads a dataset, processes it and writes results. If the input is bad, the job fails. You fix the data, rerun the job and move on. The feedback loop is tight: bad input produces visible errors.

Streaming doesn’t work that way. Events arrive continuously and get processed immediately. By the time you notice a quality issue, hundreds or thousands of events may have already flowed through. And in a stateful system like Flink, those events don’t just pass through — they update counters, modify aggregations and change the accumulated context that future events are evaluated against.

This creates three distinct failure modes, each harder to recover from than the last.

- The first is the **crash**. A null pointer exception or deserialization error kills the Flink job entirely. Processing stops for every customer, not just the ones with bad data. Everyone notices a crash, which is actually its one advantage — at least the failure is visible. But restarting from a checkpoint means reprocessing everything since the last successful snapshot.
- The second is **silent corruption**. Invalid data passes through processing without throwing an exception but produces wrong results (e.g., the null merchant ID scenario from the opening). Nothing crashed. No errors in the logs. The pipeline was running perfectly — it was just producing incorrect fraud scores for a subset of transactions. Silent corruption is worse than a crash because it can persist for hours before anyone notices.
- The third is **state pollution**. Bad data modifies Flink’s keyed state — the accumulated counters, aggregations and context that the job maintains across events. Even after you fix the input data, the state remains corrupted. A fraud velocity check that counted five invalid transactions against an account still shows those five transactions in its rolling window. Fixing state pollution typically requires either reprocessing from an earlier savepoint or manual state surgery using Flink’s [State Processor API](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/state_processor_api/).

## Defense in Depth: Where Validation Happens

Validation is a series of layers, each catching different categories of problems.

### Layer 1: Schema Validation at Ingestion

The first line of defense catches structurally malformed data before it enters the processing pipeline. If you’re using Kafka with a [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html), this happens almost automatically — the deserializer rejects records that don’t match the registered Avro or Protobuf schema. Missing required fields, wrong data types and unexpected formats all fail at deserialization.

### Layer 2: Business Rule Validation

This layer catches data that is structurally valid but semantically wrong. These are domain-specific rules that the schema can’t express: amounts must be positive, timestamps can’t be in the future, currency codes must belong to a known set, transaction amounts shouldn’t exceed system maximums.

Cross-field validation lives here too. A transaction’s currency should match the account’s configured currency. A card-present transaction shouldn’t originate from a country different than the merchant’s location. These rules encode business knowledge about what valid data actually looks like in your domain.

The distinction between schema validation and business rule validation matters for how you handle failures. A schema violation means something is broken upstream — the producer is sending garbage. A business rule violation might be a legitimate edge case, a configuration error or a bug in a specific upstream service. The response to each should be different.

### Layer 3: Enrichment Failure Handling

The previous article covered [three strategies for handling enrichment failures](8_enrichment_patterns.md#handling-enrichment-failures). From a data quality perspective, enrichment failures are another validation signal. If account lookups start failing at elevated rates, that’s a quality issue worth detecting even if your enrichment layer handles each individual failure gracefully.

### Layer 4: Dead Letter Routing

When validation fails at any layer, you have three options: crash the job, drop the record silently or route it somewhere safe. Crashing means one bad record stops processing for everyone. Dropping means silent data loss — you’ll never know records were missing. Routing preserves the failed record for investigation while keeping the pipeline running.

Routing is almost always the right answer. In Kafka-based pipelines, that means a dead letter queue.

## Input Validation in Practice

Flink’s [side output](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/) mechanism — a way to route records to an alternate output stream from within a processing function — fits validation routing well. A [ProcessFunction](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/) checks each record and either forwards valid records to the main output or diverts invalid ones to a side output for dead letter processing.

```java
public class TransactionValidator 
        extends ProcessFunction<Transaction, Transaction> {
    
    private final OutputTag<Transaction> invalidRecords = 
        new OutputTag<Transaction>("invalid-records") {};
    
    @Override
    public void processElement(
            Transaction tx, Context ctx, Collector<Transaction> out) {
        List<String> violations = new ArrayList<>();
        
        if (tx.getAccountId() == null) {
            violations.add("account_id is null");
        }
        if (tx.getAmount() == null) {
            violations.add("amount is null");
        }
        if (tx.getTimestamp() == null) {
            violations.add("timestamp is null");
        }
        
        if (violations.isEmpty()) {
            out.collect(tx);
        } else {
            ctx.output(invalidRecords, tx);
        }
    }
}
```

This basic null check would have caught the incident from the opening in under a millisecond per record. The null merchant IDs would have been diverted to the side output instead of flowing into enrichment. Beyond null checks, input validation also handles type coercion problems. String amounts that should be decimals, timestamps in inconsistent formats, currency codes with trailing whitespace.

For each of these, you face a design choice: coerce the data into the expected format, or reject it and route to the dead letter queue.
- Coercion is practical but can mask upstream problems. Used where the fix is unambiguous (trim whitespace, standardize timestamps)
- Rejection is strict but can cause unnecessary data loss when the data is recoverable. Used where interpretation is required (is the string “null” or a null value or a literal string?).

## Business Rule Validation

Schema validation confirms structure. Business rule validation confirms meaning. A transaction amount of -500.00 is perfectly valid structurally — it’s a decimal in the right field. But a negative purchase amount almost certainly indicates a problem upstream (refunds have their own transaction type).

Because business rule validation runs after input validation has already filtered null fields, these checks can safely assume required fields are present:

```java
public class BusinessRuleValidator 
        extends ProcessFunction<Transaction, Transaction> {
    
    private final OutputTag<ValidationFailure> ruleViolations = 
        new OutputTag<ValidationFailure>("rule-violations") {};
    
    @Override
    public void processElement(
            Transaction tx, Context ctx, Collector<Transaction> out) {
        List<String> violations = new ArrayList<>();
        
        if (tx.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            violations.add("Amount must be positive: " + tx.getAmount());
        }
        if (tx.getAmount().compareTo(new BigDecimal("1000000")) > 0) {
            violations.add("Amount exceeds maximum: " + tx.getAmount());
        }
        if (!tx.getCurrency().equals(tx.getAccountCurrency())) {
            violations.add("Currency mismatch: " + tx.getCurrency() 
                + " vs account " + tx.getAccountCurrency());
        }
        
        if (violations.isEmpty()) {
            out.collect(tx);
        } else {
            ctx.output(ruleViolations, 
                new ValidationFailure(tx, violations, Instant.now()));
        }
    }
}
```

The business rule violations produce a `ValidationFailure` rather than just forwarding the raw transaction. Including the violation reasons alongside the original record makes dead letter investigation far easier - you can see exactly which rule was violated without replaying the validation logic.

One important boundary to maintain: validation should be fast and deterministic. It checks whether data should have been sent this way. Business logic handles edge cases that are valid but require special treatment. A transaction in a foreign currency isn’t invalid — it might just need different enrichment or routing. Mixing validation and business logic in the same function makes both harder to reason about and test.

## Dead Letter Queues: Route Failures, Don’t Lose Them

A dead letter queue (DLQ) is a separate Kafka topic where failed records land for investigation and potential reprocessing.

In Flink, the DLQ pattern builds on side outputs. Records that fail validation get routed to a side output stream, enriched with failure metadata and written to a dead letter topic:

```java
SingleOutputStreamOperator<Transaction> validated = rawTransactions
    .process(new TransactionValidator());

DataStream<Transaction> invalidRecords = validated
    .getSideOutput(invalidTag);

invalidRecords
    .map(tx -> new DeadLetterRecord(
        tx,
        "validation_failure",
        Instant.now(),
        getValidationErrors(tx)
    ))
    .sinkTo(deadLetterSink);
```

What you include in the dead letter record matters. The original record is essential for reprocessing. The failure reason tells you why it failed. The timestamp lets you correlate with upstream events. But also capture the pipeline stage where the failure occurred and any enrichment context that was available at the time.

For a fraud detection pipeline, the DLQ typically has multiple categories rather than a single catch-all topic:
- **Schema failures** from deserialization errors
- **Validation failures** from null checks and business rules
- **Enrichment failures** from lookup timeouts or missing reference data
- **Processing errors** from unexpected exceptions in downstream logic

Separating by category lets you set different alerting thresholds and retention policies. Schema failures often point back to a producer bug, while enrichment failures suggest a database or reference data issue. Processing errors usually mean something in your own code needs attention.

### Managing the Dead Letter Queue

A DLQ that nobody monitors is just data loss with extra steps. The operational discipline around dead letters matters as much as the routing itself.

- Queue depth is the first thing to watch. A growing DLQ means something is wrong upstream, and the growth rate tells you how urgent the problem is. A sudden spike from a normal baseline of near-zero dead letters per minute to hundreds per second probably means a deployment went wrong. A gradual increase over days might indicate schema drift or a slow-moving data quality degradation.
- Alerting needs calibration. Don’t alert on every individual failure — background noise is normal in a high-volume system. A handful of dead letters per hour isn’t unusual. Alert on rate changes, not absolute counts.
- The DLQ also needs a regular review process. Investigate patterns in dead letter records: are the same validation rules failing repeatedly? Are failures concentrated on a particular upstream source? Patterns often reveal systemic issues worth fixing at the source rather than handling record by record.
- Finally, build replay capability. Some dead letter records failed due to transient issues — a momentary enrichment timeout, a brief network partition. Those records are perfectly valid and should be reprocessed once the transient issue resolves. Having a tool to selectively replay DLQ records back into the pipeline saves significant manual effort.

## Quality Metrics and Monitoring

Validation catches individual bad records. Monitoring catches systemic quality degradation — the slow decline in data quality that doesn’t trigger per-record validation failures but indicates something is going wrong.

Flink’s [metrics system](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/) provides counters, gauges and histograms that integrate with monitoring tools like Prometheus and Grafana. Embedding quality metrics directly in validation functions gives you real-time visibility into pipeline health:

```java
public class InstrumentedValidator 
        extends ProcessFunction<Transaction, Transaction> {
    
    private final OutputTag<Transaction> invalidTag = 
        new OutputTag<Transaction>("invalid-records") {};
    
    private transient Counter validRecords;
    private transient Counter invalidRecords;
    private transient Counter nullAccountId;
    private transient Counter invalidAmount;
    
    @Override
    public void open(OpenContext openContext) {
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        validRecords = metrics.counter("validation_passed");
        invalidRecords = metrics.counter("validation_failed");
        nullAccountId = metrics.counter("validation_null_account_id");
        invalidAmount = metrics.counter("validation_invalid_amount");
    }
    
    @Override
    public void processElement(
            Transaction tx, Context ctx, Collector<Transaction> out) {
        ValidationResult result = validate(tx);
        
        if (result.isValid()) {
            validRecords.inc();
            out.collect(tx);
        } else {
            invalidRecords.inc();
            for (String violation : result.getViolations()) {
                if (violation.contains("account_id")) nullAccountId.inc();
                if (violation.contains("amount")) invalidAmount.inc();
            }
            ctx.output(invalidTag, tx);
        }
    }
}
```

The specific counters for each failure type are what make this useful. Knowing that validation failures increased 10x is helpful. Knowing that null account IDs specifically increased 10x while all other validations are stable tells you exactly where to look. In production you’d likely use typed violation enums rather than string matching for categorization — this simplified version illustrates the metric pattern.

Four metrics form the core of a streaming quality dashboard:
- Validation pass rate — the percentage of records passing each validation stage — is the most important. A drop from 99.5% to 97% warrants investigation even if the absolute number of failures seems small at first glance.
- Failure category distribution complements the pass rate by showing which rules are failing most often, pointing you toward the upstream source of problems rather than just telling you something is wrong.
- Trend detection tracks whether pass rates are declining over time. A gradual slide from 99.8% to 99.2% over a week might not trigger a threshold alert, but the trend itself signals something is degrading.
- Validation latency confirms that your checks aren’t adding meaningful overhead to event processing — if validation starts taking milliseconds instead of microseconds, you need to know.

Alert on the pass rate dropping below a threshold rather than on individual failures. A threshold of 99% works for many systems, though the right number depends on your baseline and tolerance. Watch for new failure categories too — a validation rule that has never fired suddenly triggering might indicate a new type of data quality issue. DLQ growth rate matters more than absolute depth — a queue that grew from 0 to 1000 records in five minutes is more urgent than one that accumulated 1000 records over a week.

## Handling Corrections: When Bad Data Has Already Been Processed

Validation and dead letter routing handle bad data before it affects results. But what happens when bad data slips through and you discover the problem after the fact? An upstream bug sent wrong amounts for a thousand transactions. A batch of test data accidentally hit production. A merchant was incorrectly classified, affecting fraud scores across thousands of events.

There’s no universal answer. The right approach depends on how far the bad data propagated and what it affected.
- **Reprocess from Kafka**. If your Kafka topics have sufficient retention and the bad data window is relatively narrow, you can restart the Flink job from a savepoint taken before the bad data arrived. This works cleanly for stateless processing but gets complicated with stateful operations. The state in the savepoint reflects the pre-corruption state, so you’re effectively replaying a section of history. For truly stateless transformations, this is the simplest approach.
- **Send correction events**. Rather than reprocessing history, emit new events that reverse or update the effects of bad data. A correction event for a wrongly scored transaction might update the fraud flag and trigger a recalculation of downstream aggregates. This approach works when downstream systems understand corrections — and that means designing for corrections from the beginning, not bolting them on after the first incident.
- **State surgery with the State Processor API**. When bad data has corrupted Flink’s keyed state — wrong counts in velocity checks, incorrect aggregations — and reprocessing isn’t practical, you can use Flink’s [State Processor API](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/state_processor_api/) to read, modify and write state in a savepoint directly. This works but carries real risk. It requires stopping the job, modifying the savepoint offline and restarting. Any mistake in the state modification can introduce new corruption.
- **Accept and document**. Sometimes the cost of correction exceeds the cost of the error. If ten transactions were scored slightly wrong and no customers were impacted, documenting the incident and monitoring for recurrence might be the most practical response. Engineering time spent on a correction has opportunity costs too.

For the fraud detection system specifically, corrections often mean reversing fraud flags or recalculating risk scores. The downstream systems that consume fraud decisions — case management tools, customer notification services, account restriction logic — need to handle “this transaction was flagged incorrectly” as a first-class event type. Design that capability in early.

## Putting It Together: Fraud Detection Quality Patterns

Tying these patterns back to the fraud detection pipeline, the quality strategy has four components:
- Input validation runs first, checking required fields and value ranges before any enrichment or scoring touches the data. Every transaction must have an account ID, merchant ID, positive amount and a timestamp within a reasonable window. Currency codes must belong to a known set. Catching bad data here is cheap — the record hasn’t accumulated any state or triggered any downstream effects yet.
- Enrichment failure handling follows the strategy from Part 8, but from a quality perspective it also serves as a canary. Elevated enrichment failure rates are themselves a signal worth monitoring even when each individual failure is handled gracefully. A sudden spike in account lookup timeouts might mean the reference database is degrading — and that affects data quality across the entire pipeline, not just the records that explicitly fail.
- Dead letter routing captures failures from every layer, categorized by type across dedicated dead letter topics. The DLQ has its own monitoring: queue depth by category, growth rate and time-since-last-record for each category.
- The quality dashboard connects all of this. Validation pass rate by upstream source shows whether a specific producer is degrading. DLQ depth by category reveals where failures are accumulating. And fraud score distribution serves as an end-to-end quality signal — unusual shifts in the distribution of risk scores can indicate data quality issues even when per-record validation passes. That last metric is the one that would have caught the null merchant ID problem from the opening: the fraud score distribution shifted because unknown merchants were being scored as high risk.