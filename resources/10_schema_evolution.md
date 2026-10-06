# The Schema Change That Broke Production: Safe Evolution in Streaming

Link: https://medium.com/@blakelassiter/the-schema-change-that-broke-production-safe-evolution-in-streaming-508a2b3febbd

## Why Schema Matters More in Streaming

In batch systems, schema-on-read is common. You dump data into a lake, figure out the structure when you query it. If a column changes type, you update your query or transformation and reprocess. The feedback loop is slow but forgiving.

Streaming systems enforce schema-on-write. The producer serializes each message against a schema before sending it to Kafka. The consumer deserializes using its own copy of the schema. If those two schemas are incompatible, deserialization fails. And in a streaming pipeline, deserialization failures aren’t something you can quietly fix and re-run.
- Messages back up, consumer lag grows and alerts fire. If the consumer is a Flink job maintaining state for fraud detection, every minute of lag is a minute where fraudulent transactions go undetected.

The core challenge is version coexistence. During any rolling deployment, some producer instances are running old code with schema v1 while others have already restarted with schema v2. The Kafka topic contains a mix of both and consumers might be on either version. The schema needs to handle all four combinations: old producer to old consumer, old producer to new consumer, new producer to old consumer, new producer to new consumer.
- Batch systems sidestep this by processing data in discrete runs. Streaming systems don’t get that option.

## Choosing a Serialization Format

The serialization format determines how messages are encoded, how schemas are stored and what kinds of changes are safe. Avro, Protobuf and JSON Schema dominate the Kafka ecosystem, each with different tradeoffs for evolution.
- **[Avro](https://avro.apache.org/docs/++version++/specification/)** is Schema Registry’s native format and the most common choice for Kafka. Messages are compact binary, with the schema stored separately in the registry rather than embedded in each message. Each message carries only a 5-byte header (a magic byte plus a 4-byte schema ID). The consumer fetches the schema from the registry by ID, caches it and uses it for deserialization. Avro has the richest evolution support of the three formats, with explicit rules for which changes are backward-compatible and which are breaking. The tradeoff is that schemas must be registered before producing.
- **[Protobuf](https://protobuf.dev/)** uses generated code from .proto files and strong typing. Where Avro identifies fields by name, Protobuf identifies them by number - which means renaming a field is safe (the wire format never sees names) but reusing a field number will silently misinterpret old messages. If your schemas need to work across both Kafka and gRPC services, Protobuf's broader ecosystem support makes it a natural fit. [Schema Registry setup](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html) requires slightly more configuration than Avro.
- **[JSON Schema](https://json-schema.org/)** trades compactness for readability. You can inspect messages with a console consumer and no extra tooling, which makes debugging significantly easier. The cost is weaker evolution support, larger messages (field names repeated in every message) and less rigid schema enforcement — no binary encoding and no field numbering mean accidental breaking changes slip through more easily.

For our fraud detection pipeline, Avro with Schema Registry is the right choice. The combination of compact encoding, native Kafka integration and strong evolution rules outweighs the upfront cost of schema registration. The rest of this article focuses on Avro, though the compatibility concepts apply across all three formats.

## Schema Registry: The Coordination Layer

Schema Registry stores schemas organized by **subject**. By default, each Kafka topic has two subjects: `<topic>-value` for the message value schema and `<topic>-key` for the key schema. Our fraud detection pipeline's transaction topic uses the subject `transactions-value` for its Avro schema.

When a producer serializes a message, it registers the schema with the registry (or looks up the ID if the schema is already registered). The registry returns a numeric schema ID that the producer prepends to the serialized message bytes. Deserialization works in reverse: the consumer reads that schema ID from the message header, fetches the corresponding schema from the registry and uses it to decode the payload. After the first lookup for a given ID, the schema gets cached locally.

Understanding schema evolution requires distinguishing between the **writer schema** and the **reader schema**. The writer schema is whatever the producer used to serialize a given message — identified by the schema ID in the message header. The reader schema is the one the consumer was compiled against. [Avro’s schema resolution rules](https://avro.apache.org/docs/++version++/specification/#schema-resolution) reconcile differences between the two at deserialization time. Fields present in the writer but absent from the reader get ignored. Fields the reader expects but the writer didn’t include get populated from their default values — assuming defaults exist. Without a default, deserialization fails.

The registry also enforces compatibility rules. Before accepting a new schema version, it checks the proposed schema against existing versions according to the configured compatibility mode. If the check fails, registration is rejected. In production, this check typically happens in CI/CD. Schema registration failures should block deployment, not surface at runtime.

```java
// Schema Registry client for programmatic schema management.
// In production, schema registration typically happens in CI/CD pipelines
// rather than at runtime.
SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
    "http://schema-registry:8081",
    100  // schema cache capacity
);

// Register a new schema version for the transactions topic value
String subject = "transactions-value";
int schemaId = schemaRegistry.register(subject, new AvroSchema(transactionSchema));

// Test compatibility before registering - useful in CI/CD gates
boolean isCompatible = schemaRegistry.testCompatibility(
    subject, new AvroSchema(updatedSchema)
);
```

## Compatibility Modes

### BACKWARD Compatibility (the default)

### FORWARD Compatibility

### FULL Compatibility

### TRANSITIVE Variants

## Breaking vs Non-Breaking Changes

### Avro Evolution Rules

### Protobuf Evolution Rules

### JSON Schema Evolution

## Evolution Patterns

### Adding Fields Safely

### Deprecating Before Removing

### Widening Types

### The Versioned Topic Escape Hatch

## Producer-Consumer Version Skew


## Practical Application: Evolving the Fraud Detection Schema