# Configuration

All pipeline configuration is driven by environment variables with defaults suitable for the Docker Compose setup. The Java pipeline reads these via `PipelineConfig.java`. The generator and model service have their own config modules.

## Pipeline Environment Variables

Set these on the `flink-taskmanager` container to override defaults.

| **Variable** | **Default** | **Description** | **When to Change** |
|-|-|-|-|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address | Connecting to a remote Kafka cluster |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Schema Registry endpoint | Connecting to a remote Schema Registry |
| `MODEL_ENDPOINT` | `http://model-service:8000/predict` | ML model scoring URL | Using an external model service |
| `MODEL_TIMEOUT_MS` | `2000` | Timeout for model HTTP requests (ms) | Model service is slow or on a remote host |
| `MODEL_MAX_CONCURRENT` | `100` | Max concurrent async model requests | Tune based on model service capacity |
| `REVIEW_THRESHOLD` | `0.3` | Score >= this triggers REVIEW | Adjusting sensitivity (lower = more reviews) |
| `DECLINE_THRESHOLD` | `0.7` | Score >= this triggers DECLINE | Adjusting sensitivity (lower = more declines) |

Source: `flink-jobs/common/src/main/java/com/frauddetection/common/config/PipelineConfig.java`

## Generator Environment Variables

Set these on the `generator` container.

| **Variable** | **Default** | **Description** |
|-|-|-|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry endpoint |
| `TRANSACTIONS_PER_SECOND` | `100` | Transaction generation rate |
| `FRAUD_RATE` | `0.02` | Fraction of transactions that are fraudulent |
| `NUM_ACCOUNTS` | `1000` | Number of synthetic accounts to generate |
| `NUM_MERCHANTS` | `200` | Number of synthetic merchants to generate |
| `SCHEMAS_DIR` | (auto-detected) | Path to Avro schemas directory |

Source: `generator/src/config.py`

The `generator` container in `docker-compose.yml` overrides `KAFKA_BOOTSTRAP_SERVERS` to `kafka:29092` and `SCHEMA_REGISTRY_URL` to `http://schema-registry:8081` for internal Docker networking.

## Kafka

### Kafka Broker Configuration

Kafka properties are set in the environment variables in `kafka` container (in `docker-compose.yml`), applied to broker-scope configuration.

| **Property** | **Value** | **Purpose** |
|-|-|-|
| `KAFKA_NODE_ID` | `1` | Unique identifier for node |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | KRaft mode, single node for both broker and controller |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:29093` | Single-voter quorum, required for KRaft mode |
| `KAFKA_LISTENERS` | `PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:9092` | Bind addresses |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092` | What the broker tells clients to connect to |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` | Listeners used by the controller, required for KRaft mode |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT` | No TLS |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `PLAINTEXT` | Listener name used for communication between brokers |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | The replication factor for the offsets topic (default is 3) |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `1` | The replication factor for the transaction topic (default is 3) |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `1` | Min no. replicas that must acknowledge a write in order to be considered successful |

### Kafka Topics

Topics are created by `scripts/setup-topics.sh`. All use replication factor 1 (single-node dev setup).

| **Topic** | **Partitions** | **Purpose** |
|-|-|-|
| `transactions` | `6` | Incoming transactions from the generator |
| `enriched-transactions` | `6` | Transactions enriched with account and merchant data |
| `account-updates` | `3` | Account reference data updates |
| `merchant-updates` | `3` | Merchant reference data updates |
| `alerts-high-risk` | `3` | DECLINE decisions |
| `alerts-review` | `3` | REVIEW decisions |
| `dead-letter` | `1` | Invalid transactions that failed validation |

The `transactions` and `enriched-transactions` topics have 6 partitions to match the default pipeline parallelism. Reference data topics have 3 partitions since they carry lower volume. The dead-letter topic uses 1 partition since invalid records are rare.

## Flink Configuration

Flink properties are set in the `FLINK_PROPERTIES` environment variable in `flink-*` containers (in `docker-compose.yml`), applied to both JobManager and TaskManager.

| **Property** | **Value** | **Purpose** |
|-|-|-|
| `jobmanager.rpc.address` | `flink-jobmanager` | JobManager network address |
| `execution.checkpointing.interval` | `10000` | How often Flink snapshots state |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | Checkpoint consistency guarantee |
| `state.backend.type` | `hashmap` | In-memory state backend (suitable for dev) |
| `restart-strategy.type` | `fixed-delay` | Restart jobs on failure |
| `restart-strategy.fixed-delay.attempts` | `30` | Max restart attempts before giving up |
| `restart-strategy.fixed-delay.delay` | `10s` | Wait between restart attempts |
| `metrics.reporter.prom.factory.class` | `PrometheusReporterFactory` | Expose metrics to Prometheus |
| `metrics.reporter.prom.port` | `9249` | Prometheus metrics endpoint port |

TaskManager only.

| **Property** | **Value** | **Purpose** |
|-|-|-|
| `taskmanager.numberOfTaskSlots` | `4` | Parallel task slots per TaskManager |
| `taskmanager.memory.process.size` | `2048m` | Total TaskManager process memory |

For production, switch `state.backend.type` to `rocksdb` for larger-than-memory state and configure incremental checkpoints to a distributed filesystem like S3 or HDFS.

## Rule Thresholds

Rule thresholds live as constants in each rule class. For production, externalize them to environment variables via `PipelineConfig`.

| **Rule** | **Threshold** | **Value** | **Class** |
|-|-|-|-|
|||||
|||||
|||||
|||||
|||||

## Score Fusion Weights

## Docker Resource Limits

Memory limits per container in `docker-compose.yml`:

| **Container** | **Memory Limit** | **Notes** |
|-|-|-|
| `kafka` | 1536 MB | KRaft mode, single node |
| `schema-registry` | 512 MB ||
| `flink-jobmanager` | 1024 MB ||
| `flink-taskmanager` | 2560 MB | Hold all pipeline state |
| `model-service` | 512 MB | FastAPI + sklearn model |
| `generator` | 256 MB | Python process |
| `kafka-exporter` | 128 MB | Monitoring profile only |
| `prometheus` | 512 MB | Monitoring profile only |
| `grafana` | 256 MB | Monitoring profile only |

Total without monitoring: ~6 GB. Total with monitoring: ~6.9 GB. Allocate at least 8 GB to Docker.