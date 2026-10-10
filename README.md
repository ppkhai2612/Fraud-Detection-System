# Fraud Detection System

A real-time fraud detection pipeline built with Apache Flink 2.2 and Kafka 4.1. Transactions flow through validation, enrichment with account and merchant data via broadcast state joins, per-account feature computation, a five-rule scoring engine, async ML model scoring and weighted score fusion - producing approve, review or decline decisions in milliseconds.

## Architecture

![](docs/images/architecture.svg)

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full fraud detection pipeline.

## Quick Start

### Prerequisites

- Docker and Docker Compose v2
- Java 17
- Maven 3.8+
- Python 3.11+ with uv
- Make

### Run the Pipeline

```bash
# Start Kafka, Schema Registry, Flink and model service
make up && make wait-healthy

# Build the Flink job JAR
make build

# Create Kafka topics and register Avro schemas
make setup

# Deploy the pipeline to Flink
make deploy-jobs

# Start Prometheus, Grafana and kafka-exporter
make start-monitoring

# Start the transaction generator (100 TPS, 2% fraud rate)
make start-generator
```

Open http://localhost:3000 (Grafana - `admin`/`admin`) to watch transactions flow through the pipeline.

### Try the Demos

```bash
make demo-lag            # 10x traffic burst, watch consumer lag spike and recover
make demo-model-down     # Kill ML service, watch rule-only fallback
make demo-backpressure   # Throttle TaskManager CPU, observe backpressure propagation
make demo-recovery       # Kill TaskManager, watch checkpoint recovery restore state
```

See [demo/README.md](demo/README.md) for detailed walkthroughs.

## What It Demonstrates

| **Concept** | **Implementation** |
|-|-|
| Broadcast state joins | Account and merchant enrichment via `KeyedBroadcastProcessFunction` |
| Keyed state with rolling stats | Per-account feature computation (velocity, avg amount, stddev) |
| Async I/O	| ML model scoring with timeout fallback to rules-only |
| Side outputs | Dead-letter routing for invalid transactions |
| Custom Flink metrics | Alert counters by decision type and rule trigger |
| Avro + Schema Registry | All Kafka serialization with schema evolution support |
| Checkpoint recovery | Exactly-once processing with automatic state restoration |
| Score fusion | Weighted combination of rule and model scores with renormalization |

## Fraud Detection Rules

| **Rule** | **Detects** | **Score Range** |
|-|-|-|
| Velocity | High transaction frequency (>5 in 5-min window) | 0.0 - 1.0 |
| Geo Anomaly | Impossible travel (>900 km/h between transactions) | 0.0 / 0.5 / 0.9 |
| Amount Threshold | Statistical outliers via z-score against account history | 0.0 / 0.2 / 0.5 / 0.9 |
| Card Testing | Multiple small probing transactions (<$1 or <$5) | 0.0 / 0.6 / 0.8 |
| Blacklist | Blacklisted merchants, suspended accounts, high-risk flags | 0.0 / 0.4 / 0.7 /1.0 |

Rule scores are fused with the ML model score using configurable weights. See [Score Fusion](docs/ARCHITECTURE.md#score-fusion) for scoring details.

## Project Structure

```bash
.
├── demo/                       # Interactive demo scripts for failure scenarios
├── docker-compose.yml          # Docker Compose file for infrastructure management
├── docs/                       # Directory containing .md documents
├── flink-jobs
│   ├── common/                 # Shared config, Avro serde factory, geo utilities
│   ├── enrichment/             # Transaction validation, account and merchant broadcast joins
│   ├── feature-computation/    # Per-account rolling statistics and velocity tracking
│   ├── model-scorer/           # Async I/O client for ML model service
│   ├── pipeline/               # Main pipeline assembly
│   ├── pom.xml
│   ├── rule-engine/            # Five fraud detection rules with scoring interface
│   └── score-fusion/           # Weighted score fusion, decision routing, alert building
├── generator/                  # Configurable transaction generator with fraud patterns
├── Makefile                    # Makefile
├── model-service/              # FastAPI ML model service with sklearn classifier
├── monitoring/
│   ├── grafana/                # Grafana dashboards
│   └── prometheus/             # Prometheus config
├── README.md                   # README for project
├── schemas/                    # Schema definitions for all Kafka topics
├── scripts/                    # Setup scripts for topics, schemas and test data
```

## Configuration

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for the full reference.

## Monitoring

Three Grafana dashboards cover the full stack: pipeline alerts and decisions, Flink job health and Kafka topic throughput. See [docs/MONITORING.md](docs/MONITORING.md) for details.