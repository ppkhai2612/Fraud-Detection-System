# Getting Started

Everything you need to run the fraud detection pipeline from scratch.

## Prerequisites

Verify you have everything installed:

```bash
docker --version          # Docker 24+ recommended
docker compose version    # Docker Compose v2
java -version             # Java 17
mvn --version             # Maven 3.8+
python3 --version         # Python 3.11+
uv --version              # uv package manager
make --version            # GNU Make
```

Docker needs at least 8 GB of memory allocated. The full stack uses ~7 GB.

## Clone and Build

```bash
git clone https://github.com/ppkhai2612/fraud-detection-system.git
cd fraud-detection-system

# Build the Flink job JAR (compiles all modules, generates Avro classes)
make build
```

Expected output:

```

```

## Start the Infrastructure

```bash
# Start Kafka, Schema Registry, Flink and model service
make up

# Wait for all health checks to pass (up to 5 minutes)
make wait-healthy
```

This starts five core services:

| **Service** | **Port** | **Purpose** |
|-|-|-|
| Kafka | 9092 | Message broker (KRaft mode, no ZooKeeper) |
| Schema Registry | 8081 | Avro schema management |
| Flink JobManager | 8082 | Cluster coordinator and web UI |
| Flink TaskManager || Worker that runs pipeline operators |
| Model Service | 8000 | FastAPI ML scoring endpoint |

## Create Topics and Register Schemas

```bash
make setup
```

This creates seven Kafka topics and registers five Avro schemas with the Schema Registry. You can verify:

```bash
# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# List registered schemas
curl -s http://localhost:8081/subjects | python3 -m json.tool
```

## Deploy the Pipeline

```bash
make deploy-jobs
```

This builds the JAR (if not already built), copies it into the Flink JobManager container and submits it. Open the Flink Web UI at http://localhost:8082 to verify the job is in RUNNING state.

## Start the Generator

```bash
make start-generator
```

The generator produces 100 transactions per second with a 2% fraud rate. It also seeds account and merchant reference data on startup.

Check generator logs to confirm it is producing:

```bash
make logs-generator
```

Expect lines like `Produced 100 transactions (2 fraudulent)` repeating every second.

## Start Monitoring

## Verify the Pipeline

## Run the Demos

## Stop Everything

## Common First-Time Issues
