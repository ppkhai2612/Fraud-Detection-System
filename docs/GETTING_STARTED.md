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

```bash
Flink jobs built successfully.
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

```bash
make start-monitoring
```

Open Grafana at http://localhost:3000 and log in with admin/admin. Three dashboards are pre-provisioned under the "Fraud Detection" folder:
- **Fraud Detection** - alert rates, decision breakdown, fraud rate
- **Flink Jobs** - checkpoint duration, backpressure, memory usage
- **Kafka Overview** - messages per second, consumer group lag

Wait about 30 seconds after starting the generator for metrics to populate.

## Verify the Pipeline

Check that transactions are flowing through the system:

```bash
# Watch Flink TaskManager logs for alert output
docker logs -f flink-taskmanager 2>&1 | grep ALERT
```

Expect output like:

```txt
ALERT: tx=abc123 decision=APPROVE score=0.042 type=VELOCITY model=0.031
ALERT: tx=def456 decision=REVIEW score=0.451 type=GEO_ANOMALY model=0.380
ALERT: tx=ghi789 decision=DECLINE score=0.892 type=BLACKLIST model=0.710
```

## Run the Demos

Four demo scenarios show how the pipeline handles failure conditions:

```bash
make demo-lag            # Traffic burst, consumer lag spike
make demo-model-down     # ML service outage, rule-only fallback
make demo-backpressure   # CPU throttling, backpressure propagation
make demo-recovery       # TaskManager crash, checkpoint recovery
```

See [demo/README.md](../demo/README.md) for detailed descriptions of what to watch during each demo.

## Stop Everything

```bash
make down
```

This stops all containers including the generator and monitoring services. To also remove volumes and build artifacts:

```bash
make clean
```

## Common First-Time Issues

- **Docker memory too low**: The stack needs ~7 GB. If containers get OOMKilled, increase Docker Desktop memory or set `memory=8GB`
- **Port 8082 already in use**: Another service is binding to the Flink UI port. Check with `lsof -i :8082` and stop the conflicting process or change the port mapping in `docker-compose.yml`.
- **Java version mismatch**: The project requires Java 17. If `mvn clean package` fails with compilation errors, verify `java -version` shows 17 and that `JAVA_HOME` points to the correct installation.
- **Schema registration fails**: Schema Registry must be healthy before `make setup-schemas` runs. If it fails, wait for the health check to pass (`make wait-healthy`) and retry.
- **Generator exits immediately**: The generator needs both Kafka and Schema Registry to be healthy. Make sure `make wait-healthy` completed before starting the generator.