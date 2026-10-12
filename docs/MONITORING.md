# Monitoring

## Starting Monitoring

```bash
make start-monitoring
```

This starts three containers (Prometheus, Grafana and kafka-exporter) using the `monitoring` Docker Compose profile.
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

## Grafana Dashboards

Three dashboards are provisioned automatically under the "Fraud Detection" folder.

### Fraud Detection (home dashboard)

The primary operational view. Shows alert rates, decision breakdown and pipeline health.

| **Panel** | **What It Shows** |
|-|-|
| Transactions Scored Per Second | Rate of scored transactions by the pipeline |
| Decision Breakdown | Pie chart of APPROVE / REVIEW / DECLINE distribution |
| Triggered Rules by Type | Which rules are triggering most often |
| Decision Rate Over Time | Time series of each decision type |
| Pipeline Consumer Lag | How far behind the pipeline is from the latest Kafka offset |
| Total Reviews | Counter of REVIEW decisions |
| Review + Decline Rate | Ratio of non-APPROVE decisions to total |
| Pipeline Throughput | Records Processed Per Second |
| Total Transactions Scored | Counter of scored transactions |
| Total Declines | Counter of DECLINE decisions |

### Flink Jobs

Flink cluster health and performance.

| **Panel** | **What It Shows** |
|-|-|
| Running Jobs | Number of jobs in RUNNING state |
| Records Processed Per Second | Records flowing through Flink operators |
| Checkpoint Duration (ms) | How long each checkpoint takes to complete |
| Checkpoint Size (bytes) | Size of checkpoint state |
| TaskManager Heap Memory | JVM heap usage on the TaskManager |
| GC Time (ms/s) | Garbage collection overhead |
| Backpressure (ms/s) | Time spent in backpressure per second |

### Kafka Overview

Kafka broker and topic metrics scraped via kafka-exporter.

| **Panel** | **What It Shows** |
|-|-|
| Messages In Per Second | Ingestion rate by topic |
| Consumer Group Lag | Per-partition lag for the fraud detection consumer group |
| Total Consumer Lag | Sum of all partition lags |
| Topic Partition Count | Number of partitions per topic |
| Messages Per Topic | Total message count (offset) per topic |
| Under-Replicated Partitions | Should always be 0 in single-node setup |

## Key Metrics

The most useful metrics for understanding pipeline behavior:

| **Metric** | **Source** | **What to Watch** |
|-|-|-|
| `alerts_total` | Flink custom counter | Should increase steadily at ~2% of input TPS |
| `alerts_decline` | Flink custom counter | Spike indicates fraud burst or model issue |
| Consumer group lag | kafka-exporter | Growing lag means pipeline can't keep up |
| Checkpoint duration | Flink metrics | Increasing duration signals growing state size |
| Backpressure ms/s | Flink metrics | Any value above 0 means an operator is slow |
| GC time | Flink JVM metrics | Above 200ms/s indicates memory pressure |

## Alert Conditions

In a production deployement, you would alert on these conditions:
- **Consumer lag > 10000** for more than 5 minutes - pipeline falling behind
- **Checkpoint duration > 30 seconds** - state may be too large or storage too slow
- **Backpressure > 500 ms/s** sustained - throughput bottleneck
- **alerts_decline rate > 10% of total** - possible attack or model drift
- **Zero alerts for > 60 seconds** while generator is running - pipeline may be stuck

## Prometheus Targets

Three scrape targets are configured in `monitoring/prometheus/prometheus.yml`:

| **Job** | **Target** | **Metrics** |
|-|-|-|
| `flink-jobmanager` | `flink-jobmanager:9249` | Job status, checkpoint stats |
| `flink-taskmanager` | `flink-taskmanager:9249` | Operator throughput, backpressure, custom counters, JVM |
| k`afka-exporter` | `kafka-exporter:9308` | Topic offsets, consumer group lag, partition counts |

Scrape interval is 15 seconds.

## Adding Custom Metrics

To add a new counter to the pipeline, register it in the `open()` method of any `RichFunction`:

```java
private transient Counter myCounter;

@Override
public void open(OpenContext openContext) {
    myCounter = getRuntimeContext().getMetricGroup().counter("my_metric_name");
}
```

Flink exposes registered metrics to Prometheus automatically via the `PrometheusReporterFactory` configured in the `FLINK_PROPERTIES`. New metrics appear in Prometheus within one scrape interval (15s) and can be added to Grafana dashboards from there.

## Stopping Monitoring

```bash
make stop-monitoring
```

Monitoring data is not persisted across restarts. Prometheus and Grafana use ephemeral storage.