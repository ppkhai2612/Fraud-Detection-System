# Keeping It Running: Operations for Streaming Pipelines

Link: https://medium.com/@blakelassiter/keeping-it-running-operations-for-streaming-pipelines-e8ec6756d446

## The Metrics That Matter

Every streaming pipeline produces dozens of metrics through Flink’s built-in reporting and the Kafka consumer APIs. The most important five ones:
- **Consumer lag** is the metric that matters most. Lag measures how far behind your pipeline is from the latest data produced to Kafka.

    - A pipeline with zero lag is fully caught up. A pipeline with stable lag — even if it’s a few thousand messages — is healthy. A pipeline with growing lag has a problem, even if every other metric looks fine.
    - Lag works as a leading indicator because it captures the relationship between input rate and processing capacity. If your pipeline processes 50 events per second and the producer sends 50 per second, lag stays flat. If processing drops to 40 per second for any reason (slow model calls, garbage collection pauses, a downstream operator that can’t keep up), lag starts climbing immediately.
    - Lag is also useful because it’s a single number that summarizes pipeline health. A slow model service, a rule computation bogged down by growing state, checkpoint I/O competing for disk bandwidth — all of these show up as climbing lag. The cause differs each time but the symptom is consistent. Track lag by consumer group and by partition — if one partition’s lag grows while others stay flat, the problem is probably key skew rather than a system-wide issue.
    - [The Kafka monitoring documentation covers consumer group lag in detail](https://docs.confluent.io/platform/current/monitor/monitor-consumer-lag.html). In short, `consumer_lag` is the difference between the log-end offset (latest message produced) and the committed offset (latest message your consumer acknowledged). Track it per partition and per consumer group.

- **Checkpoint duration and size** come next.

    - Flink periodically snapshots the state of every operator in the pipeline (the accumulated transaction statistics in `AccountFeatureFunction`, the broadcast state holding account and merchant data, the in-flight async model requests). The [Flink checkpointing documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/) explains the mechanism
    - The operationally relevant number is the trend over time. Checkpoint duration that holds steady at 200ms is fine. Duration that creeps from 200ms to 500ms to 2 seconds over a week means state is growing or I/O is slowing. Failed checkpoints mean that if a TaskManager crashes, you lose data between the last successful checkpoint and the failure.

- **Processing latency** measures the time from event ingestion to output.

    - The p50 gives you the typical case, while the p99 shows what your slowest transactions experience. For fraud detection, a p99 that spikes from 50ms to 500ms might mean model calls are backing up. Average latency hides these tails — a p50 of 20ms and p99 of 2 seconds means 1% of transactions are waiting 100x longer than the median.

- **Throughput** should be stable and match your input rate.

    - A sudden drop from 50 events/second to 30 means something in the pipeline is rejecting or stalling on events. A gradual decline might indicate state accumulation slowing down processing.

- **Backpressure** shows up when a downstream operator can’t keep up with its input rate.

    - Flink propagates this pressure upstream through the network buffers — the source slows down because the sink (or an intermediate operator) is the bottleneck. The [Flink backpressure monitoring guide](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/) shows how to read the indicators in the Flink UI. The [Flink metrics reference](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/) documents the full set of available metrics.

## The Monitoring Stack

**Prometheus** scraping Flink metrics from both the JobManager and TaskManager on port 9249, a **Kafka exporter** for broker and consumer group metrics and **Grafana** with three pre-built dashboards.

The Grafana dashboards are at `localhost:3000` with default credentials. The configs live in `monitoring/grafana/dashboards/` and the [Prometheus configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#path) covers the scrape setup in `monitoring/prometheus/prometheus.yml`.
- The **Fraud Detection dashboard** (`fraud-detection.json`) is the one you'll spend the most time on. It shows transactions per second as the input rate, the decision breakdown across approve/review/decline as percentages, per-rule trigger rates and model latency as a histogram with p50, p95 and p99 lines. The model timeout rate panel is a quick health check for the ML integration - anything above zero sustained means the model service is struggling.
- The **Flink Jobs dashboard** (`flink-jobs.json`) shows job-level health: whether the job is running, checkpoint duration trends, records processed per operator and backpressure indicators. During normal operations, the checkpoint duration panel is the one to watch. During incidents, the backpressure panel tells you where the bottleneck is.
- The **Kafka Overview dashboard** (`kafka-overview.json`) covers consumer group lag, partition distribution and broker health. Consumer lag appears here and on the Fraud Detection dashboard - it's important enough to show in both places.

For reading dashboards, a flat line on consumer lag and steady checkpoint durations mean the pipeline is healthy. Any upward trend in either deserves investigation.

One pattern to watch for on the Fraud Detection dashboard: the decision breakdown percentages. Under normal traffic with the generator’s 2% fraud rate, you should see roughly 95% approve, 4% review and 1% decline. If those percentages shift without a corresponding change in input data, something in the scoring pipeline changed. Maybe a rule threshold moved, or the model service retrained, or an enrichment source went stale.

## Custom Metrics

Beyond the standard Flink and Kafka metrics, the pipeline registers 16 custom counters in `AlertBuilder`. These track the application-level behavior that infrastructure metrics can't see.
- Four **decision counters** track every transaction through the pipeline: `alerts_total`, `alerts_approve`, `alerts_review` and `alerts_decline`. A sudden shift in the approve/review/decline ratio - say, review jumping from 4% to 15% - indicates either a change in input data distribution or a rule threshold that needs attention.
- Six **alert type counters** record which detection methods triggered: `alerts_velocity`, `alerts_geo_anomaly`, `alerts_amount_anomaly`, `alerts_card_testing`, `alerts_blacklist` and `alerts_model_high_risk`. If `alerts_velocity` spikes while `alerts_geo_anomaly` stays flat, you know the traffic pattern changed - not the geographic profile.
- Six **rule trigger counters** track raw rule firings before fusion: `rules_velocity_triggered`, `rules_geo_anomaly_triggered`, `rules_amount_anomaly_triggered`, `rules_card_testing_triggered`, `rules_blacklist_triggered` and `rules_model_high_risk_triggered`. A high trigger rate with a low corresponding alert rate means the rule is contributing signal to the fused score but not dominating decisions on its own. That's usually the desired behavior.

All 16 counters appear on the Fraud Detection dashboard. The decision counters are useful for daily operations, while the alert type and rule trigger counters are more relevant during tuning sessions and incident investigations.

The distinction between alert type counters and rule trigger counters is subtle but operationally useful. A rule can trigger (contribute a non-zero score to the evaluation) without producing a specific alert type. The `rules_velocity_triggered` counter increments every time `VelocityRule` returns a non-zero score. The `alerts_velocity` counter increments only when velocity is the dominant alert type in the final decision. Comparing the two tells you whether a rule is firing frequently but being outweighed by other signals in fusion - which is normal - or whether it's both firing and driving decisions - which might mean thresholds need adjustment.

## Demo Scenarios

The repo includes four demo scenarios that break things on purpose. Each one demonstrates a failure mode that shows up in production.

```bash
make demo-lag          # Traffic spike at 1000 TPS
make demo-model-down   # Model service outage for 30s
make demo-recovery     # TaskManager crash and restore
make demo-backpressure # CPU throttle to 10% for 30s
```

Run these with the pipeline and monitoring stack already up. Have Grafana open in one browser tab and the Flink UI at `localhost:8082` in another.

### Demo: Lag Spike

`make demo-lag` floods the pipeline at 1000 TPS for 30 seconds. Normal throughput is 100 TPS, so this is a 10x burst.

Open the Kafka Overview dashboard in Grafana before running the demo. The consumer lag panel is the one to watch. Within seconds of the burst starting, lag climbs. The Flink pipeline is still processing at its maximum rate, but input is arriving 10 times faster than it can be consumed. The gap between production and consumption grows as a visible ramp on the chart.

After the burst ends at 30 seconds, watch the lag panel. The pipeline continues processing at full speed, now draining the backlog. Lag decreases in a steady downward slope until it returns to zero. The time it takes to drain tells you your headroom — how much burst capacity the pipeline has before lag becomes a problem.

```
Headroom calculation:
    Burst rate: 1000 TPS
    Processing rate: ~100 TPS
    Burst duration: 30s
    Messages accumulated: ~(1000 - 100) × 30 = ~27,000
    Drain time at 100 TPS: ~27,000 / 100 = ~270s (~4.5 minutes)
```

If lag doesn’t recover after the burst, your steady-state throughput is below your actual input rate. You need more parallelism or faster processing.

The lag spike demo is worth watching closely because of what the other metrics do during the burst. Throughput looks high — the pipeline is working at full capacity. Latency per event might look fine because each individual event processes quickly. Error rates stay at zero because nothing is actually failing. But lag is climbing because the pipeline is falling behind. In production, traffic bursts happen regularly — marketing campaigns, batch uploads from partner systems, time-zone-based usage peaks. Whether lag recovers afterward is what tells you if the pipeline is correctly sized.

### Demo: Model Outage

`make demo-model-down` stops the `model-service` container for 30 seconds, then restarts it. This simulates what happens when an external dependency goes offline while the pipeline keeps running.

The model timeout rate on the Fraud Detection dashboard spikes immediately to 100%. Every model call fails. But the pipeline doesn’t stall — transactions continue flowing through validation, enrichment, feature computation and rule scoring. The `AsyncModelClient` timeout fires at 500ms and returns a null model score. `WeightedScoreFusion` renormalizes, redistributing weight across the five rules that did return scores.

The decision distribution shifts during the outage. Without the model contributing its 20% weight, the fused scores change. Some transactions that would have been DECLINE under the full scoring pipeline might become REVIEW under rule-only scoring, or vice versa. The `alerts_model_high_risk` counter drops to zero while the model is down, then resumes when the service returns.

That’s the `AsyncModelClient` fallback pattern. Without explicit timeout handling, model latency spikes would cause backpressure that propagates through the entire pipeline - the async function would hold events waiting for responses that never come, eventually filling buffers and stalling sources. With the timeout, slow and failed model calls get null scores and the pipeline absorbs the impact by adjusting the fusion math.

After the model service restarts, the timeout rate drops back to zero within a few seconds and normal scoring resumes. No transactions were lost during the outage — they were scored with reduced information and the pipeline kept running. On the dashboard, model timeout rate tells you the model is down, and the decision distribution shift tells you how much the scoring changed.

### Demo: Checkpoint Recovery

`make demo-recovery` captures the current job state, kills the TaskManager container and watches the pipeline recover. This is the scenario closest to a real production incident - an unexpected process death followed by automatic recovery from checkpointed state.

In the Flink UI, the job transitions from RUNNING to RESTARTING when the TaskManager disappears. The JobManager detects the lost TaskManager and triggers the configured restart strategy: fixed-delay with 30 maximum attempts and 10 seconds between retries. A new TaskManager container spins up, registers with the JobManager and the job restores from the last successful checkpoint.

The checkpoint contains all the accumulated state: the rolling transaction statistics in `AccountFeatureFunction`'s eight state descriptors, the broadcast account and merchant data, the in-flight processing context. Flink restores this state from the checkpoint rather than rebuilding it from the raw Kafka data. The pipeline resumes processing from the Kafka offsets committed with that checkpoint.

The gap between the last successful checkpoint and the moment of failure is the reprocessing window. With the repo’s checkpoint interval of 1.5 seconds, that window is small — some events may process twice during recovery. This is the at-least-once guarantee within the pipeline. For the fraud detection use case, a duplicate score on the same transaction during a brief recovery window is acceptable. Downstream consumers can deduplicate on `transaction_id` if exact-once matters.

The numbers to watch during recovery:

```
Checkpoint interval: 1.5s
Restart delay: 10s (between attempts)
Max restart attempts: 30
Typical recovery time: 15-30 seconds (TaskManager startup + state restoration + processing resume)
Reprocessing window: up to 1.5s of events
```

Shorter checkpoint intervals reduce the reprocessing window but add overhead during normal operation. The 1.5-second interval in this repo is aggressive for a demo environment — production systems commonly use 30 to 60 seconds, accepting a larger reprocessing window in exchange for less checkpoint overhead on throughput.

The restart strategy configuration matters too. Thirty attempts with 10-second delays gives the pipeline 5 minutes of retries before it gives up. For transient failures like a container restart or network blip, that’s generous. For a persistent problem like a corrupted state backend, the job will exhaust its retries and enter FAILED state — which is when alerting needs to fire.

### Demo: Backpressure

`make demo-backpressure` throttles the TaskManager to 10% CPU for 30 seconds using `docker update --cpus 0.1`, which drops processing throughput to a fraction of its normal rate.

The Flink UI at `localhost:8082` is where backpressure becomes visible. The backpressure indicators on upstream operators turn from green to red. The pattern propagates backward: the sink operators slow down first, then the scoring operators, then enrichment, then the source. Each operator in the chain shows increasing backpressure as the bottleneck - the CPU-starved TaskManager - limits throughput.

On the Grafana dashboards, throughput drops and consumer lag starts climbing — the pipeline is falling behind because it can’t process events fast enough under the CPU constraint. After 30 seconds, the CPU limit is removed, backpressure indicators clear, throughput recovers and lag starts draining.

When debugging backpressure in production, the approach is to start at the sink and work backward through the operator chain. The operator with HIGH backpressure whose downstream neighbor has LOW backpressure is the bottleneck. In this demo, every operator shows high backpressure because the CPU constraint affects all processing uniformly. In production, backpressure is usually concentrated at one operator — a slow external call, an expensive computation or an overloaded sink.

The backpressure demo also demonstrates the relationship between backpressure and consumer lag. They’re connected but they measure different things. Backpressure is the internal pipeline mechanism — Flink slowing down upstream operators to prevent buffer overflow. Consumer lag is the external measurement — how far behind the pipeline is from the latest available data. In this demo, backpressure causes lag. In the lag spike demo, high input rate causes lag without backpressure. The distinction matters for choosing a fix: backpressure means the pipeline needs more resources or optimization, while lag without backpressure means the input rate exceeded capacity temporarily but the pipeline itself is healthy.

## Alerting Philosophy

Not every metric belongs in an alert. Some things need immediate attention. Others are worth reviewing during normal working hours but don’t justify paging someone at 2am.
- **Alert on these** — they indicate active problems that affect users or risk data loss: Consumer lag exceeding a time-based threshold (minutes behind, not message count), checkpoint failures persisting for more than one cycle, the Flink job not running and model timeout rate above 10% sustained over 5 minutes.
- **Dashboard only** — these indicate trends worth investigating but not emergencies: Lag trending upward slowly, checkpoint duration increasing week over week, individual rule trigger rates shifting, p99 latency creeping up.

The two lists come down to urgency. Growing checkpoint duration means something changed in state size or I/O patterns — worth a look on Monday. Checkpoint failures mean the pipeline can’t recover from a crash right now — that’s a page.

Three principles reduce alert fatigue.
- First, alert on symptoms rather than causes. Consumer lag climbing is a symptom that captures dozens of possible causes. Alerting on individual causes means dozens of alert rules that each miss some failure modes.
- Second, use rate-of-change thresholds instead of absolute values. A lag of 100,000 messages might be normal for a high-volume pipeline and catastrophic for a low-volume one. A lag that doubled in the last 10 minutes is concerning regardless of the absolute number.
- Third, aggregate before alerting. A single checkpoint timeout could be a transient blip, but three consecutive failures indicate a real problem. A 5-minute window for model timeout rate filters out momentary spikes.