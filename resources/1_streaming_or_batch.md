# Streaming vs Batch: What I Learned the Hard Way

Link: https://medium.com/@blakelassiter/streaming-vs-batch-what-i-learned-the-hard-way-8a8d4921a896

Before diving into Kafka and Flink implementation, it’s important to understand two things: when streaming earns its complexity and the foundational concepts batch engineers need to make sense of it.

## The Streaming Tax

Every streaming pipeline carries ongoing costs that batch pipelines don’t:
- **Operational burden**. Streaming systems run continuously. They can fail at any moment. A batch job that runs once a day has one failure window per day. A streaming pipeline has a failure window every second.

- **Debugging complexity**

    - Batch job failures tend to be localized. A staging job failed on a schema change in the source API, the logs show the missing column, you update the model and rerun from the failed step.
    - Streaming failures are different. One incident that took most of a day to diagnose: a slow consumer traced back to a schema change from the previous week that added a nested field, increasing average message size by 15%. That increase was fine at normal volumes but crossed a threshold during a traffic spike, causing backpressure - upstream slowdown from a downstream bottleneck - that cascaded into checkpoint timeouts. The alert said “checkpoint failed.” The actual problem was a schema change five days earlier.

- **Infrastructure cost**

    - Batch infrastructure scales to zero between runs. A Snowflake warehouse spins down when the job finishes. An EMR cluster terminates. The meter stops.
    - Streaming infrastructure runs continuously. Kafka brokers - the servers that store and serve streaming data - stay available 24/7 even when traffic drops to near-zero overnight. Flink clusters maintain state and checkpoints around the clock. The monitoring stack never sleeps.
    - A batch pipeline that costs $50 per daily run adds up to roughly $1,500/month. A streaming pipeline processing similar data volumes might run $3,000–8,000/month depending on redundancy requirements and cloud provider. The exact numbers depend on scale but the pattern holds: streaming costs more to keep running.

- **Skill requirements**. Batch has been the dominant paradigm in data engineering for good reason - it handles the majority of use cases. That means fewer opportunities for engineers to gain hands-on streaming experience.

## How Streaming Differs from Batch

> Concepts in streaming

- **Event time versus processing time**

    - In batch, a job processes the last hour’s data after that hour ends - when events happened and when they’re processed are clearly separated but it doesn’t affect the logic.
    - In streaming, the distinction matters constantly. A transaction might happen at 11:59pm but arrive at the processing system at 12:03am - network delays, batched mobile uploads, system backlogs. Which day does it belong to? If the logic cares about when events happened, that’s event time. If it cares about when the system saw them, that’s processing time. Most business logic wants event time. And since events don’t arrive in order - an 11:58pm event might arrive after the 11:59pm one - streaming systems have to handle that too.
    
- **Watermarks**
    
    - If events can arrive late, how does the system know when it’s safe to finalize results for a time period? A batch job processing yesterday’s data can assume all of yesterday’s data is present. A streaming job processing “the last hour” has no such guarantee - an event from 45 minutes ago might still be in transit.
    - Watermarks track progress through the stream - a way of marking “everything up to this timestamp has probably arrived.” They’re heuristics, not guarantees. Tuning them means choosing between faster results and more complete results.

- **State**
    
    - Many batch jobs are stateless. Read the input, transform it, write the output, done. Streaming jobs often need to remember things between events.
    - Counting transactions per customer over a rolling window requires remembering the running count. Detecting fraud patterns requires remembering recent transaction history. Joining two streams requires buffering events from one while waiting for matching events from the other. This state must be stored somewhere, updated with each event and recovered correctly if the system fails.

- **Checkpoints**
    
    - When a batch job fails, rerunning it from the beginning is usually feasible - the input data is finite and sitting in storage. When a streaming job fails, reprocessing from “the beginning” might mean days or weeks of data, which isn’t practical.
    - Checkpoints solve this by taking periodic snapshots - the job’s state and its position in the input streams. On recovery, the job picks up from the last checkpoint instead of starting over. More frequent checkpoints mean less reprocessing after failures but the snapshots themselves consume resources.

- **Exactly-once semantics**

    - If a job fails after processing an event but before recording that it was processed, the event might get processed again after recovery. In batch, duplicates from a rerun can often be handled by making the output idempotent or simply overwriting previous results. In streaming, results may have already triggered downstream actions - an alert went out, a record was updated, money moved. Duplicates cause real problems.
    - Exactly-once semantics guarantees each event affects output only once, even through failures. Getting there requires coordination between the stream processor, its sources and its destinations. Modern frameworks support it but it adds complexity.

- **Consumer groups and rebalancing**. Kafka splits topics into partitions - parallel lanes for data. Each partition gets assigned to exactly one consumer in the group, so you can scale throughput by adding consumers up to the number of partitions. But when consumers join or leave the group (scaling, deployments, failures), partitions must be reassigned. This rebalancing can cause brief processing pauses and, if not handled carefully, can lead to duplicates or gaps.

- **Consumer lag and backpressure**. Kafka tracks how far behind each consumer is from the latest available data - this gap is consumer lag. Some lag is normal but growing lag indicates consumers can’t keep up with producers. In severe cases, slowness propagates backward, potentially slowing producers or causing data to buffer in ways that stress the system. Monitoring consumer lag is often the first indicator of streaming pipeline health.

## When Batch Wins

- **Analytics and reporting**. Most analytical queries don’t need data from the last five minutes. A dashboard showing yesterday’s sales supports the same business decisions as one showing sales from 30 seconds ago.
- **Machine learning training**
    
    - Model training is inherently batch-oriented - processing a fixed dataset, iterating multiple times, computing gradients, evaluating loss functions. The training loop doesn’t benefit from events arriving one at a time.
    - Even organizations serving predictions in milliseconds typically train models on batch pipelines that run daily or weekly. Training data gets assembled through batch ETL. Feature engineering happens in batch. Model evaluation happens in batch. Only serving requires low latency.

- **Data warehouse loads**. Moving data from operational systems into an analytical warehouse rarely requires sub-minute latency. Analysts aren’t waiting for transactions from 30 seconds ago. They’re running reports on yesterday’s data, last week’s trends, this quarter’s patterns.
- **Cost-sensitive environments**. When infrastructure budget is tight, batch may be the only realistic option. A startup burning $4,000/month on streaming infrastructure for a use case that doesn’t require sub-minute latency is burning runway unnecessarily.

## When Streaming Makes Sense

- **Low-latency decisions**

    - Fraud detection makes the case clearly. By the time an hourly batch job flags a fraudulent transaction, the money has already moved. Prevention means deciding in milliseconds, before the transaction clears.
    - Real-time bidding has the same constraint - the auction closes in 100ms. Dynamic pricing too: the customer is on the checkout page now.

- **Continuous data sources**. Some data just arrives continuously - clickstreams, sensor readings, application logs, market data. Batching it into files and processing on a schedule works but it adds latency and fights the natural shape of the data.
- **Event-driven architecture**. When multiple downstream systems need to react to events as they happen, streaming provides the connective tissue. A user signup triggers a welcome email, creates a CRM record, initializes preferences and logs the conversion. Each downstream system subscribes independently. Polling a database for changes works but adds latency and tight coupling. With streaming, the source publishes once and consumers subscribe as needed.
- **High-volume ingestion**. At sufficient scale, batch becomes impractical. Processing 10 billion events per day in hourly batches means each job handles 400+ million records. Jobs that size take hours, require substantial compute and become fragile. Continuous streaming spreads ingestion evenly across time. Resource utilization is smoother. Failures affect smaller windows. Recovery is faster.

## Making the Call

The decision comes down to a few factors that can be evaluated before writing code:
- **Latency requirements**. This is the primary driver. Sub-minute latency generally requires streaming. Latency measured in hours calls for batch. The 5–60 minute range is where micro-batch approaches can fit.
- **Data source characteristics**. Continuous sources fit streaming. Periodic sources fit batch. API calls and database change capture are continuous. Vendor exports and partner data refreshes arrive on schedules - daily, monthly, quarterly.
- **Team experience**. A team building its first streaming pipeline will face a learning curve on the concepts covered earlier in this article. Has anyone operated a production Kafka cluster? Debugged consumer lag under pressure? Understood when exactly-once semantics apply? If not, building a first streaming pipeline while delivering a critical business project is risky.
- **Failure recovery**. Batch failures are typically contained - the job failed, the logs explain why, a rerun fixes it. Streaming failures can cascade. A slow consumer backs up messages. Backpressure propagates upstream. Checkpoints grow. The job eventually crashes and recovery requires understanding what was committed, what was in flight and whether replay will create duplicates.
- **Organizational timeline**. Streaming pipelines take longer to build, stabilize and earn trust. If the organization needs production results in two weeks, batch is the realistic path. Rushed streaming projects tend to create more problems than they solve.

## Architecture Patterns

> Once you’ve decided streaming fits, the next question is how batch and streaming coexist. Two patterns

- **Lambda architecture** runs batch and streaming in parallel. The batch layer processes complete historical data with higher latency. The streaming layer provides fast results that may be approximate or incomplete. A serving layer - the component that responds to queries - merges outputs, preferring batch when available and falling back to streaming for recent data.

    - Lambda was popular when streaming frameworks were less mature and exactly-once semantics were unreliable. The downside: maintaining two codebases doing similar work, keeping their outputs compatible and reconciling differences.

- **Kappa architecture** runs everything through streaming. Historical reprocessing replays from Kafka rather than running separate batch jobs. One codebase, no reconciliation problem.

    - Kappa requires confidence in streaming pipeline correctness - no batch layer catches bugs. It also requires sufficient Kafka retention to replay historical data when needed.

## The Cost Question

**Batch scales with execution time**. A 30-minute daily job uses resources for 30 minutes. The other 23.5 hours, those resources scale to zero or serve other work.

**Streaming scales with uptime**. Kafka brokers run 24/7 regardless of traffic. Flink clusters maintain state continuously. The meter runs around the clock.

At low volumes, streaming is expensive relative to batch. A pipeline processing 10,000 events daily still needs always-on infrastructure, while batch could handle the same work in seconds.

At high volumes with tight latency requirements, streaming becomes more competitive. The batch alternative would require oversized jobs running frequently.

A rough heuristic: under a million events per day with latency tolerance above 15 minutes, batch is usually simpler and cheaper. At billions of events per day with sub-minute latency needs, streaming becomes necessary regardless of cost.

Hidden costs matter too. Engineering time debugging streaming issues. On-call burden affecting retention. Slower feature development while managing infrastructure.