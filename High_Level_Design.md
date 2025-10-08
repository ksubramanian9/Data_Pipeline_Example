# High Level Design: Retail Transactions Data Pipeline

## Purpose and Scope
This document summarises the major building blocks of the demo retail analytics platform. It highlights how synthetic transaction data moves through batch and streaming workflows, the technologies used, and the way business users access insights. The intent is to offer a conceptual map for stakeholders before they dive into the implementation details captured in `Detailed_Design.md`.

## Architecture at a Glance
- **Data origins** – Synthetic CSV files generated to mimic point-of-sale transactions.
- **Processing engines** – Apache Spark jobs run in batch and streaming modes to cleanse, enrich, and aggregate the data.
- **Data transport** – Files land in shared storage (local folders or HDFS). Kafka carries near-real-time events for the streaming pipeline.
- **Serving layer** – Curated outputs are exposed through lightweight Flask dashboards and can be reused by external tools.
- **Container orchestration** – Docker Compose brings the services online with predefined wiring and volumes.

```
+-----------------+      +-------------------+      +------------------+
| Synthetic data  | ---> | Spark batch job    | ---> | Curated batch     |
| generator (CSV) |      | (daily aggregation) |     | datasets (Parquet |
|                 |      |                     |     | + CSV)            |
+-----------------+      +-------------------+      +------------------+
         |                          |                             |
         |                          v                             v
         |                +-----------------+            +-------------------+
         |                | Kafka producer  | ---------> | Spark streaming   |
         |                | (replay CSV)    |            | job (hourly views)|
         |                +-----------------+            +-------------------+
         |                          |                             |
         v                          v                             v
+-----------------+      +-------------------+      +------------------+
| Shared storage  |      | Kafka topic       |      | Streaming output |
| (local & HDFS)  |      |                   |      | parquet datasets |
+-----------------+      +-------------------+      +------------------+
                                      |
                                      v
                            +---------------------+
                            | Dashboards & tools   |
                            | (batch + streaming)  |
                            +---------------------+
```

## Data Flow Summary
1. **Ingestion** – The batch generator emits one CSV per day with product, store, and pricing fields. For streaming demos, the same files are replayed as JSON events through Kafka.
2. **Processing** – Spark normalises schemas, computes monetary amounts, derives daily totals (batch), and maintains rolling one-hour revenue windows (streaming).
3. **Storage** – Clean data is written to HDFS partitions for durability and to local bind mounts for dashboards. Streaming results land in a dedicated Parquet folder with checkpointing for recovery.
4. **Consumption** – Two Flask dashboards poll the curated folders to render charts and KPI tiles for batch and streaming outputs.

## Operational Considerations
- **Deployment modes** – Compose files spin up either the batch stack or the streaming stack. Services share network aliases and volumes to keep data paths consistent.
- **Scalability** – The demo keeps data volumes modest, but Spark’s distributed runtime allows vertical or horizontal scaling if the container resources increase.
- **Reliability** – Built-in monitoring relies on Spark UIs and logging. Failures surface through container logs, while checkpointing preserves streaming progress.
- **Extensibility** – Additional sources, transformations, or BI tools can be attached by pointing them at the curated Parquet/CSV outputs or the Kafka topic.

Readers looking for step-by-step logic, schemas, and configuration specifics should continue with `Detailed_Design.md`.
