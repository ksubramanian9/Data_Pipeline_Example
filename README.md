# Retail Transactions Data Pipeline Demo

This repository contains a self-contained demo that showcases a small retail
analytics platform built on top of **Hadoop**, **Spark**, **Kafka**, and two
Flask dashboards. Running the Docker Compose stacks generates synthetic
transactions, loads them into HDFS, aggregates them with Spark (batch and
Structured Streaming), and serves the resulting insights via web UIs.

The project is suitable for workshops or classroom walkthroughs where you want
to show how batch and streaming data products are assembled end-to-end.

## Prerequisites

* Docker Desktop or the Docker Engine (with Docker Compose v2)
* At least 8 GB RAM available for containers
* Python 3.10+ if you want to run the helper scripts locally (optional)

Clone the repository and open a shell in the project root before running any of
the commands below.

## Repository layout

```
.
├─ README.md                       # This guide
├─ docker-compose.batch.yml        # Batch processing stack (Spark + HDFS + dashboard)
├─ docker-compose.streaming.yml    # Streaming stack (Kafka + Spark Structured Streaming)
├─ .env                            # Shared Docker image tags (Spark, Confluent Platform)
├─ conf/                           # Hadoop configuration mounted into the containers
├─ services/
│  ├─ batch/
│  │  ├─ generate_synthetic_data.py    # Creates CSV inputs for the batch job
│  │  └─ pipeline_batch.py             # Spark ETL + aggregation (batch)
│  ├─ event-generator/
│  │  ├─ Dockerfile
│  │  └─ kafka_event_producer.py       # Streams CSV rows into Kafka
│  └─ streaming/
│     └─ streaming_sales_aggregator.py # Spark Structured Streaming job
├─ dashboard/                      # Flask + Chart.js dashboard for batch outputs
├─ streaming_dashboard/            # Flask dashboard for streaming aggregates
├─ cleanup_demo_data.py            # Utility to clear generated data/volumes
└─ Detailed_Design.md              # Deep dive into the architecture
```

> **Note:** The folders that collect runtime data (`data/`, `hdfs/`,
> `checkpoints/`, and `spark-events/`) are git-ignored. Docker Compose will
> create them automatically when the stack starts. You can also create/clear
> them manually with `python cleanup_demo_data.py`.

## Version pinning

The `.env` file holds the Spark and Confluent Platform image tags. Update
`SPARK_VERSION` or `CONFLUENT_PLATFORM_VERSION` there if you need to test a
newer release—the Compose files read the values automatically.

## Running the batch analytics stack

1. Ensure the host folders exist (skip if they were created previously):
   ```bash
   mkdir -p data/input data/output hdfs/namenode hdfs/datanode checkpoints spark-events
   ```
2. Start the complete batch stack and rebuild services if needed:
   ```bash
   docker compose -f docker-compose.batch.yml up --build dashboard
   ```

Compose starts the services in dependency order:

* `data-generator` creates 30 days of daily CSVs under `./data/input` using the
  synthetic generator in `services/batch/generate_synthetic_data.py`.
* HDFS (NameNode + DataNode) and Spark master/workers come online.
* `hdfs-init` provisions `/data/input`, `/data/output`, and `/spark-events` in
  HDFS and uploads the generated CSVs.
* `spark-app` submits `pipeline_batch.py`, which performs schema cleanup,
  computes daily revenue per product, and writes:
  * Parquet to `hdfs://namenode:8020/data/output/analysis_parquet`
  * CSV to `./data/output/analysis_csv/` (for the dashboard)
* The Flask `dashboard` container serves the results at
  **http://localhost:5000**.

Stop the stack with `docker compose -f docker-compose.batch.yml down`.

### Customising synthetic batch inputs

Run the generator script directly if you want to tweak the dataset. For
example, to generate two weeks of higher-frequency transactions starting on a
specific date:

```bash
python services/batch/generate_synthetic_data.py \
  --output data/input \
  --days 14 \
  --transactions-per-day 72 \
  --start-date 2025-01-01
```

Use `--keep-existing` to append to existing CSVs instead of replacing them.

### Re-running the batch job

After changing the Spark code or CSV inputs, rerun the job without restarting
all containers:

```bash
docker compose -f docker-compose.batch.yml up --build spark-app
```

New CSV outputs appear under `./data/output/analysis_csv/` and the dashboard
refreshes on its next poll.

## Real-time streaming pipeline

The streaming stack replays the generated CSV rows into Kafka, aggregates them
with Spark Structured Streaming, and surfaces live metrics via the streaming
dashboards.

Start the flow (Spark master/workers, Kafka, producer, streaming job, and UI)
with:

```bash
docker compose -f docker-compose.streaming.yml up --build streaming-dashboard
```

Key components:

* **Kafka producer (`event-generator`)** reads the batch CSVs and publishes to
  topic `sales`. Override its rate by setting the `EVENTS_PER_SECOND`
  environment variable or by changing the container command
  (`--rate 20 --loop`, etc.).
* **Structured Streaming job (`spark-stream`)** consumes the topic, applies the
  same cleaning logic as the batch job, and writes 1-hour revenue windows (15
  minute slide) to `./data/output/streaming_product_revenue`.
* **Streaming dashboard** at **http://localhost:5100** polls the Parquet output
  directory to draw rolling timelines, leaderboards, and window health cards.

The job automatically resolves the appropriate Kafka connector package. Set
`SPARK_KAFKA_PACKAGE` or `SPARK_EXTRA_PACKAGES` in the environment if you need
custom connector coordinates.

## Dashboards & service UIs

* Batch dashboard: http://localhost:5000
* Streaming dashboard: http://localhost:5100
* Spark master UI: http://localhost:8080
* Spark history server: http://localhost:18081
* HDFS NameNode UI: http://localhost:9870

## Cleaning up generated data

Use the helper script to wipe demo artifacts between runs. It clears batch and
streaming outputs, HDFS volumes, checkpoints, and Spark event logs, recreating
empty directories so Docker bind mounts remain valid:

```bash
python cleanup_demo_data.py
```

Add `--dry-run` to preview what would be deleted.

## Skipping HDFS for local-only runs

If you want to experiment without HDFS, set `USE_HDFS = False` inside
`services/batch/pipeline_batch.py`. The batch job will then read/write solely
from the bind-mounted `./data` directory on the host.

---

For architectural context, see `Detailed_Design.md`, which explains how the
components interact and offers ideas for extending the exercise.
