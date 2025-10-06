# Retail Transactions Pipeline — Hadoop + Spark + Kafka + Dashboard

A complete, classroom‑ready demo that ingests retail transactions, cleans & aggregates them with Spark, stores detailed data in HDFS (Parquet), and serves a simple web dashboard.

## Folder layout
```
.
├─ docker-compose.batch.yml       # Batch-oriented stack (Spark + HDFS + dashboard)
├─ docker-compose.streaming.yml   # Streaming stack (Kafka + Spark Structured Streaming)
├─ services/
│  ├─ batch/
│  │  └─ pipeline_batch.py        # Spark ETL + aggregation (batch)
│  ├─ streaming/
│  │  └─ streaming_sales_aggregator.py  # Spark Structured Streaming job
│  └─ event-generator/
│     ├─ Dockerfile               # Kafka producer container image
│     └─ kafka_event_producer.py  # CSV → Kafka event generator script
├─ data/
│  ├─ input/                      # Sample CSVs (already included)
│  └─ output/                     # Batch CSV + streaming Parquet land here for dashboards
├─ dashboard/                     # Flask + Chart.js dashboard
│  ├─ Dockerfile
│  ├─ requirements.txt
│  ├─ app.py
│  └─ static/
│     ├─ index.html
│     └─ script.js
├─ streaming_dashboard/           # Streaming-specific dashboard (Parquet reader)
│  ├─ Dockerfile
│  ├─ requirements.txt
│  ├─ app.py
│  └─ static/
│     ├─ index.html
│     └─ script.js
├─ hdfs/                          # HDFS persistent volumes
│  ├─ namenode/
│  └─ datanode/
├─ checkpoints/                   # (optional) streaming checkpoints
└─ spark-events/                  # Spark event logs (history server)
```

## Start the containers one by one
```bash
docker compose -f docker-compose.batch.yml up -d namenode datanode
docker compose -f docker-compose.batch.yml up -d hdfs-init
docker compose -f docker-compose.batch.yml up -d spark-master spark-worker-1 spark-worker-2
docker compose -f docker-compose.batch.yml up -d spark-app spark-history-server dashboard
```

## One‑command happy path
Run the entire stack, batch job, and dashboard:
```bash
docker compose -f docker-compose.batch.yml up --build dashboard
```
This will:
1. Generate 30 days of synthetic retail transactions (one CSV per day) under `./data/input`
2. Start HDFS + Spark (master + workers)
3. Initialize HDFS and load the generated CSVs into `/data/input`
4. Run the Spark batch job once (ETL + aggregation)
5. Bring up the dashboard on **http://localhost:5000**

### Customising the synthetic batch inputs

The new `data-generator` service (powered by `services/batch/generate_synthetic_data.py`) creates lightweight yet
realistic product sales before Spark starts. Adjust the behaviour by running the script manually, for example:

```bash
python services/batch/generate_synthetic_data.py --days 14 --transactions-per-day 24 --start-date 2025-01-01
```

Use `--keep-existing` if you want to append to whatever is already in `./data/input`; by default the generator clears
older CSVs so each Compose run has a fresh, predictable dataset.

## What it demonstrates
- **HDFS** as a data lake landing/warehouse (Parquet at `/data/output/analysis_parquet`)
- **Spark SQL** for ETL (schema normalization, cleaning) and aggregation (daily revenue by product)
- **Reproducible compute** with `spark-submit` into a containerized Spark cluster
- **Simple BI**: host‑readable CSV + a minimal web dashboard

## UIs
- Spark Master: http://localhost:8080
- Spark History: http://localhost:18081
- HDFS NameNode: http://localhost:9870
- Dashboard: http://localhost:5000
- Streaming Dashboard: http://localhost:5100

## Re‑running the batch
If you change code or add more input CSVs:
```bash
docker compose -f docker-compose.batch.yml up --build spark-app
```
The new output appears under `./data/output/analysis_csv/` and in HDFS under `/data/output/analysis_parquet`.

## Switching to local paths (skip HDFS)
In `services/batch/pipeline_batch.py`, set `USE_HDFS = False`. The job will read/write only from the bind‑mounted `./data` directory.

## Streaming pipeline

The streaming compose file defines an end-to-end real-time flow:

- **Producer (`event-generator`)** replays the CSV samples into Kafka topic `sales` at ~5 events/second. Tweak the
  rate by setting `EVENTS_PER_SECOND` or override the command, e.g.
  `docker compose -f docker-compose.streaming.yml run event-generator --rate 20 --loop`.
- **Structured Streaming (`spark-stream`)** consumes `sales`, applies the same cleaning logic, and maintains rolling
  1-hour revenue totals per product with 15-minute hops. In the provided compose file the job writes to the bind-mounted
  path `file:///opt/spark-data/output/streaming_product_revenue` (shared as `./data/output/streaming_product_revenue` on the host)
  so downstream apps can read the Parquet output without going through HDFS APIs.
- **Connector jars**: the stock `apache/spark` image does not ship the Kafka datasource jars. The streaming job automatically
  requests `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`. Override it with `SPARK_KAFKA_PACKAGE` or append more
  comma-separated coordinates via `SPARK_EXTRA_PACKAGES`.
- **Streaming dashboard** (`streaming-dashboard`) reads the Parquet output directly and renders live revenue timelines,
  product leaderboards, and window health indicators at http://localhost:5100.

To run the streaming stack (Kafka + Spark Structured Streaming + producer), use the dedicated compose file:

```bash
docker compose -f docker-compose.streaming.yml up --build spark-stream event-generator streaming-dashboard
```

After ~15 minutes you will have several dozen windows spanning multiple products; open http://localhost:5100 to watch the
rolling revenue chart and leaderboards update as new micro-batches arrive.

## Teaching prompts
- *Partitioning*: Why do we partition Parquet by `order_date`? Try adding another month and observe file layout.
- *Joins*: Introduce a `products.csv` dimension (category, brand) and join during ETL.
- *Quality checks*: Add constraints (e.g., non‑negative `amount`) and reject bad records to a quarantine path.
