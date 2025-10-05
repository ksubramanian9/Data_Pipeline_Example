# Retail Transactions Pipeline — Hadoop + Spark + Kafka + Dashboard

A complete, classroom‑ready demo that ingests retail transactions, cleans & aggregates them with Spark, stores detailed data in HDFS (Parquet), and serves a simple web dashboard.

## Folder layout
```
.
├─ docker-compose.yml
├─ apps/
│  ├─ pipeline_batch.py           # Spark ETL + aggregation (batch)
│  ├─ streaming_sales_aggregator.py  # Spark Structured Streaming job
│  └─ kafka_event_producer.py     # CSV → Kafka event generator script
├─ data/
│  ├─ input/                      # Sample CSVs (already included)
│  └─ output/                     # Spark writes host‑readable CSV here
├─ dashboard/                     # Flask + Chart.js dashboard
│  ├─ Dockerfile
│  ├─ requirements.txt
│  ├─ app.py
│  └─ static/
│     ├─ index.html
│     └─ script.js
├─ producers/                     # Dockerfile for Kafka event generator
├─ hdfs/                          # HDFS persistent volumes
│  ├─ namenode/
│  └─ datanode/
├─ checkpoints/                   # (optional) streaming checkpoints
└─ spark-events/                  # Spark event logs (history server)
```

## Start the containers one by one
```bash
docker compose up -d namenode datanode
docker compose up -d hdfs-init
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose up -d spark-app spark-stream spark-history-server event-generator
```

## One‑command happy path
Run the entire stack, batch job, and dashboard:
```bash
docker compose up --build dashboard
```
This will:
1. Start HDFS + Kafka + Spark
2. Initialize HDFS and seed `/data/input` with the sample host CSVs
3. Run the Spark batch job once (ETL + aggregation)
4. Launch the continuous Spark streaming job that reads Kafka `sales` events
5. Start the Python producer that replays sample CSVs into Kafka
6. Bring up the dashboard on **http://localhost:5000**

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

## Re‑running the batch
If you change code or add more input CSVs:
```bash
docker compose up --build spark-app
```
The new output appears under `./data/output/analysis_csv/` and in HDFS under `/data/output/analysis_parquet`.

## Switching to local paths (skip HDFS)
In `apps/pipeline_batch.py`, set `USE_HDFS = False`. The job will read/write only from the bind‑mounted `./data` directory.

## Streaming pipeline

The docker-compose stack now includes an end-to-end streaming flow:

- **Producer (`event-generator`)** replays the CSV samples into Kafka topic `sales` at ~5 events/second. Tweak the
  rate by setting `EVENTS_PER_SECOND` or override the command, e.g.
  `docker compose run event-generator --rate 20 --loop`.
- **Structured Streaming (`spark-stream`)** consumes `sales`, applies the same cleaning logic, and maintains rolling
  1-hour revenue totals per product with 15-minute hops. Aggregates land in HDFS at
  `hdfs://namenode:8020/data/output/streaming_product_revenue` (set `STREAM_OUTPUT_PATH=file:///opt/spark-data/output/streaming_product_revenue`
  if you prefer a host-mounted directory).
- **Dashboard** can be pointed at the streaming output by swapping its data source to the new Parquet path or by adding
  another chart that reads the streaming parquet files.

To (re)start just the streaming pieces:

```bash
docker compose up --build spark-stream event-generator
```

## Teaching prompts
- *Partitioning*: Why do we partition Parquet by `order_date`? Try adding another month and observe file layout.
- *Joins*: Introduce a `products.csv` dimension (category, brand) and join during ETL.
- *Quality checks*: Add constraints (e.g., non‑negative `amount`) and reject bad records to a quarantine path.
