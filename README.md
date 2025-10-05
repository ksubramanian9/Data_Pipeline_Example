# Retail Transactions Pipeline — Hadoop + Spark + Kafka + Dashboard

A complete, classroom‑ready demo that ingests retail transactions, cleans & aggregates them with Spark, stores detailed data in HDFS (Parquet), and serves a simple web dashboard.

## Folder layout
```
.
├─ docker-compose.yml
├─ apps/
│  └─ pipeline_batch.py           # Spark ETL + aggregation (batch)
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
docker compose up -d spark-app spark-history-server
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
4. Bring up the dashboard on **http://localhost:5000**

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

## Extending to streaming
- Add a `spark-stream` service that reads from Kafka topic `sales` and writes rolling aggregates to `/opt/spark-data/output/stream_agg/` with checkpoints in `/opt/spark-checkpoints/sales_agg`.
- Use the included Kafka broker at `kafka:9092` and a simple Python producer to push events derived from the CSVs.

## Teaching prompts
- *Partitioning*: Why do we partition Parquet by `order_date`? Try adding another month and observe file layout.
- *Joins*: Introduce a `products.csv` dimension (category, brand) and join during ETL.
- *Quality checks*: Add constraints (e.g., non‑negative `amount`) and reject bad records to a quarantine path.
