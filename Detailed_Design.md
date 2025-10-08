# Detailed Design: Retail Transactions Data Pipeline

## 0. Context
This document expands on the architecture outlined in `High_Level_Design.md` by describing the moving parts, control flow, and configuration of the demo platform. The focus is on how each component is implemented, how they exchange data, and the operational hooks that keep the solution reproducible.

## 1. Component Inventory
| Layer | Component | Technology | Key Responsibilities |
|-------|-----------|------------|-----------------------|
| Data generation | `services/batch/generate_synthetic_data.py` | Python script | Produce one CSV per day with realistic stores, products, and pricing. |
| Messaging | `services/event-generator/kafka_event_producer.py` | Python + `kafka-python` | Replay CSV rows as JSON events into Kafka topics. |
| Batch processing | `services/batch/pipeline_batch.py` | PySpark | Cleanse and aggregate historical CSV data, publish Parquet/CSV outputs. |
| Streaming processing | `services/streaming/streaming_sales_aggregator.py` | PySpark Structured Streaming | Maintain rolling revenue windows and write Parquet outputs with checkpointing. |
| Serving | `dashboard/`, `streaming_dashboard/` | Flask, Chart.js | Poll curated outputs and render web dashboards. |
| Orchestration | `docker-compose.batch.yml`, `docker-compose.streaming.yml` | Docker Compose | Provision Spark, Kafka, Hadoop, and dashboards with consistent networking and volumes. |
| Maintenance | `cleanup_demo_data.py` | Python script | Reset bind-mounted data directories between runs. |

## 2. Data Model
### 2.1 Source schema (CSV rows)
The synthetic generator emits the following columns:

| Column | Description | Type |
|--------|-------------|------|
| `order_id` | Per-day unique identifier, e.g. `20250115-0001`. | string |
| `order_date` | ISO date (`YYYY-MM-DD`). | string |
| `store_id` | Store code (e.g. `BLR-01`). | string |
| `store_city` | Human-friendly store location. | string |
| `product` | Menu item name from a curated catalogue. | string |
| `quantity` | Units purchased (1–5). | integer |
| `unit_price` | Price after promotions (₹). | decimal string |
| `amount` | Pre-computed `quantity * unit_price`. | decimal string |

### 2.2 Curated schema (batch Parquet)
Daily aggregation produces:

| Column | Description |
|--------|-------------|
| `order_date` | Date partition key. |
| `product` | Normalised product label (`UNKNOWN` if absent). |
| `total_amount` | Rounded sum of monetary value for the date/product pair. |

### 2.3 Streaming aggregates
Structured Streaming outputs hourly sliding windows:

| Column | Description |
|--------|-------------|
| `product` | Cleaned product name. |
| `revenue` | Revenue captured inside the window, rounded to two decimals. |
| `window_start` | Inclusive start timestamp of the one-hour window. |
| `window_end` | Exclusive end timestamp of the one-hour window. |

## 3. Synthetic Data Generation
### 3.1 Responsibilities
- Maintain curated lists of products (`CATALOGUE`) and stores (`STORES`).
- Generate deterministic yet varied records using a seeded `random` module.
- Output one CSV per simulated day with consistent headers.

### 3.2 Execution flow
1. **Argument parsing** – `parse_args()` collects output path, number of days, transactions per day, start date, seed, and whether to retain existing files.
2. **Directory preparation** – `_ensure_output_dir()` creates the target folder; `_clear_existing_files()` removes previous CSVs unless `--keep-existing` is set.
3. **Date selection** – `determine_start_date()` chooses the first day (explicit or relative to today) and `daterange()` iterates day offsets.
4. **Row synthesis** – `generate_day_file()` loops `transactions` times, sampling products, stores, quantities, and discount factors to produce dictionaries with `order_id`, `order_date`, pricing, and totals.
5. **File writing** – Each day produces `retail_<date>.csv` with headers written via `csv.DictWriter`. A summary of generated filenames is printed to stdout.

### 3.3 Outputs
- The batch stack mounts `/opt/spark-data/input` to `./data/input` on the host. Generated CSVs land in this directory for both batch and streaming pipelines.

## 4. Batch Spark Pipeline (`services/batch/pipeline_batch.py`)
### 4.1 Spark session setup
- App name `batch.retail_sales_clean_analyze` is used for Spark UI identification.
- The session configures `spark.sql.sources.partitionOverwriteMode=dynamic` to allow partition overwrites and enforces UTC timestamps.
- `USE_HDFS` toggles between HDFS paths (`hdfs://namenode:8020/...`) and local disk, simplifying local-only runs.

### 4.2 Input discovery
- `wait_for_input_files()` polls either HDFS via the JVM FileSystem API or local glob patterns until CSV files matching `*.csv` appear. The timeout is 5 minutes by default.
- Once files are present, Spark reads them with headers and schema inference. `recursiveFileLookup` allows subdirectory traversal when additional partitions are introduced later.

### 4.3 Schema normalisation & cleansing
1. Column names are lower-cased and trimmed to eliminate accidental whitespace.
2. Product column logic:
   - Prefer `product`, fall back to `item`, otherwise inject `UNKNOWN`.
   - `trim()` removes leading/trailing spaces; empty strings become `UNKNOWN`.
3. Order date derivation:
   - Candidate columns include `order_date`, `date`, `order_time`, `timestamp`, and `event_time`.
   - Values are parsed with `try_cast` and regex extraction to support formats like `YYYY-MM-DD` and `YYYYMMDD`.
   - Rows with missing/invalid dates are logged and dropped.
4. Monetary amount handling:
   - If `amount` exists, cast to `double`.
   - Else, compute `quantity * unit_price` (or `price`) after casting.
   - Null amounts default to `0.0` and are rounded to two decimals.
5. Additional hygiene removes null/blank products after transformation.

### 4.4 Aggregations
- `daily_product` groups by `order_date` and `product`, summing `amount` into `total_amount` and ordering chronologically.
- `kpis` collects global metrics: grand total revenue, distinct product count, and total row count (via `clean.count()`).

### 4.5 Outputs
| Destination | Format | Purpose |
|-------------|--------|---------|
| `OUT_PARQUET` (default `hdfs://namenode:8020/data/output/analysis_parquet`) | Partitioned Parquet by `order_date`. | Durable analytics store; accessible by Spark, Hive, or external tools. |
| `OUT_LOCAL_CSV` (default `/opt/spark-data/output/analysis_csv`) | Single CSV file (coalesced). | Source for the batch dashboard bind-mounted to `./data/output/analysis_csv/`. |
| `OUT_LOCAL_CSV + "_kpis"` | Single-row CSV. | Dashboard KPI tiles and quick diagnostics. |

Spark writes use overwrite mode. `_as_local_uri()` prefixes paths with `file://` when necessary to avoid unintended HDFS writes.

### 4.6 Error handling & logging
- Structured logging (INFO/WARN) surfaces key milestones and validation issues.
- Fatal exceptions trigger a `logger.exception` block; the Spark session is stopped before re-raising to prevent orphaned contexts.

## 5. Streaming Pipeline
### 5.1 Kafka event producer (`services/event-generator/kafka_event_producer.py`)
- Loads CSV inputs (default `/opt/spark-data/input`) and yields dictionaries per row with trimmed keys/values.
- Normalises timestamps by copying the first available column in (`order_ts`, `order_time`, `order_date`, `timestamp`) into `event_time`; if absent, injects the current time.
- Publishes JSON messages to Kafka using `KafkaProducer`, honouring CLI/ENV configuration for bootstrap servers, topic, rate, shuffle, and looping.
- Rate limiting uses `time.sleep(1 / rate)`; shuffle/random replay ensures varied event ordering for demos.

### 5.2 Spark streaming aggregator (`services/streaming/streaming_sales_aggregator.py`)
#### Session initialisation
- `build_spark_session()` configures shuffle partitions and ensures the Kafka connector jars or packages are available. It detects bundled versions through `_discover_local_kafka_jars()` or falls back to Maven coordinates derived from Spark’s Scala version.
- The application name `stream.retail_sales_aggregator` differentiates the query in Spark UIs.

#### Stream ingestion
- `readStream` is configured with Kafka bootstrap servers, topic, starting offsets (`latest` by default), and `failOnDataLoss=false` for resilience when Kafka trims old offsets.
- Payloads are parsed with `from_json` using `EVENT_SCHEMA`, creating a structured DataFrame with optional fields.

#### Transformation logic (`transform_orders`)
1. Product handling mirrors the batch job (`product`/`item` detection, trimming, fallback to `UNKNOWN`).
2. Timestamp reconciliation searches for `event_time`, `order_time`, `order_date`, and `timestamp`. Values are cleaned (`regexp_replace` of `T`) and converted to timestamps using several format patterns. Missing timestamps default to the Unix epoch and are filtered out subsequently.
3. Quantity, unit price, and total price are cast to `DoubleType`.
4. `line_amount` prioritises an explicit `total_price`; otherwise multiplies `quantity * unit_price`. Nulls are replaced with `0.0` and rounded.
5. Records lacking a valid `order_ts` are removed before aggregation.

#### Aggregations (`build_aggregations`)
- Applies a 15-minute watermark to tolerate late data.
- Groups by tumbling 1-hour windows that slide every 15 minutes and by product.
- Produces rounded revenue per bucket along with window boundaries, enabling the dashboard to display timelines.

#### Output sink
- `writeStream` targets Parquet files (`OUTPUT_PATH`, default `hdfs://namenode:8020/data/output/streaming_product_revenue`) with `append` mode and a checkpoint directory (`CHECKPOINT_DIR`) to support restarts.
- Trigger interval defaults to 30 seconds but can be overridden via `STREAM_TRIGGER_INTERVAL`.

## 6. Serving Layer
### 6.1 Batch dashboard (`dashboard/`)
- Flask application that reads CSV summaries (`./data/output/analysis_csv/` and `_kpis`).
- Uses Chart.js to plot revenue trends and product leaderboards.
- Refreshes data on a polling schedule (configurable via environment variables in the Docker Compose file).

### 6.2 Streaming dashboard (`streaming_dashboard/`)
- Flask server that scans the streaming Parquet output folder and transforms it into JSON payloads for the UI.
- Shows rolling revenue windows, latest leaderboard, and stream health indicators (e.g. last update time derived from Parquet metadata).

## 7. Storage & Deployment Layout
- Host directories (`data/input`, `data/output`, `hdfs/*`, `checkpoints`, `spark-events`) are bind-mounted into containers to persist artefacts between restarts.
- HDFS NameNode/DataNode services (batch stack) own the canonical dataset; local folders provide a simplified fallback when `USE_HDFS=False`.
- Docker Compose networks expose predictable hostnames (`namenode`, `spark-master`, `kafka`, etc.) referenced across scripts and applications.

## 8. Operations & Maintenance
- **Startup** – `docker compose -f docker-compose.batch.yml up --build dashboard` brings up the batch pipeline. The streaming stack uses `docker-compose.streaming.yml` with the `streaming-dashboard` target.
- **Spark history & monitoring** – Spark UIs (master and history server) are available at ports 8080 and 18081 respectively. Container logs reveal validation warnings and runtime metrics.
- **Cleanup** – `cleanup_demo_data.py` deletes generated inputs/outputs, HDFS data, checkpoints, and Spark event logs before recreating directory skeletons.
- **Extending transformations** – Additional Spark logic can be added inside `pipeline_batch.py` or `streaming_sales_aggregator.py`. Both scripts adopt modular helper functions so new cleansing or aggregation steps can be slotted in with minimal refactoring.

This detailed description should serve as a companion for engineers updating or extending the platform. Combined with the repository’s source files, it provides the technical depth required for implementation and troubleshooting.
