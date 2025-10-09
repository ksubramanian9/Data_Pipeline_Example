# Detailed Design: Schema-driven Synthetic Data Pipeline

## 0. Context
This document expands on the architecture outlined in `High_Level_Design.md` by describing the moving parts, control flow, and configuration of the schema-driven demo platform. The focus is on how each component is implemented, how they exchange data, and the operational hooks that keep the solution reproducible.

## 1. Component Inventory
| Layer | Component | Technology | Key Responsibilities |
|-------|-----------|------------|-----------------------|
| Data generation | `services/batch/generate_synthetic_data.py` | Python script | Materialise schema-driven extracts (CSV/JSONL) from JSON definitions. |
| Messaging | `services/event-generator/kafka_event_producer.py` | Python + `kafka-python` | Replay CSV rows as JSON events into Kafka topics. |
| Batch processing | `services/batch/pipeline_batch.py` | PySpark | Cleanse and aggregate historical CSV data, publish Parquet/CSV outputs. |
| Streaming processing | `services/streaming/streaming_sales_aggregator.py` | PySpark Structured Streaming | Maintain rolling revenue windows and write Parquet outputs with checkpointing. |
| Serving | `dashboard/`, `streaming_dashboard/` | Flask, Chart.js | Poll curated outputs and render web dashboards. |
| Orchestration | `docker-compose.batch.yml`, `docker-compose.streaming.yml` | Docker Compose | Provision Spark, Kafka, Hadoop, and dashboards with consistent networking and volumes. |
| Maintenance | `cleanup_demo_data.py` | Python script | Reset bind-mounted data directories between runs. |

## 2. Data Model
### 2.1 Source schema (CSV rows)
The synthetic generator reads JSON definitions (e.g., `services/batch/schemas/card_transactions.json`) and emits fields exactly as declared. The default sample schema ships with the repository and produces the following columns:

| Column | Description | Type |
|--------|-------------|------|
| `event_id` | UUID per record for traceability. | string |
| `event_time` | ISO-8601 timestamp sampled from the configured window and seasonality profile. | string |
| `city` | Origin city (weighted categorical distribution). | string |
| `channel` | Payment channel (POS, e-commerce, ATM). | string |
| `merchant_cat` | Merchant category such as Grocery, Fuel, etc. | string |
| `card_id` | Synthetic PAN surrogate generated from a pattern. | string |
| `amount` | Monetary value sampled from a log-normal distribution. | float |
| `is_international` | Whether the transaction occurred outside the home country. | boolean |
| `is_chip` | Card dipped/swiped via chip. | boolean |
| `is_contactless` | Contactless (NFC) indicator. | boolean |
| `label_fraud` | Synthetic fraud flag used for KPI ratios. | boolean |
| `key` | Derived concatenation of city, merchant category, channel, and card ID. | string |

Additional schema definitions can introduce more fields without changing the generator code; derived columns reference values already stored in the per-record context. An optional `analysis` section in the JSON schema names the timestamp, dimensions, numeric fields, booleans, dataset title, and currency so downstream components can adapt automatically.

### 2.2 Curated schema (batch Parquet)
The batch pipeline inspects the schema metadata to decide which timestamp, categorical dimensions, numeric metrics, and boolean indicators to aggregate. Using the default card transactions schema, this yields one row per `(event_time_date, city, channel, merchant_cat)` combination with the following metrics:

| Column | Description |
|--------|-------------|
| `event_time_date` | Date partition key derived from the configured timestamp field. |
| `city`, `channel`, `merchant_cat` | Normalised dimensions with blanks replaced by `UNKNOWN`. |
| `record_count` | Number of records contributing to the bucket. |
| `sum_amount` | Sum of monetary value rounded to two decimals. |
| `count_is_international`, `count_is_contactless`, `count_is_chip`, `count_label_fraud` | Counts of boolean indicators. |
| `avg_amount` | Average ticket size for the bucket. |
| `rate_is_international`, `rate_is_contactless`, `rate_is_chip`, `rate_label_fraud` | Ratio columns derived from the counts. |

### 2.3 Streaming aggregates
Structured Streaming outputs hourly sliding windows based on the (unchanged) product demo:

| Column | Description |
|--------|-------------|
| `product` | Cleaned product name. |
| `revenue` | Revenue captured inside the window, rounded to two decimals. |
| `window_start` | Inclusive start timestamp of the one-hour window. |
| `window_end` | Exclusive end timestamp of the one-hour window. |

## 3. Synthetic Data Generation
### 3.1 Responsibilities
- Load a declarative JSON schema and expose CLI overrides for record counts, output format, gzip, and seeds.
- Maintain a per-record context so derived fields (e.g., concatenations, lookups) can reference previously generated values.
- Support a broad library of field generators: UUIDs, numeric distributions, weighted categories, datetime seasonality, booleans, geo coordinates, and derived fields.

### 3.2 Execution flow
1. **Argument parsing** – `parse_args()` collects schema path, output path (file or directory/name), record override, format override, seed, gzip flag, and whether to clear old outputs.
2. **Configuration loading** – JSON is parsed once; CLI overrides take precedence for seed, output format, and entity count.
3. **Directory preparation** – `resolve_output_path()` determines the destination file; `maybe_clear_output()` removes older extracts with matching suffixes when `--clear-output` is supplied.
4. **Row synthesis** – For each index, `gen_field()` routes to type-specific helpers. Datetime sampling honours ramps or seasonal weights, while derived fields read from the shared context populated earlier in the loop.
5. **File writing** – CSV writers emit headers once followed by escaped rows. JSONL writers dump per-record dictionaries. Optional gzip compression is handled transparently via `open_out()`.

### 3.3 Outputs
- The batch stack mounts `/opt/spark-data/input` to `./data/input` on the host. Generated CSV or JSONL extracts land in this directory for both batch and streaming pipelines.

## 4. Batch Spark Pipeline (`services/batch/pipeline_batch.py`)
### 4.1 Spark session setup
- App name `batch.schema_driven_pipeline` is used for Spark UI identification.
- The JSON schema metadata is loaded from `SCHEMA_CONFIG_PATH` (default `/opt/services/batch/schemas/card_transactions.json`).
- The session configures `spark.sql.sources.partitionOverwriteMode=dynamic` to allow partition overwrites and enforces UTC timestamps.
- `USE_HDFS` toggles between HDFS paths (`hdfs://namenode:8020/...`) and local disk, simplifying local-only runs.

### 4.2 Input discovery
- `wait_for_input_files()` polls either HDFS via the JVM FileSystem API or local glob patterns until CSV files matching `*.csv` appear. The timeout is 5 minutes by default.
- Once files are present, Spark reads them with headers and schema inference. `recursiveFileLookup` allows subdirectory traversal when additional partitions are introduced later.

### 4.3 Schema normalisation & cleansing
1. Column names are lower-cased and trimmed to eliminate accidental whitespace.
2. Timestamp handling is driven by the metadata. The configured timestamp column is cast to `timestamp`, invalid rows are dropped, and a derived `<timestamp>_date` column becomes the partition key.
3. Numeric metrics listed in the metadata are cast to `double` with nulls replaced by `0.0` so aggregates can run deterministically.
4. Dimension fields are normalised: whitespace is trimmed, blanks collapse to `UNKNOWN`, and any missing columns are injected with the default value.
5. Boolean indicators are parsed via `try_cast(... AS boolean)` with fallbacks for textual yes/no variants; missing columns default to `false`.

### 4.4 Aggregations
- A single `groupBy` produces per-day metrics for each combination of the declared dimensions. Measures include record counts, `sum_<metric>` totals for numeric fields, boolean true counts (`count_<flag>`), and rate/average columns derived afterwards.
- `kpis` collects platform-wide totals using the same aggregation expressions so dashboards can display rollups consistent with the detailed output.

### 4.5 Outputs
| Destination | Format | Purpose |
|-------------|--------|---------|
| `OUT_PARQUET` (default `hdfs://namenode:8020/data/output/analysis_parquet`) | Partitioned Parquet by `<timestamp>_date`. | Durable analytics store; accessible by Spark, Hive, or external tools. |
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
- Uses Chart.js to plot revenue trends and the top city·channel segments, and renders sample aggregated rows for inspection.
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
