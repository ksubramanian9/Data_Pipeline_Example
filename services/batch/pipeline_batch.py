import glob
import logging
import os
import sys
import time

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    coalesce,
    count,
    expr,
    lit,
    lower,
    round as round_,
    sum as sum_,
    to_date,
    trim,
    when,
)
from pyspark.sql.types import DoubleType

from schema_metadata import load_schema_metadata


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("schema.batch.pipeline")

SCHEMA_CONFIG_PATH = os.environ.get(
    "SCHEMA_CONFIG_PATH", "/opt/services/batch/schemas/card_transactions.json"
)

METADATA = load_schema_metadata(SCHEMA_CONFIG_PATH)

USE_HDFS = True
if USE_HDFS:
    INPUT_PATH = "hdfs://namenode:8020/data/input"
    OUT_PARQUET = "hdfs://namenode:8020/data/output/analysis_parquet"
    OUT_LOCAL_CSV = "/opt/spark-data/output/analysis_csv"
else:
    INPUT_PATH = "/opt/spark-data/input"
    OUT_PARQUET = "/opt/spark-data/output/analysis_parquet"
    OUT_LOCAL_CSV = "/opt/spark-data/output/analysis_csv"


def _as_local_uri(path: str) -> str:
    """Ensure Spark writes to the local filesystem even when default FS is HDFS."""

    if path.startswith("file://"):
        return path

    return f"file://{path}"


APP_NAME = "batch.schema_driven_pipeline"

logger.info("Starting Spark batch pipeline '%s'", APP_NAME)
logger.info("Loaded schema metadata from %s", SCHEMA_CONFIG_PATH)
logger.info(
    "Dataset name: %s | timestamp: %s | dimensions: %s | numeric fields: %s | boolean fields: %s",
    METADATA.dataset_name,
    METADATA.timestamp_field,
    ", ".join(METADATA.dimensions) or "<none>",
    ", ".join(METADATA.numeric_fields) or "<none>",
    ", ".join(METADATA.boolean_fields) or "<none>",
)
logger.info("Connecting to Spark master via SparkSession builder ...")
spark = None
try:
    spark = (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    logger.info(
        "Spark session established (appId=%s)", spark.sparkContext.applicationId
    )
    spark.sparkContext.setLogLevel("WARN")
except Exception:  # pragma: no cover - defensive logging
    logger.exception("Unable to create Spark session")
    raise


def wait_for_input_files(
    spark_session: SparkSession,
    path: str,
    use_hdfs: bool,
    pattern: str = "*.csv",
    poll_interval: int = 5,
    timeout_seconds: int = 300,
) -> None:
    """Block until CSV input files are visible either locally or on HDFS."""

    deadline = (time.time() + timeout_seconds) if timeout_seconds else None

    if use_hdfs:
        jvm = spark_session._jvm
        fs_class = jvm.org.apache.hadoop.fs.FileSystem
        path_class = jvm.org.apache.hadoop.fs.Path
        uri_class = jvm.java.net.URI
        conf = spark_session._jsc.hadoopConfiguration()
        glob_target = path_class(f"{path.rstrip('/')}/{pattern}")

        logger.info("Waiting for HDFS files matching %s under %s ...", pattern, path)
        while True:
            try:
                fs = fs_class.get(uri_class(path), conf)
                matches = fs.globStatus(glob_target)
                if matches and len(matches) > 0:
                    logger.info("Detected %d file(s) in %s", len(matches), path)
                    return
            except Py4JJavaError as err:  # pragma: no cover - HDFS flake guard
                logger.warning(
                    "Could not access HDFS path %s: %s", path, err.java_exception
                )

            if deadline and time.time() > deadline:
                raise RuntimeError(f"Timed out waiting for files in {path}")

            time.sleep(poll_interval)
    else:
        logger.info("Waiting for local files matching %s under %s ...", pattern, path)
        while True:
            matches = glob.glob(os.path.join(path, pattern))
            if matches:
                logger.info("Detected %d file(s) in %s", len(matches), path)
                return

            if deadline and time.time() > deadline:
                raise RuntimeError(f"Timed out waiting for files in {path}")

            time.sleep(poll_interval)


def _normalise_dimension(column: str) -> Column:
    trimmed = trim(col(column))
    return when(trimmed.isNull() | (trimmed == ""), lit("UNKNOWN")).otherwise(trimmed)


def _as_boolean(column: str):
    casted = expr(f"try_cast(`{column}` AS boolean)")
    cleaned = trim(lower(col(column)))
    return coalesce(
        casted,
        when(cleaned.isNull() | (cleaned == ""), lit(False))
        .when(cleaned.isin("true", "1", "yes", "y"), lit(True))
        .when(cleaned.isin("false", "0", "no", "n"), lit(False))
        .otherwise(lit(False)),
    )


def main() -> None:
    logger.info("Reading input from: %s", INPUT_PATH)
    wait_for_input_files(spark, INPUT_PATH, USE_HDFS)

    logger.info("Loading CSV data into DataFrame ...")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("recursiveFileLookup", "true")
        .csv(INPUT_PATH)
    )

    if df.rdd.isEmpty():
        logger.warning("No input files found. Exiting gracefully.")
        spark.stop()
        raise SystemExit(0)

    columns = [c.lower().strip() for c in df.columns]
    df = df.toDF(*columns)

    timestamp_column = METADATA.timestamp_field

    if timestamp_column not in columns:
        logger.error(
            "Timestamp column '%s' not found in input schema: %s",
            timestamp_column,
            columns,
        )
        spark.stop()
        raise SystemExit(1)

    logger.info("Normalising timestamp column '%s'", timestamp_column)
    df = df.withColumn(
        timestamp_column, expr(f"try_cast(`{timestamp_column}` AS timestamp)")
    )

    invalid_ts = df.filter(col(timestamp_column).isNull())
    invalid_count = invalid_ts.count()
    if invalid_count:
        logger.warning(
            "Dropping %d row(s) with missing or unparseable timestamps", invalid_count
        )
    df = df.filter(col(timestamp_column).isNotNull())

    date_column = METADATA.date_column
    df = df.withColumn(date_column, to_date(col(timestamp_column)))

    for numeric in METADATA.numeric_fields:
        if numeric in df.columns:
            df = df.withColumn(
                numeric,
                coalesce(expr(f"try_cast(`{numeric}` AS double)"), lit(0.0)).cast(
                    DoubleType()
                ),
            )
        else:
            logger.info("Adding missing numeric column '%s'", numeric)
            df = df.withColumn(numeric, lit(0.0))

    for dimension in METADATA.dimensions:
        if dimension in df.columns:
            df = df.withColumn(dimension, _normalise_dimension(dimension))
        else:
            logger.info("Adding missing dimension column '%s'", dimension)
            df = df.withColumn(dimension, lit("UNKNOWN"))

    for boolean_field in METADATA.boolean_fields:
        if boolean_field in df.columns:
            df = df.withColumn(boolean_field, _as_boolean(boolean_field))
        else:
            df = df.withColumn(boolean_field, lit(False))

    selected_columns = {timestamp_column, date_column}
    selected_columns.update(METADATA.dimensions)
    selected_columns.update(METADATA.numeric_fields)
    selected_columns.update(METADATA.boolean_fields)

    clean = df.select(*sorted(selected_columns))

    logger.info(
        "Aggregating dataset by date (%s) and dimensions %s", date_column, METADATA.dimensions
    )

    group_columns = [date_column] + METADATA.dimensions
    aggregate_expressions = [count("*").alias("record_count")]

    for numeric in METADATA.numeric_fields:
        aggregate_expressions.append(sum_(col(numeric)).alias(f"sum_{numeric}"))

    for boolean_field in METADATA.boolean_fields:
        aggregate_expressions.append(
            sum_(when(col(boolean_field), lit(1)).otherwise(lit(0))).alias(
                f"count_{boolean_field}"
            )
        )

    aggregated = clean.groupBy(*group_columns).agg(*aggregate_expressions)

    for numeric in METADATA.numeric_fields:
        aggregated = aggregated.withColumn(
            f"avg_{numeric}",
            when(
                col("record_count") > 0,
                round_(col(f"sum_{numeric}") / col("record_count"), 4),
            ).otherwise(lit(0.0)),
        )

    for boolean_field in METADATA.boolean_fields:
        aggregated = aggregated.withColumn(
            f"rate_{boolean_field}",
            when(
                col("record_count") > 0,
                round_(col(f"count_{boolean_field}") / col("record_count"), 4),
            ).otherwise(lit(0.0)),
        )

    aggregated = aggregated.orderBy(*group_columns)

    logger.info("Calculating KPI snapshot ...")
    kpi_aggs = [count("*").alias("record_count")]
    for numeric in METADATA.numeric_fields:
        kpi_aggs.append(sum_(col(numeric)).alias(f"sum_{numeric}"))
    for boolean_field in METADATA.boolean_fields:
        kpi_aggs.append(
            sum_(when(col(boolean_field), lit(1)).otherwise(lit(0))).alias(
                f"count_{boolean_field}"
            )
        )

    kpis = clean.agg(*kpi_aggs)

    logger.info("Writing detailed parquet to: %s", OUT_PARQUET)
    (
        aggregated.repartition(date_column)
        .write.mode("overwrite")
        .partitionBy(date_column)
        .parquet(OUT_PARQUET)
    )

    local_csv_uri = _as_local_uri(OUT_LOCAL_CSV)
    logger.info("Writing summarised CSV (single file) to: %s", local_csv_uri)
    (
        aggregated.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(local_csv_uri)
    )

    logger.info("KPI snapshot:")
    kpis.show(truncate=False)
    (
        kpis.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(_as_local_uri(OUT_LOCAL_CSV + "_kpis"))
    )

    logger.info("Pipeline complete. Shutting down Spark session.")
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error in batch pipeline")
        if spark is not None:
            spark.stop()
        raise
