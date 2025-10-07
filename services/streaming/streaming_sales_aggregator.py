"""Structured Streaming job for real-time retail analytics.

The job consumes JSON order events from Kafka, applies the same cleansing
rules as the batch pipeline and maintains rolling revenue metrics per
product. Results are persisted to HDFS in parquet format so that the
existing dashboard (or other tools) can use the aggregated outputs.
"""

from __future__ import annotations

import glob
import logging
import os
import re
import sys
from typing import Iterable, List, Optional, Tuple

import pyspark

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    coalesce,
    col,
    from_json,
    lit,
    regexp_replace,
    round as round_,
    sum as sum_,
    to_date,
    to_timestamp,
    trim,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("retail.stream")


APP_NAME = "stream.retail_sales_aggregator"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sales")
CHECKPOINT_DIR = os.environ.get("STREAM_CHECKPOINT_DIR", "/opt/spark-checkpoints/streaming_sales")
OUTPUT_PATH = os.environ.get("STREAM_OUTPUT_PATH", "hdfs://namenode:8020/data/output/streaming_product_revenue")
# The Kafka connector jars are not bundled with the vanilla Spark image.
# Use the official artifact that matches the Spark/Scala version and allow
# callers to override via environment variables when needed.
DEFAULT_SCALA_BINARY = "2.12"
DEFAULT_SPARK_VERSION = "3.5.1"

# Known Kafka connector jar basenames that should be provided together
KAFKA_CONNECTOR_JARS = (
    "spark-sql-kafka-0-10_",
    "spark-token-provider-kafka-0-10_",
)


def _find_coordinates_from_jars(jar_root: str) -> Optional[Tuple[str, str]]:
    """Return Scala and Spark versions inferred from Spark jar names."""

    if not jar_root or not os.path.isdir(jar_root):
        return None

    jar_patterns = [
        os.path.join(jar_root, "spark-sql_*.jar"),
        os.path.join(jar_root, "spark-core_*.jar"),
    ]

    for pattern in jar_patterns:
        for jar_path in sorted(glob.glob(pattern)):
            filename = os.path.basename(jar_path)
            match = re.search(r"_(\d+\.\d+)-(\d+\.\d+\.\d+)", filename)
            if match:
                return match.group(1), match.group(2)

    return None

def _infer_spark_artifact_coordinates() -> Tuple[str, str]:
    """Infer the Scala binary version and Spark version for bundled jars."""

    env_scala = os.environ.get("SPARK_SCALA_VERSION")
    env_spark = os.environ.get("SPARK_VERSION")
    if env_scala and env_spark:
        return env_scala.strip(), env_spark.strip()

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        jars_root = os.path.join(spark_home, "jars")
        inferred = _find_coordinates_from_jars(jars_root)
        if inferred:
            return inferred

    pyspark_dir = os.path.dirname(pyspark.__file__)
    inferred = _find_coordinates_from_jars(os.path.join(pyspark_dir, "jars"))
    if inferred:
        return inferred

    return DEFAULT_SCALA_BINARY, DEFAULT_SPARK_VERSION


def _discover_local_kafka_jars(roots: Iterable[str]) -> List[str]:
    """Return absolute paths to bundled Kafka connector jars if present."""

    discovered: List[str] = []
    for root in roots:
        if not root:
            continue
        jars_dir = os.path.join(root, "jars") if os.path.isdir(root) and not root.endswith(".jar") else root
        if not os.path.isdir(jars_dir):
            continue
        for base in KAFKA_CONNECTOR_JARS:
            pattern = os.path.join(jars_dir, f"{base}*.jar")
            matches = sorted(glob.glob(pattern))
            discovered.extend(matches)

    return sorted(dict.fromkeys(discovered))


def _default_kafka_package() -> str:
    scala_bin, spark_version = _infer_spark_artifact_coordinates()
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_bin}:{spark_version}"


def build_spark_session() -> SparkSession:
    logger.info("Creating Spark session '%s'", APP_NAME)
    builder = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
    )

    # Honour an explicit SPARK_JARS_PACKAGES if provided, otherwise make sure
    # the Kafka connector is available. Prefer local jars (matching the runtime)
    # when bundled, with remote packages as a last resort. Additional packages
    # can be supplied via SPARK_EXTRA_PACKAGES (comma-separated) without losing
    # the default.
    # if "SPARK_JARS_PACKAGES" not in os.environ:
    if not os.environ.get("SPARK_JARS_PACKAGES"):
        candidate_roots = [
            os.environ.get("SPARK_HOME"),
            os.environ.get("SPARK_KAFKA_JAR_DIR"),
            os.path.dirname(pyspark.__file__),
        ]
        local_jars = _discover_local_kafka_jars(candidate_roots)
        extra_jars = [
            path.strip()
            for path in os.environ.get("SPARK_EXTRA_JARS", "").split(",")
            if path.strip()
        ]
        if local_jars or extra_jars:
            jars_value = ",".join(local_jars + extra_jars)
            builder = builder.config("spark.jars", jars_value)
            if local_jars:
                logger.info("Using bundled Kafka connector jars: %s", ", ".join(local_jars))
            if extra_jars:
                logger.info("Including additional Spark jars: %s", ", ".join(extra_jars))
        if not local_jars:
            kafka_package = os.environ.get("SPARK_KAFKA_PACKAGE", _default_kafka_package())
            extra_packages = os.environ.get("SPARK_EXTRA_PACKAGES", "").strip()
            packages = ",".join(
                pkg
                for pkg in [kafka_package, extra_packages]
                if pkg
            )
            if packages:
                builder = builder.config("spark.jars.packages", packages)
                logger.info("Using Kafka connector packages: %s", packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


EVENT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_time", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product", StringType(), True),
    StructField("item", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("total_price", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store", StringType(), True),
])


def transform_orders(df: DataFrame) -> DataFrame:
    logger.info("Applying cleansing and derived columns to event stream")

    cols = df.columns

    product_col = "product" if "product" in cols else ("item" if "item" in cols else None)
    if product_col:
        df = df.withColumn("product", trim(col(product_col)))
    else:
        df = df.withColumn("product", lit("UNKNOWN"))

    # Build an event timestamp from available fields
    def choose_timestamp(*candidates: str) -> Optional[Column]:
        for cand in candidates:
            if cand and cand in cols:
                return col(cand)
        return None

    ts_expr = choose_timestamp("event_time", "order_time", "order_date", "timestamp")
    if ts_expr is None:
        df = df.withColumn("order_ts", to_timestamp(lit("1970-01-01 00:00:00")))
    else:
        ts_text = trim(ts_expr.cast("string"))
        ts_spaced = regexp_replace(ts_text, "T", " ")
        df = df.withColumn(
            "order_ts",
            coalesce(
                to_timestamp(ts_text),
                to_timestamp(ts_spaced),
                to_timestamp(ts_text, "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(ts_text, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp(ts_text, "yyyy-MM-dd'T'HH:mm:ssXXX"),
                to_timestamp(ts_text, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                to_timestamp(ts_text, "yyyy-MM-dd"),
            ),
        )

    df = df.withColumn("order_date", to_date(col("order_ts")))

    df = (
        df
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("total_price", col("total_price").cast(DoubleType()))
    )

    df = df.withColumn(
        "line_amount",
        when(col("total_price").isNotNull(), col("total_price").cast(DoubleType()))
        .otherwise(col("quantity") * col("unit_price")),
    )

    df = df.withColumn(
        "line_amount",
        round_(
            when(col("line_amount").isNull(), lit(0.0)).otherwise(col("line_amount")),
            2,
        ),
    )

    return df.filter(col("order_ts").isNotNull())


def build_aggregations(df: DataFrame) -> DataFrame:
    logger.info("Creating revenue aggregates per product and hour window")
    return (
        df
        .withWatermark("order_ts", "15 minutes")
        .groupBy(
            window(col("order_ts"), "1 hour", "15 minutes").alias("time_window"),
            col("product"),
        )
        .agg(
            sum_(col("line_amount")).alias("revenue"),
        )
        .select(
            col("product"),
            round_(col("revenue"), 2).alias("revenue"),
            # col("time_window.start").alias("window_start"),
            # col("time_window.end").alias("window_end"),
            col("time_window").getField("start").alias("window_start"),
            col("time_window").getField("end").alias("window_end"),
        )
    )


def main() -> None:
    spark = build_spark_session()

    logger.info("Spark version: %s, Scala: %s", spark.version, _infer_spark_artifact_coordinates()[0])
    try:
        spark._jvm.org.apache.spark.sql.kafka010.KafkaSourceProvider  # type: ignore[attr-defined]
        logger.info("Kafka connector present.")
    except Exception:
        logger.error("Kafka connector missing/incompatible. Check Spark/Scala vs spark-sql-kafka package.")
        raise

    logger.info(
        "Subscribing to Kafka topic '%s' at %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP
    )
    logger.info(
        "Kafka reader startingOffsets set to '%s'", os.environ.get("KAFKA_STARTING_OFFSETS", "latest")
    )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "latest"))
        .load()
    )

    logger.info("Kafka source schema: %s", raw_stream.schema.simpleString())

    parsed_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )

    logger.info("Configured event schema with fields: %s", ", ".join(field.name for field in EVENT_SCHEMA))

    cleaned = transform_orders(parsed_stream)
    aggregates = build_aggregations(cleaned)

    query = (
        aggregates.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=os.environ.get("STREAM_TRIGGER_INTERVAL", "30 seconds"))
    )

    logger.info(
        "Starting streaming query with checkpoint %s and output %s",
        CHECKPOINT_DIR,
        OUTPUT_PATH,
    )
    logger.info("Streaming query output mode: append")

    query.start().awaitTermination()


if __name__ == "__main__":
    main()
