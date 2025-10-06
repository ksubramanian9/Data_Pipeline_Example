"""Structured Streaming job for real-time retail analytics.

The job consumes JSON order events from Kafka, applies the same cleansing
rules as the batch pipeline and maintains rolling revenue metrics per
product. Results are persisted to HDFS in parquet format so that the
existing dashboard (or other tools) can use the aggregated outputs.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
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
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def build_spark_session() -> SparkSession:
    logger.info("Creating Spark session '%s'", APP_NAME)
    builder = (
        SparkSession.builder
        .appName(APP_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
    )

    # Honour an explicit SPARK_JARS_PACKAGES if provided, otherwise make sure
    # the Kafka connector is available. Additional packages can be supplied via
    # SPARK_EXTRA_PACKAGES (comma-separated) without losing the default.
    if "SPARK_JARS_PACKAGES" not in os.environ:
        kafka_package = os.environ.get("SPARK_KAFKA_PACKAGE", DEFAULT_KAFKA_PACKAGE)
        extra_packages = os.environ.get("SPARK_EXTRA_PACKAGES", "").strip()
        packages = ",".join(
            pkg
            for pkg in [kafka_package, extra_packages]
            if pkg
        )
        if packages:
            builder = builder.config("spark.jars.packages", packages)

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
        df = df.withColumn("order_ts", to_timestamp(ts_expr))

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
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
        )
    )


def main() -> None:
    spark = build_spark_session()

    logger.info(
        "Subscribing to Kafka topic '%s' at %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP
    )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "latest"))
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )

    cleaned = transform_orders(parsed_stream)
    aggregates = build_aggregations(cleaned)

    query = (
        aggregates.writeStream
        .outputMode("update")
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

    query.start().awaitTermination()


if __name__ == "__main__":
    main()
