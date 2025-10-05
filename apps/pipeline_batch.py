import glob
import logging
import os
import sys
import time

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, expr, to_date, to_timestamp, trim,
    when, lit, round as round_, sum as sum_, countDistinct
)
from pyspark.sql.types import DoubleType


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
logger = logging.getLogger("retail_pipeline")

USE_HDFS = True
if USE_HDFS:
    INPUT_PATH  = "hdfs://namenode:8020/data/input"
    OUT_PARQUET = "hdfs://namenode:8020/data/output/analysis_parquet"
    OUT_LOCAL_CSV = "/opt/spark-data/output/analysis_csv"
else:
    INPUT_PATH  = "/opt/spark-data/input"
    OUT_PARQUET = "/opt/spark-data/output/analysis_parquet"
    OUT_LOCAL_CSV = "/opt/spark-data/output/analysis_csv"

APP_NAME = "batch.retail_sales_clean_analyze"

logger.info("Starting Spark batch pipeline '%s'", APP_NAME)
logger.info("Connecting to Spark master via SparkSession builder ...")
spark = None
try:
    spark = (SparkSession.builder
             .appName(APP_NAME)
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    logger.info("Spark session established (appId=%s)", spark.sparkContext.applicationId)
    spark.sparkContext.setLogLevel("WARN")
except Exception:
    logger.exception("Unable to create Spark session")
    raise

def wait_for_input_files(spark_session, path, use_hdfs, pattern="*.csv",
                         poll_interval=5, timeout_seconds=300):
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
            except Py4JJavaError as err:
                logger.warning("Could not access HDFS path %s: %s", path, err.java_exception)

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


def main():
    logger.info("Reading input from: %s", INPUT_PATH)
    wait_for_input_files(spark, INPUT_PATH, USE_HDFS)

    logger.info("Loading CSV data into DataFrame ...")
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .option("recursiveFileLookup", "true")  # reads all files under the directory
          .csv(INPUT_PATH))

# df = (spark.read
#       .option("header", True)
#       .option("inferSchema", True)
#       .csv(INPUT_PATH))

    if df.rdd.isEmpty():
        logger.warning("No input files found. Exiting gracefully.")
        spark.stop()
        raise SystemExit(0)

    cols = [c.lower().strip() for c in df.columns]
    df = df.toDF(*cols)

    product_col = "product" if "product" in cols else ("item" if "item" in cols else None)
    if product_col:
        logger.info("Normalising product column from '%s'", product_col)
        df = df.withColumn("product", trim(col(product_col)))
    else:
        logger.info("Product column missing — filling UNKNOWN placeholder")
        df = df.withColumn("product", lit("UNKNOWN"))

    date_col = None
    for cand in ["order_date", "date", "order_time", "timestamp", "event_time"]:
        if cand in cols:
            date_col = cand
            break

    if date_col is None:
        logger.info("Date column missing — defaulting order_date to epoch")
        df = df.withColumn("order_date", to_date(lit("1970-01-01")))
    else:
        logger.info("Parsing order timestamps from '%s'", date_col)
        df = (df
              .withColumn("order_ts",
                          when(col(date_col).cast("timestamp").isNotNull(),
                               to_timestamp(col(date_col)))
                          .otherwise(None))
              .withColumn("order_date",
                          when(col("order_ts").isNotNull(), col("order_ts").cast("date"))
                          .otherwise(to_date(col(date_col))))
              .drop("order_ts"))

    has_amount = "amount" in cols
    has_qty_price = ("quantity" in cols) and ("unit_price" in cols or "price" in cols)

    if has_amount:
        logger.info("Using provided 'amount' column for monetary values")
        df = df.withColumn("amount", expr("try_cast(amount AS double)"))
    elif has_qty_price:
        price_col = "unit_price" if "unit_price" in cols else "price"
        logger.info("Computing amount from quantity * %s", price_col)
        df = (df
              .withColumn("quantity", expr("try_cast(quantity AS double)"))
              .withColumn(price_col, expr(f"try_cast(`{price_col}` AS double)"))
              .withColumn("amount", (col("quantity") * col(price_col)).cast(DoubleType())))
    else:
        logger.info("No amount/quantity-price columns detected — defaulting to 0.0")
        df = df.withColumn("amount", lit(0.0).cast(DoubleType()))

    clean = (df
             .filter(col("amount").isNotNull())
             .withColumn("amount", round_(col("amount"), 2))
             .withColumn("product",
                         when(col("product").isNull() | (trim(col("product")) == ""),
                              lit("UNKNOWN"))
                         .otherwise(col("product"))))

    logger.info("Aggregating daily totals per product ...")
    daily_product = (clean.groupBy("order_date", "product")
                           .agg(sum_("amount").alias("total_amount"))
                           .orderBy("order_date", "product"))

    logger.info("Calculating KPI snapshot ...")
    kpis = (clean.agg(
                sum_("amount").alias("grand_total"),
                countDistinct("product").alias("distinct_products"))
           .withColumn("rows", lit(clean.count())))

    logger.info("Writing detailed parquet to: %s", OUT_PARQUET)
    (daily_product
        .repartition("order_date")
        .write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(OUT_PARQUET))

    logger.info("Writing summarized CSV (single file) to: %s", OUT_LOCAL_CSV)
    (daily_product
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(OUT_LOCAL_CSV))

    logger.info("KPI snapshot:")
    kpis.show(truncate=False)
    (kpis.coalesce(1)
         .write.mode("overwrite")
         .option("header", True)
         .csv(OUT_LOCAL_CSV + "_kpis"))

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
