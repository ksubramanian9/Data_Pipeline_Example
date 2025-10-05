from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, to_date, to_timestamp, trim,
    when, lit, round as round_, sum as sum_, countDistinct
)
from pyspark.sql.types import DoubleType

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

spark = (SparkSession.builder
         .appName(APP_NAME)
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print(f"[INFO] Reading input from: {INPUT_PATH}")
INPUT_PATH = "hdfs://namenode:8020/data/input"

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
    print("[WARN] No input files found. Exiting gracefully.")
    spark.stop()
    raise SystemExit(0)

cols = [c.lower().strip() for c in df.columns]
df = df.toDF(*cols)

product_col = "product" if "product" in cols else ("item" if "item" in cols else None)
if product_col:
    df = df.withColumn("product", trim(col(product_col)))
else:
    df = df.withColumn("product", lit("UNKNOWN"))

date_col = None
for cand in ["order_date", "date", "order_time", "timestamp", "event_time"]:
    if cand in cols:
        date_col = cand
        break

if date_col is None:
    df = df.withColumn("order_date", to_date(lit("1970-01-01")))
else:
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
    df = df.withColumn("amount", col("amount").cast(DoubleType()))
elif has_qty_price:
    price_col = "unit_price" if "unit_price" in cols else "price"
    df = (df
          .withColumn("quantity", col("quantity").cast(DoubleType()))
          .withColumn(price_col, col(price_col).cast(DoubleType()))
          .withColumn("amount", (col("quantity") * col(price_col)).cast(DoubleType())))
else:
    df = df.withColumn("amount", lit(0.0).cast(DoubleType()))

clean = (df
         .filter(col("amount").isNotNull())
         .withColumn("amount", round_(col("amount"), 2))
         .withColumn("product",
                     when(col("product").isNull() | (trim(col("product")) == ""),
                          lit("UNKNOWN"))
                     .otherwise(col("product"))))

daily_product = (clean.groupBy("order_date", "product")
                       .agg(sum_("amount").alias("total_amount"))
                       .orderBy("order_date", "product"))

kpis = (clean.agg(
            sum_("amount").alias("grand_total"),
            countDistinct("product").alias("distinct_products"))
       .withColumn("rows", lit(clean.count())))

print(f"[INFO] Writing detailed parquet to: {OUT_PARQUET}")
(daily_product
    .repartition("order_date")
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(OUT_PARQUET))

print(f"[INFO] Writing summarized CSV (single file) to: {OUT_LOCAL_CSV}")
(daily_product
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(OUT_LOCAL_CSV))

print("[INFO] KPIs snapshot:")
kpis.show(truncate=False)
(kpis.coalesce(1)
     .write.mode("overwrite")
     .option("header", True)
     .csv(OUT_LOCAL_CSV + "_kpis"))

print("[INFO] Done.")
spark.stop()
