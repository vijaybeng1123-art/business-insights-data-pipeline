import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    to_date,
    trim,
    upper,
    lower,
    when,
    round
)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


BRONZE_PATH = "s3://business-insights-de/bronze/order_items/"
SILVER_PATH = "s3://business-insights-de/silver/order_items/"


df = spark.read.parquet(BRONZE_PATH)

silver_df = (
    df.select(
        trim(col("APP_NAME")).alias("app_name"),
        trim(col("RESTAURANT_ID")).alias("restaurant_id"),
        trim(col("CREATION_TIME_UTC")).alias("creation_time_utc_raw"),
        trim(col("ORDER_ID")).alias("order_id"),
        trim(col("USER_ID")).alias("user_id"),
        col("PRINTED_CARD_NUMBER").cast("long").alias("printed_card_number"),
        col("IS_LOYALTY").cast("boolean").alias("is_loyalty"),
        upper(trim(col("CURRENCY"))).alias("currency"),
        trim(col("LINEITEM_ID")).alias("lineitem_id"),
        trim(col("ITEM_CATEGORY")).alias("item_category"),
        trim(col("ITEM_NAME")).alias("item_name"),
        col("ITEM_PRICE").cast("double").alias("item_price"),
        col("ITEM_QUANTITY").cast("int").alias("item_quantity")
    )
    .filter(col("order_id").isNotNull())
    .filter(col("lineitem_id").isNotNull())
    .filter(col("item_price").isNotNull())
    .filter(col("item_quantity").isNotNull())
    .withColumn("creation_timestamp_utc", to_timestamp(col("creation_time_utc_raw")))
    .withColumn("order_date", to_date(col("creation_timestamp_utc")))
    .withColumn(
        "loyalty_status",
        when(col("is_loyalty") == True, "loyalty").otherwise("non_loyalty")
    )
    .withColumn(
        "item_category",
        when(col("item_category").isNull() | (col("item_category") == ""), "unknown")
        .otherwise(lower(col("item_category")))
    )
    .withColumn(
        "item_name",
        when(col("item_name").isNull() | (col("item_name") == ""), "unknown")
        .otherwise(lower(col("item_name")))
    )
    .withColumn("item_gross_amount", round(col("item_price") * col("item_quantity"), 2))
    .withColumn("silver_processed_timestamp", current_timestamp())
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)

print(f"Silver order_items written successfully to {SILVER_PATH}")

job.commit()