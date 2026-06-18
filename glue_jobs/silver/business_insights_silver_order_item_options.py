import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
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


BRONZE_PATH = "s3://business-insights-de/bronze/order_item_options/"
SILVER_PATH = "s3://business-insights-de/silver/order_item_options/"


df = spark.read.parquet(BRONZE_PATH)

silver_df = (
    df.select(
        trim(col("ORDER_ID")).alias("order_id"),
        trim(col("LINEITEM_ID")).alias("lineitem_id"),
        trim(col("OPTION_GROUP_NAME")).alias("option_group_name"),
        trim(col("OPTION_NAME")).alias("option_name"),
        col("OPTION_PRICE").cast("double").alias("option_price"),
        col("OPTION_QUANTITY").cast("int").alias("option_quantity")
    )
    .filter(col("order_id").isNotNull())
    .filter(col("lineitem_id").isNotNull())
    .filter(col("option_price").isNotNull())
    .filter(col("option_quantity").isNotNull())
    .withColumn(
        "option_group_name",
        when(col("option_group_name").isNull() | (col("option_group_name") == ""), "unknown")
        .otherwise(lower(col("option_group_name")))
    )
    .withColumn(
        "option_name",
        when(col("option_name").isNull() | (col("option_name") == ""), "unknown")
        .otherwise(lower(col("option_name")))
    )
    .withColumn("option_gross_amount", round(col("option_price") * col("option_quantity"), 2))
    .withColumn("silver_processed_timestamp", current_timestamp())
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)

print(f"Silver order_item_options written successfully to {SILVER_PATH}")

job.commit()