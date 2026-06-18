import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month,
    weekofyear,
    lpad,
    concat,
    lit,
    round,
    sum as spark_sum,
    countDistinct
)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


ORDER_ITEMS_PATH = "s3://business-insights-de/silver/order_items/"
GOLD_PATH = "s3://business-insights-de/gold/sales_trends/"


order_items = spark.read.parquet(ORDER_ITEMS_PATH)

base = (
    order_items
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .withColumn("sales_year", year(col("order_date")))
    .withColumn("sales_month", month(col("order_date")))
    .withColumn("sales_week", weekofyear(col("order_date")))
    .withColumn(
        "sales_year_month",
        concat(
            col("sales_year").cast("string"),
            lit("-"),
            lpad(col("sales_month").cast("string"), 2, "0")
        )
    )
    .withColumn(
        "sales_year_week",
        concat(
            col("sales_year").cast("string"),
            lit("-W"),
            lpad(col("sales_week").cast("string"), 2, "0")
        )
    )
)


daily = (
    base
    .groupBy(
        col("order_date").cast("string").alias("period_value"),
        "restaurant_id",
        "item_category",
        "currency"
    )
    .agg(
        round(spark_sum("item_gross_amount"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("order_count"),
        countDistinct("user_id").alias("customer_count")
    )
    .withColumn("period_type", lit("daily"))
)


weekly = (
    base
    .groupBy(
        col("sales_year_week").alias("period_value"),
        "restaurant_id",
        "item_category",
        "currency"
    )
    .agg(
        round(spark_sum("item_gross_amount"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("order_count"),
        countDistinct("user_id").alias("customer_count")
    )
    .withColumn("period_type", lit("weekly"))
)


monthly = (
    base
    .groupBy(
        col("sales_year_month").alias("period_value"),
        "restaurant_id",
        "item_category",
        "currency"
    )
    .agg(
        round(spark_sum("item_gross_amount"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("order_count"),
        countDistinct("user_id").alias("customer_count")
    )
    .withColumn("period_type", lit("monthly"))
)


gold_df = (
    daily.select(
        "period_type",
        "period_value",
        "restaurant_id",
        "item_category",
        "currency",
        "total_revenue",
        "order_count",
        "customer_count"
    )
    .unionByName(
        weekly.select(
            "period_type",
            "period_value",
            "restaurant_id",
            "item_category",
            "currency",
            "total_revenue",
            "order_count",
            "customer_count"
        )
    )
    .unionByName(
        monthly.select(
            "period_type",
            "period_value",
            "restaurant_id",
            "item_category",
            "currency",
            "total_revenue",
            "order_count",
            "customer_count"
        )
    )
    .withColumn("gold_processed_timestamp", current_timestamp())
)


gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"Gold sales trends Delta table written successfully to {GOLD_PATH}")

job.commit()