import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    coalesce,
    lit,
    round,
    sum as spark_sum,
    avg,
    countDistinct,
    when,
    dense_rank
)
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


ORDER_ITEMS_PATH = "s3://business-insights-de/silver/order_items/"
ORDER_OPTIONS_PATH = "s3://business-insights-de/silver/order_item_options/"
GOLD_PATH = "s3://business-insights-de/gold/loyalty_location_discount/"


order_items = spark.read.parquet(ORDER_ITEMS_PATH)
order_options = spark.read.parquet(ORDER_OPTIONS_PATH)


item_order_revenue = (
    order_items
    .filter(col("order_id").isNotNull())
    .groupBy(
        "order_id",
        "restaurant_id",
        "user_id",
        "is_loyalty",
        "loyalty_status",
        "order_date",
        "currency"
    )
    .agg(
        round(spark_sum("item_gross_amount"), 2).alias("item_revenue")
    )
)

option_order_revenue = (
    order_options
    .filter(col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        round(spark_sum("option_gross_amount"), 2).alias("option_revenue"),
        round(spark_sum(when(col("option_price") < 0, col("option_gross_amount")).otherwise(0.0)), 2).alias("discount_amount")
    )
)

orders = (
    item_order_revenue
    .join(option_order_revenue, on="order_id", how="left")
    .withColumn("option_revenue", coalesce(col("option_revenue"), lit(0.0)))
    .withColumn("discount_amount", coalesce(col("discount_amount"), lit(0.0)))
    .withColumn("order_total_revenue", round(col("item_revenue") + col("option_revenue"), 2))
    .withColumn("has_discount", when(col("discount_amount") < 0, True).otherwise(False))
)


loyalty_metrics = (
    orders
    .groupBy("loyalty_status", "currency")
    .agg(
        round(spark_sum("order_total_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("total_customers"),
        round(avg("order_total_revenue"), 2).alias("average_order_value")
    )
    .withColumn("metric_type", lit("loyalty_program_impact"))
    .withColumn("metric_dimension", col("loyalty_status"))
)

location_rank_window = Window.orderBy(col("total_revenue").desc())

location_metrics = (
    orders
    .groupBy("restaurant_id", "currency")
    .agg(
        round(spark_sum("order_total_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("total_customers"),
        round(avg("order_total_revenue"), 2).alias("average_order_value")
    )
    .withColumn("location_revenue_rank", dense_rank().over(location_rank_window))
    .withColumn("metric_type", lit("location_performance"))
    .withColumn("metric_dimension", col("restaurant_id"))
)

discount_metrics = (
    orders
    .groupBy("has_discount", "currency")
    .agg(
        round(spark_sum("order_total_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("user_id").alias("total_customers"),
        round(avg("order_total_revenue"), 2).alias("average_order_value"),
        round(spark_sum("discount_amount"), 2).alias("total_discount_amount")
    )
    .withColumn("metric_type", lit("discount_effectiveness"))
    .withColumn("metric_dimension", when(col("has_discount") == True, "discounted_orders").otherwise("non_discounted_orders"))
)

loyalty_out = loyalty_metrics.select(
    "metric_type",
    "metric_dimension",
    "currency",
    "total_revenue",
    "total_orders",
    "total_customers",
    "average_order_value"
)

location_out = location_metrics.select(
    "metric_type",
    "metric_dimension",
    "currency",
    "total_revenue",
    "total_orders",
    "total_customers",
    "average_order_value"
)

discount_out = discount_metrics.select(
    "metric_type",
    "metric_dimension",
    "currency",
    "total_revenue",
    "total_orders",
    "total_customers",
    "average_order_value"
)

gold_df = (
    loyalty_out
    .unionByName(location_out)
    .unionByName(discount_out)
    .withColumn("gold_processed_timestamp", current_timestamp())
)

gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"Gold loyalty/location/discount Delta table written to {GOLD_PATH}")

job.commit()