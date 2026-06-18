import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    coalesce,
    datediff,
    lag,
    lit,
    max as spark_max,
    min as spark_min,
    avg,
    round,
    sum as spark_sum,
    countDistinct,
    when
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
GOLD_PATH = "s3://business-insights-de/gold/customer_segments/"


order_items = spark.read.parquet(ORDER_ITEMS_PATH)
order_options = spark.read.parquet(ORDER_OPTIONS_PATH)


item_revenue = (
    order_items
    .filter(col("user_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .groupBy("user_id", "order_id", "order_date")
    .agg(spark_sum("item_gross_amount").alias("item_revenue"))
)

option_revenue = (
    order_options
    .filter(col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(spark_sum("option_gross_amount").alias("option_revenue"))
)

orders = (
    item_revenue
    .join(option_revenue, on="order_id", how="left")
    .withColumn("option_revenue", coalesce(col("option_revenue"), lit(0.0)))
    .withColumn("order_total_revenue", round(col("item_revenue") + col("option_revenue"), 2))
)


customer_base = (
    orders
    .groupBy("user_id")
    .agg(
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date"),
        countDistinct("order_id").alias("frequency_orders"),
        round(spark_sum("order_total_revenue"), 2).alias("monetary_total_spend")
    )
    .withColumn("recency_days", datediff(current_date(), col("last_order_date")))
)


order_dates = orders.select("user_id", "order_id", "order_date").dropDuplicates()

gap_window = Window.partitionBy("user_id").orderBy("order_date")

order_gaps = (
    order_dates
    .withColumn("previous_order_date", lag("order_date").over(gap_window))
    .withColumn("gap_days", datediff(col("order_date"), col("previous_order_date")))
)

avg_gaps = (
    order_gaps
    .groupBy("user_id")
    .agg(round(avg("gap_days"), 2).alias("average_gap_between_orders_days"))
)


recent_spend = (
    orders
    .withColumn(
        "period",
        when(datediff(current_date(), col("order_date")) <= 30, "last_30_days")
        .when(
            (datediff(current_date(), col("order_date")) > 30) &
            (datediff(current_date(), col("order_date")) <= 60),
            "previous_30_days"
        )
    )
    .filter(col("period").isNotNull())
    .groupBy("user_id", "period")
    .agg(spark_sum("order_total_revenue").alias("period_spend"))
)

last_30 = recent_spend.filter(col("period") == "last_30_days").select(
    "user_id",
    col("period_spend").alias("last_30_day_spend")
)

prev_30 = recent_spend.filter(col("period") == "previous_30_days").select(
    "user_id",
    col("period_spend").alias("previous_30_day_spend")
)

spend_change = (
    last_30
    .join(prev_30, on="user_id", how="full")
    .withColumn("last_30_day_spend", coalesce(col("last_30_day_spend"), lit(0.0)))
    .withColumn("previous_30_day_spend", coalesce(col("previous_30_day_spend"), lit(0.0)))
    .withColumn(
        "spend_change_pct",
        when(col("previous_30_day_spend") == 0, None)
        .otherwise(round(((col("last_30_day_spend") - col("previous_30_day_spend")) / col("previous_30_day_spend")) * 100, 2))
    )
)


gold_df = (
    customer_base
    .join(avg_gaps, on="user_id", how="left")
    .join(spend_change, on="user_id", how="left")
    .withColumn(
        "rfm_segment",
        when(
            (col("recency_days") <= 30) &
            (col("frequency_orders") >= 5) &
            (col("monetary_total_spend") >= 500),
            "vip"
        )
        .when(
            (col("recency_days") <= 30) &
            (col("frequency_orders") <= 2),
            "new_customer"
        )
        .when(
            (col("recency_days") > 45) &
            (col("frequency_orders") <= 3),
            "churn_risk"
        )
        .otherwise("standard")
    )
    .withColumn(
        "inactivity_tag",
        when(col("recency_days") > 90, "inactive")
        .when(col("recency_days") > 45, "at_risk")
        .otherwise("active")
    )
    .withColumn("gold_processed_timestamp", current_timestamp())
)

gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"Gold customer segments Delta table written to {GOLD_PATH}")

job.commit()