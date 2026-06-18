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
    countDistinct,
    ntile,
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
GOLD_PATH = "s3://business-insights-de/gold/customer_ltv_daily/"


order_items = spark.read.parquet(ORDER_ITEMS_PATH)
order_options = spark.read.parquet(ORDER_OPTIONS_PATH)


item_revenue = (
    order_items
    .filter(col("user_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("order_date").isNotNull())
    .groupBy("user_id", "order_id", "order_date")
    .agg(
        spark_sum("item_gross_amount").alias("item_revenue"),
        countDistinct("lineitem_id").alias("line_items_count")
    )
)

option_revenue = (
    order_options
    .filter(col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        spark_sum("option_gross_amount").alias("option_revenue")
    )
)

order_revenue = (
    item_revenue
    .join(option_revenue, on="order_id", how="left")
    .withColumn("option_revenue", coalesce(col("option_revenue"), lit(0.0)))
    .withColumn("order_total_revenue", round(col("item_revenue") + col("option_revenue"), 2))
)

daily_customer_revenue = (
    order_revenue
    .groupBy("user_id", "order_date")
    .agg(
        spark_sum("order_total_revenue").alias("daily_revenue"),
        countDistinct("order_id").alias("daily_order_count")
    )
)

ltv_window = (
    Window
    .partitionBy("user_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

customer_ltv_daily = (
    daily_customer_revenue
    .withColumn("customer_ltv_to_date", round(spark_sum("daily_revenue").over(ltv_window), 2))
    .withColumn("lifetime_order_count_to_date", spark_sum("daily_order_count").over(ltv_window))
)

latest_customer_ltv = (
    customer_ltv_daily
    .groupBy("user_id")
    .agg(spark_sum("daily_revenue").alias("total_ltv"))
)

clv_rank_window = Window.orderBy(col("total_ltv").desc())

customer_clv_groups = (
    latest_customer_ltv
    .withColumn("clv_rank_bucket", ntile(5).over(clv_rank_window))
    .withColumn(
        "clv_group",
        when(col("clv_rank_bucket") == 1, "high_clv")
        .when(col("clv_rank_bucket") == 5, "low_clv")
        .otherwise("medium_clv")
    )
    .select("user_id", "clv_group")
)

gold_df = (
    customer_ltv_daily
    .join(customer_clv_groups, on="user_id", how="left")
    .withColumn("gold_processed_timestamp", current_timestamp())
)

gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

print(f"Gold customer LTV daily Delta table written to {GOLD_PATH}")

job.commit()