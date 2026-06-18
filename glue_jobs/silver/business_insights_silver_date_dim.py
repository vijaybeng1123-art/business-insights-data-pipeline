import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lower, trim, when


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


BRONZE_PATH = "s3://business-insights-de/bronze/date_dim/"
SILVER_PATH = "s3://business-insights-de/silver/date_dim/"


df = spark.read.parquet(BRONZE_PATH)

silver_df = (
    df.select(
        col("date_key").cast("date").alias("date_key"),
        col("year").cast("int").alias("year"),
        col("month").cast("int").alias("month"),
        col("week").cast("int").alias("week"),
        lower(trim(col("day_of_week"))).alias("day_of_week"),
        col("is_weekend").cast("boolean").alias("is_weekend"),
        col("is_holiday").cast("boolean").alias("is_holiday"),
        trim(col("holiday_name")).alias("holiday_name")
    )
    .filter(col("date_key").isNotNull())
    .withColumn(
        "day_type",
        when(col("is_weekend") == True, "weekend").otherwise("weekday")
    )
    .withColumn("silver_processed_timestamp", current_timestamp())
)

silver_df.write.mode("overwrite").parquet(SILVER_PATH)

print(f"Silver date_dim written successfully to {SILVER_PATH}")

job.commit()