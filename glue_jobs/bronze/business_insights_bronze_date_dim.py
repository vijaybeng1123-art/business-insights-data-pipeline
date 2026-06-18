import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CONNECTION_NAME = "Jdbc connection"
SOURCE_TABLE = "dbo.date_dim"
TARGET_PATH = "s3://business-insights-de/bronze/date_dim/"

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": CONNECTION_NAME,
        "dbtable": SOURCE_TABLE
    },
    transformation_ctx="read_date_dim"
)

df = dynamic_frame.toDF()

bronze_df = (
    df.withColumn("bronze_ingestion_timestamp", current_timestamp())
      .withColumn("bronze_source_table", lit(SOURCE_TABLE))
)

bronze_df.write.mode("overwrite").parquet(TARGET_PATH)

print(f"Bronze load complete for {SOURCE_TABLE} to {TARGET_PATH}")

job.commit()