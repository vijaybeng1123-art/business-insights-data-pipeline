from datetime import datetime
import time

import boto3
from airflow.sdk import dag, task


AWS_REGION = "us-east-1"


BRONZE_JOBS = [
    "business-insights-bronze-date-dim",
    "business-insights-bronze-order-items",
    "business-insights-bronze-order-item-options",
]


SILVER_JOBS = [
    "business-insights-silver-date-dim",
    "business-insights-silver-order-items",
    "business-insights-silver-order-item-options",
]


GOLD_JOBS = [
    "business-insights-gold-customer-ltv-daily",
    "business-insights-gold-customer-segments",
    "business-insights-gold-sales-trends",
    "business-insights-gold-loyalty-location-discount",
]


def run_glue_job_and_wait(job_name: str):
    glue_client = boto3.client("glue", region_name=AWS_REGION)

    print(f"Starting Glue job: {job_name}")

    response = glue_client.start_job_run(JobName=job_name)
    job_run_id = response["JobRunId"]

    print(f"Started Glue job: {job_name}")
    print(f"Glue Job Run ID: {job_run_id}")

    while True:
        job_run = glue_client.get_job_run(
            JobName=job_name,
            RunId=job_run_id
        )

        job_status = job_run["JobRun"]["JobRunState"]

        print(f"Glue job {job_name} status: {job_status}")

        if job_status == "SUCCEEDED":
            print(f"Glue job succeeded: {job_name}")
            return {
                "job_name": job_name,
                "job_run_id": job_run_id,
                "status": job_status
            }

        if job_status in ["FAILED", "STOPPED", "TIMEOUT", "ERROR"]:
            error_message = job_run["JobRun"].get("ErrorMessage", "No error message returned")
            raise Exception(
                f"Glue job failed: {job_name}. "
                f"Run ID: {job_run_id}. "
                f"Status: {job_status}. "
                f"Error: {error_message}"
            )

        time.sleep(30)


@dag(
    dag_id="business_insights_full_pipeline",
    description="SQL Server to Bronze, Silver, and Gold Delta Lake pipeline for Business Insights",
    start_date=datetime(2026, 6, 14),
    schedule=None,
    catchup=False,
    tags=["business-insights", "aws-glue", "delta-lake", "bronze-silver-gold"],
)
def business_insights_full_pipeline():

    @task
    def start_pipeline():
        print("Starting Business Insights full data pipeline")
        print("Architecture: SQL Server -> Bronze -> Silver -> Gold Delta Lake")
        return "pipeline_started"

    @task
    def run_bronze_job(job_name: str):
        print(f"Running Bronze job: {job_name}")
        return run_glue_job_and_wait(job_name)

    @task
    def run_silver_job(job_name: str):
        print(f"Running Silver job: {job_name}")
        return run_glue_job_and_wait(job_name)

    @task
    def run_gold_job(job_name: str):
        print(f"Running Gold job: {job_name}")
        return run_glue_job_and_wait(job_name)

    @task
    def validate_pipeline_completion():
        print("All Bronze, Silver, and Gold jobs completed successfully")
        print("Gold Delta Lake outputs are available in s3://business-insights-de/gold/")
        return "pipeline_completed_successfully"

    start = start_pipeline()

    bronze_tasks = run_bronze_job.expand(job_name=BRONZE_JOBS)

    silver_tasks = run_silver_job.expand(job_name=SILVER_JOBS)

    gold_tasks = run_gold_job.expand(job_name=GOLD_JOBS)

    finish = validate_pipeline_completion()

    start >> bronze_tasks >> silver_tasks >> gold_tasks >> finish


business_insights_full_pipeline()
