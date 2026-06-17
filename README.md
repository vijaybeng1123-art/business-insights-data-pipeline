# Business Insights Data Pipeline

## Project Overview

This project builds an AWS-only data engineering pipeline that ingests SQL Server restaurant transaction data, transforms it through Bronze, Silver, and Gold layers, and visualizes business metrics in a Streamlit dashboard.

## Architecture

SQL Server / Amazon RDS  
→ AWS Glue Bronze Jobs  
→ Amazon S3 Bronze Layer  
→ AWS Glue Silver Jobs  
→ Amazon S3 Silver Layer  
→ AWS Glue Gold Jobs using PySpark and Delta Lake  
→ Amazon S3 Gold Delta Tables  
→ AWS Glue Data Catalog / Amazon Athena  
→ Streamlit Dashboard  

Amazon MWAA / Apache Airflow orchestrates the full pipeline.

## Tech Stack

- AWS RDS SQL Server
- AWS Glue PySpark
- Amazon S3
- Delta Lake
- Amazon MWAA / Apache Airflow
- AWS Glue Data Catalog
- Amazon Athena
- Streamlit
- GitHub Actions

## Business Metrics

The Gold layer calculates:

- Customer Lifetime Value
- Customer CLV Groups: High, Medium, Low
- Customer RFM Segmentation
- Churn Risk Indicators
- Sales Trends by Day, Week, and Month
- Loyalty Program Impact
- Location Performance
- Pricing and Discount Effectiveness

## Dashboard

The Streamlit dashboard includes:

1. Customer Segmentation Dashboard
2. Churn Risk Indicators Dashboard
3. Sales Trends and Seasonality Dashboard
4. Loyalty Program Impact Dashboard
5. Location Performance Dashboard
6. Pricing and Discount Effectiveness Dashboard

## Why This Tech Stack Was Used

AWS Glue was selected because it is serverless, supports PySpark, integrates with JDBC sources, and fits the AWS-only requirement.

Amazon S3 was selected as the data lake storage layer because it is scalable, cost-effective, and supports Bronze, Silver, and Gold architecture.

Delta Lake was used in the Gold layer because it provides reliable transaction logs and supports curated analytics tables.

Amazon MWAA was selected for orchestration because it provides managed Apache Airflow for scheduling, retries, and dependency management.

Athena was used because it allows SQL querying directly against S3 Gold tables without adding an external data warehouse.

Streamlit was used because it provides a fast Python-based way to build interactive dashboards.

GitHub Actions was used for CI/CD validation of code before submission.

## Running the Streamlit Dashboard

```bash
conda activate streamlit-bi
cd streamlit_dashboard
python -m streamlit run app.py
