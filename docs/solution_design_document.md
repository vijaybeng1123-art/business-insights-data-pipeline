# Business Insights Data Pipeline - Solution Design Document

## 1. Project Objective

The objective of this project is to design and implement an AWS-only data engineering pipeline that ingests restaurant transaction data from SQL Server, transforms the data through Bronze, Silver, and Gold layers, and produces business insights through an interactive Streamlit dashboard.

The final solution supports customer lifetime value analysis, RFM segmentation, churn risk indicators, sales trends, loyalty program analysis, location performance, and pricing/discount effectiveness.

## 2. Source System

The source system is SQL Server hosted on Amazon RDS.

Source tables include:

- dbo.date_dim
- dbo.order_items
- dbo.order_item_options

AWS Glue connects to SQL Server using a JDBC connection.

## 3. AWS Architecture

The pipeline architecture is:

SQL Server / Amazon RDS  
→ AWS Glue Bronze ingestion jobs  
→ Amazon S3 Bronze layer  
→ AWS Glue Silver transformation jobs  
→ Amazon S3 Silver layer  
→ AWS Glue Gold transformation jobs using PySpark and Delta Lake  
→ Amazon S3 Gold Delta tables  
→ AWS Glue Data Catalog and Amazon Athena  
→ Streamlit dashboard  

Amazon MWAA orchestrates the Glue jobs.

## 4. Bronze Layer Design

The Bronze layer stores raw data extracted from SQL Server.

Bronze outputs:

- s3://business-insights-de/bronze/date_dim/
- s3://business-insights-de/bronze/order_items/
- s3://business-insights-de/bronze/order_item_options/

The Bronze layer preserves source-level data and adds ingestion audit columns.

## 5. Silver Layer Design

The Silver layer cleans and standardizes the Bronze data.

Silver transformations include:

- Column renaming
- Data type casting
- Null handling
- Date parsing
- Revenue calculations
- Standardized text casing
- Audit timestamp creation

Silver outputs:

- s3://business-insights-de/silver/date_dim/
- s3://business-insights-de/silver/order_items/
- s3://business-insights-de/silver/order_item_options/

## 6. Gold Layer Design

The Gold layer contains curated analytical datasets stored in Delta Lake format.

Gold outputs:

- s3://business-insights-de/gold/customer_ltv_daily/
- s3://business-insights-de/gold/customer_segments/
- s3://business-insights-de/gold/sales_trends/
- s3://business-insights-de/gold/loyalty_location_discount/

Delta Lake was selected for the Gold layer because it provides reliable table metadata and transaction log support for curated analytical outputs.

## 7. Business Metrics

The Gold layer calculates the following business metrics:

### Customer Lifetime Value

- Revenue per order
- Total customer spend
- Daily LTV evolution
- CLV groups: High, Medium, Low

### Customer Segmentation

- Recency
- Frequency
- Monetary spend
- RFM segment
- VIP, New Customer, Churn Risk indicators

### Churn Risk

- Days since last order
- Average gap between orders
- Spend change percentage
- Inactivity threshold greater than 45 days

### Sales Trends

- Daily revenue
- Weekly revenue
- Monthly revenue
- Revenue by location and menu category

### Loyalty Program Impact

- Loyalty vs non-loyalty revenue
- Average order value
- Repeat behavior
- Customer count

### Location Performance

- Revenue by restaurant/location
- Order volume
- Average order value
- Customer count

### Pricing and Discount Effectiveness

- Discounted vs non-discounted orders
- Revenue comparison
- Average order value comparison

## 8. Scheduling and Orchestration

Amazon MWAA, managed Apache Airflow, orchestrates the pipeline.

The DAG runs in this order:

1. Bronze Glue jobs
2. Silver Glue jobs
3. Gold Glue jobs
4. Pipeline completion validation

The DAG allows manual execution and can be updated for scheduled daily runs.

## 9. Security and Encryption

Security controls include:

- IAM roles for Glue and MWAA
- IAM user policy for Streamlit Athena access
- S3 bucket encryption
- Private subnet networking for MWAA
- Security group controls for Glue and SQL Server connectivity
- CloudWatch logging for Glue and MWAA

## 10. Failure Handling and Reload Strategy

The pipeline supports failure recovery through:

- Airflow task failure detection
- Glue job failure detection
- Airflow manual reruns
- Overwrite-based reloads in S3

Each Glue job writes to a defined S3 path using overwrite mode. If a job fails, the failed task can be rerun and the target layer is reloaded cleanly.

## 11. Validation Strategy

Validation includes:

- Checking Glue job success status
- Verifying data exists in Bronze, Silver, and Gold S3 folders
- Registering Gold Delta tables in Athena
- Running Athena queries against Gold outputs
- Validating row counts and business metrics
- Testing Streamlit dashboard tabs

## 12. Dashboard Design

The Streamlit dashboard includes:

1. Customer Segmentation Dashboard
2. Churn Risk Indicators Dashboard
3. Sales Trends and Seasonality Dashboard
4. Loyalty Program Impact Dashboard
5. Location Performance Dashboard
6. Pricing and Discount Effectiveness Dashboard

The dashboard queries Athena tables and visualizes Gold metrics interactively.

## 13. Why This Tech Stack Was Used

AWS RDS SQL Server was used to represent the source operational database.

AWS Glue was used because it is serverless, supports PySpark, and integrates with JDBC and S3.

Amazon S3 was used as the data lake because it is scalable, durable, and cost-effective.

Delta Lake was used for Gold tables because it provides transaction log support and reliable curated analytical storage.

Amazon MWAA was used because it provides managed Airflow orchestration for scheduling, retries, and dependency management.

Amazon Athena was used to query S3 data directly using SQL without requiring an external data warehouse.

Streamlit was used because it enables fast Python-based dashboard development.

GitHub Actions was used for CI/CD because it validates code changes automatically.
