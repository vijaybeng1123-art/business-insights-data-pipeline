# Setup Configurations

## AWS Region

us-east-1

## S3 Bucket

business-insights-de

## S3 Folder Structure

- bronze/
- silver/
- gold/
- airflow/dags/
- athena-results/
- logs/

## MWAA Environment

Environment name:

business-insights-mwaa

DAG folder:

s3://business-insights-de/airflow/dags/

## Glue Connection

Connection name:

Jdbc connection

Source:

Amazon RDS SQL Server

SQL Server endpoint:

database-1.ckx0iaq64gpt.us-east-1.rds.amazonaws.com

## Glue IAM Role

GlueIngestionServiceRole

## Glue Jobs

### Bronze Jobs

- business-insights-bronze-date-dim
- business-insights-bronze-order-items
- business-insights-bronze-order-item-options

### Silver Jobs

- business-insights-silver-date-dim
- business-insights-silver-order-items
- business-insights-silver-order-item-options

### Gold Jobs

- business-insights-gold-customer-ltv-daily
- business-insights-gold-customer-segments
- business-insights-gold-sales-trends
- business-insights-gold-loyalty-location-discount

## Athena Database

business_insights_gold

## Athena Tables

- customer_ltv_daily
- customer_segments
- sales_trends
- loyalty_location_discount

## Streamlit

Local command:

```bash
conda activate streamlit-bi
python -m streamlit run app.py
