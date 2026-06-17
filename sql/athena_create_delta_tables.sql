CREATE DATABASE IF NOT EXISTS business_insights_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS business_insights_gold.customer_ltv_daily
LOCATION 's3://business-insights-de/gold/customer_ltv_daily/'
TBLPROPERTIES (
  'table_type'='DELTA'
);

CREATE EXTERNAL TABLE IF NOT EXISTS business_insights_gold.customer_segments
LOCATION 's3://business-insights-de/gold/customer_segments/'
TBLPROPERTIES (
  'table_type'='DELTA'
);

CREATE EXTERNAL TABLE IF NOT EXISTS business_insights_gold.sales_trends
LOCATION 's3://business-insights-de/gold/sales_trends/'
TBLPROPERTIES (
  'table_type'='DELTA'
);

CREATE EXTERNAL TABLE IF NOT EXISTS business_insights_gold.loyalty_location_discount
LOCATION 's3://business-insights-de/gold/loyalty_location_discount/'
TBLPROPERTIES (
  'table_type'='DELTA'
);
