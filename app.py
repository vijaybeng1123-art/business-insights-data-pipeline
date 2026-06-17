import time
from io import StringIO

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st


AWS_REGION = "us-east-1"
ATHENA_DATABASE = "business_insights_gold"
ATHENA_OUTPUT = "s3://business-insights-de/athena-results/"


athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


st.set_page_config(
    page_title="Business Insights Dashboard",
    page_icon="📊",
    layout="wide"
)


@st.cache_data(ttl=300)
def run_athena_query(query: str) -> pd.DataFrame:
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    query_execution_id = response["QueryExecutionId"]

    while True:
        status_response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = status_response["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(2)

    if status != "SUCCEEDED":
        reason = status_response["QueryExecution"]["Status"].get(
            "StateChangeReason", "Unknown error"
        )
        raise Exception(f"Athena query failed: {reason}")

    output_location = status_response["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

    bucket = output_location.replace("s3://", "").split("/")[0]
    key = "/".join(output_location.replace("s3://", "").split("/")[1:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj["Body"].read().decode("utf-8")

    return pd.read_csv(StringIO(csv_data))


st.title("Business Insights Dashboard")
st.caption("SQL Server → AWS Glue → S3 Bronze/Silver/Gold → Delta Lake → Athena → Streamlit")


dashboard = st.sidebar.radio(
    "Select Dashboard",
    [
        "Customer Segmentation",
        "Churn Risk Indicators",
        "Sales Trends and Seasonality",
        "Loyalty Program Impact",
        "Location Performance",
        "Pricing and Discount Effectiveness",
    ],
)


# ------------------------------------------------------------
# Customer Segmentation Dashboard
# ------------------------------------------------------------
if dashboard == "Customer Segmentation":
    st.header("Customer Segmentation Dashboard")
    st.write("Groups customers by purchase behavior: spend, frequency, recency, and CLV group.")

    df = run_athena_query("""
        SELECT
            cs.user_id,
            cs.recency_days,
            cs.frequency_orders,
            cs.monetary_total_spend,
            cs.rfm_segment,
            cs.inactivity_tag,
            ltv.clv_group,
            ltv.customer_ltv_to_date
        FROM customer_segments cs
        LEFT JOIN (
            SELECT user_id, clv_group, MAX(customer_ltv_to_date) AS customer_ltv_to_date
            FROM customer_ltv_daily
            GROUP BY user_id, clv_group
        ) ltv
        ON cs.user_id = ltv.user_id
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Customers", len(df))
    col2.metric("Average Spend", round(df["monetary_total_spend"].mean(), 2))
    col3.metric("Average Frequency", round(df["frequency_orders"].mean(), 2))

    seg_counts = df.groupby("rfm_segment")["user_id"].count().reset_index()
    seg_counts.columns = ["Segment", "Customer Count"]

    fig = px.bar(
        seg_counts,
        x="Segment",
        y="Customer Count",
        title="Customer Count by RFM Segment"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.scatter(
        df,
        x="frequency_orders",
        y="monetary_total_spend",
        color="rfm_segment",
        size="customer_ltv_to_date",
        hover_data=["user_id", "recency_days", "clv_group"],
        title="Customer Segmentation by Frequency and Spend"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------
# Churn Risk Indicators Dashboard
# ------------------------------------------------------------
elif dashboard == "Churn Risk Indicators":
    st.header("Churn Risk Indicators Dashboard")
    st.write("Highlights customers with inactivity risk based on days since last order, order gaps, and spend trends.")

    df = run_athena_query("""
        SELECT
            user_id,
            recency_days,
            frequency_orders,
            monetary_total_spend,
            average_gap_between_orders_days,
            last_30_day_spend,
            previous_30_day_spend,
            spend_change_pct,
            rfm_segment,
            inactivity_tag
        FROM customer_segments
    """)

    at_risk_count = df[df["inactivity_tag"].isin(["at_risk", "inactive"])]["user_id"].count()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Customers", len(df))
    col2.metric("At Risk / Inactive Customers", int(at_risk_count))
    col3.metric("Average Recency Days", round(df["recency_days"].mean(), 2))

    risk_counts = df.groupby("inactivity_tag")["user_id"].count().reset_index()
    risk_counts.columns = ["Inactivity Tag", "Customer Count"]

    fig = px.pie(
        risk_counts,
        names="Inactivity Tag",
        values="Customer Count",
        title="Customer Risk Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.scatter(
        df,
        x="recency_days",
        y="average_gap_between_orders_days",
        color="inactivity_tag",
        size="monetary_total_spend",
        hover_data=["user_id", "spend_change_pct"],
        title="Churn Risk Indicators"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("At-Risk Customers")
    st.dataframe(
        df[df["inactivity_tag"].isin(["at_risk", "inactive"])],
        use_container_width=True
    )


# ------------------------------------------------------------
# Sales Trends and Seasonality Dashboard
# ------------------------------------------------------------
elif dashboard == "Sales Trends and Seasonality":
    st.header("Sales Trends and Seasonality Dashboard")
    st.write("Tracks daily, weekly, and monthly revenue by location and product category.")

    period_filter = st.selectbox("Select Period Type", ["daily", "weekly", "monthly"])

    df = run_athena_query(f"""
        SELECT
            period_type,
            period_value,
            restaurant_id,
            item_category,
            currency,
            total_revenue,
            order_count,
            customer_count
        FROM sales_trends
        WHERE period_type = '{period_filter}'
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", round(df["total_revenue"].sum(), 2))
    col2.metric("Total Orders", int(df["order_count"].sum()))
    col3.metric("Total Customers", int(df["customer_count"].sum()))

    trend_df = df.groupby("period_value")["total_revenue"].sum().reset_index()

    fig = px.line(
        trend_df,
        x="period_value",
        y="total_revenue",
        title=f"{period_filter.title()} Revenue Trend"
    )
    st.plotly_chart(fig, use_container_width=True)

    category_df = df.groupby("item_category")["total_revenue"].sum().reset_index()

    fig2 = px.bar(
        category_df,
        x="item_category",
        y="total_revenue",
        title="Revenue by Menu Category"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------
# Loyalty Program Impact Dashboard
# ------------------------------------------------------------
elif dashboard == "Loyalty Program Impact":
    st.header("Loyalty Program Impact Dashboard")
    st.write("Compares loyalty and non-loyalty customers by revenue, order volume, and average order value.")

    df = run_athena_query("""
        SELECT
            metric_dimension AS loyalty_status,
            currency,
            total_revenue,
            total_orders,
            total_customers,
            average_order_value
        FROM loyalty_location_discount
        WHERE metric_type = 'loyalty_program_impact'
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", round(df["total_revenue"].sum(), 2))
    col2.metric("Total Orders", int(df["total_orders"].sum()))
    col3.metric("Average Order Value", round(df["average_order_value"].mean(), 2))

    fig = px.bar(
        df,
        x="loyalty_status",
        y="total_revenue",
        title="Revenue by Loyalty Status"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.bar(
        df,
        x="loyalty_status",
        y="average_order_value",
        title="Average Order Value by Loyalty Status"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------
# Location Performance Dashboard
# ------------------------------------------------------------
elif dashboard == "Location Performance":
    st.header("Location Performance Dashboard")
    st.write("Ranks restaurant locations by revenue, orders, customers, and average order value.")

    df = run_athena_query("""
        SELECT
            metric_dimension AS restaurant_id,
            currency,
            total_revenue,
            total_orders,
            total_customers,
            average_order_value
        FROM loyalty_location_discount
        WHERE metric_type = 'location_performance'
    """)

    df = df.sort_values(by="total_revenue", ascending=False)

    col1, col2, col3 = st.columns(3)
    col1.metric("Top Location", df.iloc[0]["restaurant_id"] if len(df) > 0 else "N/A")
    col2.metric("Highest Revenue", round(df["total_revenue"].max(), 2))
    col3.metric("Average Order Value", round(df["average_order_value"].mean(), 2))

    fig = px.bar(
        df,
        x="restaurant_id",
        y="total_revenue",
        title="Location Revenue Ranking"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.scatter(
        df,
        x="total_orders",
        y="average_order_value",
        size="total_revenue",
        hover_data=["restaurant_id"],
        title="Orders vs Average Order Value by Location"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------
# Pricing and Discount Effectiveness Dashboard
# ------------------------------------------------------------
elif dashboard == "Pricing and Discount Effectiveness":
    st.header("Pricing and Discount Effectiveness Dashboard")
    st.write("Compares discounted and non-discounted order performance.")

    df = run_athena_query("""
        SELECT
            metric_dimension AS discount_status,
            currency,
            total_revenue,
            total_orders,
            total_customers,
            average_order_value
        FROM loyalty_location_discount
        WHERE metric_type = 'discount_effectiveness'
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", round(df["total_revenue"].sum(), 2))
    col2.metric("Total Orders", int(df["total_orders"].sum()))
    col3.metric("Average Order Value", round(df["average_order_value"].mean(), 2))

    fig = px.bar(
        df,
        x="discount_status",
        y="total_revenue",
        title="Revenue: Discounted vs Non-Discounted Orders"
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.bar(
        df,
        x="discount_status",
        y="average_order_value",
        title="Average Order Value: Discounted vs Non-Discounted"
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True)