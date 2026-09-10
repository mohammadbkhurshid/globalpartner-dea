import streamlit as st
import pandas as pd

from connection import run_query


st.set_page_config(
    page_title="GlobalPartner Business Analysis",
    page_icon="📊",
    layout="wide"
)

# Year Filter
query = """
SELECT DISTINCT year
FROM globalpartner.date_dim
ORDER BY year
"""

df_years = run_query(query)

years = df_years["year"].astype(int).tolist()

selected_year = st.sidebar.selectbox(
    "Select Year",
    ["All"] + years
)

if selected_year == "All":
    year_filter = ""
else:
    year_filter = f"WHERE YEAR(date_key) = {selected_year}"

st.title("GlobalPartner Business Analysis Dashboard")

st.header("Business Overview")

# KPI Queries

# Total Revenue
query = f"""
SELECT SUM(net_revenue) AS total_revenue
FROM globalpartner.fact_order
{year_filter}
"""
df = run_query(query)
total_revenue = float(df["total_revenue"].iloc[0])


# Total Orders
query = f"""
SELECT COUNT(*) AS total_orders
FROM globalpartner.fact_order
{year_filter}
"""
df = run_query(query)
total_orders = int(df["total_orders"].iloc[0])


# Total Customers
if selected_year == "All":
    customer_year_filter = ""
else:
    customer_year_filter = f"AND YEAR(date_key) = {selected_year}"
    
query = f"""
SELECT COUNT(DISTINCT customer_key) AS total_customers
FROM globalpartner.fact_customer_daily
WHERE customer_key <> 0
{customer_year_filter}
"""
df = run_query(query)
total_customers = int(df["total_customers"].iloc[0])

# Display KPIs
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Revenue", f"${total_revenue:,.2f}")

with col2:
    st.metric("Total Orders", f"{total_orders:,}")

with col3:
    st.metric("Total Customers", f"{total_customers:,}")


# Monthly Sales Trend
st.subheader("Monthly Sales Trend")

query = f"""
SELECT
    DATE_TRUNC('month', date_key) AS month,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
{year_filter}
GROUP BY DATE_TRUNC('month', date_key)
ORDER BY month
"""

df_sales = run_query(query)

df_sales["month"] = pd.to_datetime(df_sales["month"])
df_sales["revenue"] = pd.to_numeric(df_sales["revenue"])

st.line_chart(
    df_sales.set_index("month")["revenue"]
)


# Normal Sales Trend
st.subheader("Monthly Sales Trend — Excluding Exceptional Orders")

if selected_year == "All":
    normal_year_condition = ""
else:
    normal_year_condition = f"AND YEAR(date_key) = {selected_year}"

query = f"""
SELECT
    DATE_TRUNC('month', date_key) AS month,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
WHERE net_revenue < 40000
{normal_year_condition}
GROUP BY DATE_TRUNC('month', date_key)
ORDER BY month
"""

df_normal_sales = run_query(query)

df_normal_sales["month"] = pd.to_datetime(df_normal_sales["month"])
df_normal_sales["revenue"] = pd.to_numeric(df_normal_sales["revenue"])

st.line_chart(
    df_normal_sales.set_index("month")["revenue"]
)











