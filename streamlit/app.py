import streamlit as st
import pandas as pd

from connection import run_query


st.set_page_config(
    page_title="GlobalPartner Business Analysis",
    layout="wide"
)

# st.title("GlobalPartner Business Analysis")
st.title("GlobalPartner Business Analysis Dashboard")

st.header("Business Overview")

# KPI Queries

# Total Revenue
query = """
SELECT SUM(net_revenue) AS total_revenue
FROM globalpartner.fact_order
"""
df = run_query(query)
total_revenue = float(df["total_revenue"].iloc[0])


# Total Orders
query = """
SELECT COUNT(*) AS total_orders
FROM globalpartner.fact_order
"""
df = run_query(query)
total_orders = int(df["total_orders"].iloc[0])


# Total Customers
query = """
SELECT COUNT(DISTINCT customer_key) AS total_customers
FROM globalpartner.fact_customer_daily
WHERE customer_key <> 0
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

query = """
SELECT
    DATE_TRUNC('month', date_key) AS month,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
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

query = """
SELECT
    DATE_TRUNC('month', date_key) AS month,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
WHERE net_revenue < 400000
GROUP BY DATE_TRUNC('month', date_key)
ORDER BY month
"""

df_normal_sales = run_query(query)

df_normal_sales["month"] = pd.to_datetime(df_normal_sales["month"])
df_normal_sales["revenue"] = pd.to_numeric(df_normal_sales["revenue"])

st.line_chart(
    df_normal_sales.set_index("month")["revenue"]
)


# Revenue by Loyalty Status
st.header("Customer Insights")
st.subheader("Revenue by Loyalty Status")

query = """
SELECT
    CASE
        WHEN is_loyalty THEN 'Loyalty'
        ELSE 'Non-Loyalty'
    END AS loyalty_status,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
GROUP BY
    CASE
        WHEN is_loyalty THEN 'Loyalty'
        ELSE 'Non-Loyalty'
    END
ORDER BY revenue DESC
"""

df_loyalty = run_query(query)

df_loyalty["revenue"] = pd.to_numeric(df_loyalty["revenue"])

st.bar_chart(
    df_loyalty.set_index("loyalty_status")["revenue"]
)


# Customer Segmentation
st.subheader("RFM Based Customer Segmentation")

query = """
SELECT
    rfm_segment,
    COUNT(DISTINCT customer_key) AS customers
FROM globalpartner.fact_customer_daily
WHERE customer_key <> 0
GROUP BY rfm_segment
ORDER BY customers DESC
"""

df_segments = run_query(query)

df_segments["customers"] = pd.to_numeric(df_segments["customers"])

st.bar_chart(
    df_segments.set_index("rfm_segment")["customers"]
)


# Customer CLV Segmentation
st.subheader("Customer CLV Segmentation")

query = """
SELECT
    clv_value_group,
    COUNT(DISTINCT customer_key) AS customers
FROM globalpartner.fact_customer_daily
WHERE customer_key <> 0
GROUP BY clv_value_group
ORDER BY
    CASE clv_value_group
        WHEN 'High' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Low' THEN 3
    END
"""

df_clv = run_query(query)

df_clv["customers"] = pd.to_numeric(df_clv["customers"])

st.bar_chart(
    df_clv.set_index("clv_value_group")["customers"]
)


# Customer Churn Status
st.subheader("Customer Churn Status")

query = """
SELECT
    churn_status,
    COUNT(DISTINCT customer_key) AS customers
FROM globalpartner.fact_customer_daily
WHERE customer_key <> 0
GROUP BY churn_status
ORDER BY customers DESC
"""

df_churn = run_query(query)

df_churn["customers"] = pd.to_numeric(df_churn["customers"])

st.bar_chart(
    df_churn.set_index("churn_status")["customers"]
)


# Monthly Revenue by Year
st.subheader("Monthly Revenue by Year")

query = """
SELECT
    d.year,
    d.month AS month_number,
    SUM(f.net_revenue) AS revenue
FROM globalpartner.fact_order f
JOIN globalpartner.date_dim d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month
"""

df_yearly_monthly = run_query(query)

df_yearly_monthly["year"] = pd.to_numeric(
    df_yearly_monthly["year"]
)

df_yearly_monthly["month_number"] = pd.to_numeric(
    df_yearly_monthly["month_number"]
)

df_yearly_monthly["revenue"] = pd.to_numeric(
    df_yearly_monthly["revenue"]
)

df_yearly_monthly["year"] = df_yearly_monthly["year"].astype(str)

df_pivot = df_yearly_monthly.pivot(
    index="month_number",
    columns="year",
    values="revenue"
)
# df_pivot.index.name = "Month"
# st.line_chart(df_pivot)


month_names = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec"
}

month_order = list(month_names.values())

df_pivot.index = df_pivot.index.map(month_names)

df_pivot.index = pd.CategoricalIndex(
    df_pivot.index,
    categories=month_order,
    ordered=True
)

df_pivot = df_pivot.sort_index()

df_pivot.index.name = "Month"

st.line_chart(df_pivot)


# Monthly Revenue by Year
st.subheader("Seasonal Sales Pattern - Excluding Exceptional Orders")

query = """
SELECT
    d.year,
    d.month AS month_number,
    SUM(f.net_revenue) AS revenue
FROM globalpartner.fact_order f
JOIN globalpartner.date_dim d
    ON f.date_key = d.date_key
WHERE net_revenue < 400000
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month
"""

df_yearly_monthly = run_query(query)

df_yearly_monthly["year"] = pd.to_numeric(
    df_yearly_monthly["year"]
)

df_yearly_monthly["month_number"] = pd.to_numeric(
    df_yearly_monthly["month_number"]
)

df_yearly_monthly["revenue"] = pd.to_numeric(
    df_yearly_monthly["revenue"]
)

df_yearly_monthly["year"] = df_yearly_monthly["year"].astype(str)

df_pivot = df_yearly_monthly.pivot(
    index="month_number",
    columns="year",
    values="revenue"
)
# df_pivot.index.name = "Month"
# st.line_chart(df_pivot)


month_names = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec"
}

month_order = list(month_names.values())

df_pivot.index = df_pivot.index.map(month_names)

df_pivot.index = pd.CategoricalIndex(
    df_pivot.index,
    categories=month_order,
    ordered=True
)

df_pivot = df_pivot.sort_index()

df_pivot.index.name = "Month"

st.line_chart(df_pivot)


# Revenue by Product Category
st.subheader("Revenue by Product Category with Revenue > $10,000")

query = """
SELECT
    item_category,
    SUM(item_gross_revenue) AS revenue
FROM globalpartner.fact_order_item
GROUP BY item_category
HAVING SUM(item_gross_revenue) > 10000
ORDER BY revenue DESC
"""

df_category_revenue = run_query(query)

df_category_revenue["revenue"] = df_category_revenue["revenue"].astype(float).round(2)

st.bar_chart(
    df_category_revenue.set_index("item_category")
)

# st.line_chart(df_category_revenue.set_index("item_category")["revenue"])


# # Check Revenue by Location
# query = """
# SELECT
#     restaurant_id,
#     COUNT(*) AS orders,
#     SUM(net_revenue) AS revenue
# FROM globalpartner.fact_order
# GROUP BY restaurant_id
# ORDER BY revenue DESC
# """

# df_location = run_query(query)

# st.write(df_location)

# Revenue by Location
# st.subheader("Revenue by Location (Top 10 Restaurants)")
st.subheader("Revenue by Restaurant ID (Top 10)")

query = """
SELECT
    restaurant_id,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
GROUP BY restaurant_id
ORDER BY revenue DESC
LIMIT 10
"""

df_location = run_query(query)

df_location["revenue"] = (
    df_location["revenue"]
    .astype(float)
    .round(2)
)

st.bar_chart(
    df_location.set_index("restaurant_id")
)











