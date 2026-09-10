import streamlit as st
import pandas as pd

from connection import run_query


st.set_page_config(
    page_title="GlobalPartner Business Analysis",
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

st.title("Customer Analysis")


# Customer Segmentation
st.subheader("RFM Based Customer Segmentation")

if selected_year == "All":
    rfm_year_condition = ""
else:
    rfm_year_condition = f"AND YEAR(date_key) = {selected_year}"

query = f"""
WITH latest_customer AS (
    SELECT
        customer_key,
        rfm_segment,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY date_key DESC
        ) AS rn
    FROM globalpartner.fact_customer_daily
    WHERE customer_key <> 0
    {rfm_year_condition}
)

SELECT
    rfm_segment,
    COUNT(*) AS customers
FROM latest_customer
WHERE rn = 1
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

if selected_year == "All":
    clv_year_condition = ""
else:
    clv_year_condition = f"AND YEAR(date_key) = {selected_year}"

query = f"""
WITH latest_customer AS (
    SELECT
        customer_key,
        clv_value_group,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY date_key DESC
        ) AS rn
    FROM globalpartner.fact_customer_daily
    WHERE customer_key <> 0
    {clv_year_condition}
)

SELECT
    clv_value_group,
    COUNT(*) AS customers
FROM latest_customer
WHERE rn = 1
GROUP BY clv_value_group
ORDER BY customers DESC
"""

df_clv = run_query(query)

df_clv["customers"] = pd.to_numeric(df_clv["customers"])

st.bar_chart(
    df_clv.set_index("clv_value_group")["customers"]
)


# Customer Churn Status
st.subheader("Customer Churn Status")

if selected_year == "All":
    churn_year_condition = ""
else:
    churn_year_condition = f"AND YEAR(date_key) = {selected_year}"

query = f"""
WITH latest_customer AS (
    SELECT
        customer_key,
        churn_status,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY date_key DESC
        ) AS rn
    FROM globalpartner.fact_customer_daily
    WHERE customer_key <> 0
    {churn_year_condition}
)

SELECT
    churn_status,
    COUNT(*) AS customers
FROM latest_customer
WHERE rn = 1
GROUP BY churn_status
ORDER BY customers DESC
"""


df_churn = run_query(query)

df_churn["customers"] = pd.to_numeric(df_churn["customers"])

st.bar_chart(
    df_churn.set_index("churn_status")["customers"]
)
