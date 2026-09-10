import streamlit as st
import pandas as pd

from connection import run_query

st.set_page_config(
    page_title="Sales Analysis",
    layout="wide"
)

st.title("Sales Analysis")

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

