import streamlit as st
import pandas as pd

from connection import run_query


st.set_page_config(
    page_title="Product & Restaurant Analysis",
    layout="wide"
)

st.title("Product & Restaurant Analysis")

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

# Revenue by Product Category
st.subheader("Revenue by Product Category with Revenue > $10,000")

if selected_year == "All":
    category_year_condition = ""
else:
    category_year_condition = f"AND YEAR(f.date_key) = {selected_year}"

query = f"""
SELECT
    i.item_category,
    SUM(i.item_gross_revenue) AS revenue
FROM globalpartner.fact_order_item i
JOIN globalpartner.fact_order f
    ON i.order_id = f.order_id
WHERE 1=1
{category_year_condition}
GROUP BY i.item_category
HAVING SUM(i.item_gross_revenue) > 10000
ORDER BY revenue DESC
"""

df_category_revenue = run_query(query)

df_category_revenue["revenue"] = df_category_revenue["revenue"].astype(float).round(2)

st.bar_chart(
    df_category_revenue.set_index("item_category")
)


# Revenue by Location

st.subheader("Revenue by Restaurant ID (Top 10)")

if selected_year == "All":
    restaurant_year_condition = ""
else:
    restaurant_year_condition = f"AND YEAR(date_key) = {selected_year}"

query = f"""
SELECT
    restaurant_id,
    SUM(net_revenue) AS revenue
FROM globalpartner.fact_order
WHERE 1=1
{restaurant_year_condition}
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

