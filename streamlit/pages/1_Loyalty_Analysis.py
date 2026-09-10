import streamlit as st
import pandas as pd

from connection import run_query


st.set_page_config(
    page_title="Loyalty Customer Analysis",
    layout="wide"
)



# Loyalty Customer Comparison
st.subheader("Loyalty vs Non-Loyalty Customer Comparison")

query = """
WITH customer_metrics AS (
    SELECT
        customer_key,
        is_loyalty,
        COUNT(DISTINCT order_id) - 1 AS repeat_orders,
        SUM(net_revenue) AS customer_revenue,
        AVG(net_revenue) AS avg_order_spend
    FROM globalpartner.fact_order
    WHERE customer_key <> 0
    GROUP BY customer_key, is_loyalty
)

SELECT
    CASE
        WHEN is_loyalty THEN 'Loyalty'
        ELSE 'Non-Loyalty'
    END AS loyalty_status,
    ROUND(AVG(avg_order_spend), 2) AS avg_spend,
    ROUND(AVG(repeat_orders), 2) AS repeat_orders,
    ROUND(AVG(customer_revenue), 2) AS lifetime_value
FROM customer_metrics
GROUP BY is_loyalty
ORDER BY is_loyalty DESC
"""

df_loyalty = run_query(query)

df_loyalty["avg_spend"] = df_loyalty["avg_spend"].astype(float).round(2)
df_loyalty["repeat_orders"] = df_loyalty["repeat_orders"].astype(float).round(2)
df_loyalty["lifetime_value"] = df_loyalty["lifetime_value"].astype(float).round(2)

col1, col2, col3 = st.columns(3)

with col1:
    st.write("Average Spend")
    st.bar_chart(
        df_loyalty.set_index("loyalty_status")[["avg_spend"]]
    )

with col2:
    st.write("Repeat Orders")
    st.bar_chart(
        df_loyalty.set_index("loyalty_status")[["repeat_orders"]]
    )

with col3:
    st.write("Lifetime Value")
    st.bar_chart(
        df_loyalty.set_index("loyalty_status")[["lifetime_value"]]
    )






# Pricing and Discount Effectiveness
st.subheader("Discounted vs Full-Price Transactions")

# query = """
# SELECT
#     CASE
#         WHEN COALESCE(order_discount_amount, 0) < 0
#             THEN 'Discounted'
#         ELSE 'Full Price'
#     END AS pricing_type,
#     COUNT(DISTINCT order_id) AS orders,
#     ROUND(SUM(order_gross_revenue), 2) AS gross_revenue,
#     ROUND(SUM(ABS(order_discount_amount)), 2) AS discount_amount,
#     ROUND(SUM(net_revenue), 2) AS net_revenue,
#     ROUND(AVG(net_revenue), 2) AS avg_order_value
# FROM globalpartner.fact_order
# GROUP BY
#     CASE
#         WHEN COALESCE(order_discount_amount, 0) < 0
#             THEN 'Discounted'
#         ELSE 'Full Price'
#     END
# ORDER BY pricing_type
# """

# df_discount = run_query(query)

# st.dataframe(df_discount)


# # Check discount data
# query = """
# SELECT
#     COUNT(*) AS line_items,
#     COUNT(CASE WHEN item_discount_amount < 0 THEN 1 END) AS discounted_items,
#     ROUND(SUM(item_discount_amount), 2) AS total_discount
# FROM globalpartner.fact_order_item
# """

# df_discount_check = run_query(query)

# st.dataframe(df_discount_check)

query = """
SELECT
    COUNT(*) AS option_records,
    COUNT(CASE WHEN option_price < 0 THEN 1 END) AS negative_price_options,
    ROUND(SUM(CASE WHEN option_price < 0 THEN option_price ELSE 0 END), 2) AS negative_option_amount
FROM globalpartner.fact_order_item
"""

df = run_query(query)

st.dataframe(df)