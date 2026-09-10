from connection import run_query


query = """
SELECT COUNT(*) AS total_orders
FROM globalpartner.fact_order
"""


df = run_query(query)

print(df)