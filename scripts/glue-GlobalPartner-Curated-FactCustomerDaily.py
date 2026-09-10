import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StringType
)


# ============================================================
# Glue setup
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# S3 paths
# ============================================================

FACT_ORDER_PATH = "s3://globalpartner-data-dea/curated/fact_order/"
DIM_CUSTOMER_PATH = "s3://globalpartner-data-dea/curated/dim_customer/"
OUTPUT_PATH = "s3://globalpartner-data-dea/curated/fact_customer_daily/"


# ============================================================
# Function 1: Build daily customer metrics
# ============================================================

def build_daily_metrics(df_fact_order):

    print("Building daily customer metrics...")

    # One row per customer per date
    df_daily = (
        df_fact_order
        .groupBy(
            "date_key",
            "customer_key"
        )
        .agg(
            F.sum("net_revenue").alias("daily_revenue"),
            F.countDistinct("order_id").alias("daily_orders")
        )
    )

    # Window ordered by date for each customer
    customer_date_window = (
        Window
        .partitionBy("customer_key")
        .orderBy("date_key")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    # Cumulative revenue
    df_daily = df_daily.withColumn(
        "cumulative_revenue",
        F.sum("daily_revenue").over(customer_date_window)
    )

    # Cumulative orders
    df_daily = df_daily.withColumn(
        "cumulative_orders",
        F.sum("daily_orders").over(customer_date_window)
    )

    return df_daily


# ============================================================
# Function 2: Build customer-level metrics
# ============================================================

def build_customer_metrics(df_daily):

    print("Building customer-level metrics...")

    # --------------------------------------------------------
    # Latest transaction date
    # --------------------------------------------------------

    max_date = df_daily.select(
        F.max("date_key").alias("max_date")
    ).collect()[0]["max_date"]

    print(f"Maximum transaction date: {max_date}")


    # --------------------------------------------------------
    # Recency
    # --------------------------------------------------------

    df_customer = df_daily.withColumn(
        "recency_days",
        F.datediff(
            F.lit(max_date),
            F.col("date_key")
        )
    )


    # --------------------------------------------------------
    # 365-day rolling Frequency and Monetary
    # --------------------------------------------------------
    
    df_customer = df_customer.withColumn(
        "date_days",
        F.datediff(
            F.col("date_key"),
            F.lit("1970-01-01")
        )
    )
    
    customer_window = (
        Window
        .partitionBy("customer_key")
        .orderBy("date_days")
        .rangeBetween(-365, 0)
    )
    
    df_customer = (
        df_customer
        .withColumn(
            "frequency",
            F.sum("daily_orders").over(customer_window)
        )
        .withColumn(
            "monetary",
            F.sum("daily_revenue").over(customer_window)
        )
        .drop("date_days")
    )    

    # --------------------------------------------------------
    # Get latest record for each customer
    # --------------------------------------------------------

    latest_window = (
        Window
        .partitionBy("customer_key")
        .orderBy(F.col("date_key").desc())
    )

    df_latest = (
        df_customer
        .withColumn(
            "row_num",
            F.row_number().over(latest_window)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


    # # --------------------------------------------------------
    # # Recency score
    # # --------------------------------------------------------

    # recency_window = Window.orderBy("recency_days")

    # df_latest = df_latest.withColumn(
    #     "recency_percentile",
    #     F.percent_rank().over(recency_window)
    # )

    # df_latest = df_latest.withColumn(
    #     "recency_score",
    #     F.when(F.col("recency_percentile") <= 0.20, 5)
    #      .when(F.col("recency_percentile") <= 0.40, 4)
    #      .when(F.col("recency_percentile") <= 0.60, 3)
    #      .when(F.col("recency_percentile") <= 0.80, 2)
    #      .otherwise(1)
    #      .cast(IntegerType())
    # )


    # # --------------------------------------------------------
    # # Frequency score
    # # --------------------------------------------------------

    # frequency_window = Window.orderBy("frequency")

    # df_latest = df_latest.withColumn(
    #     "frequency_percentile",
    #     F.percent_rank().over(frequency_window)
    # )

    # df_latest = df_latest.withColumn(
    #     "frequency_score",
    #     F.when(F.col("frequency_percentile") <= 0.20, 1)
    #      .when(F.col("frequency_percentile") <= 0.40, 2)
    #      .when(F.col("frequency_percentile") <= 0.60, 3)
    #      .when(F.col("frequency_percentile") <= 0.80, 4)
    #      .otherwise(5)
    #      .cast(IntegerType())
    # )


    # # --------------------------------------------------------
    # # Monetary score
    # # --------------------------------------------------------

    # monetary_window = Window.orderBy("monetary")

    # df_latest = df_latest.withColumn(
    #     "monetary_percentile",
    #     F.percent_rank().over(monetary_window)
    # )

    # df_latest = df_latest.withColumn(
    #     "monetary_score",
    #     F.when(F.col("monetary_percentile") <= 0.20, 1)
    #      .when(F.col("monetary_percentile") <= 0.40, 2)
    #      .when(F.col("monetary_percentile") <= 0.60, 3)
    #      .when(F.col("monetary_percentile") <= 0.80, 4)
    #      .otherwise(5)
    #      .cast(IntegerType())
    # )

    # --------------------------------------------------------
    # RFM scoring
    # --------------------------------------------------------
    #
    # We use dense/tie-preserving ranking logic similar to
    # the validated pandas implementation.
    #
    # Recency:
    # lower recency_days = better = higher score
    #
    # Frequency:
    # higher frequency = better
    #
    # Monetary:
    # higher monetary = better
    # --------------------------------------------------------

    customer_count = df_latest.select(
        F.count("*").alias("customer_count")
    ).collect()[0]["customer_count"]

    print(f"Customers used for RFM scoring: {customer_count}")


    # --------------------------------------------------------
    # Recency score
    # --------------------------------------------------------

    recency_rank_window = Window.orderBy(
        F.col("recency_days")
    )

    df_latest = df_latest.withColumn(
        "recency_rank",
        F.rank().over(recency_rank_window)
    )

    df_latest = df_latest.withColumn(
        "recency_percentile",
        F.col("recency_rank") / F.lit(customer_count)
    )

    df_latest = df_latest.withColumn(
        "recency_score",
        F.when(
            F.col("recency_percentile") <= 0.20,
            5
        )
        .when(
            F.col("recency_percentile") <= 0.40,
            4
        )
        .when(
            F.col("recency_percentile") <= 0.60,
            3
        )
        .when(
            F.col("recency_percentile") <= 0.80,
            2
        )
        .otherwise(1)
        .cast(IntegerType())
    )


    # --------------------------------------------------------
    # Frequency score
    # --------------------------------------------------------

    frequency_rank_window = Window.orderBy(
        F.col("frequency")
    )

    df_latest = df_latest.withColumn(
        "frequency_rank",
        F.rank().over(frequency_rank_window)
    )

    df_latest = df_latest.withColumn(
        "frequency_percentile",
        F.col("frequency_rank") / F.lit(customer_count)
    )

    df_latest = df_latest.withColumn(
        "frequency_score",
        F.when(
            F.col("frequency_percentile") <= 0.20,
            1
        )
        .when(
            F.col("frequency_percentile") <= 0.40,
            2
        )
        .when(
            F.col("frequency_percentile") <= 0.60,
            3
        )
        .when(
            F.col("frequency_percentile") <= 0.80,
            4
        )
        .otherwise(5)
        .cast(IntegerType())
    )


    # --------------------------------------------------------
    # Monetary score
    # --------------------------------------------------------

    monetary_rank_window = Window.orderBy(
        F.col("monetary")
    )

    df_latest = df_latest.withColumn(
        "monetary_rank",
        F.rank().over(monetary_rank_window)
    )

    df_latest = df_latest.withColumn(
        "monetary_percentile",
        F.col("monetary_rank") / F.lit(customer_count)
    )

    df_latest = df_latest.withColumn(
        "monetary_score",
        F.when(
            F.col("monetary_percentile") <= 0.20,
            1
        )
        .when(
            F.col("monetary_percentile") <= 0.40,
            2
        )
        .when(
            F.col("monetary_percentile") <= 0.60,
            3
        )
        .when(
            F.col("monetary_percentile") <= 0.80,
            4
        )
        .otherwise(5)
        .cast(IntegerType())
    )

    # --------------------------------------------------------
    # RFM score
    # --------------------------------------------------------

    df_latest = df_latest.withColumn(
        "rfm_score",
        F.concat(
            F.col("recency_score"),
            F.col("frequency_score"),
            F.col("monetary_score")
        )
    )


    # --------------------------------------------------------
    # RFM segment
    # --------------------------------------------------------

    df_latest = df_latest.withColumn(
        "rfm_segment",
        F.lit("Regular")
    )

    df_latest = df_latest.withColumn(
        "rfm_segment",
        F.when(
            (F.col("recency_score") >= 4) &
            (F.col("frequency_score") >= 4) &
            (F.col("monetary_score") >= 4),
            "High Value"
        )
        .when(
            (F.col("recency_score") <= 2) &
            (
                (F.col("frequency_score") >= 3) |
                (F.col("monetary_score") >= 3)
            ),
            "At Risk"
        )
        .when(
            (F.col("recency_score") <= 2) &
            (F.col("frequency_score") <= 2) &
            (F.col("monetary_score") <= 2),
            "Low Value"
        )
        .otherwise("Regular")
    )


    # --------------------------------------------------------
    # CLV value group
    # --------------------------------------------------------

    clv_quantiles = df_latest.approxQuantile(
        "monetary",
        [0.20, 0.80],
        0.0001
    )

    low_clv_boundary = clv_quantiles[0]
    high_clv_boundary = clv_quantiles[1]

    print(f"Low CLV boundary: {low_clv_boundary}")
    print(f"High CLV boundary: {high_clv_boundary}")

    df_latest = df_latest.withColumn(
        "clv_value_group",
        F.when(
            F.col("monetary") <= low_clv_boundary,
            "Low"
        )
        .when(
            F.col("monetary") >= high_clv_boundary,
            "High"
        )
        .otherwise("Medium")
    )


    # --------------------------------------------------------
    # Churn status
    # --------------------------------------------------------

    df_latest = df_latest.withColumn(
        "churn_status",
        F.when(
            F.col("recency_days") > 365,
            "Churned"
        )
        .otherwise("Active")
    )


    # --------------------------------------------------------
    # Customer attributes
    # --------------------------------------------------------

    customer_attributes = df_latest.select(
        "customer_key",
        "recency_days",
        "frequency",
        "monetary",
        "rfm_score",
        "rfm_segment",
        "clv_value_group",
        "churn_status"
    )


    # --------------------------------------------------------
    # Join customer-level attributes back to daily rows
    # --------------------------------------------------------

    df_final = df_daily.join(
        customer_attributes,
        on="customer_key",
        how="left"
    )


    # --------------------------------------------------------
    # Handle UNKNOWN customer
    # --------------------------------------------------------

    df_final = (
        df_final
        .withColumn(
            "recency_days",
            F.when(
                F.col("customer_key") == 0,
                F.lit(0)
            ).otherwise(F.col("recency_days"))
        )
        .withColumn(
            "frequency",
            F.when(
                F.col("customer_key") == 0,
                F.lit(0)
            ).otherwise(F.col("frequency"))
        )
        .withColumn(
            "monetary",
            F.when(
                F.col("customer_key") == 0,
                F.lit(0.0)
            ).otherwise(F.col("monetary"))
        )
        .withColumn(
            "rfm_score",
            F.when(
                F.col("customer_key") == 0,
                F.lit("Unknown")
            ).otherwise(F.col("rfm_score"))
        )
        .withColumn(
            "rfm_segment",
            F.when(
                F.col("customer_key") == 0,
                F.lit("Unknown")
            ).otherwise(F.col("rfm_segment"))
        )
        .withColumn(
            "clv_value_group",
            F.when(
                F.col("customer_key") == 0,
                F.lit("Unknown")
            ).otherwise(F.col("clv_value_group"))
        )
        .withColumn(
            "churn_status",
            F.when(
                F.col("customer_key") == 0,
                F.lit("Unknown")
            ).otherwise(F.col("churn_status"))
        )
    )

    return df_final


# ============================================================
# Main
# ============================================================

try:

    print("Reading fact_order...")

    df_fact_order = spark.read.parquet(FACT_ORDER_PATH)

    print(f"fact_order rows: {df_fact_order.count()}")


    # Function 1
    df_daily = build_daily_metrics(
        df_fact_order
    )

    print(f"Daily rows: {df_daily.count()}")


    # Function 2
    df_final = build_customer_metrics(
        df_daily
    )


    # --------------------------------------------------------
    # Final column order
    # --------------------------------------------------------

    df_final = df_final.select(
        "date_key",
        "customer_key",
        "daily_revenue",
        "daily_orders",
        "cumulative_revenue",
        "cumulative_orders",
        "recency_days",
        "frequency",
        "monetary",
        "rfm_score",
        "rfm_segment",
        "clv_value_group",
        "churn_status"
    )


    # --------------------------------------------------------
    # Write output
    # --------------------------------------------------------

    print("Writing fact_customer_daily...")

    (
        df_final
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print("fact_customer_daily successfully written.")


    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    print("Final row count:", df_final.count())

    print("Null counts:")
    df_final.select([
        F.sum(
            F.col(c).isNull().cast("int")
        ).alias(c)
        for c in df_final.columns
    ]).show()

    print("RFM segments:")
    df_final.groupBy(
        "rfm_segment"
    ).count().show()

    print("CLV groups:")
    df_final.groupBy(
        "clv_value_group"
    ).count().show()

    print("Churn status:")
    df_final.groupBy(
        "churn_status"
    ).count().show()


except Exception as e:

    print("ERROR:", str(e))
    raise


finally:

    job.commit()