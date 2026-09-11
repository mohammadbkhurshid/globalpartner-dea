import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.window import Window


# ---------------------------------------------------------
# 1. Initialize Glue / Spark
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. Build fact_order
# ---------------------------------------------------------

def build_fact_order():

    try:

        # -------------------------------------------------
        # Read refined order_items
        # -------------------------------------------------

        order_items_path = (
            "s3://globalpartner-data-dea/"
            "refined/order_items/"
        )

        df_items = spark.read.parquet(order_items_path)

        df_items = df_items.toDF(
            *[c.lower() for c in df_items.columns]
        )

        print("========== ORDER ITEMS SCHEMA ==========")
        df_items.printSchema()


        # -------------------------------------------------
        # Read refined order_item_options
        # -------------------------------------------------

        options_path = (
            "s3://globalpartner-data-dea/"
            "refined/order_item_options/"
        )

        df_options = spark.read.parquet(options_path)

        df_options = df_options.toDF(
            *[c.lower() for c in df_options.columns]
        )


        # -------------------------------------------------
        # Create safe card-to-user mapping
        #
        # Only cards associated with exactly one known
        # USER_ID are used.
        # -------------------------------------------------

        card_user_counts = (
            df_items
            .filter(
                col("user_id").isNotNull()
                & col("printed_card_number").isNotNull()
            )
            .groupBy("printed_card_number")
            .agg(
                countDistinct("user_id").alias("user_count")
            )
        )

        unique_cards = (
            card_user_counts
            .filter(col("user_count") == 1)
            .select("printed_card_number")
        )

        card_to_user = (
            df_items
            .filter(
                col("user_id").isNotNull()
                & col("printed_card_number").isNotNull()
            )
            .join(
                unique_cards,
                on="printed_card_number",
                how="inner"
            )
            .select(
                "printed_card_number",
                "user_id"
            )
            .dropDuplicates()
        )


        # -------------------------------------------------
        # Resolve USER_ID
        # -------------------------------------------------

        df_resolved = (
            df_items
            .join(
                card_to_user.withColumnRenamed(
                    "user_id",
                    "card_resolved_user_id"
                ),
                on="printed_card_number",
                how="left"
            )
            .withColumn(
                "resolved_user_id",
                when(
                    col("user_id").isNotNull(),
                    col("user_id")
                )
                .otherwise(
                    col("card_resolved_user_id")
                )
            )
        )


        # -------------------------------------------------
        # Create customer mapping
        #
        # Same deterministic mapping used by dim_customer.
        # UNKNOWN = customer_key 0
        # -------------------------------------------------

        identified_customers = (
            df_resolved
            .filter(
                col("resolved_user_id").isNotNull()
            )
            .select(
                col("resolved_user_id").alias("user_id")
            )
            .dropDuplicates()
        )

        window_spec = Window.orderBy("user_id")

        customer_mapping = (
            identified_customers
            .withColumn(
                "customer_key",
                row_number().over(window_spec)
            )
            .select(
                "user_id",
                "customer_key"
            )
        )


        # -------------------------------------------------
        # Join customer_key
        # -------------------------------------------------

        df_resolved = (
            df_resolved
            .join(
                customer_mapping.withColumnRenamed(
                    "user_id",
                    "resolved_user_id_for_join"
                ),
                df_resolved["resolved_user_id"]
                == col("resolved_user_id_for_join"),
                how="left"
            )
            .drop("resolved_user_id_for_join")
            .withColumn(
                "customer_key",
                coalesce(
                    col("customer_key"),
                    lit(0)
                )
            )
        )


        # -------------------------------------------------
        # Aggregate order-level attributes
        #
        # We validated locally that these attributes do not
        # conflict within an order.
        # -------------------------------------------------

        df_orders = (
            df_resolved
            .groupBy("order_id")
            .agg(
                first(
                    "restaurant_id",
                    ignorenulls=True
                ).alias("restaurant_id"),

                first(
                    "app_name",
                    ignorenulls=True
                ).alias("app_name"),

                first(
                    "printed_card_number",
                    ignorenulls=True
                ).alias("card_number"),

                first(
                    "currency",
                    ignorenulls=True
                ).alias("currency"),

                first(
                    "is_loyalty",
                    ignorenulls=True
                ).alias("is_loyalty"),

                first(
                    "date_key",
                    ignorenulls=True
                ).alias("date_key"),

                first(
                    "customer_key",
                    ignorenulls=True
                ).alias("customer_key")
            )
        )


        # -------------------------------------------------
        # Aggregate options by lineitem
        # -------------------------------------------------

        options_summary = (
            df_options
            .groupBy("lineitem_id")
            .agg(
                sum(
                    "option_price"
                ).alias(
                    "item_option_amount"
                ),

                sum(
                    when(
                        col("option_price") < 0,
                        col("option_price")
                    ).otherwise(lit(0.0))
                ).alias(
                    "item_discount_amount"
                )
            )
        )


        # -------------------------------------------------
        # Calculate line-item revenue
        # -------------------------------------------------

        df_line_items = (
            df_resolved
            .filter(
                col("lineitem_id").isNotNull()
            )
            .join(
                options_summary,
                on="lineitem_id",
                how="left"
            )
            .withColumn(
                "item_option_amount",
                coalesce(
                    col("item_option_amount"),
                    lit(0.0)
                )
            )
            .withColumn(
                "item_discount_amount",
                coalesce(
                    col("item_discount_amount"),
                    lit(0.0)
                )
            )
            .withColumn(
                "item_base_revenue",
                col("item_price")
                * col("item_quantity")
            )
            .withColumn(
                "item_gross_revenue",
                col("item_base_revenue")
                + col("item_option_amount")
            )
        )


        # -------------------------------------------------
        # Aggregate revenue to order
        # -------------------------------------------------

        df_order_revenue = (
            df_line_items
            .groupBy("order_id")
            .agg(
                sum(
                    "item_gross_revenue"
                ).alias(
                    "order_gross_revenue"
                ),

                sum(
                    "item_option_amount"
                ).alias(
                    "order_option_amount"
                ),

                sum(
                    "item_discount_amount"
                ).alias(
                    "order_discount_amount"
                )
            )
        )


        # -------------------------------------------------
        # Calculate net revenue
        # -------------------------------------------------

        df_order_revenue = (
            df_order_revenue
            .withColumn(
                "net_revenue",
                col("order_gross_revenue")
                + col("order_discount_amount")
            )
        )


        # -------------------------------------------------
        # Join order attributes + revenue
        # -------------------------------------------------

        df_fact_order = (
            df_orders
            .join(
                df_order_revenue,
                on="order_id",
                how="inner"
            )
        )


        # -------------------------------------------------
        # Final column selection
        # -------------------------------------------------

        df_fact_order = df_fact_order.select(
            "order_id",
            "customer_key",
            "restaurant_id",
            "date_key",
            "app_name",
            "card_number",
            "currency",
            "is_loyalty",
            "net_revenue",
            "order_gross_revenue",
            "order_option_amount",
            "order_discount_amount"
        )


        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        print("========== FACT ORDER SCHEMA ==========")
        df_fact_order.printSchema()

        print("========== ROW COUNT ==========")

        row_count = df_fact_order.count()

        print(f"Rows: {row_count}")


        print("========== DUPLICATE ORDER CHECK ==========")

        duplicate_orders = (
            df_fact_order
            .groupBy("order_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(
            f"Duplicate order_id: "
            f"{duplicate_orders}"
        )


        print("========== NULL CHECK ==========")

        df_fact_order.select(
            [
                sum(
                    when(col(c).isNull(), 1)
                    .otherwise(0)
                ).alias(c)
                for c in df_fact_order.columns
            ]
        ).show()


        print("========== REVENUE CHECK ==========")

        df_fact_order.select(
            min("net_revenue").alias("min_net_revenue"),
            max("net_revenue").alias("max_net_revenue"),
            avg("net_revenue").alias("avg_net_revenue")
        ).show()


        # -------------------------------------------------
        # Write curated fact_order
        # -------------------------------------------------

        curated_path = (
            "s3://globalpartner-data-dea/"
            "curated/fact_order/"
        )

        (
            df_fact_order
            .write
            .mode("overwrite")
            .parquet(curated_path)
        )

        print(
            f"Successfully wrote fact_order "
            f"to {curated_path}"
        )


    except Exception as e:

        print(
            f"ERROR processing fact_order: "
            f"{str(e)}"
        )

        raise


# ---------------------------------------------------------
# 3. Run job
# ---------------------------------------------------------

build_fact_order()

job.commit()