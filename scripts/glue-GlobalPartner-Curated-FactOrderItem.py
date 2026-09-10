import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *


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
# 2. Build fact_order_item
# ---------------------------------------------------------

def build_fact_order_item():

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

        print("========== OPTIONS SCHEMA ==========")
        df_options.printSchema()


        # -------------------------------------------------
        # Aggregate options by lineitem_id
        # -------------------------------------------------

        options_summary = (
            df_options
            .groupBy("lineitem_id")
            .agg(
                sum("option_price").alias(
                    "item_option_amount"
                ),

                sum(
                    when(
                        col("option_price") < 0,
                        col("option_price")
                    ).otherwise(lit(0))
                ).alias(
                    "item_discount_amount"
                )
            )
        )


        # -------------------------------------------------
        # Join options to order items
        # -------------------------------------------------

        df_fact = (
            df_items
            .join(
                options_summary,
                on="lineitem_id",
                how="left"
            )
        )


        # -------------------------------------------------
        # Fill missing option amounts with zero
        # -------------------------------------------------

        df_fact = (
            df_fact
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
        )


        # -------------------------------------------------
        # Calculate gross revenue
        # -------------------------------------------------

        df_fact = df_fact.withColumn(
            "item_gross_revenue",
            (
                col("item_price")
                * col("item_quantity")
            )
            + col("item_option_amount")
        )


        # -------------------------------------------------
        # Remove invalid line item
        #
        # One source record has NULL lineitem_id.
        # -------------------------------------------------

        df_fact = df_fact.filter(
            col("lineitem_id").isNotNull()
        )


        # -------------------------------------------------
        # Select final columns
        # -------------------------------------------------

        df_fact_order_item = df_fact.select(
            "lineitem_id",
            "order_id",
            "item_category",
            "item_name",
            "item_price",
            "item_quantity",
            "item_option_amount",
            "item_discount_amount",
            "item_gross_revenue"
        )


        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        print("========== FINAL SCHEMA ==========")
        df_fact_order_item.printSchema()


        print("========== ROW COUNT ==========")

        row_count = df_fact_order_item.count()

        print(f"Rows: {row_count}")


        print("========== DUPLICATE LINEITEM CHECK ==========")

        duplicate_lineitems = (
            df_fact_order_item
            .groupBy("lineitem_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(
            f"Duplicate lineitem_id: "
            f"{duplicate_lineitems}"
        )


        print("========== NULL CHECK ==========")

        df_fact_order_item.select(
            [
                sum(
                    when(col(c).isNull(), 1)
                    .otherwise(0)
                ).alias(c)
                for c in df_fact_order_item.columns
            ]
        ).show()


        print("========== NEGATIVE QUANTITY CHECK ==========")

        negative_quantity = (
            df_fact_order_item
            .filter(col("item_quantity") < 0)
            .count()
        )

        print(
            f"Negative quantities: "
            f"{negative_quantity}"
        )


        print("========== ZERO QUANTITY CHECK ==========")

        zero_quantity = (
            df_fact_order_item
            .filter(col("item_quantity") == 0)
            .count()
        )

        print(
            f"Zero quantities: "
            f"{zero_quantity}"
        )


        # -------------------------------------------------
        # Write curated fact_order_item
        # -------------------------------------------------

        curated_path = (
            "s3://globalpartner-data-dea/"
            "curated/fact_order_item/"
        )

        (
            df_fact_order_item
            .write
            .mode("overwrite")
            .parquet(curated_path)
        )

        print(
            f"Successfully wrote fact_order_item "
            f"to {curated_path}"
        )


    except Exception as e:

        print(
            f"ERROR processing fact_order_item: "
            f"{str(e)}"
        )

        raise


# ---------------------------------------------------------
# 3. Run job
# ---------------------------------------------------------

build_fact_order_item()

job.commit()