import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


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
# 2. Build dim_customer
# ---------------------------------------------------------

def build_dim_customer():

    try:

        # -------------------------------------------------
        # Read Refined order_items
        # -------------------------------------------------

        refined_path = "s3://globalpartner-data-dea/refined/order_items/"

        df = spark.read.parquet(refined_path)

        print("========== REFINED SCHEMA ==========")
        df.printSchema()


        # -------------------------------------------------
        # Standardize column names
        # -------------------------------------------------

        df = df.toDF(*[c.lower() for c in df.columns])


        # -------------------------------------------------
        # Create safe card-to-user mapping
        #
        # Only use cards associated with exactly ONE
        # known USER_ID.
        # -------------------------------------------------

        card_user_counts = (
            df.filter(
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
            df.filter(
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
        # Resolve USER_ID using card number when USER_ID
        # is missing.
        #
        # Original USER_ID is never overwritten.
        # -------------------------------------------------

        df_resolved = (
            df.join(
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
                .otherwise(col("card_resolved_user_id"))
            )
        )


        # -------------------------------------------------
        # Create identified customers
        # -------------------------------------------------

        identified_customers = (
            df_resolved
            .filter(col("resolved_user_id").isNotNull())
            .select(
                col("resolved_user_id").alias("user_id")
            )
            .dropDuplicates()
        )


        # -------------------------------------------------
        # Generate deterministic surrogate customer_key
        #
        # UNKNOWN customer = 0
        # Identified customers = 1, 2, 3, ...
        # -------------------------------------------------

        window_spec = Window.orderBy("user_id")

        identified_customers = (
            identified_customers
            .withColumn(
                "customer_key",
                row_number().over(window_spec)
            )
            .withColumn(
                "customer_status",
                lit("IDENTIFIED")
            )
        )


        # -------------------------------------------------
        # Create UNKNOWN customer
        # -------------------------------------------------

        # unknown_customer = spark.createDataFrame(
        #     [
        #         (0, None, "UNKNOWN")
        #     ],
        #     [
        #         "customer_key",
        #         "user_id",
        #         "customer_status"
        #     ]
        # )

        unknown_schema = StructType([
            StructField("customer_key", IntegerType(), False),
            StructField("user_id", StringType(), True),
            StructField("customer_status", StringType(), False)
        ])
        
        unknown_customer = spark.createDataFrame(
            [
                (0, None, "UNKNOWN")
            ],
            schema=unknown_schema
        )
        # -------------------------------------------------
        # Combine identified + unknown
        # -------------------------------------------------

        df_dim_customer = (
            identified_customers
            .select(
                "customer_key",
                "user_id",
                "customer_status"
            )
            .unionByName(unknown_customer)
        )


        # -------------------------------------------------
        # Final schema
        # -------------------------------------------------

        print("========== DIM_CUSTOMER SCHEMA ==========")
        df_dim_customer.printSchema()


        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        print("========== ROW COUNT ==========")
        print(df_dim_customer.count())

        print("========== CUSTOMER KEY CHECK ==========")

        duplicate_keys = (
            df_dim_customer
            .groupBy("customer_key")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(f"Duplicate customer keys: {duplicate_keys}")


        print("========== CUSTOMER STATUS ==========")

        (
            df_dim_customer
            .groupBy("customer_status")
            .count()
            .show()
        )


        # -------------------------------------------------
        # Write Curated dim_customer
        # -------------------------------------------------

        curated_path = (
            "s3://globalpartner-data-dea/"
            "curated/dim_customer/"
        )

        (
            df_dim_customer
            .write
            .mode("overwrite")
            .parquet(curated_path)
        )

        print(
            f"Successfully wrote dim_customer to "
            f"{curated_path}"
        )


    except Exception as e:

        print(
            f"ERROR processing dim_customer: {str(e)}"
        )

        raise


# ---------------------------------------------------------
# 3. Run job
# ---------------------------------------------------------

build_dim_customer()

job.commit()