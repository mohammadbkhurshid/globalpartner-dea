import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.types import *
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
# 2. Build date_dim
# ---------------------------------------------------------

def build_date_dim():

    try:

        # -------------------------------------------------
        # Read refined date_dim
        # -------------------------------------------------

        refined_date_path = (
            "s3://globalpartner-data-dea/"
            "refined/date_dim/"
        )

        df_source = spark.read.parquet(refined_date_path)

        print("========== SOURCE DATE DIM SCHEMA ==========")
        df_source.printSchema()


        # -------------------------------------------------
        # Standardize column names
        # -------------------------------------------------

        df_source = df_source.toDF(
            *[c.lower() for c in df_source.columns]
        )


        # -------------------------------------------------
        # Determine transaction date range
        # from refined order_items
        # -------------------------------------------------

        refined_order_path = (
            "s3://globalpartner-data-dea/"
            "refined/order_items/"
        )

        df_orders = spark.read.parquet(refined_order_path)
        
        print("========== ORDER ITEMS DATE CHECK ==========")
        
        df_orders.select(
            min("date_key").alias("min_date"),
            max("date_key").alias("max_date")
        ).show()
        
        df_orders.orderBy(
            col("date_key").desc()
        ).select(
            "order_id",
            "creation_time_utc",
            "date_key"
        ).show(10, truncate=False)

        df_orders = df_orders.toDF(
            *[c.lower() for c in df_orders.columns]
        )

        transaction_range = (
            df_orders
            .select(
                min("date_key").alias("min_date"),
                max("date_key").alias("max_date")
            )
            .collect()[0]
        )

        min_date = transaction_range["min_date"]
        max_date = transaction_range["max_date"]

        print("========== TRANSACTION DATE RANGE ==========")
        print(f"Minimum date: {min_date}")
        print(f"Maximum date: {max_date}")


        # -------------------------------------------------
        # Generate complete date range
        # -------------------------------------------------

        df_dates = (
            spark.range(1)
            .select(
                explode(
                    sequence(
                        lit(min_date),
                        lit(max_date),
                        expr("interval 1 day")
                    )
                ).alias("date_key")
            )
        )


        # -------------------------------------------------
        # Add calendar attributes
        # -------------------------------------------------

        df_dates = (
            df_dates
            .withColumn("year", year("date_key"))
            .withColumn("month", month("date_key"))
            .withColumn(
                "week",
                weekofyear("date_key")
            )
            .withColumn(
                "day_of_week",
                date_format("date_key", "EEEE")
            )
            .withColumn(
                "is_weekend",
                dayofweek("date_key").isin(1, 7)
            )
        )


        # -------------------------------------------------
        # Preserve source holiday information
        # -------------------------------------------------

        source_holidays = (
            df_source
            .select(
                "date_key",
                "is_holiday",
                "holiday_name"
            )
        )

        df_dates = (
            df_dates
            .join(
                source_holidays,
                on="date_key",
                how="left"
            )
        )


        # -------------------------------------------------
        # Fill missing holiday values
        # -------------------------------------------------

        df_dates = (
            df_dates
            .withColumn(
                "is_holiday",
                coalesce(
                    col("is_holiday"),
                    lit(False)
                )
            )
            .withColumn(
                "holiday_name",
                col("holiday_name")
            )
        )


        # -------------------------------------------------
        # Final column order
        # -------------------------------------------------

        df_date_dim = df_dates.select(
            "date_key",
            "year",
            "month",
            "week",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "holiday_name"
        )


        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        print("========== DATE DIM SCHEMA ==========")
        df_date_dim.printSchema()

        print("========== ROW COUNT ==========")
        print(df_date_dim.count())

        print("========== DATE RANGE ==========")

        df_date_dim.select(
            min("date_key").alias("min_date"),
            max("date_key").alias("max_date")
        ).show()

        print("========== DUPLICATE DATE CHECK ==========")

        duplicate_dates = (
            df_date_dim
            .groupBy("date_key")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(f"Duplicate dates: {duplicate_dates}")

        print("========== NULL DATE CHECK ==========")

        null_dates = (
            df_date_dim
            .filter(col("date_key").isNull())
            .count()
        )

        print(f"Null dates: {null_dates}")


        # -------------------------------------------------
        # Write curated date_dim
        # -------------------------------------------------

        curated_path = (
            "s3://globalpartner-data-dea/"
            "curated/date_dim/"
        )

        (
            df_date_dim
            .write
            .mode("overwrite")
            .parquet(curated_path)
        )

        print(
            f"Successfully wrote date_dim to "
            f"{curated_path}"
        )


    except Exception as e:

        print(
            f"ERROR processing date_dim: {str(e)}"
        )

        raise


# ---------------------------------------------------------
# 3. Run job
# ---------------------------------------------------------

build_date_dim()

job.commit()