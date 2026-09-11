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

# process order_items dataset

def process_order_items():
    try:
        raw_path = "s3://globalpartner-data-dea/raw/order_items/"
        df = spark.read.parquet(raw_path)
        
        print("========== RAW SCHEMA ==========")
        df.printSchema()
        
        # Standardize column names
        df = df.toDF(*[c.lower() for c in df.columns])
        
        df = df.withColumn(
            "item_category",
             when(col("item_category") == "BBQ Plateshttps://...", "BBQ Plates")
             .when(col("item_category") == "Bowls0", "Bowls")
             .when(col("item_category") == "Sandwiches`1", "Sandwiches")
             .when(col("item_category") == "Drip Chttps://www.opendining.net/admin/restaurants/62f2ce9824813746ce6f5140/menuitems#offee", "Drip Coffee")
             .when(col("item_category") == "Hot Espresso", "Espresso")
             .when(col("item_category") == "Kid's", "Kids")
             .when(col("item_category") == "BBQ Sides & Extras", "BBQ")
             .when(col("item_category") == "BBQ Plates", "BBQ")
             .when(col("item_category") == "BBQ Plateshttps://order.pxsweb.com/admin/restaurants/622289bc6863d23d066d56ff/menuitems#", "BBQ")
             .when(col("item_category") == "Kid'shttps://www.opendining.net/admin/restaurants/6054db3295b70198148b456d/menuitems#", "Kids")
             .when(col("item_category") == "Sqalads", "Salads")
             .when(col("item_category").isNull() | (col("item_category") == ""), "UNKNOWN")
             .otherwise(col("item_category"))
        )
        
        # Add derived columns
        df = df.withColumn(
            "date_key",
            to_date(col("creation_time_utc"))
        )
        
        df = df.withColumn(
            "item_revenue",
            col("item_price") * col("item_quantity")
        )
        
        # Inspect Refined data
        print("========== REFINED SCHEMA ==========")
        df.printSchema()
        
#        print("========== SAMPLE DATA ==========")
#        df.show(10, truncate=False)
        
#        print("========== ROW COUNT ==========")
#        print(df.count())
        
        # Write to S3 Refined
        refined_path = "s3://globalpartner-data-dea/refined/order_items/"
        
        df.write \
            .mode("overwrite") \
            .parquet(refined_path)
        
        print(f"Successfully wrote refined order_items to {refined_path}")
    
    except Exception as e:
        print(f"ERROR processing order_items: {str(e)}")
        raise

# process order_item_options dataset

def process_order_item_options():
    try:
        raw_path = "s3://globalpartner-data-dea/raw/order_item_options/"
        df = spark.read.parquet(raw_path)
        
        print("========== RAW SCHEMA ==========")
        df.printSchema()
        
        # Standardize column names
        df = df.toDF(*[c.lower() for c in df.columns])
        
        # Inspect Refined data
        print("========== REFINED SCHEMA ==========")
        df.printSchema()
        
#        print("========== SAMPLE DATA ==========")
#        df.show(10, truncate=False)
        
#        print("========== ROW COUNT ==========")
#        print(df.count())
        
        # Write to S3 Refined
        refined_path = "s3://globalpartner-data-dea/refined/order_item_options/"
        
        df.write \
            .mode("overwrite") \
            .parquet(refined_path)
        
        print(f"Successfully wrote refined order_item_options to {refined_path}")

    except Exception as e:
        print(f"ERROR processing order_item_options: {str(e)}")
        raise

# process date_dim dataset

def process_date_dim():
    try:
        raw_path = "s3://globalpartner-data-dea/raw/date_dim/"
        df = spark.read.parquet(raw_path)
        
        print("========== RAW SCHEMA ==========")
        df.printSchema()
        
        # Standardize column names
        df = df.toDF(*[c.lower() for c in df.columns])
        
        # Inspect Refined data
        print("========== REFINED SCHEMA ==========")
        df.printSchema()
        
#        print("========== SAMPLE DATA ==========")
#        df.show(10, truncate=False)
        
#        print("========== ROW COUNT ==========")
#        print(df.count())
        
        # Write to S3 Refined
        refined_path = "s3://globalpartner-data-dea/refined/date_dim/"
        
        df.write \
            .mode("overwrite") \
            .parquet(refined_path)
        
        print(f"Successfully wrote refined date_dim to {refined_path}")

    except Exception as e:
        print(f"ERROR processing date_dim: {str(e)}")
        raise

# Run all methods defined above

process_order_items()
process_order_item_options()
process_date_dim()            

job.commit()
            
