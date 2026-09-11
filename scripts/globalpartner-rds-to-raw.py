import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue / Spark - @params: [JOB_NAME]

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read order_items from RDS SQL Server
def read_order_items():
    try:
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": "GlobalPartner-SQLServer-Connection",
            "dbtable": "order_items"
        }
        
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options=connection_options
        ).toDF()
        
        
        # Inspect the data
        
        print("========== SCHEMA ==========")
        df.printSchema()
        
        # print("========== SAMPLE DATA ==========")
        # df.show(10, truncate=False)
        
        # print("========== ROW COUNT ==========")
        # print(df.count())
        
        # 4. Write order_items to S3 Raw
        
        raw_path = "s3://globalpartner-data-dea/raw/order_items/"
        
        df.write \
            .mode("overwrite") \
            .parquet(raw_path)
        
        print(f"Successfully wrote order_items to {raw_path}")

    except Exception as e:
        print(f"ERROR reading order_items: {str(e)}")
        raise


# Read order_item_options from RDS SQL Server
def read_order_item_options():
    try:
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": "GlobalPartner-SQLServer-Connection",
            "dbtable": "order_item_options"
        }
        
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options=connection_options
        ).toDF()
        
        
        # Inspect the data
        
        print("========== SCHEMA ==========")
        df.printSchema()
        
        # print("========== SAMPLE DATA ==========")
        # df.show(10, truncate=False)
        
        # print("========== ROW COUNT ==========")
        # print(df.count())
        
        # 4. Write order_item_options to S3 Raw
        
        raw_path = "s3://globalpartner-data-dea/raw/order_item_options/"
        
        df.write \
            .mode("overwrite") \
            .parquet(raw_path)
        
        print(f"Successfully wrote order_item_options to {raw_path}")

    except Exception as e:
        print(f"ERROR reading order_item_options: {str(e)}")
        raise


# Read date_dim from RDS SQL Server
def read_date_dim():
    try:
        connection_options = {
            "useConnectionProperties": "true",
            "connectionName": "GlobalPartner-SQLServer-Connection",
            "dbtable": "date_dim"
        }
        
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options=connection_options
        ).toDF()
        
        
        # Inspect the data
        
        print("========== SCHEMA ==========")
        df.printSchema()
        
        # print("========== SAMPLE DATA ==========")
        # df.show(10, truncate=False)
        
        # print("========== ROW COUNT ==========")
        # print(df.count())
        
        # 4. Write date_dim to S3 Raw
        
        raw_path = "s3://globalpartner-data-dea/raw/date_dim/"
        
        df.write \
            .mode("overwrite") \
            .parquet(raw_path)
        
        print(f"Successfully wrote date_dim to {raw_path}")

    except Exception as e:
        print(f"ERROR reading date_dim: {str(e)}")
        raise


# run all methods defined above

read_order_items()
read_order_item_options()
read_date_dim()


# Finish job

job.commit()