import boto3
import time
import pandas as pd


DATABASE = "globalpartner"
S3_OUTPUT = "s3://globalpartner-data-dea/athena-results/"


def run_query(query):
    athena = boto3.client(
        "athena",
        region_name="us-east-1"
    )

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": DATABASE
        },
        ResultConfiguration={
            "OutputLocation": S3_OUTPUT
        }
    )

    query_execution_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = result["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ["FAILED", "CANCELLED"]:
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown error"
            )
            raise Exception(f"Athena query failed: {reason}")

        time.sleep(1)

    results = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )

    rows = results["ResultSet"]["Rows"]

    columns = [col["VarCharValue"] for col in rows[0]["Data"]]

    data = []

    for row in rows[1:]:
        data.append([
            col.get("VarCharValue", None)
            for col in row["Data"]
        ])

    return pd.DataFrame(data, columns=columns)