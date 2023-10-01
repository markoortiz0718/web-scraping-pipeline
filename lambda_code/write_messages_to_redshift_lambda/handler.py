import io
import json
import os
import time
import uuid
from datetime import datetime

import boto3
import pandas as pd

# apparently "AWS_REGION" is not allowed as a Lambda env variable
AWS_REGION = os.environ["AWSREGION"]
s3_client = boto3.client("s3")
S3_BUCKET_FOR_REDSHIFT_STAGING = os.environ["S3_BUCKET_FOR_REDSHIFT_STAGING"]
UNPROCESSED_SQS_MESSAGES_FOLDER = os.environ["UNPROCESSED_SQS_MESSAGES_FOLDER"]
PROCESSED_SQS_MESSAGES_FOLDER = os.environ["PROCESSED_SQS_MESSAGES_FOLDER"]

redshift_data_client = boto3.client("redshift-data")
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(".")[0]
REDSHIFT_ROLE_ARN = os.environ["REDSHIFT_ROLE_ARN"]


def execute_sql_statement(sql_statement: str) -> None:
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        Database=REDSHIFT_DATABASE_NAME,
        DbUser=REDSHIFT_USER,
        Sql=sql_statement,
    )
    time.sleep(1)
    while True:
        response = redshift_data_client.describe_statement(Id=response["Id"])
        status = response["Status"]
        if status == "FINISHED":
            print(f"Finished executing the following SQL statement: {sql_statement}")
            return
        elif status in ["SUBMITTED", "PICKED", "STARTED"]:
            time.sleep(1)
        elif status == "FAILED":
            print(response)
            raise  ### figure out useful message in exception
        else:
            print(response)
            raise  ### figure out useful message in exception


def move_s3_file(s3_bucket: str, old_s3_filename: str, new_s3_filename: str) -> None:
    s3_client.copy_object(
        Bucket=s3_bucket,
        Key=new_s3_filename,
        CopySource={"Bucket": s3_bucket, "Key": old_s3_filename},
    )
    s3_client.delete_object(
        Bucket=s3_bucket,
        Key=old_s3_filename,
    )
    print(
        f"Moved s3://{s3_bucket}/{old_s3_filename} to "
        f"s3://{s3_bucket}/{new_s3_filename}"
    )


def lambda_handler(event, context) -> None:
    records = event["Records"]
    assert len(records) == 1, f"SQS batch size should be 1. It is {len(records)}"
    record = records[0]
    body = json.loads(record["body"])
    scraped_messages = json.loads(body["Message"])
    df_messages = pd.DataFrame(scraped_messages)
    with io.BytesIO() as in_memory_csv:
        df_messages.to_csv(in_memory_csv, sep="|", header=False, index=False)
        s3_filename = (
            f"{UNPROCESSED_SQS_MESSAGES_FOLDER}/"
            f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}__{uuid.uuid4()}__"
            f"{len(df_messages)}_records.csv"
        )
        s3_client.put_object(
            Bucket=S3_BUCKET_FOR_REDSHIFT_STAGING,
            Key=s3_filename,
            Body=in_memory_csv.getvalue(),
        )

    sql_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA_NAME};",
        f"""CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME} (
            message_id int8,
            message_timestamp varchar(30),
            message_content varchar(5000),
            reply_message_id float,
            trader_id varchar(30),
            chat_link int8,
            processing_time varchar(30)
        );""",  # `reply_message_id` column is really an integer but pandas has float due to NULL
    ]
    for sql_statement in sql_statements:
        execute_sql_statement(sql_statement=sql_statement)

    sql_statement = f"""
        COPY {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}
        FROM 's3://{S3_BUCKET_FOR_REDSHIFT_STAGING}/{s3_filename}'
        REGION '{AWS_REGION}'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS CSV
        DELIMITER '|'
        QUOTE '"';
    """
    execute_sql_statement(sql_statement=sql_statement)
    move_s3_file(
        s3_bucket=S3_BUCKET_FOR_REDSHIFT_STAGING,
        old_s3_filename=s3_filename,
        new_s3_filename=s3_filename.replace(
            UNPROCESSED_SQS_MESSAGES_FOLDER,
            PROCESSED_SQS_MESSAGES_FOLDER,
        ),
    )
    return
