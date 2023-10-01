import io
import os
import random
import zipfile

import boto3
import pandas as pd

sns_topic = boto3.resource("sns").Topic(arn=os.environ["TOPIC_ARN"])
with open(
    "data-engineering-bezant-assignement-dataset.zip", "rb"
) as f:  # hard coded file
    with zipfile.ZipFile(f, "r") as zf:
        in_memory_csv_file = io.BytesIO(zf.read("telegram.csv"))  # hard coded file
        df_messages = pd.read_csv(in_memory_csv_file)


def lambda_handler(event, context) -> None:
    while True:  # do-while loop
        df_sample = df_messages.sample(random.randint(1, 1000)).reset_index(
            drop=True
        )  # hard coded between 1 and 1000 messages
        if len(df_sample.to_json()) < 256_000:  # max SQS message is 256 KB
            break
    sns_topic.publish(Message=df_sample.to_json(), Subject="scraped message")
    return
