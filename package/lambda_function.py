import boto3
import json
import time
import pandas as pd
from botocore import client
from boto3 import resources
import logging
from decouple import config

aws_access_key_id = config('aws_access_key_id')
aws_secret_access_key = config('aws_secret_access_key')
region_name = config('region_name')
endpoint_url = config('endpoint_url')


def get_client(service: str) -> client:
    """Get boto3 client"""
    return boto3.client(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        endpoint_url=endpoint_url
    )


def get_resource(service: str) -> resources:
    """Get boto3 resources"""
    return boto3.resource(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        endpoint_url=endpoint_url
    )


def get_queue_url(queue_name: str):
    """Get queue url"""
    sqs_client = get_client("sqs")
    response = sqs_client.get_queue_url(
        QueueName=queue_name,
    )
    return response["QueueUrl"]


def send_message(queue_name: str, message: dict) -> bool:
    """Send sqs message"""
    sqs_client = get_client("sqs")

    sqs_client.send_message(
        QueueUrl=get_queue_url(queue_name),
        MessageBody=json.dumps(message)
    )
    return True


def get_message(queue_name: str):
    """Get sqs message"""
    sqs_client = get_client("sqs")
    response = sqs_client.receive_message(
        QueueUrl=get_queue_url(queue_name)
    )
    return response.get("Messages", [])


def delete_message(queue_name: str, receipt_handle: str):
    """Delete sqs message"""
    sqs_client = get_client("sqs")
    response = sqs_client.delete_message(
        QueueUrl=get_queue_url(queue_name),
        ReceiptHandle=receipt_handle,
    )
    return response


def to_dynamodb(df, table_name):
    """csv file to to_dynamodb"""
    dynamodb = get_resource("dynamodb")
    table = dynamodb.Table(table_name)

    for it in df.iloc:
        id_hash = it["id"]
        body_range = it[df.columns != "id"]
        item = {
            "id": str(id_hash),
            "body": body_range.to_string(),
        }
        table.put_item(Item=item)


def get_s3_file(bucket_name: str, key: str):
    """Get s3 file from bucket"""
    obj = get_client("s3").get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj['Body'], nrows=10, low_memory=False)
    df.rename(columns={'Unnamed: 0': 'id'}, inplace=True)
    df["key"] = key.split(".")[0]
    return df


def get_avg(bucket_name: str, key: str):
    """Get avg"""
    obj = get_client("s3").get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj['Body'], nrows=10, low_memory=False)
    df.rename(columns={'Unnamed: 0': 'id'}, inplace=True)

    df['departure'] = pd.to_datetime(df["departure"])
    df['day'] = df["departure"].dt.day
    avg = df.groupby('day').mean()[[
        'distance (m)',
        'duration (sec.)',
        'duration (sec.)',
        'Air temperature (degC)'
    ]]
    return avg


def handler(event, context):
    if len(event) != 0:
        key = event["Records"][0]["s3"]["object"]["key"]
        if "count" in key:
            send_message("queue", {"key": key})
            logging.info("Send message")
        else:
            while True:
                message = get_message("queue")
                if not message:
                    logging.info("Wait message")
                    time.sleep(5)
                    continue
                else:
                    key_count = json.loads(message[0]["Body"])['key']
                    receipt_handle = message[0]["ReceiptHandle"]

                if not key.split(".")[0] in key_count or key == key_count:
                    time.sleep(5)
                    continue
                else:
                    to_dynamodb(get_s3_file("task8", key), "data_month")
                    to_dynamodb(get_s3_file("task8", key_count), "count_month")
                    to_dynamodb(get_avg("task8", key), "agv_month")
                    delete_message("queue", receipt_handle)
                    break

    else:
        response = {
            'statusCode': 400,
            'body': 'Error created item! Event is empty!',
            'event': event,
            'context': context
        }
        return response

    response = {
        'statusCode': 200,
        'body': 'successfully created item!',
        'event': event,
        'context': context
    }
    return response
