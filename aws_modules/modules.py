import boto3
from airflow.models import Variable
from botocore import client
from boto3 import resources
import os
import shutil
import logging

logging.basicConfig(level=logging.INFO)

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
region_name = Variable.get('region_name')
endpoint_url = Variable.get('endpoint_url')

bucket_name = Variable.get('bucket_name')
lambda_name = Variable.get('lambda_name')
table_date_name = Variable.get('table_date_name')
table_count_name = Variable.get('table_count_name')
table_avg_name = Variable.get('table_avg_name')
queue_name = Variable.get('queue_name')

csv_path = Variable.get('csv_path')
lambda_package_pash = Variable.get('lambda_package_pash')


def get_client(service: str) -> client:
    """Get boto3 client"""
    return boto3.client(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def get_resource(service: str) -> resources:
    """Get boto3 resources"""
    return boto3.resource(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def to_bucket(path: str, key=None) -> bool:
    """csv file to bucket"""
    if key is None:
        key = path.split("/")[-1]

    get_resource("s3")\
        .Bucket(bucket_name) \
        .put_object(
        Key=key,
        Body=open(path, "rb")
    )
    return True


def create_table(table_name: str) -> bool:
    """Create dynamodb table"""
    dynamodb = get_resource('dynamodb')

    params = {
        'TableName': table_name,
        'KeySchema': [
            {'AttributeName': 'id', 'KeyType': 'HASH'},
            {'AttributeName': 'body', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'id', 'AttributeType': 'S'},
            {'AttributeName': 'body', 'AttributeType': 'S'}
        ],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    }
    try:
        table = dynamodb.create_table(**params)

        logging.info(f"Table status: {table.table_status}")
        logging.info(f"Creating {table_name}...")

        table.wait_until_exists()
    except Exception as ex:
        if str(ex).find("ResourceInUseException"):
            table = dynamodb.Table(table_name)
            table.delete()

            logging.info(f"Updating {table.name}...")

            table.wait_until_not_exists()

            create_table(table_name)
        else:
            raise ex

    logging.info(f"Create table '{table_name}' success!!")
    return True


def create_bucket() -> bool:
    """Create bucket"""
    try:
        get_resource("s3")\
            .create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region_name})
        logging.info("Create bucket successful!!!")

    except Exception as ex:
        if str(ex).find("BucketAlreadyOwnedByYou"):
            logging.info("Bucket already exists, clear bucket....")
            get_resource("s3")\
                .Bucket(bucket_name)\
                .objects.all()\
                .delete()
            logging.info("Done!!!")
        else:
            raise ex

    logging.info(f"Create bucket '{bucket_name}' success!!")
    return True


def create_lambda() -> bool:
    """Create lambda"""
    lambda_client = get_client("lambda")

    def create() -> bool:
        lambda_client.create_function(
            FunctionName=lambda_name,
            Runtime='python3.8',
            Role='role',
            Timeout=123,
            Handler=lambda_name + '.handler',
            Code=dict(ZipFile=zipped_code)
        )
        return True

    shutil.make_archive("lambda", 'zip', lambda_package_pash)
    with open('lambda.zip', 'rb') as f:
        zipped_code = f.read()
    try:
        create()
    except Exception as ex:
        if str(ex).find("Function already exist"):
            logging.info(f"Function '{lambda_name}' already exist, updating function...")
            delete_lambda()
            create()
            logging.info("Done!!!")
        else:
            raise ex

    logging.info(f"Create function '{lambda_name}' success!!!")
    return True


def delete_lambda() -> bool:
    """Delete lambda"""
    lambda_client = get_client("lambda")
    lambda_client.delete_function(
        FunctionName=lambda_name
    )
    os.remove('lambda.zip')
    return True


def add_trigger() -> bool:
    """Add trigger to lambda"""
    get_client("s3").put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': lambda_name,
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }
    )
    logging.info(f"Trigger for '{lambda_name}' function added successfully!!!")
    return True


def create_queue() -> bool:
    """Create queue"""
    sqs_client = get_client("sqs")
    sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "DelaySeconds": "0",
            "VisibilityTimeout": "60",
        }
    )
    logging.info(f"Create queue '{queue_name}' success!!!")
    return True
