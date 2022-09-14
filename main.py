from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from airflow.models import Variable
from botocore import client
from boto3 import resources
import os
import shutil
import logging

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


def create_count_csv(month: str) -> bool:
    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()

    df = spark.read.format("csv") \
        .option("header", True) \
        .load(f"csv/month/{month}.csv")

    df.groupBy("departure_name") \
        .count() \
        .select(
        F.col("departure_name").alias("name"),
        F.col("count").alias("departure_count")
    ).alias('d') \
        .join(
        df.groupBy("return_name")
        .count()
        .select(
            F.col("return_name").alias("name"),
            F.col("count").alias("return_count")
        ).alias('r'),
        F.col('d.name') == F.col('r.name')
    ).select("d.name", "departure_count", "return_count") \
        .toPandas() \
        .to_csv(f"csv/month/{month}_count.csv")
    return True


def get_client(service: str) -> client:
    return boto3.client(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        endpoint_url=endpoint_url
    )


def get_resource(service: str) -> resources:
    return boto3.resource(
        service,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        endpoint_url=endpoint_url
    )


def to_bucket(path: str, key=None) -> bool:
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
    lambda_client = get_client("lambda")
    lambda_client.delete_function(
        FunctionName=lambda_name
    )
    os.remove('lambda.zip')
    return True


def add_trigger() -> bool:
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


def create_all_table():
    create_table("data_month")
    create_table("count_month")
    create_table("agv_month")


def all_to_bucket():
    for filename in os.listdir(csv_path):
        logging.info(f"Add {filename} to s3:")
        to_bucket("task8", f"{csv_path}/{filename}")
        logging.info("Done\n")


with DAG(
    'LocalstackTask',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='Task 8. Localstack',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Localstack'],
) as dag:

    t1 = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket,
    )

    t2 = PythonOperator(
        task_id='create_lambda',
        python_callable=create_lambda,
    )

    t3 = PythonOperator(
        task_id='create_all_table',
        python_callable=create_all_table,
    )

    t4 = PythonOperator(
        task_id='create_queue',
        python_callable=create_queue,
    )

    t5 = PythonOperator(
        task_id='add_trigger',
        python_callable=add_trigger,
    )

    t6 = PythonOperator(
        task_id='all_to_bucket',
        python_callable=all_to_bucket,
    )

    [t1, t2, t3, t4] >> t5 >> t6
