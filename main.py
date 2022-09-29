from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from aws_modules.modules import *
import logging

logging.basicConfig(level=logging.INFO)


def create_all_table() -> bool:
    """Create table data_month, count_month, agv_month"""
    create_table("data_month")
    create_table("count_month")
    create_table("agv_month")
    return True


def all_to_bucket() -> bool:
    """All file to bucket"""
    for filename in os.listdir(csv_path):
        logging.info(f"Add {filename} to s3:")
        to_bucket("task8", f"{csv_path}/{filename}")
        logging.info("Done\n")
    return True


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
