# from datetime import timedelta
from airflow import DAG

from pendulum import datetime
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "Raphaël BEVENOT",
    "depends_on_past": False,
    "email": ["raphael.bevenot@interieur.gouv.fr"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'trigger_rule': 'all_success'
}


@task
def ls_s3():
    s3_hook = S3Hook(aws_conn_id="minio_s3_conn")
    return s3_hook.list_keys(bucket_name="data")


with DAG(
    dag_id="etl_with_minio",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL", "AB", "Minio"],
    default_args=default_args,
) as dag:
    ls_s3()
