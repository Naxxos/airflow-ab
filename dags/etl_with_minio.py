import xmltodict
import gzip
import duckdb
import polars as pl

from airflow import DAG

from pendulum import datetime
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DATA_EXTRACT_PATH = "./data/2_extract"

default_args = {
    "depends_on_past": False,
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

con = duckdb.connect("file.db")
con.execute(
    """
    CREATE OR REPLACE TABLE collectivites 
            (siret_coll INTEGER PRIMARY KEY, 
            libelle_collectivite VARCHAR, 
            nature_collectivite VARCHAR,
            departement VARCHAR
            );
            """
)


@task
def ls_s3(hook):
    hook = S3Hook(aws_conn_id="minio_s3_conn")
    keys = hook.list_keys(bucket_name="data")
    return keys


@task
def process_collectivite(key_doc: str, hook):
    object_s3 = hook.get_key(bucket_name="data", key=key_doc)
    dict_from_xml = xmltodict.parse(
        gzip.GzipFile(fileobj=object_s3.get()["Body"]), dict_constructor=dict
    )
    temp_df = pl.DataFrame(get_infos_coll(dict_from_xml))
    # coll_df = coll_df.vstack(temp_df)
    print(temp_df)

    # coll_df.write_csv(DATA_EXTRACT_PATH, separator=",")
    return {"xml processed": key_doc}


@task
def process_coll(keys: list, hook):
    for file in keys:
        object_s3 = hook.get_key(bucket_name="data", key=file)
        dict_from_xml = xmltodict.parse(
            gzip.GzipFile(fileobj=object_s3.get()["Body"]), dict_constructor=dict
        )
        temp_df = pl.DataFrame(
            get_infos_coll(dict_from_xml),
            schema={
                "siret_coll": pl.UInt32,
                "libelle_collectivite": pl.Utf8,
                "nature_collectivite": pl.Categorical,
                "departement": pl.Categorical,
            },
        )
        # coll_df = coll_df.vstack(temp_df)
        print(temp_df)

    # coll_df.write_csv(DATA_EXTRACT_PATH, separator=",")
    return {"xml processed": len(keys)}


def get_infos_coll(dict_from_xml: dict):
    infos_dict = dict()
    dict_entete_doc = dict_from_xml["DocumentBudgetaire"]["EnTeteDocBudgetaire"]
    infos_dict["siret_coll"] = dict_entete_doc["IdColl"]["@V"]
    infos_dict["libelle_collectivite"] = dict_entete_doc["LibelleColl"]["@V"]
    infos_dict["nature_collectivite"] = dict_entete_doc["NatCEPL"]["@V"]
    infos_dict["departement"] = dict_entete_doc.get("Departement", {}).get("@V", None)

    return infos_dict


with DAG(
    dag_id="etl_with_minio",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL", "AB", "Minio"],
    default_args=default_args,
) as dag:
    s3_hook = S3Hook(aws_conn_id="minio_s3_conn")
    keys = ls_s3(s3_hook)
    process_coll(keys, s3_hook)
    # next_task = [process_collectivite(key, s3_hook) for key in keys]
