from modules import utils, config

from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
import gzip
import xmltodict
import pandas as pd
import duckdb
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 11),
    "catchup": False,
}

BUCKET = "full"
DATA_EXTRACT_PATH = "./data/2_extract/"
DB_NAME = DATA_EXTRACT_PATH + "actes_budgetaires.duckdb"
CHAMPS_BUDG = config.CHAMPS_LIGNE_BUDGET


@task
def initialize_db(db_name: str):
    with duckdb.connect(db_name) as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE budget ( \
                id_doc_budg UINTEGER, \
                siret_etablissement VARCHAR, \
                libelle VARCHAR, \
                code_insee VARCHAR, \
                nomenclature VARCHAR, \
                exercice UINTEGER, \
                nature_dec VARCHAR, \
                num_dec UINTEGER, \
                nature_vote VARCHAR, \
                type_budget VARCHAR, \
                id_etabl_princ VARCHAR, \
                fk_siret_collectivite VARCHAR, \
                Nature VARCHAR, \
                LibCpte VARCHAR, \
                Fonction VARCHAR, \
                Operation VARCHAR, \
                ContNat VARCHAR, \
                ArtSpe VARCHAR, \
                ContFon VARCHAR, \
                ContOp VARCHAR, \
                CodRD VARCHAR, \
                MtBudgPrec DOUBLE, \
                MtRARPrec DOUBLE, \
                MtPropNouv VARCHAR, \
                MtPrev VARCHAR, \
                CredOuv VARCHAR, \
                MtReal DOUBLE, \
                MtRAR3112 DOUBLE, \
                OpBudg VARCHAR, \
                TypOpBudg VARCHAR, \
                OpeCpteTiers VARCHAR)"
        )


@task
def list_files(hook, bucket: str):
    keys = hook.list_keys(bucket_name=bucket)
    return keys


@task
def process_file(hook: S3Hook, files: list, bucket: str):
    with duckdb.connect(DB_NAME) as conn:
        for file in files:
            id_doc_budg = file.split("-")[1].split(".")[0]

            object_s3 = hook.get_key(bucket_name=bucket, key=file)
            print(file)

            with gzip.GzipFile(fileobj=object_s3.get()["Body"]) as gz_file:
                dict_from_xml = xmltodict.parse(gz_file, dict_constructor=dict)

            temp_df = pd.DataFrame.from_dict(
                utils.parsing_infos_etablissement(dict_from_xml, id_doc_budg)
            )

            temp_df = utils.explode_annexe_json_into_rows_first_way(
                temp_df, CHAMPS_BUDG
            )

            conn.sql("INSERT INTO budget SELECT * FROM temp_df")


@dag(default_args=default_args, schedule_interval=None)
def process_files():
    hook = S3Hook(aws_conn_id="minio_s3_conn")

    initialize_db(db_name=DB_NAME)
    files = list_files(hook=hook, bucket=BUCKET)

    process_file(hook=hook, files=files, bucket=BUCKET)


dag = process_files()
