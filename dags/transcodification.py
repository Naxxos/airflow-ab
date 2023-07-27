from modules import complex_types, config

from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
import pandas as pd
import duckdb


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 7, 11),
    "catchup": False,
}

DATA_EXTRACT_PATH = "./data/2_extract/"
DATA_TRANSFORM_PATH = "./data/3_transform/"
DB_NAME = DATA_EXTRACT_PATH + "actes_budgetaires.duckdb"
CHAMPS_BUDG = config.CHAMPS_LIGNE_BUDGET


@task
def make_annexe_replace_dict() -> dict:
    return (
        complex_types.parse_all_annexes_fields_documentation()["enum"]
        .dropna()
        .to_dict()
    )


@task
def make_budget_replace_dict() -> dict:
    return complex_types.parse_budget_fields_documentation()["enum"].dropna().to_dict()


@dag(default_args=default_args, schedule_interval=None)
def transcodification():
    to_replace = make_annexe_replace_dict()
    # budget_dict_replace = make_budget_replace_dict()
    # to_replace.update(budget_dict_replace)
    print(to_replace)


dag = transcodification()
