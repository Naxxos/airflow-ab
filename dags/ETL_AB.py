"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""

# The DAG object; we'll need this to instantiate a DAG
import xmltodict
import gzip

from pendulum import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task

# Operators; we need this to operate!
DATA_SOURCE_PATH = "/opt/airflow/data/1_source"
DATA_EXTRACT_PATH = "./data/2_extract"

default_args = {
    "owner": "Raphaël BEVENOT",
    "depends_on_past": False,
    "email": ["raphael.bevenot@interieur.gouv.fr"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    # "retry_delay": timedelta(minutes=5),
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


@task()
def extract(chemin_fichier: Path):
    doc = xmltodict.parse(gzip.GzipFile(chemin_fichier), dict_constructor=dict)
    return doc


@task()
def transform(dict_from_xml: dict):
    infos_dict = dict()
    dict_entete_doc = dict_from_xml["DocumentBudgetaire"]["EnTeteDocBudgetaire"]
    infos_dict["siret_coll"] = dict_entete_doc["IdColl"]["@V"]
    infos_dict["libelle_collectivite"] = dict_entete_doc["LibelleColl"]["@V"]
    infos_dict["nature_collectivite"] = dict_entete_doc["NatCEPL"]["@V"]
    infos_dict["departement"] = dict_entete_doc.get("Departement", {}).get("@V", None)

    return infos_dict


@task()
def load(infos_dict: dict):
    print(infos_dict["libelle_collectivite"])


with DAG(
    dag_id="etl_ab",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1, tz="UTC"),
    # description="Ce DAG récupère des actes budgétaires en local pour les insérer en base de données",
    catchup=False,
    tags=["ETL", "AB"],
) as dag:
    extract_data = extract(f"{DATA_SOURCE_PATH}/20200801-1810875.xml.gz")
    transform_data = transform(extract_data)
    load(transform_data)
