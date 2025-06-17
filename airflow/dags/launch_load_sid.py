import csv
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import dotenv
import snowflake.connector

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry-delay": timedelta(minutes=1)
}

DATA_ROOT = Path("Data Hospital")  # à adapter si nécessaire
FOLDER_PATH_PREFIX = "BDD_HOSPITAL_"
FILE_PATTERN = re.compile(r"([A-Z]+)_?(\d{8})$")

ENTITY_COLUMN_COUNTS = {
    "CHAMBRE": 7,
    "CONSULTATION": 13,
    "HOSPITALISATION": 7,
    "MEDICAMENT": 5,
    "PATIENT": 17,
    "PERSONNEL": 10,
    "TRAITEMENT": 8,
}
EXPECTED_ENTITIES = set(ENTITY_COLUMN_COUNTS.keys())

dotenv.load_dotenv()

def load_files_to_snowflake(**context):
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    conn = hook.get_conn()          # ⇒ objet connexion déjà authentifié
    cursor = conn.cursor()

    dag_run = context.get("dag_run")
    date_str = (
        dag_run.conf.get("date") if dag_run and dag_run.conf else context["logical_date"].strftime("%Y%m%d")
    )

    folder_path = DATA_ROOT / f"{FOLDER_PATH_PREFIX}{date_str}"
    if not folder_path.exists():
        raise FileNotFoundError(f"Dossier {folder_path} introuvable.")

    for file_path in folder_path.glob("*.txt"):
        match = FILE_PATTERN.search(file_path.stem)
        if not match:
            continue
        entity, file_date = match.groups()
        if entity not in EXPECTED_ENTITIES or file_date != date_str:
            continue

        with file_path.open(newline="", encoding="utf-8") as f:
            rows = [row[1:] for i, row in enumerate(csv.reader(f, delimiter=";")) if i and len(row[1:]) == ENTITY_COLUMN_COUNTS[entity]]

        if not rows:
            continue

        columns = zip(*rows, strict=False)
        param_list = [
            "ARRAY_CONSTRUCT(" +
            ", ".join(
                "NULL" if v in ("", None, "NULL")
                else f"""'{str(v).replace("'", "''")}'"""
                for v in col
            ) +
            ")"
            for col in columns
        ]


        cursor.execute("USE DATABASE STG;")
        cursor.execute("USE SCHEMA PUBLIC;")
        cursor.execute(f"CALL insert_{entity.lower()}({', '.join(param_list)})")

    conn.commit()
    cursor.close()


with DAG(
    dag_id="launch_load_sid",
    start_date=datetime(2024, 4, 29),
    end_date=datetime(2024,5,8),
    schedule="@daily", 
    catchup=True,
    default_args=default_args,
    description="Pipeline complet de chargement et transformation STG → SOC",
) as dag:
    create_db = SQLExecuteQueryOperator(
        task_id="create_database",
        sql="scripts/create_database.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    create_soc = SQLExecuteQueryOperator(
        task_id="create_SOC",
        sql="scripts/create_SOC.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    create_stg = SQLExecuteQueryOperator(
        task_id="create_STG",
        sql="scripts/create_STG.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    create_wrk = SQLExecuteQueryOperator(
        task_id="create_WRK",
        sql="scripts/create_WRK.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    create_procs = SQLExecuteQueryOperator(
        task_id="create_procedures",
        sql="scripts/insert_STG_procedures.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    load_local_files = PythonOperator(
        task_id="load_data_to_STG",
        python_callable=load_files_to_snowflake,
    )

    stg_to_wrk = SQLExecuteQueryOperator(
        task_id="bascule_STG_to_WRK",
        sql="scripts/insert__STG__to__WRK_STG.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    wrk_stg_to_wrk_soc = SQLExecuteQueryOperator(
        task_id="traitement_WRK_STG_to_WRK_SOC",
        sql="scripts/insert__WRK_STG__to__WRK_SOC.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    wrk_soc_to_soc = SQLExecuteQueryOperator(
        task_id="bascule_WRK_SOC_to_SOC",
        sql="scripts/insert__WRK_SOC__to__SOC.sql",
        conn_id="my_snowflake_conn",
        split_statements=True,
    )

    (
        create_db
        >> [create_soc, create_stg, create_wrk]
        >> create_procs
        >> load_local_files
        >> stg_to_wrk
        >> wrk_stg_to_wrk_soc
        >> wrk_soc_to_soc
    )
