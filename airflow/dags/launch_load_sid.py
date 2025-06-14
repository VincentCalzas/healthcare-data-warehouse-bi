from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta
from pathlib import Path
import os
import csv
import re
import dotenv
import snowflake.connector

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_ROOT = Path("Data Hospital")  # à adapter si nécessaire
FOLDER_PATH_PREFIX = "BDD_HOSPITAL_"
FILE_PATTERN = re.compile(r'([A-Z]+)_?(\d{8})$')

ENTITY_COLUMN_COUNTS = {
    "CHAMBRE": 7,
    "CONSULTATION": 13,
    "HOSPITALISATION": 7,
    "MEDICAMENT": 5,
    "PATIENT": 17,
    "PERSONNEL": 10,
    "TRAITEMENT": 8
}
EXPECTED_ENTITIES = set(ENTITY_COLUMN_COUNTS.keys())

dotenv.load_dotenv()  

def load_files_to_snowflake(**context):
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and "date" in dag_run.conf:
        date_str = dag_run.conf["date"]
    else:
        date_str = context["logical_date"].strftime("%Y%m%d")

    if not date_str:
        raise ValueError("La date doit être fournie dans conf.date au format YYYYMMDD")

    folder_path = DATA_ROOT / f"{FOLDER_PATH_PREFIX}{date_str}"
    if not folder_path.exists():
        raise FileNotFoundError(f"Dossier {folder_path} introuvable.")

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=os.getenv("SNOWFLAKE_PRIVATE_KEY"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    cursor = conn.cursor()

    for file_path in folder_path.glob("*.txt"):
        match = FILE_PATTERN.search(file_path.stem)
        if not match:
            continue

        entity, file_date = match.group(1), match.group(2)
        if entity not in EXPECTED_ENTITIES or file_date != date_str:
            continue

        with open(file_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            all_rows = []
            for i, row in enumerate(reader):
                row = row[1:]  # Retire l’ID
                if i == 0:
                    continue  # skip header
                if len(row) != ENTITY_COLUMN_COUNTS[entity]:
                    continue
                all_rows.append(row)

        if not all_rows:
            continue

        columns = list(zip(*all_rows))
        param_list = []
        for col in columns:
            formatted = ["NULL" if v in (None, '', 'NULL') else f"'{str(v).replace('\'', '\'\'')}'" for v in col]
            param_list.append(f"ARRAY_CONSTRUCT({', '.join(formatted)})")

        call_sql = f"CALL insert_{entity.lower()}({', '.join(param_list)})"
        cursor.execute("USE DATABASE STG;")
        cursor.execute("USE SCHEMA PUBLIC;")
        cursor.execute(call_sql)

    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id="launch_load_sid",
    start_date=datetime(2024, 4, 29),
    schedule='@daily',
    catchup=True,
    default_args=default_args,
    description="Pipeline complet de chargement et transformation STG → SOC",
) as dag:

    create_db = SnowflakeSqlApiOperator(
        task_id="create_database",
        sql="scripts/create_database.sql",
        snowflake_conn_id="my_snowflake_conn"
    )

    create_soc = SnowflakeSqlApiOperator(
        task_id="create_SOC",
        sql="scripts/create_SOC.sql",
        snowflake_conn_id="my_snowflake_conn"
    )

    create_stg = SnowflakeSqlApiOperator(
        task_id="create_STG",
        sql="scripts/create_STG.sql",
        snowflake_conn_id="my_snowflake_conn"
    )

    create_wrk = SnowflakeSqlApiOperator(
        task_id="create_WRK",
        sql="scripts/create_WRK.sql",
        snowflake_conn_id="my_snowflake_conn"
    )

    create_procs = SnowflakeSqlApiOperator(
        task_id="create_procedures",
        sql="scripts/insert_STG_procedures.sql",
        snowflake_conn_id="my_snowflake_conn",
    )

    load_local_files = PythonOperator(
        task_id="load_data_to_STG",
        python_callable=load_files_to_snowflake,
    )

    stg_to_wrk = SnowflakeSqlApiOperator(
        task_id="bascule_STG_to_WRK",
        sql="scripts/insert__STG__to__WRK_STG.sql",
        snowflake_conn_id="my_snowflake_conn",
    )

    wrk_stg_to_wrk_soc = SnowflakeSqlApiOperator(
        task_id="traitement_WRK_STG_to_WRK_SOC",
        sql="scripts/insert__WRK_STG__to__WRK_SOC.sql",
        snowflake_conn_id="my_snowflake_conn",
    )

    wrk_soc_to_soc = SnowflakeSqlApiOperator(
        task_id="bascule_WRK_SOC_to_SOC",
        sql="scripts/insert__WRK_SOC__to__SOC.sql",
        snowflake_conn_id="my_snowflake_conn",
    )


    create_db >> create_soc >> create_stg >> create_wrk >> create_procs >> load_local_files >> stg_to_wrk >> wrk_stg_to_wrk_soc >> wrk_soc_to_soc
