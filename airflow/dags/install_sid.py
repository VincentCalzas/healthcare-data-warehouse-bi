from airflow import DAG
from airflow.models import Connection
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta
import json
import os

conn = Connection(
    conn_id="my_snowflake_conn",
    conn_type="snowflake",
    login="QUENTIN",
    password='aFq0YW,i%2fm#Uy_|5r-',
    extra=json.dumps({
        "account": "CVDFQQX-VN38358",
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN"
    }),
)

# Fonction utilitaire pour construire le chemin du fichier SQL
def get_sql_path(filename):
    print(os.path.join("scripts", filename))
    return os.path.join("scripts", filename)

default_args = {
    "owner": "airflow"
}

with DAG(
    dag_id="init_snowflake_tables",
    start_date = datetime.now() - timedelta(days=1),
    schedule="@once",  # Exécution manuelle uniquement
    catchup=False,
    default_args=default_args,
    description="Création des tables dans Snowflake via scripts SQL",
) as dag:

    create_db = SnowflakeSqlApiOperator(
        task_id="create_database",
        sql=get_sql_path("create_database.sql"),
        snowflake_conn_id="my_snowflake_conn"
    )

    create_soc = SnowflakeSqlApiOperator(
        task_id="create_SOC",
        sql=get_sql_path("create_SOC.sql"),
        snowflake_conn_id="my_snowflake_conn"
    )

    create_stg = SnowflakeSqlApiOperator(
        task_id="create_STG",
        sql=get_sql_path("create_STG.sql"),
        snowflake_conn_id="my_snowflake_conn"
    )

    create_wrk = SnowflakeSqlApiOperator(
        task_id="create_WRK",
        sql=get_sql_path("create_WRK.sql"),
        snowflake_conn_id="my_snowflake_conn"
    )


    create_db >> [create_soc, create_stg, create_wrk]
