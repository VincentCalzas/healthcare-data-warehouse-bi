from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {"owner": "airflow"}

with DAG(
    dag_id="install_sid",
    schedule="@once",  # Exécution manuelle uniquement
    catchup=False,
    default_args=default_args,
    description="Création des tables dans Snowflake via scripts SQL",
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

    create_db >> [create_soc, create_stg, create_wrk]
