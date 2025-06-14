from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="mon_dag",
    start_date=datetime(2023, 1, 1, tz="Europe/Paris"),
    schedule="@daily",  
    catchup=False,
) as dag:
    t1 = EmptyOperator(task_id="debut")
