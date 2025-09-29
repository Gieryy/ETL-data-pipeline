from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os
import json

from utils.generator import generate

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": None,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="generate_data",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate,
        op_kwargs={"start": 1, "end": 1000},
    )

    end = EmptyOperator(task_id="end")
    
    start >> generate_data >> end