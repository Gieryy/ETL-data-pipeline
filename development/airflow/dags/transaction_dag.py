from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

from utils.ingest_data import get_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": None,
    "retry_delay": timedelta(minutes=1)
}

def log_get_data(table, new_table_name):
    logger.info(f"Fetching data from {table} and inserting into {new_table_name}.")
    get_data(table, new_table_name)
    logger.info(f"Data fetching and inserting completed for {new_table_name}.")

with DAG(
    dag_id="transaction_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=log_get_data,
        op_kwargs={"table": "customers", "new_table_name": "new_customers"},
    )

    end = EmptyOperator(task_id="end")

    start >> get_data_task >> end
