from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os


owner = "Igal Emona"


# Default settings applied to all tasks
default_args = {
    'owner': owner,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(dag_id='example_dag',
        start_date=datetime(2019, 1, 1),
        max_active_runs=1,
        schedule_interval='0 12 8-14,22-28 * 6',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
        default_args=default_args,
        catchup=False # enable if you don't want historical dag runs to run
        ) as dag:

    empty_start_task = EmptyOperator(task_id='start')

    empty_end_task = EmptyOperator(task_id='end')

    empty_start_task >> empty_end_task