from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
# from ..includes.config.yamlParser import Parser


DAG_ID = 'jira_to_s3'
START_DATE = datetime(2021, 1, 1)

# default = Parser(file='./includes/config/default.yml')

# Default settings applied to all tasks
default_args = {
    'owner': 'sss', #default['owner'],
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=60)
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(dag_id=DAG_ID,
        start_date=START_DATE,
        max_active_runs=1,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
        tags=["s3", "jira"]
        ) as dag:

    empty_start_task = EmptyOperator(task_id='start')

    empty_end_task = EmptyOperator(task_id='end')

    empty_start_task >> empty_end_task