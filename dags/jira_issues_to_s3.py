from airflow import DAG
from includes.operators.jira.jira_to_s3 import JiraToS3Operator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, date, timedelta
from includes.config.yamlParser import Parser
from includes.config.jira.jira_fields_list_dict import jira_fields_list
import os

defaults = Parser()

DAG_ID = 'jira_to_s3'
START_DATE = datetime(2021, 1, 1)
TODAY = date.today()
JQL_REQUEST = 'created >= "{TODAY}"'
JIRA_CONNECTION_ID = defaults['jira_conn_id']

S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
S3_ROOT_PATH = 'jira/issues'
S3_KEY = f'{S3_ROOT_PATH}/jira_issues.csv'



# Default settings applied to all tasks
default_args = {
    'owner': defaults['owner'],
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

    jira_to_s3_taks = JiraToS3Operator(
        task_id='jira_to_s3',
        jql=JQL_REQUEST,
        s3_bucket_name=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        replace=True,
        jira_connection_id=JIRA_CONNECTION_ID,
        jira_fields=jira_fields_list
    )

    empty_end_task = EmptyOperator(task_id='end')

    empty_start_task >> jira_to_s3_taks >> empty_end_task