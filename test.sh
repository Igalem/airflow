export SQLALCHEMY_SILENCE_UBER_WARNING=1
export PYTHONPATH=/Users/igale/vsCode/airflow
export AIRFLOW_HOME=/Users/igale/vsCode/airflow
export S3_BUCKET_NAME='xx-xx-xx-dev'
export AIRFLOW_GPL_UNIDECODE=True
export AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION=False

# start airflow webserver (on port 9090)
airflow webserver -D \
    --port 9090 \
    -A $AIRFLOW_HOME/logs/webserver/airflow-webserver.out \
    -E $AIRFLOW_HOME/logs/webserver/airflow-webserver.err \
    -l $AIRFLOW_HOME/logs/webserver/airflow-webserver.log \
    --pid $AIRFLOW_HOME/logs/webserver/airflow-webserver.pid \
    --stderr $AIRFLOW_HOME/logs/webserver/airflow-webserver.stderr \
    --stdout $AIRFLOW_HOME/logs/webserver/airflow-webserver.stdout

# start airflow scheduler
airflow scheduler -D \
    -l $AIRFLOW_HOME/logs/scheduler/airflow-scheduler.log \
    --pid $AIRFLOW_HOME/logs/scheduler/airflow-scheduler.pid \
    --stderr $AIRFLOW_HOME/logs/scheduler/airflow-scheduler.stderr \
    --stdout $AIRFLOW_HOME/logs/scheduler/airflow-scheduler.stdout
