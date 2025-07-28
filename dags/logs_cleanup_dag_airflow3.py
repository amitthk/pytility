import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2025, 7, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='logs_cleanup_dag',
    default_args=default_args,
    description='A DAG to clean Airflow logs older than a threshold',
    schedule='@daily',
    catchup=False,
) as dag:

    clean_dag_logs_home = BashOperator(
        task_id='clean_dag_logs_home',
        env={'PATH': "/usr/sbin:"+os.environ['PATH']},
        bash_command="""
            BASE_LOG_FOLDER="/home/airflow/airflow/logs"
            MAX_LOG_AGE_IN_DAYS=30
            echo "Cleaning DAG logs in $BASE_LOG_FOLDER older than $MAX_LOG_AGE_IN_DAYS days..."
            find $BASE_LOG_FOLDER -type f -name '*.log' -mtime +$MAX_LOG_AGE_IN_DAYS -print -delete
            find $BASE_LOG_FOLDER -type d -empty -print -delete
        """,
    )

    clean_dag_logs_opt = BashOperator(
        task_id='clean_dag_logs_opt',
        env={'PATH': "/usr/sbin:"+os.environ['PATH']},
        bash_command="""
            BASE_LOG_FOLDER="/opt/apps/airflow/logs"
            MAX_LOG_AGE_IN_DAYS=30
            echo "Cleaning DAG logs in $BASE_LOG_FOLDER older than $MAX_LOG_AGE_IN_DAYS days..."
            find $BASE_LOG_FOLDER -type f -name '*.log' -mtime +$MAX_LOG_AGE_IN_DAYS -print -delete
            find $BASE_LOG_FOLDER -type d -empty -print -delete
        """,
    )

    clean_scheduler_logs = BashOperator(
        task_id='clean_scheduler_logs',
        env={'PATH': "/usr/sbin:"+os.environ['PATH']},
        bash_command="""
            echo "Cleaning scheduler logs older than 5 days..."

            if [ -d /home/airflow/airflow/logs/scheduler ]; then
                find /home/airflow/airflow/logs/scheduler -type f -mtime +5 -delete -print
            fi

            if [ -d /opt/apps/airflow/logs/scheduler ]; then
                find /opt/apps/airflow/logs/scheduler -type f -mtime +5 -delete -print
            fi
        """,
    )

    # Set dependencies: both DAG log cleaners must finish before scheduler log cleaner runs
    [clean_dag_logs_home, clean_dag_logs_opt] >> clean_scheduler_logs