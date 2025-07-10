from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='git_pull',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
) as dag:

    git_pull_task = BashOperator(
        task_id='pull_git_repo',
        bash_command='cd /opt/apps/airflow/dags && git pull origin master',
        dag=dag,
    )
