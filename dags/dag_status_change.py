from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State

@provide_session
def mark_dag_and_tasks_status(dag_run=None, session=None, **kwargs):
    conf = dag_run.conf or {}
    dag_id = conf.get('dag_id_to_fail')
    start = conf.get('start_date')
    end = conf.get('end_date')
    desired_state = conf.get('status', 'failed').lower()
    cancel_running = conf.get('cancel_running', False)

    if not all([dag_id, start, end]):
        raise ValueError("Missing one or more required parameters: dag_id_to_fail, start_date, end_date")

    if desired_state not in ['failed', 'success']:
        raise ValueError("Invalid status value. Must be 'failed' or 'success'.")

    # Use pendulum for timezone-aware parsing
    start_date = pendulum.parse(start)
    end_date = pendulum.parse(end)

    print(f"[INFO] Updating DAG: {dag_id} from {start_date} to {end_date} to state: {desired_state.upper()}")

    dag_run_state = State.FAILED if desired_state == 'failed' else State.SUCCESS
    task_instance_state = dag_run_state

    # 1. Update DAG Runs
    dag_run_count = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date >= start_date,
        DagRun.execution_date <= end_date
    ).update({DagRun.state: dag_run_state}, synchronize_session=False)

    # 2. Update Task Instances
    task_instances = session.query(TaskInstance).filter(
        TaskInstance.dag_id == dag_id,
        TaskInstance.execution_date >= start_date,
        TaskInstance.execution_date <= end_date
    )

    updated_tasks = 0
    for ti in task_instances:
        should_update = True

        if desired_state == 'failed':
            # Only cancel running/queued if requested
            if ti.state in [State.RUNNING, State.QUEUED] and not cancel_running:
                should_update = False
        ti_prev = ti.state
        if should_update:
            ti.state = task_instance_state
            print(f" - Task {ti.task_id} at {ti.execution_date} changed from {ti_prev} ➜ {task_instance_state}")
            updated_tasks += 1

    session.commit()

    print(f"[INFO] Done. DAG runs updated: {dag_run_count}, Task instances updated: {updated_tasks}")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='mark_dag_and_tasks_status',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "dag_id_to_fail": "data_pipeline_v2",
        "start_date": "2024-12-01",
        "end_date": "2025-06-25",
        "status": "success"
        },
    tags=['admin', 'utility'],
) as dag:

    fail_dag_runs_and_tasks = PythonOperator(
        task_id='fail_dag_runs_and_tasks',
        python_callable=mark_dag_and_tasks_status,
        provide_context=True
    )
