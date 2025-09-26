from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

def task_a():
    print("Task A done")

def task_b():
    print("Task B executed after 1 hour")

with DAG(dag_id="delayed_task_dag", start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:

    task_a_op = PythonOperator(
        task_id="task_a",
        python_callable=task_a
    )

    wait_1_hour = TimeDeltaSensor(
        task_id="wait_1_hour",
        delta=timedelta(hours=1),
        mode="reschedule"  # <-- frees worker while waiting
    )

    task_b_op = PythonOperator(
        task_id="task_b",
        python_callable=task_b
    )

    task_a_op >> wait_1_hour >> task_b_op
