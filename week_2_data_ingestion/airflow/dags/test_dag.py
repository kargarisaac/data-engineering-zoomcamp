import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="test7",
    default_args=default_args,
    schedule_interval="0 6 2 * *",
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    t1 = BashOperator(
        task_id="task1",
        bash_command='echo "task1_{{ execution_date }}"'
    )
    t2 = BashOperator(
        task_id="task2",
        bash_command='echo "task2_{{ execution_date }}"'
    )
    t1 >> t2

