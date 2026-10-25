"""
Your first Airflow DAG: a smoke test proving Airflow can see files in dags/.

CONCEPTS
    DAG   : Directed Acyclic Graph = a workflow. Tasks run in a defined order, never in a loop.
    Task  : one unit of work (here, a shell command).
    schedule=None : only runs when you trigger it by hand in the UI.

HOW TO USE: open http://localhost:8080 -> find "hello_ecommerce_platform"
-> click the play button -> Trigger DAG -> watch the task turn green.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_ecommerce_platform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup"],
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Airflow is wired up correctly'",
    )
