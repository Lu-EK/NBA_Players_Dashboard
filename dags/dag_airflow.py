import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_actualization",
    schedule="0 8 * * *",
    catchup=False,
    start_date=datetime.datetime(2024, 4, 2),
    dagrun_timeout=datetime.timedelta(minutes=60),
    ) as dag:
    actualize = BashOperator(
        task_id="actualize",
        bash_command="python3 '/home/lucas/Data Science/Project NBA/actualize.py'"
        )

