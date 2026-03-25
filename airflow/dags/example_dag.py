from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="silver_s3_to_postgres_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # só roda manual
    catchup=False,
    tags=["silver", "s3", "postgres"],
) as dag:

    s3_to_postgres = BashOperator(
        task_id="s3_to_postgres",
        bash_command="""
        cd /opt/airflow/src/Silver && python silver_s3_to_postgres.py
        """
    )

    s3_to_postgres