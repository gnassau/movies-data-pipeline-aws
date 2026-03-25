from multiprocessing import context

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# imports
from src.Bronze.full_load import run_ingestion
from src.Bronze.get_updated_ids import run_get_updated_ids
from src.Bronze.update_movies import run_get_movie_details
from src.Silver.silver_table import run_silver_table
from src.data_quality.dq_checks import run_dq_functions


with DAG(
    dag_id="gold_layer_pipeline_dbt",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
    params={
        "load_type": Param(
            "incremental_refresh",
            type="string",
            enum=["incremental_refresh", "full_refresh"],
            description="Tipo de ingestão"
        ),
        "start_date": Param(
            None,
            type=["null","string"],
            description="Data inicial YYYY-MM-DD"
        ),
        "end_date": Param(
            None,
            type=["null","string"],
            description="Data final YYYY-MM-DD"
        )
    }
) as dag:

    # =========================
    # BRONZE
    # =========================

    with TaskGroup("bronze_layer") as bronze:

        @task
        def full_load(**context):

            params = context["params"]

            load_type = params["load_type"]
            start_date = params["start_date"]
            end_date = params["end_date"]

            print(f"Load type: {load_type}")
            print(f"Start date: {start_date}")
            print(f"End date: {end_date}")

            run_ingestion(
                mode=load_type,
                start_date=start_date,
                end_date=end_date
            )

        @task
        def get_updated_ids(**context):
            params = context["params"]

            load_type = params["load_type"]
            start_date = params["start_date"]
            end_date = params["end_date"]
            print(f"Load type: {load_type}")
            print(f"Start date: {start_date}")
            print(f"End date: {end_date}")

            run_get_updated_ids(mode=load_type, 
                                start_date=start_date,
                                 end_date=end_date)

        @task
        def get_movie_details():
            run_get_movie_details()

        full_load() >> get_updated_ids() >> get_movie_details()


    # =========================
    # SILVER
    # =========================

    with TaskGroup("silver_layer") as silver:

        @task
        def silver_table():
            run_silver_table()

        silver_task = silver_table()


    # =========================
    # DATA QUALITY
    # =========================

    with TaskGroup("Data_quality") as dq:

        @task
        def run_dq_checks():
            run_dq_functions()

        dq_task = run_dq_checks()

    # =========================
    # S3 to Postgres
    # =========================
    with TaskGroup("Postgres") as Postgres:

        s3_to_postgres = BashOperator(
            task_id="s3_to_postgres",
            bash_command="""
            cd /opt/airflow/src/Silver && python silver_s3_to_postgres.py
            """
        )

        s3_to_postgres
    


    # =============================
    # GOLD (DBT)
    # =============================

    with TaskGroup("gold_layer") as gold:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command="""
            cd /opt/airflow/movies_dbt && dbt run --profiles-dir /opt/airflow/movies_dbt
            """
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command="""
            cd /opt/airflow/movies_dbt && dbt run --profiles-dir /opt/airflow/movies_dbt
            """
        )

        dbt_run >> dbt_test



# =========================
# PIPELINE FLOW
# =========================

bronze >> silver >> dq >> Postgres >> gold