from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models.param import Param

# imports
from src.Bronze.full_load import run_ingestion
from src.Bronze.get_updated_ids import run_get_updated_ids
from src.Bronze.update_movies import run_get_movie_details
from src.Silver.silver_table import run_silver_table
from src.data_quality.dq_checks import run_dq_functions


with DAG(
    dag_id="gold_layer_pipeline",
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
        def get_updated_ids():
            run_get_updated_ids()

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


    # =============================
    # GOLD
    # =============================

    with TaskGroup("gold_layer") as gold:

        create_database = AthenaOperator(
            task_id="create_database",
            query="CREATE DATABASE IF NOT EXISTS movies_gold",
            database="default",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_best_movies = AthenaOperator(
            task_id="create_best_movies",
            query="""
            CREATE OR REPLACE VIEW best_movies AS
            SELECT
                title,
                round(AVG(vote_average), 2) AS avg_vote_average,
                SUM(vote_count) AS total_votes
            FROM movies_silver.movies
            GROUP BY 1
            HAVING SUM(vote_count) > 100
            ORDER BY avg_vote_average DESC
            """,
            database="movies_gold",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_general_performance = AthenaOperator(
            task_id="create_general_performance",
            query="""
            CREATE OR REPLACE VIEW general_performance AS
            SELECT
                COUNT(*) as total_movies,
                AVG(vote_average) as avg_rating,
                SUM(revenue) as total_revenue,
                SUM(budget) as total_budget,
                AVG(revenue - budget) as avg_profit
            FROM movies_silver.movies
            """,
            database="movies_gold",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_high_engagement_movies = AthenaOperator(
            task_id="create_high_engagement_movies",
            query="""
            CREATE OR REPLACE VIEW high_engagement_movies AS
            SELECT
                title,
                popularity,
                round(AVG(vote_average),2) as vote_average,
                SUM(vote_count) as vote_count
            FROM movies_silver.movies
            group by 1,2
            HAVING SUM(vote_count) > 100
            order by popularity desc
            """,
            database="movies_gold",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_most_profitable_movies = AthenaOperator(
            task_id="create_most_profitable_movies",
            query="""
            CREATE OR REPLACE VIEW most_profitable_movies AS
            SELECT
                title,
                MAX(revenue) AS revenue,
                MAX(budget) AS budget,
                MAX(revenue) - MAX(budget) AS profit
            FROM movies_silver.movies
            WHERE budget > 0
            GROUP BY title
            ORDER BY profit DESC
            """,
            database="movies_gold",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_movies_by_year = AthenaOperator(
            task_id="create_movies_by_year",
            query="""
            CREATE OR REPLACE VIEW movies_by_year AS
            SELECT
                year(CAST(release_date AS DATE)) as year,
                COUNT(*) as total_movies
            FROM movies_silver.movies
            GROUP BY year(CAST(release_date AS DATE))
            ORDER BY year(CAST(release_date AS DATE)) desc
            """,
            database="movies_gold",
            output_location="s3://movies-raw-gustavo-portfolio/athena_results/",
            region_name="us-east-1"
        )

        create_database >> [
            create_best_movies,
            create_general_performance,
            create_high_engagement_movies,
            create_most_profitable_movies,
            create_movies_by_year
        ]


# =========================
# PIPELINE FLOW
# =========================

bronze >> silver >> dq >> gold