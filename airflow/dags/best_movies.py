from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

# imports
from src.Bronze.full_load import run_full_load
from src.Bronze.get_updated_ids import run_get_updated_ids
from src.Bronze.get_movie_details import run_get_movie_details
from src.Silver.silver_table import run_silver_table


with DAG(
    dag_id="gold_layer_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    # =========================
    # BRONZE
    # =========================

    with TaskGroup("bronze_layer") as bronze:

        @task
        def full_load():
            run_full_load()

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

    # =============================
    # GOLD
    # =============================

    create_database = AthenaOperator(
        task_id="create_database",
        query="CREATE DATABASE IF NOT EXISTS movies_gold",
        database="default",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )
    
    create_best_movies = AthenaOperator(
        task_id="create_best_movies",
        query="""
        CREATE OR REPLACE VIEW gold.best_movies AS
        SELECT
            title,
            vote_average,
            vote_count
        FROM silver.movies
        WHERE vote_count > 1000
        ORDER BY vote_average DESC
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )

    create_general_performance = AthenaOperator(
        task_id="create_general_performance",
        query="""
        CREATE OR REPLACE VIEW gold.general_performance AS
        SELECT
            COUNT(*) as total_movies,
            AVG(vote_average) as avg_rating,
            SUM(revenue) as total_revenue,
            SUM(budget) as total_budget,
            AVG(revenue - budget) as avg_profit
        FROM gold.movies
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )

    create_genres_performance = AthenaOperator(
        task_id="create_genres_performance",
        query=""""
        CREATE OR REPLACE VIEW gold.genres_performance AS
        SELECT
            genre,
            COUNT(*) as total_movies,
            AVG(vote_average) as avg_rating
        FROM silver.movie_genres
        GROUP BY genre
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )

    create_high_engagement_movies = AthenaOperator(
        task_id="create_high_engagement_movies",
        query="""
        CREATE OR REPLACE VIEW gold.high_engagement_movies AS
        SELECT
            popularity,
            vote_average
        FROM gold.movies
        WHERE vote_count > 100
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )

    create_most_profitable_movies = AthenaOperator(
        task_id="create_most_profitable_movies",
        query="""
        CREATE OR REPLACE VIEW gold.most_profitable_movies AS
        SELECT
            title,
            revenue,
            budget,
            revenue - budget as profit
        FROM gold.movies
        WHERE budget > 0
        ORDER BY profit DESC
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )

    create_movies_by_year = AthenaOperator(
        task_id="create_movies_by_year",
        query="""
        CREATE OR REPLACE VIEW gold.movies_by_year AS
        SELECT
            year(release_date) as year,
            COUNT(*) as total_movies
        FROM gold.movies
        GROUP BY year
        ORDER BY year
        """,
        database="movies_gold",
        output_location="s3://movies-raw-gustavo-portfolio/"
    )


create_database >> create_best_movies >> create_general_performance >> create_genres_performance >> create_high_engagement_movies >> create_most_profitable_movies >> create_movies_by_year