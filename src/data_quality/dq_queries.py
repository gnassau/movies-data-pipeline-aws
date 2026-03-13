# ---------------------------------------------------------
# DATA QUALITY QUERIES - MOVIES SILVER
# Todas as validações consideram apenas o batch mais recente
# ---------------------------------------------------------

LATEST_BATCH = """
SELECT MAX(ingestion_timestamp) AS latest
FROM movies_silver.movies
"""

ROWS_LAST_BATCH = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS rows_last_batch
FROM movies_silver.movies
WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
"""

DUPLICATE_IDS = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS duplicate_ids
FROM (
    SELECT movie_id
    FROM movies_silver.movies
    WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
    GROUP BY movie_id
    HAVING COUNT(*) > 1
)
"""

NULL_RELEASE_DATE = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS null_release_date
FROM movies_silver.movies
WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
AND release_date IS NULL
"""

INVALID_VOTE_RANGE = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS invalid_vote_range
FROM movies_silver.movies
WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
AND (vote_average < 0 OR vote_average > 10)
"""

NEGATIVE_RUNTIME = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS negative_runtime
FROM movies_silver.movies
WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
AND runtime <= 0
"""

REVENUE_LT_BUDGET = """
WITH latest_batch AS (
    SELECT MAX(ingestion_timestamp) AS latest
    FROM movies_silver.movies
)
SELECT COUNT(*) AS revenue_lt_budget
FROM movies_silver.movies
WHERE ingestion_timestamp = (SELECT latest FROM latest_batch)
AND revenue < budget
"""