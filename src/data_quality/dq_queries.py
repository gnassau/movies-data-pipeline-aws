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
SELECT COUNT(*) FROM (
    SELECT movie_id
    FROM movies_silver.movies
    GROUP BY movie_id
    HAVING COUNT(*) > 1
)
"""

NULL_RELEASE_DATE = """
SELECT COUNT(*)
FROM movies_silver.movies
WHERE release_date IS NULL
"""

INVALID_VOTE_RANGE = """
SELECT COUNT(*)
FROM movies_silver.movies
WHERE vote_average < 0 OR vote_average > 10
"""

NEGATIVE_RUNTIME = """
SELECT COUNT(*)
FROM movies_silver.movies
WHERE runtime <= 0
"""

REVENUE_LT_BUDGET = """
SELECT COUNT(*)
FROM movies_silver.movies
WHERE revenue < budget
"""