{{ config(materialized='table') }}

SELECT
    movie_id,
    title,
    ROUND(AVG(vote_average)::numeric, 2) AS avg_vote_average,
    SUM(vote_count) AS total_votes
FROM {{ source('silver','movies') }}
GROUP BY movie_id, title
HAVING SUM(vote_count) > 100
ORDER BY avg_vote_average DESC