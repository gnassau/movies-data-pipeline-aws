{{ config(materialized='table') }}

SELECT
    title,
    ROUND(AVG(vote_average)::numeric, 2) AS avg_vote_average,
    SUM(vote_count) AS total_votes
FROM silver.movies  
GROUP BY 1
HAVING SUM(vote_count) > 100
ORDER BY avg_vote_average DESC