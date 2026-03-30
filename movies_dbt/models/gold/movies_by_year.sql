SELECT
    movie_id,
    EXTRACT(YEAR FROM CAST(release_date AS DATE)) AS year,
    COUNT(*) as total_movies
FROM {{ source('silver','movies') }}
GROUP BY EXTRACT(YEAR FROM CAST(release_date AS DATE)), movie_id
ORDER BY EXTRACT(YEAR FROM CAST(release_date AS DATE)) desc