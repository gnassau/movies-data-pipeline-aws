SELECT
    EXTRACT(YEAR FROM CAST(release_date AS DATE)) AS year,
    COUNT(*) as total_movies
FROM {{ source('silver','movies') }}
GROUP BY EXTRACT(YEAR FROM CAST(release_date AS DATE))
ORDER BY EXTRACT(YEAR FROM CAST(release_date AS DATE)) desc