SELECT
    movie_id,
    title,
    MAX(revenue) AS revenue,
    MAX(budget) AS budget,
    MAX(revenue) - MAX(budget) AS profit
FROM {{ source('silver','movies') }}
WHERE budget > 0
GROUP BY title, movie_id
ORDER BY profit DESC