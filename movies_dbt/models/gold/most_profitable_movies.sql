SELECT
    title,
    MAX(revenue) AS revenue,
    MAX(budget) AS budget,
    MAX(revenue) - MAX(budget) AS profit
FROM {{ source('silver','movies') }}
WHERE budget > 0
GROUP BY title
ORDER BY profit DESC