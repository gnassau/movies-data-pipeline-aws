SELECT
    title,
    revenue,
    budget,
    revenue - budget as profit
FROM gold.movies
WHERE budget > 0
ORDER BY profit DESC