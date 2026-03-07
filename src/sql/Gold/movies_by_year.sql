SELECT
    year(release_date) as year,
    COUNT(*) as total_movies
FROM gold.movies
GROUP BY year
ORDER BY year