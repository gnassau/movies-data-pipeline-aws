SELECT
    title,
    vote_average,
    vote_count
FROM gold.movies
WHERE vote_count > 1000
ORDER BY vote_average DESC