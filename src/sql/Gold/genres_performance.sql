SELECT
    genre,
    COUNT(*) as total_movies,
    AVG(vote_average) as avg_rating
FROM gold.movie_genres
GROUP BY genre
ORDER BY avg_rating DESC