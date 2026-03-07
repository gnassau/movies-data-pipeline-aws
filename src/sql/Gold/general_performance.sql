SELECT
    COUNT(*) as total_movies,
    AVG(vote_average) as avg_rating,
    SUM(revenue) as total_revenue,
    SUM(budget) as total_budget,
    AVG(revenue - budget) as avg_profit
FROM gold.movies