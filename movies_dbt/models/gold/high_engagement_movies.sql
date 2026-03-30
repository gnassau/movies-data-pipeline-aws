SELECT
    movie_id,
    title,
    popularity,
    ROUND(AVG(vote_average)::numeric, 2) as vote_average,
    SUM(vote_count) as vote_count
FROM {{ source('silver','movies') }}
group by 1,2,3
HAVING SUM(vote_count) > 100
order by popularity desc