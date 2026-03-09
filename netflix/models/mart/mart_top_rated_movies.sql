{{
    config(materialized = 'table')
}}

-- Top-rated movies: Bayesian average (genre-weighted) + raw stats
-- Only includes movies with at least {{ var('min_rating_count') }} ratings
with ratings as (
    select * from {{ ref('fct_ratings') }}
),
movies as (
    select * from {{ ref('dim_movies') }}
),
movie_stats as (
    select
        movie_id,
        count(*)            as total_ratings,
        avg(rating)         as avg_rating,
        min(rating)         as min_rating,
        max(rating)         as max_rating,
        stddev(rating)      as stddev_rating,
        sum(case when rating >= 4.0 then 1 else 0 end)  as high_rating_count,
        min(rating_timestamp)   as first_rated_at,
        max(rating_timestamp)   as last_rated_at
    from ratings
    group by movie_id
    having count(*) >= {{ var('min_rating_count') }}
),
-- Bayesian average: (C * m + sum_ratings) / (C + n)
-- C = global mean rating, m = min votes threshold
global_stats as (
    select avg(avg_rating) as global_mean from movie_stats
),
bayesian as (
    select
        ms.movie_id,
        ms.total_ratings,
        ms.avg_rating,
        ms.min_rating,
        ms.max_rating,
        ms.stddev_rating,
        ms.high_rating_count,
        ms.first_rated_at,
        ms.last_rated_at,
        round(
            (g.global_mean * {{ var('min_rating_count') }} + ms.avg_rating * ms.total_ratings)
            / ({{ var('min_rating_count') }} + ms.total_ratings),
            4
        )                                                           as bayesian_avg_rating,
        round(ms.high_rating_count / ms.total_ratings::float, 4)   as pct_high_ratings
    from movie_stats ms
    cross join global_stats g
)
select
    m.movie_id,
    m.movie_title,
    m.release_year,
    m.genres,
    b.total_ratings,
    b.avg_rating,
    b.bayesian_avg_rating,
    b.min_rating,
    b.max_rating,
    b.stddev_rating,
    b.high_rating_count,
    b.pct_high_ratings,
    b.first_rated_at,
    b.last_rated_at,
    rank() over (order by b.bayesian_avg_rating desc, b.total_ratings desc) as overall_rank
from bayesian     as b
join movies       as m  on b.movie_id = m.movie_id
order by overall_rank
