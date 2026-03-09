{{
    config(materialized = 'table')
}}

-- Per-genre analytics: ratings volume, average rating, yearly breakdowns
-- Uses LATERAL FLATTEN to explode the genre_array from dim_movies
with movies as (
    select * from {{ ref('dim_movies') }}
),
ratings as (
    select * from {{ ref('fct_ratings') }}
),
genres_exploded as (
    select
        m.movie_id,
        g.value::string  as genre
    from movies as m,
    lateral flatten(input => m.genre_array) as g
    where g.value::string != '(no genres listed)'
),
genre_ratings as (
    select
        ge.genre,
        r.rating_year,
        count(*)            as total_ratings,
        count(distinct r.user_id)   as unique_raters,
        count(distinct r.movie_id)  as movies_rated,
        round(avg(r.rating), 4)     as avg_rating,
        round(stddev(r.rating), 4)  as stddev_rating
    from genres_exploded    as ge
    join ratings            as r  on ge.movie_id = r.movie_id
    group by ge.genre, r.rating_year
)
select
    genre,
    rating_year,
    total_ratings,
    unique_raters,
    movies_rated,
    avg_rating,
    stddev_rating,
    rank() over (partition by rating_year order by avg_rating desc)         as genre_rank_by_year,
    rank() over (partition by rating_year order by total_ratings desc)      as genre_volume_rank_by_year
from genre_ratings
order by genre, rating_year
