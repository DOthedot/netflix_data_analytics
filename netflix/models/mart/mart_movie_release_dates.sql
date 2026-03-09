{{
    config(materialized = 'table')
}}

-- Movie catalogue enriched with release dates from seed
-- and headline rating stats for quick BI consumption
with movies as (
    select * from {{ ref('dim_movies') }}
),
seed_dates as (
    select * from {{ ref('seed_movie_release_dates') }}
),
rating_stats as (
    select
        movie_id,
        count(*)        as total_ratings,
        avg(rating)     as avg_rating
    from {{ ref('fct_ratings') }}
    group by movie_id
)
select
    m.movie_id,
    m.movie_title,
    m.release_year,
    m.genres,
    d.release_date,
    case
        when d.release_date is not null then 'known'
        else 'unknown'
    end                             as release_info_status,
    coalesce(r.total_ratings, 0)   as total_ratings,
    round(r.avg_rating, 2)          as avg_rating
from movies             as m
left join seed_dates    as d  on m.movie_id = d.movie_id
left join rating_stats  as r  on m.movie_id = r.movie_id
