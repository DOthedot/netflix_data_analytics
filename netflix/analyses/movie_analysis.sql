-- Top-20 movies by average rating (minimum 100 ratings)
-- Run via: dbt compile --select movie_analysis && cat target/compiled/.../movie_analysis.sql
with ratings_summary as (
    select
        movie_id,
        round(avg(rating), 3)   as average_rating,
        count(*)                as total_ratings
    from {{ ref('fct_ratings') }}
    group by movie_id
    having count(*) >= 100
)
select
    m.movie_title,
    m.release_year,
    m.genres,
    rs.average_rating,
    rs.total_ratings
from ratings_summary            as rs
join {{ ref('dim_movies') }}    as m  on m.movie_id = rs.movie_id
order by rs.average_rating desc, rs.total_ratings desc
limit 20
