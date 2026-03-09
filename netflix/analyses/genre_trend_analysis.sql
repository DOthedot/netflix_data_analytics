-- Genre rating trends over time
-- Shows which genres grew or declined in popularity year-over-year
with genre_year as (
    select
        genre,
        rating_year,
        total_ratings,
        avg_rating,
        lag(avg_rating) over (
            partition by genre
            order by rating_year
        )                                                               as prev_year_avg,
        lag(total_ratings) over (
            partition by genre
            order by rating_year
        )                                                               as prev_year_volume
    from {{ ref('mart_genre_performance') }}
)
select
    genre,
    rating_year,
    total_ratings,
    avg_rating,
    round(avg_rating - prev_year_avg, 4)                                as avg_rating_yoy_delta,
    round(
        (total_ratings - prev_year_volume) / nullif(prev_year_volume, 0) * 100,
        2
    )                                                                   as volume_pct_change
from genre_year
order by genre, rating_year
