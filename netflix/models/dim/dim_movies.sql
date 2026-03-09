with src_movies as (
    select * from {{ ref('src_movies') }}
)
select
    movie_id,
    -- title includes "(year)" suffix in MovieLens — strip it for clean display
    trim(regexp_replace(title, '\\s*\\(\\d{4}\\)\\s*$', ''))   as movie_title,
    -- extract year from the "(YYYY)" suffix
    try_to_number(
        regexp_substr(title, '\\((\\d{4})\\)', 1, 1, 'e', 1)
    )                                                            as release_year,
    split(genres, '|')                                           as genre_array,
    genres
from src_movies
where movie_id is not null
