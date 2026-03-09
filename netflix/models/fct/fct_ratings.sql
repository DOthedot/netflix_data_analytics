{{
    config(
        materialized        = 'incremental',
        unique_key          = 'rating_id',
        on_schema_change    = 'fail'
    )
}}

with src_ratings as (
    select * from {{ ref('src_ratings') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'rating_timestamp']) }}  as rating_id,
    user_id,
    movie_id,
    rating,
    rating_timestamp,
    date_trunc('month', rating_timestamp)   as rating_month,
    year(rating_timestamp)                  as rating_year
from src_ratings
where rating is not null

{% if is_incremental() %}
    and rating_timestamp > (select max(rating_timestamp) from {{ this }})
{% endif %}
