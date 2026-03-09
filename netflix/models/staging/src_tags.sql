with raw_tags as (
    select * from {{ source('netflix', 'r_tags') }}
)
select
    userId                          as user_id,
    movieId                         as movie_id,
    lower(trim(tag))                as tag,
    to_timestamp_ltz(timestamp)     as tag_timestamp
from raw_tags
where tag is not null
