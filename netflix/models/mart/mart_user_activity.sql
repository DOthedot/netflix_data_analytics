{{
    config(materialized = 'table')
}}

-- User-level engagement summary: rating behaviour + tagging activity
with ratings as (
    select * from {{ ref('fct_ratings') }}
),
tags as (
    select * from {{ ref('src_tags') }}
),
rating_summary as (
    select
        user_id,
        count(*)                    as total_ratings,
        round(avg(rating), 4)       as avg_rating_given,
        min(rating)                 as min_rating_given,
        max(rating)                 as max_rating_given,
        round(stddev(rating), 4)    as stddev_rating_given,
        count(distinct movie_id)    as distinct_movies_rated,
        min(rating_timestamp)       as first_rating_at,
        max(rating_timestamp)       as last_rating_at,
        datediff(
            'day',
            min(rating_timestamp),
            max(rating_timestamp)
        )                           as active_days_span
    from ratings
    group by user_id
),
tag_summary as (
    select
        user_id,
        count(*)                    as total_tags_applied,
        count(distinct movie_id)    as distinct_movies_tagged,
        count(distinct tag)         as distinct_tags_used
    from tags
    group by user_id
),
combined as (
    select
        u.user_id,
        coalesce(r.total_ratings, 0)            as total_ratings,
        coalesce(r.avg_rating_given, null)       as avg_rating_given,
        coalesce(r.min_rating_given, null)       as min_rating_given,
        coalesce(r.max_rating_given, null)       as max_rating_given,
        coalesce(r.stddev_rating_given, null)    as stddev_rating_given,
        coalesce(r.distinct_movies_rated, 0)     as distinct_movies_rated,
        r.first_rating_at,
        r.last_rating_at,
        coalesce(r.active_days_span, 0)          as active_days_span,
        coalesce(t.total_tags_applied, 0)        as total_tags_applied,
        coalesce(t.distinct_movies_tagged, 0)    as distinct_movies_tagged,
        coalesce(t.distinct_tags_used, 0)        as distinct_tags_used,
        case
            when r.total_ratings >= 500 then 'power'
            when r.total_ratings >= 100 then 'regular'
            when r.total_ratings >= 10  then 'casual'
            else 'light'
        end                                      as user_segment
    from {{ ref('dim_users') }}     as u
    left join rating_summary        as r  on u.user_id = r.user_id
    left join tag_summary           as t  on u.user_id = t.user_id
)
select * from combined
