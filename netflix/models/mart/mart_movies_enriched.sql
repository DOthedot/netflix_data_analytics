{{
    config(
        materialized = 'table'
    )
}}

-- Enriched movie catalogue: movie metadata + top genome tags per movie
-- (tags with relevance_score >= 0.5 are considered "strong" associations)
with movies as (
    select * from {{ ref('dim_movies') }}
),
scores as (
    select * from {{ ref('fct_genome_scores') }}
),
tags as (
    select * from {{ ref('dim_genome_tags') }}
),
links as (
    select * from {{ ref('src_links') }}
)
select
    m.movie_id,
    m.movie_title,
    m.genres,
    m.genre_array,
    t.tag_id,
    t.tag_name,
    s.relevance_score,
    case when s.relevance_score >= 0.5 then true else false end  as is_strong_tag,
    l.imdb_id,
    l.tmdb_id
from movies          as m
left join scores     as s  on m.movie_id = s.movie_id
left join tags       as t  on s.tag_id   = t.tag_id
left join links      as l  on m.movie_id = l.movie_id
