{{
    config(
        materialized = 'ephemeral'
    )
}} 

with movies as (
    select * from {{ ref('dim_movies') }}
),
tags as (
    select * from {{ ref('dim_genome_tags') }}
),
scores as (
    select * from {{ ref('fct_genome_scores')}}
)
select 
    m.movie_id,
    m.movie_title,
    m.genres,
    t.tag_id,
    t.tag_name,
    s.relevance_score
from movies as m 
left join scores as s on m.movie_id = s.movie_id
left join tags as t on t.tag_id = s.tag_id